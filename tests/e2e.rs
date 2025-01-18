#![expect(dead_code)]

use std::{process::Command, time::Duration};

use assert_cmd::{assert::OutputAssertExt, cargo::CommandCargoExt, output::OutputOkExt};
use lynx::{
    event::Event,
    query::{InboundQuery, QueryFormat, QueryResponse},
    server::{Persistence, V1_INGEST_PATH, V1_QUERY_PATH},
    LYNX_FORMAT_HEADER,
};
use predicates::{boolean::PredicateBooleanExt, str::contains};
use rand::Rng;
use reqwest::{header::CONTENT_TYPE, StatusCode};
use tempfile::{NamedTempFile, TempDir};

mod helpers;

struct Lynx {
    process: std::process::Child,
    client: reqwest::Client,
    port: u16,
    persist_path: TempDir,
    /// Options that the test instance of lynx was configured with.
    options: LynxOptions,
}

#[derive(Default)]
struct LynxOptions {
    port: Option<u16>,
    max_events: Option<i64>,
    persist_mode: Option<Persistence>,
}

impl LynxOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_events(mut self, max_events: i64) -> Self {
        self.max_events = Some(max_events);
        self
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    pub fn with_persist_mode(mut self, persist_mode: Persistence) -> Self {
        self.persist_mode = Some(persist_mode);
        self
    }
}

impl Lynx {
    pub fn new(opts: LynxOptions) -> Self {
        let persist_path = TempDir::new().unwrap();

        let port = opts.port.unwrap_or_else(|| {
            let mut rand = rand::thread_rng();
            rand.gen_range(1024..=65535) // User port range
        });

        let max_events = opts.max_events.unwrap_or(2);
        let persist_mode = opts.persist_mode.clone().unwrap_or_default();

        let process = Command::cargo_bin(env!("CARGO_PKG_NAME"))
            .unwrap()
            .arg("server")
            .env("LYNX_PORT", port.to_string())
            .env("LYNX_PERSIST_PATH", persist_path.path())
            .env("LYNX_PERSIST_EVENTS", max_events.to_string())
            .env("LYNX_PERSIST_MODE", persist_mode.as_str())
            .spawn()
            .expect("Can run lynx");

        // Arbitrary sleep to enforce server startup period
        // TODO: this could be flakey
        std::thread::sleep(Duration::from_secs(2));

        Self {
            port,
            persist_path,
            process,
            client: reqwest::Client::new(),
            options: opts,
        }
    }

    pub async fn ingest(&self, event: &Event) {
        let json = serde_json::to_vec(event).unwrap();

        let response = self
            .client
            .post(format!("http://127.0.0.1:{}/{V1_INGEST_PATH}", self.port))
            .header(CONTENT_TYPE, "application/json")
            .body(json)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
    }

    pub async fn query(&self, namespace: &str, sql: &str, format: QueryFormat) -> String {
        let query = InboundQuery {
            namespace: namespace.to_string(),
            sql: sql.to_string(),
        };

        let json = serde_json::to_vec(&query).unwrap();

        let response = self
            .client
            .post(format!("http://127.0.0.1:{}/{V1_QUERY_PATH}", self.port))
            .header(CONTENT_TYPE, "application/json")
            .header(LYNX_FORMAT_HEADER, format.as_str())
            .body(json)
            .send()
            .await
            .unwrap();

        response.text().await.unwrap()
    }
}

impl Drop for Lynx {
    fn drop(&mut self) {
        self.process.kill().expect("Can kill process on Drop");
    }
}

#[tokio::test]
async fn query_after_persist() {
    let lynx = Lynx::new(LynxOptions::new());

    let event = helpers::arbitrary_event();
    lynx.ingest(&event).await;
    lynx.ingest(&event).await;

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let response = lynx
                .query(
                    &event.namespace,
                    &format!("SELECT * FROM {}", event.namespace),
                    QueryFormat::Json,
                )
                .await;
            match serde_json::from_str::<Vec<QueryResponse>>(&response) {
                Ok(response) => {
                    for r in response {
                        assert_eq!(r.name, event.name);
                        assert_eq!(r.value, event.value);
                    }
                    break;
                }
                Err(e) => eprintln!("Unable to read query response, retrying: {e}"),
            };
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("Timeout reached, query was not successful");
}

#[tokio::test]
async fn ingest_and_persist_check() {
    let lynx = Lynx::new(LynxOptions::new());

    let event = helpers::arbitrary_event();
    lynx.ingest(&event).await;
    lynx.ingest(&event).await;

    tokio::time::timeout(Duration::from_secs(3), async {
        let namespace_path = lynx.persist_path.path().join("lynx").join(event.namespace);
        loop {
            match std::fs::read_dir(&namespace_path) {
                Ok(entries) => {
                    // Short wait for persistence now that the path exists
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    for entry in entries {
                        let entry = entry.unwrap();
                        assert!(
                            entry.file_name().to_string_lossy().contains(".parquet"),
                            "Parquet files are persisted"
                        );
                        assert!(
                            entry.metadata().unwrap().len() > 0,
                            "Persisted file should not be blank"
                        );
                    }
                    break;
                }
                Err(e) => eprintln!("Unable to read {}, retrying: {e}", namespace_path.display()),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("Persist event did not occur");
}

#[tokio::test]
async fn persist_with_increased_counter() {
    let opts = LynxOptions::new().with_max_events(5);
    let lynx = Lynx::new(opts);

    let event = helpers::arbitrary_event();
    lynx.ingest(&event).await;
    lynx.ingest(&event).await;

    let namespace_path = lynx.persist_path.path().join("lynx").join(&event.namespace);
    assert!(
        !std::fs::exists(&namespace_path).unwrap(),
        "Persist should not have happened yet"
    );

    lynx.ingest(&event).await;
    lynx.ingest(&event).await;
    lynx.ingest(&event).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        std::fs::exists(&namespace_path).unwrap(),
        "Expected persist after 5 events"
    );
}

#[tokio::test]
async fn cli() {
    let lynx = Lynx::new(LynxOptions::new().with_max_events(1));

    let write_input = NamedTempFile::new().unwrap();
    let event = helpers::arbitrary_event();
    serde_json::to_writer(&write_input, &event).unwrap();

    let query_input = NamedTempFile::new().unwrap();
    let query = InboundQuery {
        namespace: event.namespace.clone(),
        sql: format!("SELECT * from {}", event.namespace),
    };
    serde_json::to_writer(&query_input, &query).unwrap();

    let _write_cmd = Command::cargo_bin(env!("CARGO_PKG_NAME"))
        .unwrap()
        .arg("write")
        .arg("--port")
        .arg(lynx.port.to_string())
        .arg("--file")
        .arg(write_input.path())
        .unwrap()
        .assert()
        .success();

    let query_output = Command::cargo_bin(env!("CARGO_PKG_NAME"))
        .unwrap()
        .arg("query")
        .arg("--port")
        .arg(lynx.port.to_string())
        .arg("--file")
        .arg(query_input.path())
        .arg("--format")
        .arg("pretty")
        .unwrap()
        .assert()
        .success();

    println!("{}", query_output.to_string());

    query_output
        .stdout(contains(format!("{}", event.name)).and(contains(format!("{}", event.value))));
}
