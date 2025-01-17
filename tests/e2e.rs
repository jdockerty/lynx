use std::{process::Command, time::Duration};

use assert_cmd::cargo::CommandCargoExt;
use lynx::{
    event::Event,
    query::{InboundQuery, QueryFormat, QueryResponse},
    LYNX_FORMAT_HEADER,
};
use rand::Rng;
use reqwest::{header::CONTENT_TYPE, StatusCode};
use tempfile::TempDir;

mod helpers;

const QUERY_PATH: &str = "api/v1/query";
const INGEST_PATH: &str = "api/v1/ingest";

struct Lynx {
    process: std::process::Child,
    persist_path: TempDir,
    port: u16,
    client: reqwest::Client,
}

impl Lynx {
    pub fn new(max_events: Option<i64>) -> Self {
        let mut rand = rand::thread_rng();
        let port = rand.gen_range(1024..=65535); // User port range
        let persist_path = TempDir::new().unwrap();
        let process = Command::cargo_bin(env!("CARGO_PKG_NAME"))
            .unwrap()
            .env("LYNX_PORT", port.to_string())
            .env("LYNX_PERSIST_PATH", persist_path.path())
            .env("LYNX_PERSIST_EVENTS", max_events.unwrap_or(2).to_string())
            .spawn()
            .expect("Can run lynx");

        // Arbitrary sleep to enforce server startup period
        // TODO: this could be flakey
        std::thread::sleep(Duration::from_secs(2));

        Self {
            process,
            port,
            client: reqwest::Client::new(),
            persist_path,
        }
    }

    pub async fn ingest(&self, event: &Event) {
        let json = serde_json::to_vec(event).unwrap();

        let response = self
            .client
            .post(format!("http://127.0.0.1:{}/{INGEST_PATH}", self.port))
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
            .post(format!("http://127.0.0.1:{}/{QUERY_PATH}", self.port))
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
    let lynx = Lynx::new(None);

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
                Err(e) => eprintln!("{e}"),
            };
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("Timeout reached, query was not successful");
}

#[tokio::test]
async fn ingest_and_persist_check() {
    let lynx = Lynx::new(None);

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
                Err(e) => eprintln!("{e}"),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("Persist event did not occur");
}

#[tokio::test]
async fn persist_with_increased_counter() {
    let lynx = Lynx::new(Some(5));

    let event = helpers::arbitrary_event();
    lynx.ingest(&event).await;
    lynx.ingest(&event).await;

    let namespace_path = lynx.persist_path.path().join("lynx").join(&event.namespace);
    assert!(!std::fs::exists(&namespace_path).unwrap());

    lynx.ingest(&event).await;
    lynx.ingest(&event).await;
    lynx.ingest(&event).await;
    lynx.ingest(&event).await;
    assert!(std::fs::exists(&namespace_path).unwrap());
}
