#![allow(dead_code)]

use std::{process::Command, time::Duration};

use assert_cmd::cargo::CommandCargoExt;
use rand::Rng;
use reqwest::{header::CONTENT_TYPE, StatusCode};
use tempfile::TempDir;

use lynx::{
    event::Event,
    query::{InboundQuery, QueryFormat},
    server::{Persistence, V1_INGEST_PATH, V1_QUERY_PATH},
    LYNX_FORMAT_HEADER,
};

pub struct Lynx {
    pub process: std::process::Child,
    pub client: reqwest::Client,
    pub port: u16,
    pub persist_path: TempDir,
    /// Options that the test instance of lynx was configured with.
    pub options: LynxOptions,
}

#[derive(Default)]
pub struct LynxOptions {
    pub port: Option<u16>,
    pub max_events: Option<i64>,
    pub persist_mode: Option<Persistence>,
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

        Self {
            port,
            persist_path,
            process,
            client: reqwest::Client::new(),
            options: opts,
        }
    }

    pub async fn ensure_healthy(&self) {
        tokio::time::timeout(Duration::from_secs(3), async move {
            let client = reqwest::Client::new();
            loop {
                if client
                    .get(format!("http://127.0.0.1:{}/health", self.port))
                    .send()
                    .await
                    .is_ok()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("Server is healthy");
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

    pub async fn ingest_batch(&self, events: Vec<Event>) {
        let json = serde_json::to_vec(&events).unwrap();

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

pub fn arbitrary_event() -> Event {
    Event {
        namespace: "my_namespace".to_string(),
        name: "test_event".to_string(),
        timestamp: 111,
        precision: None,
        value: 1,
        metadata: serde_json::Value::Null,
    }
}
