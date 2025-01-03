use std::{collections::HashMap, sync::Arc};

use arrow::array::{
    ArrayRef, RecordBatch, StringBuilder, TimestampMicrosecondBuilder, UInt64Builder,
};
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Precision {
    Nanosecond,
    Microsecond,
}

/// The level of persistence to run the server in, this dictates how ingested
/// events are persisted.
///
/// - Local means that events are ingested and written to local parquet files.
/// - Remote means that events are ingested and parquet files are written into
///   an object store implementation.
#[derive(Debug)]
#[allow(dead_code)]
enum Persistence {
    Local,
    Remote, // TODO
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Event {
    /// Namespace where data should be written.
    namespace: String,
    /// Name of the event which is being recorded.
    name: String,
    /// Timestamp that the event occurred.
    timestamp: i64,
    /// Optional precision of the provided timestamp. When this is not provided,
    /// nanosecond precision is assumed.
    precision: Option<Precision>,
    /// Value associated with the event.
    value: i64,
    /// Arbitrary key-value metadata associated with the event.
    metadata: serde_json::Value,
}

#[derive(Debug, Clone)]
struct ServerState {
    ingest: tokio::sync::mpsc::Sender<Event>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);

    let state = ServerState { ingest: tx };

    tokio::spawn(async move {
        let events_before_persist: i64 = std::env::var("LYNX_PERSIST_EVENTS")
            .unwrap_or("2".to_string())
            .parse()
            .unwrap();
        let mut mem_events: HashMap<String, Vec<Event>> = HashMap::new();
        loop {
            if let Some(event) = rx.recv().await {
                println!("{event:?}");
                let in_mem_event = mem_events
                    .entry(event.namespace.clone())
                    .and_modify(|f| f.push(event.clone()))
                    .or_default();

                if in_mem_event.len() == events_before_persist as usize {
                    println!("Persisting events for {}", event.namespace);
                    for (namespace, events) in &mem_events {
                        let path = format!("/tmp/lynx/{namespace}");

                        if !std::fs::exists(&path).unwrap() {
                            std::fs::create_dir_all(&path).unwrap();
                        }

                        let now = chrono::Utc::now().timestamp_micros();
                        let filename = format!("lynx-{now}.parquet");
                        let file = std::fs::File::create_new(format!("{path}/{filename}")).unwrap();
                        let mut names = StringBuilder::new();
                        let mut values = UInt64Builder::new();
                        // TODO: precision hints
                        let mut timestamps = TimestampMicrosecondBuilder::new();

                        for event in events {
                            names.append_value(&event.name);
                            values.append_value(event.value as u64);
                            timestamps.append_value(event.timestamp);
                        }

                        let names = Arc::new(names.finish()) as ArrayRef;
                        let values = Arc::new(values.finish()) as ArrayRef;
                        let timestamps = Arc::new(timestamps.finish()) as ArrayRef;

                        let batch = RecordBatch::try_from_iter(vec![
                            ("timestamp", timestamps),
                            ("name", names),
                            ("value", values),
                        ])
                        .unwrap();

                        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
                        writer.write(&batch).unwrap();
                        writer.close().unwrap();
                    }
                    mem_events.clear();
                }
            }
        }
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/ingest", post(ingest))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;

    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> &'static str {
    "OK"
}

async fn ingest(State(state): State<ServerState>, Json(event): Json<Event>) -> impl IntoResponse {
    state.ingest.send(event).await.unwrap();

    StatusCode::CREATED
}
