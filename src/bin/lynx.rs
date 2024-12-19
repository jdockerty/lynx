use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{ArrayBuilder, ArrayRef, RecordBatch, StringBuilder, TimestampMicrosecondArray, TimestampMicrosecondBuilder, UInt64Builder};
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
struct Event {
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
        let events_before_persist: i64 = std::env::var("LYNX_PERSIST_EVENTS").unwrap_or("2".to_string()).parse().unwrap();
        let mut events_buf = Vec::new();
        let mut events_recv = 0;
        loop {
            match rx.recv().await {
                Some(event) => {
                    println!("{event:?}");
                    events_buf.push(event);
                    events_recv += 1;
                    if events_recv == events_before_persist {
                        println!("Persisting events");
                        let now = chrono::Utc::now().timestamp_micros();
                        let file = std::fs::File::create_new(format!("lynx-{now}.parquet")).unwrap();

                        let mut names = StringBuilder::new();
                        let mut values = UInt64Builder::new();
                        // TODO: precision hints
                        let mut timestamps = TimestampMicrosecondBuilder::new();

                        for event in &events_buf {
                            names.append_value(&event.name);
                            values.append_value(event.value as u64);
                            timestamps.append_value(event.timestamp);
                        }

                        let names = Arc::new(names.finish()) as ArrayRef;
                        let values = Arc::new(values.finish()) as ArrayRef;
                        let timestamps = Arc::new(timestamps.finish()) as ArrayRef;

                        let batch = RecordBatch::try_from_iter(vec![("timestamp", timestamps), ("name", names), ("value", values)]).unwrap();
                        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
                        writer.write(&batch).unwrap();
                        writer.close().unwrap();
                        events_recv = 0;
                    }
                },
                None => {}
            }
        }
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/ingest", post(ingest))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();

    axum::serve(listener, app).await.unwrap();
    Ok(())
}

async fn health() -> &'static str {
    "OK"
}

async fn ingest(State(state): State<ServerState>, Json(event): Json<Event>) -> impl IntoResponse {
    state.ingest.send(event).await.unwrap();

    StatusCode::CREATED
}
