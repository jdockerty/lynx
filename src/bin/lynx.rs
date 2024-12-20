use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
};

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
use datafusion::{
    datasource::{file_format::parquet::ParquetFormat, listing::ListingOptions},
    execution::context::SessionContext,
};
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

// In-memory tracker for persisted files.
static PERSISTED_FILES: LazyLock<Arc<Mutex<HashMap<String, QueryHandler>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));

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

#[derive(Clone)]
struct ServerState {
    ingest: tokio::sync::mpsc::Sender<Event>,
}

#[derive(Clone)]
struct QueryHandler {
    ctx: Arc<SessionContext>,
    files: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (ingest_tx, mut ingest_rx) = tokio::sync::mpsc::channel(100);

    let state = ServerState { ingest: ingest_tx };

    tokio::spawn(async move {
        let events_before_persist: i64 = std::env::var("LYNX_PERSIST_EVENTS")
            .unwrap_or("2".to_string())
            .parse()
            .unwrap();
        let mut events_buf = Vec::new();
        let mut events_recv = 0;
        loop {
            if let Some(event) = ingest_rx.recv().await {
                println!("{event:?}");
                events_buf.push(event.clone());
                events_recv += 1;
                if events_recv == events_before_persist {
                    println!("Persisting events");
                    let path = format!("/tmp/lynx/{}", event.namespace);

                    if !std::fs::exists(&path).unwrap() {
                        std::fs::create_dir_all(&path).unwrap();
                    }

                    let now = chrono::Utc::now().timestamp_micros();
                    let filename = format!("lynx-{now}.parquet");
                    let file = std::fs::File::create_new(format!("{path}/{filename}")).unwrap();

                    let mut names = StringBuilder::new();
                    let mut namespaces = StringBuilder::new();
                    let mut values = UInt64Builder::new();
                    // TODO: precision hints
                    let mut timestamps = TimestampMicrosecondBuilder::new();

                    for event in &events_buf {
                        names.append_value(&event.name);
                        values.append_value(event.value as u64);
                        timestamps.append_value(event.timestamp);
                        namespaces.append_value(&event.namespace);
                    }

                    let names = Arc::new(names.finish()) as ArrayRef;
                    let namespaces = Arc::new(namespaces.finish()) as ArrayRef;
                    let values = Arc::new(values.finish()) as ArrayRef;
                    let timestamps = Arc::new(timestamps.finish()) as ArrayRef;

                    let batch = RecordBatch::try_from_iter(vec![
                        ("timestamp", timestamps),
                        ("name", names),
                        ("value", values),
                        ("namespace", namespaces),
                    ])
                    .unwrap();
                    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
                    writer.write(&batch).unwrap();
                    writer.close().unwrap();
                    {
                        let files = &mut PERSISTED_FILES.lock().await;
                        files
                            .entry(event.namespace)
                            .and_modify(|f| {
                                f.files.push(filename.clone());
                            })
                            .or_insert(QueryHandler {
                                ctx: Arc::new(SessionContext::new()),
                                files: vec![filename],
                            });
                    }
                    events_recv = 0;
                    events_buf.clear();
                }
            }
        }
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/ingest", post(ingest))
        .route("/api/v1/query", post(query))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;

    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> &'static str {
    "OK"
}

async fn ingest(State(state): State<ServerState>, Json(event): Json<Event>) -> impl IntoResponse {
    state.ingest.send(event.clone()).await.unwrap();
    StatusCode::CREATED
}

#[derive(Debug, Serialize, Deserialize)]
struct InboundQuery {
    namespace: String,
    sql: String,
}

#[axum::debug_handler]
async fn query(
    State(_state): State<ServerState>,
    Json(payload): Json<InboundQuery>,
) -> impl IntoResponse {
    if PERSISTED_FILES.lock().await.is_empty() {
        return (StatusCode::NOT_FOUND, "No persisted files");
    }
    let InboundQuery {
        ref namespace,
        ref sql,
    } = payload;

    let list_opts = ListingOptions::new(Arc::new(ParquetFormat::new()));

    let files = PERSISTED_FILES.lock().await;
    match files.get(namespace) {
        Some(handler) => {
            let path = format!("/tmp/lynx/{namespace}");
            handler
                .ctx
                .register_listing_table(namespace, path, list_opts.clone(), None, None)
                .await
                .unwrap();
            handler.ctx.sql(sql).await.unwrap().show().await.unwrap();
            // TODO: Deregistering after every request seems very wasteful
            handler.ctx.deregister_table(namespace).unwrap();
        }
        None => println!("No ctx for {namespace}"),
    }

    (StatusCode::OK, "OK")
}
