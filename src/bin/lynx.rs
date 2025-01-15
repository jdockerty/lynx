use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf};

use arrow::util::pretty::pretty_format_batches;
use axum::http::HeaderMap;
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use lynx::{event::Event, persist::PersistHandle, query::handle_sql};

const LYNX_FORMAT_HEADER: &str = "X-Lynx-Format";

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

#[derive(Debug, Clone, Default)]
enum QueryFormat {
    #[default]
    Json,
    Pretty,
}

impl From<&str> for QueryFormat {
    fn from(value: &str) -> Self {
        match value {
            "json" => QueryFormat::Json,
            "pretty" => QueryFormat::Pretty,
            _ => QueryFormat::Json, // Default to JSON
        }
    }
}

#[derive(Clone)]
struct ServerState {
    ingest: PersistHandle,
    persist_path: PathBuf,
    files: Arc<Mutex<HashMap<String, SessionContext>>>,
}

impl ServerState {
    pub fn new(
        files: Arc<Mutex<HashMap<String, SessionContext>>>,
        max_events: i64,
        persist_path: PathBuf,
    ) -> Self {
        Self {
            files: Arc::clone(&files),
            persist_path: persist_path.clone(),
            ingest: PersistHandle::new(files, persist_path, max_events),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let events_before_persist: i64 = std::env::var("LYNX_PERSIST_EVENTS")
        .unwrap_or("2".to_string())
        .parse()
        .unwrap();

    let persist_path = std::env::var("LYNX_PERSIST_PATH").unwrap_or("/tmp".to_string());

    let files = Arc::new(Mutex::new(HashMap::new()));
    let state = ServerState::new(files, events_before_persist, persist_path.into());

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

async fn ingest(
    State(mut state): State<ServerState>,
    Json(event): Json<Event>,
) -> impl IntoResponse {
    state.ingest.handle_event(event).await;
    StatusCode::CREATED
}

#[derive(Debug, Serialize, Deserialize)]
struct InboundQuery {
    namespace: String,
    sql: String,
}

async fn query(
    headers: HeaderMap,
    State(state): State<ServerState>,
    Json(payload): Json<InboundQuery>,
) -> (StatusCode, impl IntoResponse) {
    let namespace_path = &format!(
        "{}/lynx/{}",
        state.persist_path.to_string_lossy(),
        &payload.namespace
    );
    if let Some(record_batches) =
        handle_sql(state.files, &payload.namespace, payload.sql, namespace_path).await
    {
        let format = match headers.get(LYNX_FORMAT_HEADER) {
            Some(v) => String::from_utf8_lossy(v.as_bytes())
                .to_string()
                .as_str()
                .into(),
            None => QueryFormat::Json,
        };

        match format {
            QueryFormat::Pretty => {
                let output = pretty_format_batches(&record_batches).unwrap();
                (StatusCode::OK, output.to_string())
            }
            QueryFormat::Json => {
                let buf = Vec::new();
                let mut w = arrow_json::ArrayWriter::new(buf);
                for record in record_batches {
                    w.write(&record).expect("Can write to buffer");
                }
                w.finish().expect("Can finalise buffer");
                let json = String::from_utf8(w.into_inner()).expect("Valid JSON written");
                (StatusCode::OK, json)
            }
        }
    } else {
        (
            StatusCode::NOT_FOUND,
            format!("No persisted files within {}", payload.namespace),
        )
    }
}
