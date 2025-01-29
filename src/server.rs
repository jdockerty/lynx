use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf};

use crate::query::{parse_table_name_hack, InboundQuery, QueryFormat};
use crate::wal::WalHandle;
use crate::{event::Event, persist::PersistHandle, query::handle_sql};
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
use object_store::ObjectStore;
use tokio::sync::Mutex;

pub const LYNX_FORMAT_HEADER: &str = "X-Lynx-Format";

pub const V1_QUERY_PATH: &str = "api/v1/query";
pub const V1_INGEST_PATH: &str = "api/v1/ingest";

/// The level of persistence to run the server in, this dictates how ingested
/// events are persisted.
///
/// - Local: parquet files will be persisted to the local filesystem.
/// - S3: parquet files will be persisted to an S3-compatible object store.
#[derive(Debug, Clone, Default, clap::ValueEnum)]
pub enum Persistence {
    #[default]
    Local,
    S3,
}

impl Persistence {
    pub fn as_str(&self) -> &str {
        match self {
            Self::S3 => "s3",
            Self::Local => "local",
        }
    }
}

#[derive(Clone)]
struct ServerState {
    files: Arc<Mutex<HashMap<String, SessionContext>>>,
    ingest: PersistHandle,
    wal: WalHandle,
    object_store: Arc<dyn ObjectStore>,
    persist_mode: Persistence,
    persist_path: PathBuf,
}

impl ServerState {
    pub fn new(
        files: Arc<Mutex<HashMap<String, SessionContext>>>,
        max_events: i64,
        persist_path: PathBuf,
        wal_dir: PathBuf,
        wal_buffer_size: usize,
        object_store: Arc<dyn ObjectStore>,
        persist_mode: Persistence,
    ) -> Self {
        Self {
            files: Arc::clone(&files),
            persist_path: persist_path.clone(),
            ingest: PersistHandle::new(files, persist_path, max_events, object_store.clone()),
            wal: WalHandle::new(wal_dir, 0, wal_buffer_size),
            object_store,
            persist_mode,
        }
    }
}

pub struct ServerRunConfig {
    events_before_persist: i64,
    host: String,
    object_store: Arc<dyn ObjectStore>,
    persist_mode: Persistence,
    persist_path: PathBuf,
    wal_dir: PathBuf,
    wal_buffer_size: usize,
    port: u16,
}

impl ServerRunConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        host: &str,
        port: u16,
        events_before_persist: i64,
        persist_path: PathBuf,
        wal_dir: PathBuf,
        wal_buffer_size: usize,
        object_store: Arc<dyn ObjectStore>,
        persist_mode: Persistence,
    ) -> Self {
        Self {
            host: host.to_string(),
            port,
            events_before_persist,
            persist_path,
            wal_dir,
            wal_buffer_size,
            persist_mode,
            object_store,
        }
    }
}

pub async fn run(config: ServerRunConfig) -> Result<(), Box<dyn std::error::Error>> {
    let ServerRunConfig {
        host,
        port,
        events_before_persist,
        persist_path,
        wal_dir,
        wal_buffer_size,
        object_store,
        persist_mode,
    } = config;

    let files = Arc::new(Mutex::new(HashMap::new()));
    let state = ServerState::new(
        files,
        events_before_persist,
        persist_path,
        wal_dir,
        wal_buffer_size,
        object_store,
        persist_mode,
    );

    let app = Router::new()
        .route("/health", get(health))
        .route(&format!("/{V1_INGEST_PATH}"), post(ingest))
        .route(&format!("/{V1_QUERY_PATH}"), post(query))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("{host}:{port}")).await?;
    eprintln!("Running {}", listener.local_addr().unwrap());

    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> &'static str {
    "OK"
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
// Incoming events are JSON blobs which do not indicate their "type", except by
// being an array in batch and a singular JSON structure for 1 event.
#[serde(untagged)]
enum IngestType {
    Single(Event),
    Batch(Vec<Event>),
}

async fn ingest(
    State(mut state): State<ServerState>,
    Json(ingest_type): Json<IngestType>,
) -> impl IntoResponse {
    match ingest_type {
        IngestType::Single(event) => {
            state.wal.append(event.clone()).await;
            state.ingest.handle_event(event).await;
        }
        IngestType::Batch(events) => {
            for event in events {
                state.wal.append(event.clone()).await;
                state.ingest.handle_event(event).await;
            }
        }
    }
    StatusCode::CREATED
}

async fn query(
    headers: HeaderMap,
    State(state): State<ServerState>,
    Json(payload): Json<InboundQuery>,
) -> (StatusCode, impl IntoResponse) {
    let table_name = match parse_table_name_hack(&payload.sql) {
        Ok(table_name) => table_name,
        Err(e) => {
            eprintln!("query error: {e}");
            return (
                StatusCode::BAD_REQUEST,
                "An unsupported query was provided".to_string(),
            );
        }
    };
    let namespace_path = &format!(
        "{}/{}/{}",
        state.persist_path.to_string_lossy(),
        &payload.namespace,
        table_name,
    );
    if let Some(record_batches) = handle_sql(
        state.files,
        &payload.namespace,
        &table_name,
        payload.sql,
        namespace_path,
        state.object_store,
        state.persist_mode,
    )
    .await
    {
        let format = match headers.get(LYNX_FORMAT_HEADER) {
            Some(v) => v.to_str().unwrap().into(),
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
