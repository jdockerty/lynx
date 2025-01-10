use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf};

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use datafusion::datasource::{file_format::parquet::ParquetFormat, listing::ListingOptions};
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use lynx::{event::Event, persist::PersistHandle};

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

#[derive(Clone)]
struct ServerState {
    ingest: PersistHandle,
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
    State(state): State<ServerState>,
    Json(payload): Json<InboundQuery>,
) -> impl IntoResponse {
    let InboundQuery {
        ref namespace,
        ref sql,
    } = payload;

    let list_opts = ListingOptions::new(Arc::new(ParquetFormat::new()));

    match state.files.lock().await.get(namespace) {
        Some(ctx) => {
            let path = format!("/tmp/lynx/{namespace}");
            ctx.register_listing_table(namespace, path, list_opts.clone(), None, None)
                .await
                .unwrap();
            ctx.sql(sql).await.unwrap().show().await.unwrap();
            // TODO: Deregistering after every request seems very wasteful
            ctx.deregister_table(namespace).unwrap();
        }
        None => {
            return (StatusCode::NOT_FOUND, "No persisted files");
        }
    }

    (StatusCode::OK, "OK")
}
