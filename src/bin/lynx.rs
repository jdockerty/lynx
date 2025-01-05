use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;

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

// In-memory tracker for persisted files.
pub static PERSISTED_FILES: LazyLock<Arc<Mutex<HashMap<String, QueryHandler>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));

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

#[derive(Debug, Clone)]
struct ServerState {
    ingest: PersistHandle,
}

impl ServerState {
    pub fn new(max_events: i64) -> Self {
        Self {
            ingest: PersistHandle::new(max_events),
        }
    }
}

#[derive(Clone)]
pub struct QueryHandler {
    ctx: Arc<SessionContext>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let events_before_persist: i64 = std::env::var("LYNX_PERSIST_EVENTS")
        .unwrap_or("2".to_string())
        .parse()
        .unwrap();

    let state = ServerState::new(events_before_persist);

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
