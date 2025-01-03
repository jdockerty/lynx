use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
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
