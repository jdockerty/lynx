mod lynx;
mod wal;

use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
};
use clap::Parser;
use datafusion::arrow::util::pretty::print_batches;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use crate::{lynx::Lynx, wal::WriteRequest};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, env = "LYNX_HTTP_ADDR", default_value = "127.0.0.1:3000")]
    bind: SocketAddr,

    #[arg(long, env = "LYNX_WAL_DIRECTORY")]
    wal_directory: PathBuf,

    #[arg(long, env = "LYNX_WAL_MAX_SEGMENT_SIZE", default_value = "52428800")]
    wal_max_segment_size: u64,
}

struct AppState {
    /// An in-memory, durable, time-series database.
    lynx: Lynx,
}

#[derive(Deserialize)]
struct QueryRequest {
    namespace: String,
    query: String,
}

#[derive(Serialize)]
struct QueryResponse {
    // TODO
    results: Vec<serde_json::Value>,
}

async fn health() -> StatusCode {
    StatusCode::OK
}

async fn write_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<WriteRequest>,
) -> impl IntoResponse {
    match state.lynx.write(payload) {
        Ok(_) => StatusCode::OK,
        Err(e) => {
            eprintln!("{e:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

async fn query_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<QueryRequest>,
) -> impl IntoResponse {
    let result = state
        .lynx
        .query(payload.namespace, payload.query)
        .await
        .unwrap();
    result.map(|b| print_batches(&b));
    StatusCode::OK
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let state = Arc::new(AppState {
        lynx: Lynx::new(&args.wal_directory, args.wal_max_segment_size),
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/write", post(write_handler))
        .route("/api/v1/query", post(query_handler))
        .with_state(state);

    println!("Starting server on {}", args.bind);

    let listener = tokio::net::TcpListener::bind(args.bind)
        .await
        .expect("Failed to bind address");

    axum::serve(listener, app)
        .await
        .expect("Server failed to start");
}
