mod wal;

use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::{net::SocketAddr, path::PathBuf};

use crate::wal::{Wal, WriteRequest};

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, env = "LYNX_HTTP_ADDR", default_value = "127.0.0.1:3000")]
    bind: SocketAddr,

    #[arg(short, long, env = "LYNX_WAL_DIRECTORY")]
    wal_directory: PathBuf,

    #[arg(
        short,
        long,
        env = "LYNX_WAL_MAX_SEGMENT_SIZE",
        default_value = "52428800"
    )]
    wal_max_segment_size: u64,
}

struct AppState {
    wal: Mutex<Wal>,
}

#[derive(Deserialize)]
struct QueryRequest {
    // TODO
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
    match state.wal.lock().unwrap().write(payload) {
        Ok(_) => {}
        Err(e) => {
            eprintln!("unable to write to wal: {e:?}");
        }
    }
    StatusCode::OK
}

async fn query_handler(
    State(_state): State<Arc<AppState>>,
    Json(payload): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, StatusCode> {
    // TODO
    Ok(Json(QueryResponse {
        results: vec![serde_json::json!({
            "query": payload.query,
            "placeholder": "query results would go here"
        })],
    }))
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let state = Arc::new(AppState {
        wal: Mutex::new(Wal::new(&args.wal_directory, args.wal_max_segment_size)),
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
