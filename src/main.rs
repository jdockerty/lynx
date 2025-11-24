mod buffer;
mod lynx;
mod query;
mod wal;

use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
};
use clap::Parser;
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use crate::{lynx::Lynx, query::QueryResponseAdapter, wal::WriteRequest};

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

#[derive(Default, Deserialize)]
enum OutputFormat {
    #[default]
    Json,
    Table,
}

#[derive(Deserialize)]
struct QueryRequest {
    namespace: String,
    query: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    format: Option<OutputFormat>,
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
    match state.lynx.query(payload.namespace, payload.query).await {
        Ok(Some(batches)) => {
            let query_adapter = QueryResponseAdapter::new(batches);
            match payload.format {
                Some(format) => match format {
                    OutputFormat::Json => query_adapter.into_json().unwrap().into_response(),
                    OutputFormat::Table => query_adapter.into_table().unwrap().into_response(),
                },
                None => query_adapter.into_table().unwrap().into_response(),
            }
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            eprintln!("{e:?}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
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
