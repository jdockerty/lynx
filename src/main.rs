use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::Json,
    routing::{get, post},
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, env = "LYNX_HTTP_ADDR", default_value = "127.0.0.1:3000")]
    bind: SocketAddr,
}

#[derive(Clone)]
struct AppState {
    // TODO
}

#[derive(Deserialize)]
struct WriteRequest {
    // TODO
    data: serde_json::Value,
}

#[derive(Serialize)]
struct WriteResponse {
    success: bool,
    message: String,
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
    State(_state): State<Arc<AppState>>,
    Json(payload): Json<WriteRequest>,
) -> Result<Json<WriteResponse>, StatusCode> {
    // TODO
    Ok(Json(WriteResponse {
        success: true,
        message: format!("Write request received: {:?}", payload.data),
    }))
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
        // TODO
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
