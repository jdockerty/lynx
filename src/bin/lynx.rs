

use axum::{
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Precision {
    Nanosecond,
    Microsecond
}

#[derive(Debug, Serialize, Deserialize)]
struct Event {
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/ingest", post(ingest));


    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();

    axum::serve(listener, app).await.unwrap();
    Ok(())
}

async fn health() -> &'static str {
    "OK"
}

async fn ingest(Json(event): Json<Event>) -> impl IntoResponse {
    println!("{event:?}");
    (StatusCode::CREATED, "DONE")
}
