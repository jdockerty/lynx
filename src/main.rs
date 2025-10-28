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
use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::{Arc, Mutex},
};
use std::{net::SocketAddr, path::PathBuf};
use wal::TagValue;

use crate::wal::{Wal, WriteRequest};

/// Time format string for daily partition keys.
///
/// This means that data is partitioned by day at all times currently.
const DAILY_PARTITION: &str = "%Y-%m-%d";

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
    wal: Mutex<Wal>,
    buffer: Arc<Mutex<BTreeMap<String, BTreeMap<String, Measurements>>>>,
}

#[derive(Default, Debug)]
struct Measurements {
    timestamps: Vec<u64>,
    tags: Vec<(String, TagValue)>,
    values: Vec<String>,
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
    match state.wal.lock().unwrap().write(payload.clone()) {
        Ok(_) => {}
        Err(e) => {
            eprintln!("{e:?}");
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    }
    let mut buffer_guard = state.buffer.lock().unwrap();
    match buffer_guard.entry(payload.namespace) {
        Entry::Vacant(vacant) => {
            let inbound_time = chrono::DateTime::from_timestamp_micros(payload.timestamp as i64)
                .expect("timestamps are currently assumed to be microseconds");

            let partition_key = inbound_time.format(DAILY_PARTITION);
            vacant.insert(BTreeMap::from_iter([(
                partition_key.to_string(),
                Measurements {
                    timestamps: vec![payload.timestamp],
                    tags: payload.tags,
                    values: vec![payload.value],
                },
            )]));
        }
        Entry::Occupied(mut buffer_entry) => {
            let partitions = buffer_entry.get_mut();

            let inbound_time = chrono::DateTime::from_timestamp_micros(payload.timestamp as i64)
                .expect("timestamps are currently assumed to be microseconds");
            let partition_key = inbound_time.format(DAILY_PARTITION);
            match partitions.entry(partition_key.to_string()) {
                Entry::Vacant(init) => {
                    init.insert(Measurements {
                        timestamps: vec![payload.timestamp],
                        tags: payload.tags,
                        values: vec![payload.value],
                    });
                }
                Entry::Occupied(mut buffered_measurements) => {
                    let buffered_measurements = buffered_measurements.get_mut();
                    buffered_measurements.timestamps.push(payload.timestamp);
                    buffered_measurements.tags.extend(payload.tags);
                    buffered_measurements.values.push(payload.value);
                }
            };
        }
    };
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
        buffer: Arc::new(Mutex::new(BTreeMap::new())),
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
