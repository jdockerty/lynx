use std::{process::Command, time::Duration};

use assert_cmd::{assert::OutputAssertExt, cargo::CommandCargoExt, output::OutputOkExt};
use lynx::query::{InboundQuery, QueryFormat, QueryResponse};
use predicates::{boolean::PredicateBooleanExt, str::contains};
use tempfile::NamedTempFile;

mod helpers;

use helpers::{Lynx, LynxOptions};

#[tokio::test]
async fn query_after_persist() {
    let lynx = Lynx::new(LynxOptions::new());
    lynx.ensure_healthy().await;

    let event = helpers::arbitrary_event();
    lynx.ingest(&event).await;
    lynx.ingest(&event).await;

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let response = lynx
                .query(
                    &event.namespace,
                    &format!("SELECT * FROM {}", event.name),
                    QueryFormat::Json,
                )
                .await;
            match serde_json::from_str::<Vec<QueryResponse>>(&response) {
                Ok(response) => {
                    for r in response {
                        assert_eq!(r.name, event.name);
                        assert_eq!(r.value, event.value);
                    }
                    break;
                }
                Err(e) => eprintln!("Unable to read query response, retrying: {e}"),
            };
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("Timeout reached, query was not successful");
}

#[tokio::test]
async fn ingest_and_persist_check() {
    let lynx = Lynx::new(LynxOptions::new());
    lynx.ensure_healthy().await;

    let event = helpers::arbitrary_event();
    lynx.ingest(&event).await;
    lynx.ingest(&event).await;

    tokio::time::timeout(Duration::from_secs(3), async {
        let namespace_path = lynx
            .persist_path
            .path()
            .join("lynx")
            .join(event.namespace)
            .join(event.name);
        loop {
            match std::fs::read_dir(&namespace_path) {
                Ok(entries) => {
                    // Short wait for persistence now that the path exists
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    for entry in entries {
                        let entry = entry.unwrap();
                        assert!(
                            entry.file_name().to_string_lossy().contains(".parquet"),
                            "Parquet files are persisted"
                        );
                        assert!(
                            entry.metadata().unwrap().len() > 0,
                            "Persisted file should not be blank"
                        );
                    }
                    break;
                }
                Err(e) => eprintln!("Unable to read {}, retrying: {e}", namespace_path.display()),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("Persist event did not occur");
}

#[tokio::test]
async fn persist_with_increased_counter() {
    let opts = LynxOptions::new().with_max_events(5);
    let lynx = Lynx::new(opts);
    lynx.ensure_healthy().await;

    let event = helpers::arbitrary_event();
    lynx.ingest(&event).await;
    lynx.ingest(&event).await;

    let namespace_path = lynx
        .persist_path
        .path()
        .join("lynx")
        .join(&event.namespace)
        .join(&event.name);
    assert!(
        !std::fs::exists(&namespace_path).unwrap(),
        "Persist should not have happened yet"
    );

    lynx.ingest(&event).await;
    lynx.ingest(&event).await;
    lynx.ingest(&event).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        std::fs::exists(&namespace_path).unwrap(),
        "Expected persist after 5 events"
    );
}

#[tokio::test]
async fn ingest_batched_events() {
    let lynx = Lynx::new(LynxOptions::new());
    lynx.ensure_healthy().await;

    let events = vec![
        helpers::arbitrary_event(),
        helpers::arbitrary_event(),
        helpers::arbitrary_event(),
    ];
    let namespace = events[0].namespace.clone();
    let event_name = events[0].name.clone();

    lynx.ingest_batch(events).await;

    tokio::time::timeout(Duration::from_secs(5), async {
        let namespace_path = lynx
            .persist_path
            .path()
            .join("lynx")
            .join(namespace)
            .join(event_name);
        loop {
            match std::fs::read_dir(&namespace_path) {
                Ok(entries) => {
                    // Short wait for persistence now that the path exists
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    for entry in entries {
                        let entry = entry.unwrap();
                        assert!(
                            entry.file_name().to_string_lossy().contains(".parquet"),
                            "Parquet files are persisted"
                        );
                        assert!(
                            entry.metadata().unwrap().len() > 0,
                            "Persisted file should not be blank"
                        );
                    }
                    break;
                }
                Err(e) => eprintln!("Unable to read {}, retrying: {e}", namespace_path.display()),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .expect("Persist event did not occur");
}

#[tokio::test]
async fn cli() {
    let lynx = Lynx::new(LynxOptions::new().with_max_events(1));
    lynx.ensure_healthy().await;

    let write_input = NamedTempFile::new().unwrap();
    let event = helpers::arbitrary_event();
    serde_json::to_writer(&write_input, &event).unwrap();

    let query_input = NamedTempFile::new().unwrap();
    let query = InboundQuery {
        namespace: event.namespace.clone(),
        sql: format!("SELECT * from {}", event.name),
    };
    serde_json::to_writer(&query_input, &query).unwrap();

    let _write_cmd = Command::cargo_bin(env!("CARGO_PKG_NAME"))
        .unwrap()
        .arg("write")
        .arg("--port")
        .arg(lynx.port.to_string())
        .arg("--file")
        .arg(write_input.path())
        .unwrap()
        .assert()
        .success();

    let query_output = Command::cargo_bin(env!("CARGO_PKG_NAME"))
        .unwrap()
        .arg("query")
        .arg("--port")
        .arg(lynx.port.to_string())
        .arg("--file")
        .arg(query_input.path())
        .arg("--format")
        .arg("pretty")
        .unwrap()
        .assert()
        .success();

    println!("{}", query_output);

    query_output.stdout(contains(event.name.to_string()).and(contains(event.value.to_string())));
}
