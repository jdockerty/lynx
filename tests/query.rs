use std::process::Command;

use tempfile::TempDir;

#[tokio::test]
async fn query() {
    let temp = TempDir::new().unwrap();

    let _process = Command::new("lynx")
        .env("LYNX_PERSIST_EVENTS", "2")
        .env("LYNX_PERSIST_PATH", temp.path())
        .spawn().unwrap();

}
