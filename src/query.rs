use std::collections::HashMap;
use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::IntoResponse;
use datafusion::datasource::{file_format::parquet::ParquetFormat, listing::ListingOptions};
use datafusion::prelude::SessionContext;
use tokio::sync::Mutex;

pub async fn handle_sql(
    files: Arc<Mutex<HashMap<String, SessionContext>>>,
    namespace: String,
    sql: String,
    namespace_path: &str,
) -> impl IntoResponse {
    let list_opts = ListingOptions::new(Arc::new(ParquetFormat::new()));

    match files.lock().await.get(&namespace) {
        Some(ctx) => {
            ctx.register_listing_table(&namespace, namespace_path, list_opts.clone(), None, None)
                .await
                .unwrap();
            ctx.sql(&sql).await.unwrap().show().await.unwrap();
            // TODO: Deregistering after every request seems very wasteful
            ctx.deregister_table(&namespace).unwrap();
        }
        None => {
            return (
                StatusCode::NOT_FOUND,
                format!("No persisted files within {namespace}"),
            );
        }
    }
    (StatusCode::OK, "OK".to_string())
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use tempfile::TempDir;
    use tokio::sync::Mutex;

    use super::handle_sql;

    #[tokio::test]
    async fn query() {
        let persist_path = TempDir::new().unwrap();
        let namespace = "my_namespace";
        let namespace_path = &persist_path.path().join("lynx").join(namespace);

        let files = Arc::new(Mutex::new(HashMap::new()));

        handle_sql(
            files,
            namespace.to_string(),
            format!("SELECT * FROM {namespace}"),
            &namespace_path.to_string_lossy(),
        )
        .await;
    }
}
