use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::datasource::{file_format::parquet::ParquetFormat, listing::ListingOptions};
use datafusion::prelude::SessionContext;
use tokio::sync::Mutex;

pub async fn handle_sql(
    files: Arc<Mutex<HashMap<String, SessionContext>>>,
    namespace: &str,
    sql: String,
    namespace_path: &str,
) -> Option<Vec<RecordBatch>> {
    let list_opts = ListingOptions::new(Arc::new(ParquetFormat::new()));

    match files.lock().await.get(namespace) {
        Some(ctx) => {
            ctx.register_listing_table(namespace, namespace_path, list_opts.clone(), None, None)
                .await
                .unwrap();
            let df = ctx.sql(&sql).await.unwrap();

            let batches = df.collect().await.unwrap();
            // TODO: Deregistering after every request seems very wasteful
            ctx.deregister_table(namespace).unwrap();
            Some(batches)
        }
        None => None,
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use datafusion::{assert_batches_eq, execution::context::SessionContext};
    use parquet::arrow::ArrowWriter;
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
