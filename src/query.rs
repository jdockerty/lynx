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
        let ctx = SessionContext::new();

        std::fs::create_dir_all(namespace_path).expect("Can create directory for test");

        let temp_file = tempfile::Builder::new()
            .prefix("query-")
            .suffix(".parquet")
            .tempfile_in(namespace_path)
            .unwrap();

        let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();
        // NOTE: the temp file must be passed as a reference, otherwise it will
        // be moved and eventually dropped on writer close, causing it to be deleted.
        let mut writer = ArrowWriter::try_new(&temp_file, to_write.schema(), None).unwrap();
        writer.write(&to_write).unwrap();
        writer.close().unwrap();

        files.lock().await.insert(namespace.to_string(), ctx);

        for f in std::fs::read_dir(namespace_path).unwrap() {
            println!("{f:?}");
        }
        let batches = handle_sql(
            files,
            namespace,
            format!("SELECT * FROM {namespace}"),
            &namespace_path.to_string_lossy(),
        )
        .await
        .expect("Some batches exist for the test");

        #[rustfmt::skip]
        assert_batches_eq!([
            "+-----+",
            "| col |",
            "+-----+",
            "| 1   |",
            "| 2   |",
            "| 3   |",
            "+-----+",
        ], &batches);
    }
}
