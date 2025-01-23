use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::datasource::{file_format::parquet::ParquetFormat, listing::ListingOptions};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser::ast::{SetExpr, TableFactor};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::server::Persistence;

#[derive(Debug, Serialize, Deserialize)]
pub struct InboundQuery {
    pub namespace: String,
    pub sql: String,
}

/// Response from a query after persist.
///
/// TODO: this needs some investigation, but does work for now.
///
/// The timestamp should not be a string, it should be the same i64 value that
/// was given in the original event. It is also missing 'metadata', which should
/// be arbitrary k-v pairs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    pub timestamp: String,
    pub name: String,
    pub value: i64,
}

#[derive(Debug, Clone, Default, clap::ValueEnum)]
pub enum QueryFormat {
    #[default]
    Json,
    Pretty,
}

impl QueryFormat {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Json => "json",
            Self::Pretty => "pretty",
        }
    }
}

impl From<&str> for QueryFormat {
    fn from(value: &str) -> Self {
        match value {
            "json" => QueryFormat::Json,
            "pretty" => QueryFormat::Pretty,
            _ => QueryFormat::Json, // Default to JSON
        }
    }
}

pub async fn handle_sql(
    files: Arc<Mutex<HashMap<String, SessionContext>>>,
    namespace: &str,
    table_name: &str,
    sql: String,
    namespace_path: &str,
    object_store: Arc<dyn ObjectStore>,
    persist_mode: Persistence,
) -> Option<Vec<RecordBatch>> {
    let list_opts = ListingOptions::new(Arc::new(ParquetFormat::new()));

    match files.lock().await.get(namespace) {
        Some(ctx) => {
            if !ctx.table_exist(table_name).unwrap() {
                match persist_mode {
                    Persistence::S3 => {
                        // TODO: better way to do this by explicitly passing the
                        // bucket name?
                        // This leads to leaking the AWS config into the generic
                        // `handle_sql` though too.
                        let path = PathBuf::from(namespace_path);
                        let bucket = path.parent().unwrap().parent().unwrap();
                        let bucket_path =
                            format!("s3://{}/lynx/{namespace}/{table_name}", bucket.display());
                        ctx.register_object_store(
                            &reqwest::Url::parse(&bucket_path).unwrap(),
                            Arc::clone(&object_store),
                        );
                        ctx.register_listing_table(
                            table_name,
                            format!("{bucket_path}{namespace}/{table_name}/"),
                            list_opts.clone(),
                            None,
                            None,
                        )
                        .await
                        .unwrap();
                    }
                    Persistence::Local => {
                        ctx.register_listing_table(
                            table_name,
                            namespace_path,
                            list_opts.clone(),
                            None,
                            None,
                        )
                        .await
                        .unwrap();
                    }
                }
            }
            let df = ctx.sql(&sql).await.unwrap();
            let batches = df.collect().await.unwrap();
            Some(batches)
        }
        None => None,
    }
}

/// Temporary hack to parse table names.
///
/// This is very much a hack by assuming the query is simple.
pub(crate) fn parse_table_name_hack(sql: &str) -> Result<String, Box<dyn std::error::Error>> {
    let statements = datafusion::sql::parser::DFParser::parse_sql(sql)?;
    let statement = statements
        .front()
        .expect("Sub-queries are not supported at this time");

    match statement {
        Statement::Statement(stmt) => match *stmt.clone() {
            datafusion::sql::sqlparser::ast::Statement::Query(query) => match *query.body {
                SetExpr::Select(select) => {
                    let table = select
                        .from
                        .iter()
                        .take(1)
                        .next()
                        .expect("Query should be sent");
                    match &table.relation {
                        TableFactor::Table { name, .. } => Ok(name.to_string().to_lowercase()),
                        _ => todo!(),
                    }
                }
                _ => Err("Only simple SELECT queries are supported at this time".into()),
            },
            _ => Err("Only simple SELECT queries are supported at this time".into()),
        },
        _ => Err("Only simple SELECT queries are supported at this time".into()),
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

    use crate::server::Persistence;

    use super::{handle_sql, parse_table_name_hack};

    #[tokio::test]
    async fn query() {
        let persist_path = TempDir::new().unwrap();
        let namespace = "my_namespace";
        let table_name = "my_table";
        let namespace_path = &persist_path
            .path()
            .join("lynx")
            .join(namespace)
            .join(table_name);

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

        let batches = handle_sql(
            files,
            namespace,
            table_name,
            format!("SELECT * FROM {table_name}"),
            &namespace_path.to_string_lossy(),
            Arc::new(object_store::memory::InMemory::new()),
            Persistence::Local,
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

    #[test]
    fn table_name_hack() {
        assert_eq!(
            parse_table_name_hack("SELECT * from myTable").unwrap(),
            "mytable".to_string()
        );
        assert_eq!(
            parse_table_name_hack("SELECT (id, name) from users").unwrap(),
            "users".to_string()
        );
        assert_eq!(
            parse_table_name_hack("SELECT * from TEST_TABLE").unwrap(),
            "test_table".to_string()
        );
        assert_eq!(
            parse_table_name_hack(
                "SELECT user_name from all_users WHERE name = 'John' AND location = 'UK'"
            )
            .unwrap(),
            "all_users".to_string()
        );
    }
}
