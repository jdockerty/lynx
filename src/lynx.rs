use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{Arc, Mutex},
};

use datafusion::{
    arrow::{
        array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
    },
    prelude::SessionContext,
    sql::sqlparser::{dialect::GenericDialect, parser::Parser},
};

use crate::{
    buffer::{MemBuffer, Namespace, Table},
    wal::{TagValue, Wal, WriteRequest},
};

#[derive(Default, Debug, Clone)]
pub struct Measurements {
    pub timestamps: Vec<i64>,
    pub metadata: Vec<HashMap<String, TagValue>>,
    pub values: Vec<String>,
}

/// Lynx, an in-memory time-series database with durable writes.
pub struct Lynx {
    /// Write-ahead log to provide durable writes for incoming data.
    ///
    /// Data MUST be appended to the WAL before making its way into the
    /// in-memory buffer.
    wal: Mutex<Wal>,
    /// Hierarchical in-memory structure which makes the durable writes queryable.
    buffer: MemBuffer,

    query: Arc<SessionContext>,
}

impl Lynx {
    /// Create a new Lynx instance with the given WAL configuration.
    pub fn new(wal_directory: impl AsRef<Path>, max_segment_size: u64) -> Self {
        Self {
            wal: Mutex::new(Wal::new(wal_directory, max_segment_size)),
            buffer: MemBuffer::new(),
            query: Arc::new(SessionContext::new()),
        }
    }

    /// Write a new request into the database.
    ///
    /// This ensures that data is durable before it becomes queryable within
    /// an in-memory buffer.
    pub fn write(&self, payload: WriteRequest) -> Result<(), Box<dyn std::error::Error>> {
        self.wal.lock().unwrap().write(payload.clone())?;
        self.buffer.insert(payload)?;

        Ok(())
    }

    pub async fn query(
        &self,
        namespace: String,
        sql: String,
    ) -> Result<Option<Vec<RecordBatch>>, Box<dyn std::error::Error>> {
        let table_name = parse_table_name(&sql)?;
        // Get a snapshot of the current in-memory data that
        // will be queryable.
        let tables = self.buffer.get(&Namespace(namespace.clone()));

        match tables {
            Some(tables) => {
                let mut timestamps = Vec::new();
                let mut values = Vec::new();
                let mut all_metadata: Vec<HashMap<String, TagValue>> = Vec::new();

                if let Some(partitions) = tables.get(&Table(table_name.clone())) {
                    for partition_values in partitions.values() {
                        timestamps.extend_from_slice(&partition_values.timestamps);
                        values.extend_from_slice(&partition_values.values);
                        all_metadata.extend_from_slice(&partition_values.metadata);
                    }

                    // Collect all unique tag keys
                    let mut tag_keys = HashSet::new();
                    for metadata in &all_metadata {
                        for key in metadata.keys() {
                            tag_keys.insert(key.clone());
                        }
                    }

                    let mut fields = vec![
                        Field::new(
                            "timestamp",
                            DataType::Timestamp(TimeUnit::Microsecond, None),
                            false,
                        ),
                        Field::new("value", DataType::Utf8, false),
                    ];

                    for key in &tag_keys {
                        // Tag values are nullable because not every tag may be present
                        // for every write
                        fields.push(Field::new(key.as_str(), DataType::Utf8, true));
                    }

                    let schema = Arc::new(Schema::new(fields));

                    let timestamp_array =
                        Arc::new(TimestampMicrosecondArray::from_iter_values(timestamps))
                            as ArrayRef;
                    let value_array = Arc::new(StringArray::from_iter_values(values)) as ArrayRef;

                    let mut columns = vec![timestamp_array, value_array];

                    for tag_key in &tag_keys {
                        let tag_values: Vec<Option<String>> = all_metadata
                            .iter()
                            .map(|metadata| metadata.get(tag_key).map(|v| v.to_string()))
                            .collect();
                        let tag_array = Arc::new(StringArray::from(tag_values)) as ArrayRef;
                        columns.push(tag_array);
                    }

                    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();

                    // Deregister/register to show updated results from the new batch.
                    let _ = self.query.deregister_table(&table_name);
                    // TODO: is there a more elegant way to do this?
                    self.query.register_batch(&table_name, batch).unwrap();

                    let df = self.query.sql(&sql).await.unwrap();
                    let results = df.collect().await.unwrap();
                    Ok(Some(results))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }
}

/// Parse the table name out of simple 'SELECT ... FROM <table_name>' style queries.
///
/// # Note
/// This does NOT currently handle complex nested queries.
fn parse_table_name(sql: &str) -> Result<String, Box<dyn std::error::Error>> {
    let dialect = GenericDialect {};
    let mut ast = Parser::new(&dialect).try_with_sql(sql)?;

    if let Some(factor) = ast.parse_select()?.from.into_iter().next() {
        match factor.relation {
            datafusion::sql::sqlparser::ast::TableFactor::Table { name, .. } => {
                return Ok(name.to_string());
            }
            // TODO: more complex queries
            _ => return Err("only basic 'SELECT .. FROM' style queries are supported".into()),
        }
    }

    Err("could not parse a table name from query".into())
}

#[cfg(test)]
mod tests {
    use crate::buffer::PartitionKey;

    use super::*;
    use datafusion::assert_batches_eq;
    use tempfile::TempDir;

    #[test]
    fn write_multiple_requests_same_namespace() {
        let dir = TempDir::new().unwrap();
        let lynx = Lynx::new(dir.path(), 1024 * 1024);

        let request1 = WriteRequest {
            namespace: "metrics".to_string(),
            measurement: "cpu".to_string(),
            value: "100".to_string(),
            metadata: HashMap::from_iter([(
                "host".to_string(),
                TagValue::String("server1".to_string()),
            )]),
            timestamp: 1_700_000_000_000_000, // 2023-11-14
        };

        let request2 = WriteRequest {
            namespace: "metrics".to_string(),
            measurement: "cpu".to_string(),
            value: "200".to_string(),
            metadata: HashMap::from_iter([(
                "host".to_string(),
                TagValue::String("server2".to_string()),
            )]),
            timestamp: 1_700_000_001_000_000, // Same day
        };

        lynx.write(request1.clone()).unwrap();
        lynx.write(request2).unwrap();

        let tables = lynx.buffer.get(&Namespace("metrics".to_string())).unwrap();
        let partitions = tables.get(&Table("cpu".to_string())).unwrap().clone();
        assert_eq!(partitions.len(), 1);

        // The above requests are part of the same partition, as
        // they use the same timestamp. So it does not matter which
        // one we use.
        let partition_key = PartitionKey::new(request1.timestamp);
        let measurements = partitions.get(&partition_key).unwrap();

        assert_eq!(measurements.timestamps.len(), 2);
        assert_eq!(measurements.values, vec!["100", "200"]);
        assert_eq!(measurements.metadata.len(), 2);
    }

    #[test]
    fn write_multiple_namespaces() {
        let dir = TempDir::new().unwrap();
        let lynx = Lynx::new(dir.path(), 1024 * 1024);

        let request1 = WriteRequest {
            namespace: "cpu".to_string(),
            measurement: "usage".to_string(),
            value: "80".to_string(),
            metadata: HashMap::new(),
            timestamp: 1_700_000_000_000_000,
        };

        let request2 = WriteRequest {
            namespace: "memory".to_string(),
            measurement: "usage".to_string(),
            value: "4096".to_string(),
            metadata: HashMap::new(),
            timestamp: 1_700_000_000_000_000,
        };

        lynx.write(request1).unwrap();
        lynx.write(request2).unwrap();

        assert_eq!(lynx.buffer.namespace_count(), 2);
        assert!(
            lynx.buffer
                .contains_namespace(&Namespace("cpu".to_string()))
        );
        assert!(
            lynx.buffer
                .contains_namespace(&Namespace("memory".to_string()))
        );
    }

    #[test]
    fn partition_by_day() {
        let dir = TempDir::new().unwrap();
        let lynx = Lynx::new(dir.path(), 1024 * 1024);

        let request1 = WriteRequest {
            namespace: "events".to_string(),
            measurement: "clicks".to_string(),
            value: "event1".to_string(),
            metadata: HashMap::new(),
            timestamp: 1_700_000_000_000_000, // 2023-11-14
        };

        let request2 = WriteRequest {
            namespace: "events".to_string(),
            measurement: "clicks".to_string(),
            value: "event2".to_string(),
            metadata: HashMap::new(),
            timestamp: 1_700_086_400_000_000, // 2023-11-15
        };

        lynx.write(request1.clone()).unwrap();
        lynx.write(request2.clone()).unwrap();

        let tables = lynx.buffer.get(&Namespace("events".to_string())).unwrap();
        let partitions = tables.get(&Table("clicks".to_string())).unwrap();

        assert_eq!(partitions.len(), 2);

        let partition_key_req1 = PartitionKey::new(request1.timestamp);
        let partition_key_req2 = PartitionKey::new(request2.timestamp);
        assert!(partitions.contains_key(&partition_key_req1));
        assert!(partitions.contains_key(&partition_key_req2));

        assert_eq!(
            partitions.get(&partition_key_req1).unwrap().values,
            vec!["event1"]
        );
        assert_eq!(
            partitions.get(&partition_key_req2).unwrap().values,
            vec!["event2"]
        );
    }

    #[tokio::test]
    async fn query_results() {
        let dir = TempDir::new().unwrap();
        let lynx = Lynx::new(dir.path(), 1024 * 1024);

        let request = WriteRequest {
            namespace: "events".to_string(),
            measurement: "clicks".to_string(),
            value: "search_button".to_string(),
            metadata: HashMap::new(),
            timestamp: 1,
        };

        lynx.write(request.clone()).unwrap();

        let result = lynx
            .query(
                request.namespace.clone(),
                "SELECT * FROM clicks".to_string(),
            )
            .await
            .unwrap()
            .expect("results returned");

        let expected = vec![
            "+----------------------------+---------------+",
            "| timestamp                  | value         |",
            "+----------------------------+---------------+",
            "| 1970-01-01T00:00:00.000001 | search_button |",
            "+----------------------------+---------------+",
        ];

        assert_eq!(result.len(), 1);
        assert_batches_eq!(expected, &result);

        let request = WriteRequest {
            namespace: "events".to_string(),
            measurement: "clicks".to_string(),
            value: "search_button".to_string(),
            metadata: HashMap::new(),
            timestamp: 100, // updated timestamp
        };

        lynx.write(request.clone()).unwrap();
        let result = lynx
            .query(
                request.namespace.clone(),
                "SELECT * FROM clicks".to_string(),
            )
            .await
            .unwrap()
            .expect("results returned");

        let expected = vec![
            "+----------------------------+---------------+",
            "| timestamp                  | value         |",
            "+----------------------------+---------------+",
            "| 1970-01-01T00:00:00.000001 | search_button |",
            "| 1970-01-01T00:00:00.000100 | search_button |",
            "+----------------------------+---------------+",
        ];
        // Ensure that data written to the same measurement also shows up in results
        assert_batches_eq!(expected, &result);

        assert!(
            lynx.query(
                "not_exist".to_string(),
                "SELECT * FROM not_exist_table".to_string()
            )
            .await
            .unwrap()
            .is_none(),
            "Expected `None` when querying a non-existent namespace/measurement"
        );
    }

    #[test]
    fn parse_table_name_from_query() {
        assert_eq!(
            parse_table_name("SELECT * FROM foo").unwrap(),
            "foo".to_string()
        );
        assert_eq!(
            parse_table_name("SELECT name, age FROM people").unwrap(),
            "people".to_string()
        );
        assert!(parse_table_name("SELECT *").is_err());
        assert!(parse_table_name("INSERT INTO my_table (id) VALUES (1)").is_err());
    }
}
