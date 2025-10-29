use std::{
    collections::{BTreeMap, HashSet, btree_map::Entry},
    path::Path,
    sync::{Arc, Mutex},
};

use datafusion::{
    arrow::{
        array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
    },
    catalog::MemTable,
    prelude::SessionContext,
};

use crate::wal::{TagValue, Wal, WriteRequest};

/// Time format string for daily partition keys.
///
/// This means that data is partitioned by day at all times currently.
const DAILY_PARTITION: &str = "%Y-%m-%d";

#[derive(Default, Debug, Clone)]
pub struct Measurements {
    pub timestamps: Vec<i64>,
    pub tags: Vec<Vec<(String, TagValue)>>,
    pub values: Vec<String>,
}

#[derive(Ord, Eq, PartialEq, PartialOrd, Clone)]
struct PartitionKey(String);

impl PartitionKey {
    pub fn new(timestamp: i64) -> Self {
        let utc_datetime = chrono::DateTime::from_timestamp_micros(timestamp as i64)
            .expect("timestamps are currently assumed to be microseconds");
        Self(utc_datetime.format(DAILY_PARTITION).to_string())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Namespace(String);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Table(String);

/// Lynx, an in-memory time-series database with durable writes.
pub struct Lynx {
    /// Write-ahead log to provide durable writes for incoming data.
    ///
    /// Data MUST be appended to the WAL before making its way into the
    /// in-memory buffer.
    wal: Mutex<Wal>,
    /// Hierarchical in-memory structure which makes the durable writes queryable.
    buffer: Arc<Mutex<BTreeMap<Namespace, BTreeMap<Table, BTreeMap<PartitionKey, Measurements>>>>>,

    query: Arc<SessionContext>,
}

impl Lynx {
    /// Create a new Lynx instance with the given WAL configuration.
    pub fn new(wal_directory: impl AsRef<Path>, max_segment_size: u64) -> Self {
        Self {
            wal: Mutex::new(Wal::new(wal_directory, max_segment_size)),
            buffer: Arc::new(Mutex::new(BTreeMap::new())),
            query: Arc::new(SessionContext::new()),
        }
    }

    /// Write a new request into the database.
    ///
    /// This ensures that data is durable before it becomes queryable within
    /// an in-memory buffer.
    pub fn write(&self, payload: WriteRequest) -> Result<(), Box<dyn std::error::Error>> {
        self.wal.lock().unwrap().write(payload.clone())?;

        let mut buffer_guard = self.buffer.lock().unwrap();
        match buffer_guard.entry(Namespace(payload.namespace)) {
            Entry::Vacant(namespaces) => {
                let mut tables = BTreeMap::new();
                tables.insert(
                    Table(payload.measurement),
                    BTreeMap::from_iter([(
                        PartitionKey::new(payload.timestamp),
                        Measurements {
                            timestamps: vec![payload.timestamp],
                            tags: vec![payload.tags],
                            values: vec![payload.value],
                        },
                    )]),
                );
                namespaces.insert(tables);
            }
            Entry::Occupied(mut namespace_entry) => {
                let tables = namespace_entry.get_mut();

                match tables.entry(Table(payload.measurement)) {
                    Entry::Vacant(table) => {
                        table.insert(BTreeMap::from_iter([(
                            PartitionKey::new(payload.timestamp),
                            Measurements {
                                timestamps: vec![payload.timestamp],
                                tags: vec![payload.tags],
                                values: vec![payload.value],
                            },
                        )]));
                    }
                    Entry::Occupied(mut table_entry) => {
                        let partitions = table_entry.get_mut();

                        match partitions.entry(PartitionKey::new(payload.timestamp)) {
                            Entry::Vacant(init) => {
                                init.insert(Measurements {
                                    timestamps: vec![payload.timestamp],
                                    tags: vec![payload.tags],
                                    values: vec![payload.value],
                                });
                            }
                            Entry::Occupied(mut buffered_measurements) => {
                                let buffered_measurements = buffered_measurements.get_mut();
                                buffered_measurements.timestamps.push(payload.timestamp);
                                buffered_measurements.tags.push(payload.tags);
                                buffered_measurements.values.push(payload.value);
                            }
                        };
                    }
                };
            }
        };
        Ok(())
    }

    pub async fn query(
        &self,
        namespace: String,
        measurement: String,
        sql: String,
    ) -> Result<Option<Vec<RecordBatch>>, Box<dyn std::error::Error>> {
        // Get a snapshot of the current in-memory data that
        // will be queryable.
        let tables = {
            let buffer = self.buffer.lock().unwrap();
            buffer.get(&Namespace(namespace.clone())).cloned()
        };

        match tables {
            Some(tables) => {
                let mut timestamps = Vec::new();
                let mut values = Vec::new();
                // Tags may not have been included within a write.
                // When they are included, it needs to be matched with the
                // write it was with.
                let mut all_tags: Vec<Vec<(String, TagValue)>> = Vec::new();

                if let Some(partitions) = tables.get(&Table(measurement.clone())) {
                    for partition_values in partitions.values() {
                        timestamps.extend_from_slice(&partition_values.timestamps);
                        values.extend_from_slice(&partition_values.values);
                        all_tags.extend_from_slice(&partition_values.tags);
                    }

                    let mut tag_keys = Vec::new();
                    let mut tag_keys_set = HashSet::new();
                    for row_tags in &all_tags {
                        for (tag_key, _) in row_tags {
                            //
                            if tag_keys_set.insert(tag_key.clone()) {
                                tag_keys.push(tag_key.clone());
                            }
                        }
                    }
                    tag_keys.sort();

                    // Build schema
                    let mut fields = vec![
                        Field::new(
                            "timestamp",
                            DataType::Timestamp(TimeUnit::Microsecond, None),
                            false,
                        ),
                        Field::new("value", DataType::Utf8, false),
                    ];

                    for key in &tag_keys {
                        fields.push(Field::new(key.as_str(), DataType::Utf8, true));
                    }

                    let schema = Arc::new(Schema::new(fields));

                    // Build columns
                    let timestamp_array =
                        Arc::new(TimestampMicrosecondArray::from_iter_values(timestamps))
                            as ArrayRef;
                    let value_array = Arc::new(StringArray::from_iter_values(values)) as ArrayRef;

                    // Build one array per tag key
                    let mut tag_columns: Vec<ArrayRef> = Vec::new();
                    for tag_key in &tag_keys {
                        let mut tag_column_values: Vec<Option<String>> = Vec::new();
                        for row_tags in &all_tags {
                            let tag_value = row_tags
                                .iter()
                                .find(|(k, _)| k == tag_key)
                                .map(|(_, v)| v.to_string());
                            tag_column_values.push(tag_value);
                        }
                        tag_columns
                            .push(Arc::new(StringArray::from(tag_column_values)) as ArrayRef);
                    }

                    let mut columns = vec![timestamp_array, value_array];
                    columns.extend(tag_columns);

                    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
                    let memtable = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());

                    // TODO: change to table
                    self.query.register_table(measurement, memtable).unwrap();
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn write_multiple_requests_same_namespace() {
        let dir = TempDir::new().unwrap();
        let lynx = Lynx::new(dir.path(), 1024 * 1024);

        let request1 = WriteRequest {
            namespace: "metrics".to_string(),
            measurement: "cpu".to_string(),
            value: "100".to_string(),
            tags: vec![("host".to_string(), TagValue::String("server1".to_string()))],
            timestamp: 1_700_000_000_000_000, // 2023-11-14
        };

        let request2 = WriteRequest {
            namespace: "metrics".to_string(),
            measurement: "cpu".to_string(),
            value: "200".to_string(),
            tags: vec![("host".to_string(), TagValue::String("server2".to_string()))],
            timestamp: 1_700_000_001_000_000, // Same day
        };

        lynx.write(request1.clone()).unwrap();
        lynx.write(request2).unwrap();

        let buffer = lynx.buffer.lock().unwrap();
        let tables = buffer.get(&Namespace("metrics".to_string())).unwrap();
        let partitions = tables.get(&Table("cpu".to_string())).unwrap();

        // The above requests are part of the same partition, as
        // they use the same timestamp. So it does not matter which
        // one we use.
        let partition_key = PartitionKey::new(request1.timestamp);
        let measurements = partitions.get(&partition_key).unwrap();

        assert_eq!(measurements.timestamps.len(), 2);
        assert_eq!(measurements.values, vec!["100", "200"]);
        assert_eq!(measurements.tags.len(), 2);
    }

    #[test]
    fn write_multiple_namespaces() {
        let dir = TempDir::new().unwrap();
        let lynx = Lynx::new(dir.path(), 1024 * 1024);

        let request1 = WriteRequest {
            namespace: "cpu".to_string(),
            measurement: "usage".to_string(),
            value: "80".to_string(),
            tags: vec![],
            timestamp: 1_700_000_000_000_000,
        };

        let request2 = WriteRequest {
            namespace: "memory".to_string(),
            measurement: "usage".to_string(),
            value: "4096".to_string(),
            tags: vec![],
            timestamp: 1_700_000_000_000_000,
        };

        lynx.write(request1).unwrap();
        lynx.write(request2).unwrap();

        let buffer = lynx.buffer.lock().unwrap();
        assert_eq!(buffer.len(), 2);
        assert!(buffer.contains_key(&Namespace("cpu".to_string())));
        assert!(buffer.contains_key(&Namespace("memory".to_string())));
    }

    #[test]
    fn partition_by_day() {
        let dir = TempDir::new().unwrap();
        let lynx = Lynx::new(dir.path(), 1024 * 1024);

        let request1 = WriteRequest {
            namespace: "events".to_string(),
            measurement: "clicks".to_string(),
            value: "event1".to_string(),
            tags: vec![],
            timestamp: 1_700_000_000_000_000, // 2023-11-14
        };

        let request2 = WriteRequest {
            namespace: "events".to_string(),
            measurement: "clicks".to_string(),
            value: "event2".to_string(),
            tags: vec![],
            timestamp: 1_700_086_400_000_000, // 2023-11-15
        };

        lynx.write(request1.clone()).unwrap();
        lynx.write(request2.clone()).unwrap();

        let buffer = lynx.buffer.lock().unwrap();
        let tables = buffer.get(&Namespace("events".to_string())).unwrap();
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
}
