use std::{
    collections::{BTreeMap, btree_map::Entry},
    path::Path,
    sync::{Arc, Mutex},
};

use crate::wal::{TagValue, Wal, WriteRequest};

/// Time format string for daily partition keys.
///
/// This means that data is partitioned by day at all times currently.
const DAILY_PARTITION: &str = "%Y-%m-%d";

#[derive(Default, Debug)]
pub struct Measurements {
    pub timestamps: Vec<u64>,
    pub tags: Vec<(String, TagValue)>,
    pub values: Vec<String>,
}

#[derive(Ord, Eq, PartialEq, PartialOrd)]
struct PartitionKey(String);

impl PartitionKey {
    pub fn new(timestamp: u64) -> Self {
        let utc_datetime = chrono::DateTime::from_timestamp_micros(timestamp as i64)
            .expect("timestamps are currently assumed to be microseconds");
        Self(utc_datetime.format(DAILY_PARTITION).to_string())
    }
}

/// Lynx, an in-memory time-series database with durable writes.
pub struct Lynx {
    /// Write-ahead log to provide durable writes for incoming data.
    ///
    /// Data MUST be appended to the WAL before making its way into the
    /// in-memory buffer.
    wal: Mutex<Wal>,
    /// In-memory structure which makes the durable writes queryable.
    buffer: Arc<Mutex<BTreeMap<String, BTreeMap<PartitionKey, Measurements>>>>,
}

impl Lynx {
    /// Create a new Lynx instance with the given WAL configuration.
    pub fn new(wal_directory: impl AsRef<Path>, max_segment_size: u64) -> Self {
        Self {
            wal: Mutex::new(Wal::new(wal_directory, max_segment_size)),
            buffer: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Write a new request into the database.
    ///
    /// This ensures that data is durable before it becomes queryable within
    /// an in-memory buffer.
    pub fn write(&self, payload: WriteRequest) -> Result<(), Box<dyn std::error::Error>> {
        self.wal.lock().unwrap().write(payload.clone())?;

        let mut buffer_guard = self.buffer.lock().unwrap();
        match buffer_guard.entry(payload.namespace) {
            Entry::Vacant(vacant) => {
                vacant.insert(BTreeMap::from_iter([(
                    PartitionKey::new(payload.timestamp),
                    Measurements {
                        timestamps: vec![payload.timestamp],
                        tags: payload.tags,
                        values: vec![payload.value],
                    },
                )]));
            }
            Entry::Occupied(mut buffer_entry) => {
                let partitions = buffer_entry.get_mut();

                match partitions.entry(PartitionKey::new(payload.timestamp)) {
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

        Ok(())
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
            value: "100".to_string(),
            tags: vec![("host".to_string(), TagValue::String("server1".to_string()))],
            timestamp: 1_700_000_000_000_000, // 2023-11-14
        };

        let request2 = WriteRequest {
            namespace: "metrics".to_string(),
            value: "200".to_string(),
            tags: vec![("host".to_string(), TagValue::String("server2".to_string()))],
            timestamp: 1_700_000_001_000_000, // Same day
        };

        lynx.write(request1.clone()).unwrap();
        lynx.write(request2).unwrap();

        let buffer = lynx.buffer.lock().unwrap();
        let partitions = buffer.get("metrics").unwrap();

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
            value: "80".to_string(),
            tags: vec![],
            timestamp: 1_700_000_000_000_000,
        };

        let request2 = WriteRequest {
            namespace: "memory".to_string(),
            value: "4096".to_string(),
            tags: vec![],
            timestamp: 1_700_000_000_000_000,
        };

        lynx.write(request1).unwrap();
        lynx.write(request2).unwrap();

        let buffer = lynx.buffer.lock().unwrap();
        assert_eq!(buffer.len(), 2);
        assert!(buffer.contains_key("cpu"));
        assert!(buffer.contains_key("memory"));
    }

    #[test]
    fn partition_by_day() {
        let dir = TempDir::new().unwrap();
        let lynx = Lynx::new(dir.path(), 1024 * 1024);

        let request1 = WriteRequest {
            namespace: "events".to_string(),
            value: "event1".to_string(),
            tags: vec![],
            timestamp: 1_700_000_000_000_000, // 2023-11-14
        };

        let request2 = WriteRequest {
            namespace: "events".to_string(),
            value: "event2".to_string(),
            tags: vec![],
            timestamp: 1_700_086_400_000_000, // 2023-11-15
        };

        lynx.write(request1.clone()).unwrap();
        lynx.write(request2.clone()).unwrap();

        let buffer = lynx.buffer.lock().unwrap();
        let partitions = buffer.get("events").unwrap();

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
