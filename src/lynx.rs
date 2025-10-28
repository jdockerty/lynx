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

/// Lynx, an in-memory time-series database with durable writes.
pub struct Lynx {
    /// Write-ahead log to provide durable writes for incoming data.
    ///
    /// Data MUST be appended to the WAL before making its way into the
    /// in-memory buffer.
    wal: Mutex<Wal>,
    /// In-memory structure which makes the durable writes queryable.
    buffer: Arc<Mutex<BTreeMap<String, BTreeMap<String, Measurements>>>>,
}

impl Lynx {
    /// Create a new Lynx instance with the given WAL configuration.
    pub fn new(wal_directory: impl AsRef<Path>, max_segment_size: u64) -> Self {
        Self {
            wal: Mutex::new(Wal::new(wal_directory, max_segment_size)),
            buffer: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn parse_partition_key(timestamp: i64) -> String {
        let utc_datetime = chrono::DateTime::from_timestamp_micros(timestamp)
            .expect("timestamps are currently assumed to be microseconds");
        utc_datetime.format(DAILY_PARTITION).to_string()
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
                let partition_key = Self::parse_partition_key(payload.timestamp as i64);
                vacant.insert(BTreeMap::from_iter([(
                    partition_key,
                    Measurements {
                        timestamps: vec![payload.timestamp],
                        tags: payload.tags,
                        values: vec![payload.value],
                    },
                )]));
            }
            Entry::Occupied(mut buffer_entry) => {
                let partitions = buffer_entry.get_mut();

                let partition_key = Self::parse_partition_key(payload.timestamp as i64);
                match partitions.entry(partition_key) {
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
