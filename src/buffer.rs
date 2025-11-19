use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::{Arc, Mutex},
};

use crate::{lynx::Measurements, wal::WriteRequest};

/// Time format string for daily partition keys.
///
/// This means that data is partitioned by day at all times currently.
const DAILY_PARTITION: &str = "%Y-%m-%d";

/// A key used to identify an individual partition within the in-memory
/// buffer.
///
/// # Note
/// Partition keys are ALWAYS constructed from daily timestamps, such that
/// they are always in the format of [`DAILY_PARTITION`].
#[derive(Debug, Ord, Eq, PartialEq, PartialOrd, Clone)]
pub(crate) struct PartitionKey(String);

impl PartitionKey {
    pub fn new(timestamp: i64) -> Self {
        let utc_datetime = chrono::DateTime::from_timestamp_micros(timestamp)
            .expect("timestamps are currently assumed to be microseconds");
        Self(utc_datetime.format(DAILY_PARTITION).to_string())
    }
}

/// New-type wrapper for a namespace.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Namespace(pub(crate) String);

/// New-type wrapper for a table.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Table(pub(crate) String);

pub(crate) struct MemBuffer {
    // Allow this lint as it is wrapped internally in the [`MemBuffer`] type.
    #[expect(clippy::type_complexity)]
    inner: Arc<Mutex<BTreeMap<Namespace, BTreeMap<Table, BTreeMap<PartitionKey, Measurements>>>>>,
}

impl MemBuffer {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn insert(&self, payload: WriteRequest) -> Result<(), Box<dyn std::error::Error>> {
        let mut buffer_guard = self.inner.lock().unwrap();
        match buffer_guard.entry(Namespace(payload.namespace)) {
            Entry::Vacant(namespaces) => {
                let mut tables = BTreeMap::new();
                tables.insert(
                    Table(payload.measurement.clone()),
                    BTreeMap::from_iter([(
                        PartitionKey::new(payload.timestamp),
                        Measurements {
                            timestamps: vec![payload.timestamp],
                            metadata: vec![payload.metadata],
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
                                metadata: vec![payload.metadata],
                                values: vec![payload.value],
                            },
                        )]));
                    }
                    Entry::Occupied(mut table_entry) => {
                        let partitions = table_entry.get_mut();

                        let partition_key = PartitionKey::new(payload.timestamp);
                        match partitions.entry(partition_key) {
                            Entry::Vacant(init) => {
                                init.insert(Measurements {
                                    timestamps: vec![payload.timestamp],
                                    metadata: vec![payload.metadata],
                                    values: vec![payload.value],
                                });
                            }
                            Entry::Occupied(mut buffered_measurements) => {
                                let buffered_measurements = buffered_measurements.get_mut();
                                buffered_measurements.timestamps.push(payload.timestamp);
                                buffered_measurements.metadata.push(payload.metadata);
                                buffered_measurements.values.push(payload.value);
                            }
                        };
                    }
                };
            }
        };
        Ok(())
    }

    /// Return the tables of a [`Namespace`], if any, otherwise [`None`].
    pub fn tables(
        &self,
        namespace: &Namespace,
    ) -> Option<BTreeMap<Table, BTreeMap<PartitionKey, Measurements>>> {
        self.inner.lock().unwrap().get(&namespace).cloned()
    }

    /// Return the partitions of a [`Table`] within a [`Namespace`],
    /// if any, otherwise [`None`].
    pub fn partitions(
        &self,
        namespace: &Namespace,
        table: &Table,
    ) -> Option<BTreeMap<PartitionKey, Measurements>> {
        self.tables(namespace)?.get(table).cloned()
    }

    /// Return the total number of namespaces currently held.
    #[allow(dead_code)]
    pub fn namespace_count(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    /// Return the number of tables within a given [`Namespace`], otherwise [`None`].
    #[allow(dead_code)]
    pub fn table_count(&self, namespace: &Namespace) -> Option<usize> {
        Some(self.inner.lock().unwrap().get(namespace)?.len())
    }

    /// Check whether a [`Namespace`] is contained within the buffer.
    #[allow(dead_code)]
    pub fn contains_namespace(&self, namespace: &Namespace) -> bool {
        self.inner.lock().unwrap().contains_key(namespace)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::{
        buffer::{MemBuffer, Table},
        wal::WriteRequest,
    };

    #[test]
    fn buffer_ops() {
        let buffer = MemBuffer::new();

        for write in [
            WriteRequest {
                namespace: "hello".to_string(),
                measurement: "world".to_string(),
                value: "some_value".to_string(),
                metadata: HashMap::new(),
                timestamp: 1000,
            },
            WriteRequest {
                namespace: "hello".to_string(),
                measurement: "world".to_string(),
                value: "some_value".to_string(),
                metadata: HashMap::new(),
                timestamp: 300000000000,
            },
            WriteRequest {
                namespace: "another".to_string(),
                measurement: "world".to_string(),
                value: "some_value".to_string(),
                metadata: HashMap::new(),
                timestamp: 1,
            },
        ] {
            buffer.insert(write).unwrap();
        }

        assert_eq!(buffer.namespace_count(), 2);
        assert_eq!(
            buffer
                .table_count(&crate::buffer::Namespace("hello".to_string()))
                .unwrap(),
            1
        );
        assert_eq!(
            buffer
                .table_count(&crate::buffer::Namespace("another".to_string()))
                .unwrap(),
            1
        );

        let hello_tables = buffer
            .tables(&crate::buffer::Namespace("hello".to_string()))
            .unwrap();
        let hello_partitions = hello_tables.get(&Table("world".to_string())).unwrap();
        assert_eq!(hello_tables.len(), 1);
        assert_eq!(hello_partitions.len(), 2);
        let another_tables = buffer
            .tables(&crate::buffer::Namespace("another".to_string()))
            .unwrap();
        let another_partitions = another_tables.get(&Table("world".to_string())).unwrap();
        assert_eq!(another_tables.len(), 1);
        assert_eq!(another_partitions.len(), 1);
    }
}
