use std::{
    collections::BTreeMap,
    fs::File,
    io::BufReader,
    path::PathBuf,
    rc::Rc,
    sync::{Arc, Mutex, TryLockError},
    time::{Duration, Instant},
};

use chrono::TimeDelta;
use datafusion::logical_expr_common::dyn_eq::DynHash;
use tokio::{
    sync::mpsc::{Receiver, Sender, channel},
    task::JoinHandle,
};

use crate::{Namespace, Table, lynx::TableMetadata, wal::WriteRequest};

async fn run_compaction_actor(mut actor: CompactionActor, refresh_period: Duration) {
    let mut interval = tokio::time::interval(refresh_period);
    loop {
        interval.tick().await;

        let now = chrono::Utc::now();

        match actor.actor_rx.recv().await {
            Some(_) => {
                let ids: Vec<u64> = vec![];
                for id in ids {
                    let segment_path = actor.wal_directory.join(format!("{id}.wal"));

                    // TODO: update `SegmentReader` to be more robust so it can
                    // be used here.
                    let segment_file = File::open(&segment_path).unwrap();
                    let mut reader = BufReader::new(segment_file);

                    while let Some(write) = WriteRequest::from_reader(&mut reader) {
                        let guard = actor.metadata.lock().unwrap();
                        let table_metadata = guard
                            .get(&Namespace(write.namespace))
                            .expect("namespace exists for a write")
                            .get(&Table(write.measurement))
                            .expect("table metadata exists for write");

                        if let Some(retention) = &table_metadata.retention {
                            let write_time =
                                chrono::DateTime::from_timestamp_micros(write.timestamp)
                                    .expect("writes are valid timestamps");

                            let delta = now - write_time;
                            if delta.num_microseconds().unwrap() > retention.max_age as i64 {
                                // TODO: write to another segment
                            }
                        }
                    }
                }
            }
            None => {
                eprintln!("compaction actor stopping");
                return;
            }
        }
    }
}

/// Compaction for closed segments based on retention
/// for tables within the segments.
struct CompactionActor {
    wal_directory: PathBuf,
    metadata: Arc<Mutex<BTreeMap<Namespace, BTreeMap<Table, TableMetadata>>>>,
    actor_rx: Receiver<()>,
}

pub(crate) struct CompactionHandle {
    handle: JoinHandle<()>,
    actor_tx: Sender<()>,
}

impl CompactionHandle {
    pub(crate) fn new(
        wal_directory: PathBuf,
        metadata: Arc<Mutex<BTreeMap<Namespace, BTreeMap<Table, TableMetadata>>>>,
        refresh_period: Duration,
    ) -> Self {
        let (actor_tx, actor_rx) = channel(10);
        let handle = tokio::spawn(run_compaction_actor(
            CompactionActor {
                actor_rx,
                metadata,
                wal_directory,
            },
            refresh_period,
        ));
        Self { handle, actor_tx }
    }

    pub fn compact_closed_segments(&self, closed_segments: Vec<u64>) {}
}
