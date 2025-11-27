use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex, TryLockError},
    time::Duration,
};

use tokio::{
    sync::mpsc::{Receiver, Sender, channel},
    task::JoinHandle,
};

use crate::{Namespace, Table, lynx::TableMetadata};

async fn run_retention_actor(mut actor: RetentionActor, refresh_period: Duration) {
    let mut interval = tokio::time::interval(refresh_period);
    loop {
        interval.tick().await;
        match actor.actor_rx.recv().await {
            Some(_) => {
                match actor.metadata.try_lock() {
                    Ok(mut namespaces) => for (namespace, tables) in namespaces.iter() {},
                    Err(TryLockError::WouldBlock) => {
                        // Lock is already held.
                        //
                        // We can attempt to enforce retention at the next interval.
                        continue;
                    }
                    Err(_) => todo!(),
                }
            }
            None => {
                eprintln!("stopping retention enforcement");
                return;
            }
        }
    }
}

pub(crate) struct RetentionActor {
    wal_directory: PathBuf,
    metadata: Arc<Mutex<BTreeMap<Namespace, BTreeMap<Table, TableMetadata>>>>,
    actor_rx: Receiver<()>,
}

impl RetentionActor {
    pub(crate) fn new(
        wal_directory: PathBuf,
        metadata: Arc<Mutex<BTreeMap<Namespace, BTreeMap<Table, TableMetadata>>>>,
        actor_rx: Receiver<()>,
    ) -> Self {
        Self {
            wal_directory,
            metadata,
            actor_rx,
        }
    }
}

pub(crate) struct RetentionHandle {
    handle: JoinHandle<()>,
    actor_tx: Sender<()>,
}

impl RetentionHandle {
    pub(crate) fn new(
        wal_directory: PathBuf,
        metadata: Arc<Mutex<BTreeMap<Namespace, BTreeMap<Table, TableMetadata>>>>,
        refresh_period: Duration,
    ) -> Self {
        let (actor_tx, actor_rx) = channel(10);
        let actor = RetentionActor::new(wal_directory, metadata, actor_rx);

        let handle = tokio::spawn(run_retention_actor(actor, refresh_period));
        Self { handle, actor_tx }
    }
}
