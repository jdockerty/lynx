use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf};

use arrow::array::{
    ArrayRef, RecordBatch, StringBuilder, TimestampMicrosecondBuilder, UInt64Builder,
};
use datafusion::execution::context::SessionContext;
use object_store::{ObjectStore, PutPayload};
use parquet::arrow::ArrowWriter;
use tokio::sync::{
    mpsc::{Receiver, Sender},
    Mutex,
};
use tracing::{debug, info};

use crate::event::Event;

#[derive(Debug, Clone)]
pub struct PersistHandle {
    events_queue: Sender<Event>,
}

impl PersistHandle {
    pub fn new(
        files: Arc<Mutex<HashMap<String, SessionContext>>>,
        persist_path: PathBuf,
        max_events: i64,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let actor = PersistActor::new(max_events, rx, files, persist_path, object_store);
        tokio::spawn(run_persist_actor(actor));

        Self { events_queue: tx }
    }

    pub async fn handle_event(&mut self, event: Event) {
        self.events_queue.send(event).await.unwrap();
    }
}

pub struct PersistActor {
    max_events: i64,
    event_receiver: Receiver<Event>,
    events: HashMap<String, Vec<Event>>,
    files: Arc<Mutex<HashMap<String, SessionContext>>>,
    #[allow(dead_code)]
    persist_path: PathBuf,
    object_store: Arc<dyn ObjectStore>,
}

impl PersistActor {
    pub fn new(
        max_events: i64,
        event_receiver: Receiver<Event>,
        files: Arc<Mutex<HashMap<String, SessionContext>>>,
        persist_path: PathBuf,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            files,
            events: HashMap::new(),
            max_events,
            event_receiver,
            persist_path,
            object_store,
        }
    }
}

pub async fn run_persist_actor(mut actor: PersistActor) {
    while let Some(event) = actor.event_receiver.recv().await {
        let events_for_namespace = actor
            .events
            .entry(event.namespace.clone())
            .and_modify(|f| {
                debug!(namespace = event.namespace, "new event");
                f.push(event.clone())
            })
            .or_insert_with(|| {
                info!(namespace = event.namespace, "initialised with event");
                vec![event.clone()]
            });

        if events_for_namespace.len() == actor.max_events as usize {
            info!(namespace = event.namespace, "persisting events");
            let mut names = StringBuilder::new();
            let mut values = UInt64Builder::new();
            // TODO: precision hints
            let mut timestamps = TimestampMicrosecondBuilder::new();
            for event in events_for_namespace.iter() {
                names.append_value(&event.name);
                values.append_value(event.value as u64);
                timestamps.append_value(event.timestamp);
            }

            let names = Arc::new(names.finish()) as ArrayRef;
            let values = Arc::new(values.finish()) as ArrayRef;
            let timestamps = Arc::new(timestamps.finish()) as ArrayRef;

            let batch = RecordBatch::try_from_iter(vec![
                ("timestamp", timestamps),
                ("name", names),
                ("value", values),
            ])
            .unwrap();

            let mut v = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut v, batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();

            let now = chrono::Utc::now().timestamp_micros();
            let filename = format!("lynx-{now}.parquet");
            let namespace = &event.namespace;
            let event_name = &event.name;
            let path =
                object_store::path::Path::from(format!("lynx/{namespace}/{event_name}/{filename}"));
            info!(namespace, table_name = event_name, %path, "persisting");

            let payload = PutPayload::from_bytes(v.into());
            actor.object_store.put(&path, payload).await.unwrap();

            actor
                .files
                .lock()
                .await
                .entry(event.namespace.clone())
                .or_insert_with(SessionContext::new);
            events_for_namespace.clear();
            info!(namespace, "events reset");
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::{collections::HashMap, time::Duration};

    use tempfile::TempDir;
    use tokio::sync::Mutex;
    use tracing::error;

    use crate::event::Event;

    use super::{run_persist_actor, PersistActor};

    #[tokio::test]
    async fn persist() {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let temp_dir = TempDir::new().unwrap();
        let files = Arc::new(Mutex::new(HashMap::new()));
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let actor = PersistActor::new(
            1,
            rx,
            Arc::clone(&files),
            temp_dir.path().to_path_buf(),
            object_store,
        );

        let namespace = "my_org_1".to_string();

        tokio::spawn(async move {
            run_persist_actor(actor).await;
        });

        let event = Event {
            namespace: namespace.clone(),
            name: "heater".to_string(),
            timestamp: 1000,
            value: 10,
            precision: None,
            metadata: serde_json::Value::Null,
        };

        tx.send(event.clone()).await.unwrap();

        if (tokio::time::timeout(Duration::from_secs(2), async move {
            let persist_path = temp_dir.path().join("lynx").join(namespace);
            loop {
                match tokio::fs::try_exists(&persist_path).await {
                    Ok(_) => break,
                    Err(e) => error!("{e}"),
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
        .await)
            .is_err()
        {
            panic!("Persistence did not occur");
        }
    }

    #[tokio::test]
    async fn no_persist_for_different_namespace() {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let temp_dir = Arc::new(TempDir::new().unwrap());
        let files = Arc::new(Mutex::new(HashMap::new()));
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let actor = PersistActor::new(
            2,
            rx,
            Arc::clone(&files),
            temp_dir.path().to_path_buf(),
            object_store,
        );

        let namespace = "my_org_1".to_string();

        tokio::spawn(async move {
            run_persist_actor(actor).await;
        });

        let event = Event {
            namespace: "my_org_2".to_string(),
            name: "heater".to_string(),
            timestamp: 1000,
            value: 10,
            precision: None,
            metadata: serde_json::Value::Null,
        };
        tx.send(event.clone()).await.unwrap();
        assert!(
            !tokio::fs::try_exists(temp_dir.path().join("lynx").join("my_org_2"))
                .await
                .unwrap(),
            "No persistence expected for 'my_org_2', only 1 event was sent when 2 are required"
        );

        let event = Event {
            namespace: namespace.clone(),
            ..event
        };

        tx.send(event.clone()).await.unwrap();
        tx.send(event.clone()).await.unwrap();

        let persisted_dir = Arc::clone(&temp_dir);
        if (tokio::time::timeout(Duration::from_secs(2), async move {
            let persist_path = persisted_dir.path().join("lynx").join(&namespace);
            loop {
                match tokio::fs::try_exists(&persist_path).await {
                    Ok(_) => break,
                    Err(e) => eprintln!("{e}"),
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
        .await)
            .is_err()
        {
            panic!("Persistence did not occur");
        }

        assert!(
            !tokio::fs::try_exists(temp_dir.path().join("lynx").join("my_org_2"))
                .await
                .unwrap(),
            "Persistence for 'my_org_2' still should not occurr"
        );
    }
}
