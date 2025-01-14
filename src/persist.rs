use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf};

use arrow::array::{
    ArrayRef, RecordBatch, StringBuilder, TimestampMicrosecondBuilder, UInt64Builder,
};
use datafusion::execution::context::SessionContext;
use parquet::arrow::ArrowWriter;
use tokio::sync::{
    mpsc::{Receiver, Sender},
    Mutex,
};

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
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let actor = PersistActor::new(max_events, rx, files, persist_path);
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
    persist_path: PathBuf,
}

impl PersistActor {
    pub fn new(
        max_events: i64,
        event_receiver: Receiver<Event>,
        files: Arc<Mutex<HashMap<String, SessionContext>>>,
        persist_path: PathBuf,
    ) -> Self {
        Self {
            files,
            events: HashMap::new(),
            max_events,
            event_receiver,
            persist_path,
        }
    }
}

pub async fn run_persist_actor(mut actor: PersistActor) {
    while let Some(event) = actor.event_receiver.recv().await {
        let in_mem_event = actor
            .events
            .entry(event.namespace.clone())
            .and_modify(|f| f.push(event.clone()))
            .or_insert_with(|| vec![event.clone()]);

        if in_mem_event.len() == actor.max_events as usize {
            println!("Persisting events for {}", event.namespace);
            for (namespace, events) in &actor.events {
                let path = format!("{}/lynx/{namespace}", actor.persist_path.to_string_lossy());
                println!("Persisting to {path}");

                if !std::fs::exists(&path).unwrap() {
                    std::fs::create_dir_all(&path).unwrap();
                }

                let now = chrono::Utc::now().timestamp_micros();
                let filename = format!("lynx-{now}.parquet");
                let file = std::fs::File::create_new(format!("{path}/{filename}")).unwrap();
                let mut names = StringBuilder::new();
                let mut values = UInt64Builder::new();
                // TODO: precision hints
                let mut timestamps = TimestampMicrosecondBuilder::new();

                for event in events {
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

                let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
                writer.write(&batch).unwrap();
                writer.close().unwrap();
                actor
                    .files
                    .lock()
                    .await
                    .entry(event.namespace.clone())
                    .or_insert_with(SessionContext::new);
            }
            actor.events.clear();
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::{collections::HashMap, time::Duration};

    use tempfile::TempDir;
    use tokio::sync::Mutex;

    use crate::event::Event;

    use super::{run_persist_actor, PersistActor};

    #[tokio::test]
    async fn persist() {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let temp_dir = TempDir::new().unwrap();
        let files = Arc::new(Mutex::new(HashMap::new()));
        let actor = PersistActor::new(1, rx, Arc::clone(&files), temp_dir.path().to_path_buf());

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
    }
}
