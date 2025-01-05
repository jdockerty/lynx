use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, RecordBatch, StringBuilder, TimestampMicrosecondBuilder, UInt64Builder,
};
use parquet::arrow::ArrowWriter;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::event::Event;

#[derive(Debug, Clone)]
pub struct PersistHandle {
    events_queue: Sender<Event>,
}

impl PersistHandle {
    pub fn new(max_events: i64) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let actor = PersistActor::new(max_events, rx);
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
}

impl PersistActor {
    pub fn new(max_events: i64, event_receiver: Receiver<Event>) -> Self {
        Self {
            events: HashMap::new(),
            max_events,
            event_receiver,
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
                let path = format!("/tmp/lynx/{namespace}");

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
            }
            actor.events.clear();
        }
    }
}
