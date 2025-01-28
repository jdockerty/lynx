#![expect(dead_code)]

use std::io::Write;
use std::path::PathBuf;
use std::{fs::File, io::Seek};

use tokio::sync::mpsc::{Receiver, Sender};

use crate::event::Event;

const LYNX_WAL_HEADER: &str = "LYNX\n";

/// A Write-Ahead Log (WAL) implementation.
pub struct Wal {
    buffer: Vec<u8>,
    buffer_size: usize,
    dir: PathBuf,
    handle: File,
    id: u64,
    offset: usize,
}

impl Wal {
    pub fn new(dir: PathBuf, id: u64, buffer_size: Option<usize>) -> Self {
        let wal_path = dir.clone().join(format!("{id}.wal"));
        let (new, mut file_handle) = match File::create_new(&wal_path) {
            Ok(handle) => (true, handle),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (
                false,
                std::fs::OpenOptions::new()
                    .read(true)
                    .append(true)
                    .open(&wal_path)
                    .unwrap(),
            ),
            Err(e) => panic!("{e}"),
        };

        if new {
            file_handle.write_all(LYNX_WAL_HEADER.as_bytes()).unwrap();
        }

        let buffer_size = buffer_size.unwrap_or(8096);
        Self {
            id,
            dir,
            handle: file_handle,
            buffer_size,
            buffer: Vec::with_capacity(buffer_size),
            offset: 0,
        }
    }

    pub(crate) fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.handle.write_all(&self.buffer)?;
        self.handle.sync_all()?;
        self.offset += self.buffer.len();
        self.buffer.clear();
        Ok(())
    }

    pub fn append(&mut self, event: &Event) -> Result<usize, Box<dyn std::error::Error>> {
        let data = event.as_bytes();
        self.buffer.write_all(&data).unwrap();
        if self.buffer.len() >= self.buffer_size {
            self.flush()?;
        }

        Ok(data.len())
    }

    fn read(&mut self) -> Option<Event> {
        Event::from_reader(&mut self.handle)
    }

    pub fn replay(&mut self) -> Result<Vec<Event>, Box<dyn std::error::Error>> {
        self.handle
            .seek(std::io::SeekFrom::Start(LYNX_WAL_HEADER.len() as u64))
            .unwrap();

        let mut events = Vec::new();
        while let Some(event) = self.read() {
            events.push(event);
        }

        Ok(events)
    }
}

#[derive(Debug, Clone)]
pub struct WalHandle {
    events_queue: Sender<Event>,
}

impl WalHandle {
    pub fn new(wal_dir: PathBuf, id: u64, buffer_size: usize) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let actor = WalActor::new(id, wal_dir, buffer_size, rx);
        tokio::spawn(run_wal_actor(actor));

        Self { events_queue: tx }
    }

    /// Append events to the WAL.
    pub async fn append(&mut self, event: Event) {
        self.events_queue.send(event).await.unwrap();
    }
}

pub struct WalActor {
    buffer_size: usize,
    dir: PathBuf,
    event_receiver: Receiver<Event>,
    id: u64,
}

impl WalActor {
    pub fn new(
        id: u64,
        wal_dir: PathBuf,
        buffer_size: usize,
        event_receiver: Receiver<Event>,
    ) -> Self {
        Self {
            id,
            buffer_size,
            dir: wal_dir,
            event_receiver,
        }
    }
}

pub async fn run_wal_actor(mut actor: WalActor) {
    let mut wal = Wal::new(actor.dir, actor.id, Some(actor.buffer_size));

    while let Some(event) = actor.event_receiver.recv().await {
        wal.append(&event).unwrap();
    }
}

#[cfg(test)]
mod test {
    use tempfile::TempDir;

    use crate::{
        event::{Event, Precision},
        wal::LYNX_WAL_HEADER,
    };

    use super::Wal;

    #[test]
    fn write_to_wal() {
        let dir = TempDir::new().expect("Can create temp dir for test");

        let event = Event {
            namespace: "my_ns".to_string(),
            name: "my_event".to_string(),
            timestamp: 10000,
            precision: None,
            value: 10,
            metadata: serde_json::Value::Null,
        };
        let mut wal = Wal::new(dir.path().to_path_buf(), 0, None);
        let result = wal.append(&event).unwrap();

        let expected_size = event.as_bytes().len();
        assert_eq!(result, expected_size);
        assert_eq!(wal.buffer.len(), expected_size);
        assert_eq!(
            std::fs::File::open(dir.path().join("0.wal"))
                .unwrap()
                .metadata()
                .unwrap()
                .len(),
            LYNX_WAL_HEADER.len() as u64,
            "WAL was not flushed, only header should exist"
        );
    }

    #[test]
    fn replay_wal() {
        let dir = TempDir::new().unwrap();

        let mut wal = Wal::new(dir.path().to_path_buf(), 0, None);
        let event = Event {
            namespace: "my_ns".to_string(),
            name: "my_event".to_string(),
            timestamp: 10000,
            precision: None,
            value: 10,
            metadata: serde_json::Value::Null,
        };
        wal.append(&event).unwrap();
        wal.flush().unwrap();

        drop(wal);

        let mut wal = Wal::new(dir.path().to_path_buf(), 0, None);
        let events = wal.replay().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0],
            Event {
                precision: Some(Precision::Microsecond), // Default is appended when writing to WAL, rather than None
                ..event
            }
        );
    }
}
