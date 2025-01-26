use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use byteorder::WriteBytesExt;

use crate::event::{Event, Precision};

const LYNX_WAL_HEADER: &str = "LYNX\n";

/// A Write-Ahead Log (WAL) implementation.
pub struct Wal {
    id: u64,
    dir: PathBuf,
    buffer: Vec<u8>,
    handle: File,
    buffer_size: usize,
}

impl Wal {
    pub fn new(dir: PathBuf, buffer_size: Option<usize>) -> Self {
        let id = 0;
        let wal_path = dir.clone().join(format!("{}.wal", id.to_string()));
        let mut file_handle = File::create_new(&wal_path).unwrap();
        file_handle.write_all(LYNX_WAL_HEADER.as_bytes()).unwrap();
        let buffer_size = buffer_size.unwrap_or(8096);
        Self {
            id,
            dir,
            handle: file_handle,
            buffer_size,
            buffer: Vec::with_capacity(buffer_size),
        }
    }

    pub fn append(&mut self, event: &Event) -> Result<usize, Box<dyn std::error::Error>> {
        let mut write_size = 0;
        let namespace = event.namespace.as_bytes();
        let name = event.name.as_bytes();
        let timestamp = &event.timestamp.to_be_bytes();
        let precision = event.precision.clone().unwrap_or_default() as u8;
        let value = &event.value.to_be_bytes();

        self.buffer.write(namespace).unwrap();
        write_size += namespace.len();
        self.buffer.write(name).unwrap();
        write_size += name.len();
        self.buffer.write(timestamp).unwrap();
        write_size += timestamp.len();
        self.buffer.write_u8(precision).unwrap();
        write_size += std::mem::size_of::<Precision>();
        self.buffer.write(value).unwrap();
        write_size += value.len();
        // TODO: metadata is ignored for now.
        // self.buffer.write(&event.metadata.to_string().as_bytes()).unwrap();

        if self.buffer.len() >= self.buffer_size {
            self.handle.write_all(&self.buffer).unwrap();
            self.handle.sync_all().unwrap();
            self.buffer.clear();
        }

        Ok(write_size)
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
        let mut wal = Wal::new(dir.path().to_path_buf(), None);

        let expected_size = &event.namespace.as_bytes().len()
            + &event.name.as_bytes().len()
            + &event.timestamp.to_be_bytes().len()
            + Precision::Microsecond as usize
            + &event.value.to_be_bytes().len();

        let result = wal.append(&event).unwrap();
        assert_eq!(result, expected_size);
        assert_eq!(wal.buffer.len(), expected_size);
        assert_eq!(
            std::fs::File::open(dir.path().join("0.wal"))
                .unwrap()
                .metadata()
                .unwrap()
                .len(),
            LYNX_WAL_HEADER.as_bytes().len() as u64,
            "WAL was not flushed, only header should exist"
        );
    }

    #[test]
    fn flush_buffer() {
        let dir = TempDir::new().expect("Can create temp dir for test");

        let event = Event {
            namespace: "my_ns".to_string(),
            name: "my_event".to_string(),
            timestamp: 10000,
            precision: None,
            value: 10,
            metadata: serde_json::Value::Null,
        };
        let mut wal = Wal::new(dir.path().to_path_buf(), Some(10));

        let expected_size = &event.namespace.as_bytes().len()
            + &event.name.as_bytes().len()
            + &event.timestamp.to_be_bytes().len()
            + Precision::Microsecond as usize
            + &event.value.to_be_bytes().len();

        let result = wal.append(&event).unwrap();
        assert_eq!(result, expected_size);
        assert_eq!(wal.buffer.len(), 0, "Expected WAL buffer flush");
        assert_eq!(
            std::fs::File::open(dir.path().join("0.wal"))
                .unwrap()
                .metadata()
                .unwrap()
                .len(),
            35,
            "WAL was flushed, file size is expected to be header + the write"
        );
    }
}
