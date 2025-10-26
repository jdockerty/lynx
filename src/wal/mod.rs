#![allow(dead_code)]

use std::{
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

const WAL_HEADER: &str = "LYNX1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WriteRequest {
    namespace: String,
    value: String,
    tags: Vec<(String, TagValue)>,
    timestamp: u64,
}

impl WriteRequest {
    fn to_bytes(self) -> Vec<u8> {
        let mut data = Vec::with_capacity(1024);

        let namespace_data = self.namespace.as_bytes();
        let namespace_len = namespace_data.len().to_be_bytes();
        data.write_all(&namespace_len).unwrap();
        data.write_all(&namespace_data).unwrap();

        let value_data = self.value.as_bytes();
        let value_len = value_data.len().to_be_bytes();
        data.write_all(&value_len).unwrap();
        data.write_all(&value_data).unwrap();

        let tag_count = self.tags.len().to_be_bytes();
        data.write_all(&tag_count).unwrap();

        for (key, value) in &self.tags {
            let value_type = match value {
                TagValue::String(_) => 0_u8,
                TagValue::Number(_) => 1_u8,
            };
            data.write_all(&[value_type]).unwrap();

            let key_data = key.as_bytes();
            let key_size = key_data.len().to_be_bytes();
            data.write_all(&key_size).unwrap();
            data.write_all(&key_data).unwrap();

            match value {
                TagValue::String(s) => {
                    let value_data = s.as_bytes();
                    let value_size = value_data.len().to_be_bytes();
                    data.write_all(&value_size).unwrap();
                    data.write_all(&value_data).unwrap();
                }
                TagValue::Number(n) => {
                    data.write_all(&n.to_be_bytes()).unwrap();
                }
            }
        }

        data.write_all(&self.timestamp.to_be_bytes()).unwrap();

        data
    }

    fn from_reader(r: &mut impl Read) -> Option<Self> {
        let mut namespace_len = [0u8; 8];
        match r.read_exact(&mut namespace_len) {
            Ok(_) => {}
            Err(e) if matches!(e.kind(), std::io::ErrorKind::UnexpectedEof) => {
                return None;
            }
            Err(e) => panic!("{e}"),
        }
        let namespace_len = usize::from_be_bytes(namespace_len);
        let mut namespace_data = vec![0u8; namespace_len];
        r.read_exact(&mut namespace_data).unwrap();
        let namespace = String::from_utf8(namespace_data).unwrap();

        let mut value_len = [0u8; 8];
        r.read_exact(&mut value_len).unwrap();
        let value_len = usize::from_be_bytes(value_len);
        let mut value_data = vec![0u8; value_len];
        r.read_exact(&mut value_data).unwrap();
        let value = String::from_utf8(value_data).unwrap();

        let mut tag_count = [0u8; 8];
        r.read_exact(&mut tag_count).unwrap();
        let tag_count = usize::from_be_bytes(tag_count);

        let mut tags = Vec::with_capacity(tag_count);
        for _ in 0..tag_count {
            let mut value_type = [0u8; 1];
            r.read_exact(&mut value_type).unwrap();

            let mut key_size = [0u8; 8];
            r.read_exact(&mut key_size).unwrap();
            let key_size = usize::from_be_bytes(key_size);
            let mut key_data = vec![0u8; key_size];
            r.read_exact(&mut key_data).unwrap();
            let key = String::from_utf8(key_data).unwrap();

            let tag_value = match value_type[0] {
                0 => {
                    let mut value_size = [0u8; 8];
                    r.read_exact(&mut value_size).unwrap();
                    let value_size = usize::from_be_bytes(value_size);
                    let mut value_data = vec![0u8; value_size];
                    r.read_exact(&mut value_data).unwrap();
                    TagValue::String(String::from_utf8(value_data).unwrap())
                }
                1 => {
                    let mut num = [0u8; 8];
                    r.read_exact(&mut num).unwrap();
                    TagValue::Number(u64::from_be_bytes(num))
                }
                _ => panic!("Invalid tag value type"),
            };

            tags.push((key, tag_value));
        }

        let mut timestamp = [0u8; 8];
        r.read_exact(&mut timestamp).unwrap();
        let timestamp = u64::from_be_bytes(timestamp);

        Some(WriteRequest {
            namespace,
            value,
            tags,
            timestamp,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
enum TagValue {
    String(String),
    Number(u64),
}

pub struct Wal {
    active_segment: Segment,
    max_segment_size: u64,
    directory: PathBuf,
}

impl Wal {
    pub fn new(directory: impl AsRef<Path>, max_segment_size: u64) -> Self {
        Self {
            active_segment: Segment::new(0, &directory),
            directory: directory.as_ref().to_path_buf(),
            max_segment_size,
        }
    }

    pub fn write(&mut self, data: WriteRequest) -> Result<(), Box<dyn std::error::Error>> {
        if self.active_segment.size > self.max_segment_size {
            self.rotate()?;
        }
        let bytes = data.to_bytes();
        self.active_segment.write(bytes)?;
        Ok(())
    }

    fn rotate(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.active_segment.flush()?;
        let new_segment = Segment::new(self.active_segment.id() + 1, &self.directory);
        self.active_segment = new_segment;
        Ok(())
    }
}

struct Segment {
    id: u64,
    size: u64,
    active_file: File,
}

impl Segment {
    pub fn new(id: u64, directory: impl AsRef<Path>) -> Self {
        let mut file =
            std::fs::File::create_new(directory.as_ref().join(format!("{id}.wal"))).unwrap();

        file.write_all(WAL_HEADER.as_bytes())
            .expect("can write header to new file");
        let size = WAL_HEADER.len() as u64;
        Self {
            active_file: file,
            id,
            size,
        }
    }

    fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(self.active_file.flush()?)
    }

    pub fn write(&mut self, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        self.active_file.write(&data)?;
        self.active_file.flush()?;
        self.size += data.len() as u64;
        Ok(())
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}

#[cfg(test)]
mod test {
    use std::io::{Seek, SeekFrom};

    use tempfile::TempDir;

    use crate::wal::{Segment, TagValue, WAL_HEADER, Wal, WriteRequest};

    #[test]
    fn segment_header() {
        let dir = TempDir::new().unwrap();
        let id = 10;
        let _segment = Segment::new(id, dir.path());
        let contents = std::fs::read(dir.path().join(format!("{id}.wal"))).unwrap();
        assert_eq!(contents, WAL_HEADER.as_bytes());
    }

    #[test]
    fn segment_writes() {
        let dir = TempDir::new().unwrap();
        let mut segment = Segment::new(1, dir.path());
        let data = "hello world";
        segment.write(data.into()).unwrap();
        let contents = std::fs::read(dir.path().join(format!("1.wal"))).unwrap();
        let written = String::from_utf8_lossy(&contents);
        assert_eq!(segment.id, 1);
        assert_eq!(written, format!("{WAL_HEADER}{data}"))
    }

    #[test]
    fn segment_sizing() {
        let dir = TempDir::new().unwrap();
        let mut segment = Segment::new(1, dir.path());
        assert_eq!(
            segment.size,
            WAL_HEADER.len() as u64,
            "Segment size should contain only the header"
        );

        let data = b"hello world";
        segment.write(data.to_vec()).unwrap();
        assert_eq!(segment.size, WAL_HEADER.len() as u64 + data.len() as u64);
    }

    #[test]
    fn wal_writes() {
        let dir = TempDir::new().unwrap();
        let mut wal = Wal::new(dir.path(), 4096);

        let test_write = |i| WriteRequest {
            namespace: "hello".to_string(),
            value: "world".to_string(),
            tags: vec![("temp".to_string(), TagValue::Number(42))],
            timestamp: i,
        };

        for i in 1..=5 {
            wal.write(test_write(i)).unwrap();
        }

        // Seek over the header for reading from the segment file for the purposes
        // of this test.
        wal.active_segment
            .active_file
            .seek(SeekFrom::Start(WAL_HEADER.len() as u64))
            .unwrap();

        for i in 1..=5 {
            let write_from_wal =
                WriteRequest::from_reader(&mut wal.active_segment.active_file).unwrap();
            assert_eq!(write_from_wal, test_write(i));
        }

        assert!(WriteRequest::from_reader(&mut wal.active_segment.active_file).is_none());
    }

    #[test]
    fn wal_rotation() {
        let dir = TempDir::new().unwrap();
        let mut wal = Wal::new(dir.path(), 10);
        assert_eq!(wal.active_segment.id(), 0);

        let write = WriteRequest {
            namespace: "hello".to_string(),
            value: "world".to_string(),
            tags: vec![],
            timestamp: 100,
        };
        wal.write(write.clone()).unwrap();
        assert_eq!(wal.active_segment.id(), 0, "ID is still expected to be 0");
        wal.write(write).unwrap();
        assert_eq!(
            wal.active_segment.id(),
            1,
            "Most recent write should cause rotation to occur"
        );
    }
}
