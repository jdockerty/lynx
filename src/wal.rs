#![allow(dead_code)]

use std::{
    collections::HashMap,
    fmt::Display,
    fs::File,
    io::{BufReader, ErrorKind, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::buffer::MemBuffer;

const WAL_HEADER: &str = "LYNX1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct WriteRequest {
    pub namespace: String,
    pub measurement: String,
    pub value: String,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, TagValue>,
    pub timestamp: i64,
}

impl WriteRequest {
    fn into_bytes(self) -> Vec<u8> {
        let mut data = Vec::with_capacity(2048);

        let namespace_data = self.namespace.as_bytes();
        let namespace_len = namespace_data.len().to_be_bytes();
        data.write_all(&namespace_len).unwrap();
        data.write_all(namespace_data).unwrap();

        let measurement_data = self.measurement.as_bytes();
        let measurement_len = measurement_data.len().to_be_bytes();
        data.write_all(&measurement_len).unwrap();
        data.write_all(measurement_data).unwrap();

        let value_data = self.value.as_bytes();
        let value_len = value_data.len().to_be_bytes();
        data.write_all(&value_len).unwrap();
        data.write_all(value_data).unwrap();

        let tag_count = self.metadata.len().to_be_bytes();
        data.write_all(&tag_count).unwrap();

        for (key, value) in &self.metadata {
            let value_type = match value {
                TagValue::String(_) => 0_u8,
                TagValue::Number(_) => 1_u8,
            };
            data.write_all(&[value_type]).unwrap();

            let key_data = key.as_bytes();
            let key_size = key_data.len().to_be_bytes();
            data.write_all(&key_size).unwrap();
            data.write_all(key_data).unwrap();

            match value {
                TagValue::String(s) => {
                    let value_data = s.as_bytes();
                    let value_size = value_data.len().to_be_bytes();
                    data.write_all(&value_size).unwrap();
                    data.write_all(value_data).unwrap();
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
        // If we hit an EOF on the namespace, then we can stop reading.
        //
        // TODO: Elegant way to handle this? This match seems a little clunky
        match r.read_exact(&mut namespace_len) {
            Ok(_) => {
                let namespace_len = usize::from_be_bytes(namespace_len);
                let mut namespace_data = vec![0u8; namespace_len];
                r.read_exact(&mut namespace_data).unwrap();
                let namespace = String::from_utf8(namespace_data).unwrap();

                let mut measurement_len = [0u8; 8];
                r.read_exact(&mut measurement_len).unwrap();
                let measurement_len = usize::from_be_bytes(measurement_len);
                let mut measurement_data = vec![0u8; measurement_len];
                r.read_exact(&mut measurement_data).unwrap();
                let measurement = String::from_utf8(measurement_data).unwrap();

                let mut value_len = [0u8; 8];
                r.read_exact(&mut value_len).unwrap();
                let value_len = usize::from_be_bytes(value_len);
                let mut value_data = vec![0u8; value_len];
                r.read_exact(&mut value_data).unwrap();
                let value = String::from_utf8(value_data).unwrap();

                let mut tag_count = [0u8; 8];
                r.read_exact(&mut tag_count).unwrap();
                let tag_count = usize::from_be_bytes(tag_count);

                let mut metadata = HashMap::with_capacity(tag_count);
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
                    metadata.insert(key, tag_value);
                }

                let mut timestamp = [0u8; 8];
                r.read_exact(&mut timestamp).unwrap();
                let timestamp = i64::from_be_bytes(timestamp);

                Some(WriteRequest {
                    namespace,
                    measurement,
                    value,
                    metadata,
                    timestamp,
                })
            }
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => None,
            Err(e) => panic!("{e:?}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TagValue {
    String(String),
    Number(u64),
}

impl Display for TagValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TagValue::String(s) => write!(f, "{s}"),
            TagValue::Number(n) => write!(f, "{n}"),
        }
    }
}

pub struct Wal {
    active_segment: Segment,
    closed_segments: Vec<u64>,
    max_segment_size: u64,
    directory: PathBuf,
}

impl Wal {
    pub fn new(
        directory: impl AsRef<Path>,
        segment_id: u64,
        max_segment_size: u64,
        closed_segments: Vec<u64>,
    ) -> Self {
        Self {
            active_segment: Segment::new(segment_id, &directory),
            closed_segments,
            directory: directory.as_ref().to_path_buf(),
            max_segment_size,
        }
    }

    pub fn write(&mut self, data: WriteRequest) -> Result<(), Box<dyn std::error::Error>> {
        if self.active_segment.size > self.max_segment_size {
            self.rotate()?;
        }
        let bytes = data.into_bytes();
        self.active_segment.write(bytes)?;
        Ok(())
    }

    fn rotate(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.active_segment.flush()?;
        let new_segment = Segment::new(self.active_segment.id() + 1, &self.directory);
        self.active_segment = new_segment;
        Ok(())
    }

    /// Perform a WAL replay into the [`MemBuffer`], returning the highest
    /// [`Segment`] ID that was observed.
    pub fn replay(
        directory: impl AsRef<Path>,
        buffer: &MemBuffer,
    ) -> Result<(u64, Vec<u64>), Box<dyn std::error::Error>> {
        WalReader::new(directory.as_ref(), buffer).read()
    }
}

struct Segment {
    id: u64,
    size: u64,
    active_file: File,
}

impl Segment {
    pub fn new(id: u64, directory: impl AsRef<Path>) -> Self {
        let mut file = File::create_new(directory.as_ref().join(format!("{id}.wal"))).unwrap();

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
        self.active_file.write_all(&data)?;
        self.active_file.flush()?;
        self.size += data.len() as u64;
        Ok(())
    }

    pub fn id(&self) -> u64 {
        self.id
    }
}

/// Reader implementation for a WAL, passing the
/// replayed values from each [`Segment`] into
/// the [`MemBuffer`].
struct WalReader<'a> {
    directory: PathBuf,
    buffer: &'a MemBuffer,
}

impl<'a> WalReader<'a> {
    pub fn new(directory: impl AsRef<Path>, buffer: &'a MemBuffer) -> Self {
        Self {
            directory: directory.as_ref().to_path_buf(),
            buffer,
        }
    }

    /// Read encoded values into the [`MemBuffer`], returning
    /// the highest segment ID that was observed and a list
    /// of all closed segments.
    pub fn read(&self) -> Result<(u64, Vec<u64>), Box<dyn std::error::Error>> {
        let files = std::fs::read_dir(&self.directory)?;

        let mut highest_segment = 0;

        let mut observed_segment_ids = Vec::new();

        for file in files {
            let file = file?;
            if file.file_type()?.is_dir() {
                continue;
            }

            let mut segment_reader = SegmentReader::new(file.path(), self.buffer)?;
            observed_segment_ids.push(segment_reader.segment_id);
            highest_segment = highest_segment.max(segment_reader.segment_id);
            segment_reader.read()?;
        }

        Ok((highest_segment, observed_segment_ids))
    }
}

/// Reader implementation for a [`Segment`].
struct SegmentReader<'a> {
    segment_path: PathBuf,
    segment_id: u64,
    buffer: &'a MemBuffer,
    reader: BufReader<File>,
}

impl<'a> SegmentReader<'a> {
    fn new(
        segment_path: impl AsRef<Path>,
        buffer: &'a MemBuffer,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let segment_id = segment_path
            .as_ref()
            .file_stem()
            .expect("segment file has name")
            .to_string_lossy()
            .parse::<u64>()?;

        let segment_file = File::open(&segment_path)?;
        let reader = BufReader::new(segment_file);
        Ok(Self {
            segment_path: segment_path.as_ref().to_path_buf(),
            segment_id,
            buffer,
            reader,
        })
    }

    fn read(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.verify()?;

        // `None` is returned when reaching EOF, so we can break at this point.
        while let Some(write) = WriteRequest::from_reader(&mut self.reader) {
            self.buffer.insert(write)?;
        }
        Ok(())
    }

    fn verify(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Always seek to the beginning of the reader,
        // as this is where the header is located.
        self.reader.seek(SeekFrom::Start(0))?;

        let mut buf = [0u8; WAL_HEADER.len()];
        self.reader.read_exact(&mut buf)?;

        let header = String::from_utf8(buf.to_vec())?;

        if &header != WAL_HEADER {
            Err(format!("segment file must contain header ({WAL_HEADER})").into())
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, fs::File, io::Write};

    use tempfile::TempDir;

    use crate::{
        buffer::{MemBuffer, Namespace, PartitionKey, Table},
        wal::{Segment, SegmentReader, WAL_HEADER, Wal, WalReader, WriteRequest},
    };

    #[test]
    fn segment_header() {
        let dir = TempDir::new().unwrap();
        let id = 10;
        let segment = Segment::new(id, dir.path());
        let contents = std::fs::read(dir.path().join(format!("{id}.wal"))).unwrap();
        assert_eq!(contents, WAL_HEADER.as_bytes());

        let buffer = MemBuffer::new();
        let segment_path = dir.path().join(format!("{id}.wal"));
        let mut segment_reader = SegmentReader::new(&segment_path, &buffer).unwrap();
        assert!(segment_reader.verify().is_ok());

        let next_id = segment.id + 1;
        let temp_file_path = dir.path().join(format!("{next_id}.wal"));
        let mut not_lynx_file = File::create_new(&temp_file_path).unwrap();
        not_lynx_file.write_all(b"not_a_lynx_file").unwrap();
        let mut segment_reader = SegmentReader::new(&temp_file_path, &buffer).unwrap();
        assert_eq!(
            segment_reader.verify().unwrap_err().to_string(),
            format!("segment file must contain header ({WAL_HEADER})")
        );
    }

    #[test]
    fn segment_writes() {
        let dir = TempDir::new().unwrap();
        let mut segment = Segment::new(1, dir.path());
        let data = "hello world";
        segment.write(data.into()).unwrap();
        let contents = std::fs::read(dir.path().join("1.wal")).unwrap();
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
    fn wal_rotation() {
        let dir = TempDir::new().unwrap();
        let mut wal = Wal::new(dir.path(), 0, 10, vec![]);
        assert_eq!(wal.active_segment.id(), 0);

        let write = WriteRequest {
            namespace: "hello".to_string(),
            measurement: "test".to_string(),
            value: "world".to_string(),
            metadata: HashMap::new(),
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

    #[test]
    fn from_reader() {
        let dir = TempDir::new().unwrap();
        let mut wal = Wal::new(dir.path(), 0, 10, vec![]);
        let write = WriteRequest {
            namespace: "hello".to_string(),
            measurement: "test".to_string(),
            value: "world".to_string(),
            metadata: HashMap::new(),
            timestamp: 100,
        };
        wal.write(write.clone()).unwrap();

        let segment_path = dir.path().join("0.wal");
        let buffer = MemBuffer::new();
        let mut segment_reader = SegmentReader::new(segment_path, &buffer).unwrap();

        assert!(segment_reader.verify().is_ok());

        let read = WriteRequest::from_reader(&mut segment_reader.reader).unwrap();
        assert_eq!(read, write);
    }

    #[test]
    fn wal_replay() {
        let dir = TempDir::new().unwrap();
        let mut wal = Wal::new(dir.path(), 1, 10, vec![]);

        let write = WriteRequest {
            namespace: "hello".to_string(),
            measurement: "test".to_string(),
            value: "world".to_string(),
            metadata: HashMap::new(),
            timestamp: 100,
        };

        (0..10).for_each(|_| wal.write(write.clone()).unwrap());
        assert_eq!(wal.active_segment.id(), 10);
        drop(wal);

        let buffer = MemBuffer::new();
        let (segment_id, observed_segment_ids) = Wal::replay(dir.path(), &buffer).unwrap();
        let namespace = Namespace("hello".to_string());
        let table = Table("test".to_string());

        assert_eq!(segment_id, 10);
        assert_eq!(observed_segment_ids.len(), 10);
        assert_eq!(buffer.namespace_count(), 1);
        assert_eq!(buffer.table_count(&namespace).unwrap(), 1);
        assert_eq!(buffer.partitions(&namespace, &table).unwrap().len(), 1);
    }

    #[test]
    fn wal_reader() {
        let dir = TempDir::new().unwrap();
        let mut wal = Wal::new(dir.path(), 1, 10, vec![]);

        let write = WriteRequest {
            namespace: "hello".to_string(),
            measurement: "test".to_string(),
            value: "world".to_string(),
            metadata: HashMap::new(),
            timestamp: 100,
        };

        (0..10).for_each(|_| wal.write(write.clone()).unwrap());

        drop(wal);

        let buffer = MemBuffer::new();
        let reader = WalReader::new(dir.path(), &buffer);
        let (max_segment_id, observed_segment_ids) = reader.read().unwrap();
        assert_eq!(max_segment_id, 10);
        assert_eq!(observed_segment_ids.len(), 10);

        assert_eq!(
            buffer
                .tables(&Namespace("hello".to_string()))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            buffer
                .partitions(&Namespace("hello".to_string()), &Table("test".to_string()))
                .unwrap()
                .get(&PartitionKey::new(write.timestamp))
                .unwrap()
                .timestamps
                .len(),
            10,
            "Expected 10 writes to be present from timestamp length"
        );
    }

    #[test]
    fn segment_reader() {
        let dir = TempDir::new().unwrap();
        let mut wal = Wal::new(dir.path(), 1, 10, vec![]);

        let write = WriteRequest {
            namespace: "hello".to_string(),
            measurement: "test".to_string(),
            value: "world".to_string(),
            metadata: HashMap::new(),
            timestamp: 100,
        };

        wal.write(write.clone()).unwrap();

        let buffer = MemBuffer::new();
        let mut reader = SegmentReader::new(
            dir.path().join(format!("{}.wal", wal.active_segment.id())),
            &buffer,
        )
        .unwrap();
        assert_eq!(reader.segment_id, wal.active_segment.id);
        reader.read().unwrap();

        assert_eq!(
            buffer
                .tables(&Namespace("hello".to_string()))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            buffer
                .partitions(&Namespace("hello".to_string()), &Table("test".to_string()))
                .unwrap()
                .get(&PartitionKey::new(write.timestamp))
                .unwrap()
                .timestamps
                .len(),
            1,
        );
    }
}
