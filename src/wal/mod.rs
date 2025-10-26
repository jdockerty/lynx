#![allow(dead_code)]

use std::{
    fs::File,
    io::Write,
    path::{Path, PathBuf},
};

const WAL_HEADER: &str = "LYNX1";

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

    pub fn write(&mut self, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        if self.active_segment.size > self.max_segment_size {
            self.rotate()?;
        }
        self.active_segment.write(data)?;
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
    use tempfile::TempDir;

    use crate::wal::{Segment, WAL_HEADER, Wal};

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
    fn wal_rotation() {
        let dir = TempDir::new().unwrap();
        let mut wal = Wal::new(dir.path(), 10);
        assert_eq!(wal.active_segment.id(), 0);

        wal.write(b"hello world".to_vec()).unwrap();
        assert_eq!(wal.active_segment.id(), 0, "ID is still expected to be 0");
        wal.write(b"more data".to_vec()).unwrap();
        assert_eq!(
            wal.active_segment.id(),
            1,
            "Most recent write should cause rotation to occur"
        );
    }
}
