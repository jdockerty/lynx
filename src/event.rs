use std::{
    default::Default,
    io::{Read, Write},
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// Namespace where data should be written.
    pub namespace: String,
    /// Name of the event which is being recorded.
    pub name: String,
    /// Timestamp that the event occurred.
    pub timestamp: i64,
    /// Optional precision of the provided timestamp. When this is not provided,
    /// nanosecond precision is assumed.
    pub precision: Option<Precision>,
    /// Value associated with the event.
    pub value: i64,
    /// Arbitrary key-value metadata associated with the event.
    pub metadata: Value,
}

impl Event {
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1024);

        buf.write_u64::<BigEndian>(self.namespace.len() as u64)
            .unwrap();
        buf.write_all(self.namespace.as_bytes()).unwrap();

        buf.write_u64::<BigEndian>(self.name.len() as u64).unwrap();
        buf.write_all(self.name.as_bytes()).unwrap();

        buf.write_i64::<BigEndian>(self.timestamp).unwrap();

        buf.write_u8(self.precision.clone().unwrap_or_default() as u8)
            .unwrap();

        buf.write_i64::<BigEndian>(self.value).unwrap();
        // TODO: write metadata, it is ignored for now.
        buf
    }

    pub fn from_reader(mut buf: impl Read) -> Option<Event> {
        let sz = match buf.read_u64::<BigEndian>() {
            Ok(sz) => sz,
            Err(_) => return None,
        };

        let mut namespace_buf = vec![0; sz as usize];
        buf.read_exact(&mut namespace_buf).unwrap();
        let namespace = String::from_utf8(namespace_buf).unwrap();

        let sz = buf.read_u64::<BigEndian>().unwrap();
        let mut name_buf = vec![0; sz as usize];

        buf.read_exact(&mut name_buf).unwrap();
        let name = String::from_utf8(name_buf).unwrap();

        let timestamp = buf.read_i64::<BigEndian>().unwrap();
        let precision = buf.read_u8().unwrap();
        let value = buf.read_i64::<BigEndian>().unwrap();

        Some(Event {
            namespace,
            name,
            timestamp,
            precision: Some(Precision::try_from(precision).unwrap()),
            value,
            metadata: serde_json::Value::Null,
        })
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
#[serde(rename_all = "lowercase")]
pub enum Precision {
    Nanosecond,
    #[default]
    Microsecond,
}

impl TryFrom<u8> for Precision {
    type Error = Box<dyn ::std::error::Error>;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Nanosecond),
            1 => Ok(Self::Microsecond),
            _ => Err("invalid precision for {value}".into()),
        }
    }
}
