pub mod event;
pub mod persist;
pub mod query;
pub mod server;
pub(crate) mod wal;


pub const LYNX_FORMAT_HEADER: &str = "X-Lynx-Format";
