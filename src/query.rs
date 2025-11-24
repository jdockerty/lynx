use datafusion::arrow::{
    array::RecordBatch, json::ArrayWriter, util::pretty::pretty_format_batches,
};

/// An adapter which takes input [`RecordBatch`]es and
/// converts them into a specified response.
///
/// Only `table` and `json` are expected currently, as
/// defined in [`OutputFormat`].
pub(crate) struct QueryResponseAdapter {
    batches: Vec<RecordBatch>,
}

impl QueryResponseAdapter {
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self { batches }
    }

    /// Convert the input [`RecordBatch`]es into a JSON array.
    pub fn into_json(self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let buf = Vec::new();
        let mut writer = ArrayWriter::new(buf);
        for batch in &self.batches {
            writer.write(batch)?;
        }
        writer.finish()?;
        Ok(writer.into_inner())
    }

    /// Convert the input [`RecordBatch`]es into a pretty-formatted
    /// table.
    pub fn into_table(self) -> Result<String, Box<dyn std::error::Error>> {
        Ok(pretty_format_batches(&self.batches)?.to_string())
    }
}
