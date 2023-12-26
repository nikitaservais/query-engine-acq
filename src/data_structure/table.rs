use arrow::array::{Array, RecordBatch, StringArray};
use arrow::util::pretty::pretty_format_batches;
use std::fmt::{Display, Formatter};

#[derive(Clone)]
pub struct Table {
    pub(crate) name: String,
    pub(crate) data: RecordBatch,
}

impl Display for Table {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            pretty_format_batches(&[self.data.clone()]).unwrap()
        )
    }
}

impl Table {
    pub fn set_name(&mut self, name: &String) {
        self.name = name.clone();
    }
    pub fn get_data(&self) -> &RecordBatch {
        &self.data
    }
    pub(crate) fn get_column(&self, index: &usize) -> Option<&StringArray> {
        self.data
            .column(*index)
            .as_any()
            .downcast_ref::<StringArray>()
    }

    pub fn is_empty(&self) -> bool {
        self.data.num_rows() == 0
    }
}
