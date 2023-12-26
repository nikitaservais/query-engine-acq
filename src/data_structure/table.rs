use std::fmt::{Display, Formatter};

use arrow::array::{Array, RecordBatch, StringArray};
use arrow::util::pretty::pretty_format_batches;

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
    pub fn set_data(&mut self, data: RecordBatch) {
        self.data = data;
    }

    pub fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }
    pub fn get_data(&self) -> RecordBatch {
        self.data.clone()
    }

    pub fn get_column(&self, index: &usize) -> Option<&StringArray> {
        self.data
            .column(*index)
            .as_any()
            .downcast_ref::<StringArray>()
    }

    pub fn is_empty(&self) -> bool {
        self.data.num_rows() == 0
    }

    pub fn project(&self, indices: &[usize]) -> Self {
        let data = self.data.project(indices).unwrap();
        let table = self.clone();
        Table {
            name: table.name,
            data,
        }
    }
}
