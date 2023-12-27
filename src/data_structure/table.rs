use std::fmt::{Display, Formatter};

use crate::data_structure::query::Term;
use crate::data_structure::query::Term::{Constant, Variable};
use arrow::array::{Array, BooleanArray, RecordBatch, StringArray};
use arrow::util::pretty::pretty_format_batches;
use arrow_ord::cmp::distinct;
use arrow_select::filter::filter_record_batch;

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

    pub fn projection(&self, indices: &[usize]) -> Self {
        let data = self.data.project(indices).unwrap();
        let table = self.clone();
        Table {
            name: table.name,
            data,
        }
    }

    pub fn project(&self, attr: &[Term]) -> Self {
        let mut indices = vec![];
        for (index, term) in attr.iter().enumerate() {
            match term {
                Variable(name) => {
                    if let Ok(index) = self.data.schema().index_of(name) {
                        indices.push(index);
                    }
                }
                Constant(_) => {
                    indices.push(index);
                }
            }
        }
        self.projection(&indices)
    }

    pub fn intersection(&self, table: &Table) -> Table {
        let column = self.get_column(&0).unwrap();
        let column_2 = table.get_column(&0).unwrap();
        let mut filter = BooleanArray::from(vec![false; column.len()]);
        for i in 0..column_2.len() {
            let eq = arrow::compute::kernels::cmp::eq(
                &column,
                &StringArray::new_scalar(column_2.value(i)),
            )
            .unwrap();
            filter = arrow::compute::kernels::boolean::or(&filter, &eq).unwrap();
        }
        let data = filter_record_batch(&self.get_data(), &filter).unwrap();
        Table {
            name: self.name.clone(),
            data,
        }
    }

    pub fn filter_unique(&self) -> Self {
        let column = self.get_column(&0).unwrap();
        let filter = distinct(column, column).unwrap();
        let data = filter_record_batch(&self.get_data(), &filter).unwrap();
        Table {
            name: self.name.clone(),
            data,
        }
    }
}
