use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow_schema::Schema;
use arrow_select::filter::filter_record_batch;

use crate::data_structure::query::Atom;
use crate::data_structure::query::Term::{Constant, Variable};
use crate::data_structure::table::Table;

pub fn semi_join(query: &Atom, query_2: &Atom, table: &Table, table_2: &Table) -> Table {
    let join_table = join(query, query_2, table, table_2);
    let mut table = join_table.project(&query.terms);
    table.set_name(&query.relation_name);
    table
}

pub fn join(left: &Atom, right: &Atom, left_table: &Table, right_table: &Table) -> Table {
    let cartesian_product = cartesian_product(left_table, right_table);
    let mut join_table = select(&Atom::merge(left, right), &cartesian_product);
    join_table = join_table.project(&Atom::union(left, right));
    join_table
}

pub fn select(query: &Atom, table: &Table) -> Table {
    let mut filter = BooleanArray::from(vec![true; table.get_data().num_rows()]);
    for (index, term) in query.terms.iter().enumerate() {
        match term {
            Variable(name) => {
                let same_variables = query
                    .terms
                    .iter()
                    .enumerate()
                    .filter(|(i, t)| {
                        if let Variable(other_name) = t {
                            name == other_name && index < *i
                        } else {
                            false
                        }
                    })
                    .map(|(i, _)| i)
                    .collect::<Vec<_>>();
                if same_variables.is_empty() {
                    continue;
                }
                for same_var in same_variables {
                    let var_filter = arrow_ord::cmp::eq(
                        &table.get_column(same_var).unwrap(),
                        &table.get_column(index).unwrap(),
                    )
                    .unwrap();
                    filter = arrow::compute::and(&filter, &var_filter).unwrap();
                }
            }
            Constant(constant) => {
                let column = table.get_column(index).unwrap();
                let constant_filter =
                    arrow_ord::cmp::eq(&column, &StringArray::new_scalar(constant)).unwrap();
                filter = arrow::compute::and(&filter, &constant_filter).unwrap();
            }
        };
    }
    let data = filter_record_batch(&table.get_data(), &filter).unwrap();
    Table {
        name: table.name.clone(),
        data,
    }
}

fn cartesian_product(left: &Table, right: &Table) -> Table {
    let schema = Schema::new(
        [
            left.data
                .schema()
                .all_fields()
                .into_iter()
                .cloned()
                .collect::<Vec<_>>(),
            right
                .data
                .schema()
                .all_fields()
                .into_iter()
                .cloned()
                .collect(),
        ]
        .concat(),
    );
    if left.is_empty() || right.is_empty() {
        let d = RecordBatch::new_empty(Arc::new(schema));

        return Table {
            name: format!("{}_{}", left.name, right.name),
            data: d,
        };
    }

    let mut new_left_columns: Vec<ArrayRef> = vec![];
    for column in left.data.columns() {
        let new_column = array_multiply(column, right.data.num_rows());

        new_left_columns.push(new_column);
    }
    let mut new_right_columns = vec![];
    for column in right.data.columns() {
        let new_column = array_multiply(column, left.data.num_rows());

        new_right_columns.push(new_column);
    }
    let data = RecordBatch::try_new(
        Arc::new(schema),
        [
            new_left_columns.into_iter().collect::<Vec<_>>(),
            new_right_columns.into_iter().collect::<Vec<_>>(),
        ]
        .concat(),
    )
    .unwrap();
    Table {
        name: format!("{}_{}", left.name, right.name),
        data,
    }
}

fn array_multiply(array: &ArrayRef, times: usize) -> ArrayRef {
    arrow_select::concat::concat(
        &std::iter::repeat(array.as_ref())
            .take(times)
            .collect::<Vec<_>>(),
    )
    .unwrap()
}
