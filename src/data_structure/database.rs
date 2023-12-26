use crate::data_structure::query::Term::{Constant, Variable};
use crate::data_structure::query::{Atom, Query, Term};
use crate::data_structure::table::Table;
use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::RecordBatch;
use arrow::array::StringArray;
use arrow_schema::{Field, Schema};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone)]
pub struct Database {
    pub beers: Table,
    pub breweries: Table,
    pub categories: Table,
    pub locations: Table,
    pub styles: Table,
}

impl Database {}

impl Display for Database {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}\n{}\n{}\n{}\n{}",
            self.beers, self.breweries, self.categories, self.locations, self.styles
        )
    }
}

impl Database {
    pub fn rename(&mut self, query: &Query) {
        for atom in query.body.iter() {
            let table = self.get_table_by_name(&atom.relation_name);
            let mut new_table = table.clone();
            let mut new_field: Vec<Field> = vec![];
            for (index, term) in atom.terms.iter().enumerate() {
                match term {
                    Variable(name) => {
                        let field = table.get_data().schema().field(index).clone();
                        new_field.push(field.clone().with_name(name));
                    }
                    Constant(_) => {
                        new_field.push(table.get_data().schema().field(index).clone());
                    }
                }
            }
            let schema = Schema::new(new_field);
            let new_columns = table.get_data().columns().to_vec();
            new_table.set_data(RecordBatch::try_new(Arc::new(schema), new_columns).unwrap());
            self.set_table(&atom.relation_name, new_table);
        }
    }

    pub fn set_table(&mut self, table_name: &str, table: Table) {
        match table_name {
            "beers" => self.beers.data = table.get_data(),
            "breweries" => self.breweries.data = table.get_data(),
            "categories" => self.categories.data = table.get_data(),
            "locations" => self.locations.data = table.get_data(),
            "styles" => self.styles.data = table.get_data(),
            _ => panic!("Table not found"),
        }
    }

    pub fn get_table_by_name(&self, relation_name: &str) -> &Table {
        match relation_name {
            "beers" => &self.beers,
            "breweries" => &self.breweries,
            "categories" => &self.categories,
            "locations" => &self.locations,
            "styles" => &self.styles,
            _ => panic!("Table not found"),
        }
    }

    pub fn project(&self, attr: &[Term], table: &Table) -> Table {
        let mut indices = vec![];
        for (index, term) in attr.iter().enumerate() {
            match term {
                Variable(name) => {
                    if let Ok(index) = table.get_data().schema().index_of(name) {
                        indices.push(index);
                    }
                }
                Constant(_) => {
                    indices.push(index);
                }
            }
        }
        table.project(&indices)
    }

    pub fn intersection(&self, table: &Table, table_2: &Table) -> Table {
        let column = table.get_column(&0).unwrap();
        let column_2 = table_2.get_column(&0).unwrap();
        let mut filter = BooleanArray::from(vec![false; column.len()]);
        for i in 0..column_2.len() {
            let eq = arrow::compute::kernels::cmp::eq(
                &column,
                &StringArray::new_scalar(column_2.value(i)),
            )
            .unwrap();
            filter = arrow::compute::kernels::boolean::or(&filter, &eq).unwrap();
        }
        let data = arrow::compute::filter_record_batch(&table.get_data(), &filter).unwrap();
        Table {
            name: table.name.clone(),
            data,
        }
    }

    pub fn semi_join(&self, query: &Atom, query_2: &Atom, table: &Table, table_2: &Table) -> Table {
        let join_table = self.join(query, query_2, table, table_2);
        let mut table = self.project(&query.terms, &join_table);
        table.set_name(&query.relation_name);
        table
    }

    pub fn join(
        &self,
        left: &Atom,
        right: &Atom,
        left_table: &Table,
        right_table: &Table,
    ) -> Table {
        let cartesian_product = self.cartesian_product(left_table, right_table);
        let union = self.merge(left, right);
        let mut join_table = self.select(&union, &cartesian_product);
        let union = Atom::union(left, right);
        join_table = self.project(&union, &join_table);
        join_table
    }

    fn merge(&self, left: &Atom, right: &Atom) -> Atom {
        Atom {
            relation_name: format!("{}_{}", left.relation_name, right.relation_name),
            terms: [left.terms.clone(), right.terms.clone()].concat(),
        }
    }

    fn cartesian_product(&self, left: &Table, right: &Table) -> Table {
        let schema = Schema::new(
            [
                left.data
                    .schema()
                    .all_fields()
                    .into_iter()
                    .map(|f| f.clone())
                    .collect::<Vec<_>>(),
                right
                    .data
                    .schema()
                    .all_fields()
                    .into_iter()
                    .map(|f| f.clone())
                    .collect(),
            ]
            .concat(),
        );
        if left.data.num_rows() == 0 || right.data.num_rows() == 0 {
            let d = RecordBatch::new_empty(Arc::new(schema));

            return Table {
                name: format!("{}_{}", left.name, right.name),
                data: d,
            };
        }

        let mut new_left_columns: Vec<ArrayRef> = vec![];
        for column in left.data.columns().clone() {
            let clone = column.clone();
            let v = std::iter::repeat(clone.as_ref())
                .take(right.data.num_rows())
                .collect::<Vec<_>>();
            if v.is_empty() {
                new_left_columns.push(clone.clone());
                continue;
            }
            let new_column = arrow_select::concat::concat(&v).unwrap();

            new_left_columns.push(new_column);
        }
        let mut new_right_columns = vec![];
        for column in right.data.columns().clone() {
            let clone = column.clone();
            let v = std::iter::repeat(clone.as_ref())
                .take(left.data.num_rows())
                .collect::<Vec<_>>();
            if v.is_empty() {
                new_right_columns.push(clone.clone());
                continue;
            }
            let new_column = arrow_select::concat::concat(&v).unwrap();

            new_right_columns.push(new_column);
        }
        let data = RecordBatch::try_new(
            Arc::new(schema),
            [
                new_left_columns
                    .into_iter()
                    .map(|c| c.into())
                    .collect::<Vec<_>>(),
                new_right_columns
                    .into_iter()
                    .map(|c| c.into())
                    .collect::<Vec<_>>(),
            ]
            .concat(),
        )
        .unwrap();
        Table {
            name: format!("{}_{}", left.name, right.name),
            data,
        }
    }

    pub fn select(&self, query: &Atom, table: &Table) -> Table {
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
                    if same_variables.len() == 0 {
                        continue;
                    }
                    for same_var in same_variables {
                        let var_filter = arrow_ord::cmp::eq(
                            &table.get_column(&same_var).unwrap(),
                            &table.get_column(&index).unwrap(),
                        )
                        .unwrap();
                        filter = arrow::compute::and(&filter, &var_filter).unwrap();
                    }
                }
                Constant(constant) => {
                    let column = table.get_column(&index).unwrap();
                    let constant_filter =
                        arrow_ord::cmp::eq(&column, &StringArray::new_scalar(constant)).unwrap();
                    filter = arrow::compute::and(&filter, &constant_filter).unwrap();
                }
            };
        }
        let data = arrow::compute::filter_record_batch(&table.get_data(), &filter).unwrap();
        Table {
            name: table.name.clone(),
            data,
        }
    }
}
