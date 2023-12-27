use std::fmt::{Display, Formatter};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_schema::{Field, Schema};

use crate::data_structure::query::Query;
use crate::data_structure::query::Term::{Constant, Variable};
use crate::data_structure::table::Table;

#[derive(Clone)]
pub struct Database {
    pub beers: Table,
    pub breweries: Table,
    pub categories: Table,
    pub locations: Table,
    pub styles: Table,
}

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
            let table = self.get_table(&atom.relation_name);
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

    pub fn get_table(&self, name: &str) -> &Table {
        match name {
            "beers" => &self.beers,
            "breweries" => &self.breweries,
            "categories" => &self.categories,
            "locations" => &self.locations,
            "styles" => &self.styles,
            _ => panic!("Table not found"),
        }
    }
}
