use arrow::datatypes::{Field, Schema};

pub struct Beers {
    table_name: String,

    beer_id: Field,
    brew_id: Field,
    beer: Field,
    abv: Field,
    ibu: Field,
    ounces: Field,
    style: Field,
    style2: Field,

    schema: Schema,
}

impl Beers {
    fn table_name(&self) -> String {
        "beers".to_string()
    }

    fn foreign_key(&self) -> String {
        "brew_id".to_string()
    }
}

pub struct Styles {
    style_id: i32,
    cat_id: i32,
    style: String,
}

pub struct Categories {
    cat_id: i32,
    cat_name: String,
}

pub struct Breweries {
    brew_id: i32,
    brew_name: String,
    address1: String,
    address2: String,
    city: String,
    state: String,
    code: String,
    country: String,
    phone: String,
    website: String,
    description: String,
}
