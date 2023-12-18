use serde::Serialize;

#[derive(Serialize)]
pub struct Beers {
    beer_id: i32,
    brew_id: i32,
    beer: String,
    abv: f64,
    ibu: String,
    ounces: f64,
    style: String,
    style2: String,
}

#[derive(Serialize)]
pub struct Styles {
    style_id: i32,
    cat_id: i32,
    style: String,
}

#[derive(Serialize)]
pub struct Categories {
    cat_id: i32,
    cat_name: String,
}

#[derive(Serialize)]
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