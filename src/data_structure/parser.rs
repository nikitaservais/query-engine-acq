use crate::data_structure::query::{Atom, Query, Term};
use nom::branch::alt;
use nom::bytes::complete::{tag, take_while, take_while1};
use nom::combinator::map;
use nom::multi::separated_list0;
use nom::sequence::{delimited, tuple};
use nom::IResult;

fn parse_variable(input: &str) -> IResult<&str, Term> {
    map(take_while1(|c: char| c.is_alphanumeric()), |s: &str| {
        Term::Variable(s.to_string())
    })(input)
}

fn parse_constant(input: &str) -> IResult<&str, Term> {
    map(
        delimited(tag("'"), take_while(|c: char| c != '\''), tag("'")),
        |s: &str| Term::Constant(s.to_string()),
    )(input)
}

fn parse_term(input: &str) -> IResult<&str, Term> {
    alt((parse_constant, parse_variable))(input)
}

fn parse_terms(input: &str) -> IResult<&str, Vec<Term>> {
    separated_list0(tag(","), parse_term)(input)
}

fn parse_atom(input: &str) -> IResult<&str, Atom> {
    map(
        tuple((
            take_while(|c: char| c.is_alphanumeric()),
            delimited(tag("("), parse_terms, tag(")")),
        )),
        |(relation_name, terms)| Atom {
            relation_name: relation_name.to_string().to_lowercase(),
            terms,
        },
    )(input)
}

fn parse_head(input: &str) -> IResult<&str, Atom> {
    map(
        tuple((
            tag("Answer"),
            delimited(tag("("), parse_terms, tag(")")),
            tag(":-"),
        )),
        |(name, terms, _)| Atom {
            relation_name: name.to_string().to_lowercase(),
            terms,
        },
    )(input)
}

fn parse_body(input: &str) -> IResult<&str, Vec<Atom>> {
    map(
        tuple((separated_list0(tag(","), parse_atom), tag("."))),
        |(atoms, _)| atoms,
    )(input)
}

pub fn parse_query(input: &str) -> Query {
    let Ok((_, result)) = map(tuple((parse_head, parse_body)), |(head, body)| Query {
        head,
        body,
    })(input) else {
        panic!("Error parsing query: {}", input);
    };

    result
}

pub fn parse_queries(path: &str) -> Vec<Query> {
    let mut queries = Vec::new();
    let input = std::fs::read_to_string(path).unwrap();
    let lines = input.lines();

    for line in lines {
        let query = parse_query(line);
        queries.push(query);
    }
    queries
}

pub fn get_query_1() -> Query {
    let head = Atom {
        relation_name: "answer".to_string(),
        terms: vec![],
    };

    let body = vec![
        Atom {
            relation_name: "beers".to_string(),
            terms: vec![
                Term::Variable("u1".to_string()),
                Term::Variable("x".to_string()),
                Term::Variable("u2".to_string()),
                Term::Constant("0.07".to_string()),
                Term::Variable("u3".to_string()),
                Term::Variable("u4".to_string()),
                Term::Variable("y".to_string()),
                Term::Variable("u5".to_string()),
            ],
        },
        Atom {
            relation_name: "styles".to_string(),
            terms: vec![
                Term::Variable("u6".to_string()),
                Term::Variable("z".to_string()),
                Term::Variable("y".to_string()),
            ],
        },
        Atom {
            relation_name: "categories".to_string(),
            terms: vec![
                Term::Variable("z".to_string()),
                Term::Variable("u7".to_string()),
            ],
        },
        Atom {
            relation_name: "locations".to_string(),
            terms: vec![
                Term::Variable("u8".to_string()),
                Term::Variable("x".to_string()),
                Term::Variable("u9".to_string()),
                Term::Variable("u10".to_string()),
                Term::Variable("u11".to_string()),
            ],
        },
        Atom {
            relation_name: "breweries".to_string(),
            terms: vec![
                Term::Variable("x".to_string()),
                Term::Variable("u12".to_string()),
                Term::Variable("u13".to_string()),
                Term::Variable("u14".to_string()),
                Term::Variable("u15".to_string()),
                Term::Variable("u16".to_string()),
                Term::Variable("u17".to_string()),
                Term::Variable("u18".to_string()),
                Term::Variable("u13".to_string()),
                Term::Variable("u14".to_string()),
                Term::Variable("u15".to_string()),
            ],
        },
    ];

    Query { head, body }
}

pub fn get_query_2() -> Query {
    let head = Atom {
        relation_name: "Answer".to_string(),
        terms: vec![
            Term::Variable("x".to_string()),
            Term::Variable("y".to_string()),
            Term::Variable("z".to_string()),
        ],
    };

    let body = vec![
        Atom {
            relation_name: "breweries".to_string(),
            terms: vec![
                Term::Variable("w".to_string()),
                Term::Variable("x".to_string()),
                Term::Constant("Westmalle".to_string()),
                Term::Variable("u1".to_string()),
                Term::Variable("u2".to_string()),
                Term::Variable("u3".to_string()),
                Term::Variable("u4".to_string()),
                Term::Variable("u5".to_string()),
                Term::Variable("u6".to_string()),
                Term::Variable("u7".to_string()),
                Term::Variable("u8".to_string()),
            ],
        },
        Atom {
            relation_name: "locations".to_string(),
            terms: vec![
                Term::Variable("u9".to_string()),
                Term::Variable("w".to_string()),
                Term::Variable("y".to_string()),
                Term::Variable("z".to_string()),
                Term::Variable("u10".to_string()),
            ],
        },
    ];

    Query { head, body }
}

pub fn get_query() -> Query {
    let head = Atom {
        relation_name: "answer".to_string(),
        terms: vec![],
    };

    let body = vec![
        Atom {
            relation_name: "beers".to_string(),
            terms: vec![
                Term::Variable("beer_id".to_string()),
                Term::Variable("brew_id".to_string()),
                Term::Variable("beer".to_string()),
                Term::Variable("abv".to_string()),
                Term::Variable("ibu".to_string()),
                Term::Variable("ounces".to_string()),
                Term::Variable("style".to_string()),
                Term::Variable("style2".to_string()),
            ],
        },
        Atom {
            relation_name: "styles".to_string(),
            terms: vec![
                Term::Variable("style_id".to_string()),
                Term::Variable("cat_id".to_string()),
                Term::Variable("style".to_string()),
            ],
        },
        Atom {
            relation_name: "categories".to_string(),
            terms: vec![
                Term::Variable("cat_id".to_string()),
                Term::Constant("Belgian and French Ale".to_string()),
            ],
        },
    ];

    Query { head: head, body }
}
