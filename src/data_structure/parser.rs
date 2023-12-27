use nom::branch::alt;
use nom::bytes::complete::{tag, take_while, take_while1};
use nom::combinator::map;
use nom::multi::separated_list0;
use nom::sequence::{delimited, tuple};
use nom::IResult;

use crate::data_structure::query::{Atom, Query, Term};

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
        if line.is_empty() {
            continue;
        };
        let query = parse_query(line);
        queries.push(query);
    }
    queries
}
