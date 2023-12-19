use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;

use serde::Serialize;

mod database;
mod test;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Term {
    Variable(String),
    Constant(String),
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Term::Variable(var) => write!(f, "Variable({})", var),
            Term::Constant(name) => write!(f, "Constant({})", name),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Variable(String);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Constant(String);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Atom {
    relation_name: String,
    terms: Vec<Term>,
}

#[derive(Debug)]
pub struct Query {
    head: Atom,
    body: Vec<Atom>,
}

impl Query {
    pub fn is_acyclic(&self) -> bool {
        let hypergraph = Hypergraph::new(self);
        hypergraph.is_acyclic()
    }

    fn construct_join_tree(&self) -> Option<JoinTree> {
        if !self.is_acyclic() {
            return None;
        }
        let mut join_tree = JoinTree::new();
        let mut hypergraph = Hypergraph::new(self);

        while let Some((ear, witness)) = hypergraph.find_ear() {
            if ear == witness {
                hypergraph.hyperedges.remove(&ear);
                continue;
            }
            join_tree.add_edge(ear.clone(), witness.clone());
            hypergraph.hyperedges.remove(&ear);
        }

        Some(join_tree)
    }
}

#[derive(Clone, Debug)]
struct JoinTree {
    edges: HashSet<(Atom, Atom)>,
}

impl JoinTree {
    fn new() -> Self {
        JoinTree {
            edges: HashSet::new(),
        }
    }

    fn add_edge(&mut self, ear: Atom, witness: Atom) {
        self.edges.insert((ear, witness));
    }
}

impl fmt::Display for JoinTree {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "JoinTree:\n")?;
        for (ear, witness) in &self.edges {
            write!(f, "[")?;
            let ear_terms: Vec<String> = ear.terms.iter().map(|term| term.0.clone()).collect();
            write!(f, "{}", ear_terms.join(", "))?;
            write!(f, "] -> [")?;
            let witness_terms: Vec<String> =
                witness.terms.iter().map(|term| term.0.clone()).collect();
            write!(f, "{}", witness_terms.join(", "))?;
            write!(f, "]\n")?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct Hypergraph {
    hyperedges: HashMap<Atom, HashSet<Term>>,
}

impl Hypergraph {
    fn new(query: &Query) -> Self {
        let mut edges = HashMap::new();
        for atom in &query.body {
            let variables = atom.terms.iter().cloned().collect();
            edges.insert(atom.clone(), variables);
        }
        Hypergraph { hyperedges: edges }
    }

    fn find_ear(&self) -> Option<(Atom, Atom)> {
        for (ear_candidate, ear_vertices) in &self.hyperedges {
            // all vertices of the hyperedge are exclusive to that hyperedge
            if self.is_vertices_exclusive(ear_candidate, ear_vertices) {
                return Some((ear_candidate.clone(), ear_candidate.clone()));
            }
            // there exists another hyperedge w (called a witness of ear_candidate) such that every vertices in ear_candidate is either exclusive to ear_candidate or also occurring in witness
            let mut exclusive_vertices = HashSet::new();
            let mut witness_vertices = HashSet::new();
            for vertex in ear_vertices {
                if self.is_vertex_exclusive(ear_candidate, vertex) {
                    exclusive_vertices.insert(vertex.clone());
                } else {
                    witness_vertices.insert(vertex.clone());
                }
            }
            for (witness_candidate, witness_candidate_vertices) in &self.hyperedges {
                if ear_candidate != witness_candidate {
                    if witness_vertices.is_subset(witness_candidate_vertices) {
                        return Some((ear_candidate.clone(), witness_candidate.clone()));
                    }
                }
            }
        }
        None
    }

    fn is_acyclic(&self) -> bool {
        match self.find_ear() {
            // If an ear is found, remove it and check the remaining hypergraph
            Some((ear, _)) => {
                let mut remaining_hypergraph = self.clone();
                remaining_hypergraph.hyperedges.remove(&ear);
                remaining_hypergraph.is_acyclic()
            }
            // If no ear is found, terminate and check the hypergraph is empty
            None => self.hyperedges.is_empty(),
        }
    }

    fn is_vertices_exclusive(&self, hyperedge: &Atom, vertices: &HashSet<Term>) -> bool {
        for (other_hyperedge, other_vertices) in &self.hyperedges {
            if hyperedge != other_hyperedge {
                for vertex in vertices {
                    if other_vertices.contains(vertex) {
                        return false;
                    }
                }
            }
        }
        true
    }

    fn is_vertex_exclusive(&self, hyperedge: &Atom, vertex: &Term) -> bool {
        for (other_hyperedge, other_vertices) in &self.hyperedges {
            if hyperedge != other_hyperedge {
                if other_vertices.contains(vertex) {
                    return false;
                }
            }
        }
        true
    }
}

fn main() {
    let query = get_query();
    let join_tree = query.construct_join_tree().unwrap();
    println!("{}", join_tree);
    // load_data();
    // read();
}

fn get_query() -> Query {
    let head = Atom {
        relation_name: "Answer".to_string(),
        terms: vec![],
    };

    let body = vec![
        Atom {
            relation_name: "Beers".to_string(),
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
            relation_name: "Styles".to_string(),
            terms: vec![
                Term::Variable("style_id".to_string()),
                Term::Variable("cat_id".to_string()),
                Term::Variable("style".to_string()),
            ],
        },
        Atom {
            relation_name: "Categories".to_string(),
            terms: vec![
                Term::Variable("cat_id".to_string()),
                Term::Constant("Belgian and French Ale".to_string()),
            ],
        },
    ];

    Query { head: head, body }
}

fn get_query_1() -> Query {
    let head = Atom {
        relation_name: "Answer".to_string(),
        terms: vec![],
    };

    let body = vec![
        Atom {
            relation_name: "Beers".to_string(),
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
            relation_name: "Styles".to_string(),
            terms: vec![
                Term::Variable("u6".to_string()),
                Term::Variable("z".to_string()),
                Term::Variable("y".to_string()),
            ],
        },
        Atom {
            relation_name: "Categories".to_string(),
            terms: vec![
                Term::Variable("z".to_string()),
                Term::Variable("u7".to_string()),
            ],
        },
        Atom {
            relation_name: "Breweries".to_string(),
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
