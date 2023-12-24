use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops::Index;
use std::str::FromStr;

use serde::Serialize;

use crate::database::schema::{get_database, Database, Table};

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
pub struct Atom {
    relation_name: String,
    terms: Vec<Term>,
}

#[derive(Debug)]
pub struct Query {
    head: Atom,
    body: Vec<Atom>,
}

impl Query {
    fn yannakakis(&self, database: &Database) {
        // build tree from query
        let join_tree = self.construct_join_tree().unwrap();
        println!("join tree:\n {}", join_tree);
        // build consistent database
        let consistent_database = self.construct_consistent_db(&join_tree, database);
        let root = join_tree.get_root();
        // let root_table = self.query(&root, &consistent_database);
        let mut a_database = consistent_database.clone();
        let mut nodes = join_tree.get_nodes();
        while !nodes.is_empty() {
            let tem = nodes.clone();
            let s = tem
                .iter()
                .find(|node| {
                    let parent_node = join_tree.get_parent(node);
                    match parent_node {
                        Some(parent) => !nodes.contains(&parent),
                        None => true,
                    }
                })
                .unwrap();
            for child in join_tree.get_children(s) {
                let a_s = a_database.semi_join(
                    s,
                    &child,
                    consistent_database.get_table_by_name(&child.relation_name),
                    a_database.get_table_by_name(&s.relation_name),
                );
                a_database.set_table(a_s);
            }
            nodes.remove(s);
        }
        let mut o_database = a_database.clone();
        println!("a_database:\n {}", a_database);
        let mut nodes = join_tree.get_nodes();
        while !nodes.is_empty() {
            let tem = nodes.clone();
            let s = tem
                .iter()
                .find(|node| {
                    let child_nodes = join_tree.get_children(node);
                    child_nodes.iter().all(|child| !nodes.contains(child))
                })
                .unwrap();
            if !join_tree.get_children(s).is_empty() {
                for child in join_tree.get_children(s) {
                    let o_s = o_database.join(
                        s,
                        &child,
                        consistent_database.get_table_by_name(&child.relation_name),
                        o_database.get_table_by_name(&s.relation_name),
                    );
                    o_database.set_table(o_s);
                }
            }
            nodes.remove(s);
        }
        println!("o_database:\n {}", o_database);
    }
}

impl Query {
    // fn run(&self, database: &Database) {
    //     let mut result = database.select(&self.body[0]);
    //     for atom in &self.body[1..] {
    //         result = result.semi_join(&atom);
    //     }
    // }
    pub fn is_acyclic(&self) -> bool {
        let hypergraph = Hypergraph::new(self);
        hypergraph.is_acyclic()
    }

    fn query(&self, atom: &Atom, database: &Database) -> Table {
        let mut db = database.clone();
        let node_table = db.get_table_by_name(&atom.relation_name);
        db.select(atom, node_table)
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

    fn construct_consistent_db(&self, join_tree: &JoinTree, d: &Database) -> Database {
        let mut consistent_database: Database = d.clone();
        let mut nodes = join_tree.get_nodes();
        while !nodes.is_empty() {
            let tem = nodes.clone();
            let s = tem
                .iter()
                .find(|node| {
                    let child_nodes = join_tree.get_children(node);
                    child_nodes.iter().all(|child| !nodes.contains(child))
                })
                .unwrap();
            let q_s = self.query(s, &consistent_database);
            if join_tree.get_children(s).is_empty() {
                consistent_database.set_table(q_s);
            } else {
                for child in join_tree.get_children(s) {
                    let Q_s = consistent_database.semi_join(
                        s,
                        &child,
                        consistent_database.get_table_by_name(&s.relation_name),
                        consistent_database.get_table_by_name(&child.relation_name),
                    );
                    consistent_database.set_table(Q_s);
                }
            }
            nodes.remove(s);
        }
        consistent_database
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
    fn get_root(&self) -> Atom {
        self.edges
            .clone()
            .into_iter()
            .find(|(parent, _)| self.get_parent(parent).is_none())
            .unwrap()
            .0
    }

    fn add_edge(&mut self, ear: Atom, witness: Atom) {
        self.edges.insert((ear, witness));
    }

    fn get_parent(&self, child: &Atom) -> Option<Atom> {
        for (parent, child_check) in &self.edges {
            if child == child_check {
                return Some(parent.clone());
            }
        }
        None
    }

    fn get_nodes(&self) -> HashSet<Atom> {
        let mut nodes = HashSet::new();
        for (parent, child) in &self.edges {
            nodes.insert(parent.clone());
            nodes.insert(child.clone());
        }
        nodes
    }

    pub fn get_children(&self, parent: &Atom) -> HashSet<Atom> {
        let mut children = HashSet::new();
        for (parent_check, child) in &self.edges {
            if parent == parent_check {
                children.insert(child.clone());
                children.extend(self.get_children(child));
            }
        }
        children
    }
}

impl fmt::Display for JoinTree {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut result = String::new();
        for (parent, child) in &self.edges {
            result.push_str(&format!(
                "{} -> {}\n",
                parent.relation_name, child.relation_name
            ));
        }
        write!(f, "{}", result)
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
    let database = get_database();
    // database.semi_join(
    //     &Atom {
    //         relation_name: "styles".to_string(),
    //         terms: vec![
    //             Term::Variable("style_id".to_string()),
    //             Term::Variable("cat_id".to_string()),
    //             Term::Variable("style".to_string()),
    //         ],
    //     },
    //     &Atom {
    //         relation_name: "categories".to_string(),
    //         terms: vec![
    //             Term::Variable("cat_id".to_string()),
    //             Term::Constant("Belgian and French Ale".to_string()),
    //         ],
    //     },
    // );
    let query = get_query();
    query.yannakakis(&database);
}

fn get_query() -> Query {
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

fn get_query_1() -> Query {
    let head = Atom {
        relation_name: "answer".to_string(),
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
