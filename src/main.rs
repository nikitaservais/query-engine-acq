use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::Display;
use std::ops::Index;
use std::str::FromStr;

use arrow::array::Array;
use nom::bytes::complete::{is_not, take_till};
use nom::character::complete::char;
use nom::character::is_alphabetic;
use nom::{
    branch::alt,
    bytes::complete::{tag, take_while},
    character::complete::multispace0,
    combinator::map,
    multi::separated_list0,
    sequence::{delimited, preceded, tuple},
    IResult,
};
use serde::Serialize;

use crate::database::schema::{get_database, Database, Table};
use crate::parser::parse_queries;

mod database;
mod parser;
mod test;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Term {
    Variable(String),
    Constant(String),
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Term::Variable(var) => write!(f, "{}", var),
            Term::Constant(name) => write!(f, "{}", name),
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
    fn union(&self, left: &Vec<Term>, right: &Vec<Term>) -> Vec<Term> {
        let mut result = left.clone();
        for term in right {
            if !result.contains(&term) {
                result.push(term.clone());
            }
        }
        result
    }

    fn print_query_database(&self, database: &Database) {
        println!("query database:");
        for atom in &self.body {
            println!(
                "{}:\n{}",
                atom.relation_name,
                database.get_table_by_name(&atom.relation_name)
            );
        }
    }

    fn yannakakis(&self, database: &Database) {
        // build tree from query
        let mut db = database.clone();
        let join_tree = self.construct_join_tree().unwrap();
        println!("join tree:\n{}", join_tree);
        db.rename(&self);
        // build consistent database
        let consistent_database = self.construct_consistent_db(&join_tree, &db);
        self.print_query_database(&consistent_database);
        let mut o_database = consistent_database.clone();
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
                let union = self.union(&s.terms, &self.head.terms);
                println!("{:?}∪\n{:?}\n={:?}", s.terms, self.head.terms, union);
                for child in join_tree.get_children(s) {
                    let old_O_s = o_database.get_table_by_name(&s.relation_name).clone();
                    println!(
                        "Join table: \n{}\n{}",
                        o_database.get_table_by_name(&s.relation_name),
                        o_database.get_table_by_name(&child.relation_name),
                    );
                    let join = o_database.join(
                        s,
                        &child,
                        o_database.get_table_by_name(&s.relation_name),
                        o_database.get_table_by_name(&child.relation_name),
                    );
                    println!(
                        "O_{} ⋈ O_{} =\n{}",
                        s.relation_name, child.relation_name, join
                    );
                    let o_s = o_database.project(&union, &join);
                    println!("set O_{}\n{}\nto\n{}", s.relation_name, old_O_s, o_s);
                    o_database.set_table(&s.relation_name, o_s);
                }
            }
            nodes.remove(s);
        }
        self.print_query_database(&o_database);
        let O_r = o_database.get_table_by_name(&join_tree.get_root().relation_name);
        let answer = o_database.project(&self.head.terms, &O_r);

        println!("O_r:\n{}", answer);
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
        let mut Q: Database = d.clone();
        let mut nodes = join_tree.get_nodes();
        let root = join_tree.get_root();
        // preorder traversal
        while !nodes.is_empty() {
            let tem = nodes.clone();
            let s = tem
                .iter()
                .find(|node| {
                    let child_nodes = join_tree.get_children(node);
                    child_nodes.iter().all(|child| !nodes.contains(child))
                })
                .unwrap();
            let q_s = self.query(s, &Q);
            if join_tree.get_children(s).is_empty() {
                println!("Q_{} =: q_{}", s.relation_name, s.relation_name);
                println!("set Q_{}\n to\n{}", s.relation_name, &q_s);
                Q.set_table(&s.relation_name, q_s);
            } else {
                let old_Q_s = Q.get_table_by_name(&s.relation_name).clone();
                println!(
                    "Q_{} =: {}",
                    s.relation_name,
                    join_tree
                        .get_children(s)
                        .iter()
                        .map(|child| format!(
                            "(q_{} ⋉ Q_{})",
                            s.relation_name,
                            child.relation_name.clone()
                        ))
                        .collect::<Vec<String>>()
                        .join(" ∩ ")
                );
                // let mut
                let mut Q_s = q_s.clone();
                for child in join_tree.get_children(s) {
                    let semi_join =
                        Q.semi_join(s, &child, &q_s, Q.get_table_by_name(&child.relation_name));
                    println!(
                        "q_{} ⋉ Q_{} =\n{}",
                        s.relation_name, child.relation_name, semi_join
                    );
                    Q_s = Q.intersection(&Q_s, &semi_join);
                    // println!("∩ {}\n{}", Q_s, semi_join)
                }

                println!("set Q_{}\n{}\nto\n{}", s.relation_name, old_Q_s, Q_s);
                Q.set_table(&s.relation_name, Q_s);
            }
            nodes.remove(s);
        }
        let mut a_database = Q.clone();
        let mut nodes = join_tree.get_nodes();
        let a_r = self.query(&root, &Q);
        a_database.set_table(&root.relation_name, a_r);
        // postorder traversal
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
                let old_A_child = a_database.get_table_by_name(&child.relation_name).clone();
                println!(
                    "A_{} =: Q_{} ⋉ A_{}",
                    child.relation_name, child.relation_name, s.relation_name
                );
                let a_child = a_database.semi_join(
                    &child,
                    s,
                    Q.get_table_by_name(&child.relation_name),
                    a_database.get_table_by_name(&s.relation_name),
                );
                println!(
                    "set A_{}\n{}\nto\n{}",
                    child.relation_name, old_A_child, a_child
                );
                a_database.set_table(&child.relation_name, a_child);
            }
            nodes.remove(s);
        }
        a_database
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
    let queries = parse_queries();
    let database = get_database();
    let query = get_query_2();
    query.yannakakis(&database);
}
