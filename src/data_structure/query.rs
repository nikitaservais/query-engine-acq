use crate::data_structure::database::Database;
use crate::data_structure::hypergraph::Hypergraph;
use crate::data_structure::join_tree::JoinTree;
use std::fmt;

use crate::data_structure::table::Table;

#[derive(Debug)]
pub struct Query {
    pub head: Atom,
    pub body: Vec<Atom>,
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

    pub fn yannakakis(&self, mut database: Database) {
        database.rename(&self);
        let Some(join_tree) = self.construct_join_tree() else {
            println!("Not acyclic");
            return;
        };
        println!("join tree:\n{}", join_tree);
        if self.is_boolean() {
            print!("Boolean query:");
            self.yannakakis_boolean(&join_tree, &database);
            return;
        }
        let Some(consistent_database) = self.construct_consistent_db(&join_tree, &database) else {
            println!("Not consistent, no answer");
            return;
        };
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
    fn is_boolean(&self) -> bool {
        self.head.terms.is_empty()
    }
    fn yannakakis_boolean(&self, join_tree: &JoinTree, database: &Database) -> bool {
        let root = join_tree.get_root();
        let Q_root = self.compute_Q(join_tree, database);
        if !Q_root.get_table_by_name(&root.relation_name).is_empty() {
            println!("answer: true");
            return true;
        }
        print!("answer: false");
        false
    }

    fn compute_Q(&self, join_tree: &JoinTree, database: &Database) -> Database {
        let mut Q: Database = database.clone();
        let mut nodes = join_tree.get_nodes();
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
        Q
    }
}

impl Query {
    pub fn is_acyclic(&self) -> bool {
        let hypergraph = Hypergraph::new(self);
        hypergraph.is_acyclic()
    }

    fn query(&self, atom: &Atom, database: &Database) -> Table {
        let node_table = database.get_table_by_name(&atom.relation_name);
        database.select(atom, node_table)
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
            join_tree.add_edge(witness.clone(), ear.clone());
            hypergraph.hyperedges.remove(&ear);
        }

        Some(join_tree)
    }

    fn construct_consistent_db(
        &self,
        join_tree: &JoinTree,
        database: &Database,
    ) -> Option<Database> {
        // preorder traversal
        let Q: Database = self.compute_Q(join_tree, database);
        let root = join_tree.get_root();
        let a_r = Q.get_table_by_name(&root.relation_name);
        if a_r.is_empty() {
            return None;
        }
        let mut a_database = database.clone();
        a_database.set_table(&root.relation_name, a_r.clone());
        // postorder traversal
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
        Some(a_database)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Variable(String);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Constant(String);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Atom {
    pub relation_name: String,
    pub terms: Vec<Term>,
}

impl Atom {
    pub fn union(left: &Atom, right: &Atom) -> Vec<Term> {
        let mut result = left.terms.clone();
        for term in right.terms.clone() {
            if !result.contains(&term) {
                result.push(term.clone());
            }
        }
        result
    }
}

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
