use std::fmt;

use crate::data_structure::database::Database;
use crate::data_structure::hypergraph::Hypergraph;
use crate::data_structure::join_tree::JoinTree;
use crate::data_structure::relational_algebra;
use crate::data_structure::table::Table;

#[derive(Debug)]
pub struct Query {
    pub head: Atom,
    pub body: Vec<Atom>,
}

impl Query {
    fn print_query_database(&self, database: &Database) {
        println!("query database:");
        for atom in &self.body {
            println!(
                "{}:\n{}",
                atom.relation_name,
                database.get_table(&atom.relation_name)
            );
        }
    }

    pub fn yannakakis(&self, database: Database) -> Table {
        let Some(join_tree) = self.construct_join_tree() else {
            println!("Not acyclic");
            return Table::new_empty(self.head.relation_name.clone());
        };
        println!("join tree:\n{}", join_tree);
        if self.is_boolean() {
            return Table::new_empty(self.head.relation_name.clone());
        }
        let Some(consistent_database) = self.construct_consistent_db(&join_tree, &database) else {
            println!("Not consistent, no answer");
            return Table::new_empty(self.head.relation_name.clone());
        };
        self.print_query_database(&consistent_database);
        let mut o_database = consistent_database.clone();
        let mut nodes = join_tree.get_nodes();
        while !nodes.is_empty() {
            let s = &join_tree.find_node_with_no_child_in_nodes(&nodes).unwrap();
            if !join_tree.is_leaf(s) {
                let union = Atom::union(s, &self.head);
                println!("{:?}∪\n{:?}\n={:?}", s.terms, self.head.terms, union);
                for child in join_tree.get_children(s) {
                    let old_big_o_s = o_database.get_table(&s.relation_name).clone();
                    println!(
                        "Join table: \n{}\n{}",
                        o_database.get_table(&s.relation_name),
                        o_database.get_table(&child.relation_name),
                    );
                    let join = relational_algebra::join(
                        s,
                        &child,
                        o_database.get_table(&s.relation_name),
                        o_database.get_table(&child.relation_name),
                    );
                    println!(
                        "O_{} ⋈ O_{} =\n{}",
                        s.relation_name, child.relation_name, join
                    );
                    let o_s = join.project(&union);
                    println!("set O_{}\n{}\nto\n{}", s.relation_name, old_big_o_s, o_s);
                    o_database.set_table(&s.relation_name, o_s);
                }
            }
            nodes.remove(s);
        }
        self.print_query_database(&o_database);
        let big_o_r = o_database.get_table(&join_tree.get_root().relation_name);
        let answer = big_o_r.project(&self.head.terms);

        println!("O_r:\n{}", answer);
        answer
    }

    pub fn is_boolean(&self) -> bool {
        self.head.terms.is_empty()
    }

    pub fn yannakakis_boolean(&self, database: &Database) -> bool {
        if !self.is_boolean() {
            return false;
        }
        let join_tree = self.construct_join_tree().unwrap();
        let root = join_tree.get_root();
        let big_q = self.compute_big_q(&join_tree, database);
        if !big_q.get_table(&root.relation_name).is_empty() {
            return true;
        }
        false
    }

    pub fn compute_big_q(&self, join_tree: &JoinTree, database: &Database) -> Database {
        let mut big_q: Database = database.clone();
        let mut nodes = join_tree.get_nodes();
        while !nodes.is_empty() {
            let s = &join_tree.find_node_with_no_child_in_nodes(&nodes).unwrap();
            let q_s = self.compute_atom(s, &big_q);
            if join_tree.get_children(s).is_empty() {
                println!("Q_{} =: q_{}", s.relation_name, s.relation_name);
                println!("set Q_{}\n to\n{}", s.relation_name, &q_s);
                big_q.set_table(&s.relation_name, q_s);
            } else {
                let old_big_q_s = big_q.get_table(&s.relation_name).clone();
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
                let mut big_q_s = q_s.clone();
                for child in join_tree.get_children(s) {
                    let semi_join = relational_algebra::semi_join(
                        s,
                        &child,
                        &q_s,
                        big_q.get_table(&child.relation_name),
                    );
                    println!(
                        "q_{} ⋉ Q_{} =\n{}",
                        s.relation_name, child.relation_name, semi_join
                    );
                    big_q_s = semi_join.intersection(&big_q_s);
                }

                println!(
                    "set Q_{}\n{}\nto\n{}",
                    s.relation_name, old_big_q_s, big_q_s
                );
                big_q.set_table(&s.relation_name, big_q_s);
            }
            nodes.remove(s);
        }
        big_q
    }
}

impl Query {
    pub fn is_acyclic(&self) -> bool {
        let hypergraph = Hypergraph::new(self);
        hypergraph.is_acyclic()
    }

    fn compute_atom(&self, atom: &Atom, database: &Database) -> Table {
        let node_table = database.get_table(&atom.relation_name);
        relational_algebra::select(atom, node_table)
    }

    pub fn construct_join_tree(&self) -> Option<JoinTree> {
        if !self.is_acyclic() {
            return None;
        }
        let mut join_tree = JoinTree::default();
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
        let big_q = self.compute_big_q(join_tree, database);
        let mut a_database = database.clone();
        let root = join_tree.get_root();
        a_database.set_table(
            &root.relation_name,
            big_q.get_table(&root.relation_name).clone(),
        );
        let mut nodes = join_tree.get_nodes();
        while !nodes.is_empty() {
            let s = &join_tree.find_node_with_no_parent_in_nodes(&nodes).unwrap();
            for child in join_tree.get_children(s) {
                let old_big_a_child = a_database.get_table(&child.relation_name).clone();
                println!(
                    "A_{} =: Q_{} ⋉ A_{}",
                    child.relation_name, child.relation_name, s.relation_name
                );
                let a_child = relational_algebra::semi_join(
                    &child,
                    s,
                    big_q.get_table(&child.relation_name),
                    a_database.get_table(&s.relation_name),
                );
                println!(
                    "set A_{}\n{}\nto\n{}",
                    child.relation_name, old_big_a_child, a_child
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
    pub fn merge(left: &Atom, right: &Atom) -> Self {
        Self {
            relation_name: format!("{}_{}", left.relation_name, right.relation_name),
            terms: [left.terms.clone(), right.terms.clone()].concat(),
        }
    }

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
