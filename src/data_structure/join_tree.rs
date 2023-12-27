use crate::data_structure::query::Atom;
use std::collections::HashSet;
use std::fmt;

#[derive(Clone, Debug)]
pub struct JoinTree {
    edges: HashSet<(Atom, Atom)>,
}

impl Default for JoinTree {
    fn default() -> Self {
        Self::new()
    }
}

impl JoinTree {
    fn new() -> Self {
        JoinTree {
            edges: HashSet::new(),
        }
    }
    pub fn get_root(&self) -> Atom {
        self.edges
            .clone()
            .into_iter()
            .find(|(parent, _)| self.get_parent(parent).is_none())
            .unwrap()
            .0
    }

    pub fn add_edge(&mut self, ear: Atom, witness: Atom) {
        self.edges.insert((ear, witness));
    }

    pub fn get_parent(&self, child: &Atom) -> Option<Atom> {
        for (parent, child_check) in &self.edges {
            if child == child_check {
                return Some(parent.clone());
            }
        }
        None
    }

    pub fn get_nodes(&self) -> HashSet<Atom> {
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

    pub fn find_node_with_no_child_in_nodes(&self, nodes: &HashSet<Atom>) -> Option<Atom> {
        let Some(node) = nodes.iter().find(|node| {
            let child_nodes = self.get_children(node);
            child_nodes.iter().all(|child| !nodes.contains(child))
        }) else {
            return None;
        };
        Some(node.clone())
    }

    pub fn find_node_with_no_parent_in_nodes(&self, nodes: &HashSet<Atom>) -> Option<Atom> {
        let Some(node) = nodes.iter().find(|node| {
            let parent_node = self.get_parent(node);
            match parent_node {
                Some(parent) => !nodes.contains(&parent),
                None => true,
            }
        }) else {
            return None;
        };
        Some(node.clone())
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
