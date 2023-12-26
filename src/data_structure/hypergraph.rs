use crate::data_structure::query::{Atom, Query, Term};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug)]
pub struct Hypergraph {
    pub hyperedges: HashMap<Atom, HashSet<Term>>,
}

impl Hypergraph {
    pub fn new(query: &Query) -> Self {
        let mut edges = HashMap::new();
        for atom in &query.body {
            let variables = atom.terms.iter().cloned().collect();
            edges.insert(atom.clone(), variables);
        }
        Hypergraph { hyperedges: edges }
    }

    pub(crate) fn find_ear(&self) -> Option<(Atom, Atom)> {
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

    pub fn is_acyclic(&self) -> bool {
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
