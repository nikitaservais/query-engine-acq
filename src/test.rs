#[cfg(test)]
mod tests {
    use super::super::*;

    #[test]
    fn test_is_acyclic() {
        let head = Atom {
            relation_name: "Answer".to_string(),
            terms: vec![],
        };

        let body = vec![
            Atom {
                relation_name: "Beers".to_string(),
                terms: vec![
                    Term("beer_id".to_string()),
                    Term("brew_id".to_string()),
                    Term("beer".to_string()),
                    Term("abv".to_string()),
                    Term("ibu".to_string()),
                    Term("ounces".to_string()),
                    Term("style".to_string()),
                    Term("style2".to_string()),
                ],
            },
            Atom {
                relation_name: "Styles".to_string(),
                terms: vec![
                    Term("style_id".to_string()),
                    Term("cat_id".to_string()),
                    Term("style".to_string()),
                ],
            },
            Atom {
                relation_name: "Categories".to_string(),
                terms: vec![
                    Term("cat_id".to_string()),
                    Term("‘Belgian and French Ale’".to_string()),
                ],
            },
        ];

        let query = Query { _head: head, body };
        let hypergraph = Hypergraph::new(&query);
        assert!(hypergraph.is_acyclic());

        let head = Atom {
            relation_name: "Answer".to_string(),
            terms: vec![],
        };

        let body = vec![
            Atom {
                relation_name: "R1".to_string(),
                terms: vec![
                    Term("x".to_string()),
                    Term("y".to_string()),
                    Term("z".to_string()),
                ],
            },
            Atom {
                relation_name: "R2".to_string(),
                terms: vec![
                    Term("x".to_string()),
                    Term("y".to_string()),
                    Term("v".to_string()),
                ],
            },
            Atom {
                relation_name: "R3".to_string(),
                terms: vec![
                    Term("y".to_string()),
                    Term("z".to_string()),
                    Term("t".to_string()),
                ],
            },
            Atom {
                relation_name: "R4".to_string(),
                terms: vec![
                    Term("x".to_string()),
                    Term("y".to_string()),
                    Term("u".to_string()),
                ],
            },
            Atom {
                relation_name: "R5".to_string(),
                terms: vec![
                    Term("u".to_string()),
                    Term("w".to_string()),
                ],
            },
        ];

        let query = Query { _head: head, body };
        let hypergraph = Hypergraph::new(&query);
        assert!(hypergraph.is_acyclic());
        let jointree = query.construct_join_tree().unwrap();
        println!("{}", jointree);
    }

    #[test]
    fn test_is_not_acyclic() {
        let head = Atom {
            relation_name: "Answer".to_string(),
            terms: vec![],
        };

        let body = vec![
            Atom {
                relation_name: "Beers".to_string(),
                terms: vec![
                    Term("beer_id".to_string()),
                    Term("brew_id".to_string()),
                    Term("beer".to_string()),
                    Term("abv".to_string()),
                    Term("ibu".to_string()),
                    Term("ounces".to_string()),
                    Term("style".to_string()),
                    Term("style2".to_string()),
                ],
            },
            Atom {
                relation_name: "Styles".to_string(),
                terms: vec![
                    Term("style_id".to_string()),
                    Term("cat_id".to_string()),
                    Term("style".to_string()),
                ],
            },
            Atom {
                relation_name: "Categories".to_string(),
                terms: vec![
                    Term("cat_id".to_string()),
                    Term("‘Belgian and French Ale’".to_string()),
                    Term("ounces".to_string()),
                ],
            },
        ];

        let query = Query { _head: head, body };
        let hypergraph = Hypergraph::new(&query);
        assert!(!hypergraph.is_acyclic());
    }
}