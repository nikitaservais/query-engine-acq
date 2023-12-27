use query_engine_acq::assignment::{answers_query, bool_answer, is_acyclic};
use query_engine_acq::data_structure::parser::parse_queries;

#[derive(Debug)]
pub struct Answer {
    query_id: usize,
    is_acyclic: bool,
    bool_answer: Option<bool>,
    x: Vec<String>,
    y: Vec<String>,
    z: Vec<String>,
    w: Vec<String>,
}

fn main() {
    let query = parse_queries("input.txt");
    let mut answers: Vec<Answer> = vec![];
    for (id, q) in query.iter().enumerate() {
        let is_acyclic = is_acyclic(q);
        if !is_acyclic {
            answers.push(Answer {
                query_id: id + 1,
                is_acyclic,
                bool_answer: None,
                x: vec![],
                y: vec![],
                z: vec![],
                w: vec![],
            });
            continue;
        }
        let bool_answer = bool_answer(q);
        if bool_answer.is_some() {
            answers.push(Answer {
                query_id: id + 1,
                is_acyclic,
                bool_answer,
                x: vec![],
                y: vec![],
                z: vec![],
                w: vec![],
            });
            continue;
        }
        let answer = answers_query(q);
        answers.push(Answer {
            query_id: id + 1,
            is_acyclic,
            bool_answer,
            x: answer[0].clone(),
            y: answer[1].clone(),
            z: answer[2].clone(),
            w: answer[3].clone(),
        });
    }
    for answer in answers {
        println!("{:?}", answer);
    }
}
