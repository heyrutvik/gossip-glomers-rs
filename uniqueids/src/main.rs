use std::collections::HashMap;

use node::core::{Handler, Message, Node, Type, Workload};
use node::helper::{Error, Result};
use node::Runner;

fn handler_generate(node: &mut Node, msg: Message) -> Result<Vec<Message>> {
    match msg.body {
        Workload::Generate { msg_id } => {
            let body = Workload::generate_ok(msg_id, node.gen_msg_id(), node.gen_unique_id());
            Ok(vec![node.reply(msg.src.clone(), body)])
        }
        _ => Err(Box::new(Error::ExpectedMessage {
            found: msg.body.key().unwrap_or(Type::Invalid),
            expected: Type::Generate,
        })),
    }
}

fn create_node() -> Node {
    let mut handlers: HashMap<Type, Handler> = HashMap::new();
    handlers.insert(Type::Generate, handler_generate);
    Node::new(handlers)
}

fn main() {
    let node = create_node();
    let mut runner = Runner::new(node);
    runner.start();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uniqueids() {
        let mut node = create_node();
        let init_json = r#"{"src":"c1","dest":"n1","body":{"type":"init","msg_id":1,"node_id":"n1","node_ids":["n1","n2","n3"]}}"#;
        let init_message = serde_json::from_str::<Message>(init_json).unwrap();
        let _ = node.process(init_message);

        let generate_json = r#"{"src":"c1","dest":"n1","body":{"type":"generate","msg_id":1}}"#;
        let generate_message = serde_json::from_str::<Message>(generate_json).unwrap();
        let reply = node.process(generate_message);
        assert!(reply.is_ok());
        assert!(match reply.unwrap().first().unwrap().body {
            Workload::GenerateOk { in_reply_to, .. } => in_reply_to == 1,
            _ => false,
        });
    }
}
