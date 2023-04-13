use std::collections::HashMap;

use node::core::{Handler, Message, Node, Type, Workload};
use node::helper::{Error, Result};
use node::Runner;

fn handler_echo(node: &mut Node, msg: Message) -> Result<Message> {
    match msg.body {
        Workload::Echo { msg_id, echo } => {
            let body = Workload::EchoOk {
                in_reply_to: msg_id,
                echo,
                msg_id: node.gen_msg_id(),
            };
            Ok(node.reply(msg.src.clone(), body))
        }
        _ => Err(Box::new(Error::ExpectedMessage {
            found: msg.body.key().unwrap_or(Type::Invalid),
            expected: Type::Echo,
        })),
    }
}

fn create_node() -> Node {
    let mut handlers: HashMap<Type, Handler> = HashMap::new();
    handlers.insert(Type::Echo, handler_echo);
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
    fn test_echo() {
        let mut node = create_node();
        let init_json = r#"{"src":"c1","dest":"n1","body":{"type":"init","msg_id":1,"node_id":"n1","node_ids":["n1","n2","n3"]}}"#;
        let init_message = serde_json::from_str::<Message>(init_json).unwrap();
        let _ = node.process(init_message);

        let echo_json =
            r#"{"src":"c1","dest":"n1","body":{"type":"echo","echo":"Hello, World!","msg_id":1}}"#;
        let echo_message = serde_json::from_str::<Message>(echo_json).unwrap();
        let reply = node.process(echo_message);
        assert!(reply.is_ok());

        let reply = serde_json::to_string(&reply.unwrap()).unwrap();
        assert_eq!(
            reply,
            r#"{"src":"n1","dest":"c1","body":{"type":"echo_ok","in_reply_to":1,"msg_id":1,"echo":"Hello, World!"}}"#
        );
    }
}
