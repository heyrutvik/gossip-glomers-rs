use std::collections::HashMap;

use node::core::{Handler, Message, Node, Type, Workload};
use node::helper::{Error, Result};
use node::Runner;

fn handler_generate(node: &mut Node, msg: Message) -> Result<Message> {
    match msg.body {
        Workload::Generate { msg_id } => {
            let body = Workload::GenerateOk {
                in_reply_to: msg_id,
                msg_id: node.gen_msg_id(),
                id: node.gen_unique_id(),
            };
            Ok(node.reply(msg.src.clone(), body))
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
