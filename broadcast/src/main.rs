use std::collections::HashMap;

use node::core::{BroadcastMessage, Handler, Message, Node, NodeId, Type, Workload};
use node::helper::{Error, Result};
use node::Runner;

fn broadcast_message(node: &mut Node, src: NodeId, message: BroadcastMessage) -> Vec<Message> {
    let mut replies = Vec::new();
    if !node.broadcast_messages().contains(&message) {
        node.push_broadcast_message(message);
        let neighbors = node.neighbors().clone(); // FIXME
        for neighbor in neighbors {
            if *neighbor != src {
                let body = Workload::Broadcast {
                    msg_id: node.gen_msg_id(),
                    message,
                };
                let reply = node.reply(neighbor.clone(), body);
                replies.push(reply);
            }
        }
    }
    replies
}

fn handler_broadcast(node: &mut Node, msg: Message) -> Result<Vec<Message>> {
    match msg.body {
        Workload::Broadcast { msg_id, message } => {
            let mut replies = broadcast_message(node, msg.src.clone(), message);
            let body = Workload::broadcast_ok(msg_id, node.gen_msg_id());
            replies.push(node.reply(msg.src.clone(), body));
            Ok(replies)
        }
        _ => Err(Box::new(Error::ExpectedMessage {
            found: msg.body.key().unwrap_or(Type::Invalid),
            expected: Type::Broadcast,
        })),
    }
}

fn handler_read(node: &mut Node, msg: Message) -> Result<Vec<Message>> {
    match msg.body {
        Workload::Read { msg_id } => {
            let body = Workload::read_ok(msg_id, node.gen_msg_id(), node.broadcast_messages());
            Ok(vec![node.reply(msg.src.clone(), body)])
        }
        _ => Err(Box::new(Error::ExpectedMessage {
            found: msg.body.key().unwrap_or(Type::Invalid),
            expected: Type::Read,
        })),
    }
}

fn handler_topology(node: &mut Node, msg: Message) -> Result<Vec<Message>> {
    match msg.body {
        Workload::Topology {
            msg_id,
            mut topology,
        } => {
            let node_id = node.node_id();
            let neighbors = topology.remove(&node_id).unwrap_or(Vec::new());
            node.set_neighbors(neighbors);
            let body = Workload::topology_ok(msg_id, node.gen_msg_id());
            Ok(vec![node.reply(msg.src.clone(), body)])
        }
        _ => Err(Box::new(Error::ExpectedMessage {
            found: msg.body.key().unwrap_or(Type::Invalid),
            expected: Type::Topology,
        })),
    }
}

fn create_node() -> Node {
    let mut handlers: HashMap<Type, Handler> = HashMap::new();
    handlers.insert(Type::Broadcast, handler_broadcast);
    handlers.insert(Type::Read, handler_read);
    handlers.insert(Type::Topology, handler_topology);
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
    fn test_broadcast() {
        let mut node = create_node();
        let init_json = r#"{"src":"c1","dest":"n1","body":{"type":"init","msg_id":1,"node_id":"n1","node_ids":["n1","n2","n3"]}}"#;
        let init_message = serde_json::from_str::<Message>(init_json).unwrap();
        let _ = node.process(init_message);

        let broadcast_json =
            r#"{"src":"c1","dest":"n1","body":{"type":"broadcast","message":1000,"msg_id":1}}"#;
        let broadcast_message = serde_json::from_str::<Message>(broadcast_json).unwrap();
        let reply = node.process(broadcast_message);
        assert!(reply.is_ok());

        let reply = serde_json::to_string(&reply.unwrap().first().unwrap()).unwrap();
        assert_eq!(
            reply,
            r#"{"src":"n1","dest":"c1","body":{"type":"broadcast_ok","in_reply_to":1,"msg_id":1}}"#
        );

        let broadcast_json =
            r#"{"src":"c1","dest":"n1","body":{"type":"broadcast","message":10,"msg_id":2}}"#;
        let broadcast_message = serde_json::from_str::<Message>(broadcast_json).unwrap();
        let _ = node.process(broadcast_message);

        let read_json = r#"{"src":"c1","dest":"n1","body":{"type":"read","msg_id":2}}"#;
        let read_message = serde_json::from_str::<Message>(read_json).unwrap();
        let reply = node.process(read_message);
        assert!(reply.is_ok());

        let reply = serde_json::to_string(&reply.unwrap().first().unwrap()).unwrap();
        assert_eq!(
            reply,
            r#"{"src":"n1","dest":"c1","body":{"type":"read_ok","in_reply_to":2,"msg_id":3,"messages":[1000,10]}}"#
        );
    }
}
