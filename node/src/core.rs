use std::collections::HashMap;
use std::num::Wrapping;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::helper::{Error, Result};
use serde::{Deserialize, Serialize};

pub type NodeId = String;
pub type MessageId = u32;
pub type CodeId = u32;
pub type Handler = fn(&mut Node, Message) -> Result<Vec<Message>>;
pub type BroadcastMessage = u64;

pub struct Node {
    node_id: Option<NodeId>,
    node_ids: Option<Vec<NodeId>>,
    handlers: HashMap<Type, Handler>,

    msg_counter: u32,
    uid_counter: Wrapping<u8>,
    broadcast_messages: Vec<BroadcastMessage>,
}

impl Node {
    pub fn new(mut handlers: HashMap<Type, Handler>) -> Self {
        handlers
            .entry(Type::Init)
            .or_insert(Self::handler_init as Handler);
        Self {
            handlers,
            node_id: None,
            node_ids: None,
            msg_counter: 0,
            uid_counter: Wrapping::default(),
            broadcast_messages: Vec::new(),
        }
    }

    pub fn gen_msg_id(&mut self) -> MessageId {
        self.msg_counter += 1;
        self.msg_counter
    }

    pub fn reply(&self, dest: NodeId, body: Workload) -> Message {
        Message {
            src: self.node_id(),
            dest,
            body,
        }
    }

    pub fn process(&mut self, message: Message) -> Result<Vec<Message>> {
        message.body.key().and_then(|key| {
            if !self.is_initialized() && key != Type::Init {
                return Err(Box::new(Error::NotInitializedYet));
            }

            // workaround to let the handler take "self".
            match self.handlers.get(&key) {
                Some(handler) => handler(self, message),
                None if self.is_initialized() && key == Type::Init => {
                    Err(Box::new(Error::AlreadyInitialized))
                }
                None => Err(Box::new(Error::HandlerNotFound { key })),
            }
        })
    }

    // will return empty node_id if node is not initialized.
    pub fn node_id(&self) -> NodeId {
        self.node_id.clone().unwrap_or(String::new())
    }

    pub fn gen_unique_id(&mut self) -> String {
        let now = SystemTime::now();
        let epoch = now
            .duration_since(UNIX_EPOCH)
            .expect("Unique id: should be able to get unix epoch.")
            .as_millis() as u64;
        let part1 = (epoch << 30) >> 7; // 48 bit epoch

        let node_id_mask = 0x000000000000FF00;
        let node_id: u64 = self
            .node_id()
            .chars()
            .skip(1)
            .collect::<String>()
            .parse()
            .unwrap();
        let part2 = (node_id << 8) & node_id_mask; // 8 bit node id

        self.uid_counter += 1;
        let part3 = self.uid_counter.0 as u64; // 8 bit unique id counter (wrapped)

        let unique_id = part1 | part2 | part3;
        unique_id.to_string()
    }

    pub fn push_broadcast_message(&mut self, message: BroadcastMessage) {
        self.broadcast_messages.push(message);
    }

    pub fn broadcast_messages(&self) -> Vec<BroadcastMessage> {
        self.broadcast_messages.clone()
    }

    fn is_initialized(&self) -> bool {
        self.node_id.is_some() && self.node_ids.is_some()
    }

    // once initialized, remove "init" handler,
    // so that node can log error on receiving "init" message again!
    fn init(&mut self, node_id: NodeId, node_ids: Vec<NodeId>) {
        self.node_id = Some(node_id);
        self.node_ids = Some(node_ids);
        self.handlers.remove(&Type::Init);
    }

    fn handler_init(node: &mut Node, message: Message) -> Result<Vec<Message>> {
        match message.body {
            Workload::Init {
                msg_id,
                node_id,
                node_ids,
            } => {
                node.init(node_id, node_ids);
                let reply = node.reply(message.src, Workload::init_ok(msg_id));
                Ok(vec![reply])
            }
            _ => Err(Box::new(Error::ExpectedMessage {
                found: message.body.key().unwrap_or(Type::Invalid),
                expected: Type::Init,
            })),
        }
    }
}

impl Default for Node {
    fn default() -> Self {
        let handlers = HashMap::new();
        Node::new(handlers)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Message {
    pub src: NodeId,
    pub dest: NodeId,
    pub body: Workload,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Workload {
    Init {
        msg_id: MessageId,
        node_id: NodeId,
        node_ids: Vec<NodeId>,
    },
    InitOk {
        in_reply_to: MessageId,
    },
    Error {
        in_reply_to: MessageId,
        code: CodeId,
        text: String,
    },
    Echo {
        msg_id: MessageId,
        echo: String,
    },
    EchoOk {
        in_reply_to: MessageId,
        msg_id: MessageId,
        echo: String,
    },
    Generate {
        msg_id: MessageId,
    },
    GenerateOk {
        in_reply_to: MessageId,
        msg_id: MessageId,
        id: String,
    },
    Broadcast {
        msg_id: MessageId,
        message: BroadcastMessage,
    },
    BroadcastOk {
        in_reply_to: MessageId,
        msg_id: MessageId,
    },
    Read {
        msg_id: MessageId,
    },
    ReadOk {
        in_reply_to: MessageId,
        msg_id: MessageId,
        messages: Vec<BroadcastMessage>,
    },
    Topology {
        msg_id: MessageId,
        topology: HashMap<NodeId, Vec<NodeId>>,
    },
    TopologyOk {
        in_reply_to: MessageId,
        msg_id: MessageId,
    },
}

impl Workload {
    pub fn key(&self) -> Result<Type> {
        match self {
            Workload::Init { .. } => Ok(Type::Init),
            Workload::Echo { .. } => Ok(Type::Echo),
            Workload::Generate { .. } => Ok(Type::Generate),
            Workload::Broadcast { .. } => Ok(Type::Broadcast),
            Workload::Read { .. } => Ok(Type::Read),
            Workload::Topology { .. } => Ok(Type::Topology),
            _ => Err(Box::new(Error::KeyNotFound)),
        }
    }

    pub fn echo_ok(in_reply_to: MessageId, msg_id: MessageId, echo: String) -> Workload {
        Workload::EchoOk {
            in_reply_to,
            msg_id,
            echo,
        }
    }

    pub fn generate_ok(in_reply_to: MessageId, msg_id: MessageId, id: String) -> Workload {
        Workload::GenerateOk {
            in_reply_to,
            msg_id,
            id,
        }
    }

    pub fn broadcast_ok(in_reply_to: MessageId, msg_id: MessageId) -> Workload {
        Workload::BroadcastOk {
            in_reply_to,
            msg_id,
        }
    }

    pub fn read_ok(
        in_reply_to: MessageId,
        msg_id: MessageId,
        messages: Vec<BroadcastMessage>,
    ) -> Workload {
        Workload::ReadOk {
            in_reply_to,
            msg_id,
            messages,
        }
    }

    pub fn topology_ok(in_reply_to: MessageId, msg_id: MessageId) -> Workload {
        Workload::TopologyOk {
            in_reply_to,
            msg_id,
        }
    }

    fn init_ok(in_reply_to: MessageId) -> Workload {
        Workload::InitOk { in_reply_to }
    }
}

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub enum Type {
    Init,
    Echo,
    Generate,
    Broadcast,
    Read,
    Topology,

    Invalid, // received key is either not listed or missing in the message.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_init() {
        let mut node = Node::default();
        let message = serde_json::from_str::<Message>(
            r#"{"src":"c1","dest":"n1","body":{"type":"init","msg_id":1,"node_id":"n1","node_ids":["n1","n2","n3"]}}"#,
        )
        .unwrap();
        let reply = node.process(message);
        assert!(reply.is_ok());

        let reply = serde_json::to_string(&reply.unwrap().first().unwrap()).unwrap();
        assert_eq!(
            reply,
            r#"{"src":"n1","dest":"c1","body":{"type":"init_ok","in_reply_to":1}}"#
        );
    }

    #[test]
    fn test_node_not_init() {
        let mut node = Node::default();
        let json =
            r#"{"src":"c1","dest":"n2","body":{"type":"echo","echo":"Hello, World!","msg_id":1}}"#;
        let message = serde_json::from_str::<Message>(json).unwrap();
        let reply = node.process(message); // received valid message but node is not initialized yet.
        assert!(reply.is_err());
        assert_eq!(
            reply.err().unwrap().to_string(),
            Box::new(Error::NotInitializedYet).to_string()
        );
    }

    #[test]
    fn test_node_fail_reini() {
        let mut node = Node::default();
        let json = r#"{"src":"c1","dest":"n1","body":{"type":"init","msg_id":1,"node_id":"n1","node_ids":["n1","n2","n3"]}}"#;
        let message = serde_json::from_str::<Message>(json).unwrap();

        let _ = node.process(message.clone()); // initialized.
        let reply = node.process(message); // receiving "init" again!
        assert!(reply.is_err());
        assert_eq!(
            reply.err().unwrap().to_string(),
            Box::new(Error::AlreadyInitialized).to_string()
        );
    }

    // TODO test unique id generator
}
