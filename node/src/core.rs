use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::helper::{Error, Result};
use serde::{Deserialize, Serialize};

type NodeId = String;
type MessageId = u32;
type CodeId = u32;
pub type Handler = fn(&mut Node, Message) -> Result<Message>;

pub struct Node {
    counter: AtomicU32,
    node_id: Option<NodeId>,
    node_ids: Option<Vec<NodeId>>,
    handlers: HashMap<Type, Handler>,
}

impl Node {
    pub fn new(mut handlers: HashMap<Type, Handler>) -> Self {
        handlers
            .entry(Type::Init)
            .or_insert(Self::handler_init as Handler);
        Self {
            counter: AtomicU32::new(1),
            node_id: None,
            node_ids: None,
            handlers,
        }
    }

    pub fn gen_msg_id(&mut self) -> MessageId {
        self.counter.fetch_add(1, Ordering::SeqCst)
    }

    pub fn reply(&self, dest: NodeId, body: Workload) -> Message {
        Message {
            src: self.node_id(),
            dest,
            body,
        }
    }

    pub fn process(&mut self, message: Message) -> Result<Message> {
        message.body.key().and_then(|key| {
            if !self.is_initialized() && key != Type::Init {
                return Err(Box::new(Error::NotInitializedYet));
            }

            // workaround to let the handler take "self".
            match self.handlers.get(&key) {
                Some(handler) => handler(self, message),
                None if self.is_initialized() => Err(Box::new(Error::AlreadyInitialized)),
                None => Err(Box::new(Error::HandlerNotFound { key })),
            }
        })
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

    // will return empty node_id if node is not initialized.
    fn node_id(&self) -> NodeId {
        self.node_id.clone().unwrap_or(String::new())
    }

    fn handler_init(node: &mut Node, message: Message) -> Result<Message> {
        match message.body {
            Workload::Init {
                msg_id,
                node_id,
                node_ids,
            } => {
                node.init(node_id, node_ids);
                Ok(node.reply(message.src, Workload::init_ok(msg_id)))
            }
            _ => Err(Box::new(Error::ExpectedMessage {
                found: message.body.key().unwrap_or(Type::Undefined),
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
}

impl Workload {
    pub fn key(&self) -> Result<Type> {
        match self {
            Workload::Init { .. } => Ok(Type::Init),
            Workload::Echo { .. } => Ok(Type::Echo),
            _ => Err(Box::new(Error::KeyNotFound)),
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
    Undefined,
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

        let reply = serde_json::to_string(&reply.unwrap()).unwrap();
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
}
