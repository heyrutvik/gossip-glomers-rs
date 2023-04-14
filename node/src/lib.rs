use crate::core::{Message, Node};
use std::io::{stdin, stdout, Stdin, Stdout, Write};

pub mod core;
pub mod helper;

pub struct Runner {
    node: Node,
    stdin: Stdin,
    stdout: Stdout,
}

impl Runner {
    pub fn new(node: Node) -> Self {
        Self {
            node,
            stdin: stdin(),
            stdout: stdout(),
        }
    }

    pub fn start(&mut self) {
        let mut buffer = String::new();
        while let Ok(_) = self.stdin.read_line(&mut buffer) {
            let reply = serde_json::from_str::<Message>(buffer.trim_end())
                .map_err(|error| error.into())
                .and_then(|message| self.node.process(message))
                .map(|replies| replies.iter().for_each(|reply| self.write(reply)));

            if reply.is_err() {
                let e = reply.unwrap_err();
                eprintln!("{e}");
            }
            buffer.clear();
        }
    }

    fn write(&mut self, message: &Message) {
        let reply =
            serde_json::to_string(message).expect("Interpreter should serialize the message.");
        let mut lock = self.stdout.lock();
        writeln!(lock, "{}", reply).expect("A message should be written to STDOUT.");
    }
}
