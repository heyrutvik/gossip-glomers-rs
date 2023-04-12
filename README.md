# Gossip Glomers in Rust

A series of distributed systems challenges brought to you by [Fly.io](https://fly.io/dist-sys/).

0. [Maelstrom node implementation](node/README.md)
1. [Echo challenge](echo/README.md)

### How to run?
1. [Install](https://github.com/jepsen-io/maelstrom/blob/main/doc/01-getting-ready/index.md#installation) Maelstrom
2. `cargo build --release`
3. `./maelstrom test -w echo --bin target/release/echo --node-count 1 --time-limit 10`, and it should print something like: "Everything looks good! ヽ(‘ー`)ノ"