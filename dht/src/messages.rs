use crate::{KVPair, NodeId};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tokio::sync::oneshot;

#[derive(Debug)]
pub enum LocalMessage<K, V>
where
    V: Clone,
    K: Clone,
{
    Get {
        key: K,
        response_sender: oneshot::Sender<Option<V>>,
    },
    Put {
        pair: KVPair<K, V>,
        response_sender: oneshot::Sender<bool>,
    },
    TriPut {
        pairs: [KVPair<K, V>; 3],
        response_sender: oneshot::Sender<bool>,
    },
    // message from test harness when all tests finish
    Done,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PeerMessage<K, V>
where
    V: Clone,
    K: Clone,
{
    // === Quorum Read Protocol ===
    // Coordinator asks a replica to read a key
    QuorumGet { key: K, req_id: u64 },
    // Replica responds with its local value + version
    QuorumGetResponse {
        val: Option<V>,
        version: u64,
        req_id: u64,
    },

    // === Paxos-Commit Write Protocol ===
    // Coordinator asks a replica to prepare (acquire stripe locks)
    Prepare {
        pairs: Vec<KVPair<K, V>>,
        tx_id: u64,
        version: u64,
    },
    // RM sends vote to ALL nodes (phase 2a)
    Vote {
        tx_id: u64,
        rm_id: NodeId,
        vote: bool,
    },
    // Acceptor confirms vote to ALL nodes (phase 2b)
    Accepted {
        tx_id: u64,
        rm_id: NodeId,
        vote: bool,
    },

    // === Crash Detection ===
    Ping,
    Pong,

    // This node has finished its tests
    Done,
}
