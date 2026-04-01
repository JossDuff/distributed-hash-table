use crate::KVPair;
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
    // Coordinator asks a replica to prepare for writing (acquire stripe locks)
    Prepare {
        pairs: Vec<KVPair<K, V>>,
        tx_id: u64,
        version: u64,
    },
    // Replica votes yes (locks acquired, ready to commit)
    VotePrepared { tx_id: u64 },
    // Replica votes no (cannot acquire locks)
    VoteAbort { tx_id: u64 },
    // Coordinator tells replicas to commit (quorum achieved)
    Commit { tx_id: u64 },
    // Coordinator tells replicas to abort
    Abort { tx_id: u64 },

    // === Crash Detection ===
    Ping,
    Pong,

    // This node has finished its tests
    Done,
}
