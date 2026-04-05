use crate::{KVPair, NodeId};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tokio::sync::oneshot;

// Handshake sent by a peer node on connection.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadyPeerMessage(pub NodeId);

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

// === Client-Node Protocol (over TCP) ===

// Handshake sent by a client to identify itself (distinct from ReadyPeerMessage).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientHello {
    pub client_name: String,
}

// Request from client to node.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientMessage<K, V>
where
    K: Clone,
    V: Clone,
{
    Get { key: K, req_id: u64 },
    Put { pair: KVPair<K, V>, req_id: u64 },
    TriPut { pairs: [KVPair<K, V>; 3], req_id: u64 },
    Done,
}

// Response from node back to client.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientResponse<V>
where
    V: Clone,
{
    GetResult { val: Option<V>, req_id: u64 },
    WriteResult { success: bool, req_id: u64 },
}

// First message on any inbound TCP connection.
// The listener deserializes this to decide peer vs client handling.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FirstMessage {
    Peer(ReadyPeerMessage),
    Client(ClientHello),
}
