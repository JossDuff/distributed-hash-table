//use paxos_commit::whatever;
mod config;
mod db;
mod handlers;
mod messages;
mod net;

use anyhow::{anyhow, Result};
pub use config::Config;
use db::StripedDb;
use handlers::{
    handle_local_get, handle_local_put, handle_local_triput, handle_peer_commit,
    handle_peer_prepare,
};
pub use messages::{LocalMessage, PeerMessage};
use net::{connect_all, Peers};
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::DefaultHasher, HashMap, HashSet},
    fmt::{self, Debug},
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::OwnedMutexGuard;
use tokio::sync::{mpsc, Mutex, Notify};
use tracing::{debug, error, info, warn};

const CHANNEL_BUFFER_SIZE: usize = 64;
pub(crate) const COORDINATOR_VOTE_TIMEOUT: Duration = Duration::from_millis(200);

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct KVPair<K, V>
where
    V: Clone,
    K: Clone,
{
    pub key: K,
    pub val: V,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct NodeId {
    sunlab_name: String,
    id: usize,
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}_{}", self.sunlab_name, self.id)
    }
}

pub(crate) struct PendingTx<K: Clone, V: Clone> {
    pub(crate) pairs: Vec<(KVPair<K, V>, usize)>, // (pair, stripe_index)
    pub(crate) guards: HashMap<usize, OwnedMutexGuard<HashMap<K, (V, u64)>>>,
    pub(crate) version: u64,
}

// Shared state accessible by both tasks: local message handler and peer message handler
pub(crate) struct Shared<K: Clone, V: Clone> {
    pub(crate) senders: HashMap<NodeId, mpsc::Sender<PeerMessage<K, V>>>,
    pub(crate) cluster: Vec<NodeId>,
    pub(crate) my_node_id: NodeId,
    pub(crate) replication_degree: usize,
    pub(crate) db: StripedDb<K, V>,
    // Lamport clock for write versioning
    pub(crate) version_counter: AtomicU64,
    // Quorum read response collection
    pub(crate) awaiting_quorum_get: Arc<Mutex<HashMap<u64, mpsc::Sender<(Option<V>, u64)>>>>,
    // Paxos-Commit vote collection (votes carry sender identity for per-key quorum checking)
    pub(crate) awaiting_votes: Arc<Mutex<HashMap<u64, mpsc::Sender<(NodeId, bool)>>>>,
    pub(crate) pending_prepares: Arc<Mutex<HashMap<u64, PendingTx<K, V>>>>,
    // Crash detection
    pub(crate) alive_nodes: Arc<Mutex<HashSet<NodeId>>>,
    pub(crate) last_pong: Arc<Mutex<HashMap<NodeId, Instant>>>,
    pub(crate) done_count: AtomicUsize,
    // signaled when done_count reaches cluster.len()
    pub(crate) shutdown: Notify,
}

impl<K, V> Shared<K, V>
where
    K: Send
        + Sync
        + 'static
        + Debug
        + Serialize
        + for<'de> Deserialize<'de>
        + Hash
        + Eq
        + PartialEq
        + Clone
        + Copy,
    V: Send + Sync + 'static + Debug + Serialize + for<'de> Deserialize<'de> + Clone,
{
    // Increment done_count and signal shutdown if all nodes are done.
    pub(crate) fn mark_done(&self) {
        let count = self.done_count.fetch_add(1, Ordering::SeqCst) + 1;
        if count >= self.cluster.len() {
            self.shutdown.notify_waiters();
        }
    }

    pub(crate) fn get_key_replicas(&self, key: &K) -> Vec<&NodeId> {
        key_replica_indices(key, self.cluster.len(), self.replication_degree)
            .into_iter()
            .map(|i| &self.cluster[i])
            .collect()
    }

    /// Assign a new version for a write operation (Lamport clock).
    pub(crate) fn next_version(&self) -> u64 {
        self.version_counter.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Update local Lamport clock on receiving a remote version.
    pub(crate) fn observe_version(&self, remote_version: u64) {
        self.version_counter.fetch_max(remote_version, Ordering::SeqCst);
    }

    /// Quorum size for this replication degree (strict majority).
    pub(crate) fn quorum_size(&self) -> usize {
        self.replication_degree / 2 + 1
    }

    /// Get alive replicas for a key.
    pub(crate) async fn get_alive_replicas(&self, key: &K) -> Vec<NodeId> {
        let all_replicas = self.get_key_replicas(key);
        let alive = self.alive_nodes.lock().await;
        all_replicas
            .into_iter()
            .filter(|r| alive.contains(r))
            .cloned()
            .collect()
    }

    // Send a message to a peer.  Awaits until the bounded channel has space.
    // This is fine because each task has its own receiver — a blocked send
    // here can never prevent the *other* task from draining its inbox.
    pub(crate) async fn send_to_peer(&self, target: &NodeId, msg: PeerMessage<K, V>) -> Result<()> {
        if let Some(sender) = self.senders.get(target) {
            sender
                .send(msg)
                .await
                .map_err(|_| anyhow!("Channel to {} closed", target))?;
        }
        Ok(())
    }
}

pub struct Node<K, V>
where
    V: Clone,
    K: Clone,
{
    shared: Arc<Shared<K, V>>,
    local_inbox: mpsc::Receiver<LocalMessage<K, V>>,
    peer_inbox: mpsc::Receiver<(NodeId, PeerMessage<K, V>)>,
}

impl<K, V> Node<K, V>
where
    K: Send
        + Sync
        + 'static
        + Debug
        + Serialize
        + for<'de> Deserialize<'de>
        + Hash
        + Eq
        + PartialEq
        + Clone
        + Copy,
    V: Send + Sync + 'static + Debug + Serialize + for<'de> Deserialize<'de> + Clone,
{
    pub async fn new(
        config: Config,
        net_handle: &tokio::runtime::Handle,
    ) -> Result<(Self, mpsc::Sender<LocalMessage<K, V>>)> {
        let db = StripedDb::new(config.stripes);
        let pending_prepares: Arc<Mutex<HashMap<u64, PendingTx<K, V>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let awaiting_votes: Arc<Mutex<HashMap<u64, mpsc::Sender<(NodeId, bool)>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let awaiting_quorum_get: Arc<Mutex<HashMap<u64, mpsc::Sender<(Option<V>, u64)>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // this is a barrier
        let (peers, cluster, my_node_id): (Peers<K, V>, Vec<NodeId>, NodeId) =
            connect_all::<K, V>(&config.name, &config.connections, net_handle).await?;

        // for sending/ receiving messages from the test harness
        let (local_sender, local_inbox) = mpsc::channel(CHANNEL_BUFFER_SIZE);

        // Destructure Peers so we can give the inbox to one task and
        // the senders map to the shared state.
        let peer_inbox = peers.inbox;
        let senders = peers.senders;

        // Initialize alive_nodes with all cluster members
        let alive_nodes: HashSet<NodeId> = cluster.iter().cloned().collect();
        // Initialize last_pong timestamps for all peers
        let now = Instant::now();
        let last_pong: HashMap<NodeId, Instant> = cluster
            .iter()
            .filter(|n| **n != my_node_id)
            .map(|n| (n.clone(), now))
            .collect();

        let shared = Arc::new(Shared {
            senders,
            cluster,
            my_node_id,
            replication_degree: config.repication_degree,
            db,
            version_counter: AtomicU64::new(0),
            awaiting_quorum_get,
            awaiting_votes,
            pending_prepares,
            alive_nodes: Arc::new(Mutex::new(alive_nodes)),
            last_pong: Arc::new(Mutex::new(last_pong)),
            done_count: AtomicUsize::new(0),
            shutdown: Notify::new(),
        });

        Ok((
            Self {
                shared,
                local_inbox,
                peer_inbox,
            },
            local_sender,
        ))
    }

    // Split into two independent tasks, each with its own receiver.
    pub async fn run(self) -> Result<()> {
        let shared_local = self.shared.clone();
        let shared_peer = self.shared.clone();
        let shutdown = self.shared.clone();

        let local_handle = tokio::spawn(run_local_loop(shared_local, self.local_inbox));
        let peer_handle = tokio::spawn(run_peer_loop(shared_peer, self.peer_inbox));

        // Wait for shutdown signal (done_count >= cluster.len())
        shutdown.shutdown.notified().await;
        info!("All peers done, shutting down");

        // Cancel both loops.  We don't strictly need to abort
        local_handle.abort();
        peer_handle.abort();

        // Swallow JoinErrors from the aborts
        let _ = local_handle.await;
        let _ = peer_handle.await;

        Ok(())
    }
}

async fn run_local_loop<K, V>(
    s: Arc<Shared<K, V>>,
    mut inbox: mpsc::Receiver<LocalMessage<K, V>>,
) -> Result<()>
where
    K: Send
        + Sync
        + 'static
        + Debug
        + Serialize
        + for<'de> Deserialize<'de>
        + Hash
        + Eq
        + PartialEq
        + Clone
        + Copy,
    V: Send + Sync + 'static + Debug + Serialize + for<'de> Deserialize<'de> + Clone,
{
    while let Some(msg) = inbox.recv().await {
        debug!("Got {:?}", msg);
        match msg {
            LocalMessage::Get {
                key,
                response_sender,
            } => {
                handle_local_get(&s, key, response_sender).await?;
            }
            LocalMessage::Put {
                pair,
                response_sender,
            } => {
                handle_local_put(&s, pair, response_sender).await?;
            }
            // This node is the coordinator for the TRIPUT
            LocalMessage::TriPut {
                pairs,
                response_sender,
            } => {
                handle_local_triput(&s, pairs, response_sender).await;
            }
            LocalMessage::Done => {
                info!("I am done with my tests, notifying peers");
                let my_node_id = s.my_node_id.clone();
                for (node_id, sender) in s.senders.iter() {
                    if *node_id != my_node_id {
                        let _ = sender.send(PeerMessage::Done).await;
                    }
                }
                s.mark_done();
            }
        }
    }

    Ok(())
}

async fn run_peer_loop<K, V>(
    s: Arc<Shared<K, V>>,
    mut inbox: mpsc::Receiver<(NodeId, PeerMessage<K, V>)>,
) -> Result<()>
where
    K: Send
        + Sync
        + 'static
        + Debug
        + Serialize
        + for<'de> Deserialize<'de>
        + Hash
        + Eq
        + PartialEq
        + Clone
        + Copy,
    V: Send + Sync + 'static + Debug + Serialize + for<'de> Deserialize<'de> + Clone,
{
    while let Some((from, msg)) = inbox.recv().await {
        debug!("Got {:?} from {}", msg, from);
        match msg {
            // === Quorum Read ===
            PeerMessage::QuorumGet { key, req_id } => {
                let db = s.db.clone();
                let senders = s.senders.clone();
                tokio::spawn(async move {
                    let (val, version) = match db.get(&key).await {
                        Some((v, ver)) => (Some(v), ver),
                        None => (None, 0),
                    };
                    let resp: PeerMessage<K, V> =
                        PeerMessage::QuorumGetResponse { val, version, req_id };
                    if let Some(sender) = senders.get(&from) {
                        let _ = sender.send(resp).await;
                    }
                });
            }

            PeerMessage::QuorumGetResponse { val, version, req_id } => {
                debug!("QuorumGetResponse for req {} with version {}", req_id, version);
                let awaiting = s.awaiting_quorum_get.lock().await;
                if let Some(tx) = awaiting.get(&req_id) {
                    let _ = tx.send((val, version)).await;
                } else {
                    error!("QuorumGetResponse for unknown req_id {}", req_id);
                }
            }

            // === Paxos-Commit ===
            PeerMessage::Prepare { pairs, tx_id, version } => {
                handle_peer_prepare(&s, from, pairs, tx_id, version);
            }

            PeerMessage::VotePrepared { tx_id } => {
                debug!("VotePrepared from {} for tx {}", from, tx_id);
                let tx = {
                    debug!("VotePrepared: getting lock on awaiting_votes for tx {tx_id}...");
                    let awaiting = s.awaiting_votes.lock().await;
                    debug!("VotePrepared: got lock on awaiting_votes for tx {tx_id}");
                    awaiting.get(&tx_id).cloned()
                };
                if let Some(tx) = tx {
                    let _ = tx.send((from, true)).await;
                } else {
                    warn!("VotePrepared: No sender for tx {tx_id}");
                }
            }
            PeerMessage::VoteAbort { tx_id } => {
                debug!("VoteAbort from {} for tx {}", from, tx_id);
                let tx = {
                    debug!("VoteAbort: getting lock on awaiting_votes for tx {tx_id}...");
                    let awaiting = s.awaiting_votes.lock().await;
                    debug!("VoteAbort: got lock on awaiting_votes for tx {tx_id}");
                    awaiting.get(&tx_id).cloned()
                };
                if let Some(tx) = tx {
                    let _ = tx.send((from, false)).await;
                } else {
                    warn!("VoteAbort: No sender for tx {tx_id}");
                }
            }

            PeerMessage::Commit { tx_id } => {
                handle_peer_commit(&s, tx_id);
            }

            PeerMessage::Abort { tx_id } => {
                debug!("Received Abort for tx {tx_id}, removing from pending_prepares...");
                s.pending_prepares.lock().await.remove(&tx_id);
                debug!("Successfully removed tx {tx_id} from pending_prepares");
            }

            // === Crash Detection ===
            PeerMessage::Ping => {
                let _ = s.send_to_peer(&from, PeerMessage::Pong).await;
            }
            PeerMessage::Pong => {
                s.last_pong.lock().await.insert(from, Instant::now());
            }

            // === Shutdown ===
            PeerMessage::Done => {
                info!("{} is done with their test", from);
                s.mark_done();
            }
        }
    }

    Ok(())
}

// Pure function: given a sorted cluster and replication degree, return the
// indices of nodes that own this key.
pub(crate) fn key_replica_indices<K: Hash>(
    key: &K,
    cluster_len: usize,
    replication_degree: usize,
) -> Vec<usize> {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let start = (hasher.finish() as usize) % cluster_len;
    let degree = replication_degree.min(cluster_len);

    (0..degree).map(|i| (start + i) % cluster_len).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cluster(n: usize) -> Vec<NodeId> {
        let names = [
            "ariel", "caliban", "callisto", "ceres", "chiron", "cupid", "eris", "europa", "hydra",
            "iapetus",
        ];
        (0..n)
            .map(|i| NodeId {
                sunlab_name: names[i].to_string(),
                id: i,
            })
            .collect()
    }

    #[test]
    fn single_replica_returns_one_node() {
        let cluster = make_cluster(5);
        let key: u64 = 42;
        let replicas = key_replica_indices(&key, cluster.len(), 1);
        assert_eq!(replicas.len(), 1);
        assert!(replicas[0] < cluster.len());
    }

    #[test]
    fn replication_degree_returns_correct_count() {
        let cluster = make_cluster(5);
        let key: u64 = 42;

        for degree in 1..=5 {
            let replicas = key_replica_indices(&key, cluster.len(), degree);
            assert_eq!(replicas.len(), degree);
        }
    }

    #[test]
    fn no_duplicate_replicas() {
        let cluster = make_cluster(5);
        let key: u64 = 99;
        let replicas = key_replica_indices(&key, cluster.len(), 5);

        let mut unique = replicas.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), replicas.len());
    }

    #[test]
    fn replicas_are_contiguous_on_ring() {
        let cluster = make_cluster(5);
        let key: u64 = 7;
        let replicas = key_replica_indices(&key, cluster.len(), 3);

        for i in 1..replicas.len() {
            assert_eq!(replicas[i], (replicas[i - 1] + 1) % cluster.len());
        }
    }

    #[test]
    fn degree_capped_at_cluster_size() {
        let cluster = make_cluster(3);
        let key: u64 = 55;
        let replicas = key_replica_indices(&key, cluster.len(), 10);
        assert_eq!(replicas.len(), 3);
    }

    #[test]
    fn same_key_same_replicas() {
        let cluster = make_cluster(5);
        let key: u64 = 123;
        let a = key_replica_indices(&key, cluster.len(), 2);
        let b = key_replica_indices(&key, cluster.len(), 2);
        assert_eq!(a, b);
    }

    #[test]
    fn different_keys_distribute() {
        let cluster = make_cluster(5);
        let mut primary_counts = vec![0usize; cluster.len()];

        for key in 0u64..1000 {
            let replicas = key_replica_indices(&key, cluster.len(), 1);
            primary_counts[replicas[0]] += 1;
        }

        for (i, count) in primary_counts.iter().enumerate() {
            assert!(
                *count > 50,
                "node {} only got {} keys out of 1000, distribution looks broken",
                i,
                count
            );
        }
    }

    #[test]
    fn wraps_around_ring() {
        let cluster = make_cluster(5);
        let key = (0u64..10000)
            .find(|k| {
                let replicas = key_replica_indices(k, cluster.len(), 1);
                replicas[0] == cluster.len() - 1
            })
            .expect("should find a key mapping to last node");

        let replicas = key_replica_indices(&key, cluster.len(), 3);
        assert_eq!(replicas[0], cluster.len() - 1);
        assert_eq!(replicas[1], 0);
        assert_eq!(replicas[2], 1);
    }
}
