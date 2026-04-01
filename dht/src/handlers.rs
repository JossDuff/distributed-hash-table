use crate::{KVPair, NodeId, PeerMessage, PendingTx, Shared};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug, hash::Hash, sync::Arc};
use tokio::sync::{oneshot, OwnedMutexGuard};
use tracing::debug;

// Temporary: reads local DB only. Will be rewritten for quorum reads in Phase 5.
pub(crate) async fn handle_local_get<K, V>(
    s: &Arc<Shared<K, V>>,
    key: K,
    response_sender: oneshot::Sender<Option<V>>,
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
    let db = s.db.clone();
    debug!("handle_local_get: reading key {:?} from local DB", key);
    tokio::spawn(async move {
        let resp = db.get(&key).await.map(|(v, _)| v);
        debug!("handle_local_get: key {:?} result: {:?}", key, resp);
        let _ = response_sender.send(resp);
    });
    Ok(())
}

// Temporary: writes local DB only. Will be rewritten for Paxos-Commit in Phase 5.
pub(crate) async fn handle_local_put<K, V>(
    s: &Arc<Shared<K, V>>,
    pair: KVPair<K, V>,
    response_sender: oneshot::Sender<bool>,
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
    let db = s.db.clone();
    let version = s.next_version();
    debug!(
        "handle_local_put: writing key {:?} with version {}",
        pair.key, version
    );
    tokio::spawn(async move {
        db.put(pair.key, pair.val, version).await;
        let _ = response_sender.send(true);
    });
    Ok(())
}

// Temporary: writes to local DB only. Will be rewritten for Paxos-Commit in Phase 5.
pub(crate) async fn handle_local_triput<K, V>(
    s: &Arc<Shared<K, V>>,
    pairs: [KVPair<K, V>; 3],
    response_sender: oneshot::Sender<bool>,
) where
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
    let db = s.db.clone();
    let version = s.next_version();
    debug!(
        "handle_local_triput: writing 3 pairs with version {}",
        version
    );
    tokio::spawn(async move {
        for pair in pairs {
            db.put(pair.key, pair.val, version).await;
        }
        let _ = response_sender.send(true);
    });
}

/// RM role: receive Prepare from coordinator, try to acquire locks, broadcast Vote.
pub(crate) fn handle_peer_prepare<K, V>(
    s: &Arc<Shared<K, V>>,
    from: NodeId,
    pairs: Vec<KVPair<K, V>>,
    tx_id: u64,
    version: u64,
) where
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
    let db = s.db.clone();
    let pending_prepares = s.pending_prepares.clone();
    let senders = s.senders.clone();
    let my_node_id = s.my_node_id.clone();
    let acceptor_log = s.acceptor_log.clone();
    tokio::spawn(async move {
        debug!(
            "peer_prepare: received Prepare for tx {} from {}",
            tx_id, from
        );
        let mut entries: Vec<(KVPair<K, V>, usize)> = pairs
            .iter()
            .map(|p| (p.clone(), db.stripe_index(&p.key)))
            .collect();
        entries.sort_by_key(|(_, idx)| *idx);

        let mut guards: HashMap<usize, OwnedMutexGuard<HashMap<K, (V, u64)>>> = HashMap::new();
        let mut lock_failed = false;
        for (_, idx) in &entries {
            if !guards.contains_key(idx) {
                let stripe = db.get_stripe_by_index(*idx);
                match stripe.try_lock_owned() {
                    Ok(guard) => {
                        debug!("peer_prepare: locked stripe {} for tx {}", idx, tx_id);
                        guards.insert(*idx, guard);
                    }
                    Err(_) => {
                        debug!(
                            "peer_prepare: try_lock failed for tx {} on stripe {}",
                            tx_id, idx
                        );
                        lock_failed = true;
                        break;
                    }
                }
            }
        }

        let vote = !lock_failed;

        if vote {
            debug!("peer_prepare: getting lock on pending_prepares for tx {tx_id}");
            pending_prepares.lock().await.insert(
                tx_id,
                PendingTx {
                    pairs: entries,
                    guards,
                    version,
                },
            );
            debug!("peer_prepare: added tx {tx_id} to pending_prepares");
        }

        // Self-accept (local acceptor role for own vote)
        acceptor_log
            .lock()
            .await
            .insert((tx_id, my_node_id.clone()), vote);

        // Send Vote (phase 2a) + self-Accepted (phase 2b) to all other nodes
        debug!(
            "peer_prepare: broadcasting Vote(vote={}) for tx {} to all nodes",
            vote, tx_id
        );
        for (node_id, sender) in senders.iter() {
            if *node_id != my_node_id {
                let _ = sender
                    .send(PeerMessage::Vote {
                        tx_id,
                        rm_id: my_node_id.clone(),
                        vote,
                    })
                    .await;
                let _ = sender
                    .send(PeerMessage::Accepted {
                        tx_id,
                        rm_id: my_node_id.clone(),
                        vote,
                    })
                    .await;
            }
        }
    });
}
