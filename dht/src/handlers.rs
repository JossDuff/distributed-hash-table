use crate::{KVPair, NodeId, PeerMessage, PendingTx, Shared, COORDINATOR_VOTE_TIMEOUT};
use anyhow::Result;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug, hash::Hash, sync::Arc};
use tokio::sync::{mpsc, oneshot, OwnedMutexGuard};
use tracing::{debug, error, warn};

// Temporary: reads local DB only. Will be rewritten for quorum reads in Phase 6.
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

// Temporary: writes local DB only. Will be rewritten for Paxos-Commit in Phase 6.
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
    debug!("handle_local_put: writing key {:?} with version {}", pair.key, version);
    tokio::spawn(async move {
        db.put(pair.key, pair.val, version).await;
        let _ = response_sender.send(true);
    });
    Ok(())
}

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
    let tx_id: u64 = rand::rng().random();
    let version = s.next_version();

    // Group pairs by primary, precompute stripe indices
    let mut primary_to_entries: HashMap<NodeId, Vec<(KVPair<K, V>, usize)>> = HashMap::new();
    for pair in pairs {
        let primary = s.get_key_replicas(&pair.key)[0].clone();
        let stripe_idx = s.db.stripe_index(&pair.key);
        primary_to_entries
            .entry(primary)
            .or_default()
            .push((pair, stripe_idx));
    }

    // Extract local entries
    let local_entries = primary_to_entries.remove(&s.my_node_id);

    // Compute remote primaries and build prepare messages
    let remote_primaries: Vec<NodeId> = primary_to_entries.keys().cloned().collect();
    let remote_prepare_msgs: Vec<(NodeId, PeerMessage<K, V>)> = primary_to_entries
        .iter()
        .map(|(primary, entries)| {
            let pairs_only: Vec<KVPair<K, V>> = entries.iter().map(|(p, _)| p.clone()).collect();
            (
                primary.clone(),
                PeerMessage::Prepare {
                    pairs: pairs_only,
                    tx_id,
                    version,
                },
            )
        })
        .collect();

    // Vote channel carries (NodeId, bool) for per-key quorum checking
    let num_remote = remote_primaries.len();
    let (vote_tx, mut vote_rx) = mpsc::channel(num_remote.max(1));

    if !remote_primaries.is_empty() {
        debug!("triput: getting lock on awaiting_votes for tx {tx_id}");
        s.awaiting_votes.lock().await.insert(tx_id, vote_tx);
        debug!("triput: added tx {} to awaiting_votes", tx_id);
    }

    let db = s.db.clone();
    let pending_prepares = s.pending_prepares.clone();
    let awaiting_votes = s.awaiting_votes.clone();
    let all_senders = s.senders.clone();

    tokio::spawn(async move {
        // Local prepare
        let local_voted = if let Some(mut entries) = local_entries {
            entries.sort_by_key(|(_, idx)| *idx);

            let mut guards: HashMap<usize, OwnedMutexGuard<HashMap<K, (V, u64)>>> = HashMap::new();
            let mut lock_failed = false;
            for (pair, idx) in &entries {
                if !guards.contains_key(idx) {
                    let stripe = db.get_stripe_by_index(*idx);
                    match stripe.try_lock_owned() {
                        Ok(guard) => {
                            debug!(
                                "Local prepare try_lock successful for stripe {}, key {:?}, tx {}",
                                idx, pair.key, tx_id
                            );
                            guards.insert(*idx, guard);
                        }
                        Err(_) => {
                            debug!(
                                "Local prepare try_lock failed for stripe {}, key {:?}, tx {}",
                                idx, pair.key, tx_id
                            );
                            lock_failed = true;
                            break;
                        }
                    }
                }
            }

            if lock_failed {
                false
            } else {
                debug!("triput: getting lock on pending_prepares for tx {tx_id}");
                pending_prepares.lock().await.insert(
                    tx_id,
                    PendingTx {
                        pairs: entries,
                        guards,
                        version,
                    },
                );
                debug!("triput: added tx {tx_id} to pending_prepares");
                true
            }
        } else {
            true // no local entries to prepare
        };

        // Send Prepare to remote primaries
        for (primary, msg) in &remote_prepare_msgs {
            debug!("triput: sending Prepare for tx {} to {}", tx_id, primary);
            if let Some(sender) = all_senders.get(primary) {
                let _ = sender.send(msg.clone()).await;
            }
        }

        // Collect remote votes
        let remote_ok = if num_remote == 0 {
            true
        } else {
            let collect_result = tokio::time::timeout(COORDINATOR_VOTE_TIMEOUT, async {
                for _ in 0..num_remote {
                    match vote_rx.recv().await {
                        Some((_, true)) => {}
                        _ => return false,
                    }
                }
                true
            })
            .await;
            if let Ok(true) = collect_result {
                debug!("triput: got enough remote votes for tx {}", tx_id);
                true
            } else {
                debug!("triput: did not get enough remote votes for tx {}", tx_id);
                false
            }
        };

        debug!("triput: getting lock on awaiting_votes to remove tx {tx_id}");
        awaiting_votes.lock().await.remove(&tx_id);
        debug!("triput: removed tx {} from awaiting_votes", tx_id);

        let all_prepared = local_voted && remote_ok;

        if all_prepared {
            debug!("triput: all prepared for tx {}, committing", tx_id);
            // Apply local changes
            {
                let mut pending = pending_prepares.lock().await;
                if let Some(mut tx) = pending.remove(&tx_id) {
                    debug!("triput: removed tx {} from pending_prepares", tx_id);
                    for (pair, stripe_idx) in &tx.pairs {
                        if let Some(guard) = tx.guards.get_mut(stripe_idx) {
                            debug!(
                                "triput: inserting key {:?}, val {:?} for tx {} with version {}",
                                pair.key, pair.val, tx_id, tx.version
                            );
                            guard.insert(pair.key, (pair.val.clone(), tx.version));
                        } else {
                            warn!("triput: stripe {} not found in guards for tx {}", stripe_idx, tx_id);
                        }
                    }
                }
            }

            // Send Commit to remote primaries
            for primary in &remote_primaries {
                debug!("triput: sending Commit for tx {} to {}", tx_id, primary);
                if let Some(sender) = all_senders.get(primary) {
                    let _ = sender.send(PeerMessage::Commit { tx_id }).await;
                }
            }

            let _ = response_sender.send(true);
        } else {
            debug!("triput: aborting tx {}", tx_id);
            pending_prepares.lock().await.remove(&tx_id);

            for primary in &remote_primaries {
                debug!("triput: sending Abort for tx {} to {}", tx_id, primary);
                if let Some(sender) = all_senders.get(primary) {
                    let _ = sender.send(PeerMessage::Abort { tx_id }).await;
                }
            }

            let _ = response_sender.send(false);
        }
    });
}

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
    tokio::spawn(async move {
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

        if lock_failed {
            debug!("peer_prepare: sending VoteAbort for tx {} to {}", tx_id, from);
            if let Some(sender) = senders.get(&from) {
                let _ = sender.send(PeerMessage::VoteAbort { tx_id }).await;
            }
        } else {
            debug!("peer_prepare: getting lock on pending_prepares for tx {tx_id}");
            pending_prepares.lock().await.insert(
                tx_id,
                PendingTx {
                    pairs: entries,
                    guards,
                    version,
                },
            );
            debug!("peer_prepare: added tx {tx_id} to pending_prepares, sending VotePrepared to {from}");

            if let Some(sender) = senders.get(&from) {
                let _ = sender.send(PeerMessage::VotePrepared { tx_id }).await;
            }
        }
    });
}

pub(crate) fn handle_peer_commit<K, V>(s: &Arc<Shared<K, V>>, tx_id: u64)
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
    debug!("peer_commit: attempting to commit tx {tx_id}");
    let pending_prepares = s.pending_prepares.clone();
    tokio::spawn(async move {
        debug!("peer_commit: getting lock on pending_prepares for tx {tx_id}...");
        let mut pending = pending_prepares.lock().await;
        debug!("peer_commit: got lock on pending_prepares for tx {tx_id}");
        if let Some(mut tx) = pending.remove(&tx_id) {
            for (pair, stripe_idx) in &tx.pairs {
                if let Some(guard) = tx.guards.get_mut(stripe_idx) {
                    debug!(
                        "peer_commit: inserting key {:?}, val {:?} for tx {} with version {}",
                        pair.key, pair.val, tx_id, tx.version
                    );
                    guard.insert(pair.key, (pair.val.clone(), tx.version));
                } else {
                    warn!("peer_commit: stripe {} not found in guards for tx {}", stripe_idx, tx_id);
                }
            }
        } else {
            error!("peer_commit: tx {} not found in pending_prepares", tx_id);
        }
    });
}
