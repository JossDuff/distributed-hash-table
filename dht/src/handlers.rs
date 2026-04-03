use crate::{
    KVPair, NodeId, PeerMessage, PendingTx, Shared, TxVoteTracker, PREPARE_LOCK_TIMEOUT,
    QUORUM_READ_TIMEOUT, VOTE_LEARN_TIMEOUT,
};
use anyhow::Result;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{collections::{HashMap, HashSet}, fmt::Debug, hash::Hash, sync::Arc};
use tokio::sync::{mpsc, oneshot, Mutex, Notify, OwnedMutexGuard};
use tracing::debug;

/// 5a. Quorum read: read from alive replicas, return highest-versioned value.
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
    let alive_replicas = s.get_alive_replicas(&key).await;
    let quorum = s.quorum_size();

    if alive_replicas.len() < quorum {
        debug!(
            "handle_local_get: not enough alive replicas ({} < {})",
            alive_replicas.len(),
            quorum
        );
        let _ = response_sender.send(None);
        return Ok(());
    }

    let req_id: u64 = rand::rng().random();
    let (resp_tx, mut resp_rx) = mpsc::channel(alive_replicas.len());

    s.awaiting_quorum_get
        .lock()
        .await
        .insert(req_id, resp_tx.clone());

    let s = s.clone();
    let is_local_replica = alive_replicas.contains(&s.my_node_id);
    tokio::spawn(async move {
        debug!("get req {}: spawned, key={:?}, replicas={}, local={}", req_id, key, alive_replicas.len(), is_local_replica);
        // Send QuorumGet to remotes FIRST (non-blocking), then do local read
        for replica in &alive_replicas {
            if *replica != s.my_node_id {
                s.send_to_peer(replica, PeerMessage::QuorumGet { key, req_id });
            }
        }
        debug!("get req {}: sent remote QuorumGets", req_id);

        // Local read (may block on stripe lock, so do it after remote sends)
        if is_local_replica {
            debug!("get req {}: starting local db.get, stripe={}", req_id, s.db.stripe_index(&key));
            let (val, version) = match s.db.get(&key).await {
                Some((v, ver)) => (Some(v), ver),
                None => (None, 0),
            };
            debug!("get req {}: local db.get done, version={}", req_id, version);
            let _ = resp_tx.try_send((val, version));
        }

        debug!("get req {}: entering response collection", req_id);
        // Collect quorum responses with timeout
        let mut responses = Vec::new();
        let _ = tokio::time::timeout(QUORUM_READ_TIMEOUT, async {
            while responses.len() < quorum {
                match resp_rx.recv().await {
                    Some(resp) => {
                        debug!("get req {}: got response #{}", req_id, responses.len() + 1);
                        responses.push(resp);
                    }
                    None => break,
                }
            }
        })
        .await;

        debug!("get req {}: collection done, got {} responses (need {})", req_id, responses.len(), quorum);
        s.awaiting_quorum_get.lock().await.remove(&req_id);
        let result = if responses.len() >= quorum {
            paxos_commit::select_best_read(&responses)
        } else {
            debug!(
                "handle_local_get: only got {} responses, need {}",
                responses.len(),
                quorum
            );
            None
        };
        debug!("handle_local_get: quorum read result: {:?}", result);
        let _ = response_sender.send(result);
    });

    Ok(())
}

/// 5b. Paxos-Commit write (unified handler for put and triput).
///
/// Creates TxVoteTracker inline (before spawning) so the peer loop can
/// deliver Accepted messages immediately. Spawns a task for lock acquisition,
/// voting, and outcome determination.
pub(crate) async fn handle_local_write<K, V>(
    s: &Arc<Shared<K, V>>,
    pairs: Vec<KVPair<K, V>>,
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
    let quorum = s.quorum_size();
    let aq = s.acceptor_quorum();
    let my_node_id = s.my_node_id.clone();

    // 1. Compute key_replicas and check feasibility
    let mut key_replica_vecs: Vec<Vec<NodeId>> = Vec::new();
    let mut all_rms: HashSet<NodeId> = HashSet::new();

    for pair in &pairs {
        let all_replicas: Vec<NodeId> =
            s.get_key_replicas(&pair.key).into_iter().cloned().collect();
        let alive = s.get_alive_replicas(&pair.key).await;
        if alive.len() < quorum {
            debug!(
                "local_write tx {}: key {:?} has {} alive replicas, need {}",
                tx_id,
                pair.key,
                alive.len(),
                quorum
            );
            let _ = response_sender.send(false);
            return;
        }
        for rm in &all_replicas {
            all_rms.insert(rm.clone());
        }
        key_replica_vecs.push(all_replicas);
    }

    // 2. Create TxVoteTracker inline (before spawning, so peer loop sees it)
    let tracker = Arc::new(TxVoteTracker {
        rm_votes: Mutex::new(HashMap::new()),
        notify: Notify::new(),
    });
    s.tx_trackers.lock().await.insert(tx_id, tracker.clone());

    // 3. Spawn task for the rest
    let s = s.clone();
    tokio::spawn(async move {
        // Determine local entries (keys where this node is a replica)
        let mut local_entries: Vec<(KVPair<K, V>, usize)> = Vec::new();
        for (i, pair) in pairs.iter().enumerate() {
            if key_replica_vecs[i].contains(&my_node_id) {
                let stripe_idx = s.db.stripe_index(&pair.key);
                local_entries.push((pair.clone(), stripe_idx));
            }
        }

        // Remote RMs
        let remote_rms: Vec<NodeId> = all_rms
            .iter()
            .filter(|rm| **rm != my_node_id)
            .cloned()
            .collect();

        // Send Prepare to remote RMs FIRST (so their trackers exist before our Vote arrives)
        for rm in &remote_rms {
            debug!("local_write tx {}: sending Prepare to {}", tx_id, rm);
            s.send_to_peer(
                rm,
                PeerMessage::Prepare {
                    pairs: pairs.clone(),
                    tx_id,
                    version,
                },
            );
        }

        // Local prepare
        let is_local_rm = !local_entries.is_empty();
        let local_voted_prepared = if is_local_rm {
            local_entries.sort_by_key(|(_, idx)| *idx);

            let mut guards: HashMap<usize, OwnedMutexGuard<HashMap<K, (V, u64)>>> =
                HashMap::new();
            let mut lock_failed = false;
            for (_, idx) in &local_entries {
                if !guards.contains_key(idx) {
                    let stripe = s.db.get_stripe_by_index(*idx);
                    match stripe.try_lock_owned() {
                        Ok(guard) => {
                            debug!("local_write tx {}: locked stripe {}", tx_id, idx);
                            guards.insert(*idx, guard);
                        }
                        Err(_) => {
                            debug!("local_write tx {}: try_lock failed stripe {}", tx_id, idx);
                            lock_failed = true;
                            break;
                        }
                    }
                }
            }

            let vote = !lock_failed;
            if vote {
                s.pending_prepares.lock().await.insert(
                    tx_id,
                    PendingTx {
                        pairs: local_entries.clone(),
                        guards,
                        version,
                    },
                );
            } else {
                // Drop partially-acquired guards immediately to avoid self-deadlock
                drop(guards);
            }

            // Self-accept + broadcast Vote + Accepted
            s.acceptor_log
                .lock()
                .await
                .insert((tx_id, my_node_id.clone()), vote);
            for (node_id, sender) in s.senders.iter() {
                if *node_id != my_node_id {
                    let _ = sender.try_send(PeerMessage::Vote {
                        tx_id,
                        rm_id: my_node_id.clone(),
                        vote,
                    });
                    let _ = sender.try_send(PeerMessage::Accepted {
                        tx_id,
                        rm_id: my_node_id.clone(),
                        vote,
                    });
                }
            }

            // Update tracker with self-accept
            {
                let mut rm_votes = tracker.rm_votes.lock().await;
                let entry = rm_votes
                    .entry(my_node_id.clone())
                    .or_insert_with(|| (HashSet::new(), HashSet::new()));
                if vote {
                    entry.0.insert(my_node_id.clone());
                } else {
                    entry.1.insert(my_node_id.clone());
                }
            }
            tracker.notify.notify_waiters();

            vote
        } else {
            false
        };

        // Wait for outcome
        let outcome = tokio::time::timeout(VOTE_LEARN_TIMEOUT, async {
            loop {
                // Create Notified future BEFORE checking condition to avoid race
                let notified = tracker.notify.notified();
                let result = {
                    let rm_votes = tracker.rm_votes.lock().await;
                    let alive = s.alive_nodes.lock().await;
                    paxos_commit::evaluate_commit_rule(
                        &key_replica_vecs,
                        &rm_votes,
                        quorum,
                        aq,
                        &alive,
                    )
                };
                if let Some(result) = result {
                    return result;
                }
                notified.await;
            }
        })
        .await;

        let committed = match outcome {
            Ok(result) => result,
            Err(_) => {
                debug!("local_write tx {}: timeout, re-evaluating", tx_id);
                let rm_votes = tracker.rm_votes.lock().await;
                let alive = s.alive_nodes.lock().await;
                paxos_commit::evaluate_commit_rule(
                    &key_replica_vecs,
                    &rm_votes,
                    quorum,
                    aq,
                    &alive,
                )
                .unwrap_or(false)
            }
        };

        // Apply outcome
        if committed {
            debug!("local_write tx {}: committed", tx_id);
            if is_local_rm && local_voted_prepared {
                // Apply writes from locked guards
                let mut pending = s.pending_prepares.lock().await;
                if let Some(mut tx) = pending.remove(&tx_id) {
                    let stripes: Vec<usize> = tx.pairs.iter().map(|(_, idx)| *idx).collect();
                    for (pair, stripe_idx) in &tx.pairs {
                        if let Some(guard) = tx.guards.get_mut(stripe_idx) {
                            guard.insert(pair.key, (pair.val.clone(), tx.version));
                        }
                    }
                    debug!("local_write tx {}: released stripe locks {:?}", tx_id, stripes);
                }
            } else if is_local_rm {
                // Voted Aborted but tx committed: apply via db.put()
                s.pending_prepares.lock().await.remove(&tx_id);
                for (pair, _) in &local_entries {
                    s.db.put(pair.key, pair.val.clone(), version).await;
                }
            }
        } else {
            debug!("local_write tx {}: aborted", tx_id);
            // Drop guards (release locks)
            let mut pending = s.pending_prepares.lock().await;
            if let Some(tx) = pending.remove(&tx_id) {
                let stripes: Vec<usize> = tx.pairs.iter().map(|(_, idx)| *idx).collect();
                debug!("local_write tx {}: releasing stripe locks {:?}", tx_id, stripes);
            }
        }

        // Clean up
        s.tx_trackers.lock().await.remove(&tx_id);
        {
            let mut log = s.acceptor_log.lock().await;
            log.retain(|k, _| k.0 != tx_id);
        }
        debug!("local_write tx {}: cleanup done", tx_id);

        let _ = response_sender.send(committed);
    });
}

/// 5c. RM role: receive Prepare, create tracker (inline), then spawn
/// lock acquisition + voting + self-determination task.
///
/// The tracker is created before spawning so that Accepted messages arriving
/// in the peer loop (from the coordinator's Vote) find the tracker immediately.
pub(crate) async fn handle_peer_prepare<K, V>(
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
    debug!("peer_prepare tx {}: received from {}", tx_id, from);
    s.observe_version(version);
    let my_node_id = s.my_node_id.clone();
    let quorum = s.quorum_size();
    let aq = s.acceptor_quorum();

    // Compute key_replicas for ALL keys in the transaction
    let key_replica_vecs: Vec<Vec<NodeId>> = pairs
        .iter()
        .map(|p| s.get_key_replicas(&p.key).into_iter().cloned().collect())
        .collect();
    // Create TxVoteTracker inline (before spawning, so peer loop sees it)
    let tracker = Arc::new(TxVoteTracker {
        rm_votes: Mutex::new(HashMap::new()),
        notify: Notify::new(),
    });
    s.tx_trackers.lock().await.insert(tx_id, tracker.clone());

    // Pre-populate tracker from acceptor_log (recover self-acceptances
    // for votes we accepted before this tracker existed)
    {
        let log = s.acceptor_log.lock().await;
        let mut rm_votes = tracker.rm_votes.lock().await;
        for (k, &vote) in log.iter() {
            if k.0 == tx_id {
                let entry = rm_votes
                    .entry(k.1.clone())
                    .or_insert_with(|| (HashSet::new(), HashSet::new()));
                if vote {
                    entry.0.insert(my_node_id.clone());
                } else {
                    entry.1.insert(my_node_id.clone());
                }
            }
        }
    }

    // Spawn task for lock acquisition, voting, and self-determination
    let s = s.clone();
    tokio::spawn(async move {
        // Determine which keys this node is a replica for
        let mut my_entries: Vec<(KVPair<K, V>, usize)> = Vec::new();
        for pair in &pairs {
            let replicas = s.get_key_replicas(&pair.key);
            if replicas.contains(&&my_node_id) {
                let stripe_idx = s.db.stripe_index(&pair.key);
                my_entries.push((pair.clone(), stripe_idx));
            }
        }

        // Try to acquire stripe locks
        my_entries.sort_by_key(|(_, idx)| *idx);
        let mut guards: HashMap<usize, OwnedMutexGuard<HashMap<K, (V, u64)>>> = HashMap::new();
        let mut lock_failed = false;
        for (_, idx) in &my_entries {
            if !guards.contains_key(idx) {
                let stripe = s.db.get_stripe_by_index(*idx);
                match stripe.try_lock_owned() {
                    Ok(guard) => {
                        debug!("peer_prepare tx {}: locked stripe {}", tx_id, idx);
                        guards.insert(*idx, guard);
                    }
                    Err(_) => {
                        debug!("peer_prepare tx {}: try_lock failed stripe {}", tx_id, idx);
                        lock_failed = true;
                        break;
                    }
                }
            }
        }

        let vote = !lock_failed;

        if vote {
            s.pending_prepares.lock().await.insert(
                tx_id,
                PendingTx {
                    pairs: my_entries.clone(),
                    guards,
                    version,
                },
            );
        } else {
            // Drop partially-acquired guards immediately to avoid self-deadlock:
            // if the tx later commits, the "apply via db.put()" path would
            // lock().await on stripes we still hold → deadlock.
            drop(guards);
        }

        // Self-accept + broadcast Vote + Accepted
        s.acceptor_log
            .lock()
            .await
            .insert((tx_id, my_node_id.clone()), vote);
        debug!(
            "peer_prepare tx {}: broadcasting Vote(vote={})",
            tx_id, vote
        );
        for (node_id, sender) in s.senders.iter() {
            if *node_id != my_node_id {
                let _ = sender.try_send(PeerMessage::Vote {
                    tx_id,
                    rm_id: my_node_id.clone(),
                    vote,
                });
                let _ = sender.try_send(PeerMessage::Accepted {
                    tx_id,
                    rm_id: my_node_id.clone(),
                    vote,
                });
            }
        }

        // Update tracker with self-accept
        {
            let mut rm_votes = tracker.rm_votes.lock().await;
            let entry = rm_votes
                .entry(my_node_id.clone())
                .or_insert_with(|| (HashSet::new(), HashSet::new()));
            if vote {
                entry.0.insert(my_node_id.clone());
            } else {
                entry.1.insert(my_node_id.clone());
            }
        }
        tracker.notify.notify_waiters();

        // Self-determination: wait for outcome
        let outcome = tokio::time::timeout(PREPARE_LOCK_TIMEOUT, async {
            loop {
                // Create Notified future BEFORE checking condition to avoid race
                let notified = tracker.notify.notified();
                let result = {
                    let rm_votes = tracker.rm_votes.lock().await;
                    let alive = s.alive_nodes.lock().await;
                    paxos_commit::evaluate_commit_rule(
                        &key_replica_vecs,
                        &rm_votes,
                        quorum,
                        aq,
                        &alive,
                    )
                };
                if let Some(result) = result {
                    return result;
                }
                notified.await;
            }
        })
        .await;

        let committed = match outcome {
            Ok(result) => result,
            Err(_) => {
                debug!(
                    "peer_prepare tx {}: PREPARE_LOCK_TIMEOUT, aborting locally",
                    tx_id
                );
                false
            }
        };

        // Apply outcome
        if committed {
            debug!("peer_prepare tx {}: committed", tx_id);
            if vote {
                // Apply writes from locked guards
                let mut pending = s.pending_prepares.lock().await;
                if let Some(mut tx) = pending.remove(&tx_id) {
                    let stripes: Vec<usize> = tx.pairs.iter().map(|(_, idx)| *idx).collect();
                    for (pair, stripe_idx) in &tx.pairs {
                        if let Some(guard) = tx.guards.get_mut(stripe_idx) {
                            guard.insert(pair.key, (pair.val.clone(), tx.version));
                        }
                    }
                    debug!("peer_prepare tx {}: released stripe locks {:?}", tx_id, stripes);
                }
            } else {
                // Voted Aborted but tx committed: apply via db.put()
                s.pending_prepares.lock().await.remove(&tx_id);
                for (pair, _) in &my_entries {
                    s.db.put(pair.key, pair.val.clone(), version).await;
                }
            }
        } else {
            debug!("peer_prepare tx {}: aborted", tx_id);
            // Drop guards (release locks)
            let mut pending = s.pending_prepares.lock().await;
            if let Some(tx) = pending.remove(&tx_id) {
                let stripes: Vec<usize> = tx.pairs.iter().map(|(_, idx)| *idx).collect();
                debug!("peer_prepare tx {}: releasing stripe locks {:?}", tx_id, stripes);
            }
        }

        // Clean up
        s.tx_trackers.lock().await.remove(&tx_id);
        {
            let mut log = s.acceptor_log.lock().await;
            log.retain(|k, _| k.0 != tx_id);
        }
        debug!("peer_prepare tx {}: cleanup done", tx_id);
    });
}
