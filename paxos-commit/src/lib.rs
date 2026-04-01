use std::collections::{HashMap, HashSet};
use std::hash::Hash;

/// Acceptor quorum: majority of ALL nodes in the cluster.
/// With N acceptors, tolerates floor((N-1)/2) crashes.
pub fn acceptor_quorum(cluster_size: usize) -> usize {
    cluster_size / 2 + 1
}

/// Replica quorum: majority of a key's replicas.
/// With R replicas, need R/2+1 Prepared votes for a key's write to succeed.
pub fn replica_quorum(replication_degree: usize) -> usize {
    replication_degree / 2 + 1
}

/// Status of an individual RM's vote as seen by a learner.
enum VoteStatus {
    /// acceptor_quorum acceptors confirmed Prepared
    LearnedPrepared,
    /// acceptor_quorum acceptors confirmed Aborted
    LearnedAborted,
    /// RM is dead and insufficient acceptors to ever learn the vote
    Unlearnable,
    /// Not yet enough acceptors to learn, but still possible
    Pending,
}

/// Classify an RM's vote based on collected Accepted messages.
fn classify_rm_vote<N: Eq + Hash>(
    rm_votes: &HashMap<N, (HashSet<N>, HashSet<N>)>,
    rm: &N,
    aq: usize,
    alive_nodes: &HashSet<N>,
) -> VoteStatus {
    match rm_votes.get(rm) {
        Some((prepared, aborted)) => {
            if prepared.len() >= aq {
                VoteStatus::LearnedPrepared
            } else if aborted.len() >= aq {
                VoteStatus::LearnedAborted
            } else if !alive_nodes.contains(rm) {
                VoteStatus::Unlearnable
            } else {
                VoteStatus::Pending
            }
        }
        None => {
            if !alive_nodes.contains(rm) {
                VoteStatus::Unlearnable
            } else {
                VoteStatus::Pending
            }
        }
    }
}

/// Evaluate the Paxos-Commit commit rule for a transaction.
///
/// For each key in the transaction, checks whether a quorum of its replicas
/// voted Prepared (learned via acceptor quorum of Accepted messages).
///
/// Returns:
/// - `Some(true)` — committed: every key has `rq` replicas with learned Prepared votes
/// - `Some(false)` — aborted: some key can never reach `rq` Prepared votes
/// - `None` — not yet determined: waiting for more Accepted messages
///
/// Dead RMs with insufficient Accepted messages are treated as Aborted (safe:
/// an unlearned vote was never "chosen" in the Paxos sense).
pub fn evaluate_commit_rule<N: Eq + Hash + Clone>(
    key_replicas: &[Vec<N>],
    rm_votes: &HashMap<N, (HashSet<N>, HashSet<N>)>,
    rq: usize,
    aq: usize,
    alive_nodes: &HashSet<N>,
) -> Option<bool> {
    let mut any_pending = false;

    for replicas in key_replicas {
        let mut prepared_count = 0;
        let mut max_possible_prepared = 0;

        for replica in replicas {
            match classify_rm_vote(rm_votes, replica, aq, alive_nodes) {
                VoteStatus::LearnedPrepared => {
                    prepared_count += 1;
                    max_possible_prepared += 1;
                }
                VoteStatus::LearnedAborted | VoteStatus::Unlearnable => {}
                VoteStatus::Pending => {
                    max_possible_prepared += 1;
                }
            }
        }

        if prepared_count >= rq {
            continue;
        }
        if max_possible_prepared < rq {
            return Some(false);
        }
        any_pending = true;
    }

    if any_pending {
        None
    } else {
        Some(true)
    }
}

/// Pick the highest-versioned value from quorum read responses.
///
/// Filters out `None` responses (replicas that don't have the key),
/// then selects the value with the highest version number.
pub fn select_best_read<V: Clone>(responses: &[(Option<V>, u64)]) -> Option<V> {
    responses
        .iter()
        .filter_map(|(val, version)| val.as_ref().map(|v| (v, *version)))
        .max_by_key(|(_, version)| *version)
        .map(|(v, _)| v.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Quorum size tests ---

    #[test]
    fn test_acceptor_quorum() {
        assert_eq!(acceptor_quorum(1), 1);
        assert_eq!(acceptor_quorum(2), 2);
        assert_eq!(acceptor_quorum(3), 2);
        assert_eq!(acceptor_quorum(4), 3);
        assert_eq!(acceptor_quorum(5), 3);
    }

    #[test]
    fn test_replica_quorum() {
        assert_eq!(replica_quorum(1), 1);
        assert_eq!(replica_quorum(2), 2);
        assert_eq!(replica_quorum(3), 2);
        assert_eq!(replica_quorum(4), 3);
        assert_eq!(replica_quorum(5), 3);
    }

    // --- Helpers for commit rule tests ---

    fn prepared_by(acceptors: &[usize]) -> (HashSet<usize>, HashSet<usize>) {
        (acceptors.iter().copied().collect(), HashSet::new())
    }

    fn aborted_by(acceptors: &[usize]) -> (HashSet<usize>, HashSet<usize>) {
        (HashSet::new(), acceptors.iter().copied().collect())
    }

    fn alive(nodes: &[usize]) -> HashSet<usize> {
        nodes.iter().copied().collect()
    }

    // --- evaluate_commit_rule tests ---

    #[test]
    fn commit_all_prepared() {
        // 3 nodes, R=3, rq=2, aq=2. All 3 RMs voted Prepared (learned).
        let key_replicas = vec![vec![0, 1, 2]];
        let mut rm_votes = HashMap::new();
        rm_votes.insert(0, prepared_by(&[0, 1]));
        rm_votes.insert(1, prepared_by(&[0, 1]));
        rm_votes.insert(2, prepared_by(&[0, 1]));

        assert_eq!(
            evaluate_commit_rule(&key_replicas, &rm_votes, 2, 2, &alive(&[0, 1, 2])),
            Some(true)
        );
    }

    #[test]
    fn commit_one_aborted_but_quorum_met() {
        // RMs 0,1 Prepared, RM 2 Aborted. 2 >= rq(2) -> commit.
        let key_replicas = vec![vec![0, 1, 2]];
        let mut rm_votes = HashMap::new();
        rm_votes.insert(0, prepared_by(&[0, 1]));
        rm_votes.insert(1, prepared_by(&[0, 1]));
        rm_votes.insert(2, aborted_by(&[0, 1]));

        assert_eq!(
            evaluate_commit_rule(&key_replicas, &rm_votes, 2, 2, &alive(&[0, 1, 2])),
            Some(true)
        );
    }

    #[test]
    fn abort_too_many_aborted() {
        // RM 0 Prepared, RMs 1,2 Aborted. 1 < rq(2), max=1 < 2 -> abort.
        let key_replicas = vec![vec![0, 1, 2]];
        let mut rm_votes = HashMap::new();
        rm_votes.insert(0, prepared_by(&[0, 1]));
        rm_votes.insert(1, aborted_by(&[0, 1]));
        rm_votes.insert(2, aborted_by(&[0, 1]));

        assert_eq!(
            evaluate_commit_rule(&key_replicas, &rm_votes, 2, 2, &alive(&[0, 1, 2])),
            Some(false)
        );
    }

    #[test]
    fn pending_not_enough_accepted() {
        // RM 0 learned Prepared (2 acceptors). RM 1 only 1 acceptor (pending). RM 2 no votes (pending).
        // prepared=1, max_possible=3 -> None.
        let key_replicas = vec![vec![0, 1, 2]];
        let mut rm_votes = HashMap::new();
        rm_votes.insert(0, prepared_by(&[0, 1]));
        rm_votes.insert(1, prepared_by(&[0])); // 1 < aq(2)

        assert_eq!(
            evaluate_commit_rule(&key_replicas, &rm_votes, 2, 2, &alive(&[0, 1, 2])),
            None
        );
    }

    #[test]
    fn dead_rm_treated_as_aborted_still_commits() {
        // RMs 0,1 Prepared (learned). RM 2 is dead, no votes -> Unlearnable.
        // prepared=2 >= rq(2) -> commit.
        let key_replicas = vec![vec![0, 1, 2]];
        let mut rm_votes = HashMap::new();
        rm_votes.insert(0, prepared_by(&[0, 1]));
        rm_votes.insert(1, prepared_by(&[0, 1]));

        assert_eq!(
            evaluate_commit_rule(&key_replicas, &rm_votes, 2, 2, &alive(&[0, 1])),
            Some(true)
        );
    }

    #[test]
    fn dead_rm_causes_abort() {
        // RM 0 pending (1 acceptor, alive). RMs 1,2 dead, no votes -> Unlearnable.
        // prepared=0, max_possible=1 < rq(2) -> abort.
        let key_replicas = vec![vec![0, 1, 2]];
        let mut rm_votes = HashMap::new();
        rm_votes.insert(0, prepared_by(&[0])); // 1 < aq(2), still pending

        assert_eq!(
            evaluate_commit_rule(&key_replicas, &rm_votes, 2, 2, &alive(&[0])),
            Some(false)
        );
    }

    #[test]
    fn triput_all_keys_committed() {
        // 3 keys, same replica set [0,1,2], all Prepared.
        let key_replicas = vec![vec![0, 1, 2], vec![0, 1, 2], vec![0, 1, 2]];
        let mut rm_votes = HashMap::new();
        rm_votes.insert(0, prepared_by(&[0, 1]));
        rm_votes.insert(1, prepared_by(&[0, 1]));
        rm_votes.insert(2, prepared_by(&[0, 1]));

        assert_eq!(
            evaluate_commit_rule(&key_replicas, &rm_votes, 2, 2, &alive(&[0, 1, 2])),
            Some(true)
        );
    }

    #[test]
    fn triput_one_key_aborted() {
        // 5 nodes, R=3. Key 0: replicas [0,1,2]. Key 1: replicas [2,3,4].
        // RMs 0,1,2 Prepared. RMs 3,4 Aborted. aq=3.
        // Key 0: 3 Prepared >= rq(2). Key 1: 1 Prepared (RM 2) < rq(2), max=1 -> abort.
        let key_replicas = vec![vec![0, 1, 2], vec![2, 3, 4]];
        let mut rm_votes = HashMap::new();
        rm_votes.insert(0, prepared_by(&[0, 1, 2]));
        rm_votes.insert(1, prepared_by(&[0, 1, 2]));
        rm_votes.insert(2, prepared_by(&[0, 1, 2]));
        rm_votes.insert(3, aborted_by(&[0, 1, 2]));
        rm_votes.insert(4, aborted_by(&[0, 1, 2]));

        assert_eq!(
            evaluate_commit_rule(&key_replicas, &rm_votes, 2, 3, &alive(&[0, 1, 2, 3, 4])),
            Some(false)
        );
    }

    #[test]
    fn abort_detected_even_with_pending_keys() {
        // Key 0: all Aborted (definitely aborted). Key 1: pending.
        // Should return Some(false), not None.
        let key_replicas = vec![vec![0, 1, 2], vec![3, 4, 5]];
        let mut rm_votes = HashMap::new();
        rm_votes.insert(0, aborted_by(&[0, 1]));
        rm_votes.insert(1, aborted_by(&[0, 1]));
        rm_votes.insert(2, aborted_by(&[0, 1]));
        // Key 1 RMs: no votes yet (pending)

        assert_eq!(
            evaluate_commit_rule(&key_replicas, &rm_votes, 2, 2, &alive(&[0, 1, 2, 3, 4, 5])),
            Some(false)
        );
    }

    // --- select_best_read tests ---

    #[test]
    fn best_read_highest_version() {
        let responses = vec![
            (Some("old".to_string()), 1),
            (Some("new".to_string()), 3),
            (Some("mid".to_string()), 2),
        ];
        assert_eq!(select_best_read(&responses), Some("new".to_string()));
    }

    #[test]
    fn best_read_with_nones() {
        let responses = vec![
            (None, 0),
            (Some("val".to_string()), 1),
            (None, 0),
        ];
        assert_eq!(select_best_read(&responses), Some("val".to_string()));
    }

    #[test]
    fn best_read_all_none() {
        let responses: Vec<(Option<String>, u64)> = vec![(None, 0), (None, 0)];
        assert_eq!(select_best_read::<String>(&responses), None);
    }

    #[test]
    fn best_read_empty() {
        let responses: Vec<(Option<String>, u64)> = vec![];
        assert_eq!(select_best_read::<String>(&responses), None);
    }
}
