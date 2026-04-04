use anyhow::Result;
use clap::Parser;
use dht::{
    ClientHello, ClientMessage, ClientResponse, FirstMessage, KVPair,
    get_connect_str, recv_msg, send_msg,
};
use rand::{rngs::ThreadRng, Rng};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{oneshot, Mutex, Notify, Semaphore};
use tracing::{debug, error, info};

const PUT_FREQUENCY: usize = 20;
const TRI_PUT_FREQUENCY: usize = 20;
const COLLECT_INTERVAL: Duration = Duration::from_millis(500);
const MAX_IN_FLIGHT: usize = 64;
const MAX_RETRIES: usize = 10;

#[derive(Parser, Debug)]
struct ClientConfig {
    /// Name of this client's co-located node
    #[arg(short, long, required = true)]
    name: String,

    /// Names of all OTHER nodes in the cluster (for failover)
    #[arg(short, long, required = true, value_delimiter = ',')]
    connections: Vec<String>,

    /// Number of operations to run
    #[arg(short = 'k', long, default_value = "1000000")]
    num_keys: usize,

    /// Range of keys
    #[arg(short, long, default_value = "1000")]
    key_range: u64,
}

struct IntervalMetrics {
    latency_sum_us: AtomicU64,
    op_count: AtomicU64,
    success_count: AtomicU64,
    max_retries_hit: AtomicU64,
}

struct Snapshot {
    elapsed_secs: f64,
    avg_latency_ms: f64,
    successful_txns: u64,
}

/// Pending response map: req_id -> oneshot sender for the response
type PendingMap = Arc<Mutex<HashMap<u64, oneshot::Sender<ClientResponse<u8>>>>>;

/// Shared connection state
struct ConnectionState {
    writers: Vec<Arc<Mutex<OwnedWriteHalf>>>,
    pending: PendingMap,
    disconnect: Arc<Notify>,
    failed_over: bool,
}

#[derive(Debug, Clone, Copy)]
enum Operation {
    Get { key: u64 },
    Put { pair: KVPair<u64, u8> },
    TriPut { pairs: [KVPair<u64, u8>; 3] },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;

    rt.block_on(run())
}

async fn run() -> Result<()> {
    let config = ClientConfig::parse();

    info!("Client for node {}", config.name);
    info!("Will failover to {:?}", config.connections);

    // Connect to co-located node
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let disconnect = Arc::new(Notify::new());

    let (writer, _reader_handle) = connect_to_node(&config.name, &config.name, pending.clone(), disconnect.clone()).await?;

    let conn_state = Arc::new(Mutex::new(ConnectionState {
        writers: vec![writer],
        pending: pending.clone(),
        disconnect: disconnect.clone(),
        failed_over: false,
    }));

    // Generate test operations
    info!(
        "Generating {} operations with key range {}",
        config.num_keys, config.key_range
    );
    let test_data = generate_test_operations(config.num_keys, config.key_range);

    // Give the node time to start handling
    tokio::time::sleep(Duration::from_millis(100)).await;

    info!("Starting test");
    let start = Instant::now();

    // Interval metrics collection
    let metrics = Arc::new(IntervalMetrics {
        latency_sum_us: AtomicU64::new(0),
        op_count: AtomicU64::new(0),
        success_count: AtomicU64::new(0),
        max_retries_hit: AtomicU64::new(0),
    });
    let snapshots: Arc<Mutex<Vec<Snapshot>>> = Arc::new(Mutex::new(Vec::new()));

    let collector_metrics = metrics.clone();
    let collector_snapshots = snapshots.clone();
    let collector_start = start;
    let collector_handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(COLLECT_INTERVAL).await;
            let lat_sum = collector_metrics
                .latency_sum_us
                .swap(0, Ordering::Relaxed);
            let count = collector_metrics.op_count.swap(0, Ordering::Relaxed);
            let success = collector_metrics
                .success_count
                .swap(0, Ordering::Relaxed);
            let avg_lat = if count > 0 {
                lat_sum as f64 / count as f64 / 1000.0
            } else {
                0.0
            };
            let elapsed_secs = collector_start.elapsed().as_secs_f64();
            let ops_sec = success as f64 / COLLECT_INTERVAL.as_secs_f64();
            info!(
                "{:.1}s | txns: {} | ops/sec: {:.0} | avg_latency: {:.3}ms",
                elapsed_secs, success, ops_sec, avg_lat
            );
            collector_snapshots.lock().await.push(Snapshot {
                elapsed_secs,
                avg_latency_ms: avg_lat,
                successful_txns: success,
            });
        }
    });

    let semaphore = Arc::new(Semaphore::new(MAX_IN_FLIGHT));
    let req_counter = Arc::new(AtomicU64::new(0));
    let round_robin = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::with_capacity(test_data.len());
    for i in 0..test_data.len() {
        print_progress(i, test_data.len());
        let operation = test_data[i];
        let m = metrics.clone();
        let sem = semaphore.clone();
        let conn = conn_state.clone();
        let req_ctr = req_counter.clone();
        let rr = round_robin.clone();
        let all_connections = config.connections.clone();
        let home_name = config.name.clone();

        handles.push(tokio::spawn(async move {
            let mut permit = sem.acquire().await.unwrap();
            let req_start = Instant::now();
            let success = match operation {
                Operation::Get { key } => {
                    let req_id = req_ctr.fetch_add(1, Ordering::Relaxed);
                    send_and_receive_get(key, req_id, &conn, &rr).await
                }
                Operation::Put { pair } => {
                    let mut attempts = 0;
                    loop {
                        attempts += 1;
                        let req_id = req_ctr.fetch_add(1, Ordering::Relaxed);
                        match send_and_receive_write(
                            ClientMessage::Put { pair, req_id },
                            req_id,
                            &conn,
                            &rr,
                            &all_connections,
                            &home_name,
                        )
                        .await
                        {
                            Ok(true) => break true,
                            Ok(false) if attempts < MAX_RETRIES => {
                                debug!(
                                    "Put {:?} failed (attempt {}), retrying",
                                    pair.key, attempts
                                );
                                drop(permit);
                                let backoff =
                                    Duration::from_millis(10 * (1 << attempts.min(5)))
                                        + Duration::from_millis(rand::random_range(0..20));
                                tokio::time::sleep(backoff).await;
                                permit = sem.acquire().await.unwrap();
                            }
                            Ok(false) => {
                                debug!(
                                    "Put {:?} failed after {} attempts",
                                    pair.key, attempts
                                );
                                m.max_retries_hit.fetch_add(1, Ordering::Relaxed);
                                break false;
                            }
                            Err(_) => break false,
                        }
                    }
                }
                Operation::TriPut { pairs } => {
                    let mut attempts = 0;
                    let result = loop {
                        attempts += 1;
                        let req_id = req_ctr.fetch_add(1, Ordering::Relaxed);
                        match send_and_receive_write(
                            ClientMessage::TriPut { pairs, req_id },
                            req_id,
                            &conn,
                            &rr,
                            &all_connections,
                            &home_name,
                        )
                        .await
                        {
                            Ok(true) => break true,
                            Ok(false) if attempts < MAX_RETRIES => {
                                debug!("TriPut failed (attempt {}), retrying", attempts);
                                drop(permit);
                                let backoff =
                                    Duration::from_millis(10 * (1 << attempts.min(5)))
                                        + Duration::from_millis(rand::random_range(0..20));
                                tokio::time::sleep(backoff).await;
                                permit = sem.acquire().await.unwrap();
                            }
                            Ok(false) => {
                                debug!("TriPut failed after {} attempts", attempts);
                                m.max_retries_hit.fetch_add(1, Ordering::Relaxed);
                                break false;
                            }
                            Err(_) => break false,
                        }
                    };
                    result
                }
            };

            let latency = req_start.elapsed();
            m.latency_sum_us
                .fetch_add(latency.as_micros() as u64, Ordering::Relaxed);
            m.op_count.fetch_add(1, Ordering::Relaxed);
            if success {
                m.success_count.fetch_add(1, Ordering::Relaxed);
            }
            latency
        }));
    }

    // Wait for all to complete
    let mut latencies: Vec<Duration> = Vec::with_capacity(handles.len());
    for handle in handles {
        latencies.push(handle.await?);
    }
    let elapsed = start.elapsed();

    // Stop collector and print interval data
    collector_handle.abort();
    let _ = collector_handle.await;

    let snaps = snapshots.lock().await;
    info!(
        "=== Interval Metrics ({}s intervals) ===",
        COLLECT_INTERVAL.as_secs()
    );
    info!(
        "{:<12} {:>20} {:>20}",
        "Time(s)", "Successful Txns", "Avg Latency(ms)"
    );
    for snap in snaps.iter() {
        info!(
            "{:<12.1} {:>20} {:>20.3}",
            snap.elapsed_secs, snap.successful_txns, snap.avg_latency_ms
        );
    }
    drop(snaps);

    // Send Done to home node (if still connected, i.e., not in failover mode)
    {
        let state = conn_state.lock().await;
        if !state.failed_over {
            if let Some(writer) = state.writers.first() {
                let mut w = writer.lock().await;
                let _ = send_msg(&mut *w, &ClientMessage::<u64, u8>::Done).await;
            }
        }
    }

    info!("Completed {} operations in {:?}", config.num_keys, elapsed);
    info!(
        "Throughput: {:.2} ops/sec",
        config.num_keys as f64 / elapsed.as_secs_f64()
    );
    latencies.sort();
    let p50 = latencies[latencies.len() / 2];
    let p99 = latencies[latencies.len() * 99 / 100];
    let avg = latencies.iter().sum::<Duration>() / latencies.len() as u32;
    info!("Latency avg: {:?}, p50: {:?}, p99: {:?}", avg, p50, p99);
    let max_retries = metrics.max_retries_hit.load(Ordering::Relaxed);
    info!(
        "Transactions that hit MAX_RETRIES ({}): {}",
        MAX_RETRIES, max_retries
    );

    // Wait a moment for Done to propagate, then exit
    tokio::time::sleep(Duration::from_millis(500)).await;
    std::process::exit(0);
}

/// Connect to a node, returning the writer and spawning a reader task.
async fn connect_to_node(
    node_name: &str,
    client_name: &str,
    pending: PendingMap,
    disconnect: Arc<Notify>,
) -> Result<(Arc<Mutex<OwnedWriteHalf>>, tokio::task::JoinHandle<()>)> {
    let addr = get_connect_str(node_name);
    info!("Connecting to {} at {}", node_name, addr);

    let mut stream = TcpStream::connect(&addr).await?;
    let _ = stream.set_nodelay(true);

    // Send client handshake
    send_msg(
        &mut stream,
        &FirstMessage::Client(ClientHello {
            client_name: format!("client-{}", client_name),
        }),
    )
    .await?;

    info!("Connected to {}", node_name);

    let (read_half, write_half) = stream.into_split();
    let writer = Arc::new(Mutex::new(write_half));

    let reader_handle = tokio::spawn(client_reader_task(node_name.to_string(), read_half, pending, disconnect));

    Ok((writer, reader_handle))
}

/// Reader task: reads ClientResponse from a node, dispatches to pending oneshot senders.
async fn client_reader_task(
    node_name: String,
    mut read_half: OwnedReadHalf,
    pending: PendingMap,
    disconnect: Arc<Notify>,
) {
    loop {
        match recv_msg::<ClientResponse<u8>, _>(&mut read_half).await {
            Ok(resp) => {
                let req_id = match &resp {
                    ClientResponse::GetResult { req_id, .. } => *req_id,
                    ClientResponse::WriteResult { req_id, .. } => *req_id,
                };
                if let Some(sender) = pending.lock().await.remove(&req_id) {
                    let _ = sender.send(resp);
                }
            }
            Err(_) => {
                info!("Node '{}' died — connection lost, triggering failover", node_name);
                disconnect.notify_waiters();
                break;
            }
        }
    }
}

/// Perform failover: connect to all surviving nodes, drain pending requests.
async fn failover(
    conn_state: &Arc<Mutex<ConnectionState>>,
    connections: &[String],
    home_name: &str,
) {
    let mut state = conn_state.lock().await;

    // Already failed over
    if state.failed_over {
        return;
    }

    info!("Failing over to {} nodes", connections.len());

    // Drain all pending requests as failures
    let mut pending = state.pending.lock().await;
    for (_, sender) in pending.drain() {
        let _ = sender.send(ClientResponse::WriteResult {
            success: false,
            req_id: 0,
        });
    }
    drop(pending);

    // Connect to all surviving nodes
    let new_pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
    let new_disconnect = Arc::new(Notify::new());
    let mut new_writers = Vec::new();

    for node_name in connections {
        match connect_to_node(
            node_name,
            home_name,
            new_pending.clone(),
            new_disconnect.clone(),
        )
        .await
        {
            Ok((writer, _handle)) => {
                info!("Failover: connected to {}", node_name);
                new_writers.push(writer);
            }
            Err(e) => {
                error!("Failover: failed to connect to {}: {}", node_name, e);
            }
        }
    }

    if new_writers.is_empty() {
        error!("Failover: no surviving nodes available!");
    }

    state.writers = new_writers;
    state.pending = new_pending;
    state.disconnect = new_disconnect;
    state.failed_over = true;
}

/// Send a Get and receive the response.
async fn send_and_receive_get(
    key: u64,
    req_id: u64,
    conn_state: &Arc<Mutex<ConnectionState>>,
    round_robin: &AtomicU64,
) -> bool {
    let (tx, rx) = oneshot::channel();

    // Extract what we need under lock, then release before I/O
    let (writer, pending, disconnect_notify) = {
        let state = conn_state.lock().await;
        if state.writers.is_empty() {
            return false;
        }
        let idx = round_robin.fetch_add(1, Ordering::Relaxed) as usize % state.writers.len();
        (
            state.writers[idx].clone(),
            state.pending.clone(),
            state.disconnect.clone(),
        )
    };

    pending.lock().await.insert(req_id, tx);

    {
        let mut w = writer.lock().await;
        if let Err(e) = send_msg(&mut *w, &ClientMessage::<u64, u8>::Get { key, req_id }).await {
            error!("Error sending Get: {e}");
            pending.lock().await.remove(&req_id);
            return false;
        }
    }

    // Wait for response or disconnect
    tokio::select! {
        result = rx => {
            matches!(result, Ok(ClientResponse::GetResult { .. }))
        }
        _ = disconnect_notify.notified() => {
            pending.lock().await.remove(&req_id);
            true
        }
    }
}

/// Send a write (Put or TriPut) and receive the response.
async fn send_and_receive_write(
    msg: ClientMessage<u64, u8>,
    req_id: u64,
    conn_state: &Arc<Mutex<ConnectionState>>,
    round_robin: &AtomicU64,
    all_connections: &[String],
    home_name: &str,
) -> Result<bool> {
    let (tx, rx) = oneshot::channel();

    // Extract what we need under lock, then release before I/O
    let (writer, pending, disconnect_notify) = {
        let state = conn_state.lock().await;
        if state.writers.is_empty() {
            drop(state);
            failover(conn_state, all_connections, home_name).await;
            return Ok(false);
        }
        let idx = round_robin.fetch_add(1, Ordering::Relaxed) as usize % state.writers.len();
        (
            state.writers[idx].clone(),
            state.pending.clone(),
            state.disconnect.clone(),
        )
    };

    pending.lock().await.insert(req_id, tx);

    {
        let mut w = writer.lock().await;
        if let Err(e) = send_msg(&mut *w, &msg).await {
            error!("Error sending write: {e}");
            pending.lock().await.remove(&req_id);
            return Ok(false);
        }
    }

    // Wait for response or disconnect
    tokio::select! {
        result = rx => {
            match result {
                Ok(ClientResponse::WriteResult { success, .. }) => Ok(success),
                Ok(ClientResponse::GetResult { .. }) => Ok(false),
                Err(_) => Ok(false),
            }
        }
        _ = disconnect_notify.notified() => {
            pending.lock().await.remove(&req_id);
            failover(conn_state, all_connections, home_name).await;
            Ok(false)
        }
    }
}

fn generate_test_operations(num_operations: usize, key_range: u64) -> Vec<Operation> {
    let mut rng = rand::rng();
    let total = PUT_FREQUENCY + TRI_PUT_FREQUENCY;

    (0..num_operations)
        .map(|_| {
            let roll = rng.random_range(0..100);
            if roll < PUT_FREQUENCY {
                Operation::Put {
                    pair: KVPair {
                        key: rng.random_range(0..key_range),
                        val: rng.random(),
                    },
                }
            } else if roll < total {
                let k1 = rng.random_range(0..key_range);
                let k2 = unique_random(k1, k1, &mut rng, key_range);
                let k3 = unique_random(k1, k2, &mut rng, key_range);
                Operation::TriPut {
                    pairs: [
                        KVPair {
                            key: k1,
                            val: rng.random(),
                        },
                        KVPair {
                            key: k2,
                            val: rng.random(),
                        },
                        KVPair {
                            key: k3,
                            val: rng.random(),
                        },
                    ],
                }
            } else {
                Operation::Get {
                    key: rng.random_range(0..key_range),
                }
            }
        })
        .collect()
}

fn unique_random(k1: u64, k2: u64, rng: &mut ThreadRng, key_range: u64) -> u64 {
    let mut res = rng.random_range(0..key_range);
    while res == k1 || res == k2 {
        res = rng.random_range(0..key_range);
    }
    res
}

fn print_progress(i: usize, total: usize) {
    let step = total / 10;
    if step > 0 && i % step == 0 {
        info!("{}% complete", (i * 100) / total);
    }
}
