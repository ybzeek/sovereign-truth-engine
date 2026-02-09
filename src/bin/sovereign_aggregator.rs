//! Sovereign Aggregator (Direct-to-PDS Edition)
//! Bypasses centralized relays and connects to every individual PDS.
//! 
//! This tool proves that a single home computer can manage 10,000+ persistent
//! WebSocket connections to aggregate the global ATProto firehose.

use did_mmap_cache::pds_ledger::{PdsEntry, PdsLedger};
use futures::StreamExt;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use std::collections::HashSet;
use dashmap::{DashMap, DashSet};
use fastbloom::BloomFilter;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use bytes::Bytes;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;
use tracing::{info, warn, error};
use sonic_rs::JsonValueTrait;

use std::io::Write;

/// The Sovereign Registry: Tracks all known PDS endpoints
struct PdsRegistry {
    endpoints: DashSet<String>,
    active_workers: DashSet<String>,
    ledger: Mutex<Option<PdsLedger>>,
    url_to_idx: DashMap<String, usize>,
    bloom: Mutex<BloomFilter>, 
}

enum WorkerResponse {
    Connected(String),
    Success(Arc<String>, [u8; 32], Bytes),
    Failure(String),
    Closed,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage:");
        eprintln!("  {} discover <pds_list_file>   - Crawl PLC to find PDS nodes", args[0]);
        eprintln!("  {} siege <pds_list_file>      - Connect to all nodes in the list", args[0]);
        eprintln!("  {} migrate <pds_list_file>    - Convert .txt list to .bin ledger", args[0]);
        eprintln!("  {} inspect <pds_ledger_file>  - Display statistics from binary ledger", args[0]);
        return Ok(());
    }
    
    let mode = &args[1];
    let list_path = &args[2];

    if mode == "discover" {
        run_discovery(list_path).await
    } else if mode == "siege" {
        run_siege(list_path).await
    } else if mode == "migrate" {
        run_migration(list_path).await
    } else if mode == "inspect" {
        run_inspection(list_path).await
    } else {
        eprintln!("Unknown mode: {}. Use 'discover' or 'siege'.", mode);
        Ok(())
    }
}

async fn run_discovery(list_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting Sovereign Discovery (Phase 1: Reconnaissance)");
    
    let bin_path = if list_path.ends_with(".txt") {
        list_path.replace(".txt", ".bin")
    } else {
        list_path.to_string()
    };
    
    let mut ledger = PdsLedger::open_or_create(&bin_path)?;
    let (new_pds_tx, mut new_pds_rx) = mpsc::channel::<String>(1000);
    let endpoints = Arc::new(DashSet::new());
    let total_scanned = Arc::new(AtomicU64::new(0));

    // Warm up the DashSet from binary ledger
    {
        for i in 0..ledger.entry_count() {
            if let Some(entry) = ledger.get_entry(i) {
                let url = entry.get_url();
                if !url.is_empty() {
                    endpoints.insert(url);
                }
            }
        }
        info!("Loaded {} existing PDS endpoints from binary ledger.", endpoints.len());
    }

    let cursor_path = format!("{}.cursor", list_path);
    let after = if let Ok(ts) = std::fs::read_to_string(&cursor_path) {
        let ts = ts.trim().to_string();
        info!("Resuming discovery from cursor: {}", ts);
        ts
    } else {
        "2023-01-01T00:00:00.000Z".to_string()
    };

    // 1. Health Check & Interactive Prompt
    let client = reqwest::Client::new();
    info!("Checking PLC Directory rate limits...");
    let probe_url = format!("https://plc.directory/export?count=1&after={}", after);
    let resp = client.get(&probe_url).send().await?;
    
    if resp.status().as_u16() == 429 {
        error!("PLC Directory is currently rate-limiting this IP (429). Please wait a few minutes.");
        return Ok(());
    } else if resp.status().is_success() {
        print!("PLC is reachable. Resume discovery from {}? (y/n): ", after);
        std::io::stdout().flush()?;
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if input.trim().to_lowercase() != "y" {
            info!("Discovery aborted by user.");
            return Ok(());
        }
    } else {
        error!("Unexpected status from PLC: {}", resp.status());
        return Ok(());
    }

    // Spawn Discovery Module (Polite Single-Threaded Crawl)
    let discovery_tx = new_pds_tx.clone();
    let registry_discovery = Arc::clone(&endpoints);
    let total_scanned_spawn = Arc::clone(&total_scanned);
    let cursor_path_clone = cursor_path.clone();
    let mut after_loop = after.clone();
    
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let mut last_cursor_save = Instant::now();
        let mut target_delay = Duration::from_millis(500);
        let mut floor_delay = Duration::from_millis(150);
        let mut success_streak = 0;
        
        loop {
            let url = format!("https://plc.directory/export?count=1000&after={}", after_loop);
            match client.get(&url).send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.as_u16() == 429 {
                        success_streak = 0;
                        // The "Sweet Spot" Logic: If we hit a 429, the previous delay was our "Failure Point".
                        // We set a new Floor 50ms higher than that failure point.
                        let failure_point = target_delay;
                        floor_delay = (failure_point + Duration::from_millis(50)).min(Duration::from_secs(2));
                        target_delay = (floor_delay + Duration::from_millis(250)).min(Duration::from_secs(5));

                        warn!("SWEET SPOT SEARCH: {}ms was too fast. Setting new safe floor to {}ms and cooling down...", 
                            failure_point.as_millis(), floor_delay.as_millis());
                        
                        tokio::time::sleep(Duration::from_secs(120)).await;
                        continue;
                    }
                    if !status.is_success() {
                        success_streak = 0;
                        error!("Discovery: Request failed with status: {}. Backing off...", status);
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        continue;
                    }

                    // Success: Try to optimize toward the floor
                    success_streak += 1;
                    if success_streak >= 8 && target_delay > floor_delay {
                        target_delay -= Duration::from_millis(25);
                        success_streak = 0;
                        info!("Discovery Strategy: Optimizing toward sweet spot (Target: {}ms, Floor: {}ms)", 
                            target_delay.as_millis(), floor_delay.as_millis());
                    }

                    if let Ok(text) = resp.text().await {
                        if text.trim().is_empty() { 
                            // Reached the current tip
                            tokio::time::sleep(Duration::from_secs(60)).await;
                            continue;
                        }
                        
                        let mut current_page_max_ts = None;
                        for line in text.lines() {
                            total_scanned_spawn.fetch_add(1, Ordering::Relaxed);
                            
                            // POWERHOUSE: Using sonic_rs for SIMD-accelerated JSON scanning
                            if let Ok(v) = sonic_rs::from_str::<sonic_rs::Value>(line) {
                                if let Some(endpoint) = v.pointer(["operation", "services", "atproto_pds", "endpoint"])
                                    .as_str() {
                                    
                                    let mut pds_url = endpoint.to_string();
                                    if pds_url.starts_with("https://") {
                                        pds_url = pds_url.replace("https://", "wss://");
                                    }
                                    if !pds_url.ends_with("/xrpc/com.atproto.sync.subscribeRepos") {
                                        pds_url = format!("{}/xrpc/com.atproto.sync.subscribeRepos", pds_url.trim_end_matches('/'));
                                    }

                                    if !registry_discovery.contains(&pds_url) {
                                        if registry_discovery.insert(pds_url.clone()) {
                                            let _ = discovery_tx.send(pds_url).await;
                                        }
                                    }
                                }
                                if let Some(ts) = v.get("createdAt").as_str() {
                                    current_page_max_ts = Some(ts.to_string());
                                }
                            }
                        }

                        if let Some(ts) = current_page_max_ts {
                            after_loop = ts;
                            if last_cursor_save.elapsed() >= Duration::from_secs(10) {
                                let _ = std::fs::write(&cursor_path_clone, &after_loop);
                                last_cursor_save = Instant::now();
                            }
                        }
                    }
                    // Adaptive Throughput: Uses the dynamically adjusted target_delay
                    tokio::time::sleep(target_delay).await;
                }
                Err(e) => {
                    success_streak = 0;
                    target_delay = (target_delay + Duration::from_millis(200)).min(Duration::from_secs(5));
                    error!("Discovery: Network error: {}. Increasing delay to {}ms", e, target_delay.as_millis());
                    // Exponential backoff on network errors (10s -> 30s)
                    tokio::time::sleep(Duration::from_secs(15)).await;
                }
            }
        }
    });

    let mut last_report = Instant::now();
    let mut session_found = 0;
    let mut last_scanned = 0;
    let mut last_found = 0;

    loop {
        tokio::select! {
            Some(url) = new_pds_rx.recv() => {
                if let Some(entry) = PdsEntry::new(&url) {
                    ledger.append(&entry)?;
                    session_found += 1;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }

        if last_report.elapsed() >= Duration::from_secs(5) {
            let scanned = total_scanned.load(Ordering::Relaxed);
            if scanned > last_scanned || session_found > last_found {
                info!("DISCOVERY: Found {} new PDS nodes (Total Scanned: {})", session_found, scanned);
                let _ = ledger.flush();
                last_scanned = scanned;
                last_found = session_found;
            }
            last_report = Instant::now();
        }
    }
}

async fn run_siege(list_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting Sovereign Siege (Phase 2: Stress Test)");
    
    // 1. Connection Pooling setup - (Arc<String> source, Hash, Bytes)
    let (tx, mut rx) = mpsc::channel::<WorkerResponse>(500_000);
    let registry = Arc::new(PdsRegistry {
        endpoints: DashSet::new(),
        active_workers: DashSet::new(),
        ledger: Mutex::new(None),
        url_to_idx: DashMap::new(),
        bloom: Mutex::new(BloomFilter::with_num_bits(8 * 1024 * 1024).hashes(4)), // 1MB Bloom Filter
    });

    // 2. Load the PDS list (Prefer binary ledger)
    if list_path.ends_with(".bin") || std::path::Path::new(list_path).exists() && !list_path.ends_with(".txt") {
        let ledger = PdsLedger::open_or_create(list_path)?;
        for i in 0..ledger.entry_count() {
            if let Some(entry) = ledger.get_entry(i) {
                let url = entry.get_url();
                if !url.is_empty() {
                    registry.endpoints.insert(url.clone());
                    registry.url_to_idx.insert(url, i);
                }
            }
        }
        *registry.ledger.lock().unwrap() = Some(ledger);
    } else {
        let list_content = std::fs::read_to_string(list_path)?;
        for line in list_content.lines() {
            let ep = line.trim();
            if !ep.is_empty() {
                registry.endpoints.insert(ep.to_string());
            }
        }
    }
    
    let total_endpoints = registry.endpoints.len();
    
    // 3. Metrics Task
    let registry_agg = Arc::clone(&registry);
    let mut global_seq = 0u64;
    
    let mut msg_count = 0u64;
    let mut total_bytes = 0u64;
    let mut unique_count = 0u64;
    let mut last_report = Instant::now();

    let mut success_count = 0u64;
    let mut fail_count = 0u64;
    
    let mut seen_hashes: std::collections::VecDeque<[u8; 32]> = std::collections::VecDeque::new();
    let mut hash_set: std::collections::HashSet<[u8; 32]> = std::collections::HashSet::new();

    // 4. Ingestion & Storage Loop
    let mut join_set = JoinSet::new();
    let tx_worker = tx.clone();
    let max_concurrency = 10000; 

    info!("Starting Siege Phase: Automated Audit & Real-time Aggregation");
    info!("Total Target Nodes: {}", total_endpoints);

    let mut report_interval = tokio::time::interval(Duration::from_millis(1000));
    report_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut spawn_interval = tokio::time::interval(Duration::from_millis(50));
    spawn_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = spawn_interval.tick() => {
                // 5. Worker Management (Check if we need to spawn more)
                let active_count = registry.active_workers.len();
                if active_count < max_concurrency {
                    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
                    
                    let to_spawn: Vec<String> = registry.endpoints
                        .iter()
                        .filter(|e| {
                            if registry.active_workers.contains(e.as_str()) { return false; }
                            if let Some(idx) = registry.url_to_idx.get(e.as_str()) {
                                if let Some(l) = registry.ledger.lock().unwrap().as_ref() {
                                    if let Some(entry) = l.get_entry(*idx) {
                                        if entry.penalty_until > now { return false; }
                                    }
                                }
                            }
                            true
                        })
                        .take(200) // Aggressive ramp-up
                        .map(|e| e.clone())
                        .collect();

                    for endpoint in to_spawn {
                        spawn_worker(&endpoint, &tx_worker, &mut join_set, Arc::clone(&registry));
                        if registry.active_workers.len() >= max_concurrency { break; }
                    }
                }
            }
            _ = report_interval.tick() => {
                let dur = last_report.elapsed().as_secs_f64();
                if dur > 0.0 {
                    let rate = msg_count as f64 / dur;
                    let u_rate = unique_count as f64 / dur;
                    let mbps = (total_bytes as f64 * 8.0) / (dur * 1024.0 * 1024.0);
                    
                    // CLEAN MONITOR: Using \r to overwrite line for a "dashboard" feel
                    print!(
                        "\r[SIEGE] Active: {:>5} | OK={:<5} ERR={:<5} | {:>4.1}k msg/s (U:{:>4.1}k) | {:>5.1} Mbps | Seq: {:<8}", 
                        registry_agg.active_workers.len(),
                        success_count,
                        fail_count,
                        rate / 1000.0,
                        u_rate / 1000.0,
                        mbps,
                        global_seq
                    );
                    use std::io::Write;
                    std::io::stdout().flush().unwrap();
                    
                    msg_count = 0;
                    unique_count = 0;
                    total_bytes = 0;
                    last_report = Instant::now();
                    
                    if let Some(l) = registry.ledger.lock().unwrap().as_mut() {
                        let _ = l.flush();
                    }
                }
            }
            Some(msg) = rx.recv() => {
                match msg {
                    WorkerResponse::Connected(url) => {
                        success_count += 1;
                        if let Some(idx) = registry.url_to_idx.get(&url) {
                            if let Some(l) = registry.ledger.lock().unwrap().as_mut() {
                                if let Some(entry) = l.get_entry_mut(*idx) {
                                    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
                                    entry.last_success = now;
                                    entry.last_attempt = now;
                                    
                                    if entry.fail_count > 0 {
                                        entry.fail_count = 0;
                                        entry.penalty_until = 0;
                                    }
                                }
                            }
                        }
                    },
                    WorkerResponse::Success(_url_origin, hash, data) => {
                        msg_count += 1;
                        total_bytes += data.len() as u64;

                        // POWERHOUSE: Bloom Filter First Defense
                        let mut bloom = registry.bloom.lock().unwrap();
                        if !bloom.contains(&hash) {
                            bloom.insert(&hash);
                            
                            // Secondary HashSet for 100% collision safety
                            if !hash_set.contains(&hash) {
                                unique_count += 1;
                                hash_set.insert(hash);
                                seen_hashes.push_back(hash);
                                global_seq += 1;

                                if seen_hashes.len() > 500_000 {
                                    if let Some(old) = seen_hashes.pop_front() {
                                        hash_set.remove(&old);
                                    }
                                }
                            }
                        }
                    },
                    WorkerResponse::Failure(url) => {
                        fail_count += 1;
                        if let Some(idx) = registry.url_to_idx.get(&url) {
                            if let Some(l) = registry.ledger.lock().unwrap().as_mut() {
                                if let Some(entry) = l.get_entry_mut(*idx) {
                                    entry.fail_count += 1;
                                    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
                                    entry.last_attempt = now;
                                    let penalty_secs = (30 * 2u64.pow(entry.fail_count.min(7))).min(3600);
                                    entry.penalty_until = now + penalty_secs;
                                }
                            }
                        }
                    },
                    WorkerResponse::Closed => {}
                }
            }
            Some(res) = join_set.join_next() => {
                if let Err(e) = res {
                    error!("Worker task panicked: {:?}", e);
                }
            }
        }
    }
}


fn spawn_worker(
    endpoint: &str, 
    tx: &mpsc::Sender<WorkerResponse>, 
    join_set: &mut JoinSet<String>,
    registry: Arc<PdsRegistry>
) {
    let url = Arc::new(endpoint.to_string());
    let worker_tx = tx.clone();
    let reg = Arc::clone(&registry);

    reg.active_workers.insert(url.to_string());
    
    join_set.spawn(async move {
        loop {
            match connect_to_pds(Arc::clone(&url), &worker_tx).await {
                Ok(_) => {
                    let _ = worker_tx.send(WorkerResponse::Closed).await;
                    reg.active_workers.remove(url.as_ref());
                    break;
                }
                Err(_e) => {
                    let _ = worker_tx.send(WorkerResponse::Failure(url.to_string())).await;
                    reg.active_workers.remove(url.as_ref());
                    break; // Exit worker on failure to obey central penalty logic
                }
            }
        }
        url.to_string()
    });
}

async fn connect_to_pds(url_arc: Arc<String>, tx: &mpsc::Sender<WorkerResponse>) -> Result<(), String> {
    let url = Url::parse(&url_arc).map_err(|e| e.to_string())?;
    let (ws_stream, _) = connect_async(url).await.map_err(|e| e.to_string())?;
    
    // Notify aggregator we connected successfully (Recovery point)
    let _ = tx.send(WorkerResponse::Connected(url_arc.to_string())).await;

    let (_write, mut read) = ws_stream.split();

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Binary(data)) => {
                let bytes = Bytes::from(data);
                let hash = blake3::hash(&bytes).into();
                if tx.send(WorkerResponse::Success(Arc::clone(&url_arc), hash, bytes)).await.is_err() {
                    break; 
                }
            }
            Ok(Message::Close(_)) => break,
            Err(e) => {
                return Err(format!("Stream error on {}: {}", url_arc, e));
            }
            _ => {}
        }
    }

    Ok(())
}

async fn run_migration(txt_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    info!("Migrating {} to binary ledger format...", txt_path);
    let bin_path = txt_path.replace(".txt", ".bin");
    if bin_path == txt_path {
        return Err("Path must end in .txt to migrate to .bin".into());
    }

    let mut ledger = PdsLedger::open_or_create(&bin_path)?;
    let content = std::fs::read_to_string(txt_path)?;
    
    let mut added = 0;
    let mut existing = HashSet::new();
    
    // Scan ledger for existing to avoid duplicates in migration
    for i in 0..ledger.entry_count() {
        if let Some(entry) = ledger.get_entry(i) {
            existing.insert(entry.get_url());
        }
    }

    for line in content.lines() {
        let url = line.trim();
        if url.is_empty() { continue; }
        
        if !existing.contains(url) {
            if let Some(entry) = PdsEntry::new(url) {
                ledger.append(&entry)?;
                existing.insert(url.to_string());
                added += 1;
            }
        }
    }

    ledger.flush()?;
    info!("Migration complete. Added {} nodes. Total in ledger: {}.", added, ledger.entry_count());
    info!("Source of truth is now: {}", bin_path);
    Ok(())
}

async fn run_inspection(bin_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let ledger = PdsLedger::open_or_create(bin_path)?;
    let count = ledger.entry_count();
    
    let mut active = 0;
    let mut penalized = 0;
    let mut total_fails = 0u64;
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs();

    println!("--- Sovereign Ledger Inspection: {} ---", bin_path);
    println!("Total Entries: {}", count);
    
    for i in 0..count {
        if let Some(entry) = ledger.get_entry(i) {
            if entry.url[0] == 0 { continue; }
            
            total_fails += entry.fail_count as u64;
            if entry.last_success > 0 {
                active += 1;
            }
            if entry.penalty_until > now {
                penalized += 1;
            }
        }
    }

    println!("Nodes with some success: {}", active);
    println!("Nodes currently penalized: {}", penalized);
    println!("Total failures across mesh: {}", total_fails);
    
    if count > 0 {
        println!("\nSample Entries:");
        for i in 0..count.min(10) {
            if let Some(entry) = ledger.get_entry(i) {
                if entry.url[0] == 0 { continue; }
                let url = entry.get_url();
                let fails = entry.fail_count;
                let status = if entry.penalty_until > now { "Penalized" } else if entry.last_success > 0 { "Healthy" } else { "Fresh" };
                println!("  - {}: Fails={}, Status={}", url, fails, status);
            }
        }
    }

    Ok(())
}
