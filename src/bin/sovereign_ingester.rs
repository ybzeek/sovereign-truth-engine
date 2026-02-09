//! Sovereign Ingester: The direct PDS siege.
//! Connects to multiple high-grade PDS nodes simultaneously to bypass central relays.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;
use crossbeam_channel::{unbounded, Sender};
use url::Url;
use tungstenite::{connect, Message};
use serde::{Deserialize, Serialize};

use did_mmap_cache::mmap_did_cache::MmapDidCache;
use did_mmap_cache::archive::MultiShardArchive;
use did_mmap_cache::monitor::{SovereignMonitor, ErrorType};
use did_mmap_cache::parser::core::parse_input;
use did_mmap_cache::resolver::{resolve_did, resolve_handle};
use did_mmap_cache::verify::verify_commit;
use did_mmap_cache::mst::car::CarStore;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Path to mesh_map.json
    #[arg(short, long, default_value = "mesh_map.json")]
    mesh: String,

    /// Minimum grade to include (A, B, C, etc.)
    #[arg(short, long, default_value = "A")]
    min_grade: String,

    /// Max concurrent connections
    #[arg(short, long, default_value_t = 150)]
    max_conns: usize,

    /// Path to mmap cache
    #[arg(short, long, default_value = "atomic_cache.bin")]
    cache: String,

    /// Path to archive directory
    #[arg(short, long, default_value = "sovereign_archive")]
    archive: String,

    /// Dry run: Do not save data to archive
    #[arg(long)]
    dry_run: bool,

    /// Live mode: Ignore saved pds_cursors.json and start from current head
    #[arg(long)]
    live: bool,

    /// Delay between new connection attempts in milliseconds
    #[arg(long, default_value_t = 100)]
    conn_delay: u64,

    /// Relay URL to compare against (can be specified multiple times)
    #[arg(long)]
    relay: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct PdsReport {
    url: String,
    hostname: String,
    grade: String,
}

struct SharedState {
    monitor: Arc<SovereignMonitor>,
    global_seq: AtomicU64,
    archive: Arc<MultiShardArchive>,
    cache: Arc<RwLock<MmapDidCache>>,
    running: Arc<AtomicBool>,
    dry_run: bool,
    pds_cursors: Arc<DashMap<String, u64>>,
    blocked_pds: Arc<DashMap<String, bool>>,
    arrival_log: Arc<DashMap<Vec<u8>, (Instant, bool, bool)>>, // CID -> (Time, IsRelay, WasMatched)
    ghost_content: Arc<DashMap<Vec<u8>, (String, Vec<u8>)>>, // CID -> (SourceHost, Raw Message)
    relay_hosts: Arc<DashMap<String, bool>>,
}

use dashmap::DashMap;

fn main() -> Result<()> {
    let args = Args::parse();

    // 1. Load Mesh Map, Cursors, and Blocklist
    let mesh_data = fs::read_to_string(&args.mesh)?;
    let all_nodes: Vec<PdsReport> = serde_json::from_str(&mesh_data)?;
    
    let pds_cursors = Arc::new(DashMap::new());
    if !args.live {
        if let Ok(cursor_data) = fs::read_to_string("pds_cursors.json") {
            if let Ok(map) = serde_json::from_str::<HashMap<String, u64>>(&cursor_data) {
                for (k, v) in map { pds_cursors.insert(k, v); }
            }
        }
    }

    let blocked_pds = Arc::new(DashMap::new());
    if let Ok(block_data) = fs::read_to_string("pds_blocked.json") {
        if let Ok(list) = serde_json::from_str::<Vec<String>>(&block_data) {
            for host in list { blocked_pds.insert(host, true); }
            println!("[Sovereign] Loaded {} blocked (private) PDS nodes.", blocked_pds.len());
        }
    }
    
    let targets: Vec<PdsReport> = all_nodes.into_iter()
        .filter(|n| {
            if blocked_pds.contains_key(&n.hostname) { return false; }
            
            let grade = n.grade.to_uppercase();
            let min_grade = args.min_grade.to_uppercase();
            
            // Grades are A, B, C, D, E, F (F is fail)
            // A is better than B, etc.
            // If min_grade is 'C', we want A, B, C.
            
            if grade == "F" { return false; } // Never include Grade F
            
            let grade_val = match grade.as_str() {
                "A" => 1,
                "B" => 2,
                "C" => 3,
                "D" => 4,
                "E" => 5,
                _ => 10,
            };
            
            let min_val = match min_grade.as_str() {
                "A" => 1,
                "B" => 2,
                "C" => 3,
                "D" => 4,
                "E" => 5,
                _ => 0, // Include everything except F
            };
            
            grade_val <= min_val
        })
        .take(args.max_conns)
        .collect();

    println!("[Sovereign] Initializing with {} PDS targets...", targets.len());

    // 2. Initialize Infrastructure
    let cache = Arc::new(RwLock::new(MmapDidCache::open_mut(&args.cache)?));
    let dict = fs::read("atproto_firehose.dict").ok();
    // Balanced configuration: 16 shards for faster testing/visibility.
    // Segment size tuned to 500 for live head to see files quickly.
    let segment_size = if args.live { 500 } else { 50_000 };
    let archive = Arc::new(MultiShardArchive::new(&args.archive, 16, segment_size, dict)?);
    let monitor = Arc::new(SovereignMonitor::new());
    let global_seq = AtomicU64::new(0);
    let running = Arc::new(AtomicBool::new(true));
    let arrival_log = Arc::new(DashMap::new());
    let ghost_content = Arc::new(DashMap::new());
    let relay_hosts = Arc::new(DashMap::new());
    for r in &args.relay {
        if let Ok(u) = url::Url::parse(r) {
            let host = u.host_str().unwrap_or(r).to_string();
            relay_hosts.insert(host, true);
        } else {
            relay_hosts.insert(r.clone(), true);
        }
    }

    let state = Arc::new(SharedState {
        monitor,
        global_seq,
        archive,
        cache,
        running: Arc::clone(&running),
        dry_run: args.dry_run,
        pds_cursors: Arc::clone(&pds_cursors),
        blocked_pds: Arc::clone(&blocked_pds),
        arrival_log,
        ghost_content,
        relay_hosts,
    });

    // Handle Shutdown
    let running_ctrlc = Arc::clone(&running);
    ctrlc::set_handler(move || {
        println!("\n[Shutdown] Stop signal received. Finishing loops...");
        running_ctrlc.store(false, Ordering::SeqCst);
    })?;

    let (tx, rx) = unbounded::<(String, Vec<u8>)>();

    // 3. Thread Spawner Helper
    // We limit the stack size to 256KB per thread (vs 2-8MB default)
    // to allow 10,000+ threads without eating all RAM.
    let spawn_optimized = |name: String, f: Box<dyn FnOnce() + Send + 'static>| {
        thread::Builder::new()
            .name(name)
            .stack_size(256 * 1024) // 256 KB
            .spawn(f)
            .expect("Failed to spawn optimized thread")
    };

    // background handle resolver
    let state_h = Arc::clone(&state);
    let running_h = Arc::clone(&running);
    spawn_optimized("handle-resolver".to_string(), Box::new(move || {
        use did_mmap_cache::resolver::resolve_handle;
        while running_h.load(Ordering::SeqCst) {
            let mut to_resolve = Vec::new();
            {
                let board = &state_h.monitor.leaderboard;
                let mut entries: Vec<_> = board.iter().map(|e| (e.key().clone(), *e.value())).collect();
                entries.sort_by(|a, b| b.1.cmp(&a.1));
                for (did, _) in entries.iter().take(20) {
                    if !state_h.monitor.handle_cache.contains_key(did) {
                        to_resolve.push(did.clone());
                    }
                }
            }

            for did in to_resolve {
                if let Some(handle) = resolve_handle(&did) {
                    state_h.monitor.handle_cache.insert(did, handle);
                } else {
                    state_h.monitor.handle_cache.insert(did, "unresolved".to_string());
                }
                thread::sleep(Duration::from_millis(100)); // Be nice to PLC dir
            }
            thread::sleep(Duration::from_secs(5));
        }
    }));

    // 4. Processing Pipeline (Verification & Archival)
    // Start these BEFORE connections so they are ready to catch messages immediately
    // Increased to 4x CPUs to handle threads blocked on DID resolution network I/O.
    let num_verifiers = num_cpus::get() * 4;
    for i in 0..num_verifiers {
        let rx = rx.clone();
        let state = Arc::clone(&state);
        spawn_optimized(format!("verifier-{}", i), Box::new(move || {
            while let Ok((pds_host, msg)) = rx.recv() {
                process_sovereign_message(msg, pds_host, &state);
            }
        }));
    }

    // 5. Start Monitor Dashboard in a background thread
    let state_monitor = Arc::clone(&state);
    let rx_monitor = rx.clone();
    let running_monitor = Arc::clone(&running);
    spawn_optimized("monitor-ui".to_string(), Box::new(move || {
        let mut last_total = 0;
        let mut last_time = Instant::now();
        while running_monitor.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(500));
            let total = state_monitor.monitor.total.load(Ordering::Relaxed);
            let now = Instant::now();
            let delta_total = total - last_total;
            let delta_time = now.duration_since(last_time).as_secs_f64();
            let rate = delta_total as f64 / delta_time;
            
            state_monitor.monitor.render(rx_monitor.len(), rate);
            last_total = total;
            last_time = now;
        }
    }));

    // Cleanup & Ghost Detection Thread
    let state_ghosts = Arc::clone(&state);
    let running_ghosts = Arc::clone(&running);
    spawn_optimized("ghost-detector".to_string(), Box::new(move || {
        println!("[Sovereign] Ghost Detection Thread started.");
        while running_ghosts.load(Ordering::SeqCst) {
            state_ghosts.monitor.ghost_hunter_loops.fetch_add(1, Ordering::Relaxed);
            thread::sleep(Duration::from_millis(500));
            let now = Instant::now();
            let mut drops_count = 0;
            let mut to_remove = Vec::new();
            
            let log_len = state_ghosts.arrival_log.len();
            if log_len > 0 && state_ghosts.monitor.ghost_hunter_loops.load(Ordering::Relaxed) % 10 == 0 {
                if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open("ghost_hunter.log") {
                    let _ = writeln!(f, "Scanning {} entries...", log_len);
                }
            }

            for entry in state_ghosts.arrival_log.iter() {
                let (time, is_relay, was_matched) = *entry.value();
                let age = now.duration_since(time);

                if !was_matched && !is_relay && age > Duration::from_secs(3) {
                    // MESH saw it, RELAY didn't in window.
                    drops_count += 1;
                    
                    // Log to relay_drops.log
                    if let Some(content_val) = state_ghosts.ghost_content.get(entry.key()) {
                        let (source_host, msg_bytes) = content_val.value();
                        let cid_hex = hex::encode(entry.key());
                        let mut snippet = String::from("No block content");
                        let mut info = String::from("?");
                        
                        if let Some(envelope) = parse_input(msg_bytes) {
                            if let Some(did_bytes) = envelope.did {
                                let did_str = std::str::from_utf8(did_bytes).unwrap_or("?");
                                
                                let handle = if let Some(h) = state_ghosts.monitor.handle_cache.get(did_str) {
                                    h.value().clone()
                                } else if let Some(h) = resolve_handle(did_str) {
                                    state_ghosts.monitor.handle_cache.insert(did_str.to_string(), h.clone());
                                    h
                                } else {
                                    did_str.to_string()
                                };

                                if let Some(blocks) = envelope.blocks {
                                    if let Some(s) = extract_better_snippet(blocks) {
                                        snippet = s;
                                    }
                                }
                                info = format!("Handle: {} (Source: {})", handle, source_host);
                            }
                        }
                        
                        if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open("relay_drops.log") {
                            use std::io::Write;
                            let _ = writeln!(f, "[DROP] CID: {} | {} | Sample: {}", cid_hex, info, snippet);
                        }
                        
                        // Push to Monitor TUI
                        state_ghosts.monitor.push_drop(format!("{} dropped {}", info, cid_hex));
                    }

                    // Mark as 'matched' (handled) so we dont count again
                    to_remove.push(entry.key().clone());
                } else if age > Duration::from_secs(60) {
                    // Old entries (matched or relay-first) - safe to purge from RAM
                    to_remove.push(entry.key().clone());
                }
            }

            if drops_count > 0 {
                state_ghosts.monitor.dropped_by_relay.fetch_add(drops_count, Ordering::Relaxed);
                state_ghosts.monitor.healed.fetch_add(drops_count, Ordering::Relaxed);
            }

            for key in to_remove {
                state_ghosts.arrival_log.remove(&key);
                state_ghosts.ghost_content.remove(&key);
            }
        }
    }));

    // 6. Spawn Connection Workers (Staggered Ramp-Up)
    let mut workers = Vec::new();

    // Start Relay Workers
    for relay_url in args.relay {
        println!("[Sovereign] Starting Relay Audit on {}...", relay_url);
        let state = Arc::clone(&state);
        let tx = tx.clone();
        let live = args.live;
        let url_copy = relay_url.clone();
        workers.push(spawn_optimized(format!("relay-{}", relay_url), Box::new(move || {
            worker_loop(url_copy, state, tx, live);
        })));
    }

    // Start Mesh Workers
    for node in targets {
        let node_url = node.url.clone();
        let state = Arc::clone(&state);
        let tx = tx.clone();
        let live = args.live;
        let host_copy = node.hostname.clone();
        
        workers.push(spawn_optimized(format!("pds-{}", host_copy), Box::new(move || {
            worker_loop(node_url, state, tx, live);
        })));

        if args.conn_delay > 0 {
            thread::sleep(Duration::from_millis(args.conn_delay));
        }
    }

    drop(tx); // Close the channel from the main thread so verifiers can exit when workers finish

    // Keep the main thread alive until shutdown
    while running.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_secs(1));
    }

    println!("[Shutdown] Saving final cursors and closing archive...");
    
    // 1. Signal Archive to flush
    state.archive.shutdown();
    
    // 2. Save Cursors
    let mut final_map = HashMap::new();
    for entry in state.pds_cursors.iter() {
        final_map.insert(entry.key().clone(), *entry.value());
    }
    if let Ok(json) = serde_json::to_string_pretty(&final_map) {
        match fs::write("pds_cursors.json", json) {
            Ok(_) => println!("[Shutdown] Saved {} cursors.", final_map.len()),
            Err(e) => eprintln!("[Shutdown] Failed to save cursors: {}", e),
        }
    }

    // 3. Save Blocked PDS (Blacklist)
    let blocked_list: Vec<String> = state.blocked_pds.iter().map(|e| e.key().clone()).collect();
    if let Ok(json) = serde_json::to_string_pretty(&blocked_list) {
        match fs::write("pds_blocked.json", json) {
            Ok(_) => println!("[Shutdown] Saved {} blocked nodes.", blocked_list.len()),
            Err(e) => eprintln!("[Shutdown] Failed to save blocklist: {}", e),
        }
    }

    // Give it a second to clean up network threads
    thread::sleep(Duration::from_millis(500));
    
    println!("[Shutdown] Finalizing archive segments...");
    state.archive.shutdown();

    println!("[Shutdown] Complete.");

    Ok(())
}

fn worker_loop(pds_url: String, state: Arc<SharedState>, tx: Sender<(String, Vec<u8>)>, start_live: bool) {
    let hostname = match Url::parse(&pds_url) {
        Ok(u) => {
            let host = u.host_str().unwrap_or("unknown").trim().to_string();
            if let Some(port) = u.port() {
                format!("{}:{}", host, port)
            } else {
                host
            }
        }
        Err(_) => pds_url.trim()
            .trim_start_matches("wss://")
            .trim_start_matches("ws://")
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .split('/')
            .next()
            .unwrap_or("")
            .to_string(),
    };

    if hostname.is_empty() || hostname.contains(' ') || hostname.contains('\n') || hostname.len() > 128 {
        return;
    }
    
    let mut session_started = false;
    while state.running.load(Ordering::SeqCst) {
        let cursor = if start_live && !session_started { 
            None 
        } else { 
            state.pds_cursors.get(&hostname).map(|e| *e.value()) 
        };
        
        let mut ws_url = format!("wss://{}/xrpc/com.atproto.sync.subscribeRepos", hostname);
        if let Some(c) = cursor {
            ws_url.push_str(&format!("?cursor={}", c));
        }

        match connect(&ws_url) {
            Ok((mut socket, _)) => {
                session_started = true;
                // Set a read timeout so we can send Pings if the connection is idle
                let stream = socket.get_mut();
                let _ = match stream {
                    tungstenite::stream::MaybeTlsStream::Plain(s) => s.set_read_timeout(Some(Duration::from_secs(20))),
                    tungstenite::stream::MaybeTlsStream::Rustls(s) => s.get_mut().set_read_timeout(Some(Duration::from_secs(20))),
                    _ => Ok(()),
                };

                state.monitor.active_conns.fetch_add(1, Ordering::Relaxed);
                while state.running.load(Ordering::SeqCst) {
                    match socket.read() {
                        Ok(msg) => {
                            if let Message::Binary(bin) = msg {
                                if tx.send((hostname.clone(), bin)).is_err() { 
                                    state.monitor.active_conns.fetch_sub(1, Ordering::Relaxed);
                                    return; 
                                }
                            }
                        }
                        Err(tungstenite::Error::Io(e)) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                            // idle timeout: send a Ping to keep the connection alive
                            if socket.send(Message::Ping(Vec::new())).is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            state.monitor.conn_errors.fetch_add(1, Ordering::Relaxed);
                            
                            // Log unexpected drops
                            if let Ok(mut file) = fs::OpenOptions::new().create(true).append(true).open("sovereign_errors.log") {
                                use std::io::Write;
                                let _ = writeln!(file, "[{}] Drop on {}: {:?}", chrono::Local::now(), hostname, e);
                            }

                            if !state.running.load(Ordering::SeqCst) { return; }
                            break; 
                        }
                    }
                }
                // If we exit the inner while loop
                state.monitor.active_conns.fetch_sub(1, Ordering::SeqCst);
                thread::sleep(Duration::from_secs(5)); // Back off after connection drop to avoid spinning
            }
            Err(e) => {
                state.monitor.conn_errors.fetch_add(1, Ordering::Relaxed);
                
                // CRITICAL: Handle Authentication Required (401), Not Found (404), Forbidden (403), etc.
                // If the PDS is private or misconfigured, stop retrying it to save resources.
                // We expand this to any 4xx or 5xx that indicates it's not a public valid firehose,
                // or a 200 OK which means it's returning a webpage instead of upgrading to WS.
                let is_unrecoverable = match &e {
                    tungstenite::Error::Http(resp) => {
                        let s = resp.status().as_u16();
                        s == 400 || s == 401 || s == 403 || s == 404 || s >= 500 || s == 200
                    },
                    tungstenite::Error::Url(tungstenite::error::UrlError::UnsupportedUrlScheme) => true,
                    _ => false,
                };

                if let Ok(mut file) = fs::OpenOptions::new().create(true).append(true).open("sovereign_errors.log") {
                    use std::io::Write;
                    let _ = writeln!(file, "[{}] Failed to connect to {} (via {}): {:?}", chrono::Local::now(), hostname, ws_url, e);
                }

                if is_unrecoverable {
                    let reason = if let tungstenite::Error::Http(resp) = &e {
                        format!("HTTP {}", resp.status())
                    } else if matches!(e, tungstenite::Error::Url(_)) {
                        "Unsupported URL Scheme".to_string()
                    } else {
                        "Unrecoverable".to_string()
                    };
                    
                    if let Ok(mut file) = fs::OpenOptions::new().create(true).append(true).open("sovereign_errors.log") {
                        use std::io::Write;
                        let _ = writeln!(file, "[{}] BLACKLISTED {} status: {}", chrono::Local::now(), hostname, reason);
                    }

                    state.blocked_pds.insert(hostname, true);
                    return; // EXIT WORKER THREAD
                }

                if !state.running.load(Ordering::SeqCst) { return; }
                thread::sleep(Duration::from_secs(30)); // Back off longer for errors
            }
        }
    }
}

fn extract_better_snippet(blocks: &[u8]) -> Option<String> {
    let store = CarStore::new(blocks);
    
    // Higher priority fields for social posts
    let targets = [
        "\"text\":", 
        "\"displayName\":", 
        "\"description\":", 
        "\"subject\":", 
        "\"uri\":", 
        "\"val\":",  // Used in some labels/custom lexicons
        "\"$type\":" // To at least see what it is
    ];

    for block in store.blocks.values() {
        for target in targets {
            if let Some(pos) = block.windows(target.len()).position(|w| w == target.as_bytes()) {
                let start = pos + target.len();
                let mut end = start;
                while end < block.len() && end < start + 120 {
                    // Stop at quote (if not escaped) or comma/brace
                    if block[end] == b'\"' && (end == 0 || block[end-1] != b'\\') {
                        break;
                    }
                    if block[end] == b',' || block[end] == b'}' || block[end] == b']' {
                        break;
                    }
                    end += 1;
                }
                if end > start {
                    let text = String::from_utf8_lossy(&block[start..end])
                        .to_string()
                        .trim_matches(|c| c == '\"' || c == ' ')
                        .to_string();
                    
                    // Prefix with the field type to make it clearer
                    let field = target.trim_matches(|c| c == '\"' || c == ':');
                    return Some(format!("{}: {}", field, text));
                }
            }
        }
    }
    
    // Last resort: printable filter for anything that looks like content
    for block in store.blocks.values() {
        let readable: String = block.iter()
            .filter(|&&b| b >= 32 && b <= 126)
            .map(|&b| b as char)
            .collect();
        
        if readable.len() > 10 {
            // If it looks like a URI or has common structure
            if readable.contains("at://") {
                if let Some(idx) = readable.find("at://") {
                    return Some(readable[idx..].chars().take(80).collect());
                }
            }
            // Just return a chunk of printable text
            let chunk: String = readable.chars().take(50).collect();
            if !chunk.is_empty() {
                return Some(format!("Raw: {}...", chunk));
            }
        }
    }
    None
}

fn process_sovereign_message(msg: Vec<u8>, pds_host: String, state: &SharedState) {
    if let Some(envelope) = parse_input(&msg.clone()) {
        // Track per-PDS cursor
        if let Some(pds_seq) = envelope.sequence {
            state.pds_cursors.insert(pds_host.clone(), pds_seq);
        }

        // --- RELAY SHADOW LOGIC ---
        let is_relay = state.relay_hosts.contains_key(&pds_host);
        
        // Prefer record_cid for matching posts/likes, fallback to commit cid
        let target_cid = envelope.record_cid.or(envelope.cid);

        if let Some(mut cid) = target_cid {
            // Normalize CID: Remove leading 0x00 common in binary CID encoding
            if cid.first() == Some(&0x00) {
                cid = &cid[1..];
            }

            let now = Instant::now();
            let entry = state.arrival_log.get(cid);
            
            if let Some(prev) = entry {
                let (first_time, first_was_relay, already_matched) = *prev.value();
                if !already_matched && first_was_relay != is_relay {
                    if first_was_relay && !is_relay {
                        // Relay arrived first, Mesh just arrived.
                        state.monitor.relay_wins.fetch_add(1, Ordering::Relaxed);
                    } else if !first_was_relay && is_relay {
                        // Mesh arrived first, Relay just arrived.
                        state.monitor.mesh_wins.fetch_add(1, Ordering::Relaxed);
                        let diff = now.duration_since(first_time).as_millis() as u64;
                        state.monitor.total_lat_gain_ms.fetch_add(diff, Ordering::Relaxed);
                    }
                    
                    // Mark as matched so we don't count it again for other mesh nodes
                    drop(prev);
                    state.arrival_log.insert(cid.to_vec(), (first_time, first_was_relay, true));
                    
                    // If matched, we don't need to keep the content for drop inspection
                    state.ghost_content.remove(cid);
                }
            } else {
                // First time seeing this CID
                state.arrival_log.insert(cid.to_vec(), (now, is_relay, false));
                
                // If Mesh saw it first, store content for potential Drop Inspection
                if !is_relay {
                    state.ghost_content.insert(cid.to_vec(), (pds_host.clone(), msg.clone()));
                }
            }
        }
        // --------------------------

        // In Sovereign mode, we use a global monotonic sequence for the archive,
        let seq = state.global_seq.fetch_add(1, Ordering::Relaxed);

        if let Some(t) = envelope.t {
            if t == b"#commit" || t == b"commit" {
                // Proof of Decoding: Every 50 commits, push a snippet to the TUI
                if !is_relay && seq % 50 == 0 {
                    if let Some(blocks) = envelope.blocks.clone() {
                        if let Some(snippet) = extract_better_snippet(blocks) {
                            state.monitor.push_tap(snippet);
                        }
                    }
                }

                if let Some(did_bytes) = envelope.did {
                    if let Ok(did) = std::str::from_utf8(did_bytes) {
                        
                        let key_entry = {
                            let lock = state.cache.read().unwrap();
                            lock.get(did)
                        };

                        let key_entry = if key_entry.is_none() {
                            // Resolve missing keys via network (Slow Path)
                            if let Some((pk, kt)) = resolve_did(did) {
                                let mut lock = state.cache.write().unwrap();
                                lock.atomic_update_or_tombstone(did, Some(kt), Some(&pk));
                                Some((pk, kt))
                            } else {
                                None
                            }
                        } else {
                            key_entry
                        };

                        if let Some((mut pk, mut kt)) = key_entry {
                            // Verify and Archive
                            if verify_commit(&envelope, &pk, kt) {
                                state.monitor.record_event(did, true, None, Some(kt));
                                if !state.dry_run {
                                    // Handle operations (create/update/delete)
                                    let mut primary_path = "".to_string();
                                    for op in &envelope.ops {
                                        if op.action == "delete" {
                                            state.archive.delete_by_path(did, &op.path);
                                        } else if primary_path.is_empty() {
                                            primary_path = op.path.clone();
                                        }
                                    }
                                    state.archive.ingest(seq, did, primary_path, msg);
                                }
                            } else {
                                // Potential key rotation - try re-resolving (Slow Path)
                                let mut resolved_again = false;
                                if let Some((new_pk, new_kt)) = resolve_did(did) {
                                    if new_pk != pk || new_kt != kt {
                                        {
                                            let mut lock = state.cache.write().unwrap();
                                            lock.atomic_update_or_tombstone(did, Some(new_kt), Some(&new_pk));
                                        }
                                        pk = new_pk;
                                        kt = new_kt;
                                        if verify_commit(&envelope, &pk, kt) {
                                            resolved_again = true;
                                        }
                                    }
                                }

                                if resolved_again {
                                    state.monitor.record_event(did, true, None, Some(kt));
                                    if !state.dry_run {
                                        let mut primary_path = "".to_string();
                                        for op in &envelope.ops {
                                            if op.action == "delete" {
                                                state.archive.delete_by_path(did, &op.path);
                                            } else if primary_path.is_empty() {
                                                primary_path = op.path.clone();
                                            }
                                        }
                                        state.archive.ingest(seq, did, primary_path, msg);
                                    }
                                } else {
                                    state.monitor.record_event(did, false, Some(ErrorType::InvalidSignature), Some(kt));
                                    if let Ok(mut file) = fs::OpenOptions::new().create(true).append(true).open("sovereign_errors.log") {
                                        use std::io::Write;
                                        let _ = writeln!(file, "[{}] INVALID SIG from {} for DID {}", chrono::Local::now(), pds_host, did);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
