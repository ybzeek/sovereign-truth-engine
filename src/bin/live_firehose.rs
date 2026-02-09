//! Live Firehose Consumer for ATProto (High Performance Multithreaded Edition)
//! Connects to the Bluesky firehose and verifies commit frames using mmap cache

use did_mmap_cache::mmap_did_cache::MmapDidCache;
use did_mmap_cache::parser::core::{parse_input, CommitEnvelope};
use did_mmap_cache::resolver::resolve_did;
use did_mmap_cache::monitor::{SovereignMonitor, ErrorType};
use did_mmap_cache::mst::{MstNode, visualize::draw_mst_visual};
use did_mmap_cache::mst::car::CarStore;
use tungstenite::Message;
use url::Url;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::collections::HashMap;
use std::thread;
use std::fs;
use std::cell::RefCell;
use crossbeam_channel::unbounded;

use k256::ecdsa::{VerifyingKey as K256VerifyingKey, Signature as K256Signature};
use p256::ecdsa::{VerifyingKey as P256VerifyingKey, Signature as P256Signature};
use k256::ecdsa::signature::hazmat::PrehashVerifier as _;
use sha2::{Digest, Sha256};

#[derive(Clone, Debug)]
enum ParsedKey {
    Secp256k1(K256VerifyingKey),
    P256(P256VerifyingKey),
}

thread_local! {
    static KEY_CACHE: RefCell<HashMap<String, (ParsedKey, [u8; 33])>> = RefCell::new(HashMap::with_capacity(5000));
}

fn resolve_did_cached(did: &str, cache: &Arc<RwLock<MmapDidCache>>) -> Option<(ParsedKey, [u8; 33])> {
    // Phase 0: Thread Local (No Lock)
    if let Some(entry) = KEY_CACHE.with(|c| c.borrow().get(did).cloned()) {
        return Some(entry);
    }

    // Phase 1: Mmap Cache (Read Lock)
    let (pubkey_bytes, key_type) = {
        let lock = cache.read().unwrap();
        lock.get(did)
    }?;

    let parsed = match key_type {
        1 => K256VerifyingKey::from_sec1_bytes(&pubkey_bytes).ok().map(ParsedKey::Secp256k1),
        2 => P256VerifyingKey::from_sec1_bytes(&pubkey_bytes).ok().map(ParsedKey::P256),
        _ => None,
    }?;

    let entry = (parsed, pubkey_bytes);
    KEY_CACHE.with(|c| {
        let mut map = c.borrow_mut();
        if map.len() >= 5000 { map.clear(); } 
        map.insert(did.to_string(), entry.clone());
    });
    return Some(entry);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <mmap_cache_file> [target_did]", args[0]);
        return;
    }
    let cache_path = &args[1];
    let target_did_filter = args.get(2).map(|s| s.to_string());

    // Zero-Stop: Load cursor from file
    let initial_cursor = fs::read_to_string("cursor.txt")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok());

    if let Some(c) = initial_cursor {
        println!("[Info] Resuming from sequence: {}", c);
    }

    println!("[Info] Opening cache: {}", cache_path);
    // Wrap cache in Arc<RwLock> safely so many threads can read and write
    let cache = Arc::new(RwLock::new(
        MmapDidCache::open_mut(cache_path).expect("Failed to open cache")
    ));

    // Track DIDs currently being resolved and buffer messages for them
    let pending_resolutions = Arc::new(Mutex::new(HashMap::<String, Vec<Vec<u8>>>::new()));

    let monitor = Arc::new(SovereignMonitor::new());
    let last_seq = Arc::new(AtomicU64::new(initial_cursor.unwrap_or(0)));
    let running = Arc::new(AtomicBool::new(true));

    // Zero-Stop: Set up Graceful Shutdown
    let last_seq_ctrlc = Arc::clone(&last_seq);
    let running_ctrlc = Arc::clone(&running);
    ctrlc::set_handler(move || {
        println!("\n[Shutdown] Control-C detected. Finishing work and saving cursor...");
        running_ctrlc.store(false, Ordering::SeqCst);
        let final_seq = last_seq_ctrlc.load(Ordering::SeqCst);
        if final_seq > 0 {
            fs::write("cursor.txt", final_seq.to_string()).expect("Failed to save cursor.txt");
            println!("[Shutdown] Saved cursor: {}", final_seq);
        }
        std::process::exit(0);
    }).expect("Error setting Ctrl-C handler");

    // Channel for frames: Producer pushes, Workers pull
    let (tx, rx) = unbounded::<Vec<u8>>();

    // 1. Ingestion Thread (The Producer)
    // This thread does NOTHING but read from the socket and push to the queue.
    let running_ingest = Arc::clone(&running);
    let last_seq_ingest = Arc::clone(&last_seq);

    thread::spawn(move || {
        while running_ingest.load(Ordering::SeqCst) {
            let current_cursor = last_seq_ingest.load(Ordering::Relaxed);
            let mut firehose_url = "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos".to_string();
            if current_cursor > 0 {
                firehose_url.push_str(&format!("?cursor={}", current_cursor));
            }
            
            let url = Url::parse(&firehose_url).unwrap();
            let host = url.host_str().unwrap();
            let port = url.port_or_known_default().unwrap();
            let addr = format!("{}:{}", host, port);
            
            println!("[Info] Connecting to {}... (cursor={})", addr, current_cursor);
            
            println!("[Info] Connecting to {}...", firehose_url);
            
            let mut socket = match tungstenite::connect(url.as_str()) {
                Ok((s, _)) => s,
                Err(e) => {
                    eprintln!("[Error] Websocket connect failed: {}. Retrying in 5s...", e);
                    thread::sleep(std::time::Duration::from_secs(5));
                    continue;
                }
            };
            
            println!("[Info] Connected to firehose.");

            while running_ingest.load(Ordering::SeqCst) {
                match socket.read() {
                    Ok(msg) => {
                        if let Message::Binary(bin) = msg {
                            if tx.send(bin).is_err() { return; } // Channel closed
                        }
                    }
                    Err(e) => {
                        eprintln!("[Error] Websocket error: {}. Reconnecting...", e);
                        break; // Break inner loop to trigger reconnect
                    }
                }
            }
            
            if running_ingest.load(Ordering::SeqCst) {
                thread::sleep(std::time::Duration::from_secs(2)); // Small breathing room
            }
        }
    });

    // 2. Worker Threads (The Verification Pool)
    let logical_cpus = num_cpus::get();
    let num_workers = logical_cpus; // 1:1 ratio for pure CPU tasks
    println!("[Info] Detected {} hardware threads. Spawning {} verification workers...", logical_cpus, num_workers);
    
    for _ in 0..num_workers {
        let rx = rx.clone();
        let cache = Arc::clone(&cache);
        let pending_resolutions = Arc::clone(&pending_resolutions);
        let monitor = Arc::clone(&monitor);
        let last_seq = Arc::clone(&last_seq);
        let filter_did = target_did_filter.clone();

        thread::spawn(move || {
            while let Ok(msg) = rx.recv() {
                process_message(msg, &cache, &pending_resolutions, &monitor, &last_seq, filter_did.as_deref());
            }
        });
    }

    // 3. Monitor Thread (The UI Dashboard)
    let mut last_total = 0;
    let mut last_time = std::time::Instant::now();
    
    while running.load(Ordering::SeqCst) {
        thread::sleep(std::time::Duration::from_millis(500)); // Update dashboard twice per second
        let total = monitor.total.load(Ordering::Relaxed);
        
        let now = std::time::Instant::now();
        let delta_total = total - last_total;
        let delta_time = now.duration_since(last_time).as_secs_f64();
        let rate = delta_total as f64 / delta_time;
        
        monitor.render(rx.len(), rate);
        
        last_total = total;
        last_time = now;
    }
}

fn process_message(
    msg: Vec<u8>, 
    cache: &Arc<RwLock<MmapDidCache>>, 
    pending_resolutions: &Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>>,
    monitor: &Arc<SovereignMonitor>,
    last_seq: &AtomicU64,
    filter_did: Option<&str>
) {
    if let Some(envelope) = parse_input(&msg) {
        // Track the cursor
        if let Some(seq) = envelope.sequence {
            let current = last_seq.load(Ordering::Relaxed);
            if seq > current {
                last_seq.store(seq, Ordering::Relaxed);
            }
        }

        if let Some(t_bytes) = envelope.t {
            if t_bytes == b"#commit" || t_bytes == b"commit" {
                if let Some(did_bytes) = envelope.did {
                    if let Ok(did) = std::str::from_utf8(did_bytes) {
                        
                        // Ultra-Fast Path: Thread-Local + Pre-Parsed Key
                        let mut key_entry = resolve_did_cached(did, cache);

                        // Phase 2: SLOW PATH (Network Resolution)
                        if key_entry.is_none() {
                            let mut pending = pending_resolutions.lock().unwrap();
                            if let Some(backlog) = pending.get_mut(did) {
                                // Already being resolved, just buffer this message
                                backlog.push(msg.clone());
                            } else {
                                // Not being resolved, start resolution and buffer this message
                                pending.insert(did.to_string(), vec![msg.clone()]);
                                
                                // Release lock before network call
                                drop(pending);

                                if let Some((pk, kt)) = resolve_did(did) {
                                    monitor.healed.fetch_add(1, Ordering::Relaxed);
                                    let mut lock = cache.write().unwrap();
                                    lock.atomic_update_or_tombstone(did, Some(kt), Some(&pk));
                                    
                                    let parsed = match kt {
                                        1 => K256VerifyingKey::from_sec1_bytes(&pk).ok().map(ParsedKey::Secp256k1),
                                        2 => P256VerifyingKey::from_sec1_bytes(&pk).ok().map(ParsedKey::P256),
                                        _ => None,
                                    };
                                    if let Some(p) = parsed {
                                        key_entry = Some((p, pk));
                                    }
                                }

                                // Resolution finished, retrieve backlog and clear pending entry
                                let mut pending = pending_resolutions.lock().unwrap();
                                let backlog = pending.remove(did).unwrap_or_default();
                                drop(pending);

                                // Process all messages in the backlog now that we have the key
                                if let Some((parsed, pk)) = &key_entry {
                                    for b_msg in backlog {
                                        if let Some(env) = parse_input(&b_msg) {
                                            verify_envelope(&env, parsed, pk, did, monitor, cache, filter_did);
                                        }
                                    }
                                    return; // Already processed this message as part of the backlog
                                }
                            }
                        }

                        if let Some((parsed, pk)) = key_entry {
                            verify_envelope(&envelope, &parsed, &pk, did, monitor, cache, filter_did);
                        } else { 
                            monitor.record_event(did, false, Some(ErrorType::MissingKey), None);
                        }
                    }
                }
            }
        }
    }
}

fn verify_envelope(
    envelope: &CommitEnvelope,
    pubkey: &ParsedKey,
    pubkey_bytes: &[u8; 33],
    did: &str,
    monitor: &Arc<SovereignMonitor>,
    cache: &Arc<RwLock<MmapDidCache>>,
    filter_did: Option<&str>
) {
    let kt_val = match pubkey { ParsedKey::Secp256k1(_) => 1, ParsedKey::P256(_) => 2 };

    if verify_commit(envelope, pubkey) {
        monitor.record_event(did, true, None, Some(kt_val));

        // MST VISUALIZER: Trigger ONLY if it's our specific target DID
        let is_target = filter_did.map_or(false, |f| f == did);

        if is_target {
            println!("\n[MST VISUALIZER] Update for {}", did);
            if let Some(commit_data) = envelope.commit {
                if let Some(root_cid) = MstNode::get_root_from_commit(commit_data) {
                    println!("  [*] Root CID: {}", root_cid);
                    if let Some(blocks) = envelope.blocks {
                        let store = CarStore::new(blocks);
                        let root_cid_bytes = root_cid.to_bytes();
                        if let Some(root_block) = store.get_block(&root_cid_bytes) {
                            if let Ok(root_node) = MstNode::from_bytes(root_block) {
                                draw_mst_visual(&root_node, &store, 0, Vec::new());
                            }
                        }
                    }
                }
            }
            println!("[MST VISUALIZER - END]\n");
        }
    } else {
        // Phase 3: STALE CACHE RECOVERY
        // Key might have rotated? 
        if let Some((fresh_pk, fresh_kt)) = resolve_did(did) {
            if fresh_pk != *pubkey_bytes {
                monitor.healed.fetch_add(1, Ordering::Relaxed);
                let mut lock = cache.write().unwrap();
                lock.atomic_update_or_tombstone(did, Some(fresh_kt), Some(&fresh_pk));
                
                let fresh_key = match fresh_kt {
                    1 => K256VerifyingKey::from_sec1_bytes(&fresh_pk).ok().map(ParsedKey::Secp256k1),
                    2 => P256VerifyingKey::from_sec1_bytes(&fresh_pk).ok().map(ParsedKey::P256),
                    _ => None,
                };

                if let Some(fk) = fresh_key {
                    if verify_commit(envelope, &fk) {
                        monitor.record_event(did, true, None, Some(fresh_kt));
                    } else {
                        monitor.record_event(did, false, Some(ErrorType::InvalidSignature), Some(fresh_kt));
                    }
                }
            } else {
                monitor.record_event(did, false, Some(ErrorType::InvalidSignature), Some(kt_val));
            }
        } else {
            monitor.record_event(did, false, Some(ErrorType::MissingKey), Some(kt_val));
        }
    }
}

fn verify_commit(envelope: &CommitEnvelope, pubkey: &ParsedKey) -> bool {
    let sig_bytes = match envelope.signature { Some(b) => b, None => return false };
    let commit_raw = match envelope.commit { Some(b) => b, None => return false };
    
    // Zero-Copy Hash (Updates hasher directly from raw buffer slices)
    let mut hasher = Sha256::new();
    if did_mmap_cache::parser::canonical::hash_canonical_commit(commit_raw, &mut hasher) {
        let hash = hasher.finalize();

        match pubkey {
            ParsedKey::Secp256k1(vk) => {
                if let Ok(sig) = K256Signature::from_slice(sig_bytes) {
                    vk.verify_prehash(&hash, &sig).is_ok()
                } else { false }
            },
            ParsedKey::P256(vk) => {
                if let Ok(sig) = P256Signature::from_slice(sig_bytes) {
                    vk.verify_prehash(&hash, &sig).is_ok()
                } else { false }
            }
        }
    } else {
        false
    }
}
