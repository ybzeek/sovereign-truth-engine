//! Integrated Pipeline Stress Test
//! Verifies: WebSocket -> Parsers -> Sig Verify -> Zstd Compression -> Archive Disk Write
//! Reports: Per-stage throughput and queue depths to identify bottlenecks.

use did_mmap_cache::mmap_did_cache::MmapDidCache;
use did_mmap_cache::parser::core::parse_input;
use did_mmap_cache::archive::ArchiveWriter;
use tungstenite::Message;
use url::Url;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::fs;
use std::time::{Instant, Duration};
use crossbeam_channel::unbounded;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <mmap_cache_file> <archive_dir>", args[0]);
        return;
    }
    let cache_path = &args[1];
    let archive_dir = &args[2];

    println!("[Info] Starting Integrated Pipeline Stress Test");
    println!("[Info] Cache: {}, Archive Dir: {}", cache_path, archive_dir);

    // Load DIDs for verification
    let _cache = Arc::new(RwLock::new(
        MmapDidCache::open(cache_path).expect("Failed to open cache")
    ));

    // Load Zstd dictionary
    let dict_path = "atproto_firehose.dict";
    let dict_data = fs::read(dict_path).expect("Missing atproto_firehose.dict - run capture_and_train first");
    println!("[Info] Loaded Zstd dictionary ({} bytes)", dict_data.len());

    // Setup ArchiveWriter (starts at seq 0 for test, or we could load from cursor)
    let initial_cursor = fs::read_to_string("cursor.txt")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(0);
        
    let archive_writer = Arc::new(Mutex::new(
        ArchiveWriter::new(archive_dir, 0, initial_cursor, 50_000, Some(dict_data.clone())).expect("Failed to init ArchiveWriter")
    ));

    // Stats
    let ingested = Arc::new(AtomicU64::new(0));
    let verified = Arc::new(AtomicU64::new(0));
    let compressed = Arc::new(AtomicU64::new(0));
    let raw_bytes = Arc::new(AtomicU64::new(0));
    let running = Arc::new(AtomicBool::new(true));
    let last_seq = Arc::new(AtomicU64::new(initial_cursor));

    // Channels
    let (tx_verify, rx_verify) = unbounded::<Vec<u8>>();
    let (tx_compress, rx_compress) = unbounded::<(u64, String, Vec<u8>)>();

    // 1. Ingestion Worker
    let running_ingest = Arc::clone(&running);
    let ingested_ingest = Arc::clone(&ingested);
    let last_seq_ingest = Arc::clone(&last_seq);
    thread::spawn(move || {
        while running_ingest.load(Ordering::SeqCst) {
            let mut firehose_url = "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos".to_string();
            let cur = last_seq_ingest.load(Ordering::Relaxed);
            if cur > 0 { firehose_url.push_str(&format!("?cursor={}", cur)); }
            
            println!("[Stage 1] Connecting to firehose (cursor={})...", cur);
            
            let url = Url::parse(&firehose_url).unwrap();
            let host = url.host_str().unwrap();
            let port = url.port_or_known_default().unwrap();
            let addr = format!("{}:{}", host, port);
            
            let tcp_stream = match std::net::TcpStream::connect(&addr) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[Error] TCP connect failed: {}. Retrying...", e);
                    thread::sleep(Duration::from_secs(5));
                    continue;
                }
            };
            
            let connector = native_tls::TlsConnector::new().unwrap();
            let tls_stream = match connector.connect(host, tcp_stream) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[Error] TLS handshake failed: {}. Retrying...", e);
                    thread::sleep(Duration::from_secs(5));
                    continue;
                }
            };
            
            let (mut socket, _) = match tungstenite::client(url, tls_stream) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[Error] Tungstenite handshake failed: {}. Retrying...", e);
                    thread::sleep(Duration::from_secs(5));
                    continue;
                }
            };

            println!("[Stage 1] Ingest Connected");
            
            while running_ingest.load(Ordering::SeqCst) {
                match socket.read() {
                    Ok(Message::Binary(bin)) => {
                        ingested_ingest.fetch_add(1, Ordering::Relaxed);
                        let _ = tx_verify.send(bin);
                    }
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("[Error] Socket read error: {}. Reconnecting...", e);
                        break;
                    }
                }
            }
        }
    });

    // 2. Verification Workers (N threads)
    let num_verify_threads = num_cpus::get(); // Use all cores
    for _ in 0..num_verify_threads {
        let rx = rx_verify.clone();
        let tx = tx_compress.clone();
        let verified_ref = Arc::clone(&verified);
        let raw_bytes_ref = Arc::clone(&raw_bytes);
        let last_seq_ref = Arc::clone(&last_seq);
        let running_ref = Arc::clone(&running);
        
        thread::spawn(move || {
            while running_ref.load(Ordering::SeqCst) {
                if let Ok(bin) = rx.recv() {
                    // Extract seq and verify
                    if let Some(envelope) = parse_input(&bin) {
                        if let Some(seq) = envelope.sequence {
                            last_seq_ref.fetch_max(seq, Ordering::Relaxed);
                            
                            let did_str = envelope.did
                                .and_then(|b| std::str::from_utf8(b).ok())
                                .unwrap_or("unknown")
                                .to_string();

                            raw_bytes_ref.fetch_add(bin.len() as u64, Ordering::Relaxed);
                            verified_ref.fetch_add(1, Ordering::Relaxed);
                            let _ = tx.send((seq, did_str, bin));
                        }
                    }
                }
            }
        });
    }

    // 3. Compression & Archive Worker (Single Threaded - the bottleneck we fear)
    let archive_ref = Arc::clone(&archive_writer);
    let compressed_ref = Arc::clone(&compressed);
    let running_comp = Arc::clone(&running);
    let rx_comp_worker = rx_compress.clone();
    
    thread::spawn(move || {
        println!("[Stage 3] Archive Worker Started (Clustered Batching Enabled)");
        
        while running_comp.load(Ordering::SeqCst) || !rx_comp_worker.is_empty() {
            if let Ok((seq, did, raw_data)) = rx_comp_worker.recv() {
                // Write - ArchiveWriter now handles compression & clustering internally
                let mut writer = archive_ref.lock().unwrap();
                writer.append_message(seq, &did, "test/path", &raw_data).expect("Archive write failed");
                
                compressed_ref.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Finalize the last segment before exiting
        let mut writer = archive_ref.lock().unwrap();
        writer.finalize_segment().ok();
        println!("[Stage 3] Finalized segments.");
    });

    // 4. Monitoring Thread
    let start_time = Instant::now();
    while running.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_secs(1));
        let elapsed = start_time.elapsed().as_secs_f64();
        
        let i = ingested.load(Ordering::Relaxed);
        let v = verified.load(Ordering::Relaxed);
        let c = compressed.load(Ordering::Relaxed);
        let r_b = raw_bytes.load(Ordering::Relaxed);

        let compressed_bytes = {
            let writer = archive_writer.lock().unwrap();
            writer.total_compressed_bytes
        };
        
        let q_v = rx_verify.len();
        let q_c = rx_compress.len();
        
        let ratio = if r_b > 0 { (1.0 - (compressed_bytes as f64 / r_b as f64)) * 100.0 } else { 0.0 };

        println!("\n--- Pipeline Stats (t={:.1}s) ---", elapsed);
        println!("Ingested:   {:8} ({:.1} msg/s)", i, i as f64 / elapsed);
        println!("Verified:   {:8} ({:.1} msg/s) | Queue: {}", v, v as f64 / elapsed, q_v);
        println!("Compressed: {:8} ({:.1} msg/s) | Savings: {:.1}%", c, c as f64 / elapsed, ratio);
        
        if q_c > 1000 {
            println!("[ALERT] COMPRESSION CLOG DETECTED! Queue growing: {}", q_c);
        }
        
        // Stop after 400,000 messages or 10 minutes
        if c >= 400_000 || elapsed > 600.0 {
            println!("[Info] Reached target message count (400k) or timeout. Shutting down.");
            running.store(false, Ordering::SeqCst);
            break;
        }
    }
}
