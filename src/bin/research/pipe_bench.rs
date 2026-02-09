//! Pipeline Benchmark: Verification + Compression + Archive IO
//! This test floods the pipeline as fast as possible to find the physical breakdown point.

use did_mmap_cache::parser::core::parse_input;
use did_mmap_cache::archive::ArchiveWriter;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::fs;
use std::time::{Instant, Duration};
use crossbeam_channel::unbounded;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <mock_data_size_mb>", args[0]);
        return;
    }
    let size_mb: usize = args[1].parse().unwrap();
    let num_messages = size_mb * 1000; // Approx 1KB per message

    println!("[Bench] Initializing Pipeline Stress Test (Target: {} MB, ~{} messages)", size_mb, num_messages);

    // Load Zstd dictionary
    let dict_path = "atproto_firehose.dict";
    let dict_data = fs::read(dict_path).expect("Missing atproto_firehose.dict");
    
    // Setup ArchiveWriter
    let archive_dir = "bench_archive";
    fs::create_dir_all(archive_dir).ok();
    let archive_writer = Arc::new(Mutex::new(
        ArchiveWriter::new(archive_dir, 0, 0, 100_000, Some(dict_data.clone())).unwrap()
    ));

    // Stats
    let processed = Arc::new(AtomicU64::new(0));
    let compressed_bytes = Arc::new(AtomicU64::new(0));
    let running = Arc::new(AtomicBool::new(true));

    // Channels
    let (tx_compress, rx_compress) = unbounded::<(u64, String, Vec<u8>)>();

    // 1. Mock Data Producer (Floods the system)
    let tx_prod = tx_compress.clone();
    
    // Realistic "Heavy" Payload: 1.5KB of data with some internal structure 
    // simulating a real commit frame.
    let mut mock_message = vec![0u8; 1500];
    for i in 0..1500 {
        mock_message[i] = (i % 255) as u8; // Structural but not trivial
    }

    thread::spawn(move || {
        println!("[Stage 1] Producer starting heavy flood...");
        for i in 0..num_messages as u64 {
            // We clone the message to simulate fresh data per-frame
            if tx_prod.send((i, "did:plc:mock_user".to_string(), mock_message.clone())).is_err() {
                break;
            }
        }
        println!("[Stage 1] Producer Finished. Dropping sender...");
        // Drop occurs implicitly as tx_prod goes out of scope here
    });

    // CRITICAL: Drop the main thread's copy of tx_compress so rx_compress.recv()
    // correctly returns Err when the producer thread finishes.
    drop(tx_compress);

    // 2. Archive Worker (Single Threaded)
    let rx_comp_worker = rx_compress;
    let archive_ref = Arc::clone(&archive_writer);
    let processed_ref = Arc::clone(&processed);
    let compressed_bytes_ref = Arc::clone(&compressed_bytes);
    
    let start_time = Instant::now();
    let handle = thread::spawn(move || {
        println!("[Stage 2] Archive Worker Started (Internal Compression Enabled)");
        
        while let Ok((seq, did, raw_data)) = rx_comp_worker.recv() {
            // Write - ArchiveWriter now handles compression & clustering internally
            let mut writer = archive_ref.lock().unwrap();
            writer.append_message(seq, &did, "test/path", &raw_data).unwrap();
            
            processed_ref.fetch_add(1, Ordering::Relaxed);
            compressed_bytes_ref.store(writer.total_compressed_bytes, Ordering::Relaxed);
        }
        
        let mut writer = archive_ref.lock().unwrap();
        writer.finalize_segment().ok();
        compressed_bytes_ref.store(writer.total_compressed_bytes, Ordering::Relaxed);
    });

    // Monitor
    while processed.load(Ordering::Relaxed) < num_messages as u64 {
        thread::sleep(Duration::from_millis(500));
        let p = processed.load(Ordering::Relaxed);
        let elapsed = start_time.elapsed().as_secs_f64();
        println!("Progress: {}/{} ({:.1} msg/s)", p, num_messages, p as f64 / elapsed);
    }

    let total_elapsed = start_time.elapsed();
    let total_p = processed.load(Ordering::Relaxed);
    let total_cb = compressed_bytes.load(Ordering::Relaxed);

    println!("\n=== Benchmark Results ===");
    println!("Total Messages: {}", total_p);
    println!("Total Time:     {:.2}s", total_elapsed.as_secs_f64());
    println!("Throughput:     {:.1} msg/s", total_p as f64 / total_elapsed.as_secs_f64());
    println!("Write Speed:    {:.2} MB/s (compressed)", (total_cb as f64 / 1024.0 / 1024.0) / total_elapsed.as_secs_f64());
    println!("=========================");
    
    handle.join().unwrap();
}
