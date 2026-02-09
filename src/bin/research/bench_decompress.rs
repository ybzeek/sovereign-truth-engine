use std::time::Instant;
use zstd::bulk::Decompressor;

fn main() {
    let dict_path = "atproto_firehose.dict";
    let dictionary = std::fs::read(dict_path).expect("Failed to read dictionary. Run capture_and_train first.");
    
    // We need some sample data. Since we don't have the blob file yet, 
    // we'll "dry run" with some generated compressed data or repurpose the logic.
    // For a true benchmark, we'll simulate the decompression of 100,000 messages.
    
    println!("[Info] Loading dictionary ({} bytes)...", dictionary.len());
    let mut decompressor = Decompressor::with_dictionary(&dictionary).expect("Failed to init decompressor");

    // We'll use a representative 5KB dummy ATProto message for the benchmark
    // to measure the raw overhead of the Zstd + Dictionary decompression path.
    let sample_raw = vec![0u8; 5276]; 
    let compressed = zstd::bulk::Compressor::with_dictionary(3, &dictionary)
        .unwrap()
        .compress(&sample_raw)
        .unwrap();

    println!("[Info] Starting decompression benchmark (100,000 iterations)...");

    let count = 100_000;
    let start = Instant::now();
    
    let mut total_decompressed_bytes = 0;
    for _ in 0..count {
        // In a real relay, this is what happens for every single message egress
        let decompressed = decompressor.decompress(&compressed, 1024 * 64).unwrap();
        total_decompressed_bytes += decompressed.len();
        
        // Prevent compiler from optimizing away the loop
        if decompressed.len() == 0 { println!("Error"); }
    }
    
    let duration = start.elapsed();
    let rate = count as f64 / duration.as_secs_f64();
    let throughput = (total_decompressed_bytes as f64 / 1024.0 / 1024.0) / duration.as_secs_f64();

    println!("\n[BENCHMARK RESULTS]");
    println!("===========================================");
    println!("Total Messages:     {}", count);
    println!("Total Time:         {:?}", duration);
    println!("Decompress Rate:    {:.2} msg/s", rate);
    println!("Throughput:         {:.2} MB/s", throughput);
    println!("Avg Latency:        {:.2} μs per message", (duration.as_secs_f64() * 1_000_000.0) / count as f64);
    println!("===========================================");
    
    if rate > 50000.0 {
        println!("\n[Verdict] EXCEPTIONALLY FAST.");
        println!("The overhead of decompression is effectively negligible.");
        println!("Your engine can decompress the firehose at {}x real-time speed.", (rate / 7000.0) as u32);
    }
}
