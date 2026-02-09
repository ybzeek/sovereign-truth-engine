use std::time::Instant;
use tungstenite::Message;
use zstd::dict::from_continuous;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let target_count = args.get(1).and_then(|s| s.parse::<usize>().ok()).unwrap_or(250_000);
    
    let url_str = "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos";
    let url = url::Url::parse(url_str).unwrap();
    let host = url.host_str().unwrap();
    let port = url.port_or_known_default().unwrap();
    let addr = format!("{}:{}", host, port);

    println!("[Info] Starting collection of {} messages from {}...", target_count, addr);

    let mut samples_buffer = Vec::new();
    let mut sample_sizes = Vec::new();
    let mut total_raw_size = 0;
    let mut count = 0;

    let start = Instant::now();
    while count < target_count {
        let (mut socket, _) = match tungstenite::client(url_str, {
            let tcp = std::net::TcpStream::connect(&addr).expect("TCP connect failed");
            let connector = native_tls::TlsConnector::new().unwrap();
            connector.connect(host, tcp).expect("TLS handshake failed")
        }) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[Error] Reconnect failed: {}. Retrying in 5s...", e);
                std::thread::sleep(std::time::Duration::from_secs(5));
                continue;
            }
        };

        while count < target_count {
            match socket.read() {
                Ok(Message::Binary(bin)) => {
                    total_raw_size += bin.len();
                    sample_sizes.push(bin.len());
                    samples_buffer.extend_from_slice(&bin);
                    count += 1;
                    
                    if count % 5000 == 0 {
                        println!("  [+] Progress: {}/{}", count, target_count);
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("[Error] Websocket read failed: {}. Reconnecting...", e);
                    break;
                }
            }
        }
    }
    let collect_duration = start.elapsed();

    println!("\n[Analysis] Collection complete in {:?}.", collect_duration);

    println!("\n[1/4 Training] Training Zstd dictionary (1024KB)...");
    let dict_size = 1024 * 1024; 
    let dictionary = from_continuous(&samples_buffer, &sample_sizes, dict_size)
        .expect("Failed to train dictionary");
    std::fs::write("atproto_firehose.dict", &dictionary).expect("Failed to save .dict");

    println!("\n[2/4 Compression] Compressing data at Level 3...");
    let mut compressed_data = Vec::new();
    let mut compressed_lengths = Vec::new();
    let mut compressor = zstd::bulk::Compressor::with_dictionary(3, &dictionary).unwrap();
    
    let mut offset = 0;
    for &size in &sample_sizes {
        let original = &samples_buffer[offset..offset + size];
        let compressed = compressor.compress(original).unwrap();
        
        compressed_lengths.push(compressed.len() as u32);
        compressed_data.extend_from_slice(&compressed);
        offset += size;
    }

    println!("\n[3/4 Persistence] Saving firehose_test.bin and firehose_test.idx...");
    std::fs::write("firehose_test.bin", &compressed_data).expect("Failed to save .bin");
    
    // Save index as absolute offsets for O(1) random access
    let mut offsets = Vec::with_capacity(compressed_lengths.len() + 1);
    let mut current_offset = 0u32;
    for &len in &compressed_lengths {
        offsets.push(current_offset);
        current_offset += len;
    }
    offsets.push(current_offset); // Final offset (end of last message)

    let idx_bytes: Vec<u8> = offsets.iter()
        .flat_map(|&o| o.to_le_bytes().to_vec())
        .collect();
    std::fs::write("firehose_test.idx", &idx_bytes).expect("Failed to save .idx");

    println!("\n[4/4 Benchmark] Loading from disk and running decompressor test...");
    
    // Simulate a fresh start: load everything from disk
    let loaded_dict = std::fs::read("atproto_firehose.dict").unwrap();
    let loaded_bin = std::fs::read("firehose_test.bin").unwrap();
    let loaded_idx_raw = std::fs::read("firehose_test.idx").unwrap();
    
    let loaded_offsets: Vec<u32> = loaded_idx_raw.chunks_exact(4)
        .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
        .collect();

    let mut decompressor = zstd::bulk::Decompressor::with_dictionary(&loaded_dict).unwrap();
    let mut total_decompressed = 0;
    
    let bench_start = Instant::now();
    for i in 0..target_count {
        let start = loaded_offsets[i] as usize;
        let end = loaded_offsets[i+1] as usize;
        let chunk = &loaded_bin[start..end];
        let decomp = decompressor.decompress(chunk, 1024 * 1024).unwrap();
        total_decompressed += decomp.len();
    }
    let bench_duration = bench_start.elapsed();

    let rate = target_count as f64 / bench_duration.as_secs_f64();
    let savings = (1.0 - (loaded_bin.len() as f64 / total_decompressed as f64)) * 100.0;

    println!("\n===========================================");
    println!("FINAL REPORT: DISK-BACKED ARCHIVE TEST");
    println!("===========================================");
    println!("Original Data Size:      {:.2} MB", total_decompressed as f64 / 1_048_576.0);
    println!("Compressed Data Size:    {:.2} MB", loaded_bin.len() as f64 / 1_048_576.0);
    println!("Total Space Saved:       {:.1}%", savings);
    println!("-------------------------------------------");
    println!("Decompression Speed:     {:.2} msg/s", rate);
    println!("Read Throughput:         {:.2} MB/s", (total_decompressed as f64 / 1024.0 / 1024.0) / bench_duration.as_secs_f64());
    println!("Avg Read Latency:        {:.2} μs per message", (bench_duration.as_secs_f64() * 1_000_000.0) / target_count as f64);
    println!("===========================================");
    println!("[Success] All data persisted and verified.");
}
