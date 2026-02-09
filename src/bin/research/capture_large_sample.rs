//! Large-Scale Firehose Capture Tool
//! Collects 1M+ messages and saves them to a raw file for dictionary training.

use std::fs::{OpenOptions, File};
use std::io::{Write, Read};
use std::time::Instant;
use tungstenite::Message;
use url::Url;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <target_count> [output_base_name]", args[0]);
        return;
    }

    let target_count: usize = args[1].parse().expect("Invalid target count");
    let base_name = args.get(2).map(|s| s.as_str()).unwrap_or("firehose_capture");
    
    let raw_path = format!("{}.raw", base_name);
    let size_path = format!("{}.sizes", base_name);

    println!("[Stage 1] Collecting {} messages...", target_count);
    println!("[Info] Data: {}, Sizes: {}", raw_path, size_path);

    let mut raw_file = OpenOptions::new().create(true).append(true).open(&raw_path).unwrap();
    let mut size_file = OpenOptions::new().create(true).append(true).open(&size_path).unwrap();

    let mut count = 0;
    let start_time = Instant::now();
    let mut last_progress = Instant::now();

    let url_str = "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos";
    
    while count < target_count {
        println!("[Info] Connecting to firehose...");
        let url = Url::parse(url_str).unwrap();
        let host = url.host_str().unwrap();
        let port = url.port_or_known_default().unwrap();
        let addr = format!("{}:{}", host, port);

        let tcp_stream = match std::net::TcpStream::connect(&addr) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[Error] TCP connect failed: {}. Retrying in 5s...", e);
                std::thread::sleep(std::time::Duration::from_secs(5));
                continue;
            }
        };

        let connector = native_tls::TlsConnector::new().unwrap();
        let tls_stream = match connector.connect(host, tcp_stream) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[Error] TLS handshake failed: {}. Retrying in 5s...", e);
                std::thread::sleep(std::time::Duration::from_secs(5));
                continue;
            }
        };

        let (mut socket, _) = match tungstenite::client(url, tls_stream) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[Error] Tungstenite handshake failed: {}. Retrying in 5s...", e);
                std::thread::sleep(std::time::Duration::from_secs(5));
                continue;
            }
        };

        println!("[Stage 1] Ingest Connected");
        while count < target_count {
            match socket.read() {
                Ok(Message::Binary(bin)) => {
                    // Write size as u32 little endian
                    size_file.write_all(&(bin.len() as u32).to_le_bytes()).unwrap();
                    // Write data
                    raw_file.write_all(&bin).unwrap();
                    
                    count += 1;
                    
                    if count % 1000 == 0 {
                        let elapsed = start_time.elapsed().as_secs_f64();
                        let rate = count as f64 / elapsed;
                        let last_rate = 1000.0 / last_progress.elapsed().as_secs_f64();
                        println!("  [+] Progress: {:8}/{} ({:.1} msg/s avg, {:.1} cur)", count, target_count, rate, last_rate);
                        last_progress = Instant::now();
                        
                        // Sync every 10k messages
                        if count % 10000 == 0 {
                            raw_file.sync_all().unwrap();
                            size_file.sync_all().unwrap();
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("[Error] Read failed: {}. Reconnecting...", e);
                    break;
                }
            }
        }
    }

    println!("\n[Stage 1] Collection Complete. Captured {} messages in {:.1}s", count, start_time.elapsed().as_secs_f64());
    println!("[Stage 2] Starting Dictionary Training...");
    
    // Now load everything to train
    drop(raw_file);
    drop(size_file);
    
    let mut raw_data = Vec::new();
    File::open(&raw_path).unwrap().read_to_end(&mut raw_data).unwrap();
    
    let mut size_data = Vec::new();
    File::open(&size_path).unwrap().read_to_end(&mut size_data).unwrap();
    
    let sample_sizes: Vec<usize> = size_data.chunks_exact(4)
        .map(|c| u32::from_le_bytes(c.try_into().unwrap()) as usize)
        .collect();

    let dict_size = 1024 * 1024; // 1MB
    let dictionary = zstd::dict::from_continuous(&raw_data, &sample_sizes, dict_size)
        .expect("Failed to train dictionary");
    
    std::fs::write("atproto_firehose.dict", &dictionary).expect("Failed to save .dict");
    println!("[Success] New 1MB dictionary saved to atproto_firehose.dict");

    // Quick verification
    let mut compressor = zstd::bulk::Compressor::with_dictionary(3, &dictionary).unwrap();
    let sample = &raw_data[0..sample_sizes[0]];
    let compressed = compressor.compress(sample).unwrap();
    println!("[Info] Sample compression check: {} bytes -> {} bytes ({:.1}%)", 
        sample.len(), compressed.len(), (compressed.len() as f64 / sample.len() as f64) * 100.0);
}
