//! Training utility to generate Zstd dictionaries from captured disk samples.
//! Memory-intensive: Requires enough RAM to hold the raw capture (approx 6GB for 1M messages).

use std::fs::File;
use std::io::{Read, Write};
use zstd::dict::from_continuous;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let base_name = args.get(1).map(|s| s.as_str()).unwrap_or("firehose_capture");
    
    let raw_path = format!("{}.raw", base_name);
    let size_path = format!("{}.sizes", base_name);
    let dict_path = "atproto_firehose.dict";
    let dict_size = 1024 * 1024; // 1MB chosen for maximum "Global Floor" compression density.

    println!("[1/3] Loading sample sizes from {}...", size_path);
    let mut size_file = match File::open(&size_path) {
        Ok(f) => f,
        Err(_) => {
            eprintln!("[Error] Could not find {}. Did the capture finish?", size_path);
            return;
        }
    };
    
    let mut size_bytes = Vec::new();
    size_file.read_to_end(&mut size_bytes).unwrap();
    
    let sizes: Vec<usize> = size_bytes
        .chunks_exact(4)
        .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()) as usize)
        .collect();

    println!("[2/3] Loading raw sample data into memory (Buffer: {})...", raw_path);
    let mut raw_file = File::open(&raw_path).expect("Cannot open raw file");
    let mut buffer = Vec::new();
    // Pre-allocate to avoid re-allocations during read
    let metadata = raw_file.metadata().unwrap();
    buffer.reserve(metadata.len() as usize);
    raw_file.read_to_end(&mut buffer).unwrap();

    println!("[3/3] Training 1MB Zstd dictionary using a 1-in-3 stride (333,333 samples across the full 1M)...");
    
    let mut sampled_buffer = Vec::new();
    let mut sampled_sizes = Vec::new();
    
    let mut current_offset = 0;
    for (i, &size) in sizes.iter().enumerate() {
        if i % 3 == 0 {
            sampled_buffer.extend_from_slice(&buffer[current_offset..current_offset + size]);
            sampled_sizes.push(size);
        }
        current_offset += size;
    }

    println!("  Sampled Buffer length: {} bytes", sampled_buffer.len());
    println!("  Sampled Messages:      {}", sampled_sizes.len());
    
    let start = std::time::Instant::now();
    let dictionary = from_continuous(&sampled_buffer, &sampled_sizes, dict_size)
        .expect("Failed to train dictionary");
    let duration = start.elapsed();

    File::create(dict_path)
        .unwrap()
        .write_all(&dictionary)
        .expect("Failed to save dictionary");

    println!("\n[Success] Dictionary generated in {:?}.", duration);
    println!("  Output:      {}", dict_path);
    println!("  Total Size:  {} bytes", buffer.len());
    println!("  Avg Message: {:.1} bytes", buffer.len() as f64 / sizes.len() as f64);
}
