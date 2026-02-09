use std::fs::File;
use std::io::{Read, Write};
use std::collections::HashSet;
use did_mmap_cache::mst::car::CarStore;
use zstd::bulk::{Compressor, Decompressor};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("Usage: fetch_user_repo <did>");
        return Ok(());
    }
    let did = &args[1];
    let url = format!("https://bsky.social/xrpc/com.atproto.sync.getRepo?did={}", did);

    println!("[1/4] Fetching repo for {}...", did);
    let mut response = reqwest::blocking::get(url)?;
    if !response.status().is_success() {
        return Err(format!("Failed to fetch repo: {}", response.status()).into());
    }
    
    let mut car_data = Vec::new();
    response.read_to_end(&mut car_data)?;
    println!("      Downloaded: {:.2} KB", car_data.len() as f64 / 1024.0);

    // 1. Analyze Blocks
    println!("[2/4] Analyzing IPLD blocks...");
    let store = CarStore::new(&car_data);
    let mut total_block_bytes = 0;
    let mut blocks = Vec::new();
    for (cid, data) in &store.blocks {
        total_block_bytes += data.len();
        blocks.push(data.to_vec());
    }
    println!("      Unique Blocks: {}", blocks.len());
    println!("      Total Block Data: {:.2} KB", total_block_bytes as f64 / 1024.0);

    // 2. Global Dictionary Compression
    println!("[3/4] Testing Global Dictionary Compression...");
    let mut global_dict = Vec::new();
    if let Ok(mut f) = File::open("atproto_firehose.dict") {
        f.read_to_end(&mut global_dict)?;
    } else {
        println!("      [Warning] atproto_firehose.dict not found. Skipping global dict test.");
    }

    let mut global_compressed_size = 0;
    if !global_dict.is_empty() {
        let mut compressor = Compressor::with_dictionary(3, &global_dict)?;
        for block in &blocks {
            let compressed = compressor.compress(block)?;
            global_compressed_size += compressed.len();
        }
        println!("      Global Dict Size: {:.2} KB ({:.1}% savings)", 
            global_compressed_size as f64 / 1024.0,
            (1.0 - (global_compressed_size as f64 / total_block_bytes as f64)) * 100.0);
    }

    // 3. User-Specific Dictionary Training
    println!("[4/4] Training User-Specific Dictionary (Simulated 100% visibility)...");
    // We train on the first 50% of blocks, then see how well it compresses the next 50%?
    // Or just train on all and see the "mathematical limit".
    let dict_size = 1024 * 1024; // 1MB
    let user_dict = zstd::dict::from_continuous(&car_data, &blocks.iter().map(|b| b.len()).collect::<Vec<_>>(), dict_size)?;
    
    let mut user_compressor = Compressor::with_dictionary(3, &user_dict)?;
    let mut user_compressed_size = 0;
    for block in &blocks {
        let compressed = user_compressor.compress(block)?;
        user_compressed_size += compressed.len();
    }
    
    println!("      User-Spec Dict Size: {:.2} KB ({:.1}% savings)", 
        user_compressed_size as f64 / 1024.0,
        (1.0 - (user_compressed_size as f64 / total_block_bytes as f64)) * 100.0);

    println!("\n[Final Verdict]");
    if user_compressed_size < global_compressed_size {
        let improvement = (1.0 - (user_compressed_size as f64 / global_compressed_size as f64)) * 100.0;
        println!("User-specific dictionaries are **{:.1}% better** than global ones for this user.", improvement);
    }

    Ok(())
}
