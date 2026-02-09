use memmap2::MmapOptions;
use std::fs::File;
use std::io::Read;
use std::collections::HashSet;
use did_mmap_cache::parser::core::parse_input;
use did_mmap_cache::mst::car::CarStore;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Load Dictionary and Archive
    let mut dict_file = File::open("atproto_firehose.dict")?;
    let mut dict_data = Vec::new();
    dict_file.read_to_end(&mut dict_data)?;

    let bin_file = File::open("firehose_test.bin")?;
    let bin_mmap = unsafe { MmapOptions::new().map(&bin_file)? };

    let idx_file = File::open("firehose_test.idx")?;
    let idx_mmap = unsafe { MmapOptions::new().map(&idx_file)? };

    let offsets: Vec<u32> = idx_mmap.chunks_exact(4)
        .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
        .collect();

    let num_messages = offsets.len() - 1;
    let mut decompressor = zstd::bulk::Decompressor::with_dictionary(&dict_data)?;

    println!("[Info] Analyzing {} messages for CAR block redundancy...", num_messages);

    let mut global_block_cache = HashSet::new();
    let mut total_car_bytes_received = 0;
    let mut total_unique_block_bytes = 0;
    let mut total_blocks_seen = 0;
    let mut total_unique_blocks = 0;

    for i in 0..num_messages {
        let start = offsets[i] as usize;
        let end = offsets[i+1] as usize;
        let compressed_chunk = &bin_mmap[start..end];
        
        let decompressed = decompressor.decompress(compressed_chunk, 1024 * 1024).unwrap();
        
        if let Some(parsed) = parse_input(&decompressed) {
            if let Some(car_data) = parsed.blocks {
                total_car_bytes_received += car_data.len();
                
                // Parse CAR file into blocks
                let store = CarStore::new(car_data);
                for (cid, data) in store.blocks {
                    total_blocks_seen += 1;
                    
                    // Use a combination of CID bytes for the hash set
                    // We store a copy of the CID to simulate the index overhead
                    if !global_block_cache.contains(cid) {
                        global_block_cache.insert(cid.to_vec());
                        total_unique_blocks += 1;
                        total_unique_block_bytes += data.len();
                        total_unique_block_bytes += cid.len(); // Include CID in storage cost
                    }
                }
            }
        }

        if (i + 1) % 2500 == 0 {
            println!("  [+] Processed {}/{} messages...", i + 1, num_messages);
        }
    }

    println!("\n[CAR BLOCK DEDUPLICATION RESULTS]");
    println!("===========================================");
    println!("Total CAR Messages:      {}", num_messages);
    println!("Total Blocks Processed:  {}", total_blocks_seen);
    println!("Unique Blocks Identified: {}", total_unique_blocks);
    println!("-------------------------------------------");
    println!("Total Raw CAR Data:      {:.2} MB", total_car_bytes_received as f64 / 1_048_576.0);
    println!("Deduplicated Store Size: {:.2} MB", total_unique_block_bytes as f64 / 1_048_576.0);
    
    let savings = (1.0 - (total_unique_block_bytes as f64 / total_car_bytes_received as f64)) * 100.0;
    println!("Deduplication Savings:   **{:.1}%**", savings);
    println!("===========================================");
    
    println!("\n[Theoretical Best]");
    let current_total_compressed = bin_mmap.len() as f64 / 1_048_576.0;
    let potential_reduction = (total_car_bytes_received - total_unique_block_bytes) as f64 / 1_048_576.0;
    println!("Current Archive Size:    {:.2} MB", current_total_compressed);
    println!("Estimated New Size:      {:.2} MB", current_total_compressed - potential_reduction);
    println!("Total Effective Savings: **{:.1}%**", (1.0 - ((current_total_compressed - potential_reduction) / (total_car_bytes_received as f64 / (0.926) / 1_048_576.0))) * 100.0);

    Ok(())
}
