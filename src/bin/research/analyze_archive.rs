use memmap2::MmapOptions;
use std::fs::File;
use std::io::Read;
use did_mmap_cache::parser::core::parse_input;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Load Dictionary and Files
    let mut dict_file = File::open("atproto_firehose.dict")?;
    let mut dict_data = Vec::new();
    dict_file.read_to_end(&mut dict_data)?;

    let bin_file = File::open("firehose_test.bin")?;
    let bin_mmap = unsafe { MmapOptions::new().map(&bin_file)? };

    let idx_file = File::open("firehose_test.idx")?;
    let idx_mmap = unsafe { MmapOptions::new().map(&idx_file)? };

    let mut total_decompressed_bytes = 0;
    let mut did_bytes = 0;
    let mut sig_bytes = 0;
    let mut block_data_bytes = 0;
    let mut metadata_misc_bytes = 0;

    println!("[Info] Parsing 16-byte index records...");
    
    // Each record is 16 bytes: bin_off (8), inner_off (4), m_len (4)
    let num_messages = idx_mmap.len() / 16;
    
    // To avoid redundant decompression of the same cluster, we'll track seen offsets
    let mut seen_clusters = std::collections::HashSet::new();

    for i in 0..num_messages {
        let chunk = &idx_mmap[i*16..(i+1)*16];
        let bin_off = u64::from_le_bytes(chunk[0..8].try_into().unwrap()) as usize;
        let _inner_off = u32::from_le_bytes(chunk[8..12].try_into().unwrap()) as usize;
        let _m_len = u32::from_le_bytes(chunk[12..16].try_into().unwrap()) as usize;

        if seen_clusters.insert(bin_off) {
            let compressed_chunk = &bin_mmap[bin_off..];
            // Use decode_all because we don't know cluster size
            if let Ok(decompressed) = zstd::stream::decode_all_with_dictionary(compressed_chunk, &dict_data) {
                // Since this is a research tool, we'll parse the entire cluster's messages
                // but for simplicity here, we'll just handle the fact that parse_input
                // might need to be called on individual messages. 
                // Actually, the easiest way to "audit" is to parse every message one by one.
                // We'll skip the "seen_clusters" optimization for parsing, but use it for raw stats.
            }
        }
    }

    // REWRITING LOOP FOR ACCURACY: Iterate through every message, parse it.
    for i in 0..num_messages {
        let chunk = &idx_mmap[i*16..(i+1)*16];
        let bin_off = u64::from_le_bytes(chunk[0..8].try_into().unwrap()) as usize;
        let inner_off = u32::from_le_bytes(chunk[8..12].try_into().unwrap()) as usize;
        let m_len = u32::from_le_bytes(chunk[12..16].try_into().unwrap()) as usize;

        // Note: This is slow because it decompresses the cluster for EVERY message.
        // In research, that's fine for small samples.
        let compressed_chunk = &bin_mmap[bin_off..];
        if let Ok(cluster) = zstd::stream::decode_all_with_dictionary(compressed_chunk, &dict_data) {
            let decompressed = &cluster[inner_off..inner_off + m_len];
            total_decompressed_bytes += decompressed.len();

            if let Some(parsed) = parse_input(decompressed) {
                // DID bytes
                if let Some(did) = parsed.did {
                    did_bytes += did.len();
                }

                // Signature bytes
                if let Some(sig) = parsed.signature {
                    sig_bytes += sig.len();
                }

                // Block data (CAR blocks / MST nodes)
                if let Some(blocks) = parsed.blocks {
                    block_data_bytes += blocks.len();
                }

                // The rest
                let accounted = 
                    parsed.did.map(|d| d.len()).unwrap_or(0) +
                    parsed.signature.map(|s| s.len()).unwrap_or(0) +
                    parsed.blocks.map(|b| b.len()).unwrap_or(0);
                
                if decompressed.len() > accounted {
                    metadata_misc_bytes += decompressed.len() - accounted;
                }
            }
        }
    }

    println!("\n[BYTE BREAKDOWN (DECOMPRESSED)]");
    println!("===========================================");
    println!("Total Size:       {:.2} MB", total_decompressed_bytes as f64 / 1_048_576.0);
    println!("-------------------------------------------");
    
    print_row("Signatures", sig_bytes, total_decompressed_bytes);
    print_row("DIDs", did_bytes, total_decompressed_bytes);
    print_row("Block/CAR Data", block_data_bytes, total_decompressed_bytes);
    print_row("Misc/Protocol", metadata_misc_bytes, total_decompressed_bytes);
    
    println!("===========================================");
    
    println!("\n[Conclusion]");
    let entropy_floor = (sig_bytes as f64 / total_decompressed_bytes as f64) * 100.0;
    println!("* Uncompressible 'Entropy Floor' (Sigs): {:.1}%", entropy_floor);
    
    if block_data_bytes > (total_decompressed_bytes / 2) {
        println!("* Recommendation: Block-level Deduplication (CAR dedupe) is the best path.");
    } else if did_bytes > (total_decompressed_bytes / 5) {
        println!("* Recommendation: DID Indexing (String -> U32) would yield massive gains.");
    } else {
        println!("* Recommendation: Dictionary tuning is likely the most efficient path.");
    }

    Ok(())
}

fn print_row(label: &str, size: usize, total: usize) {
    let pct = (size as f64 / total as f64) * 100.0;
    println!("{:<15} {:>10.2} MB ({:>5.1}%)", label, size as f64 / 1_048_576.0, pct);
}
