use did_mmap_cache::archive::SegmentedArchive;
use std::fs;
use std::path::Path;
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let archive_dir = if args.len() > 1 { &args[1] } else { "test_archive" }; 
    let dict_path = "atproto_firehose.dict";

    if !Path::new(archive_dir).exists() {
        println!("[Error] No 'archive' directory found. Run integrated_stress_test first.");
        return Ok(());
    }

    let dict_data = if Path::new(dict_path).exists() {
        Some(fs::read(dict_path)?)
    } else {
        println!("[Warning] No dictionary found, using raw decompression.");
        None
    };

    println!("[Info] Opening archive at {}...", archive_dir);
    let archive = SegmentedArchive::open_directory(archive_dir, None, dict_data.map(Arc::new))?;
    
    if archive.segment_count() == 0 {
        println!("[Error] Archive is empty.");
        return Ok(());
    }

    // Try to find the first available sequence number
    let mut entries: Vec<_> = fs::read_dir(archive_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("idx"))
        .filter_map(|e| e.path().file_stem()?.to_str()?.parse::<u64>().ok())
        .collect();
    entries.sort();

    let start_seq = entries[0];
    println!("[Info] Opening segment for sequence: {}\n", start_seq);

    // Instead of guessing sequences, let's look at the actual index bytes
    let idx_path = Path::new(archive_dir).join(format!("{}.idx", start_seq));
    let idx_bytes = fs::read(idx_path)?;
    
    for i in 0..5 {
        // Updated for 28-byte index format
        let idx_off = 32 + i * 28;
        if idx_off + 28 > idx_bytes.len() { break; }
        
        let chunk = &idx_bytes[idx_off..idx_off + 28];
        let _bin_off = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
        let _inner_off = u32::from_le_bytes(chunk[12..16].try_into().unwrap()); // c_len is 8..12, inner_off is 12..16
        let m_len = u32::from_le_bytes(chunk[16..20].try_into().unwrap()); // i_len is 16..20
        
        if m_len == 0 {
            println!("[Message #{}] Index record is empty (skip)", i);
            continue;
        }

        // Calculate sequence number (start_seq + relative index)
        let seq = start_seq + i as u64;
        
        match archive.get_message_by_seq(seq, None) {
            Ok(data) => {
                println!("--- [Message #{} | Seq: {} | Size: {} bytes] ---", i, seq, data.len());
                let hex_snippet = data.iter().take(48).map(|b| format!("{:02x}", b)).collect::<String>();
                println!("Hex: {}...", hex_snippet);
                
                let data_str = String::from_utf8_lossy(&data);
                if let Some(pos) = data_str.find("did:plc:") {
                    let end = (pos + 32).min(data_str.len());
                    println!("Found DID: {}", &data_str[pos..end]);
                }
                
                // Extract CID
                if let Some(pos) = data.windows(4).position(|w| w == [0xd8, 0x2a, 0x58, 0x25]) {
                    let cid_bytes = &data[pos+4..pos+4+37];
                    let cid_hex = cid_bytes.iter().map(|b| format!("{:02x}", b)).collect::<String>();
                    println!("EXTRACTED_CID_HEX: {}", cid_hex);
                }

                // Check if it's CBOR-ish (often starts with a map code or tag)
                if data.len() > 4 {
                    println!("Head bytes: {:02x} {:02x} {:02x} {:02x}", data[0], data[1], data[2], data[3]);
                }
                println!();
            },
            Err(e) => {
                println!("[Error] Could not fetch index record {}: {}", i, e);
            }
        }
    }

    Ok(())
}
