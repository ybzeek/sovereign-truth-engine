//! Theoretical "Upper Bound" Compression Test: DID-based Clustering.
//! This script groups messages by DID before compression to see if user-specific 
//! patterns can push us toward that elusive 60% mark.

use std::fs::File;
use std::io::{Read};
use std::collections::HashMap;

fn main() {
    let base_name = "firehose_capture";
    let raw_path = format!("{}.raw", base_name);
    let size_path = format!("{}.sizes", base_name);
    let dict_path = "atproto_firehose.dict";

    println!("[1/4] Loading dictionary and 100k messages...");
    let mut dict_file = File::open(dict_path).expect("Run trainer first");
    let mut dict_bytes = Vec::new();
    dict_file.read_to_end(&mut dict_bytes).unwrap();

    let mut size_bytes = Vec::new();
    File::open(&size_path).unwrap().read_to_end(&mut size_bytes).unwrap();
    let sizes: Vec<usize> = size_bytes.chunks_exact(4).map(|c| u32::from_le_bytes(c.try_into().unwrap()) as usize).collect();

    let mut raw_file = File::open(&raw_path).unwrap();
    let sample_count = 100_000;
    
    // We only need the first 100k for the test
    let mut total_raw_size = 0;
    let mut messages = Vec::with_capacity(sample_count);

    for i in 0..sample_count {
        let size = sizes[i];
        let mut msg_buf = vec![0u8; size];
        raw_file.read_exact(&mut msg_buf).unwrap();
        messages.push(msg_buf);
        total_raw_size += size;
    }

    println!("[2/4] Parsing DIDs and clustering (Cluttering the lab table)...");
    let mut clusters: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
    
    for msg in &messages {
        // ATProto messages are CBOR/CAR, but the envelope we captured often has 
        // a JSON-like structure or we can just look for the DID string if we're quick.
        // For this science test, we'll use a simple heuristic to find "did:plc:..." or "did:web:..."
        // or just parse the first few bytes. 
        // Actually, since we want "True Science", let's just find the DID string in the buffer.
        
        let msg_str = String::from_utf8_lossy(msg);
        if let Some(start) = msg_str.find("did:plc:") {
            let did = &msg_str[start..start+32]; // Close enough for clustering
            clusters.entry(did.to_string()).or_default().push(msg.clone());
        } else {
            clusters.entry("unknown".to_string()).or_default().push(msg.clone());
        }
    }

    println!("  Clusters found: {}", clusters.len());
    println!("  Largest cluster: {} messages", clusters.values().map(|v| v.len()).max().unwrap_or(0));

    println!("[3/4] Compressing clustered vs unclustered...");
    
    // Baseline: Global Dictionary, Level 3, Serial Ingestion
    let mut compressor = zstd::bulk::Compressor::with_dictionary(3, &dict_bytes).unwrap();
    let mut baseline_size = 0;
    for msg in &messages {
        baseline_size += compressor.compress(msg).unwrap().len();
    }

    // Experiment: Compress entire clusters as single blocks
    // This simulates the "Archive segment sorted by user" approach
    let mut clustered_size = 0;
    let mut cluster_compressor = zstd::bulk::Compressor::with_dictionary(3, &dict_bytes).unwrap();
    
    for (did, msgs) in clusters {
        if did == "unknown" {
            for msg in msgs {
                clustered_size += cluster_compressor.compress(&msg).unwrap().len();
            }
            continue;
        }

        // We join all messages for a specific DID and compress them as a single stream
        // This is the theoretical "Maximum Context" gain.
        let mut did_blob = Vec::new();
        for msg in msgs {
            did_blob.extend_from_slice(&msg);
        }
        
        // Use a single compression call for the whole user history
        clustered_size += cluster_compressor.compress(&did_blob).unwrap().len();
    }

    println!("\n[4/4] THEORETICAL LIMITS:");
    println!("---------------------------");
    println!("Raw Size:              {:>12} bytes", total_raw_size);
    println!("Baseline (Global):     {:>12} bytes ({:.2}%)", baseline_size, (1.0 - (baseline_size as f64 / total_raw_size as f64)) * 100.0);
    println!("Clustered (Temporal):  {:>12} bytes ({:.2}%)", clustered_size, (1.0 - (clustered_size as f64 / total_raw_size as f64)) * 100.0);
    
    let improvement = (baseline_size as f64 - clustered_size as f64) / baseline_size as f64;
    println!("Vertical Gain:         {:.2}% better than global", improvement * 100.0);
}
