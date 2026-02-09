//! ATProto Firehose Processor: Parses and verifies commit frames using mmap cache

use did_mmap_cache::mmap_did_cache::MmapDidCache;
use did_mmap_cache::parser::core::{parse_input, skip_cbor_value};
use did_mmap_cache::verify::verify_commit;
use std::env;
use std::fs::File;
use std::io::Read;

fn process_message(data: &[u8], cache: &MmapDidCache) {
    if let Some(envelope) = parse_input(data) {
        // Only process #commit messages
        if let Some(t) = envelope.t {
            if t == b"#commit" {
                if let Some(did_bytes) = envelope.did {
                    if let Ok(did) = std::str::from_utf8(did_bytes) {
                        match cache.get(did) {
                            Some((pubkey, kt)) => {
                                if verify_commit(&envelope, &pubkey, kt) {
                                    let seq = envelope.sequence.map_or(-1, |s| s as i64);
                                    println!("[OK] seq={} did={}", seq, did);
                                } else {
                                    println!("[Signature Failure] seq=? did={}", did);
                                }
                            }
                            None => println!("[Cache Miss] did={}", did),
                        }
                    } else {
                        println!("[Parse Error] Invalid UTF-8 in DID");
                    }
                } else {
                    println!("[Parse Error] No DID in envelope");
                }
            } else {
                // Log non-commit frames as info
                if let Ok(tstr) = std::str::from_utf8(t) {
                    println!("[Info] Skipping non-commit frame: {}", tstr);
                } else {
                    println!("[Info] Skipping non-commit frame (non-utf8 type)");
                }
            }
        } else {
            println!("[Info] Skipping frame with no type");
        }
    } else {
        println!("[Malformed Frame] Could not parse as CommitEnvelope");
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        println!("Usage: {} <cache_file> <input_file>", args[0]);
        return;
    }
    let cache_file = &args[1];
    let input_file = &args[2];
    let cache = MmapDidCache::open(cache_file).expect("Failed to open cache file");
    let mut file = File::open(input_file).expect("Failed to open input file");
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).expect("Failed to read input file");

    // Frame segmentation: loop over concatenated CBOR frames
    let mut offset = 0;
    let total = buf.len();
    while offset < total {
        // Find end of header
        let Some(header_end) = skip_cbor_value(&buf, offset) else { break };
        // Find end of payload
        let Some(payload_end) = skip_cbor_value(&buf, header_end) else { break };
        let frame = &buf[offset..payload_end];
        process_message(frame, &cache);
        offset = payload_end;
    }
}
