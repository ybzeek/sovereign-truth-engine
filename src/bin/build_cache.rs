// build_cache.rs
// CLI tool to build the mmap DID→pubkey cache from a preprocessed PLC JSONL file
// Usage: cargo run --bin build_cache -- <input_file.jsonl.preprocessed> <output_cache.bin>

use zerocopy_derive::{FromBytes, Unaligned, FromZeroes};
use zerocopy::AsBytes;
use std::env;
use std::fs::File;
use std::io::BufReader;
use memmap2::MmapMut;
use sha2::{Digest, Sha256};
use serde::Deserialize;
use serde_json::Value;

// Slot size: 99 bytes (32 DID hash + 1 key type + 33 pubkey + 32 reserved + 1 valid/version)
const SLOT_SIZE: usize = 99;
const NUM_SLOTS: usize = 150_000_001;

#[derive(Debug, Deserialize)]
struct PlcRecord {
    pub did: String,
    pub operation: Option<Value>,
    #[serde(default)]
    pub nullified: Option<bool>,
}

fn find_all_keys(op: &serde_json::Value) -> Vec<String> {
    let mut keys = Vec::new();
    if let Some(vm) = op.get("verificationMethods") {
        if let Some(obj) = vm.as_object() {
            for (_method, val) in obj.iter() {
                if val.is_string() {
                    if let Some(s) = val.as_str() {
                        keys.push(s.to_string());
                    }
                } else if val.is_array() {
                    for v in val.as_array().unwrap() {
                        if v.is_string() {
                            if let Some(s) = v.as_str() {
                                keys.push(s.to_string());
                            }
                        }
                    }
                }
            }
        }
    }
    if let Some(sk) = op.get("signingKey") {
        if sk.is_string() {
            if let Some(s) = sk.as_str() {
                keys.push(s.to_string());
            }
        }
    }
    keys
}

// Updated slot layout for atomic lock-free protocol (see lockfree_atomic_update_plan.md)
#[repr(C)]
#[derive(Copy, Clone, Debug, AsBytes, FromBytes, Unaligned, FromZeroes)]
struct CacheEntry {
    did_hash: [u8; 32],
    key_type: u8,
    pubkey: [u8; 33],
    reserved: [u8; 32],
    valid: u8, // 0 = empty, 1 = valid, (future: 2 = deleted, >1 = version)
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <input_file.jsonl.preprocessed> <output_cache.bin>", args[0]);
        std::process::exit(1);
    }
    let input_path = &args[1];
    let output_path = &args[2];

    println!("Allocating 14.7GB Mmap file...");
    let out_file = File::options().read(true).write(true).create(true).truncate(true)
        .open(output_path).expect("Failed to create bin file");
    out_file.set_len((SLOT_SIZE * NUM_SLOTS) as u64).expect("Failed to resize file");
    let mut mmap = unsafe { MmapMut::map_mut(&out_file).expect("Mmap failed") };

    use std::io::BufRead;
    use std::collections::HashMap;
    println!("Starting Single-Pass Enrichment (Line-by-Line Mode)...");
    let mut count = 0;
    let file = File::open(input_path).expect("Failed to open input");
    let reader = BufReader::with_capacity(16 * 1024 * 1024, file);
    let mut all_keys: HashMap<[u8; 32], (u8, [u8; 33])> = HashMap::with_capacity(65_000_000);
    let mut nullified_dids: HashMap<[u8; 32], bool> = HashMap::with_capacity(1_000_000);
    let mut hasher = Sha256::new();
    for line_result in reader.lines() {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => continue,
        };
        let rec: PlcRecord = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(_) => continue,
        };
        hasher.update(rec.did.as_bytes());
        let did_hash: [u8; 32] = hasher.finalize_reset().into();
        if rec.nullified.unwrap_or(false) {
            nullified_dids.insert(did_hash, true);
            all_keys.remove(&did_hash);
            continue;
        }
        if let Some(op) = rec.operation.as_ref() {
            for mut sig_key in find_all_keys(op) {
                if sig_key.starts_with("did:key:") {
                    sig_key = sig_key.trim_start_matches("did:key:").to_string();
                }
                let key_type_byte = if sig_key.starts_with("zDna") {
                    1
                } else if sig_key.starts_with("zUC7") {
                    2
                } else {
                    1
                };
                let decode_result = multibase::decode(&sig_key);
                match decode_result {
                    Ok((_base, pk_bytes)) => {
                        let pubkey_bytes = if pk_bytes.len() == 35 && (pk_bytes[0] == 0xe7 || pk_bytes[0] == 0x12) {
                            &pk_bytes[2..]
                        } else if pk_bytes.len() == 34 && (pk_bytes[0] == 0xe7 || pk_bytes[0] == 0x12) {
                            &pk_bytes[1..]
                        } else if pk_bytes.len() >= 33 {
                            &pk_bytes[0..33]
                        } else {
                            &pk_bytes[..]
                        };
                        let mut pubkey = [0u8; 33];
                        let len = pubkey_bytes.len().min(33);
                        pubkey[..len].copy_from_slice(&pubkey_bytes[..len]);
                        all_keys.insert(did_hash, (key_type_byte, pubkey));
                    },
                    Err(_) => {},
                }
            }
        }
        count += 1;
        if count % 1_000_000 == 0 {
            println!("Processed {}M operations...", count / 1_000_000);
        }
    }
    let mut written = 0u64;
    let mmap_len = mmap.len();
    for (did_hash, (key_type_byte, pubkey)) in all_keys.drain() {
        if nullified_dids.get(&did_hash).copied().unwrap_or(false) {
            // Skip writing any slot for nullified DIDs
            continue;
        }
        // Normal entry write
        let entry = CacheEntry {
            did_hash,
            key_type: key_type_byte,
            pubkey,
            reserved: [0u8; 32],
            valid: 0, // Start as invalid
        };
        let mut slot = (fxhash::hash64(&did_hash) % NUM_SLOTS as u64) as usize;
        loop {
            let start = slot * SLOT_SIZE;
            let end = start + SLOT_SIZE;
            if end > mmap_len {
                slot = 0;
                continue;
            }
            let existing_valid = mmap[start + 98]; // valid/version byte (last byte)
            let existing_did_hash = &mmap[start..start + 32];
            if existing_valid == 0 || existing_did_hash == did_hash {
                mmap[start..start + 98].copy_from_slice(&entry.as_bytes()[..98]);
                mmap[start + 98] = 1; // 1 = valid
                break;
            }
            slot = (slot + 1) % NUM_SLOTS;
        }
        written += 1;
        if written % 1_000_000 == 0 {
            println!("Wrote {}M keys to mmap... (Fill rate: {:.2}%)", written / 1_000_000, (written as f64 / NUM_SLOTS as f64) * 100.0);
        }
    }
    println!("Flushing to disk...");
    mmap.flush().expect("Final flush failed");
    println!("Done! Processed {} total operations, wrote {} keys.", count, written);
}
