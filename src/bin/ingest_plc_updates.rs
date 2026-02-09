// ingest_plc_updates.rs
// Rust ingestor for PLC directory: HTTP /export catch-up
// Full production version with Atomic Updates and Timestamp-Advancing logic.

use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use reqwest::blocking::Client;
use serde_json::Value;
use std::thread::sleep;
use std::time::Duration;
use did_mmap_cache::mmap_did_cache::MmapDidCache;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 5 {
        eprintln!("Usage: {} <plc_dump.jsonl> <cache_file> <cursor_file> <updates_file>", args[0]);
        std::process::exit(1);
    }
    
    let dump_path = &args[1];
    let cache_path = &args[2];
    let _cursor_path = &args[3];
    let updates_path = &args[4];


    // --- State Recovery Logic ---
    // 1. Tail the end of the dump file for the latest createdAt
    fn tail_latest_created_at(path: &str, max_lines: usize) -> Option<String> {
        use std::fs::File;
        use std::io::{BufRead, BufReader, Seek, SeekFrom};
        let file = File::open(path).ok()?;
        let mut reader = BufReader::new(file);
        let mut lines = Vec::new();
        // Seek to near the end (1MB)
        let file_len = reader.get_ref().metadata().ok()?.len();
        let seek_pos = if file_len > 1024 * 1024 { file_len - 1024 * 1024 } else { 0 };
        reader.seek(SeekFrom::Start(seek_pos)).ok()?;
        let mut buf = String::new();
        // Discard first partial line
        reader.read_line(&mut buf).ok()?;
        for line in reader.lines().filter_map(|l| l.ok()) {
            lines.push(line);
        }
        lines.iter().rev().take(max_lines).find_map(|line| {
            serde_json::from_str::<Value>(line).ok()?.get("createdAt").and_then(|c| c.as_str()).map(|s| s.to_string())
        })
    }

    // 2. Get latest createdAt from updates.log
    fn latest_created_at_in_file(path: &str, max_lines: usize) -> Option<String> {
        use std::fs::File;
        use std::io::{BufRead, BufReader};
        let file = File::open(path).ok()?;
        let reader = BufReader::new(file);
        let lines: Vec<_> = reader.lines().filter_map(|l| l.ok()).collect();
        lines.iter().rev().take(max_lines).find_map(|line| {
            serde_json::from_str::<Value>(line).ok()?.get("createdAt").and_then(|c| c.as_str()).map(|s| s.to_string())
        })
    }

    let dump_latest = tail_latest_created_at(dump_path, 100);
    let updates_latest = latest_created_at_in_file(updates_path, 100);

    // 3. Use the most recent date
    let mut start_date = dump_latest.clone();
    if let Some(ref upd) = updates_latest {
        if let (Ok(dump_dt), Ok(upd_dt)) = (chrono::DateTime::parse_from_rfc3339(dump_latest.as_deref().unwrap_or("")), chrono::DateTime::parse_from_rfc3339(upd)) {
            if upd_dt > dump_dt {
                start_date = Some(upd.clone());
            }
        }
    }

    // 4. Prompt user for confirmation
    if let Some(ref date) = start_date {
        println!("[INFO] Will start ingest from createdAt: {}", date);
        print!("Is this correct? (y/n): ");
        std::io::stdout().flush().unwrap();
        let mut input = String::new();
        std::io::stdin().read_line(&mut input).unwrap();
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Aborting by user request.");
            std::process::exit(0);
        }
    } else {
        println!("[WARN] Could not determine a starting date. Aborting.");
        std::process::exit(1);
    }
    let mut last_created_at = start_date;

    let client = Client::new();
    let mut cache = match MmapDidCache::open_mut(cache_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[ERROR] Failed to open mmap cache: {}", e);
            std::process::exit(1);
        }
    };

    let mut fetched_count = 0u64;

    println!("Starting PLC ingest. Latest TS: {:?}", last_created_at);

    loop {
        // Always use the last timestamp for paging
        let url = match &last_created_at {
            Some(ts) => format!("https://plc.directory/export?after={}&count=1000", ts),
            None => {
                println!("[FATAL] No starting timestamp available.");
                break;
            }
        };

        println!("Fetching: {}", url);

        let response = match client.get(&url).send() {
            Ok(res) if res.status().is_success() => res.text().unwrap_or_default(),
            Ok(res) => {
                eprintln!("[WARN] HTTP Status {}. Retrying...", res.status());
                sleep(Duration::from_secs(5));
                continue;
            }
            Err(e) => {
                eprintln!("[ERROR] Network error: {}. Retrying...", e);
                sleep(Duration::from_secs(5));
                continue;
            }
        };

        let mut lines_processed = 0;
        let _updates_file = OpenOptions::new().append(true).create(true).open(updates_path).expect("Cannot open updates file");

        for line in response.lines() {
            let v: Value = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(_) => continue,
            };

            let did = v["did"].as_str().unwrap_or("");
            let is_null = v["nullified"].as_bool().unwrap_or(false);
            
            if is_null {
                cache.remove_did(did);
            } else if let Some(op) = v.get("operation") {
                if let Some(pubkey_str) = extract_signing_key(op) {
                    let key_type_byte = if pubkey_str.starts_with("zDna") {
                        1
                    } else if pubkey_str.starts_with("zUC7") {
                        2
                    } else {
                        1
                    };
                    if let Ok(decoded_bytes) = decode_multibase_key(&pubkey_str) {
                        cache.atomic_update_or_tombstone(did, Some(key_type_byte), Some(&decoded_bytes));
                    }
                }
            }

            // Journaling to disk
            // Advance the pointer
            if let Some(ts) = v["createdAt"].as_str() { 
                last_created_at = Some(ts.to_string()); 
                // Overwrite updates.log with the latest timestamp only
                if let Ok(mut file) = OpenOptions::new().write(true).create(true).truncate(true).open(updates_path) {
                    if let Err(e) = writeln!(file, "{}", ts) {
                        eprintln!("[FATAL] Disk write error: {}", e);
                        std::process::exit(1);
                    }
                }
            }
            lines_processed += 1;
            fetched_count += 1;
        }

        // If we got nothing back, we are caught up to the head of the directory
        if lines_processed == 0 {
            println!("Catch-up complete. All records processed.");
            break;
        }

        println!("Batch complete: {} processed. Latest TS: {:?}", lines_processed, last_created_at);

        sleep(Duration::from_millis(500)); 
    }

    println!("Finished. Total fetched this session: {}", fetched_count);
}

// Helper: Extract key regardless of PLC versioning
fn extract_signing_key(op: &Value) -> Option<String> {
    if let Some(sk) = op.get("signingKey").and_then(|v| v.as_str()) {
        return Some(sk.to_string());
    }
    op.get("verificationMethods")
        .and_then(|vm| vm.as_object())
        .and_then(|obj| obj.values().next())
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

// Helper: Decode and slice to exactly 33 bytes for the mmap
fn decode_multibase_key(key: &str) -> Result<[u8; 33], String> {
    let clean_key = if key.starts_with("did:key:") {
        &key[8..]
    } else {
        key
    };
    
    let (_base, bytes) = multibase::decode(clean_key).map_err(|e| e.to_string())?;
    
    let mut out = [0u8; 33];
    // We grab the last 33 bytes to strip off multicodec headers (like 0xe7 for secp256k1)
    let len = bytes.len().min(33);
    let start = bytes.len() - len;
    out[33-len..].copy_from_slice(&bytes[start..]);
    
    Ok(out)
}