use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;
use rand::seq::IteratorRandom;
use reqwest::blocking::Client;
use did_mmap_cache::mmap_did_cache::MmapDidCache;
use base64::{engine::general_purpose, Engine as _};

#[test]
#[ignore] // Requires full atomic_cache.bin and network access
fn test_cache_vs_plc_api() {
    // Path to the test cache file
    let cache_path = "../atomic_cache.bin";
    let plc_dump_path = "../atomic_cache.bin"; // Using production cache for test context
    let cache = MmapDidCache::open(cache_path).expect("Failed to open cache");
    let file = File::open(plc_dump_path).expect("Failed to open PLC dump");
    let reader = BufReader::new(file);
    let mut dids = Vec::new();
    // Collect DIDs from the PLC dump
    for line in reader.lines().flatten() {
        if let Some(did) = line.split('"').nth(3) {
            dids.push(did.to_string());
        }
    }
    // Select 50 random DIDs
    let mut rng = rand::thread_rng();
    let sample: Vec<_> = dids.iter().choose_multiple(&mut rng, 50);
    let client = Client::new();
    let mut run_results = Vec::new();
    for run in 1..=2 {
        let mut correct = 0;
        let mut total = 0;
        let mut timings = Vec::new();
        for did in &sample {
            let start = Instant::now();
            let cache_pubkey = cache.get(did).map(|(pk, _)| pk);
            let duration = start.elapsed();
            timings.push(duration.as_nanos());
            // Query PLC API for the DID
            let url = format!("https://plc.directory/{}", did);
            let resp = client.get(&url).send();
            if let Ok(r) = resp {
                if let Ok(json) = r.json::<serde_json::Value>() {
                    if let Some(vm) = json.get("verificationMethod") {
                        if let Some(pk_mb) = vm.get(0).and_then(|v| v.get("publicKeyMultibase")).and_then(|v| v.as_str()) {
                            // Decode multibase (bs58 or base64)
                            let decoded = bs58::decode(&pk_mb[1..]).into_vec().ok()
                                .or_else(|| general_purpose::STANDARD.decode(&pk_mb[1..]).ok());
                            if let (Some(cache_bytes), Some(plc_bytes)) = (cache_pubkey, decoded) {
                                // Compare first 33 bytes (builder logic)
                                if &cache_bytes[1..] == &plc_bytes[0..33] {
                                    correct += 1;
                                }
                            }
                        }
                    }
                }
            }
            total += 1;
        }
        // Output timing statistics
        let min = *timings.iter().min().unwrap();
        let max = *timings.iter().max().unwrap();
        let avg = timings.iter().sum::<u128>() as f64 / timings.len() as f64;
        run_results.push((run, min, max, avg, correct, total));
    }
    for (run, min, max, avg, correct, total) in &run_results {
        println!("RUN {}: Cache lookup times (nanoseconds):", run);
        println!("  min   = {}", min);
        println!("  max   = {}", max);
        println!("  avg   = {:.2}", avg);
        println!("RUN {}: Correct lookups: {}/{}", run, correct, total);
    }
    assert!(run_results[0].4 > 40 && run_results[1].4 > 40, "Less than 80% correct lookups!");
}
