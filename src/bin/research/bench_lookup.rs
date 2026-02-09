use did_mmap_cache::mmap_did_cache::MmapDidCache;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("Usage: bench_did_lookup <cache_path>");
        return;
    }
    
    let cache = MmapDidCache::open(&args[1]).expect("Failed to open cache");
    
    // We'll look up some common DIDs if they exist, or just random stuff
    // This assumes the cache is populated with some known DIDs
    // Let's just pull 1000 DIDs from the cache itself if possible
    // Actually, we'll just benchmark the lookup speed for a random DID string
    
    let did = "did:plc:cy4n4s63dve3g62u2kvu2pev"; // One we know exists from earlier
    
    println!("[Bench] Starting DID Lookup Benchmark (1,000,000 iterations)");
    let start = Instant::now();
    for _ in 0..1_000_000 {
        let _ = cache.get(did);
    }
    let elapsed = start.elapsed();
    
    println!("Total Time:      {:?}", elapsed);
    println!("Avg Latency:     {:.2} ns", elapsed.as_nanos() as f64 / 1_000_000.0);
    println!("Throughput:      {:.2} million lookups/sec", 1.0 / (elapsed.as_secs_f64()));
}
