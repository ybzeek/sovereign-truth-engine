#[cfg(test)]
mod bench {
    use did_mmap_cache::mmap_did_cache::MmapDidCache;
    use tempfile::tempdir;
    use std::time::Instant;
    use std::fs;

    #[test]
    fn test_bench_identity_lookup() {
        // RP-5: Identity Lookup Latency (~73ns)
        // We will test with a smaller cache size for the test (10k slots)
        let dir = tempdir().unwrap();
        let path = dir.path().join("cache_bench.bin");

        // Initialize with 10k slots (99 bytes each = 990,000 bytes)
        let slot_size = 99;
        let num_slots = 10000;
        let size = slot_size * num_slots;
        fs::write(&path, vec![0u8; size]).unwrap();

        let mut cache = MmapDidCache::open_mut(&path).unwrap();
        
        let did = "did:plc:abcdefghijklmnopqrstuvwxyz";
        let pubkey = [0u8; 33];
        
        // Populate one slot
        cache.atomic_update_or_tombstone(did, Some(1), Some(&pubkey));
        
        // Benchmark lookups
        let iterations = 100_000;
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = cache.get(did);
        }
        let elapsed = start.elapsed();
        let per_op = elapsed.as_nanos() / iterations as u128;
        
        println!("Identity Lookup Latency: {} ns/op", per_op);
        // Note: Performance varies on hardware, but ~73ns is the target on dev's machine.
        // We just ensure it’s < 1000ns.
        assert!(per_op < 1000, "Lookup is too slow: {}ns", per_op);
    }
}
