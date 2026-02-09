#[cfg(test)]
mod compression {
    use did_mmap_cache::archive::ArchiveWriter;
    use tempfile::tempdir;
    use std::fs;

    #[test]
    fn test_se2_compression_ratio() {
        // SE-2: CVL Clustered Compression
        let dir = tempdir().unwrap();
        let mut writer = ArchiveWriter::new(dir.path(), 0, 0, 1000, None).unwrap();
        
        let mut total_raw_bytes = 0;
        // Ingest 100 similar messages
        for i in 0..100 {
            let did = "did:plc:abcdefghijklmnopqrstuvwxyz";
            let path = format!("app.bsky.feed.post/{}", i);
            let msg = format!(r#"{{"text": "Hello world! This is message number {} with some repeated text structure.", "createdAt": "2024-01-01T00:00:00.000Z"}}"#, i).into_bytes();
            total_raw_bytes += msg.len();
            writer.append_message(i as u64, did, &path, &msg).unwrap();
        }
        writer.finalize_segment().unwrap();
        
        let bin_path = dir.path().join("s0_0.bin");
        let compressed_bytes = fs::metadata(bin_path).unwrap().len();
        
        let reduction = 100.0 * (1.0 - (compressed_bytes as f64 / total_raw_bytes as f64));
        println!("Raw bytes: {}, Compressed bytes: {}, Reduction: {:.2}%", total_raw_bytes, compressed_bytes, reduction);
        
        // Claim: 68.22% reduction. We should see at least 50% for clustered similar messages.
        assert!(reduction > 50.0, "Compression reduction should be significant (found {:.2}%)", reduction);
    }
}
