#[cfg(test)]
mod v2_2_tests {
    use did_mmap_cache::archive::{ArchiveWriter, SegmentedArchive, MultiShardArchive};
    use tempfile::tempdir;
    use fxhash::FxHasher;
    use std::hash::{Hasher, Hash};

    #[test]
    fn test_v2_2_path_hash_and_gaps() {
        let dir = tempdir().unwrap();
        let archive_dir = dir.path().join("v2_archive");
        
        // Use a small segment size to force gaps and finalization
        let mut writer = ArchiveWriter::new(&archive_dir, 0, 100, 10, None).unwrap();
        
        let did = "did:plc:test_v2_2";
        let path1 = "app.bsky.feed.post/123";
        let path2 = "app.bsky.feed.post/456";
        let msg1 = b"Message 1";
        let msg2 = b"Message 2";
        
        // 1. Ingest with a GAP in sequences
        writer.append_message(100, did, path1, msg1).unwrap();
        // Skip from 100 to 105
        writer.append_message(105, did, path2, msg2).unwrap();
        
        writer.finalize_segment().unwrap();
        
        // 2. Open and Verify Bit-Perfect Gap Handling
        let archive = SegmentedArchive::open_directory(&archive_dir, None, None).unwrap();
        
        // Check retrieval of valid messages
        assert_eq!(archive.get_message_by_seq(100, None).unwrap(), msg1);
        assert_eq!(archive.get_message_by_seq(105, None).unwrap(), msg2);
        
        // Check gap (should be io::ErrorKind::NotFound)
        let gap_res = archive.get_message_by_seq(102, None);
        assert!(gap_res.is_err());
        assert_eq!(gap_res.unwrap_err().kind(), std::io::ErrorKind::NotFound);
        
        // 3. Test Path-Hash Lookup
        let mut hasher = FxHasher::default();
        path2.hash(&mut hasher);
        let target_hash = hasher.finish();
        
        let found_seq = archive.find_sequence_by_path(target_hash).unwrap();
        assert_eq!(found_seq, 105);
        
        // 4. Test Integrity Verification
        assert!(archive.verify_integrity_at_seq(100, None).unwrap());
    }

    #[test]
    fn test_v2_2_tombstone_filtering() {
        let dir = tempdir().unwrap();
        let archive_dir = dir.path().join("v2_archive");
        
        // Create 16-shard archive
        let archive = MultiShardArchive::new(&archive_dir, 16, 50, None).unwrap();
        
        let did = "did:plc:tombstone_test";
        let msg = b"Delete me";
        archive.ingest(500, did, "path/to/delete".to_string(), msg.to_vec());
        archive.shutdown();
        
        // Re-open in read-only mode
        let archive_ro = MultiShardArchive::open_readonly(&archive_dir, None).unwrap();
        
        // Verify message exists
        assert_eq!(archive_ro.get_message_by_seq(500).unwrap(), msg);
        
        // Mark as deleted
        archive_ro.mark_deleted(500);
        
        // Verify message is now "Not Found" due to tombstone
        let res = archive_ro.get_message_by_seq(500);
        assert!(res.is_err());
        
        // Verify raw cluster filtering
        let cluster_res = archive_ro.get_raw_cluster_at_seq(500);
        assert!(cluster_res.is_err(), "Raw cluster should be rejected if message is tombstoned");
    }

    #[test]
    fn test_v2_2_dictionary_compression() {
        let dir = tempdir().unwrap();
        let archive_dir = dir.path().join("v2_archive");
        
        // Use a real-looking dictionary (just some repeated patterns)
        let mut dict = Vec::new();
        for _ in 0..100 { dict.extend_from_slice(b"atproto_special_pattern_"); }
        
        let mut writer = ArchiveWriter::new(&archive_dir, 0, 1000, 10, Some(dict.clone())).unwrap();
        
        let msg = b"atproto_special_pattern_DATA_HERE";
        writer.append_message(1000, "did:1", "p1", msg).unwrap();
        writer.finalize_segment().unwrap();
        
        let archive = SegmentedArchive::open_directory(&archive_dir, None, Some(std::sync::Arc::new(dict))).unwrap();
        let res = archive.get_message_by_seq(1000, None).unwrap();
        
        assert_eq!(res, msg);
    }
}
