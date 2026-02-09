#[cfg(test)]
mod tests {
    use did_mmap_cache::mmap_did_cache::MmapDidCache;
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn test_cache_lookup() {
        // Setup: create a dummy cache file
        let path = "test_cache.bin";
        let file = File::create(path).unwrap();
        // Write dummy data (zeroed)
        (&file).write_all(&[0u8; 98 * 10]).unwrap();
        let cache = MmapDidCache::open(path).unwrap();
        // Should not find any DIDs
        assert!(cache.get("did:plc:dummy").is_none());
        // Cleanup
        std::fs::remove_file(path).unwrap();
    }
}

#[cfg(test)]
mod more_tests {
    use did_mmap_cache::mmap_did_cache::MmapDidCache;
    use std::fs::File;
    use tempfile::tempdir;
    use rand::{distributions::Alphanumeric, Rng};

    fn random_did() -> String {
        format!("did:plc:{}", rand::thread_rng().sample_iter(&Alphanumeric).take(20).map(char::from).collect::<String>())
    }

    #[test]
    fn test_insert_and_get() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_cache.bin");
        let cache_size = 99 * 1000; // Small test size
        let file = File::create(&path).unwrap();
        file.set_len(cache_size as u64).unwrap();
        let mut cache = MmapDidCache::open_mut(path.to_str().unwrap()).unwrap();
        let did = random_did();
        let pubkey = [42u8; 33];
        let key_type = 1u8;
        assert!(cache.atomic_update_or_tombstone(&did, Some(key_type), Some(&pubkey)));
        let (pk, kt) = cache.get(&did).unwrap();
        assert_eq!(pk, pubkey);
        assert_eq!(kt, key_type);
    }

    #[test]
    fn test_remove_did() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_cache.bin");
        let cache_size = 99 * 1000;
        let file = File::create(&path).unwrap();
        file.set_len(cache_size as u64).unwrap();
        let mut cache = MmapDidCache::open_mut(path.to_str().unwrap()).unwrap();
        let did = random_did();
        let pubkey = [99u8; 33];
        let key_type = 2u8;
        assert!(cache.atomic_update_or_tombstone(&did, Some(key_type), Some(&pubkey)));
        assert!(cache.remove_did(&did));
        assert!(cache.get(&did).is_none());
    }
}

#[cfg(test)]
mod archive_tests {
    use did_mmap_cache::archive::{ArchiveWriter, SegmentedArchive};
    use tempfile::tempdir;

    #[test]
    fn test_clustered_archive_integrity() {
        let dir = tempdir().unwrap();
        let archive_dir = dir.path().join("archive");
        
        // 1. Create Archive with a small segment size (5 messages)
        let mut writer = ArchiveWriter::new(&archive_dir, 0, 100, 5, None).unwrap();
        
        let did_a = "did:plc:aaaaa";
        let did_b = "did:plc:bbbbb";
        
        // Use varying lengths to test the inner_off / m_len logic
        let msg1 = b"Hello from A - 1";
        let msg2 = b"Hello from A - 2 (longer)";
        let msg3 = b"Hello from B - 1";
        let msg4 = b"Hello from A - 3 (multi-cluster)";
        let msg5 = b"Hello from B - 2";
        
        writer.append_message(100, did_a, "test/path", msg1).unwrap();
        writer.append_message(101, did_a, "test/path", msg2).unwrap();
        writer.append_message(102, did_b, "test/path", msg3).unwrap();
        writer.append_message(103, did_a, "test/path", msg4).unwrap();
        writer.append_message(104, did_b, "test/path", msg5).unwrap();
        
        // At 5 messages, it should have automatically triggered finalize_segment
        assert!(writer.total_compressed_bytes > 0); 
        
        // 2. Open and Retrieve
        let archive = SegmentedArchive::open_directory(&archive_dir, None, None).unwrap();
        assert_eq!(archive.segment_count(), 1);

        let res1 = archive.get_message_by_seq(100, None).unwrap();
        let res2 = archive.get_message_by_seq(101, None).unwrap();
        let res3 = archive.get_message_by_seq(102, None).unwrap();
        let res4 = archive.get_message_by_seq(103, None).unwrap();
        let res5 = archive.get_message_by_seq(104, None).unwrap();

        assert_eq!(res1, msg1);
        assert_eq!(res2, msg2);
        assert_eq!(res3, msg3);
        assert_eq!(res4, msg4);
        assert_eq!(res5, msg5);
    }
}
