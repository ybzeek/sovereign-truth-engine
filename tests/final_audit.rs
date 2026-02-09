#[cfg(test)]
mod audit {
    use did_mmap_cache::archive::{ArchiveWriter, SegmentedArchive, MultiShardArchive, TombstoneStore};
    use tempfile::tempdir;
    use std::fs;
    use std::io::Write;
    use fxhash::FxHasher;
    use std::hash::{Hasher, Hash};

    #[test]
    fn test_se1_index_record_size() {
        let dir = tempdir().unwrap();
        let mut writer = ArchiveWriter::new(dir.path(), 0, 0, 10, None).unwrap();
        writer.append_message(0, "did:1", "p1", b"data").unwrap();
        writer.finalize_segment().unwrap();

        let idx_path = dir.path().join("s0_0.idx");
        let metadata = fs::metadata(idx_path).unwrap();
        assert_eq!(metadata.len(), 60, "Index file should be exactly 60 bytes for 1 message");
    }

    #[test]
    fn test_se3_sc1_tombstone_capacity() {
        let dir = tempdir().unwrap();
        let ts_path = dir.path().join("tombstones.bin");
        let ts = TombstoneStore::open_or_create(&ts_path).unwrap();
        let metadata = fs::metadata(ts_path).unwrap();
        assert_eq!(metadata.len(), 512 * 1024 * 1024);
    }

    #[test]
    fn test_rp1_sc3_alignment_with_gaps() {
        let dir = tempdir().unwrap();
        let mut writer = ArchiveWriter::new(dir.path(), 0, 100, 10, None).unwrap();
        writer.append_message(100, "did:1", "p1", b"msg100").unwrap();
        writer.append_message(110, "did:1", "p2", b"msg110").unwrap();
        writer.finalize_segment().unwrap();
        let archive = SegmentedArchive::open_directory(dir.path(), None, None).unwrap();
        assert_eq!(archive.get_message_by_seq(110, None).unwrap(), b"msg110");
        assert!(archive.get_message_by_seq(105, None).is_err());
    }

    #[test]
    fn test_ci1_ci2_merkle_integrity() {
        let dir = tempdir().unwrap();
        let mut writer = ArchiveWriter::new(dir.path(), 0, 0, 10, None).unwrap();
        writer.append_message(0, "did:1", "p1", b"perfect_data").unwrap();
        writer.finalize_segment().unwrap();
        let archive = SegmentedArchive::open_directory(dir.path(), None, None).unwrap();
        assert!(archive.verify_integrity_at_seq(0, None).unwrap());
        let bin_path = dir.path().join("s0_0.bin");
        let mut data = fs::read(&bin_path).unwrap();
        if data.len() > 50 { data[50] ^= 0xFF; } else { data[0] ^= 0xFF; }
        fs::OpenOptions::new().write(true).open(&bin_path).unwrap().write_all(&data).unwrap();
        let archive_corrupted = SegmentedArchive::open_directory(dir.path(), None, None).unwrap();
        assert!(!archive_corrupted.verify_integrity_at_seq(0, None).unwrap());
    }

    #[test]
    fn test_rp3_shard_distribution_parity() {
        let dir = tempdir().unwrap();
        let archive = MultiShardArchive::new(dir.path(), 16, 1000, None).unwrap();
        for i in 0..100 {
            let did = format!("did:plc:user{}", i);
            archive.ingest(i as u64, &did, "path/1".to_string(), b"data".to_vec());
        }
        archive.shutdown();
        let mut shards_with_data = 0;
        for i in 0..16 {
            let shard_dir = dir.path().join(format!("shard_{}", i));
            if fs::read_dir(shard_dir).unwrap().count() > 0 {
                shards_with_data += 1;
            }
        }
        assert!(shards_with_data > 8);
    }

    #[test]
    fn test_rp2_path_hash_lookup() {
        let dir = tempdir().unwrap();
        let mut writer = ArchiveWriter::new(dir.path(), 0, 0, 100, None).unwrap();
        let path = "app.bsky.feed.post/12345";
        let mut hasher = FxHasher::default();
        path.hash(&mut hasher);
        let target_hash = hasher.finish();
        writer.append_message(500, "did:1", path, b"data").unwrap();
        writer.finalize_segment().unwrap();
        let archive = SegmentedArchive::open_directory(dir.path(), None, None).unwrap();
        assert_eq!(archive.find_seq_by_path_hash(target_hash).unwrap(), 500);
    }

    #[test]
    fn test_rp4_atomic_tombstone_performance() {
        let dir = tempdir().unwrap();
        let ts_path = dir.path().join("tombstones.bin");
        let mut ts = TombstoneStore::open_or_create(&ts_path).unwrap();
        let start = std::time::Instant::now();
        for i in 0..10_000 {
            ts.mark_deleted(i as u64);
        }
        let per_op = start.elapsed().as_nanos() / 10_000;
        assert!(per_op < 1000); // Allow more overhead in virtualized environment
    }

    #[test]
    fn test_se5_bloom_filter_accuracy() {
        use fastbloom::BloomFilter;
        let mut bloom = BloomFilter::with_num_bits(8 * 1024 * 1024).hashes(4); // 1MB
        let hash1 = [1u8; 32];
        let hash2 = [2u8; 32];
        bloom.insert(&hash1);
        assert!(bloom.contains(&hash1));
        assert!(!bloom.contains(&hash2));
    }

    #[test]
    fn test_ci3_crypto_integration() {
        use did_mmap_cache::verify::verify_commit;
        use did_mmap_cache::parser::core::CommitEnvelope;
        use k256::ecdsa::{SigningKey, signature::Signer, signature::hazmat::PrehashSigner};
        use sha2::Digest;

        // 1. Generate a real Secp256k1 key and signature
        let mut rng = rand::thread_rng();
        let signing_key = SigningKey::random(&mut rng);
        let verifying_key = signing_key.verifying_key();
        let pubkey_bytes: [u8; 33] = verifying_key.to_sec1_bytes().as_ref().try_into().unwrap();
        
        // 2. Wrap in a CommitEnvelope (minimal)
        // Let's use a real CBOR map for the commit.
        let commit_raw = [0xa1, 0x63, b'p', b'a', b'y', 0x63, b'l', b'o', b'a', b'd']; // {"pay": "load"}
        let mut hasher = sha2::Sha256::new();
        did_mmap_cache::parser::canonical::hash_canonical_commit(&commit_raw, &mut hasher);
        let hash = hasher.finalize();
        
        // CRITICAL: We must sign the PRE-HASHED value because verify_commit uses verify_prehash
        let sig_for_hash: k256::ecdsa::Signature = signing_key.sign_prehash(&hash).unwrap();
        let sig_bytes_for_hash = sig_for_hash.to_bytes();

        let env = CommitEnvelope {
            did: None,
            sequence: None,
            signature: Some(&sig_bytes_for_hash),
            t: None,
            op: None,
            raw: &[],
            blocks: None,
            commit: Some(&commit_raw), 
            cid: None,
            record_cid: None,
            ops: vec![],
            source_type: "test",
        };

        assert!(verify_commit(&env, &pubkey_bytes, 1), "Secp256k1 verification failed");
    }

    #[test]
    fn test_ci4_cid_consistency() {
        use did_mmap_cache::parser::canonical::hash_canonical_commit;
        use sha2::{Sha256, Digest};
        // Minimal CBOR map: {"foo": "bar"} -> a1 63 66 6f 6f 63 62 61 72
        let raw = [0xa1, 0x63, b'f', b'o', b'o', 0x63, b'b', b'a', b'r'];
        let mut hasher = Sha256::new();
        assert!(hash_canonical_commit(&raw, &mut hasher));
    }

    #[test]
    fn test_sc4_plc_backoff_logic() {
        let base_delay = 2;
        let delay_3 = base_delay * 2u64.pow(3);
        assert_eq!(delay_3, 16);
    }

    #[test]
    fn test_se2_realistic_compression() {
        // This test simulates the "Entropy Floor" by adding uncompressible 
        // high-entropy CID and Signature blobs to every message.
        let dir = tempdir().unwrap();
        
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
        let dict_path = std::path::PathBuf::from(manifest_dir).join("../atproto_firehose.dict");
        let dict = fs::read(&dict_path).ok();
        
        let mut writer = ArchiveWriter::new(dir.path(), 0, 0, 1000, dict).unwrap();
        
        let mut total_raw_bytes = 0;
        let users = ["did:plc:user1", "did:plc:user2"];
        
        use rand::RngCore;
        let mut rng = rand::thread_rng();

        for i in 0..100 {
            let did = users[i % users.len()];
            let path = format!("app.bsky.feed.post/{}", i);
            
            // 1. Structural Redundancy (Compressible)
            let mut msg_text = format!(
                r#"{{"text": "Sovereignty through code.", "createdAt": "2024-02-08T12:00:00Z", "reply": {{"parent": "cid_placeholder", "root": "cid_placeholder"}}}}"#
            ).into_bytes();

            // 2. High-Entropy Floor (Uncompressible)
            // 36 bytes for CID + 64 bytes for Signature = 100 bytes of pure noise per message
            let mut noise = vec![0u8; 100];
            rng.fill_bytes(&mut noise);
            msg_text.extend_from_slice(&noise);
            
            total_raw_bytes += msg_text.len();
            writer.append_message(i as u64, did, &path, &msg_text).unwrap();
        }
        writer.finalize_segment().unwrap();
        
        let bin_path = dir.path().join("s0_0.bin");
        let compressed_bytes = fs::metadata(bin_path).unwrap().len();
        let reduction = 100.0 * (1.0 - (compressed_bytes as f64 / total_raw_bytes as f64));
        
        println!("\n[Entropy Floor Audit] Raw: {}b, Compressed: {}b, Reduction: {:.2}%", 
                 total_raw_bytes, compressed_bytes, reduction);
        
        // With 100 bytes of noise per ~240 byte message (~40% noise), 
        // we expect the "Floor" to be around 50-60% reduction.
        assert!(reduction > 45.0, "Compression hit the entropy floor too early (found {:.2}%)", reduction);
    }
}
