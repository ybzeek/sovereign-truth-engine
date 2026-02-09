use did_mmap_cache::archive::{ArchiveWriter, SegmentedArchive};
use std::fs;
use std::path::Path;
use std::sync::Arc;

#[test]
fn test_archive_clustering_and_reconstruction() {
    let dict_path = "atproto_firehose.dict";
    let test_dir = "tests/test_archive_clustered";
    let _ = fs::remove_dir_all(test_dir);
    fs::create_dir_all(test_dir).unwrap();

    let dict_bytes = if Path::new(dict_path).exists() {
        Some(fs::read(dict_path).expect("Failed to read dictionary"))
    } else {
        None
    };
    
    // 1. Initialize ArchiveWriter
    // Start at sequence 1000, max 10 messages per segment
    // Start at seq 1000
    let mut writer = ArchiveWriter::new(test_dir, 0, 1000, 10, dict_bytes.clone()).unwrap();

    // 2. Append messages in mixed order (by user) to test clustering
    // Seq numbers are monotonic, but users alternate
    let u1 = "did:plc:user1";
    let u2 = "did:plc:user2";
    
    let msg1000 = b"message 1000 from user 1";
    let msg1001 = b"message 1001 from user 2";
    let msg1002 = b"message 1002 from user 1 again";

    writer.append_message(1000, u1, "test/path", msg1000).expect("Append 1000 failed");
    writer.append_message(1001, u2, "test/path", msg1001).expect("Append 1001 failed");
    writer.append_message(1002, u1, "test/path", msg1002).expect("Append 1002 failed");

    // Force flush to disk
    writer.finalize_segment().expect("Finalize failed");

    // 3. Reconstruct using SegmentedArchive
    // Open the directory and read back by sequence number
    let archive = SegmentedArchive::open_directory(test_dir, None, dict_bytes.map(Arc::new)).expect("Open archive failed");
    
    let rec1000 = archive.get_message_by_seq(1000, None).expect("Read 1000 failed");
    let rec1001 = archive.get_message_by_seq(1001, None).expect("Read 1001 failed");
    let rec1002 = archive.get_message_by_seq(1002, None).expect("Read 1002 failed");

    assert_eq!(msg1000, rec1000.as_slice());
    assert_eq!(msg1001, rec1001.as_slice());
    assert_eq!(msg1002, rec1002.as_slice());

    println!("Success: Clustered storage correctly reconstructed chronological order via the virtual log index.");
    
    // Cleanup
    let _ = fs::remove_dir_all(test_dir);
}
