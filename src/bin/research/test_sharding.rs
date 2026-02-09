use did_mmap_cache::archive::{ArchiveWriter, SegmentedArchive};
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let test_dir = "test_archive_shards";
    if fs::metadata(test_dir).is_ok() {
        fs::remove_dir_all(test_dir)?;
    }
    fs::create_dir_all(test_dir)?;

    println!("[1/3] Writing sharded archive (segments of 10 messages)...");
    
    // Create writer starting at seq 1000, 10 messages per segment
    let mut writer = ArchiveWriter::new(test_dir, 0, 1000, 10, None)?;
    
    for i in 0..25 {
        let seq = 1000 + i;
        let msg = format!("Message content for sequence {}", seq);
        writer.append_message(seq, "did:plc:test", "test/path", msg.as_bytes())?;
    }
    writer.finalize_segment()?;
    
    println!("[2/3] Verifying files on disk...");
    let entries: Vec<_> = fs::read_dir(test_dir)?.collect();
    println!("      Found {} files in directory.", entries.len());
    for entry in entries {
        println!("      - {:?}", entry?.file_name());
    }

    println!("[3/3] Testing Segmented Reader...");
    let archive = SegmentedArchive::open_directory(test_dir, None, None)?;
    println!("      Archive loaded {} segments.", archive.segment_count());

    let test_seqs = [1000, 1005, 1010, 1019, 1020, 1024];
    for &seq in &test_seqs {
        match archive.get_message_by_seq(seq, None) {
            Ok(data) => {
                let msg = std::str::from_utf8(&data)?;
                println!("      Seq {}: SUCCESS -> {}", seq, msg);
            }
            Err(e) => println!("      Seq {}: FAILED ({})", seq, e),
        }
    }

    // Clean up
    fs::remove_dir_all(test_dir)?;
    Ok(())
}
