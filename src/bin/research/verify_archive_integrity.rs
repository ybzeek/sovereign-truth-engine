use did_mmap_cache::archive::SegmentedArchive;
use did_mmap_cache::mmap_did_cache::MmapDidCache;
use did_mmap_cache::parser::core::parse_input;
use std::fs;
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let archive_dir = "archive_dir";
    let dict_path = "atproto_firehose.dict";
    let cache_path = "atomic_cache.bin";
    
    let dict_data = fs::read(dict_path)?;
    let archive = SegmentedArchive::open_directory(archive_dir, None, Some(Arc::new(dict_data)))?;
    let _cache = MmapDidCache::open(cache_path)?;
    
    // Pick first sequence from the directory
    let mut entries: Vec<_> = fs::read_dir(archive_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("idx"))
        .filter_map(|e| e.path().file_stem()?.to_str()?.parse::<u64>().ok())
        .collect();
    entries.sort();
    let start_seq = entries[0];

    println!("[Info] Verifying archive integrity starting at seq: {}", start_seq);

    for i in 0..10 {
        let seq = start_seq + i as u64;
        let data = archive.get_message_by_seq(seq, None)?;
        
        // Use our parser to verify the signature
        match parse_input(&data) {
            Some(_) => {
                println!("[Success] Seq {}: Parsed Successfully", seq);
            },
            None => {
                println!("[Failure] Seq {}: Parsing Error", seq);
            }
        }
    }

    Ok(())
}
