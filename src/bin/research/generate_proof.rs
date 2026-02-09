use did_mmap_cache::archive::SegmentedArchive;
use did_mmap_cache::parser::core::parse_input;
use std::fs;
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let archive_dir = "test_archive"; 
    let dict_path = "atproto_firehose.dict";

    let dict_data = fs::read(dict_path)?;
    let archive = SegmentedArchive::open_directory(archive_dir, None, Some(Arc::new(dict_data)))?;
    
    // Find first segment start
    let mut entries: Vec<_> = fs::read_dir(archive_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("idx"))
        .filter_map(|e| e.path().file_stem()?.to_str()?.parse::<u64>().ok())
        .collect();
    entries.sort();

    let start_seq = entries[0];
    
    // We'll search for one message that has a clear DID and sequence
    for i in 0..100 {
        let seq = start_seq + i;
        if let Ok(data) = archive.get_message_by_seq(seq, None) {
            if let Some(parsed) = parse_input(&data) {
                if let Some(did_bytes) = parsed.did {
                    let did = std::str::from_utf8(did_bytes)?;
                    
                    println!("\n[INTEGRITY PROOF GENERATED]");
                    println!("===========================================");
                    println!("Sequence ID:  {}", seq);
                    println!("User DID:     {}", did);
                    println!("Message Size: {} bytes", data.len());
                    
                    if let Some(commit_cid) = parsed.commit {
                        println!("Commit CID:   {}", hex::encode(commit_cid));
                    }

                    println!("\n[Verification Command]");
                    println!("-------------------------------------------");
                    println!("Check this sequence on the public firehose relay:");
                    println!("curl -s \"https://bsky.network/xrpc/com.atproto.sync.getLatestCommit?did={}\"", did);
                    println!("\nOr view the user's profile to confirm they are active:");
                    println!("https://bsky.app/profile/{}", did);
                    println!("===========================================");
                    
                    return Ok(());
                }
            }
        }
    }

    Ok(())
}
