use did_mmap_cache::archive::SegmentedArchive;
use std::fs;
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let archive_dir = "archive_dir";
    let dict_path = "atproto_firehose.dict";
    let dict_data = fs::read(dict_path).ok();
    
    let archive = SegmentedArchive::open_directory(archive_dir, None, dict_data.map(Arc::new))?;
    let seq = 26994991896;
    
    let data = archive.get_message_by_seq(seq, None)?;
    
    // Look for CID tag d8 2a 58 25
    if let Some(pos) = data.windows(4).position(|w| w == [0xd8, 0x2a, 0x58, 0x25]) {
        let cid_bytes = &data[pos+4..pos+4+37];
        let cid_hex = cid_bytes.iter().map(|b| format!("{:02x}", b)).collect::<String>();
        println!("CID_HEX: {}", cid_hex);
        
        // Convert to multibase string (b + cid_hex) or just use cid crate if available
        // For simplicity, let's just print the hex and we can compare.
    } else {
        println!("CID tag not found");
    }
    
    println!("DID_SEARCH: {}", String::from_utf8_lossy(&data));

    Ok(())
}
