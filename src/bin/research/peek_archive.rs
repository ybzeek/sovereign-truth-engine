use memmap2::MmapOptions;
use std::fs::File;
use std::io::Read;
use std::time::Instant;
use did_mmap_cache::parser::core::parse_input;
use zstd::dict::DecoderDictionary;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Load Dictionary
    let mut dict_file = File::open("atproto_firehose.dict")?;
    let mut dict_data = Vec::new();
    dict_file.read_to_end(&mut dict_data)?;
    let dict = DecoderDictionary::copy(&dict_data);

    // 2. Mmap Archive and Index
    let bin_file = File::open("firehose_test.bin")?;
    let bin_mmap = unsafe { MmapOptions::new().map(&bin_file)? };

    let idx_file = File::open("firehose_test.idx")?;
    let idx_mmap = unsafe { MmapOptions::new().map(&idx_file)? };

    let num_messages = idx_mmap.len() / 4;
    println!("[Info] Archive contains {} messages", num_messages);

    // 3. Peek at Message #0
    peek_at(&bin_mmap, &idx_mmap, &dict, 0)?;

    // 4. Random Access Stress Test
    println!("\n[Benchmark] Random Access Latency (Read + Decompress + Parse)");
    let iterations = 1000;
    let start = Instant::now();
    
    for i in 0..iterations {
        let idx = (i * 12345) % num_messages;
        let _ = fetch_and_parse(&bin_mmap, &idx_mmap, &dict, idx)?;
    }
    
    let duration = start.elapsed();
    println!("Total time for {} random reads: {:?}", iterations, duration);
    println!("Average latency per message: {:?}", duration / iterations as u32);

    Ok(())
}

fn fetch_and_parse(
    bin: &[u8], 
    idx: &[u8], 
    dict: &DecoderDictionary,
    msg_id: usize
) -> Result<(), Box<dyn std::error::Error>> {
    let offset_bytes = &idx[msg_id * 4..(msg_id + 1) * 4];
    let start_offset = u32::from_le_bytes(offset_bytes.try_into()?) as usize;
    
    let end_offset = if msg_id + 1 < idx.len() / 4 {
        let next_offset_bytes = &idx[(msg_id + 1) * 4..(msg_id + 2) * 4];
        u32::from_le_bytes(next_offset_bytes.try_into()?) as usize
    } else {
        bin.len()
    };

    let compressed_chunk = &bin[start_offset..end_offset];
    let mut decoder = zstd::stream::Decoder::with_prepared_dictionary(std::io::Cursor::new(compressed_chunk), dict)?;
    let mut decompressed = Vec::with_capacity(compressed_chunk.len() * 3);
    decoder.read_to_end(&mut decompressed)?;

    let _ = parse_input(&decompressed);
    
    Ok(())
}

fn peek_at(bin: &[u8], idx: &[u8], dict: &DecoderDictionary, msg_id: usize) -> Result<(), Box<dyn std::error::Error>> {
    let offset_bytes = &idx[msg_id * 4..(msg_id + 1) * 4];
    let start_offset = u32::from_le_bytes(offset_bytes.try_into()?) as usize;
    
    let end_offset = if msg_id + 1 < idx.len() / 4 {
        let next_offset_bytes = &idx[(msg_id + 1) * 4..(msg_id + 2) * 4];
        u32::from_le_bytes(next_offset_bytes.try_into()?) as usize
    } else {
        bin.len()
    };

    let compressed_chunk = &bin[start_offset..end_offset];
    let mut decoder = zstd::stream::Decoder::with_prepared_dictionary(std::io::Cursor::new(compressed_chunk), dict)?;
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;

    println!("[Success] Decompressed message #{} ({} bytes raw -> {} bytes decompressed)", 
        msg_id, compressed_chunk.len(), decompressed.len());

    if let Some(parsed) = parse_input(&decompressed) {
        println!("\n[MESSAGE DETAILS]");
        println!("===========================================");
        println!("Type:       {}", std::str::from_utf8(parsed.t.unwrap_or(b"unknown")).unwrap_or("err"));
        println!("DID:        {}", std::str::from_utf8(parsed.did.unwrap_or(b"none")).unwrap_or("err"));
        println!("Sequence:   {}", parsed.sequence.unwrap_or(0));
        
        if let Some(commit) = parsed.commit {
            let c_len = commit.len().min(16);
            println!("Commit Hex: {}...", hex::encode(&commit[..c_len]));
        }
        
        if let Some(sig) = parsed.signature {
            let s_len = sig.len().min(16);
            println!("Signature:  {}...", hex::encode(&sig[..s_len]));
        }
        println!("===========================================");
    } else {
        println!("\n[Warning] Message decompressed but failed to parse as ATProto envelope.");
    }

    Ok(())
}
