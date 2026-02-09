use memmap2::MmapOptions;
use std::fs::File;
use std::io::Read;
use std::time::Instant;
use zstd::bulk::Decompressor;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Load Dictionary
    let mut dict_file = File::open("atproto_firehose.dict")?;
    let mut dict_data = Vec::new();
    dict_file.read_to_end(&mut dict_data)?;

    // 2. Mmap Archive and Index
    let bin_file = File::open("firehose_test.bin")?;
    let bin_mmap = unsafe { MmapOptions::new().map(&bin_file)? };

    let idx_file = File::open("firehose_test.idx")?;
    let idx_mmap = unsafe { MmapOptions::new().map(&idx_file)? };

    let num_messages = (idx_mmap.len() / 4) - 1;
    let offsets: Vec<u32> = idx_mmap.chunks_exact(4)
        .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
        .collect();

    println!("[Info] Prepared benchmark for {} messages", num_messages);

    // 3. Sequential Egress Stress Test (Simulating Catching up a client)
    // We use a single buffer to simulate the outgoing socket buffer
    let mut egress_buffer = vec![0u8; 128 * 1024]; // 128KB buffer
    let mut decompressor = Decompressor::with_dictionary(&dict_data)?;

    println!("\n[Benchmark] Sequential Egress Speed (Mmap -> Decompress -> Buffer)");
    let start = Instant::now();
    let mut bytes_processed = 0;

    for i in 0..num_messages {
        let s = offsets[i] as usize;
        let e = offsets[i+1] as usize;
        let chunk = &bin_mmap[s..e];
        
        // Decompress directly into our "socket buffer"
        let size = decompressor.decompress_to_buffer(chunk, &mut egress_buffer)?;
        bytes_processed += size;
    }
    
    let duration = start.elapsed();
    let msg_s = num_messages as f64 / duration.as_secs_f64();
    let gb_s = (bytes_processed as f64 / 1024.0 / 1024.0 / 1024.0) / duration.as_secs_f64();

    println!("===========================================");
    println!("Total Time:          {:?}", duration);
    println!("Throughput (msg/s):  {:.0}", msg_s);
    println!("Throughput (GB/s):   {:.2} GB/s", gb_s);
    println!("===========================================");
    println!("[Analysis] This relay could serve the entire 20TB firehose in {:.1} hours.", 
        (20000.0 / gb_s) / 3600.0);

    Ok(())
}
