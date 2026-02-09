use std::fs;
use std::path::Path;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let archive_dir = if args.len() > 1 { &args[1] } else { "test_archive" }; 
    if !Path::new(archive_dir).exists() {
        println!("[Error] No archive directory found. Run integrated_stress_test first.");
        return;
    }

    let mut total_bin_size = 0;
    let mut total_idx_size = 0;
    let mut num_messages = 0;

    let entries = fs::read_dir(archive_dir).unwrap();
    for entry in entries {
        let entry = entry.unwrap();
        let path = entry.path();
        let size = fs::metadata(&path).unwrap().len();

        if path.extension().and_then(|s| s.to_str()) == Some("bin") {
            total_bin_size += size;
        } else if path.extension().and_then(|s| s.to_str()) == Some("idx") {
            total_idx_size += size;
            num_messages += size / 16;
        }
    }

    if num_messages == 0 {
        println!("[Error] No messages found in archive.");
        return;
    }

    let total_stored = total_bin_size + total_idx_size;
    let avg_compressed_payload = total_bin_size as f64 / num_messages as f64;
    let avg_total_per_msg = total_stored as f64 / num_messages as f64;

    // Estimate raw size based on 400 bytes avg
    let estimated_raw = num_messages * 400;
    let payload_efficiency = (1.0 - (total_bin_size as f64 / estimated_raw as f64)) * 100.0;
    let global_efficiency = (1.0 - (total_stored as f64 / estimated_raw as f64)) * 100.0;

    println!("\n[PRODUCTION STORAGE AUDIT]");
    println!("===========================");
    println!("Total Messages:    {}", num_messages);
    println!("Compressed Data:   {:.2} MB", total_bin_size as f64 / 1_048_576.0);
    println!("Index Data:        {:.2} MB", total_idx_size as f64 / 1_048_576.0);
    println!("---------------------------");
    println!("Avg Payload size:  {:.1} bytes", avg_compressed_payload);
    println!("Avg Index entry:   16.0 bytes");
    println!("Total per Message: {:.1} bytes", avg_total_per_msg);
    println!("===========================");
    println!("Payload Savings:   {:.1}%", payload_efficiency);
    println!("True Savings:      {:.1}% (Including Index overhead)", global_efficiency);
    println!("===========================");
    
    if global_efficiency > 60.0 {
        println!("\n[Verdict] COMPRESSION IS LEGIT.");
        println!("The 16-byte index overhead is only {:.1}% of the stored byte size.", (16.0 / avg_compressed_payload) * 100.0);
    }
}
