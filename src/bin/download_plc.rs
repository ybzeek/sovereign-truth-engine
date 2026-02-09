use std::{fs::OpenOptions, io::{Write, BufRead, BufReader}, thread, time::Duration, fs::File};
use reqwest::blocking::Client;
use serde_json::Value;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    let output_file = "plc_dump.jsonl";
    let mut after = "1970-01-01T00:00:00.000Z".to_string();
    let mut total = 0;

    // Resume: find last createdAt in output file if it exists
    if let Ok(file) = File::open(output_file) {
        let reader = BufReader::new(file);
        let mut last_line = String::new();
        for line in reader.lines() {
            if let Ok(l) = line {
                if !l.trim().is_empty() {
                    last_line = l;
                }
            }
        }
        if !last_line.is_empty() {
            if let Ok(v) = serde_json::from_str::<Value>(&last_line) {
                if let Some(ts) = v["createdAt"].as_str() {
                    after = ts.to_string();
                    println!("Resuming from last timestamp: {}", after);
                }
            }
        }
        // Count lines for total
        total = std::fs::read_to_string(output_file)?.lines().count();
    }

    println!("Starting export...");

    const MAX_RETRIES: u32 = 8;
    const BASE_DELAY: u64 = 2; // seconds

    loop {
        let url = format!("https://plc.directory/export?count=1000&after={}", after);
        let mut retries = 0;
        let text = loop {
            match client.get(&url).send() {
                Ok(resp) => match resp.text() {
                    Ok(t) => break t,
                    Err(e) => {
                        eprintln!("[ERROR] Failed to read response text: {}", e);
                    }
                },
                Err(e) => {
                    eprintln!("[ERROR] Request failed: {}", e);
                }
            }
            if retries >= MAX_RETRIES {
                eprintln!("[FATAL] Max retries reached. Exiting.");
                return Err("Max retries reached".into());
            }
            let delay = BASE_DELAY * 2u64.pow(retries);
            eprintln!("Retrying in {} seconds... (attempt {}/{})", delay, retries + 1, MAX_RETRIES);
            thread::sleep(Duration::from_secs(delay));
            retries += 1;
        };

        if text.trim().is_empty() {
            println!("Finished syncing.");
            break;
        }

        // Each line is a JSON object
        let lines: Vec<&str> = text.lines().collect();
        if lines.is_empty() {
            println!("No more records.");
            break;
        }

        // Append to file
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(output_file)?;
        for line in &lines {
            writeln!(file, "{}", line)?;
        }
        total += lines.len();

        // Get last createdAt
        let last_line = lines.last().unwrap();
        let v: Value = serde_json::from_str(last_line)?;
        let last_timestamp = v["createdAt"].as_str().unwrap_or("");
        if last_timestamp.is_empty() {
            println!("No more records.");
            break;
        }
        after = last_timestamp.to_string();
        println!("Synced up to: {} (total: {})", after, total);
        thread::sleep(Duration::from_secs(1)); // Respectful delay
    }
    Ok(())
}
