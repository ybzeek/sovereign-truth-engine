//! Mesh Crawler: Mapping the 10,000 PDS Sovereignty
//! This tool extracts all PDS endpoints from the PLC dump and performs parallel health grading.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;
use dashmap::DashMap;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json;
use sonic_rs::{from_str, Value, JsonValueTrait, JsonContainerTrait};
use url::Url;
use did_mmap_cache::pds_ledger::PdsLedger;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the PLC dump file (jsonl or .bin ledger)
    #[arg(short, long, default_value = "pds_list.bin")]
    input: String,

    /// Number of concurrent crawler threads
    #[arg(short, long, default_value_t = 128)]
    threads: usize,

    /// Timeout for each PDS probe in seconds
    #[arg(long, default_value_t = 5)]
    timeout: u64,

    /// Save results to this file
    #[arg(short, long, default_value = "mesh_map.json")]
    output: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
enum HealthGrade {
    A, // Fast, responsive, modern
    B, // Responsive, but slower or lacks features
    C, // Unreliable / Slow
    D, // Barely responsive
    F, // Dead / Refused
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct PdsReport {
    url: String,
    hostname: String,
    latency_ms: u128,
    grade: HealthGrade,
    version: Option<String>,
    app_version: Option<String>,
    error: Option<String>,
    last_seen: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    println!("[Mesh Crawler] Phase 1: Extracting PDS endpoints from {}...", args.input);
    let endpoints = extract_endpoints(&args.input)?;
    println!("[Mesh Crawler] Found {} unique PDS candidates.", endpoints.len());

    println!("[Mesh Crawler] Phase 2: Probing health using {} threads...", args.threads);
    let results = probe_endpoints(endpoints, &args);

    println!("[Mesh Crawler] Phase 3: Generating Mesh Map...");
    save_results(&results, &args.output)?;

    print_summary(&results);

    Ok(())
}

fn extract_endpoints(path: &str) -> Result<HashSet<String>> {
    let mut endpoints = HashSet::new();

    if path.ends_with(".jsonl") {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            if let Ok(v) = from_str::<Value>(&line) {
                if let Some(services) = v["operation"]["services"].as_object() {
                    for (_, service) in services {
                        if service["type"].as_str() == Some("AtprotoPersonalDataServer") {
                            if let Some(endpoint) = service["endpoint"].as_str() {
                                endpoints.insert(endpoint.to_string());
                            }
                        }
                    }
                }
            }
        }
    } else if path.ends_with(".bin") {
        let ledger = PdsLedger::open_or_create(path)?;
        for i in 0..ledger.entry_count() {
            if let Some(entry) = ledger.get_entry(i) {
                let url = entry.get_url();
                if !url.is_empty() {
                    // Convert wss:// to https:// for HTTP probes
                    let converted = url.replace("wss://", "https://")
                                       .replace("/xrpc/com.atproto.sync.subscribeRepos", "");
                    endpoints.insert(converted);
                }
            }
        }
    } else {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        // Assume plain text list of URLs
        for line in reader.lines() {
            let line = line?;
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                // Convert wss:// to https:// for probes
                let converted = trimmed.replace("wss://", "https://")
                                       .replace("/xrpc/com.atproto.sync.subscribeRepos", "");
                endpoints.insert(converted);
            }
        }
    }

    Ok(endpoints)
}

fn probe_endpoints(endpoints: HashSet<String>, args: &Args) -> Vec<PdsReport> {
    let client = Client::builder()
        .timeout(Duration::from_secs(args.timeout))
        .user_agent("SovereignMeshCrawler/1.0 (ATProto Discovery)")
        .build()
        .expect("Failed to build HTTP client");

    let queue = Arc::new(Mutex::new(endpoints.into_iter().collect::<Vec<_>>()));
    let results = Arc::new(DashMap::new());
    let progress = Arc::new(AtomicUsize::new(0));
    let total = queue.lock().unwrap().len();

    let mut workers = Vec::new();
    for _ in 0..args.threads {
        let queue = Arc::clone(&queue);
        let results = Arc::clone(&results);
        let client = client.clone();
        let progress = Arc::clone(&progress);

        workers.push(thread::spawn(move || {
            loop {
                let url_str = {
                    let mut q = queue.lock().unwrap();
                    if q.is_empty() {
                        break;
                    }
                    q.pop().unwrap()
                };

                let report = probe_pds(&client, url_str);
                results.insert(report.url.clone(), report);
                
                let cur = progress.fetch_add(1, Ordering::Relaxed);
                if cur % 100 == 0 {
                    eprint!("\r[Crawler] {}/{} probed...", cur, total);
                }
            }
        }));
    }

    for w in workers {
        w.join().unwrap();
    }
    println!("\r[Crawler] {}/{} probed. Done.", total, total);

    // Collect results from DashMap
    let mut final_results = Vec::new();
    for entry in results.iter() {
        final_results.push(entry.value().clone());
    }
    final_results
}

fn probe_pds(client: &Client, url_str: String) -> PdsReport {
    let start = Instant::now();
    let hostname = Url::parse(&url_str)
        .map(|u| u.host_str().unwrap_or("unknown").to_string())
        .unwrap_or_else(|_| "invalid".to_string());

    // We probe the /xrpc/com.atproto.server.describeServer endpoint
    let probe_url = format!("{}/xrpc/com.atproto.server.describeServer", url_str.trim_end_matches('/'));
    
    match client.get(&probe_url).send() {
        Ok(resp) => {
            let latency = start.elapsed().as_millis();
            let status = resp.status();
            
            if status.is_success() {
                if let Ok(json) = resp.json::<serde_json::Value>() {
                    let version = json["version"].as_str().map(|s| s.to_string());
                    
                    let grade = if latency < 200 { HealthGrade::A } 
                               else if latency < 500 { HealthGrade::B }
                               else { HealthGrade::C };

                    PdsReport {
                        url: url_str,
                        hostname,
                        latency_ms: latency,
                        grade,
                        version,
                        app_version: None,
                        error: None,
                        last_seen: chrono::Utc::now().to_rfc3339(),
                    }
                } else {
                    PdsReport {
                        url: url_str,
                        hostname,
                        latency_ms: latency,
                        grade: HealthGrade::C,
                        version: None,
                        app_version: None,
                        error: Some("Invalid JSON response".to_string()),
                        last_seen: chrono::Utc::now().to_rfc3339(),
                    }
                }
            } else {
                PdsReport {
                    url: url_str,
                    hostname,
                    latency_ms: latency,
                    grade: HealthGrade::D,
                    version: None,
                    app_version: None,
                    error: Some(format!("HTTP {}", status)),
                    last_seen: chrono::Utc::now().to_rfc3339(),
                }
            }
        }
        Err(e) => {
            PdsReport {
                url: url_str,
                hostname,
                latency_ms: start.elapsed().as_millis(),
                grade: HealthGrade::F,
                version: None,
                app_version: None,
                error: Some(e.to_string()),
                last_seen: chrono::Utc::now().to_rfc3339(),
            }
        }
    }
}

fn save_results(results: &[PdsReport], path: &str) -> Result<()> {
    let file = File::create(path)?;
    serde_json::to_writer_pretty(file, results)?;
    Ok(())
}

fn print_summary(results: &[PdsReport]) {
    let mut grades = HashMap::new();
    for r in results {
        *grades.entry(format!("{:?}", r.grade)).or_insert(0) += 1;
    }

    println!("\n--- Mesh Health Summary ---");
    println!("Total PDS Nodes: {}", results.len());
    for (grade, count) in grades {
        println!("Grade {}: {}", grade, count);
    }
    println!("---------------------------\n");
}
