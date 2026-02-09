//! Firehose Tap - Minimal CLI to prove data flows through
//!
//! Usage:
//!   cargo run --release -p did_mmap_cache --bin firehose_tap
//!   cargo run --release -p did_mmap_cache --bin firehose_tap -- --endpoint wss://some-pds.example.com
//!   cargo run --release -p did_mmap_cache --bin firehose_tap -- --limit 100
//!
//! Connects to the firehose, parses commits, and outputs JSON to stdout.

use did_mmap_cache::parser::core::parse_input;
use tungstenite::Message;
use url::Url;
use std::io::{self, Write};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    
    // Parse simple args
    let mut endpoint = "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos".to_string();
    let mut limit: Option<u64> = None;
    let mut raw_mode = false;
    
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--endpoint" | "-e" => {
                if i + 1 < args.len() {
                    endpoint = args[i + 1].clone();
                    // Auto-append the subscribeRepos path if it looks like a bare host
                    if !endpoint.contains("subscribeRepos") {
                        if !endpoint.ends_with('/') {
                            endpoint.push('/');
                        }
                        endpoint.push_str("xrpc/com.atproto.sync.subscribeRepos");
                    }
                    i += 1;
                }
            }
            "--limit" | "-n" => {
                if i + 1 < args.len() {
                    limit = args[i + 1].parse().ok();
                    i += 1;
                }
            }
            "--raw" | "-r" => {
                raw_mode = true;
            }
            "--help" | "-h" => {
                eprintln!("Firehose Tap - Minimal ATProto Firehose Consumer");
                eprintln!();
                eprintln!("Usage: firehose_tap [OPTIONS]");
                eprintln!();
                eprintln!("Options:");
                eprintln!("  -e, --endpoint <URL>   WebSocket endpoint (default: bsky.network relay)");
                eprintln!("  -n, --limit <N>        Stop after N messages");
                eprintln!("  -r, --raw              Output raw hex instead of parsed JSON");
                eprintln!("  -h, --help             Show this help");
                eprintln!();
                eprintln!("Examples:");
                eprintln!("  firehose_tap                          # Relay stream to stdout");
                eprintln!("  firehose_tap -n 10                    # First 10 messages");
                eprintln!("  firehose_tap -e wss://pds.example.com # Direct PDS connection");
                eprintln!("  firehose_tap | jq .did                # Pipe to jq for filtering");
                return;
            }
            _ => {}
        }
        i += 1;
    }

    let url = match Url::parse(&endpoint) {
        Ok(u) => u,
        Err(e) => {
            eprintln!("{{\"error\":\"invalid_url\",\"message\":\"{}\"}}", e);
            return;
        }
    };

    let mut socket = match tungstenite::connect(url.as_str()) {
        Ok((s, _)) => s,
        Err(e) => {
            eprintln!("{{\"error\":\"connection_failed\",\"message\":\"{}\"}}", e);
            return;
        }
    };

    let stdout = io::stdout();
    let mut out = stdout.lock();
    let mut count: u64 = 0;

    loop {
        match socket.read() {
            Ok(Message::Binary(bin)) => {
                if raw_mode {
                    // Output raw hex for debugging
                    writeln!(out, "{}", hex::encode(&bin)).ok();
                } else {
                    // Parse and output as JSON
                    match parse_input(&bin) {
                        Some(envelope) => {
                            let did_str = envelope.did
                                .and_then(|d| std::str::from_utf8(d).ok())
                                .unwrap_or("unknown");
                            let seq = envelope.sequence.unwrap_or(0);
                            let sig_hex = envelope.signature
                                .map(|s| hex::encode(s))
                                .unwrap_or_default();
                            let event_type = envelope.t
                                .and_then(|t| std::str::from_utf8(t).ok())
                                .unwrap_or("unknown");
                            
                            let json_out = serde_json::json!({
                                "seq": seq,
                                "did": did_str,
                                "type": event_type,
                                "signature_hex": sig_hex,
                                "raw_bytes": bin.len(),
                            });
                            writeln!(out, "{}", json_out).ok();
                        }
                        None => {
                            // Non-commit message (identity, handle, etc.) - output minimal info
                            writeln!(out, "{{\"raw_bytes\":{},\"parse\":\"non-commit\"}}", bin.len()).ok();
                        }
                    }
                }

                count += 1;
                if let Some(n) = limit {
                    if count >= n {
                        break;
                    }
                }
            }
            Ok(_) => {} // Ignore non-binary messages
            Err(_) => {
                break;
            }
        }
    }
}
