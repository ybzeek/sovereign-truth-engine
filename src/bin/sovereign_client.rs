//! Sovereign Client - Example consumer for the Sovereign Relay.
//! Demonstrates the Zstd-dictionary handshake and high-efficiency decompression.

use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;
use futures::StreamExt;
use clap::Parser;
use zstd::bulk::Decompressor;
use serde_json::Value;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Relay URL
    #[arg(short, long, default_value = "ws://localhost:8080")]
    url: String,

    /// Optional cursor to start from
    #[arg(short, long)]
    cursor: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    
    let mut url = args.url;
    if !url.ends_with('/') && args.cursor.is_some() {
        url.push('/');
    }
    if let Some(c) = args.cursor {
        url.push_str(&format!("?cursor={}", c));
    }

    println!("[Client] Connecting to {}...", url);
    let (ws_stream, _) = connect_async(url).await?;
    let (mut _ws_sink, mut ws_source) = ws_stream.split();

    // 1. Receive Handshake (JSON)
    let msg = ws_source.next().await.ok_or("No handshake message")??;
    if let Message::Text(text) = msg {
        let handshake: Value = serde_json::from_str(&text)?;
        println!("[Handshake] Received protocol metadata: {}", handshake);
    } else {
        return Err("Expected JSON handshake".into());
    }

    // 2. Receive Dictionary (Binary)
    let msg = ws_source.next().await.ok_or("No dictionary message")??;
    let dict_data = if let Message::Binary(bin) = msg {
        println!("[Handshake] Received Zstd dictionary ({} bytes)", bin.len());
        bin
    } else {
        return Err("Expected binary dictionary".into());
    };

    // 3. Prepare Decompressor
    let mut decompressor = Decompressor::with_dictionary(&dict_data)?;
    let mut output_buffer = vec![0u8; 1024 * 1024]; // 1MB buffer for decompressed frame

    println!("[Stream] Handshake complete. Waiting for records...\n");

    // 4. Stream Loop (Cluster Mode)
    while let Some(msg) = ws_source.next().await {
        match msg? {
            Message::Binary(compressed_cluster) => {
                // Decompress the entire cluster burst
                match decompressor.decompress_to_buffer(&compressed_cluster, &mut output_buffer) {
                    Ok(size) => {
                        let cluster_raw = &output_buffer[..size];
                        if size < 2 { continue; }

                        // Parse Sovereign Cluster Header
                        let num_records = u16::from_le_bytes(cluster_raw[0..2].try_into().unwrap());
                        let mut offset = 2 + (num_records as usize * 12);
                        
                        for i in 0..num_records as usize {
                            let head_ptr = 2 + i * 12;
                            let seq = u64::from_le_bytes(cluster_raw[head_ptr..head_ptr + 8].try_into().unwrap());
                            let len = u32::from_le_bytes(cluster_raw[head_ptr + 8..head_ptr + 12].try_into().unwrap()) as usize;
                            
                            if offset + len <= size {
                                let record = &cluster_raw[offset..offset + len];
                                println!("[Seq {}] Recv {} bytes (cluster burst)", seq, len);
                                
                                // Peek at record
                                let peek_len = len.min(32);
                                println!("  Peek: {}", hex::encode(&record[..peek_len]));
                                
                                offset += len;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("[Error] Decompression failed: {}", e);
                    }
                }
            }
            Message::Close(_) => {
                println!("[Info] Server closed connection.");
                break;
            }
            _ => {}
        }
    }

    Ok(())
}
