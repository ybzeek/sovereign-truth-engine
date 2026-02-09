//! Sovereign Relay - Unfiltered firehose provider.
//! Serves historical and live ATProto records from high-efficiency archival storage.
//! Supports Zstd-compressed framing for 70% egress reduction.

use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::protocol::Message;
use futures::{StreamExt, SinkExt};
use clap::Parser;
use did_mmap_cache::archive::MultiShardArchive;
use std::path::PathBuf;
use tracing::{info, warn, error};

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Port to listen on
    #[arg(short, long, default_value_t = 8080)]
    port: u16,

    /// Path to archive directory
    #[arg(short, long, default_value = "sovereign_archive")]
    archive: String,

    /// Path to Zstd dictionary
    #[arg(short, long, default_value = "atproto_firehose.dict")]
    dict: String,

    /// Compression level (1-22)
    #[arg(long, default_value_t = 3)]
    compression_level: i32,
}

use std::sync::atomic::{AtomicU64, Ordering};

struct RelayState {
    archive: MultiShardArchive,
    dict: Vec<u8>,
    _compression_level: i32,
    sent_clusters: AtomicU64,
    sent_bytes: AtomicU64,
    filtered_msgs: AtomicU64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    info!("Starting Sovereign Relay on port {}", args.port);

    // 1. Load Dictionary
    let dict = std::fs::read(&args.dict).expect("Failed to read dictionary");
    info!("Loaded Zstd dictionary ({} bytes)", dict.len());

    // 2. Load Archive (Multi-shard aware)
    let archive_path = PathBuf::from(&args.archive);
    let combined_archive = MultiShardArchive::open_readonly(&archive_path, Some(dict.clone()))?;
    
    info!("Archive ready with {} shards", combined_archive.reader_count());

    let state = Arc::new(RelayState {
        archive: combined_archive,
        dict,
        _compression_level: args.compression_level,
        sent_clusters: AtomicU64::new(0),
        sent_bytes: AtomicU64::new(0),
        filtered_msgs: AtomicU64::new(0),
    });

    let listener = TcpListener::bind(format!("0.0.0.0:{}", args.port)).await?;
    info!("Listening for connections...");

    let state_clone = Arc::clone(&state);
    let _server_task = tokio::spawn(async move {
        while let Ok((stream, addr)) = listener.accept().await {
            let state = Arc::clone(&state_clone);
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, state, addr).await {
                    error!("Connection error ({}): {}", addr, e);
                }
            });
        }
    });

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    info!("Shutdown signal received. Finalizing metrics...");

    let sent_c = state.sent_clusters.load(Ordering::Relaxed);
    let sent_b = state.sent_bytes.load(Ordering::Relaxed);
    let filtered = state.filtered_msgs.load(Ordering::Relaxed);

    println!("\n╔═══════════════════════════════════════════════════════════════════════╗");
    println!("║                   SOVEREIGN RELAY SHUTDOWN SUMMARY                  ║");
    println!("╚═══════════════════════════════════════════════════════════════════════╝");
    println!("  Total Clusters Served:   {}", sent_c);
    println!("  Total Egress Data:       {:.2} MB", sent_b as f64 / 1024.0 / 1024.0);
    println!("  Tombstones Filtered:     {} messages", filtered);
    println!("-------------------------------------------------------------------------");
    println!("  Archive Location:        {}", args.archive);
    println!("  Status:                  Clean Exit\n");

    Ok(())
}

async fn handle_connection(stream: TcpStream, state: Arc<RelayState>, addr: std::net::SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::{AtomicU64, Ordering};
    
    let cursor_atomic = Arc::new(AtomicU64::new(u64::MAX));
    let cursor_clone = Arc::clone(&cursor_atomic);

    info!("New connection from: {}", addr);

    let ws_stream = match tokio_tungstenite::accept_hdr_async(stream, move |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response: tokio_tungstenite::tungstenite::handshake::server::Response| {
        let uri = request.uri();
        info!("  WS Request URI: {}", uri);
        if let Some(query) = uri.query() {
            for part in query.split('&') {
                if part.starts_with("cursor=") {
                    if let Ok(val) = part[7..].parse::<u64>() {
                        cursor_clone.store(val, Ordering::SeqCst);
                    }
                }
            }
        }
        Ok(response)
    }).await {
        Ok(s) => s,
        Err(e) => {
            error!("  WebSocket Handshake failed for {}: {}", addr, e);
            return Err(e.into());
        }
    };

    let cursor_val = cursor_atomic.load(Ordering::SeqCst);
    let cursor = if cursor_val == u64::MAX { None } else { Some(cursor_val) };

    let (mut ws_sink, mut _ws_source) = ws_stream.split();

    // 1. Handshake: Send protocol metadata and dictionary
    let dict_hash = hex::encode(blake3::hash(&state.dict).as_bytes());
    let handshake = serde_json::json!({
        "version": 1,
        "compression": "zstd",
        "dict_hash": dict_hash,
        "info": "Sovereign Relay v0.1.0 - Unfiltered Firehose"
    });

    if let Err(e) = ws_sink.send(Message::Text(handshake.to_string())).await {
        warn!("  Failed to send handshake JSON to {}: {}", addr, e);
        return Ok(());
    }
    if let Err(e) = ws_sink.send(Message::Binary(state.dict.clone())).await {
        warn!("  Failed to send dictionary to {}: {}", addr, e);
        return Ok(());
    }
    info!("  Handshake complete for {}. Dictionary sent (hash: {})", addr, &dict_hash[..8]);

    // 2. Negotiation (Start from cursor or min_seq)
    // If no segments exist yet, wait until some appear
    let mut start_seq = cursor.or_else(|| state.archive.min_seq());
    
    while start_seq.is_none() {
        info!("  No segments found in archive. Waiting...");
        state.archive.refresh().ok();
        tokio::time::sleep(Duration::from_secs(5)).await;
        start_seq = cursor.or_else(|| state.archive.min_seq());
    }
    
    let mut current_seq = start_seq.unwrap_or(0);
    info!("  Streaming to {} starting from seq {}", addr, current_seq);

    // ZERO-COPY RELAY LOOP
    // We send entire compressed clusters as they are stored on disk.
    // This allows the server to act as a pure byte-streamer with minimal CPU.
    
    let mut last_cluster_hash = [0u8; 32];

    loop {
        // 1. Fetch the raw compressed cluster from the archive
        // NOTE: If the sequence is tombstoned, this currently returns NotFound.
        // We should distinguish between "Tombstoned" and "End of Archive".
        match state.archive.get_raw_cluster_at_seq(current_seq) {
            Ok(cluster_data) => {
                let current_hash = blake3::hash(&cluster_data).into();
                
                // Only send the cluster if it's new (multiple sequences share one cluster)
                if current_hash != last_cluster_hash {
                    let len = cluster_data.len();
                    if let Err(e) = ws_sink.send(Message::Binary(cluster_data)).await {
                        warn!("  Failed to send cluster to {}: {}", addr, e);
                        break;
                    }
                    state.sent_clusters.fetch_add(1, Ordering::Relaxed);
                    state.sent_bytes.fetch_add(len as u64, Ordering::Relaxed);
                    last_cluster_hash = current_hash;
                }
                
                // Track current progress
                current_seq += 1;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let err_msg = e.to_string();
                if err_msg.contains("tombstoned") {
                    // Skip this message but continue to next
                    state.filtered_msgs.fetch_add(1, Ordering::Relaxed);
                    current_seq += 1;
                } else {
                    // End of current archive data. Refresh and wait.
                    state.archive.refresh().ok();
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            Err(e) => {
                error!("  Archive read error for {}: {}", addr, e);
                break;
            }
        }

        tokio::task::yield_now().await;
    }

    info!("Closing connection");
    Ok(())
}
