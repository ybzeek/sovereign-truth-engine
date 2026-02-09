use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

pub enum ErrorType {
    InvalidSignature,
    MissingKey,
    RepoNotFound,
    MalformedCbor,
}

pub struct SovereignMonitor {
    pub total: AtomicU64,
    pub verified: AtomicU64,
    pub healed: AtomicU64,
    pub failed_sig: AtomicU64,
    pub failed_missing: AtomicU64,
    pub failed_other: AtomicU64,
    
    // Ghost Hunter Specifics
    pub ghost_hunter_loops: AtomicU64,
    pub dropped_by_relay: AtomicU64,
    pub relay_wins: AtomicU64,
    pub mesh_wins: AtomicU64,
    pub total_lat_gain_ms: AtomicU64,

    // Networking
    pub active_conns: AtomicU64,
    pub conn_errors: AtomicU64,
    
    // Key Mix
    pub k256_count: AtomicU64,
    pub p256_count: AtomicU64,
    
    // Leaderboard (DID -> Count)
    pub leaderboard: DashMap<String, u64>,
    pub handle_cache: DashMap<String, String>,
    
    // Recent Bursts for Tap
    pub tap_buffer: Mutex<Vec<String>>,
    pub drop_buffer: Mutex<Vec<String>>,

    pub start_time: Instant,
}

impl SovereignMonitor {
    pub fn new() -> Self {
        Self {
            total: AtomicU64::new(0),
            verified: AtomicU64::new(0),
            healed: AtomicU64::new(0),
            failed_sig: AtomicU64::new(0),
            failed_missing: AtomicU64::new(0),
            failed_other: AtomicU64::new(0),
            
            ghost_hunter_loops: AtomicU64::new(0),
            dropped_by_relay: AtomicU64::new(0),
            relay_wins: AtomicU64::new(0),
            mesh_wins: AtomicU64::new(0),
            total_lat_gain_ms: AtomicU64::new(0),

            active_conns: AtomicU64::new(0),
            conn_errors: AtomicU64::new(0),

            k256_count: AtomicU64::new(0),
            p256_count: AtomicU64::new(0),
            leaderboard: DashMap::with_capacity(10000),
            handle_cache: DashMap::with_capacity(1000),
            tap_buffer: Mutex::new(Vec::with_capacity(100)),
            drop_buffer: Mutex::new(Vec::with_capacity(100)),
            start_time: Instant::now(),
        }
    }

    pub fn push_tap(&self, msg: String) {
        let mut buf = self.tap_buffer.lock().unwrap();
        if buf.len() >= 50 { buf.remove(0); }
        buf.push(msg);
    }

    pub fn push_drop(&self, msg: String) {
        let mut buf = self.drop_buffer.lock().unwrap();
        if buf.len() >= 50 { buf.remove(0); }
        buf.push(msg);
    }

    pub fn record_event(&self, did: &str, success: bool, error: Option<ErrorType>, key_type: Option<u8>) {
        self.total.fetch_add(1, Ordering::Relaxed);
        
        // Update Leaderboard
        *self.leaderboard.entry(did.to_string()).or_insert(0) += 1;

        if success {
            self.verified.fetch_add(1, Ordering::Relaxed);
            if let Some(kt) = key_type {
                match kt {
                    1 => { self.k256_count.fetch_add(1, Ordering::Relaxed); },
                    2 => { self.p256_count.fetch_add(1, Ordering::Relaxed); },
                    _ => {}
                }
            }
        } else {
            match error {
                Some(ErrorType::InvalidSignature) => { self.failed_sig.fetch_add(1, Ordering::Relaxed); },
                Some(ErrorType::MissingKey) => { self.failed_missing.fetch_add(1, Ordering::Relaxed); },
                _ => { self.failed_other.fetch_add(1, Ordering::Relaxed); },
            };
        }
    }

    pub fn render(&self, queue_len: usize, rate: f64) {
        // Clear screen and move cursor to top-left
        print!("\x1B[2J\x1B[H");

        let total = self.total.load(Ordering::Relaxed);
        let verified = self.verified.load(Ordering::Relaxed);
        let f_sig = self.failed_sig.load(Ordering::Relaxed);
        let f_miss = self.failed_missing.load(Ordering::Relaxed);
        let healed = self.healed.load(Ordering::Relaxed);
        let k256 = self.k256_count.load(Ordering::Relaxed);
        let p256 = self.p256_count.load(Ordering::Relaxed);
        let active = self.active_conns.load(Ordering::Relaxed);
        let c_errs = self.conn_errors.load(Ordering::Relaxed);

        let k_pct = if verified > 0 { (k256 as f64 / verified as f64) * 100.0 } else { 0.0 };
        let p_pct = if verified > 0 { (p256 as f64 / verified as f64) * 100.0 } else { 0.0 };

        // 1. Header
        println!("\x1B[1;36m╔═══════════════════════════════════════════════════════════════════════╗\x1B[0m");
        println!("\x1B[1;36m║           SOVEREIGN TRUTH ENGINE - LIVE FIREHOSE MONITOR            ║\x1B[0m");
        println!("\x1B[1;36m╚═══════════════════════════════════════════════════════════════════════╝\x1B[0m");

        // 2. Throughput & Connections
        let queue_bar = self.make_bar(queue_len, 5000); // Assume 5k is 'Full'
        println!("\x1B[1;37mRate:\x1B[0m \x1B[1;32m{:.2} msg/s\x1B[0m | \x1B[1;37mTotal:\x1B[0m {} | \x1B[1;37mHealed:\x1B[0m {}", rate, total, healed);
        println!("\x1B[1;37mConns:\x1B[0m \x1B[1;32m{}\x1B[0m | \x1B[1;37mConn Errs:\x1B[0m \x1B[1;31m{}\x1B[0m | \x1B[1;37mQueue Saturation:\x1B[0m [{}] {:5} msgs", active, c_errs, queue_bar, queue_len);
        println!();

        // 3. Ghost Hunter Status (Mesh vs Relay)
        let m_wins = self.mesh_wins.load(Ordering::Relaxed);
        let r_wins = self.relay_wins.load(Ordering::Relaxed);
        let total_wins = m_wins + r_wins;
        let win_pct = if total_wins > 0 { (m_wins as f64 / total_wins as f64) * 100.0 } else { 0.0 };
        let avg_gain = if m_wins > 0 { self.total_lat_gain_ms.load(Ordering::Relaxed) as f64 / m_wins as f64 } else { 0.0 };

        println!("\x1B[1;37m[ Ghost Hunter Status ]\x1B[0m                 \x1B[1;37m[ Network Efficiency ]\x1B[0m");
        println!("  Mesh Win Rate: \x1B[1;32m{:>3.1}%\x1B[0m ({:>8})            Relay Wins: \x1B[1;31m{}\x1B[0m", win_pct, m_wins, r_wins);
        println!("  Avg Mesh Gain: \x1B[1;32m{:.1}ms\x1B[0m                    Relay Drops: \x1B[1;31m{}\x1B[0m", avg_gain, self.dropped_by_relay.load(Ordering::Relaxed));
        println!();

        // 4. Stats Grid
        println!("\x1B[1;37m[ Crypto Breakdown ]\x1B[0m                     \x1B[1;37m[ Error Diagnostics ]\x1B[0m");
        println!("  Secp256k1: \x1B[1;34m{:>3.1}%\x1B[0m ({:>8})            Invalid Sig: \x1B[1;31m{}\x1B[0m", k_pct, k256, f_sig);
        println!("  P-256:     \x1B[1;35m{:>3.1}%\x1B[0m ({:>8})            Missing Key: \x1B[1;33m{}\x1B[0m", p_pct, p256, f_miss);
        println!();

        // 4. Leaderboard
        println!("\x1B[1;37m[ Top 10 Active DIDs (Intensity) ]\x1B[0m");
        let mut board: Vec<_> = self.leaderboard.iter().map(|kv| (kv.key().clone(), *kv.value())).collect();
        board.sort_by(|a, b| b.1.cmp(&a.1));
        
        for (i, (did, count)) in board.iter().take(10).enumerate() {
            let display_name = if let Some(handle) = self.handle_cache.get(did) {
                format!("{:<30} ({})", handle.value(), did)
            } else {
                did.clone()
            };
            println!("  {:>2}. \x1B[32m{:<50}\x1B[0m | \x1B[1;33m{:>8} msgs\x1B[0m", i + 1, display_name, count);
        }

        // Self-clean leaderboard periodically if it explodes
        if self.leaderboard.len() > 100000 {
            self.leaderboard.clear();
        }
        if self.handle_cache.len() > 10000 {
            self.handle_cache.clear();
        }

        println!("\x1B[90m-------------------------------------------------------------------------");
        println!(" Uptime: {:?} | Press Ctrl+C to save cursor and exit\x1B[0m", self.start_time.elapsed());
    }

    fn make_bar(&self, val: usize, max: usize) -> String {
        let width = 40;
        let filled = ((val as f64 / max as f64) * width as f64) as usize;
        let filled = filled.min(width);
        let mut bar = String::with_capacity(width);
        for i in 0..width {
            if i < filled {
                bar.push('█');
            } else {
                bar.push('░');
            }
        }
        
        // Color the bar based on saturation
        if filled > (width * 8 / 10) {
            format!("\x1B[31m{}\x1B[0m", bar) // Red
        } else if filled > (width / 2) {
            format!("\x1B[33m{}\x1B[0m", bar) // Yellow
        } else {
            format!("\x1B[32m{}\x1B[0m", bar) // Green
        }
    }
}
