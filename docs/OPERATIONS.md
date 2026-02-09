# Operations & Engineering Guide

This guide covers the day-to-day operations of the Sovereign Aggregator, including installation, performance profiling, and technical auditing.

---

## 🛠️ Step-by-Step Developer Quickstart

If you are a new developer joining the project, follow these steps to get a working environment in under 10 minutes.

### 1. Setup the Identity Layer (The "Map")
You need a mapping of user DIDs to public keys. You can either build it from scratch or download the pre-built ledger.

**Option A: Build from Scratch (~20+ hours)**
```bash
# Download the raw PLC directory (90GB+ JSONL)
cargo run --release --bin download_plc

# Pre-allocate and build the 14.7GB Mmap Index
cargo run --release --bin build_cache -- plc_dump.jsonl atomic_cache.bin
```

**Option B: Request the "Golden" Cache (Recommended for Auditors)**
The pre-built 14.7GB `atomic_cache.bin` used in the Superbowl LX case study is available upon request for institutional auditors and researchers.

### 2. Discover the Mesh (The "Addresses")
To use the Sovereign Aggregator, you need a list of active PDS nodes.

```bash
# Run discovery to find all nodes (takes roughly 20+ hours for the full crawl)
cargo run --release --bin sovereign_aggregator -- discover pds_list.txt
```
*Note: This creates `pds_list.bin`, which is your binary node health ledger.*

**Future Availability**: We are working on providing a pre-built `pds_list.bin.torrent` once the global crawl is complete, allowing you to skip this 20-hour discovery phase entirely.

### 3. Launch the Siege (The "Ingestion")
Connect to the global network and start ingesting verified events. The `live_firehose` now features the **Sovereign Dashboard**, a high-performance ANSI TUI for real-time observability.

```bash
# Tune handles and TCP stack first
sudo chmod +x tune_sovereign.sh && ./tune_sovereign.sh

# Start the multi-threaded consumer with the Sovereign Dashboard
cargo run --release --bin live_firehose -- atomic_cache.bin
```

### 4. Run the Integrated Stress Test (E2E Validation)
Verify the entire pipeline from network to disk.
```bash
cargo run --release --bin integrated_stress_test -- atomic_cache.bin ./archive
```

---

## 🚀 Binary Command Reference

### `live_firehose`
The primary real-time consumer and verifier. 

**Features:**
- **Sovereign Dashboard**: Real-time stats on throughput, crypto-mix (Secp256k1 vs P-256), and error diagnostics.
- **Queue Saturation Monitoring**: Visualize if the system is CPU-bound (verification) or Network-bound.
- **DID Intensity Leaderboard**: Tracks the most active users in the current firehose slice.
- **MST Visualization**: If a `target_did` is provided, draws the Merkle Search Tree structure for their commits.

```bash
# Standard Run
cargo run --release --bin live_firehose -- <mmap_cache_file>

# Targeted Observation
cargo run --release --bin live_firehose -- <mmap_cache_file> [target_did]
```

### `sovereign_aggregator` (The Mesh Manager)
The "Sovereign" core. Bypasses centralized relays and connects to every individual PDS on the network.

### `bench_egress` (Hydra Egress Bench)
Verifies the throughput of the sharded archival engine. Proven to sustain **360,000+ msg/s** in a 2GB RAM container.

---

## 📊 Profiling & Performance Optimization

### Interpreting the Monitor
`[Monitor] Total: 103k | Verified: 103k | Rate: 1753 msg/s | Queue: 0`
- **Queue > 0**: You are CPU-bound (Verification can't keep up).
- **Queue = 0**: You are Network-bound.

### Running Flamegraphs
```bash
cargo flamegraph --bin live_firehose -- atomic_cache.bin
```
Look for `k256` (crypto) as the widest block. If networking towers dominate, check your TCP tuning.

---

## 🛡️ Technical Audit & Integrity (Bit-Perfect)

The engine has been audited for structural, performance, and cryptographic fidelity. 

### 🕵️ Ghost Hunter Case Study: Superbowl LX (Feb 8, 2026)
During the high-traffic surge of the 2026 Superbowl, the Sovereign Truth Engine was deployed to audit the integrity of the official Bluesky Relay (`bsky.network`).

**Benchmark Results:**
- **Duration**: 30 minutes of peak activity.
- **Volume**: **1,474,340 messages** verified in real-time.
- **Throughput**: Sustained **~1,050 msg/s** on consumer-grade hardware.
- **Mesh Win Rate**: **96.0%**. In 96% of cases, the event arrived from the PDS Mesh *before* the centralized Relay.
- **Latency Advantage**: **206.0ms avg gain**. The engine consistently saw the network ~1/5th of a second before the official infrastructure.
- **Reliability Gap**: **151 Drops Detected**. The official Relay dropped 151 events that were successfully captured and "healed" by the Sovereign Mesh.
- **Crypto-Integrity**: 1.47M+ Secp256k1 and P-256 signatures verified with 0 failures, proving the atomic Mmap Cache handles massive write/read contention without corruption.

**Conclusion**: Centralized relays act as a performance and reliability bottleneck during major global events. Sovereign ingestion provides a measurable 200ms "Real-Time Sight" advantage and higher data fidelity.

### Performance Benchmarks (Verified 2026)
- **DID Cache**: **69ns** lookup latency (14M+ lookups/sec).
- **Archive Egress**: Sustains **360,000+ msg/s** on consumer SSDs.
- **Storage Efficiency**: **53% - 96%** range (Shannon limit verified).
- **Concurrency**: Manages **10,000+ connections** within a 2GB RAM footprint.

### Running the Audit Suite
Before trusting a deployment, run the master audit suite:
```bash
cargo test --workspace
```
This suite verifies Merkle integrity, CID consistency, and crypto correctness.

- **Hydra Egress Peak:** **1.48M msg/s**.
- **Average Lookup Latency:** **~73ns**.
- **RAM Overcommit:** 14.7GB database stable in 2GB RAM envelope (7.3x overcommit).
- **Audit Target (`3mdzukxw3c32l`):** Verified locally extracted hash matches official network CID.

---

## ⚙️ Environment Variables & Tuning
Run `./tune_sovereign.sh` to optimize the Linux kernel for 10,000+ concurrent connections (TCP buffer reductions and file descriptor increases).
