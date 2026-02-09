# ATProto Sovereign Architecture

## 1. Overview
The **Sovereign Truth Engine** is a high-performance, zero-copy infrastructure layer for the AT Protocol. It operates on a "Dual-Path" ingestion model, designed to verify the integrity of the global firehose by connecting directly to thousands of Personal Data Servers (PDS), bypassing or shadowing centralized relays.

---

## 2. Core Pillars

### 2.1 Clustered Virtual Log (CVL)
Standard chronological firehose storage suffers from high entropy. The Truth Engine uses **CVL** to reorganize messages by **DID** (Identity) within archival segments before compression.
- **Efficiency**: Verified at **68.22% average reduction** (reaching **96%** for high-volume users). 
- **Entropy Floor**: Successfully maintains a **53.34% reduction** even when payloads contain 100 bytes of uncompressible cryptographic signatures per message.
- **V2.2 Indexing**: Uses a 28-byte fixed-width index record:
  - `bin_off(8)`: File offset in binary data.
  - `c_len(4)`: Compressed cluster length.
  - `inner_off(4)`: Offset within the decompressed cluster.
  - `i_len(4)`: Original message length.
  - `path_hash(8)`: FxHash of the message path (e.g., `app.bsky.feed.post/123`).
- **Integrity**: Every segment contains a **Blake3 Merkle Root**, allowing for verifiable proofs of inclusion.
- **Sharding**: Parallelized across 16 shards to eliminate I/O bottlenecks.

### 2.2 Memory-Mapped DID Cache (Identity Layer)
A 14.7GB lock-free hash map mapped directly from disk into memory.
- **Lookup Speed**: **69ns** average per DID (Verified via `bench_cache`).
- **Atomic Updates**: Uses a Release-Acquire protocol to allow concurrent, high-speed identity updates without service interruption.
- **Capacity**: Optimized for >150 million identity records on a single consumer machine.

### 2.3 The Ghost Hunter (Audit Engine)
The core of the censorship detection logic.
- **Mesh Path**: Connects directly to thousands of PDS nodes via the **Sovereign Mesh Aggregator**.
- **Relay Path**: Shadow-consumes the official Relay (`bsky.network`).
- **Race Detection**: Uses a concurrent `DashMap` to track CID arrival times across both paths.
- **Latency Measurement**: Identifies the precise millisecond delta between the Mesh and the Relay (Observed avg: ~132ms).
- **Drop Detection**: If an event appears on the Mesh but is absent from the Relay after a 5-second window, it is flagged as a **True Drop**.

### 2.4 Sovereign Mesh (The "Siege" Engine)
- **Fan-In Scale**: Manages 10,000+ persistent WebSockets.
- **Bloom Filter Defense**: A 1MB L1-guard that rejects duplicate events at L3-cache speeds.
- **Adaptive Discovery**: An autonomous "Sweet Spot" crawler that dynamically adjusts request throughput based on PLC Directory rate limits.

### 2.5 The Tombstone Lattice (Global Bitset)
A performance-critical 512MB memory-mapped bitset (`TombstoneStore`) providing atomic, O(1) message deletions across the entire archive.
- **Capacity**: Supports masking up to 4.29 Billion messages.
- **Synchronicity**: A single bit flip immediately hides an event from all relay subscribers.

### 2.6 Real-Time Egress Filtering
The `sovereign_relay` implements a "Transparent Proxy" for the archive.
- **Dynamic Masking**: On-the-fly filtering of deleted messages using the Tombstone Lattice.
- **Zstd Cluster Re-cycling**: Efficiently decompresses and re-compresses clusters only when a message within them is deleted, preserving maximum bandwidth for the common case.

---

## 3. Logical Pipeline
1. **Ingestion**: `sovereign_aggregator` (Discovery) and `sovereign_ingester` (Siege) gather data from the Mesh.
2. **De-duplication**: L1 Bloom Filter checks CIDs against the local state.
3. **Normalization**: Raw DAG-CBOR CIDs are normalized to ensure binary equivalence for auditing.
4. **Verification**: Parallel worker threads verify cryptographic signatures using the Mmap Cache.
5. **Archival**: `ArchiveWriter` clusters messages into compressed segments using Zstd with custom dictionaries.
6. **Reporting**: The `monitor.rs` TUI visualizes live throughput, win-rates, and cryptographic health.

---

## 4. Documentation Map
- **[The Journey](../JOURNEY.md)**: Narrative of the build and findings.
- **[Technical Struggles](struggles.md)**: Engineering post-mortem on specific hurdles.
- **[Operations Guide](OPERATIONS.md)**: Deployment and usage instructions.
- **[The Human Post-Mortem](../POSTMORTEM.md)**: Reflections on the reality of high-performance development.
