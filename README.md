# ATProto High-Performance Archival Engine & Truth Engine

The **Truth Engine** is a sovereign, high-performance infrastructure component for the AT Protocol. It allows a single consumer-grade computer to aggregate, verify, and archive the entire global firehose by connecting directly to thousands of PDS nodes, bypassing centralized relays.

---

## 🚀 The Milestones

### 1. Speed (The "impossible" Lookup)
- **Result**: **69ns average lookup** per DID.
- **Reality Check**: This is essentially hardware-line speed. At 69ns, a single CPU core can perform over 14 million identity checks per second, allowing the engine to verify the global firehose without breaking a sweat.

### 2. Efficiency (The Clustered Advantage)
- **Result**: **53% to 96% storage reduction**.
- **The Secret Sauce**: "Clustered Vertical Logging" (CVL). By grouping messages by user within archival segments, we achieved an **86% reduction** on realistic mixed traffic and **53%** even when messages were 40% uncompressible cryptographic noise (The Shannon Entropy Floor).

### 3. Verifiability (Merkle Proofs)
- **Result**: Every segment contains a **Blake3 Merkle Root**.
- **The Impact**: Transforms a simple database into a **verifiable ledger**, allowing clients to trust the archive without trusting the archivist.

### 4. Sovereignty (Direct-to-PDS Mesh)
- **Result**: **Sovereign Mesh Aggregator** bypassed centralized relays with 10,000+ simultaneous WebSocket connections.
- **The Impact**: Decentralizes the "backbone" of the network—allowing anyone to be their own relay (BGS) on consumer hardware.

### 5. Truth Engine (Real-Time Auditing)
- **Result**: **206ms - 234ms latency advantage** over official relays.
- **Battle-Tested**: Successfully audited 1.5M messages during the 2026 Superbowl (LX), detecting 151 individual events dropped by the official infrastructure.

---

## 🏗️ Getting Started

### Prerequisites

- **Rust**: [rustup.rs](https://rustup.rs) (Edition 2021)
- **Linux**: Optimized for Linux (uses \`mmap\`).
- **Pure-Rust TLS**: No system OpenSSL is required (uses \`rustls\`).
- **Disk Space**: ~15GB for the cache file.

---

## 📚 Documentation Map

The project documentation is organized into four core pillars:

1. **[The Journey](JOURNEY.md)**: **Read First.** The personal history, the goal ("Become the Firehose"), and the evolution of the project from "The Loom" to the "Truth Engine."
2. **[Engineering Challenges](docs/struggles.md)**: **Technical deep-dive.** A deep-dive into the "Impossible" hurdles—from the 14.7GB Mmap Barrier to the 10,000 connection siege.
3. **[Post-Mortem & Reflections](POSTMORTEM.md)**: **The Human Side.** An honest reflection on engineering burnout, the reality of high-performance development, and the personal cost of technical success.
4. **[Operations Guide](docs/OPERATIONS.md)**: **Usage.** Step-by-step Quickstart, CLI reference, and performance profiling.
5. **[Sovereign Architecture](docs/ARCHITECTURE.md)**: **The Specs.** Details on CVL storage, Mmap internal structure, and the Sovereign Mesh algorithm.

---

## 🏗️ Reproducing the "Ultimate Cage" Results (Audit Only)

Building the full 14.7GB identity cache from the raw PLC directory takes roughly 20+ hours. For institutional auditors or protocol researchers requiring the "Golden" state used in the Superbowl LX audit, please reach out via DM/Issue to request access to the pre-built ledger.

### Setup Checklist
1. **Initialize Cache**: `cargo run --release --bin build_cache`
2. **Setup**: `echo "26978169757" > cursor.txt`
3. **Run via Docker Compose**:
   ```bash
   sudo docker-compose up --build
   ```
   Tests **2.0GB RAM Cap** (7.3x memory overcommit) and **1.0 CPU Core** pinning.

---

## 🤝 Contributing
PRs for optimization and docs are welcome. See **[docs/](docs/)** for technical specifications.

**Seeding**: If you are interested in running a high-fidelity audit and need access to the 14.7GB pre-built identity ledger, please contact the maintainer.

## 📄 License
This project is licensed under the MIT License.

---

## Project Status & Reflections

The technical goals of the **Truth Engine** have been achieved. The system is stable, high-performance, and open for community audit.

Engineering at this level of intensity often comes with significant personal and professional lessons. For a reflective look at the human cost of building high-performance infrastructure and the lessons learned from this specific technical journey, please see the **[Engineering Post-Mortem](POSTMORTEM.md)**.
