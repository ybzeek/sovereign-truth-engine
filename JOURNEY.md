# The Goal: Become the Firehose

This project began with a technical challenge: 

**"Can a single consumer-grade machine become the Firehose?"**

The AT Protocol (Bluesky) relies on a "Relay" architecture. PDS (Personal Data Servers) send data to a Relay, and the Relay emits the stream everyone consumes. 

My goal was to bypass that centralization entirely. I didn't just want to consume the stream; I wanted to **be** the stream. I wanted to connect directly to every PDS in the mesh, aggregate the traffic, and prove that "Sovereign Aggregation" is possible on a home PC.

## The Evolution

As I built the "Sovereign Ingester"—handling thousands of concurrent WebSocket connections and processing ~500-8000+ messages per second—the project evolved weirdly. I realized I wasn't just rebuilding the Firehose; I was creating a unique vantage point.

By holding the "Sovereign" stream (direct from PDS) and the "Official" stream (from the Relay) simultaneously, I had built a **Lie Detector**.

The project shifted from pure aggregation to forensic auditing.

## The Findings (Feb 2026)

After significant optimization (moving from naive locks to `DashMap` concurrency and atomic state tracking), the answer is **Yes**.

1.  **Reliability:** The official Relay (`bsky.network`) is incredibly robust. In stress tests utilizing this engine, it demonstrated **0.00% data loss**.
2.  **Latency:** The cost of centralization is time. The Relay consistently trails the direct Mesh connection by approximately **132ms**.
3.  **Efficiency:** It is possible to run this level of surveillance on standard hardware using Rust's async runtime (`tokio`).

## Why This Exists

This codebase is not a product. It is a research instrument. It exists to prove that the network *can* be independently audited by anyone with a decent internet connection. 

It is released under the MIT license because the tools to verify truth should be free.

## The Technical Struggles

This wasn't a clean build. It was a fight against the hardware limits. 

For the full technical post-mortem, including the "Ghost" in the TTY, the 14.7GB Mmap Barrier, and the 10,000 connection concurrency roadblocks, see **[Engineering Challenges](docs/struggles.md)**.


