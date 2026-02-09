//! did_mmap_cache: High-performance mmap-based DID→pubkey cache
//!
//! # Usage
//! See README.md for details and examples.

pub mod mmap_did_cache;
pub mod mmap_cache_entry;
pub mod resolver;
pub mod parser {
	pub mod core;
	pub mod canonical;
}
pub mod verify;
pub mod mst;
pub mod archive;
pub mod pds_ledger;
pub mod monitor;
