// High-performance verification logic for ATProto commit blocks
use crate::parser::core::CommitEnvelope;
use k256::ecdsa::signature::hazmat::PrehashVerifier as _;
use sha2::{Digest, Sha256};
use dashmap::DashMap;
use std::sync::OnceLock;

// Global caches for parsed VerifyingKeys to eliminate EC parsing overhead.
// These are keyed by the 33-byte raw SEC1 pubkey.
static SECP_CACHE: OnceLock<DashMap<[u8; 33], k256::ecdsa::VerifyingKey>> = OnceLock::new();
static P256_CACHE: OnceLock<DashMap<[u8; 33], p256::ecdsa::VerifyingKey>> = OnceLock::new();

pub fn verify_commit(envelope: &CommitEnvelope, pubkey_bytes: &[u8; 33], key_type: u8) -> bool {
    let commit_raw = match envelope.commit {
        Some(c) => c,
        None => return false,
    };
    let sig_bytes = envelope.signature.unwrap_or(&[]);
    
    // 1. Hash and Verify (Zero-Copy)
    let mut hasher = Sha256::new();
    if crate::parser::canonical::hash_canonical_commit(commit_raw, &mut hasher) {
        let hash = hasher.finalize();

        match key_type {
            1 => { // Secp256k1
                let cache = SECP_CACHE.get_or_init(|| DashMap::with_capacity(10000));
                
                // Fast Path: Check if the key is already parsed in our cache
                let signature_res = k256::ecdsa::Signature::from_slice(sig_bytes);
                if let Ok(signature) = signature_res {
                    if let Some(vk) = cache.get(pubkey_bytes) {
                        return vk.verify_prehash(&hash, &signature).is_ok();
                    }

                    // Slow Path: Parse and cache it
                    if let Ok(verifying_key) = k256::ecdsa::VerifyingKey::from_sec1_bytes(pubkey_bytes) {
                        let ok = verifying_key.verify_prehash(&hash, &signature).is_ok();
                        // Self-cleaning cache if it grows too large (e.g., > 100k entries)
                        if cache.len() > 100_000 { cache.clear(); }
                        cache.insert(*pubkey_bytes, verifying_key);
                        return ok;
                    }
                }
            },
            2 => { // P-256
                let cache = P256_CACHE.get_or_init(|| DashMap::with_capacity(10000));
                
                let signature_res = p256::ecdsa::Signature::from_slice(sig_bytes);
                if let Ok(signature) = signature_res {
                    if let Some(vk) = cache.get(pubkey_bytes) {
                        return vk.verify_prehash(&hash, &signature).is_ok();
                    }

                    if let Ok(verifying_key) = p256::ecdsa::VerifyingKey::from_sec1_bytes(pubkey_bytes) {
                        let ok = verifying_key.verify_prehash(&hash, &signature).is_ok();
                        if cache.len() > 100_000 { cache.clear(); }
                        cache.insert(*pubkey_bytes, verifying_key);
                        return ok;
                    }
                }
            },
            _ => return false,
        }
    }
    false
}

