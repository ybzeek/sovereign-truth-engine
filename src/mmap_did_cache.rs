impl MmapDidCache {
    /// Atomically insert or update a slot for a DID (valid=1), or tombstone/delete (valid=2).
    /// For tombstone, pass None for key_type/pubkey. Returns true if written, false if not found.
    /// SAFETY: Caller must ensure exclusive access to the mmap for mutation.
    pub fn atomic_update_or_tombstone(&mut self, did: &str, key_type: Option<u8>, pubkey: Option<&[u8;33]>) -> bool {
        use std::sync::atomic::{fence, Ordering};
        let mut hasher = Sha256::new();
        hasher.update(did.as_bytes());
        let did_hash: [u8; 32] = hasher.finalize().into();
        let mmap_mut = self.mmap_mut.as_mut().expect("MmapDidCache must be opened with open_mut() for mutation");
        let mmap_len = mmap_mut.len();
        let mut slot = (fxhash::hash64(&did_hash) % NUM_SLOTS as u64) as usize;
        for _ in 0..NUM_SLOTS {
            let start = slot * SLOT_SIZE;
            let end = start + SLOT_SIZE;
            if end > mmap_len {
                slot = 0;
                continue;
            }
            let entry_bytes = &mut mmap_mut[start..end];
            let entry_did_hash = &entry_bytes[0..32];
            let valid = entry_bytes[98];
            if valid == 0 || entry_did_hash == did_hash {
                // Write all fields except valid
                entry_bytes[0..32].copy_from_slice(&did_hash);
                if let (Some(kt), Some(pk)) = (key_type, pubkey) {
                    entry_bytes[32] = kt;
                    entry_bytes[33..66].copy_from_slice(pk);
                    entry_bytes[66..98].fill(0);
                    // Release fence before setting valid
                    fence(Ordering::Release);
                    entry_bytes[98] = 1; // valid
                } else {
                    // Tombstone: zero key_type/pubkey/reserved
                    entry_bytes[32] = 0;
                    entry_bytes[33..98].fill(0);
                    fence(Ordering::Release);
                    entry_bytes[98] = 2; // tombstone
                }
                return true;
            }
            slot = (slot + 1) % NUM_SLOTS;
        }
        false
    }

    /// Remove a DID from the cache by clearing its slot (valid=0)
    pub fn remove_did(&mut self, did: &str) -> bool {
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(did.as_bytes());
        let did_hash: [u8; 32] = hasher.finalize().into();
        let mmap_mut = self.mmap_mut.as_mut().expect("MmapDidCache must be opened with open_mut() for mutation");
        let mmap_len = mmap_mut.len();
        let mut slot = (fxhash::hash64(&did_hash) % NUM_SLOTS as u64) as usize;
        for _ in 0..NUM_SLOTS {
            let start = slot * SLOT_SIZE;
            let end = start + SLOT_SIZE;
            if end > mmap_len {
                slot = 0;
                continue;
            }
            let entry_bytes = &mut mmap_mut[start..end];
            let entry_did_hash = &entry_bytes[0..32];
            let valid = entry_bytes[98];
            if valid != 0 && entry_did_hash == did_hash {
                // DON'T zero the slot - that breaks linear probing chains!
                // Instead, set valid to 2 (Tombstone).
                entry_bytes[98] = 2; 
                return true;
            }
            slot = (slot + 1) % NUM_SLOTS;
        }
        false
    }
}
use memmap2::{Mmap, MmapMut};

pub struct MmapDidCache {
    mmap: Option<Mmap>,
    mmap_mut: Option<MmapMut>,
}
use fxhash;
use sha2::{Sha256, Digest};
// Slot size: 99 bytes (32 DID hash + 1 key type + 33 pubkey + 32 reserved + 1 valid/version)
const SLOT_SIZE: usize = 99;
const NUM_SLOTS: usize = 150_000_001;

impl MmapDidCache {
    /// Open the cache file for read-only access
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new().read(true).open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(MmapDidCache { mmap: Some(mmap), mmap_mut: None })
    }

    /// Open the cache file for mutable access
    pub fn open_mut<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new().read(true).write(true).open(path)?;
        let mmap_mut = unsafe { MmapMut::map_mut(&file)? };
        Ok(MmapDidCache { mmap: None, mmap_mut: Some(mmap_mut) })
    }
    /// Linear probing hash map lookup, matching plc_file_enricher.rs
    pub fn get(&self, did: &str) -> Option<([u8; 33], u8)> {
        // 1. Hash the DID to get a 32-byte did_hash
        let mut hasher = Sha256::new();
        hasher.update(did.as_bytes());
        let did_hash: [u8; 32] = hasher.finalize().into();

        // 2. Access the data (from either read-only or mutable mmap)
        let mmap_data: &[u8] = if let Some(m) = self.mmap.as_ref() {
            m
        } else if let Some(m) = self.mmap_mut.as_ref() {
            m
        } else {
            panic!("MmapDidCache must be opened before use");
        };

        let mmap_len = mmap_data.len();
        let mut slot = (fxhash::hash64(&did_hash) % NUM_SLOTS as u64) as usize;

        // 3. Linear probe
        for _ in 0..NUM_SLOTS {
            let start = slot * SLOT_SIZE;
            let end = start + SLOT_SIZE;
            if end > mmap_len {
                slot = 0;
                continue;
            }
            let entry_bytes = &mmap_data[start..end];
            let entry_did_hash = &entry_bytes[0..32];
            let key_type = entry_bytes[32];
            let valid = entry_bytes[98]; // last byte
            match valid {
                0 => return None, // Empty slot: stop probing
                1 => {
                    if entry_did_hash == did_hash {
                        // Hit: return the pubkey + key type
                        let mut pubkey = [0u8; 33];
                        pubkey.copy_from_slice(&entry_bytes[33..66]);
                        return Some((pubkey, key_type));
                    }
                }
                2 => {
                    // Tombstone/deleted: skip, keep probing
                }
                _ => {
                    // Future: versioned slot, treat as valid for now if did_hash matches
                    if entry_did_hash == did_hash {
                        let mut pubkey = [0u8; 33];
                        pubkey.copy_from_slice(&entry_bytes[33..66]);
                        return Some((pubkey, key_type));
                    }
                }
            }
            slot = (slot + 1) % NUM_SLOTS;
        }
        None
    }
}
