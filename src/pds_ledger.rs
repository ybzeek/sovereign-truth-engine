use std::fs::{File, OpenOptions};
use memmap2::MmapMut;
use std::path::Path;
use tracing::{info, warn};

pub const ENTRY_SIZE: usize = 256;
pub const URL_MAX_LEN: usize = 200;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PdsEntry {
    /// Null-terminated UTF-8 URL
    pub url: [u8; URL_MAX_LEN],
    /// Number of consecutive failures
    pub fail_count: u32,
    /// Alignment padding
    pub _pad: u32,
    /// Unix timestamp of last successful message
    pub last_success: u64,
    /// Unix timestamp of last connection attempt
    pub last_attempt: u64,
    /// Unix timestamp until which this node is penalized
    pub penalty_until: u64,
    /// Reserved for future metrics
    pub reserved: [u8; 24],
}

impl PdsEntry {
    pub fn new(url_str: &str) -> Option<Self> {
        if url_str.len() >= URL_MAX_LEN {
            warn!("URL too long ({} chars): {}", url_str.len(), url_str);
            return None;
        }

        // Basic sanity check for "weird" entries
        if url_str.contains('\x1b') || url_str.contains('\x00') {
             warn!("URL contains illegal characters: {}", url_str);
             return None;
        }

        let mut url = [0u8; URL_MAX_LEN];
        url[..url_str.len()].copy_from_slice(url_str.as_bytes());

        Some(Self {
            url,
            fail_count: 0,
            _pad: 0,
            last_success: 0,
            last_attempt: 0,
            penalty_until: 0,
            reserved: [0u8; 24],
        })
    }

    pub fn get_url(&self) -> String {
        let len = self.url.iter().position(|&b| b == 0).unwrap_or(URL_MAX_LEN);
        String::from_utf8_lossy(&self.url[..len]).to_string()
    }
}

pub struct PdsLedger {
    file: File,
    mmap: MmapMut,
    capacity: usize,
}

impl PdsLedger {
    pub fn open_or_create<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let metadata = file.metadata()?;
        let mut len = metadata.len() as usize;

        // Ensure file is at least 1 entry size
        if len == 0 {
            len = ENTRY_SIZE;
            file.set_len(len as u64)?;
        }

        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok(Self { file, mmap, capacity: len / ENTRY_SIZE })
    }

    pub fn entry_count(&self) -> usize {
        // Find the first "empty" entry to determine logical count
        // Or just return capacity if we want to be simple
        // For our use case, we'll scan backwards for the first non-zero URL
        for i in (0..self.capacity).rev() {
            if let Some(entry) = self.get_entry(i) {
                if entry.url[0] != 0 {
                    return i + 1;
                }
            }
        }
        0
    }

    pub fn get_entry(&self, index: usize) -> Option<&PdsEntry> {
        if index >= self.capacity {
            return None;
        }
        let offset = index * ENTRY_SIZE;
        unsafe {
            let ptr = self.mmap.as_ptr().add(offset) as *const PdsEntry;
            Some(&*ptr)
        }
    }

    pub fn get_entry_mut(&mut self, index: usize) -> Option<&mut PdsEntry> {
        if index >= self.capacity {
            return None;
        }
        let offset = index * ENTRY_SIZE;
        unsafe {
            let ptr = self.mmap.as_mut_ptr().add(offset) as *mut PdsEntry;
            Some(&mut *ptr)
        }
    }

    pub fn append(&mut self, entry: &PdsEntry) -> anyhow::Result<usize> {
        let logical_count = self.entry_count();
        
        if logical_count >= self.capacity {
            // GROW: Pre-allocate 40960 entries (approx 1MB) at a time
            let new_capacity = self.capacity + 4096;
            let new_len = new_capacity * ENTRY_SIZE;
            
            self.file.set_len(new_len as u64)?;
            self.mmap = unsafe { MmapMut::map_mut(&self.file)? };
            self.capacity = new_capacity;
            info!("PDS Ledger capacity grown to {} entries.", new_capacity);
        }

        let index = logical_count;
        let entry_mut = self.get_entry_mut(index).unwrap();
        *entry_mut = *entry;
        
        Ok(index)
    }

    /// Flushes changes to disk
    pub fn flush(&self) -> anyhow::Result<()> {
        self.mmap.flush()?;
        Ok(())
    }
}
