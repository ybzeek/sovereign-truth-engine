pub mod car;
pub mod visualize;
pub mod builder;

use libipld::Cid;
use crate::parser::core::{parse_cbor_len, parse_cbor_text, parse_cbor_bytes, parse_cbor_tag, skip_cbor_value};

#[derive(Debug)]
pub struct MstEntry {
    pub prefix_len: u64,
    pub key_suffix: Vec<u8>,
    pub value: Cid,
    pub tree: Option<Cid>,
}

#[derive(Debug)]
pub struct MstNode {
    pub left: Option<Cid>,
    pub entries: Vec<MstEntry>,
}

/// Helper to parse a CID from DAG-CBOR bytes, handling optional Tag 42 
/// and the mandatory 0x00 prefix byte for binary CIDs in Tag 42.
fn parse_cbor_cid(data: &[u8], mut off: usize) -> Option<(Cid, usize)> {
    if off >= data.len() { return None; }
    
    // Check for Tag 42
    if let Some((tag, next_t)) = parse_cbor_tag(data, off) {
        if tag == 42 { off = next_t; }
    }
    
    // CID bytes are Major Type 2 (Byte String)
    if let Some((cid_bytes, next_off)) = parse_cbor_bytes(data, off) {
        if cid_bytes.is_empty() { return None; }
        // DAG-CBOR Tag 42 values are prefixed with a 0x00 multibase byte
        let target = if cid_bytes[0] == 0x00 { &cid_bytes[1..] } else { cid_bytes };
        if let Ok(cid) = Cid::read_bytes(target) {
            return Some((cid, next_off));
        }
    }
    None
}

impl MstNode {
    /// Manual parsing of an MST Node from DAG-CBOR bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let mut off = 0;
        let mut left = None;
        let mut entries = Vec::new();

        // Skip any top-level tags
        while off < data.len() && (data[off] >> 5) == 6 {
            if let Some((_, next)) = parse_cbor_len(data, off) { off = next; } else { break; }
        }

        if off >= data.len() || (data[off] >> 5) != 5 {
            return Err("Expected CBOR Map for MST Node".into());
        }

        if let Some((n_pairs, next_off)) = parse_cbor_len(data, off) {
            off = next_off;
            for _ in 0..n_pairs {
                if let Some((key, next_k)) = parse_cbor_text(data, off) {
                    off = next_k;
                    let val_start = off;
                    match key {
                        b"l" => {
                            if let Some((cid, _)) = parse_cbor_cid(data, off) {
                                left = Some(cid);
                            }
                        }
                        b"e" => {
                            if let Some((n_entries, next_e)) = parse_cbor_len(data, off) {
                                let mut e_off = next_e;
                                for _ in 0..n_entries {
                                    if let Some((n_fields, it_f_off)) = parse_cbor_len(data, e_off) {
                                        let mut f_off = it_f_off;
                                        let mut prefix_len = 0;
                                        let mut key_suffix = Vec::new();
                                        let mut value = None;
                                        let mut tree = None;
                                        for _ in 0..n_fields {
                                            if let Some((f_name, next_fn)) = parse_cbor_text(data, f_off) {
                                                f_off = next_fn;
                                                let entry_val_start = f_off;
                                                match f_name {
                                                    b"p" => { if let Some((v, _)) = parse_cbor_len(data, f_off) { prefix_len = v as u64; } }
                                                    b"k" => {
                                                        if let Some((v, _)) = parse_cbor_bytes(data, f_off).or_else(|| parse_cbor_text(data, f_off)) {
                                                            key_suffix = v.to_vec();
                                                        }
                                                    }
                                                    b"v" => {
                                                        if let Some((cid, _)) = parse_cbor_cid(data, f_off) {
                                                            value = Some(cid);
                                                        }
                                                    }
                                                    b"t" => {
                                                        if let Some((cid, _)) = parse_cbor_cid(data, f_off) {
                                                            tree = Some(cid);
                                                        }
                                                    }
                                                    _ => {}
                                                }
                                                f_off = skip_cbor_value(data, entry_val_start).unwrap_or(f_off);
                                            }
                                        }
                                        if let Some(v) = value {
                                            entries.push(MstEntry { prefix_len, key_suffix, value: v, tree });
                                        }
                                        e_off = f_off;
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                    off = skip_cbor_value(data, val_start).unwrap_or(off);
                }
            }
        }

        Ok(MstNode { left, entries })
    }

    pub fn get_root_from_commit(data: &[u8]) -> Option<Cid> {
        if data.is_empty() { return None; }
        let mut off = 0;
        
        // Skip tags
        while off < data.len() && (data[off] >> 5) == 6 {
            if let Some((_, next)) = parse_cbor_len(data, off) { off = next; } else { break; }
        }

        if off >= data.len() || (data[off] >> 5) != 5 {
             // Not a map? 
             return None;
        }
        
        if let Some((n_pairs, next_off)) = parse_cbor_len(data, off) {
            off = next_off;
            for _ in 0..n_pairs {
                if let Some((key, next_k)) = parse_cbor_text(data, off) {
                    off = next_k;
                    let val_start = off;
                    if key == b"data" || key == b"root" {
                        if let Some((cid, _)) = parse_cbor_cid(data, off) {
                            return Some(cid);
                        } else {
                            println!("  [Debug] Found 'data' key but failed CID parse at offset {}. Bytes: {:02x?}", off, &data[off..std::cmp::min(off+10, data.len())]);
                        }
                    }
                    off = skip_cbor_value(data, val_start).unwrap_or(off + 1);
                } else {
                    off = skip_cbor_value(data, off).unwrap_or(off+1);
                }
            }
        }
        None
    }

    /// Recursively walks the tree and prints all keys found.
    pub fn walk_and_collect_keys(&self, store: &car::CarStore) {
        if let Some(left_cid) = self.left {
            let cid_bytes = left_cid.to_bytes();
            if let Some(block_data) = store.get_block(&cid_bytes) {
                if let Ok(child_node) = Self::from_bytes(block_data) {
                    child_node.walk_and_collect_keys(store);
                }
            }
        }

        for entry in &self.entries {
            let key_suffix_str = String::from_utf8_lossy(&entry.key_suffix);
            println!("  [MST Record] PrefixLen: {} | Key: {}", entry.prefix_len, key_suffix_str);

            if let Some(right_cid) = entry.tree {
                let cid_bytes = right_cid.to_bytes();
                if let Some(block_data) = store.get_block(&cid_bytes) {
                    if let Ok(child_node) = Self::from_bytes(block_data) {
                        child_node.walk_and_collect_keys(store);
                    }
                }
            }
        }
    }
}
