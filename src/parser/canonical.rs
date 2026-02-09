// Canonicalizer for ATProto commit blocks: strips "sig" and sorts keys (DAG-CBOR)
// Uses only manual CBOR helpers for parsing and encoding
use crate::parser::core::{parse_cbor_len, skip_cbor_value};

use sha2::{Sha256, Digest};

fn is_sig_key(key: &[u8]) -> bool {
    key == b"sig"
}

// Minimal CBOR key slice collector (includes header + key bytes)
fn get_cbor_key_slice(buf: &[u8], i: usize) -> Option<(&[u8], &[u8], usize)> {
    if i >= buf.len() { return None; }
    let start = i;
    let (len, next) = parse_cbor_len(buf, i)?;
    if next + len > buf.len() { return None; }
    let key_bytes = &buf[next..next+len];
    Some((key_bytes, &buf[start..next+len], next+len))
}

pub fn hash_canonical_commit(raw: &[u8], hasher: &mut Sha256) -> bool {
    if raw.is_empty() { return false; }
    
    let mut i = 0;
    while i < raw.len() && (raw[i] >> 5) == 6 {
        let (_, next) = match parse_cbor_len(raw, i) { Some(res) => res, None => return false };
        i = next;
    }
    
    if i >= raw.len() { return false; }
    let first_byte = raw[i];
    
    // Efficiency: Use a stack-allocated buffer to avoids heap allocations for keys.
    // Bluesky commits usually have 3-7 keys. 16 is plenty.
    let mut entries_buf = [(&[][..], &[][..], &[][..]); 16];
    let mut entry_count = 0;
    let mut idx = i;

    if first_byte == 0xbf {
        // Indefinite length map
        idx += 1;
        while idx < raw.len() && raw[idx] != 0xff {
            let (key_bytes, key_slice, next_idx) = match get_cbor_key_slice(raw, idx) { Some(res) => res, None => return false };
            idx = next_idx;
            let val_start = idx;
            idx = match skip_cbor_value(raw, idx) { Some(next) => next, None => return false };
            if !is_sig_key(key_bytes) {
                if entry_count >= 16 { return false; }
                entries_buf[entry_count] = (key_bytes, key_slice, &raw[val_start..idx]);
                entry_count += 1;
            }
        }
    } else {
        // Definite length map
        let (map_len, next) = match parse_cbor_len(raw, idx) { Some(res) => res, None => return false };
        idx = next;
        for _ in 0..map_len {
            let (key_bytes, key_slice, next_idx) = match get_cbor_key_slice(raw, idx) { Some(res) => res, None => return false };
            idx = next_idx;
            let val_start = idx;
            idx = match skip_cbor_value(raw, idx) { Some(next) => next, None => return false };
            if !is_sig_key(key_bytes) {
                if entry_count >= 16 { return false; }
                entries_buf[entry_count] = (key_bytes, key_slice, &raw[val_start..idx]);
                entry_count += 1;
            }
        }
    }

    if entry_count == 0 { return false; }
    let entries = &mut entries_buf[..entry_count];

    // Sort keys according to DAG-CBOR (length first, then bytes)
    entries.sort_by(|a, b| {
        let la = a.0.len();
        let lb = b.0.len();
        la.cmp(&lb).then_with(|| a.0.cmp(b.0))
    });

    // Hash the reconstructed map header
    if entry_count < 24 {
        hasher.update(&[0xa0 | (entry_count as u8)]);
    } else if entry_count < 256 {
        hasher.update(&[0xb8, entry_count as u8]);
    } else {
        // Handle > 255 entries if needed
        hasher.update(&[0xb9]);
        hasher.update(&(entry_count as u16).to_be_bytes());
    }

    // Hash each key and value slice directly from the original buffer
    for (_, k_slice, v_slice) in entries.iter() {
        hasher.update(k_slice);
        hasher.update(v_slice);
    }

    true
}

pub fn prepare_canonical_commit(raw: &[u8]) -> Option<Vec<u8>> {
    if raw.is_empty() { return None; }
    
    let mut i = 0;
    while i < raw.len() && (raw[i] >> 5) == 6 {
        let (_, next) = match parse_cbor_len(raw, i) { Some(res) => res, None => return None };
        i = next;
    }
    
    if i >= raw.len() { return None; }
    let first_byte = raw[i];
    
    let mut entries = Vec::new();
    let mut idx = i;

    if first_byte == 0xbf {
        idx += 1;
        while idx < raw.len() && raw[idx] != 0xff {
            let (key_bytes, key_slice, next_idx) = get_cbor_key_slice(raw, idx)?;
            idx = next_idx;
            let val_start = idx;
            idx = skip_cbor_value(raw, idx)?;
            if !is_sig_key(key_bytes) {
                entries.push((key_bytes, key_slice, &raw[val_start..idx]));
            }
        }
    } else {
        let (map_len, next) = parse_cbor_len(raw, idx)?;
        idx = next;
        for _ in 0..map_len {
            let (key_bytes, key_slice, next_idx) = get_cbor_key_slice(raw, idx)?;
            idx = next_idx;
            let val_start = idx;
            idx = skip_cbor_value(raw, idx)?;
            if !is_sig_key(key_bytes) {
                entries.push((key_bytes, key_slice, &raw[val_start..idx]));
            }
        }
    }

    if entries.is_empty() { return None; }
    entries.sort_by(|a, b| {
        let la = a.0.len();
        let lb = b.0.len();
        la.cmp(&lb).then_with(|| a.0.cmp(b.0))
    });

    let mut out = Vec::new();
    let entry_count = entries.len();
    if entry_count < 24 {
        out.push(0xa0 | (entry_count as u8));
    } else {
        out.push(0xb8);
        out.push(entry_count as u8);
    }
    for (_, k_slice, v_slice) in entries {
        out.extend_from_slice(k_slice);
        out.extend_from_slice(v_slice);
    }
    Some(out)
}
