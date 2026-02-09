/// Parse a Dag-CBOR commit block and extract fields according to ATProto spec.
pub fn parse_commit_block(bytes: &[u8]) -> ParsedCommit {
    let mut parsed = ParsedCommit::new();
    if bytes.is_empty() { return parsed; }
    // Recursive-lite: descend if top-level is tag or bytestring
    let major = bytes[0] >> 5;
    if major == 2 {
        // Embedded bytestring: treat as new CBOR stream
        let (len, hdr) = parse_cbor_bytestring_len(bytes).unwrap_or((0, 0));
        if hdr + len > bytes.len() {
            // eprintln!("[commit parser] Embedded bytestring out of bounds");
            return parsed;
        }
        return parse_commit_block(&bytes[hdr..hdr+len]);
    } else if major == 6 {
        // Tag: parse tag number, then descend
        let (_tag, taglen) = parse_cbor_tag(bytes).unwrap_or((0, 1));
        if taglen >= bytes.len() {
            // eprintln!("[commit parser] Tag header out of bounds");
            return parsed;
        }
        return parse_commit_block(&bytes[taglen..]);
    } else if major != 5 {
        // eprintln!("[commit parser] Not a CBOR map: major type {}", major);
        return parsed;
    }
    // Parse map length (robust, all CBOR definite-length widths)
    let (map_len, hdr) = parse_cbor_map_len(bytes).unwrap_or((0, 1));
    let mut i = hdr;
    if map_len == u64::MAX {
        // eprintln!("[commit parser] Indefinite length map not supported");
        return parsed;
    }
    // Scan all map entries for required fields
    for _ in 0..map_len {
        if i >= bytes.len() { break; }
        // Parse key (must be text)
        let (key, keylen) = parse_cbor_text_key(bytes, i);
        if key.is_none() {
            // eprintln!("[commit parser] Non-string key at {}", i);
            break;
        }
        let key = key.unwrap();
        let key_str = std::str::from_utf8(key).unwrap_or("");
        i = keylen;
        // Parse value by key
        if key_str == "rev" || key_str == "did" {
            let (val, next) = parse_cbor_text(bytes, i).unwrap_or((&[], i));
            if !val.is_empty() {
                let s = String::from_utf8_lossy(val).into_owned();
                if key_str == "rev" { parsed.rev = Some(s); }
                else { parsed.did = Some(s); }
            }
            i = next;
        } else if key_str == "sig" {
            let (val, next) = parse_cbor_bytes(bytes, i).unwrap_or((&[], i));
            if !val.is_empty() { parsed.sig = Some(val.to_vec()); }
            i = next;
        } else if key_str == "data" || key_str == "prev" {
            // Tag 42 (CID)
            let (tag, taglen) = parse_cbor_tag(bytes.get(i..).unwrap_or(&[])).unwrap_or((0, 1));
            i += taglen;
            if tag == 42 {
                let (val, next) = parse_cbor_bytes(bytes, i).unwrap_or((&[], i));
                if !val.is_empty() {
                    if key_str == "data" { parsed.data = Some(val.to_vec()); }
                    else { parsed.prev = Some(Some(val.to_vec())); }
                }
                i = next;
            } else if key_str == "prev" && bytes.get(i) == Some(&0xf6) {
                // Null
                parsed.prev = Some(None);
                i += 1;
            } else {
                // eprintln!("[commit parser] {}: missing tag 42 or null", key_str);
            }
        } else if key_str == "version" {
            let (val, next) = parse_cbor_uint(bytes, i).unwrap_or((0, i));
            parsed.version = Some(val);
            i = next;
        } else {
            // Unknown key, skip value
            // eprintln!("[commit parser] Unknown key: {}", key_str);
            i = skip_cbor_value(bytes, i).unwrap_or(i+1);
        // Minimal CBOR value skipper for unknown values
        fn skip_cbor_value(buf: &[u8], i: usize) -> Option<usize> {
            if i >= buf.len() { return None; }
            let major = buf[i] >> 5;
            let addl = buf[i] & 0x1f;
            let len: usize;
            let mut hdr = 1;
            match addl {
                n @ 0..=23 => { len = n as usize; },
                24 => { if i+1 < buf.len() { len = buf[i+1] as usize; hdr = 2; } else { return None; } },
                25 => { if i+2 < buf.len() { len = ((buf[i+1] as usize) << 8) | buf[i+2] as usize; hdr = 3; } else { return None; } },
                26 => { if i+4 < buf.len() { len = ((buf[i+1] as usize) << 24) | ((buf[i+2] as usize) << 16) | ((buf[i+3] as usize) << 8) | buf[i+4] as usize; hdr = 5; } else { return None; } },
                27 => { if i+8 < buf.len() {
                    let mut l = 0usize;
                    for j in 1..=8 { l = (l << 8) | buf[i+j] as usize; }
                    len = l;
                    hdr = 9;
                } else { return None; } },
                31 => { return None; }, // Indefinite not supported
                _ => { return None; },
            }
            match major {
                0 | 1 | 7 => Some(i+hdr), // int, negint, simple
                2 | 3 => Some(i+hdr+len), // bytes, text
                4 => { // array
                    let mut idx = i+hdr;
                    for _ in 0..len { idx = skip_cbor_value(buf, idx)?; }
                    Some(idx)
                },
                5 => { // map
                    let mut idx = i+hdr;
                    for _ in 0..len { idx = skip_cbor_value(buf, idx)?; idx = skip_cbor_value(buf, idx)?; }
                    Some(idx)
                },
                6 => skip_cbor_value(buf, i+hdr), // tag, skip tag then value
                _ => None,
            }
        }
        }
    }
    parsed
}

// --- CBOR helpers for robust parsing ---
fn parse_cbor_bytestring_len(buf: &[u8]) -> Option<(usize, usize)> {
    if buf.is_empty() { return None; }
    let addl = buf[0] & 0x1f;
    match addl {
        n @ 0..=23 => Some((n as usize, 1)),
        24 => if buf.len() > 1 { Some((buf[1] as usize, 2)) } else { None },
        25 => if buf.len() > 2 { Some(((buf[1] as usize) << 8 | buf[2] as usize, 3)) } else { None },
        26 => if buf.len() > 4 { Some(((buf[1] as usize) << 24 | (buf[2] as usize) << 16 | (buf[3] as usize) << 8 | buf[4] as usize, 5)) } else { None },
        27 => if buf.len() > 8 {
            let mut n = 0usize;
            for j in 1..=8 { n = (n << 8) | buf[j] as usize; }
            Some((n, 9))
        } else { None },
        _ => None,
    }
}

fn parse_cbor_map_len(buf: &[u8]) -> Option<(u64, usize)> {
    if buf.is_empty() { return None; }
    let addl = buf[0] & 0x1f;
    match addl {
        n @ 0..=23 => Some((n as u64, 1)),
        24 => if buf.len() > 1 { Some((buf[1] as u64, 2)) } else { None },
        25 => if buf.len() > 2 { Some(((buf[1] as u64) << 8 | buf[2] as u64, 3)) } else { None },
        26 => if buf.len() > 4 { Some(((buf[1] as u64) << 24 | (buf[2] as u64) << 16 | (buf[3] as u64) << 8 | buf[4] as u64, 5)) } else { None },
        27 => if buf.len() > 8 {
            let mut n = 0u64;
            for j in 1..=8 { n = (n << 8) | buf[j] as u64; }
            Some((n, 9))
        } else { None },
        31 => Some((u64::MAX, 1)), // Indefinite
        _ => None,
    }
}

fn parse_cbor_tag(buf: &[u8]) -> Option<(u64, usize)> {
    if buf.is_empty() { return None; }
    if buf[0] >> 5 != 6 { return None; }
    let addl = buf[0] & 0x1f;
    match addl {
        n @ 0..=23 => Some((n as u64, 1)),
        24 => if buf.len() > 1 { Some((buf[1] as u64, 2)) } else { None },
        25 => if buf.len() > 2 { Some(((buf[1] as u64) << 8 | buf[2] as u64, 3)) } else { None },
        26 => if buf.len() > 4 { Some(((buf[1] as u64) << 24 | (buf[2] as u64) << 16 | (buf[3] as u64) << 8 | buf[4] as u64, 5)) } else { None },
        27 => if buf.len() > 8 {
            let mut n = 0u64;
            for j in 1..=8 { n = (n << 8) | buf[j] as u64; }
            Some((n, 9))
        } else { None },
        _ => None,
    }
}

fn parse_cbor_text_key<'a>(buf: &'a [u8], i: usize) -> (Option<&'a [u8]>, usize) {
    if i >= buf.len() { return (None, i); }
    let major = buf[i] >> 5;
    if major != 3 { return (None, i+1); }
    let addl = buf[i] & 0x1f;
    let mut len = addl as usize;
    let mut hdr = 1;
    match addl {
        0..=23 => {},
        24 => { if i+1 < buf.len() { len = buf[i+1] as usize; hdr = 2; } else { return (None, i+1); } },
        25 => { if i+2 < buf.len() { len = ((buf[i+1] as usize) << 8) | buf[i+2] as usize; hdr = 3; } else { return (None, i+2); } },
        26 => { if i+4 < buf.len() { len = ((buf[i+1] as usize) << 24) | ((buf[i+2] as usize) << 16) | ((buf[i+3] as usize) << 8) | buf[i+4] as usize; hdr = 5; } else { return (None, i+4); } },
        27 => { if i+8 < buf.len() {
            len = 0;
            for j in 1..=8 { len = (len << 8) | buf[i+j] as usize; }
            hdr = 9;
        } else { return (None, i+8); } },
        _ => return (None, i+1),
    }
    if i+hdr+len > buf.len() { return (None, i+hdr+len); }
    (Some(&buf[i+hdr..i+hdr+len]), i+hdr+len)
}

fn parse_cbor_text<'a>(buf: &'a [u8], i: usize) -> Option<(&'a [u8], usize)> {
    if i >= buf.len() { return None; }
    let major = buf[i] >> 5;
    if major != 3 { return None; }
    let addl = buf[i] & 0x1f;
    let mut len = addl as usize;
    let mut hdr = 1;
    match addl {
        0..=23 => {},
        24 => { if i+1 < buf.len() { len = buf[i+1] as usize; hdr = 2; } else { return None; } },
        25 => { if i+2 < buf.len() { len = ((buf[i+1] as usize) << 8) | buf[i+2] as usize; hdr = 3; } else { return None; } },
        26 => { if i+4 < buf.len() { len = ((buf[i+1] as usize) << 24) | ((buf[i+2] as usize) << 16) | ((buf[i+3] as usize) << 8) | buf[i+4] as usize; hdr = 5; } else { return None; } },
        27 => { if i+8 < buf.len() {
            len = 0;
            for j in 1..=8 { len = (len << 8) | buf[i+j] as usize; }
            hdr = 9;
        } else { return None; } },
        _ => return None,
    }
    if i+hdr+len > buf.len() { return None; }
    Some((&buf[i+hdr..i+hdr+len], i+hdr+len))
}

fn parse_cbor_bytes<'a>(buf: &'a [u8], i: usize) -> Option<(&'a [u8], usize)> {
    if i >= buf.len() { return None; }
    let major = buf[i] >> 5;
    if major != 2 { return None; }
    let addl = buf[i] & 0x1f;
    let mut len = addl as usize;
    let mut hdr = 1;
    match addl {
        0..=23 => {},
        24 => { if i+1 < buf.len() { len = buf[i+1] as usize; hdr = 2; } else { return None; } },
        25 => { if i+2 < buf.len() { len = ((buf[i+1] as usize) << 8) | buf[i+2] as usize; hdr = 3; } else { return None; } },
        26 => { if i+4 < buf.len() { len = ((buf[i+1] as usize) << 24) | ((buf[i+2] as usize) << 16) | ((buf[i+3] as usize) << 8) | buf[i+4] as usize; hdr = 5; } else { return None; } },
        27 => { if i+8 < buf.len() {
            len = 0;
            for j in 1..=8 { len = (len << 8) | buf[i+j] as usize; }
            hdr = 9;
        } else { return None; } },
        _ => return None,
    }
    if i+hdr+len > buf.len() { return None; }
    Some((&buf[i+hdr..i+hdr+len], i+hdr+len))
}

fn parse_cbor_uint(buf: &[u8], i: usize) -> Option<(u64, usize)> {
    if i >= buf.len() { return None; }
    let major = buf[i] >> 5;
    if major != 0 { return None; }
    let addl = buf[i] & 0x1f;
    let mut val = addl as u64;
    let mut hdr = 1;
    match addl {
        0..=23 => {},
        24 => { if i+1 < buf.len() { val = buf[i+1] as u64; hdr = 2; } else { return None; } },
        25 => { if i+2 < buf.len() { val = ((buf[i+1] as u64) << 8) | buf[i+2] as u64; hdr = 3; } else { return None; } },
        26 => { if i+4 < buf.len() { val = ((buf[i+1] as u64) << 24) | ((buf[i+2] as u64) << 16) | ((buf[i+3] as u64) << 8) | buf[i+4] as u64; hdr = 5; } else { return None; } },
        27 => { if i+8 < buf.len() {
            val = 0;
            for j in 1..=8 { val = (val << 8) | buf[i+j] as u64; }
            hdr = 9;
        } else { return None; } },
        _ => return None,
    }
    Some((val, i+hdr))
}
// Struct for binary mmap cache entry, matching plc_file_enricher.rs

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct CacheEntry {
    pub did_hash: [u8; 32],
    pub key_type: u8,
    pub pubkey: [u8; 33],
    pub reserved: [u8; 32],
    pub valid: u8, // 0 = empty, 1 = valid, (optionally: 2 = deleted, >1 = version)
}

impl CacheEntry {
    pub fn pubkey_bytes(&self) -> &[u8] {
        &self.pubkey
    }
    pub fn is_valid(&self) -> bool {
        self.valid == 1
    }
}

#[derive(Debug)]
pub struct ParsedCommit {
    pub rev: Option<String>,
    pub did: Option<String>,
    pub sig: Option<Vec<u8>>,
    pub data: Option<Vec<u8>>, // CID bytes
    pub prev: Option<Option<Vec<u8>>>, // Some(CID bytes) or None/null
    pub version: Option<u64>,
}

impl ParsedCommit {
    pub fn new() -> Self {
        ParsedCommit {
            rev: None,
            did: None,
            sig: None,
            data: None,
            prev: None,
            version: None,
        }
    }
}
