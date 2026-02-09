use std::str;

#[derive(Debug, Clone)]
pub struct RepoOp {
    pub action: String,
    pub path: String,
    pub cid: Option<Vec<u8>>,
}

#[derive(Debug)]
pub struct CommitEnvelope<'a> {
    pub did: Option<&'a [u8]>,
    pub sequence: Option<u64>,
    pub signature: Option<&'a [u8]>,
    pub t: Option<&'a [u8]>,
    pub op: Option<u64>,
    pub raw: &'a [u8],
    pub blocks: Option<&'a [u8]>,
    pub commit: Option<&'a [u8]>,
    pub cid: Option<&'a [u8]>,
    pub record_cid: Option<&'a [u8]>,
    pub ops: Vec<RepoOp>,
    pub source_type: &'static str,
}

// --- CBOR LOW-LEVEL HELPERS ---

pub fn parse_cbor_len(buf: &[u8], i: usize) -> Option<(usize, usize)> {
    if i >= buf.len() { return None; }
    let addl = buf[i] & 0x1f;
    let mut idx = i + 1;
    let len = match addl {
        n @ 0..=23 => n as usize,
        24 => { let n = *buf.get(idx)? as usize; idx += 1; n },
        25 => { let n = u16::from_be_bytes([*buf.get(idx)?, *buf.get(idx+1)?]) as usize; idx += 2; n },
        26 => { let n = u32::from_be_bytes([*buf.get(idx)?, *buf.get(idx+1)?, *buf.get(idx+2)?, *buf.get(idx+3)?]) as usize; idx += 4; n },
        27 => { let n = u64::from_be_bytes([
            *buf.get(idx)?, *buf.get(idx+1)?, *buf.get(idx+2)?, *buf.get(idx+3)?,
            *buf.get(idx+4)?, *buf.get(idx+5)?, *buf.get(idx+6)?, *buf.get(idx+7)?]) as usize; idx += 8; n },
        _ => return None,
    };
    Some((len, idx))
}

pub fn parse_cbor_uint(buf: &[u8], i: usize) -> Option<(u64, usize)> {
    if i >= buf.len() || (buf[i] >> 5) != 0 { return None; }
    let (val, next) = parse_cbor_len(buf, i)?;
    Some((val as u64, next))
}

pub fn parse_cbor_bytes(buf: &[u8], i: usize) -> Option<(&[u8], usize)> {
    if i >= buf.len() || (buf[i] >> 5) != 2 { return None; }
    let (len, header_end) = parse_cbor_len(buf, i)?;
    let end = header_end + len;
    if end > buf.len() { return None; }
    Some((&buf[header_end..end], end))
}

pub fn parse_cbor_text(buf: &[u8], i: usize) -> Option<(&[u8], usize)> {
    if i >= buf.len() || (buf[i] >> 5) != 3 { return None; }
    let (len, header_end) = parse_cbor_len(buf, i)?;
    let end = header_end + len;
    if end > buf.len() { return None; }
    Some((&buf[header_end..end], end))
}

pub fn parse_cbor_tag(buf: &[u8], i: usize) -> Option<(u64, usize)> {
    if i >= buf.len() || (buf[i] >> 5) != 6 { return None; }
    let (tag, next) = parse_cbor_len(buf, i)?;
    Some((tag as u64, next))
}

pub fn skip_cbor_value(buf: &[u8], i: usize) -> Option<usize> {
    if i >= buf.len() { return None; }
    let head = buf[i];
    let major = head >> 5;
    let addl = head & 0x1f;

    if addl == 31 {
        // Indefinite length
        match major {
            2 | 3 | 4 | 5 => {
                let mut idx = i + 1;
                while idx < buf.len() && buf[idx] != 0xff {
                    idx = skip_cbor_value(buf, idx)?;
                }
                return if idx < buf.len() && buf[idx] == 0xff { Some(idx + 1) } else { None };
            }
            _ => return None,
        }
    }

    match major {
        0 | 1 => parse_cbor_len(buf, i).map(|(_, n)| n),
        2 | 3 => {
            let (len, n) = parse_cbor_len(buf, i)?;
            Some(n + len)
        }
        4 => {
            let (len, mut next) = parse_cbor_len(buf, i)?;
            for _ in 0..len { next = skip_cbor_value(buf, next)?; }
            Some(next)
        }
        5 => {
            let (len, mut next) = parse_cbor_len(buf, i)?;
            for _ in 0..(len * 2) { next = skip_cbor_value(buf, next)?; }
            Some(next)
        }
        6 => {
            let (_, next) = parse_cbor_len(buf, i)?;
            skip_cbor_value(buf, next)
        }
        7 => { Some(i + 1) } // Simple values
        _ => None,
    }
}

// --- VARINT & CAR EXTRACTION ---

fn read_varint(buf: &[u8], mut offset: usize) -> Option<(u64, usize)> {
    let mut value = 0u64;
    let mut shift = 0;
    let start = offset;
    while offset < buf.len() {
        let byte = buf[offset];
        value |= ((byte & 0x7F) as u64) << shift;
        offset += 1;
        if (byte & 0x80) == 0 { return Some((value, offset - start)); }
        shift += 7;
        if shift >= 64 { return None; }
    }
    None
}

fn parse_raw_cid_len(input: &[u8]) -> Option<usize> {
    let mut offset = 0;
    let (ver, n1) = read_varint(input, offset)?;
    if ver != 1 { return None; }
    offset += n1;
    let (_, n2) = read_varint(input, offset)?; // codec
    offset += n2;
    let (_, n3) = read_varint(input, offset)?; // hash type
    offset += n3;
    let (mh_len, n4) = read_varint(input, offset)?; // hash len
    offset += n4;
    Some(offset + (mh_len as usize))
}

fn extract_from_car<'a>(data: &'a [u8], target_cid: Option<&[u8]>) -> Option<&'a [u8]> {
    if data.is_empty() { return None; }
    
    // CAR file starts with a varint-encoded header length, followed by the CBOR header
    let (header_len, v_len) = match read_varint(data, 0) {
        Some(res) => res,
        None => return None,
    };
    let mut offset = v_len + (header_len as usize);

    while offset < data.len() {
        let (total_len, v_len) = match read_varint(data, offset) {
            Some(res) => res,
            None => break,
        };
        offset += v_len;
        let block_start = offset;
        let block_end = block_start + (total_len as usize);
        if block_end > data.len() { break; }

        let cid_len = match parse_raw_cid_len(&data[offset..]) {
            Some(len) => len,
            None => {
                offset = block_end;
                continue;
            }
        };
        let cid_bytes = &data[offset..offset + cid_len];
        let data_start = offset + cid_len;
        let data_len = (total_len as usize).saturating_sub(cid_len);

        match target_cid {
            Some(target) => {
                let clean_target = if target.first() == Some(&0x00) { &target[1..] } else { target };
                if cid_bytes == clean_target {
                    if data_start + data_len <= data.len() {
                        return Some(&data[data_start..data_start + data_len]);
                    }
                }
            }
            None => {
                if data_start + data_len <= data.len() {
                    return Some(&data[data_start..data_start + data_len]);
                }
            }
        }
        offset = block_end;
    }
    None
}

// --- MAIN ENTRY POINT ---

pub fn parse_input<'a>(input: &'a [u8]) -> Option<CommitEnvelope<'a>> {
    if input.is_empty() { return None; }

    let header_end = skip_cbor_value(input, 0)?;
    let is_firehose = header_end < input.len();

    if is_firehose {
        let header = &input[0..header_end];
        let payload = &input[header_end..];
        
        let mut event_t = None;
        let mut op_code = None;

        // Parse Header
        let mut h_off = 0;
        while h_off < header.len() && (header[h_off] >> 5) == 6 {
            let (_, next) = parse_cbor_len(header, h_off).unwrap_or((0, h_off + 1));
            h_off = next;
        }

        if let Some((h_pairs, it_h_off)) = parse_cbor_len(header, h_off) {
            h_off = it_h_off;
            for _ in 0..h_pairs {
                if let Some((key, next_k)) = parse_cbor_text(header, h_off) {
                    h_off = next_k;
                    let key_str = str::from_utf8(key).unwrap_or("");
                    match key_str {
                        "t" => {
                            if let Some((v, n)) = parse_cbor_text(header, h_off) {
                                event_t = Some(v); h_off = n;
                            } else { h_off = skip_cbor_value(header, h_off).unwrap_or(h_off + 1); }
                        }
                        "op" => {
                            if let Some((v, n)) = parse_cbor_uint(header, h_off) {
                                op_code = Some(v); h_off = n;
                            } else { h_off = skip_cbor_value(header, h_off).unwrap_or(h_off + 1); }
                        }
                        _ => h_off = skip_cbor_value(header, h_off).unwrap_or(h_off + 1),
                    }
                } else { break; }
            }
        }

        // Parse Payload
        let mut p_off = 0;
        while p_off < payload.len() && (payload[p_off] >> 5) == 6 {
            let (_, next) = parse_cbor_len(payload, p_off).unwrap_or((0, p_off + 1));
            p_off = next;
        }

        let (pairs, it_p_off) = parse_cbor_len(payload, p_off).unwrap_or((0, p_off));
        p_off = it_p_off;
        let mut did = None;
        let mut seq = None;
        let mut blocks_bytes = None;
        let mut commit_cid = None;
        let mut signature = None;
        let mut ops = Vec::new();

        for _ in 0..pairs {
            if let Some((key, next_k)) = parse_cbor_text(payload, p_off) {
                p_off = next_k;
                let key_str = str::from_utf8(key).unwrap_or("");

                match key_str {
                    "repo" | "did" => {
                        if let Some((v, n)) = parse_cbor_text(payload, p_off).or_else(|| parse_cbor_bytes(payload, p_off)) {
                            did = Some(v); p_off = n;
                        } else { p_off = skip_cbor_value(payload, p_off).unwrap_or(p_off + 1); }
                    }
                    "ops" => {
                        if let Some((op_len, next_op)) = parse_cbor_len(payload, p_off) {
                            p_off = skip_cbor_value(payload, p_off).unwrap_or(next_op);
                            let mut op_idx = next_op;
                            for _ in 0..op_len {
                                if let Some((o_pairs, next_o)) = parse_cbor_len(payload, op_idx) {
                                    op_idx = next_o;
                                    let mut action = String::new();
                                    let mut path = String::new();
                                    let mut op_cid = None;
                                    for _ in 0..o_pairs {
                                        if let Some((k, n_k)) = parse_cbor_text(payload, op_idx) {
                                            op_idx = n_k;
                                            let k_str = str::from_utf8(k).unwrap_or("");
                                            match k_str {
                                                "action" => {
                                                    if let Some((v, n)) = parse_cbor_text(payload, op_idx) {
                                                        action = str::from_utf8(v).unwrap_or("").to_string();
                                                        op_idx = n;
                                                    } else { op_idx = skip_cbor_value(payload, op_idx).unwrap_or(op_idx + 1); }
                                                }
                                                "path" => {
                                                    if let Some((v, n)) = parse_cbor_text(payload, op_idx) {
                                                        path = str::from_utf8(v).unwrap_or("").to_string();
                                                        op_idx = n;
                                                    } else { op_idx = skip_cbor_value(payload, op_idx).unwrap_or(op_idx + 1); }
                                                }
                                                "cid" => {
                                                    let mut it_op_idx = op_idx;
                                                    if payload.get(it_op_idx) == Some(&0xd8) && payload.get(it_op_idx+1) == Some(&0x2a) {
                                                        it_op_idx += 2;
                                                    }
                                                    if let Some((v, n)) = parse_cbor_bytes(payload, it_op_idx) {
                                                        op_cid = Some(v.to_vec());
                                                        op_idx = n;
                                                    } else { op_idx = skip_cbor_value(payload, op_idx).unwrap_or(op_idx + 1); }
                                                }
                                                _ => op_idx = skip_cbor_value(payload, op_idx).unwrap_or(op_idx + 1),
                                            }
                                        } else { break; }
                                    }
                                    ops.push(RepoOp { action, path, cid: op_cid });
                                } else { break; }
                            }
                        } else { p_off = skip_cbor_value(payload, p_off).unwrap_or(p_off + 1); }
                    }
                    "seq" => {
                        if let Some((v, n)) = parse_cbor_uint(payload, p_off) {
                            seq = Some(v); p_off = n;
                        } else { p_off = skip_cbor_value(payload, p_off).unwrap_or(p_off + 1); }
                    }
                    "blocks" => {
                        if let Some((v, n)) = parse_cbor_bytes(payload, p_off) {
                            blocks_bytes = Some(v); p_off = n;
                        } else { p_off = skip_cbor_value(payload, p_off).unwrap_or(p_off + 1); }
                    }
                    "commit" => {
                        // Handle potential tag 42 before the CID bytes
                        let mut it_p_off = p_off;
                        if payload.get(it_p_off) == Some(&0xd8) && payload.get(it_p_off+1) == Some(&0x2a) {
                            it_p_off += 2;
                        }
                        if let Some((v, n)) = parse_cbor_bytes(payload, it_p_off) {
                            commit_cid = Some(v); p_off = n;
                        } else { p_off = skip_cbor_value(payload, p_off).unwrap_or(p_off + 1); }
                    }
                    "sig" => {
                        if let Some((v, n)) = parse_cbor_bytes(payload, p_off) {
                            signature = Some(v); p_off = n;
                        } else {
                            // Try skipping tag if signature is tagged for some reason
                            let mut it_p_off = p_off;
                            if it_p_off < payload.len() && (payload[it_p_off] >> 5) == 6 {
                                let (_, next) = parse_cbor_len(payload, it_p_off).unwrap_or((0, it_p_off + 1));
                                it_p_off = next;
                            }
                            if let Some((v, n)) = parse_cbor_bytes(payload, it_p_off) {
                                signature = Some(v); p_off = n;
                            } else {
                                p_off = skip_cbor_value(payload, p_off).unwrap_or(p_off + 1);
                            }
                        }
                    }
                    _ => p_off = skip_cbor_value(payload, p_off).unwrap_or(p_off + 1),
                }
            } else { break; }
        }

        let extracted = blocks_bytes.and_then(|b| extract_from_car(b, commit_cid));
        // If signature is missing from top-level (standard for firehose), extract it from commit object
        if signature.is_none() {
            if let Some(commit_data) = extracted {
                // The commit block is a CBOR map (a6 ...). We search it for "sig".
                let mut c_off = 0;
                if let Some((c_pairs, next_c)) = parse_cbor_len(commit_data, c_off) {
                    c_off = next_c;
                    for _ in 0..c_pairs {
                        if let Some((k, next_k)) = parse_cbor_text(commit_data, c_off) {
                            c_off = next_k;
                            if k == b"sig" {
                                if let Some((sig_val, _)) = parse_cbor_bytes(commit_data, c_off) {
                                    signature = Some(sig_val);
                                    break;
                                } 
                            }
                            c_off = skip_cbor_value(commit_data, c_off).unwrap_or(c_off + 1);
                        } else { break; }
                    }
                }
            }
        }
        
        Some(CommitEnvelope {
            did, sequence: seq, signature, t: event_t, op: op_code,
            raw: input, blocks: blocks_bytes, commit: extracted,
            cid: commit_cid, record_cid: None, // Will be improved later
            ops,
            source_type: "firehose",
        })
    } else {
        let extracted = extract_from_car(input, None);
        Some(CommitEnvelope {
            did: None, sequence: None, signature: None, t: None, op: None,
            raw: input, blocks: Some(input), commit: extracted,
            cid: None, record_cid: None,
            ops: Vec::new(),
            source_type: "car_file",
        })
    }
}