use fxhash::FxHashMap;

/// A lightweight, zero-copy CAR file indexer.
/// Stores references to blocks within the raw buffer, indexed by their raw CID bytes.
pub struct CarStore<'a> {
    pub blocks: FxHashMap<&'a [u8], &'a [u8]>,
}

impl<'a> CarStore<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        let mut blocks = FxHashMap::default();
        if data.is_empty() {
            return Self { blocks };
        }

        // CAR file header
        let (header_len, v_len) = match read_varint(data, 0) {
            Some(res) => res,
            None => return Self { blocks },
        };
        let mut offset = (v_len as usize) + (header_len as usize);

        // Iterate through blocks
        while offset < data.len() {
            let (total_len, v_len) = match read_varint(data, offset) {
                Some(res) => res,
                None => break,
            };
            offset += v_len;
            let block_start = offset;
            let block_end = block_start + (total_len as usize);
            if block_end > data.len() {
                break;
            }

            // Inside each block: [CID][Data]
            if let Some(cid_len) = parse_raw_cid_len(&data[block_start..block_end]) {
                let cid_bytes = &data[block_start..block_start + cid_len];
                let block_data = &data[block_start + cid_len..block_end];
                
                // Index by the raw CID
                blocks.insert(cid_bytes, block_data);
            }

            offset = block_end;
        }

        Self { blocks }
    }

    pub fn get_block(&self, cid: &[u8]) -> Option<&'a [u8]> {
        // Some CIDs are prefixed with 0x00 (multibase identity) in some ATProto contexts
        let clean_cid = if cid.first() == Some(&0x00) { &cid[1..] } else { cid };
        
        if let Some(block) = self.blocks.get(clean_cid) {
            return Some(block);
        }

        // Second pass: ATProto CIDs in CAR files are often raw binary. 
        // If the lookup failed, maybe the search key is slightly different (v0 vs v1).
        // For now, let's try matching the suffix if the CID is long.
        if clean_cid.len() > 30 {
            for (key, block) in &self.blocks {
                if key.ends_with(&clean_cid[clean_cid.len()-31..]) {
                    return Some(block);
                }
            }
        }

        None
    }
}

// Internal helpers mirrored from core.rs for standalone modularity
fn read_varint(buf: &[u8], mut offset: usize) -> Option<(u64, usize)> {
    let mut value = 0u64;
    let mut shift = 0;
    let start = offset;
    while offset < buf.len() {
        let byte = buf[offset];
        value |= ((byte & 0x7F) as u64) << shift;
        offset += 1;
        if (byte & 0x80) == 0 {
            return Some((value, offset - start));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    None
}

fn parse_raw_cid_len(input: &[u8]) -> Option<usize> {
    let mut offset = 0;
    let (ver, n1) = read_varint(input, offset)?;
    if ver != 1 {
        return None;
    }
    offset += n1;
    let (_, n2) = read_varint(input, offset)?; // codec
    offset += n2;
    let (_, n3) = read_varint(input, offset)?; // hash type
    offset += n3;
    let (mh_len, n4) = read_varint(input, offset)?; // hash len
    offset += n4;
    Some(offset + (mh_len as usize))
}
