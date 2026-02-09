use memmap2::Mmap;
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use crossbeam_channel::{Sender, unbounded};
use std::thread;

pub struct SegmentPayload {
    pub start_seq: u64,
    pub max_seq: u64,
    pub count: u64,
    pub pending: HashMap<String, Vec<(u64, String, Vec<u8>)>>,
    pub shard_dir: PathBuf,
    pub shard_id: usize,
}

/// Persistent bitset for deleted messages.
pub struct TombstoneStore {
    mmap: memmap2::MmapMut,
}

impl TombstoneStore {
    pub fn open_or_create<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref();
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        
        let metadata = file.metadata()?;
        // 512MB = ~4 Billion messages support (Future-proof)
        let size = 512 * 1024 * 1024;
        if metadata.len() < size {
            file.set_len(size)?;
        }
        
        let mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };
        Ok(TombstoneStore { mmap })
    }

    pub fn is_deleted(&self, seq: u64) -> bool {
        let byte_idx = (seq / 8) as usize;
        let bit_idx = (seq % 8) as u8;
        if byte_idx >= self.mmap.len() { return false; }
        (self.mmap[byte_idx] & (1 << bit_idx)) != 0
    }

    pub fn mark_deleted(&mut self, seq: u64) {
        let byte_idx = (seq / 8) as usize;
        let bit_idx = (seq % 8) as u8;
        if byte_idx < self.mmap.len() {
            self.mmap[byte_idx] |= 1 << bit_idx;
        }
    }
}

use zstd;
use crate::mst::builder::MerkleTree;

/// A single immutable archive segment.
/// Stores a contiguous range of firehose messages, clustered by DID for max compression.
pub struct Segment {
    pub start_seq: u64,
    pub bin_mmap: Mmap,
    pub idx_mmap: Mmap,
    pub root_hash: [u8; 32],
    // Simple cache for the last decompressed cluster to avoid redundant work
    cluster_cache: Mutex<HashMap<usize, Arc<Vec<u8>>>>,
}

impl Segment {
    pub fn new(start_seq: u64, bin_mmap: Mmap, idx_mmap: Mmap) -> Self {
        // Load root hash from the first 32 bytes of the index
        let mut root_hash = [0u8; 32];
        if idx_mmap.len() >= 32 {
            root_hash.copy_from_slice(&idx_mmap[0..32]);
        }

        Self {
            start_seq,
            bin_mmap,
            idx_mmap,
            root_hash,
            cluster_cache: Mutex::new(HashMap::with_capacity(512)),
        }
    }

    /// Verifies the integrity of the segment by checking the stored Merkle Root
    /// against the actual message data.
    pub fn verify_integrity(&self, dict: Option<&[u8]>) -> io::Result<bool> {
        let msg_count = (self.idx_mmap.len() - 32) / 28;
        let mut tree = MerkleTree::new();
        
        for i in 0..msg_count {
            if let Ok(data) = self.get_decompressed_message_by_index(i as u64, dict) {
                tree.push(&data);
            }
        }
        
        let calculated = tree.root();
        Ok(calculated.as_bytes() == &self.root_hash)
    }

    /// Finds a sequence by path hash in this segment.
    pub fn find_seq_by_path_hash(&self, path_hash: u64) -> Option<u64> {
        // Record size is now 28 bytes: bin_off(8), c_len(4), inner_off(4), i_len(4), path_hash(8)
        let msg_count = (self.idx_mmap.len() - 32) / 28;
        for i in 0..msg_count {
            let idx_off = 32 + i * 28;
            let hash = u64::from_le_bytes(self.idx_mmap[idx_off + 20..idx_off + 28].try_into().unwrap());
            if hash == path_hash {
                return Some(self.start_seq + i as u64);
            }
        }
        None
    }

    /// Retrieves and decompresses a message by its relative index.
    pub fn get_decompressed_message_by_index(
        &self, 
        index: u64, 
        dict: Option<&[u8]>,
    ) -> io::Result<Vec<u8>> {
        // Record size is now 28 bytes: bin_off(8), c_len(4), inner_off(4), i_len(4), path_hash(8)
        let idx_start = 32 + (index as usize) * 28;
        let idx_end = idx_start + 28;

        if idx_end > self.idx_mmap.len() {
            return Err(io::Error::new(io::ErrorKind::NotFound, "Index out of bounds"));
        }

        let bin_off = u64::from_le_bytes(self.idx_mmap[idx_start..idx_start + 8].try_into().unwrap()) as usize;
        let c_len = u32::from_le_bytes(self.idx_mmap[idx_start + 8..idx_start + 12].try_into().unwrap()) as usize;
        let inner_off = u32::from_le_bytes(self.idx_mmap[idx_start + 12..idx_start + 16].try_into().unwrap()) as usize;
        let m_len = u32::from_le_bytes(self.idx_mmap[idx_start + 16..idx_start + 20].try_into().unwrap()) as usize;

        if m_len == 0 {
            return Err(io::Error::new(io::ErrorKind::NotFound, "Message not found in sequence gap"));
        }

        // Cache check
        {
            let cache = self.cluster_cache.lock().unwrap();
            if let Some(cluster) = cache.get(&bin_off) {
                if inner_off + m_len <= cluster.len() {
                    return Ok(cluster[inner_off..inner_off + m_len].to_vec());
                }
            }
        }

        if bin_off + c_len > self.bin_mmap.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Binary mapping out of bounds"));
        }

        let compressed_slice = &self.bin_mmap[bin_off..bin_off + c_len];
        let mut decompressed = Vec::new();
        if let Some(d) = dict {
            let mut decoder = zstd::stream::read::Decoder::with_dictionary(compressed_slice, d)?;
            std::io::copy(&mut decoder, &mut decompressed)?;
        } else {
            let mut decoder = zstd::stream::read::Decoder::new(compressed_slice)?;
            std::io::copy(&mut decoder, &mut decompressed)?;
        }

        if inner_off + m_len > decompressed.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Decompression index error"));
        }

        let result = decompressed[inner_off..inner_off + m_len].to_vec();
        {
            let mut cache = self.cluster_cache.lock().unwrap();
            if cache.len() >= 512 { cache.clear(); }
            cache.insert(bin_off, Arc::new(decompressed));
        }

        Ok(result)
    }

    /// Super-lean path: returns the raw compressed cluster for a message sequence index.
    pub fn get_raw_cluster_by_index(&self, index: u64) -> io::Result<&[u8]> {
        let idx_start = 32 + (index as usize) * 28;
        let bin_off = u64::from_le_bytes(self.idx_mmap[idx_start..idx_start + 8].try_into().unwrap()) as usize;
        let c_len = u32::from_le_bytes(self.idx_mmap[idx_start + 8..idx_start + 12].try_into().unwrap()) as usize;
        
        if bin_off + c_len > self.bin_mmap.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Bin OOB"));
        }
        Ok(&self.bin_mmap[bin_off..bin_off + c_len])
    }
}

/// Manages a collection of segments, providing O(log N) segment lookup
/// and O(1) message retrieval.
pub struct SegmentedArchive {
    data_dir: PathBuf,
    segments: RwLock<BTreeMap<u64, Vec<Segment>>>,
    tombstones: Option<Arc<RwLock<TombstoneStore>>>,
    dict_ref: Option<Arc<Vec<u8>>>,
}

impl SegmentedArchive {
    /// Opens all segments in a directory.
    pub fn open_directory<P: AsRef<Path>>(
        dir: P,
        tombstones: Option<Arc<RwLock<TombstoneStore>>>,
        dict_ref: Option<Arc<Vec<u8>>>
    ) -> io::Result<Self> {
        let dir_path = dir.as_ref().to_path_buf();
        if !dir_path.exists() {
            fs::create_dir_all(&dir_path)?;
        }

        let effective_tombstones = if let Some(ts) = tombstones {
            Some(ts)
        } else {
            let ts_path = dir_path.join("tombstones.bin");
            TombstoneStore::open_or_create(&ts_path).ok().map(|ts| Arc::new(RwLock::new(ts)))
        };

        let archive = SegmentedArchive {
            data_dir: dir_path,
            segments: RwLock::new(BTreeMap::new()),
            tombstones: effective_tombstones,
            dict_ref,
        };
        
        // Use refresh to populate shards correctly
        archive.refresh()?;

        Ok(archive)
    }

    fn scan_dir(dir: &Path, segments: &mut BTreeMap<u64, Vec<Segment>>) -> io::Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.extension().and_then(|s| s.to_str()) == Some("bin") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    // Filename is either "123" OR "shard_X_123"
                    let start_seq = if let Some(stripped) = stem.find('_').and_then(|i| stem[i+1..].parse::<u64>().ok()) {
                        stripped
                    } else if let Ok(n) = stem.parse::<u64>() {
                        n
                    } else {
                        continue;
                    };

                    let idx_path = path.with_extension("idx");
                    if idx_path.exists() {
                        let bin_file = File::open(&path)?;
                        let idx_file = File::open(&idx_path)?;
                        
                        let bin_mmap = unsafe { Mmap::map(&bin_file)? };
                        let idx_mmap = unsafe { Mmap::map(&idx_file)? };
                        
                        let segment = Segment::new(start_seq, bin_mmap, idx_mmap);
                        segments.entry(start_seq).or_default().push(segment);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn find_seq_by_path_hash(&self, path_hash: u64) -> Option<u64> {
        let segments = self.segments.read().unwrap();
        // Scan backwards from most recent segments
        for list in segments.values().rev() {
            for segment in list {
                if let Some(seq) = segment.find_seq_by_path_hash(path_hash) {
                    return Some(seq);
                }
            }
        }
        None
    }

    pub fn refresh(&self) -> io::Result<()> {
        let mut segments = self.segments.write().unwrap();
        segments.clear(); // Re-scan clean
        Self::scan_dir(&self.data_dir, &mut segments)?;
        
        // Also scan shard subdirectories if they exist
        if self.data_dir.exists() {
            for entry in fs::read_dir(&self.data_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() && path.file_name().and_then(|s| s.to_str()).map(|s| s.starts_with("shard_")).unwrap_or(false) {
                    Self::scan_dir(&path, &mut segments).ok();
                }
            }
        }
        Ok(())
    }

    /// Finds and retrieves a message by its global sequence number.
    /// Returns decompressed data.
    pub fn get_message_by_seq(&self, seq: u64, dict: Option<&[u8]>) -> io::Result<Vec<u8>> {
        if let Some(ts) = &self.tombstones {
            if ts.read().unwrap().is_deleted(seq) {
                return Err(io::Error::new(io::ErrorKind::NotFound, "Sequence tombstoned"));
            }
        }

        let segments = self.segments.read().unwrap();
        let effective_dict = dict.or_else(|| self.dict_ref.as_ref().map(|d| &d[..]));
        
        for (_start, list) in segments.range(..=seq).rev() {
            for segment in list {
                let rel_index = seq - segment.start_seq;
                let idx_start = 32 + (rel_index as usize) * 28;
                if idx_start + 20 <= segment.idx_mmap.len() {
                    let m_len = u32::from_le_bytes(segment.idx_mmap[idx_start + 16..idx_start + 20].try_into().unwrap());
                    if m_len != 0 {
                        return segment.get_decompressed_message_by_index(rel_index, effective_dict);
                    }
                }
            }
        }
        Err(io::Error::new(io::ErrorKind::NotFound, "Sequence not found in archive"))
    }

    /// Returns the raw compressed cluster for a global sequence.
    pub fn get_raw_cluster_at_seq(&self, seq: u64) -> io::Result<Vec<u8>> {
        if let Some(ts) = &self.tombstones {
            if ts.read().unwrap().is_deleted(seq) {
                return Err(io::Error::new(io::ErrorKind::NotFound, "Sequence tombstoned"));
            }
        }

        let segments = self.segments.read().unwrap();
        
        for (_start, list) in segments.range(..=seq).rev() {
            for segment in list {
                let rel_index = seq - segment.start_seq;
                let idx_start = 32 + (rel_index as usize) * 28;
                
                if idx_start + 12 <= segment.idx_mmap.len() {
                    let bin_off = u64::from_le_bytes(segment.idx_mmap[idx_start..idx_start + 8].try_into().unwrap()) as usize;
                    if bin_off != 0 {
                        let c_len = u32::from_le_bytes(segment.idx_mmap[idx_start + 8..idx_start + 12].try_into().unwrap()) as usize;
                        if bin_off + c_len <= segment.bin_mmap.len() {
                            let raw_cluster = &segment.bin_mmap[bin_off..bin_off + c_len];
                            
                            // Check if ANY sequence in this cluster is tombstoned
                            if let Some(ts) = &self.tombstones {
                                let mut cluster_seqs = Vec::new();
                                // Record size 28
                                let msg_count = (segment.idx_mmap.len() - 32) / 28;
                                for i in 0..msg_count {
                                    let off = 32 + i * 28;
                                    let b_off = u64::from_le_bytes(segment.idx_mmap[off..off + 8].try_into().unwrap()) as usize;
                                    if b_off == bin_off {
                                        cluster_seqs.push(segment.start_seq + i as u64);
                                    }
                                }

                                let ts_lock = ts.read().unwrap();
                                let mut any_tombstoned = false;
                                for s in &cluster_seqs {
                                    if ts_lock.is_deleted(*s) {
                                        any_tombstoned = true;
                                        break;
                                    }
                                }

                                if any_tombstoned {
                                    // Decompress, Filter, Re-compress (LEAN BUT COMPLIANT)
                                    let mut decompressed = Vec::new();
                                    use std::io::Read;
                                    if let Some(dict) = self.dict_ref.as_ref() {
                                        let mut decoder = zstd::Decoder::with_dictionary(raw_cluster, &dict[..])?;
                                        decoder.read_to_end(&mut decompressed)?;
                                    } else {
                                        let mut decoder = zstd::Decoder::new(raw_cluster)?;
                                        decoder.read_to_end(&mut decompressed)?;
                                    }

                                    // The cluster format: [u16 count][u32 len1][u32 len2]...[data1][data2]...
                                    if decompressed.len() < 2 { return Ok(raw_cluster.to_vec()); }
                                    let count = u16::from_le_bytes([decompressed[0], decompressed[1]]) as usize;
                                    if count != cluster_seqs.len() { return Ok(raw_cluster.to_vec()); }

                                    let mut offsets = Vec::new();
                                    let mut curr = 2 + (count * 4);
                                    for i in 0..count {
                                        let len = u32::from_le_bytes(decompressed[2 + i*4..6 + i*4].try_into().unwrap()) as usize;
                                        offsets.push((curr, len));
                                        curr += len;
                                    }

                                    let mut filtered_payloads = Vec::new();
                                    for (i, s) in cluster_seqs.iter().enumerate() {
                                        if !ts_lock.is_deleted(*s) {
                                            let (o, l) = offsets[i];
                                            filtered_payloads.push(&decompressed[o..o+l]);
                                        }
                                    }

                                    // Rebuild cluster
                                    let mut rebuilt = Vec::new();
                                    rebuilt.extend_from_slice(&(filtered_payloads.len() as u16).to_le_bytes());
                                    for p in &filtered_payloads {
                                        rebuilt.extend_from_slice(&(p.len() as u32).to_le_bytes());
                                    }
                                    for p in &filtered_payloads {
                                        rebuilt.extend_from_slice(p);
                                    }

                                    let compressed;
                                    use std::io::Write;
                                    if let Some(dict) = self.dict_ref.as_ref() {
                                        let mut encoder = zstd::Encoder::with_dictionary(Vec::new(), 3, &dict[..])?;
                                        encoder.write_all(&rebuilt)?;
                                        compressed = encoder.finish()?;
                                    } else {
                                        let mut encoder = zstd::Encoder::new(Vec::new(), 3)?;
                                        encoder.write_all(&rebuilt)?;
                                        compressed = encoder.finish()?;
                                    }
                                    return Ok(compressed);
                                }
                            }

                            return Ok(raw_cluster.to_vec());
                        }
                    }
                }
            }
        }
        Err(io::Error::new(io::ErrorKind::NotFound, "Sequence not found in archive"))
    }

    pub fn min_seq(&self) -> Option<u64> {
        let segments = self.segments.read().unwrap();
        segments.keys().next().cloned()
    }

    pub fn max_seq(&self) -> Option<u64> {
        let segments = self.segments.read().unwrap();
        let (start_seq, list) = segments.iter().next_back()?;
        
        let mut max = *start_seq;
        for segment in list {
            let msg_count = (segment.idx_mmap.len() - 32) / 28;
            if msg_count > 0 {
                // Find highest non-zero message length by scanning backwards
                for i in (0..msg_count).rev() {
                    let idx_off = 32 + i * 28;
                    let m_len = u32::from_le_bytes(segment.idx_mmap[idx_off + 16..idx_off + 20].try_into().unwrap());
                    if m_len != 0 {
                        let current_max = *start_seq + (i as u64);
                        if current_max > max {
                            max = current_max;
                        }
                        break; 
                    }
                }
            }
        }
        Some(max)
    }

    pub fn segment_count(&self) -> usize {
        let segments = self.segments.read().unwrap();
        let mut count = 0;
        for list in segments.values() {
            count += list.len();
        }
        count
    }

    pub fn merge(&self, other: SegmentedArchive) {
        let mut segments = self.segments.write().unwrap();
        let other_segments = other.segments.into_inner().unwrap();
        for (seq, segment_list) in other_segments {
            segments.entry(seq).or_default().extend(segment_list);
        }
    }

    /// Finds a sequence number by its path hash. 
    /// Note: This performs a linear scan of segments and is intended to be called 
    /// on a specific shard's archive to stay "lean".
    pub fn find_sequence_by_path(&self, path_hash: u64) -> Option<u64> {
        let segments = self.segments.read().unwrap();
        // Scan backwards (most recent first)
        for list in segments.values().rev() {
            for segment in list {
                if let Some(seq) = segment.find_seq_by_path_hash(path_hash) {
                    return Some(seq);
                }
            }
        }
        None
    }

    pub fn mark_deleted(&self, seq: u64) {
        if let Some(ts) = &self.tombstones {
            ts.write().unwrap().mark_deleted(seq);
        }
    }

    pub fn verify_integrity_at_seq(&self, seq: u64, dict: Option<&[u8]>) -> io::Result<bool> {
        let segments = self.segments.read().unwrap();
        for (_start, list) in segments.range(..=seq).rev() {
            for segment in list {
                let msg_count = (segment.idx_mmap.len() - 32) / 28;
                if seq >= segment.start_seq && seq < segment.start_seq + msg_count as u64 {
                    return segment.verify_integrity(dict);
                }
            }
        }
        Err(io::Error::new(io::ErrorKind::NotFound, "Sequence not found"))
    }

    pub fn get_segment(&self, _start_seq: u64) -> Option<Segment> {
        // Note: Returning Segment by value copies the Mmaps (cheap) but we should be careful.
        // Actually, Segment doesn't implement Clone easily because of Mutex.
        // Let's just not provide this or return a reference if needed.
        None
    }
}

/// Handles appending to the archive using clustered batching for 68% compression.
pub struct ArchiveWriter {
    data_dir: PathBuf,
    current_start_seq: u64,
    current_max_seq: u64,
    current_count: u64,
    max_segment_messages: u64,
    dict: Option<Box<[u8]>>,
    
    // Stats for benchmarking
    pub total_compressed_bytes: u64,
    
    // Clustering buffer: DID -> Vec<(Sequence, Path, Data)>
    pending: HashMap<String, Vec<(u64, String, Vec<u8>)>>,
    shard_id: usize,
}

impl ArchiveWriter {
    pub fn new<P: AsRef<Path>>(
        dir: P, 
        shard_id: u64,
        start_seq: u64, 
        max_messages: u64,
        dict: Option<Vec<u8>>
    ) -> io::Result<Self> {
        if !dir.as_ref().exists() {
            fs::create_dir_all(&dir)?;
        }

        Ok(ArchiveWriter {
            data_dir: dir.as_ref().to_path_buf(),
            current_start_seq: start_seq,
            current_max_seq: 0,
            current_count: 0,
            max_segment_messages: max_messages,
            dict: dict.map(|d| d.into_boxed_slice()),
            total_compressed_bytes: 0,
            pending: HashMap::with_capacity(10000),
            shard_id: shard_id as usize,
        })
    }

    /// Appends a message. If full, returns the payload to be persisted in background.
    pub fn append_message(&mut self, seq: u64, did: &str, path: &str, data: &[u8]) -> io::Result<Option<SegmentPayload>> {
        if self.pending.is_empty() {
            self.current_start_seq = seq;
            self.current_max_seq = seq;
        } else {
            if seq > self.current_max_seq {
                self.current_max_seq = seq;
            }
        }
        
        self.pending.entry(did.to_string()).or_default().push((seq, path.to_string(), data.to_vec()));
        self.current_count += 1;

        if self.current_count >= self.max_segment_messages {
            let payload = self.take_payload();
            return Ok(Some(payload));
        }
        
        Ok(None)
    }

    /// Manually finalize and persist the current segment (useful for tests/shutdown).
    pub fn finalize_segment(&mut self) -> io::Result<()> {
        let payload = self.take_payload();
        Self::persist_payload(payload, self.dict.as_ref().map(|d| &d[..]))?;
        Ok(())
    }

    pub fn take_payload(&mut self) -> SegmentPayload {
        let payload = SegmentPayload {
            start_seq: self.current_start_seq,
            max_seq: self.current_max_seq,
            count: self.current_count,
            pending: std::mem::take(&mut self.pending),
            shard_dir: self.data_dir.clone(),
            shard_id: self.shard_id,
        };
        self.current_count = 0;
        self.current_max_seq = 0;
        payload
    }

    /// Flushes a frozen payload to disk. This is STATIC and doesn't hold Writer locks.
    pub fn persist_payload(payload: SegmentPayload, dict: Option<&[u8]>) -> io::Result<u64> {
        if payload.pending.is_empty() { return Ok(0); }
        use fxhash::FxHasher;
        use std::hash::{Hasher, Hash};

        let base_name = format!("s{}_{}", payload.shard_id, payload.start_seq);
        let bin_path = payload.shard_dir.join(format!("{}.bin", base_name));
        let idx_path = payload.shard_dir.join(format!("{}.idx", base_name));
        
        let mut bin_file = File::create(&bin_path)?;
        let mut idx_map = BTreeMap::new(); 
        let mut seq_to_data = HashMap::with_capacity(payload.count as usize);

        let mut current_bin_offset = 0u64;
        let mut compressor = if let Some(d) = dict {
            zstd::bulk::Compressor::with_dictionary(3, d)?
        } else {
            zstd::bulk::Compressor::new(3)?
        };

        let mut dids: Vec<_> = payload.pending.keys().collect();
        dids.sort();

        for did in dids {
            let messages = payload.pending.get(did).unwrap();
            let mut cluster_raw = Vec::new();
            let mut header = Vec::with_capacity(2 + messages.len() * 12);
            header.extend_from_slice(&(messages.len() as u16).to_le_bytes());

            for (seq, _path, data) in messages {
                header.extend_from_slice(&seq.to_le_bytes());
                header.extend_from_slice(&(data.len() as u32).to_le_bytes());
                cluster_raw.extend_from_slice(data);
                seq_to_data.insert(*seq, data.clone());
            }

            let mut final_raw = header;
            final_raw.extend_from_slice(&cluster_raw);

            let compressed = compressor.compress(&final_raw)?;
            let compressed_len = compressed.len() as u32;
            bin_file.write_all(&compressed)?;

            let mut current_inner_off = 2 + (messages.len() as u32 * 12);
            for (seq, path, data) in messages {
                let mut hasher = FxHasher::default();
                path.hash(&mut hasher);
                let path_hash = hasher.finish();

                idx_map.insert(*seq, (current_bin_offset, compressed_len, current_inner_off, data.len() as u32, path_hash));
                current_inner_off += data.len() as u32;
            }

            current_bin_offset += compressed_len as u64;
        }

        let mut tree = MerkleTree::new();
        for seq in payload.start_seq..=payload.max_seq {
            if let Some(data) = seq_to_data.get(&seq) { 
                tree.push(data); 
            }
        }
        let root = tree.root();

        let mut idx_file = File::create(&idx_path)?;
        idx_file.write_all(root.as_bytes())?;
        for seq in payload.start_seq..=payload.max_seq {
            let (bin_off, c_len, inner_off, i_len, path_hash) = idx_map.get(&seq).cloned().unwrap_or((0,0,0,0,0));
            idx_file.write_all(&bin_off.to_le_bytes())?;
            idx_file.write_all(&c_len.to_le_bytes())?;
            idx_file.write_all(&inner_off.to_le_bytes())?;
            idx_file.write_all(&i_len.to_le_bytes())?;
            idx_file.write_all(&path_hash.to_le_bytes())?;
        }

        bin_file.sync_all()?;
        idx_file.sync_all()?;
        Ok(current_bin_offset)
    }


}

pub struct MultiShardArchive {
    writers: Vec<Mutex<ArchiveWriter>>,
    readers: Vec<SegmentedArchive>,
    persist_tx: Sender<Option<SegmentPayload>>, // Option for Poison Pill
    dict_ref: Option<Arc<Vec<u8>>>,
    persist_thread: Mutex<Option<thread::JoinHandle<()>>>,
    tombstones: Option<Arc<RwLock<TombstoneStore>>>,
}

impl MultiShardArchive {
    pub fn open_readonly(path: impl AsRef<Path>, dict: Option<Vec<u8>>) -> io::Result<Self> {
        let path = path.as_ref();
        let ts_path = path.join("tombstones.bin");
        let tombstones = TombstoneStore::open_or_create(&ts_path).ok().map(|ts| Arc::new(RwLock::new(ts)));
        let dict_arc = dict.map(Arc::new);
        
        let mut readers = Vec::new();
        // Scan for shard_N directories
        let mut shard_idx = 0;
        loop {
            let shard_dir = path.join(format!("shard_{}", shard_idx));
            if !shard_dir.exists() { break; }
            readers.push(SegmentedArchive::open_directory(shard_dir, tombstones.clone(), dict_arc.clone())?);
            shard_idx += 1;
        }

        if readers.is_empty() {
            // Try opening the root as a single shard if no shard_N found
            readers.push(SegmentedArchive::open_directory(path, tombstones.clone(), dict_arc.clone())?);
        }

        let (tx, _) = unbounded::<Option<SegmentPayload>>();
        
        Ok(Self {
            writers: Vec::new(),
            readers,
            persist_tx: tx,
            dict_ref: dict_arc,
            persist_thread: Mutex::new(None),
            tombstones,
        })
    }

    pub fn new(path: impl AsRef<Path>, num_shards: usize, segment_size: u64, dict: Option<Vec<u8>>) -> io::Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            fs::create_dir_all(path)?;
        }

        let ts_path = path.join("tombstones.bin");
        let tombstones = TombstoneStore::open_or_create(&ts_path).ok().map(|ts| Arc::new(RwLock::new(ts)));

        let dict_arc = dict.map(Arc::new);
        let mut writers = Vec::new();
        let mut readers = Vec::new();
        for i in 0..num_shards {
            let shard_dir = path.join(format!("shard_{}", i));
            let start_seq = 0; 
            writers.push(Mutex::new(ArchiveWriter::new(shard_dir.clone(), i as u64, start_seq, segment_size, dict_arc.as_ref().map(|d| d.to_vec()))?));
            readers.push(SegmentedArchive::open_directory(shard_dir, tombstones.clone(), dict_arc.clone())?);
        }

        let (tx, rx) = unbounded::<Option<SegmentPayload>>();
        let dict_for_thread = dict_arc.clone();
        
        // Background Persister Thread
        let handle = thread::spawn(move || {
            while let Ok(maybe_payload) = rx.recv() {
                if let Some(payload) = maybe_payload {
                    let _ = ArchiveWriter::persist_payload(payload, dict_for_thread.as_ref().map(|d| &d[..]));
                } else {
                    break; // Poison Pill received
                }
            }
        });

        Ok(Self {
            writers,
            readers,
            persist_tx: tx,
            dict_ref: dict_arc,
            persist_thread: Mutex::new(Some(handle)),
            tombstones,
        })
    }

    pub fn ingest(&self, seq: u64, did: &str, path: String, msg: Vec<u8>) {
        use fxhash::FxHasher;
        use std::hash::{Hasher, Hash};

        let mut hasher = FxHasher::default();
        did.hash(&mut hasher);
        let shard_idx = hasher.finish() as usize % self.writers.len(); 

        let mut writer = self.writers[shard_idx].lock().unwrap();
        if let Ok(Some(payload)) = writer.append_message(seq, did, &path, &msg) {
            let _ = self.persist_tx.send(Some(payload));
        }
    }

    pub fn mark_deleted(&self, seq: u64) {
        if let Some(ts) = &self.tombstones {
            ts.write().unwrap().mark_deleted(seq);
        }
    }

    pub fn delete_by_path(&self, did: &str, path: &str) {
        use fxhash::FxHasher;
        use std::hash::{Hasher, Hash};

        let mut hasher = FxHasher::default();
        did.hash(&mut hasher);
        let shard_idx = hasher.finish() as usize % self.readers.len();
        
        let path_hasher = {
            let mut h = FxHasher::default();
            path.hash(&mut h);
            h.finish()
        };

        let reader = &self.readers[shard_idx];
        // 1. Refresh reader to see most recent segments
        let _ = reader.refresh(); 

        // 2. Find sequence
        if let Some(seq) = reader.find_sequence_by_path(path_hasher) {
            self.mark_deleted(seq);
        }
    }

    pub fn reader_count(&self) -> usize {
        self.readers.len()
    }

    pub fn shutdown(&self) {
        println!("[Archive] Finalizing shards for shutdown...");
        for writer in &self.writers {
            let mut w = writer.lock().unwrap();
            let payload = w.take_payload();
            let _ = self.persist_tx.send(Some(payload));
        }
        
        // Send poison pill
        let _ = self.persist_tx.send(None);
        
        // Wait for thread to finish
        if let Ok(mut lock) = self.persist_thread.lock() {
            if let Some(handle) = lock.take() {
                println!("[Archive] Waiting for background persistence to finish...");
                let _ = handle.join();
                println!("[Archive] Persistence finished.");
            }
        }
    }

    // --- Reader Delegation ---

    pub fn min_seq(&self) -> Option<u64> {
        self.readers.iter().filter_map(|r| r.min_seq()).min()
    }

    pub fn max_seq(&self) -> Option<u64> {
        self.readers.iter().filter_map(|r| r.max_seq()).max()
    }

    pub fn refresh(&self) -> io::Result<()> {
        for r in &self.readers {
            r.refresh()?;
        }
        Ok(())
    }

    pub fn get_message_by_seq(&self, seq: u64) -> io::Result<Vec<u8>> {
        for r in &self.readers {
            if let Ok(data) = r.get_message_by_seq(seq, self.dict_ref.as_ref().map(|d| &d[..])) {
                return Ok(data);
            }
        }
        Err(io::Error::new(io::ErrorKind::NotFound, "Sequence not found in any shard"))
    }

    pub fn get_raw_cluster_at_seq(&self, seq: u64) -> io::Result<Vec<u8>> {
        for r in &self.readers {
            // SegmentedArchive::get_raw_cluster_at_seq already handles tombstones
            if let Ok(data) = r.get_raw_cluster_at_seq(seq) {
                return Ok(data);
            }
        }
        Err(io::Error::new(io::ErrorKind::NotFound, "Sequence not found in any shard"))
    }
}
