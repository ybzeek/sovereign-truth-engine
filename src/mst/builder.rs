use blake3;

/// A simple, high-performance Merkle Tree builder for segment verification.
pub struct MerkleTree {
    leaves: Vec<blake3::Hash>,
}

impl MerkleTree {
    pub fn new() -> Self {
        Self { leaves: Vec::with_capacity(50000) }
    }

    pub fn push(&mut self, data: &[u8]) {
        self.leaves.push(blake3::hash(data));
    }

    pub fn root(&self) -> blake3::Hash {
        if self.leaves.is_empty() {
            return blake3::hash(&[]);
        }
        
        let mut current_layer = self.leaves.clone();
        while current_layer.len() > 1 {
            let mut next_layer = Vec::with_capacity((current_layer.len() + 1) / 2);
            for chunk in current_layer.chunks(2) {
                if chunk.len() == 2 {
                    let mut hasher = blake3::Hasher::new();
                    hasher.update(chunk[0].as_bytes());
                    hasher.update(chunk[1].as_bytes());
                    next_layer.push(hasher.finalize());
                } else {
                    next_layer.push(chunk[0]);
                }
            }
            current_layer = next_layer;
        }
        current_layer[0]
    }
}
