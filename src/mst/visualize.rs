use crate::mst::{MstNode, car::CarStore};

pub fn draw_mst_visual(node: &MstNode, store: &CarStore, depth: usize, last_reconstructed_key: Vec<u8>) {
    let indent = "│   ".repeat(depth);
    
    // 1. Draw Left Subtree
    if let Some(left_cid) = node.left {
        let cid_bytes = left_cid.to_bytes();
        if let Some(block_data) = store.get_block(&cid_bytes) {
            if let Ok(child_node) = MstNode::from_bytes(block_data) {
                println!("{}├── 📂 [Left] {}", indent, left_cid);
                draw_mst_visual(&child_node, store, depth + 1, last_reconstructed_key.clone());
            }
        }
    }

    // 2. Draw Entries
    let mut current_key = last_reconstructed_key;
    for (i, entry) in node.entries.iter().enumerate() {
        // Reconstruct key from prefix compression
        let prefix_len = entry.prefix_len as usize;
        let mut full_key = if prefix_len <= current_key.len() {
            current_key[..prefix_len].to_vec()
        } else {
            current_key.clone()
        };
        full_key.extend_from_slice(&entry.key_suffix);
        current_key = full_key.clone();

        let key_str = String::from_utf8_lossy(&full_key);
        let is_last = i == node.entries.len() - 1 && entry.tree.is_none();
        let connector = if is_last { "└──" } else { "├──" };

        println!("{}{} 📄 {} (CID: {})", 
            indent, 
            connector, 
            key_str, 
            entry.value.to_string().chars().take(12).collect::<String>() + "..."
        );

        // 3. Draw Right Subtree (attached to entry)
        if let Some(right_cid) = entry.tree {
            let cid_bytes = right_cid.to_bytes();
            if let Some(block_data) = store.get_block(&cid_bytes) {
                if let Ok(child_node) = MstNode::from_bytes(block_data) {
                    println!("{}│   └── 📂 [Right] {}", indent, right_cid);
                    draw_mst_visual(&child_node, store, depth + 1, current_key.clone());
                }
            }
        }
    }
}
