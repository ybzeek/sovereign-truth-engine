use reqwest::blocking::Client;
use serde_json::Value;
use std::sync::OnceLock;

static CLIENT: OnceLock<Client> = OnceLock::new();

fn get_client() -> &'static Client {
    CLIENT.get_or_init(|| Client::new())
}

/// Resolves a DID (supports did:plc, did:web, and did:key)
/// Returns (pubkey_bytes, key_type) if found.
pub fn resolve_did(did: &str) -> Option<([u8; 33], u8)> {
    if did.starts_with("did:plc:") {
        resolve_did_plc(did)
    } else if did.starts_with("did:web:") {
        resolve_did_web(did)
    } else if did.starts_with("did:key:") {
        did_key_to_raw_pubkey(did)
    } else {
        None
    }
}

pub fn resolve_handle(did: &str) -> Option<String> {
    if !did.starts_with("did:plc:") { return None; }
    let url = format!("https://plc.directory/{}/data", did);
    let client = get_client();
    let resp = client.get(url).send().ok()?;
    if !resp.status().is_success() { return None; }
    let json: Value = resp.json().ok()?;
    
    // alsoKnownAs is usually ["at://..."]
    if let Some(aka) = json.get("alsoKnownAs").and_then(|a| a.as_array()) {
        if let Some(first) = aka.get(0).and_then(|v| v.as_str()) {
            return Some(first.trim_start_matches("at://").to_string());
        }
    }
    None
}

fn resolve_did_plc(did: &str) -> Option<([u8; 33], u8)> {
    let url = format!("https://plc.directory/{}/log/last", did);
    let client = get_client();
    
    let resp = client.get(url).send().ok()?;
    if !resp.status().is_success() {
        return None;
    }
    
    let json: Value = resp.json().ok()?;
    
    // We look for Secp256k1 keys in prioritized order:
    // 1. "atproto" verification method (Standard for Repo signing)
    // 2. "signingKey" (Master key)
    // 3. Any other valid key in the document
    
    let vms = json.get("verificationMethods").and_then(|v| v.as_object());
    
    // Priority 1: atproto
    if let Some(atproto_key) = vms.and_then(|m| m.get("atproto")).and_then(|v| v.as_str()) {
        if let Some(res) = did_key_to_raw_pubkey(atproto_key) {
            return Some(res);
        }
    }
    
    // Priority 2: signingKey
    if let Some(signing_key) = json.get("signingKey").and_then(|v| v.as_str()) {
        if let Some(res) = did_key_to_raw_pubkey(signing_key) {
            return Some(res);
        }
    }

    // Priority 3: any other key in verificationMethods
    if let Some(obj) = vms {
        for (id, val) in obj.iter() {
            if id == "atproto" { continue; } // already tried
            if let Some(s) = val.as_str() {
                if let Some(res) = did_key_to_raw_pubkey(s) {
                    return Some(res);
                }
            }
        }
    }

    // Compatibility check for older operation nested format
    if let Some(op) = json.get("operation") {
        let vms_inner = op.get("verificationMethods").and_then(|v| v.as_object());
        if let Some(atproto_key) = vms_inner.and_then(|m| m.get("atproto")).and_then(|v| v.as_str()) {
            if let Some(res) = did_key_to_raw_pubkey(atproto_key) {
                return Some(res);
            }
        }
        if let Some(signing_key) = op.get("signingKey").and_then(|v| v.as_str()) {
            if let Some(res) = did_key_to_raw_pubkey(signing_key) {
                return Some(res);
            }
        }
    }
    
    None
}

fn resolve_did_web(did: &str) -> Option<([u8; 33], u8)> {
    // did:web:example.com -> https://example.com/.well-known/did.json
    // did:web:example.com:path:to:user -> https://example.com/path/to/user/did.json
    let parts: Vec<&str> = did.split(':').collect();
    if parts.len() < 3 {
        return None;
    }

    let host = parts[2];
    let url = if parts.len() == 3 {
        format!("https://{}/.well-known/did.json", host)
    } else {
        let path = parts[3..].join("/");
        format!("https://{}/{}/did.json", host, path)
    };

    let client = get_client();
    let resp = client.get(url).send().ok()?;
    if !resp.status().is_success() {
        return None;
    }

    let json: Value = resp.json().ok()?;

    // In a DID document, keys are in verificationMethod
    if let Some(vms) = json.get("verificationMethod").and_then(|v| v.as_array()) {
        for vm in vms {
            // We look for Secp256k1 keys. 
            // 1. publicKeyMultibase (common in ATP)
            if let Some(pk_multi) = vm.get("publicKeyMultibase").and_then(|v| v.as_str()) {
                if let Some((pk, kt)) = multibase_to_raw_pubkey(pk_multi) {
                    return Some((pk, kt));
                }
            }
            // 2. publicKeyJwk (common in other did:web implementations)
            if let Some(jwk) = vm.get("publicKeyJwk") {
                if let Some((pk, kt)) = jwk_to_raw_pubkey(jwk) {
                    return Some((pk, kt));
                }
            }
        }
    }

    None
}

/// Decodes a JWK into a raw pubkey if it's Secp256k1 or P-256
fn jwk_to_raw_pubkey(jwk: &Value) -> Option<([u8; 33], u8)> {
    // Expected: kty: "EC", crv: "secp256k1" or "P-256", x: ..., y: ...
    let kty = jwk.get("kty")?.as_str()?;
    let crv = jwk.get("crv")?.as_str()?;
    
    let key_type = if kty == "EC" && crv == "secp256k1" {
        1u8 // Secp256k1
    } else if kty == "EC" && crv == "P-256" {
        2u8 // P-256
    } else {
        return None;
    };

    let x_b64 = jwk.get("x")?.as_str()?;
    let y_b64 = jwk.get("y")?.as_str()?;
    
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    let x = URL_SAFE_NO_PAD.decode(x_b64).ok()?;
    let y = URL_SAFE_NO_PAD.decode(y_b64).ok()?;
    
    if x.len() == 32 && y.len() == 32 {
        // Convert to compressed form: 02 or 03 based on y's parity
        let mut pk = [0u8; 33];
        pk[0] = if y[31] % 2 == 0 { 0x02 } else { 0x03 };
        pk[1..33].copy_from_slice(&x);
        return Some((pk, key_type));
    }
    None
}

/// Decodes a multibase public key (e.g. "zQ3sh..." for secp256k1 or "zDna..." for P-256)
fn multibase_to_raw_pubkey(multibase_key: &str) -> Option<([u8; 33], u8)> {
    if !multibase_key.starts_with('z') {
        return None;
    }
    let rest = &multibase_key[1..];
    let decoded = bs58::decode(rest).into_vec().ok()?;
    
    // Secp256k1 prefix: 0xe7 0x01 (35 bytes total)
    if decoded.starts_with(&[0xe7, 0x01]) && decoded.len() == 35 {
        let mut pk = [0u8; 33];
        pk.copy_from_slice(&decoded[2..]);
        return Some((pk, 1)); // 1 = Secp256k1
    }
    
    // P-256 prefix: 0x80 0x24 (35 bytes total)
    if decoded.starts_with(&[0x80, 0x24]) && decoded.len() == 35 {
        let mut pk = [0u8; 33];
        pk.copy_from_slice(&decoded[2..]);
        return Some((pk, 2)); // 2 = P-256
    }

    None
}

/// Helper to decode did:key:z... (secp256k1 or P-256)
fn did_key_to_raw_pubkey(did_key: &str) -> Option<([u8; 33], u8)> {
    if !did_key.starts_with("did:key:z") {
        return None;
    }
    multibase_to_raw_pubkey(&did_key[8..])
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_did_web_transform() {
        let did = "did:web:example.com";
        let parts: Vec<&str> = did.split(':').collect();
        let host = parts[2];
        let url = if parts.len() == 3 {
            format!("https://{}/.well-known/did.json", host)
        } else {
            let path = parts[3..].join("/");
            format!("https://{}/{}/did.json", host, path)
        };
        assert_eq!(url, "https://example.com/.well-known/did.json");

        let did2 = "did:web:example.com:user:alice";
        let parts2: Vec<&str> = did2.split(':').collect();
        let host2 = parts2[2];
        let path2 = parts2[3..].join("/");
        let url2 = format!("https://{}/{}/did.json", host2, path2);
        assert_eq!(url2, "https://example.com/user/alice/did.json");
    }
}
