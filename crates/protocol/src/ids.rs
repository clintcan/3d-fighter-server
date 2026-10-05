//! Identifiers and tokens generated with the OS CSPRNG.

use rand::rngs::OsRng;
use rand::RngCore;

/// Room-code alphabet: no `0`, `O`, `1`, `I`, `L`.
pub const ROOM_CODE_ALPHABET: &[u8] = b"ABCDEFGHJKMNPQRSTUVWXYZ23456789";
/// Room-code length in characters.
pub const ROOM_CODE_LEN: usize = 6;

/// A six-character room code, uniformly chosen from the alphabet.
pub fn generate_room_code() -> String {
    let mut bytes = [0u8; ROOM_CODE_LEN];
    OsRng.fill_bytes(&mut bytes);
    bytes
        .iter()
        .map(|b| ROOM_CODE_ALPHABET[(*b as usize) % ROOM_CODE_ALPHABET.len()] as char)
        .collect()
}

/// `n` random bytes rendered as lowercase hex (session tokens, resume tokens).
pub fn generate_hex_token(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    OsRng.fill_bytes(&mut buf);
    hex::encode(buf)
}

/// `N` random bytes from the OS CSPRNG (raw token/key material).
pub fn generate_bytes<const N: usize>() -> [u8; N] {
    let mut buf = [0u8; N];
    OsRng.fill_bytes(&mut buf);
    buf
}

/// A session id like `s_1a2b3c4d`.
pub fn generate_session_id() -> String {
    format!("s_{}", generate_hex_token(4))
}

/// A room id like `r_1a2b3c4d`.
pub fn generate_room_id() -> String {
    format!("r_{}", generate_hex_token(4))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn room_code_shape_and_alphabet() {
        for _ in 0..200 {
            let code = generate_room_code();
            assert_eq!(code.len(), ROOM_CODE_LEN);
            assert!(code.bytes().all(|b| ROOM_CODE_ALPHABET.contains(&b)));
            assert!(!code.contains(['0', 'O', '1', 'I', 'L']));
        }
    }

    #[test]
    fn tokens_have_expected_hex_length() {
        assert_eq!(generate_hex_token(16).len(), 32);
        assert_eq!(generate_hex_token(8).len(), 16);
        assert!(generate_session_id().starts_with("s_"));
        assert!(generate_room_id().starts_with("r_"));
    }
}
