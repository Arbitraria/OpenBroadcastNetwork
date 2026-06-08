//! Cryptographic primitives and utilities for the decentralized streaming CDN.
//!
//! This module provides cryptographic functionality including hashing, signatures,
//! and encryption/decryption utilities used throughout the system.

use std::error::Error;
use std::fmt;

/// Error type for cryptographic operations
#[derive(Debug)]
pub struct CryptoError {
    message: String,
}

impl fmt::Display for CryptoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Crypto error: {}", self.message)
    }
}

impl Error for CryptoError {}

impl From<&str> for CryptoError {
    fn from(err: &str) -> Self {
        CryptoError {
            message: err.to_string(),
        }
    }
}

impl From<String> for CryptoError {
    fn from(err: String) -> Self {
        CryptoError { message: err }
    }
}

/// Generate a secure random byte vector of the specified length
pub fn random_bytes(length: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; length];
    getrandom::getrandom(&mut bytes).expect("Failed to generate random bytes");
    bytes
}

/// Hash data using SHA-256
pub fn sha256(data: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Compute a blake3 hash suitable for signing (32 bytes)
pub fn hash_for_signing(data: &[u8]) -> [u8; 32] {
    *blake3::hash(data).as_bytes()
}

/// Sign data using a libp2p identity keypair
#[cfg(not(target_arch = "wasm32"))]
pub fn sign_with_keypair(
    keypair: &libp2p::identity::Keypair,
    data: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let hash = hash_for_signing(data);
    keypair
        .sign(&hash)
        .map_err(|e| CryptoError::from(format!("Signing failed: {}", e)))
}

/// Verify a signature using a libp2p identity public key
#[cfg(not(target_arch = "wasm32"))]
pub fn verify_with_public_key(
    pubkey: &libp2p::identity::PublicKey,
    data: &[u8],
    signature: &[u8],
) -> Result<bool, CryptoError> {
    let hash = hash_for_signing(data);
    Ok(pubkey.verify(&hash, signature))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random_bytes() {
        let bytes = random_bytes(32);
        assert_eq!(bytes.len(), 32);

        // Very basic test to ensure we're getting different values
        let bytes2 = random_bytes(32);
        assert_ne!(bytes, bytes2);
    }

    #[test]
    fn test_sha256() {
        let data = b"hello, world";
        let hash = sha256(data);
        assert_eq!(hash.len(), 32);

        // Test that different inputs produce different hashes
        let hash2 = sha256(b"hello, world!");
        assert_ne!(hash, hash2);
    }

    #[test]
    fn test_hash_for_signing() {
        let hash = hash_for_signing(b"test data");
        assert_eq!(hash.len(), 32);

        let hash2 = hash_for_signing(b"different data");
        assert_ne!(hash, hash2);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_sign_and_verify_with_keypair() {
        let keypair = libp2p::identity::Keypair::generate_ed25519();
        let data = b"hello, signed world";

        let signature = sign_with_keypair(&keypair, data).unwrap();
        assert!(!signature.is_empty());

        let pubkey = keypair.public();
        let valid = verify_with_public_key(&pubkey, data, &signature).unwrap();
        assert!(valid);

        // Tampered data should fail
        let invalid = verify_with_public_key(&pubkey, b"tampered", &signature).unwrap();
        assert!(!invalid);
    }
}
