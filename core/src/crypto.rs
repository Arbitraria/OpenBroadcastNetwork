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

/// Sign data with a private key
pub fn sign(_private_key: &[u8], _data: &[u8]) -> Result<Vec<u8>, CryptoError> {
    // Implementation depends on the cryptographic library you're using
    // This is a placeholder implementation
    Ok(vec![])
}

/// Verify a signature with a public key
pub fn verify(_public_key: &[u8], _data: &[u8], _signature: &[u8]) -> Result<bool, CryptoError> {
    // Implementation depends on the cryptographic library you're using
    // This is a placeholder implementation
    Ok(false)
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
}
