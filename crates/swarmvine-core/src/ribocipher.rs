//! riboCipher transport signal constants (Wave 111).
//!
//! Defines the first-byte signal values used to identify the transport
//! security tier of an incoming connection. All primals use these constants
//! for consistent wire format handling.

/// Clear-channel signal — same-gate UDS, JSON-RPC payload follows.
pub const SIGNAL_CLEAR: u8 = 0xEC;

/// Mito-obfuscated signal — cross-gate traffic via songBird.
pub const SIGNAL_MITO: u8 = 0xED;

/// Nuclear-sealed signal — high-security envelope.
pub const SIGNAL_NUCLEAR: u8 = 0xEE;

/// riboCipher clear-channel prefix for JSON-RPC frames.
/// Two bytes: `[SIGNAL_CLEAR, PROTOCOL_JSONRPC]`.
pub const CLEAR_JSONRPC: [u8; 2] = [SIGNAL_CLEAR, PROTOCOL_JSONRPC];

/// Protocol type byte: JSON-RPC.
pub const PROTOCOL_JSONRPC: u8 = 0x01;

/// Protocol type byte: health probe.
pub const PROTOCOL_HEALTH_PROBE: u8 = 0x00;
