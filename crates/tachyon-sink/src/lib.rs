//! `tachyon-sink`: conector sink Paimon.
//!
//! Writer por bucket con sequence numbers y commit (ver DESIGN.md §7).

pub mod compact;
pub mod dimension;
pub mod redpanda;
pub mod shard;
pub mod writer;
