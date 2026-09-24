//! Errores de Tachyon.

use thiserror::Error;

/// Error base de Tachyon.
#[derive(Debug, Error)]
pub enum Error {
    #[error("invariante de alineación roto: partitions ({expected}) != buckets ({found})")]
    Alignment { expected: usize, found: usize },

    #[error("claves de particionado no alineadas: {0}")]
    KeyMismatch(String),

    #[error("config inválida: {0}")]
    Config(String),

    #[error("SQL inválida: {0}")]
    Sql(String),
}
