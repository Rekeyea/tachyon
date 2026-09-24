//! `tachyon-sql`: parsing de `pipeline.sql`.
//!
//! Extrae la query de transformación y (en fases posteriores) las extensiones
//! de streaming (watermarks, windowing, state TTL). En el MVP solo se extrae
//! la query stateless.

pub mod parse;

pub use parse::parse_sql;
