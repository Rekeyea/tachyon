//! Conversor ligero de esquemas Arrow a un esquema base común.
//!
//! Permite que UNION ALL o JOIN consuman fuentes con esquemas distintos pero
//! compatibles, mapeando columnas y coerciendo tipos sin escribir código.

mod coerce;
mod mapping;

pub use coerce::coerce_array;
pub use mapping::{ColumnMapping, SchemaConverter, SchemaMapping};
