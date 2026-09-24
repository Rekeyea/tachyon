//! Tipos base de Tachyon.

/// Una columna de un stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// Nombre de la columna.
    pub name: String,
    /// Tipo de dato (simplificado; se mapea a `arrow::datatypes::DataType`).
    pub data_type: String,
}

/// Un stream lógico de entrada (vinculado a un topic físico en la config).
#[derive(Debug, Clone)]
pub struct StreamDef {
    /// Nombre lógico referenciado en la SQL.
    pub name: String,
    /// Columnas del stream.
    pub columns: Vec<Column>,
}

/// La clave de particionado: la única clave `K` a través de Redpanda, el
/// estado, la fusión y los buckets de Paimon (ver DESIGN.md §5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionKey {
    /// Columna que actúa como clave de particionado.
    pub column: String,
}
