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

/// Offsets de fuente de un checkpoint: `topic -> partición -> próximo offset a
/// consumir` (el offset del último registro incluido + 1, convención Kafka).
///
/// Es la unidad de progreso del exactly-once: se persiste atómicamente junto
/// al commit de Paimon (ver `tachyon-sink::writer::PaimonSink::commit_checkpoint`)
/// y al recuperar se re-posiciona el consumo exactamente ahí.
pub type SourceOffsets =
    std::collections::BTreeMap<String, std::collections::BTreeMap<i32, i64>>;
