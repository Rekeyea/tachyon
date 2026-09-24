//! La unidad de dato cruda que emite la fuente, **independiente del broker**.
//!
//! Esto decoupla el consumidor rdkafka (que depende de un broker) del resto del
//! conector (decoder + `PartitionStream`), que se puede testear con una fuente
//! in-memory que produce los mismos `SourceRecord`.

/// Un registro crudo de la fuente, antes de decodificar.
#[derive(Debug, Clone)]
pub struct SourceRecord {
    /// Partición de origen.
    pub partition: i32,
    /// Offset en la partición.
    pub offset: i64,
    /// Clave de particionado (opcional).
    pub key: Option<Vec<u8>>,
    /// El payload a decodificar (JSON o Avro).
    pub value: Vec<u8>,
}

impl SourceRecord {
    /// Construye un registro (útil en tests y en el consumidor).
    pub fn new(partition: i32, offset: i64, key: Option<Vec<u8>>, value: Vec<u8>) -> Self {
        Self {
            partition,
            offset,
            key,
            value,
        }
    }
}
