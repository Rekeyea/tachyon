//! Tipos base de Tachyon.

use serde::{Deserialize, Serialize};

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

/// La posición de un input en el checkpoint (la unidad de progreso del
/// exactly-once, por tipo de fuente).
///
/// - **Kafka/Redpanda:** `topic -> partición -> próximo offset` (idéntico al
///   `SourceOffsets` de un solo input; el sidecar v0 lo persiste así).
/// - **Kinesis:** `shard id -> último sequence number consumido`. Al
///   recuperar, el iterator arranca en `AFTER_SEQUENCE_NUMBER` de ese seq.
/// - **SQS:** sin posición persistente. La garantía es at-least-once: los
///   mensajes se borran después del commit y los duplicados los deduplica el
///   sink (PK + `sequence.field`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InputPosition {
    /// topic -> partición -> próximo offset (Kafka/Redpanda).
    Kafka(std::collections::BTreeMap<i32, i64>),
    /// shard id -> último sequence number consumido (Kinesis).
    Kinesis(std::collections::BTreeMap<String, String>),
    /// Sin posición persistente (SQS).
    Sqs,
}

/// Nombre lógico del input -> posición. Es el cuerpo del sidecar v3
/// (ver `CheckpointBody::Positions`).
pub type InputPositions = std::collections::BTreeMap<String, InputPosition>;

/// Compara dos sequence numbers de Kinesis.
///
/// Son números de 128 bits codificados en grupos de 6 dígitos cero-llenados:
/// misma longitud fija, así que la comparación lexicográfica es la numérica.
pub fn kinesis_seq_le(a: &str, b: &str) -> bool {
    a.len() < b.len() || (a.len() == b.len() && a <= b)
}

/// Fusiona `update` en `positions` (el progreso nunca retrocede): Kafka =
/// máximo por partición, Kinesis = máximo por shard, SQS = noop.
pub fn merge_positions(positions: &mut InputPositions, update: &InputPositions) {
    for (input, incoming) in update {
        let entry = positions.entry(input.clone()).or_insert_with(InputPosition::default_sqs);
        match (entry, incoming) {
            (InputPosition::Kafka(current), InputPosition::Kafka(next)) => {
                for (partition, &offset) in next {
                    let slot = current.entry(*partition).or_insert(offset);
                    *slot = (*slot).max(offset);
                }
            }
            (InputPosition::Kinesis(current), InputPosition::Kinesis(next)) => {
                for (shard, seq) in next {
                    let slot = current.entry(shard.clone()).or_insert_with(|| seq.clone());
                    if !kinesis_seq_le(seq, slot) {
                        *slot = seq.clone();
                    }
                }
            }
            _ => {}
        }
    }
}

/// Extrae del body v3 solo las posiciones Kafka (topic -> partición -> offset),
/// en la forma que busca el consumidor.
pub fn kafka_offsets(positions: &InputPositions) -> SourceOffsets {
    positions
        .iter()
        .filter_map(|(input, position)| match position {
            InputPosition::Kafka(offsets) => Some((input.clone(), offsets.clone())),
            _ => None,
        })
        .collect()
}

impl InputPosition {
    /// Posición vacía de un input SQS (la única que se produce).
    fn default_sqs() -> Self {
        InputPosition::Sqs
    }
}
