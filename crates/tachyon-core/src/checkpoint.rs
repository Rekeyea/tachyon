//! Sidecar de checkpoint.
//!
//! v0 es el `SourceOffsets` pelado del pass-through. v1 es el estado de
//! ventanas, en el mismo path, visible solo si el snapshot de Paimon con ese
//! identifier existe. El sink decide cuál de los dos escribió.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{InputPosition, InputPositions, SourceOffsets};

/// Tope del archivo de sidecar. Por encima no se publica el rename.
pub const MAX_SIDECAR_BYTES: usize = 512 * 1024 * 1024;

/// Cuerpo que se escribe en `tachyon-offsets/<commit_user>/<id>.json`.
#[derive(Debug, Clone, PartialEq)]
pub enum CheckpointBody {
    /// JSON v0. Pass-through. El archivo es un `SourceOffsets` sin clave `v`.
    Offsets(SourceOffsets),
    /// JSON v1. Ventana.
    Window(WindowCheckpointV1),
    /// JSON v2. Join por intervalo.
    Join(JoinCheckpointV1),
    /// JSON v3. Posiciones por input (mezcla de fuentes: Kafka, Kinesis, SQS).
    /// Un pipeline solo-Kafka sigue escribiendo v0: v3 solo cuando hay al
    /// menos un input no-Kafka.
    Positions(InputPositions),
}

/// Sidecar v1. Un documento, un rename.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowCheckpointV1 {
    pub v: u32,
    /// Igual al identifier del snapshot con el que se commitea.
    pub commit_identifier: i64,
    /// `consumers_per_topic` con el que se armaron los `group.instance.id`.
    pub consumers_per_topic: u32,
    /// Registros que el operador ya terminó. `topic → partición → próximo offset`.
    pub applied: SourceOffsets,
    /// Watermark por partición. El flag de idle no se persiste.
    pub progress: BTreeMap<String, BTreeMap<i32, PartitionProgress>>,
    /// Watermark de instancia ya monótono. No retrocede en un restore.
    pub instance_watermark_ms: Option<i64>,
    pub spec: WindowSpecId,
    pub state: OperatorState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionProgress {
    pub max_event_time_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowSpecId {
    pub input: String,
    pub event_time: String,
    pub kind: WindowKind,
    pub size_ms: i64,
    pub slide_ms: Option<i64>,
    pub gap_ms: Option<i64>,
    /// Incluye la clave de partición. Vacío es error de arranque, no un parcial.
    pub group_columns: Vec<String>,
    pub partial: bool,
    pub aggs: Vec<AggSpec>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowKind {
    Tumble,
    Hop,
    Session,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggSpec {
    pub kind: AggKind,
    /// `None` en `COUNT(*)`.
    pub input: Option<String>,
    pub alias: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggKind {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperatorState {
    /// En el archivo es un array de `[hex, estado]`, no un objeto: `Vec<u8>`
    /// no es una clave JSON válida.
    #[serde(serialize_with = "ser_keys", deserialize_with = "de_keys")]
    pub keys: BTreeMap<Vec<u8>, KeyState>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyState {
    /// Partición de Kafka de la que salió la clave. Sirve para llevarse solo
    /// esas ventanas cuando la partición cambia de proceso. Un sidecar viejo
    /// no trae el campo: queda en 0.
    #[serde(default)]
    pub partition: i32,
    /// TUMBLE y HOP. Vacío en SESSION. La clave `i64` viaja como string decimal.
    pub windows: BTreeMap<i64, Accumulators>,
    /// SESSION. Vacío en TUMBLE/HOP. Ordenado por `start_ms`.
    pub sessions: Vec<SessionState>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionState {
    pub start_ms: i64,
    pub end_ms: i64,
    pub acc: Accumulators,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Accumulators {
    pub slots: Vec<AggState>,
    /// Paralelo a `slots`. `false` = todavía no llegó un valor no-null
    /// (`SUM`/`MIN`/`MAX` salen null). Vacío en un sidecar viejo: se trata
    /// como todo presente.
    #[serde(default)]
    pub present: Vec<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggState {
    Count(i64),
    /// String decimal: un número JSON por encima de 2^53 no es exacto.
    SumI64(#[serde(with = "i128_string")] i128),
    SumF64(f64),
    MinI64(i64),
    MinF64(f64),
    MaxI64(i64),
    MaxF64(f64),
    Avg { sum: f64, count: i64 },
}

mod i128_string {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &i128, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&v.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<i128, D::Error> {
        let text = String::deserialize(d)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

fn ser_keys<S: serde::Serializer>(
    keys: &BTreeMap<Vec<u8>, KeyState>,
    s: S,
) -> Result<S::Ok, S::Error> {
    let pairs: Vec<(String, &KeyState)> = keys
        .iter()
        .map(|(k, v)| (hex_encode(k), v))
        .collect();
    pairs.serialize(s)
}

fn de_keys<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<BTreeMap<Vec<u8>, KeyState>, D::Error> {
    let pairs = Vec::<(String, KeyState)>::deserialize(d)?;
    let mut keys = BTreeMap::new();
    for (hex, state) in pairs {
        let bytes = hex_decode(&hex).map_err(serde::de::Error::custom)?;
        keys.insert(bytes, state);
    }
    Ok(keys)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
    out
}

fn hex_decode(text: &str) -> Result<Vec<u8>, String> {
    if text.len() % 2 != 0 {
        return Err(format!("hex de longitud impar: {text}"));
    }
    let mut out = Vec::with_capacity(text.len() / 2);
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_nibble(bytes[i])?;
        let lo = hex_nibble(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

fn hex_nibble(b: u8) -> Result<u8, String> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        _ => Err(format!("nibble hex inválido: {}", b as char)),
    }
}

/// Sidecar v2. Los eventos que todavía pueden matchear, más los offsets aplicados.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinCheckpointV1 {
    pub v: u32,
    pub commit_identifier: i64,
    pub applied: SourceOffsets,
    pub spec: JoinSpec,
    /// `topic → partición → max event time` ya visto. El watermark sale de acá.
    pub progress: BTreeMap<String, BTreeMap<i32, i64>>,
    pub state: JoinState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinSpec {
    pub left: String,
    pub right: String,
    pub left_time: String,
    pub right_time: String,
    pub lower_ms: i64,
    pub upper_ms: i64,
    /// El tiempo de la izquierda es la base del `BETWEEN`.
    pub base_is_left: bool,
    pub columns: Vec<JoinColumnSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinColumnSpec {
    pub side_left: bool,
    pub column: String,
    pub alias: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinState {
    #[serde(serialize_with = "ser_join_keys", deserialize_with = "de_join_keys")]
    pub keys: BTreeMap<Vec<u8>, JoinKeyState>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct JoinKeyState {
    pub left: Vec<JoinEvent>,
    pub right: Vec<JoinEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinEvent {
    pub time_ms: i64,
    pub values: Vec<JoinCell>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JoinCell {
    I64(i64),
    F64(f64),
    Text(String),
    Null,
}

fn ser_join_keys<S: serde::Serializer>(
    keys: &BTreeMap<Vec<u8>, JoinKeyState>,
    s: S,
) -> Result<S::Ok, S::Error> {
    let pairs: Vec<(String, &JoinKeyState)> = keys
        .iter()
        .map(|(k, v)| (hex_encode(k), v))
        .collect();
    pairs.serialize(s)
}

fn de_join_keys<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<BTreeMap<Vec<u8>, JoinKeyState>, D::Error> {
    let pairs = Vec::<(String, JoinKeyState)>::deserialize(d)?;
    let mut keys = BTreeMap::new();
    for (hex, state) in pairs {
        let bytes = hex_decode(&hex).map_err(serde::de::Error::custom)?;
        keys.insert(bytes, state);
    }
    Ok(keys)
}

/// Distingue v0, v1, v2 y v3. Otra forma, con el snapshot presente, es corrupción.
pub fn parse_checkpoint(bytes: &[u8]) -> Result<CheckpointBody, String> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|e| format!("sidecar no es JSON: {e}"))?;
    if let Some(version) = value.get("v").and_then(|v| v.as_u64()) {
        if version == 3 {
            let positions = value
                .get("positions")
                .cloned()
                .ok_or_else(|| "sidecar v3 sin clave 'positions'".to_string())?;
            let positions: InputPositions =
                serde_json::from_value(positions).map_err(|e| format!("sidecar v3 corrupto: {e}"))?;
            return Ok(CheckpointBody::Positions(positions));
        }
        if version == 2 {
            let checkpoint: JoinCheckpointV1 = serde_json::from_value(value)
                .map_err(|e| format!("sidecar v2 corrupto: {e}"))?;
            if checkpoint.v != 2 {
                return Err(format!("versión de sidecar desconocida: {}", checkpoint.v));
            }
            return Ok(CheckpointBody::Join(checkpoint));
        }
        if version != 1 {
            return Err(format!("versión de sidecar desconocida: {version}"));
        }
        let checkpoint: WindowCheckpointV1 = serde_json::from_value(value)
            .map_err(|e| format!("sidecar v1 corrupto: {e}"))?;
        if checkpoint.v != 1 {
            return Err(format!("versión de sidecar desconocida: {}", checkpoint.v));
        }
        return Ok(CheckpointBody::Window(checkpoint));
    }
    let offsets: SourceOffsets = serde_json::from_value(value)
        .map_err(|e| format!("offsets del checkpoint corruptos: {e}"))?;
    Ok(CheckpointBody::Offsets(offsets))
}

/// Ficha de una partición, visible para cualquier `commit_user`. Se escribe
/// después del snapshot. Sin snapshot no hay ficha nueva: queda la anterior.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionTicketV1 {
    pub v: u32,
    pub topic: String,
    pub partition: i32,
    pub commit_user: String,
    pub snapshot_id: i64,
    pub applied_offset: i64,
    pub max_event_time_ms: Option<i64>,
    pub spec: WindowSpecId,
    #[serde(serialize_with = "ser_keys", deserialize_with = "de_keys")]
    pub keys: BTreeMap<Vec<u8>, KeyState>,
}

impl PartitionTicketV1 {
    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        serde_json::to_vec(self).map_err(|e| e.to_string())
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        let ticket: Self =
            serde_json::from_slice(bytes).map_err(|e| format!("ficha de partición corrupta: {e}"))?;
        if ticket.v != 1 {
            return Err(format!("versión de ficha desconocida: {}", ticket.v));
        }
        Ok(ticket)
    }
}

/// Una ficha por partición presente en `applied`. Las claves de otras
/// particiones no entran. Una partición sin ventanas abiertas igual publica
/// su offset, para no releer lo ya cerrado.
pub fn partition_tickets(
    checkpoint: &WindowCheckpointV1,
    commit_user: &str,
) -> Vec<PartitionTicketV1> {
    let mut tickets = Vec::new();
    for (topic, parts) in &checkpoint.applied {
        for (&partition, &applied_offset) in parts {
            let keys = checkpoint
                .state
                .keys
                .iter()
                .filter(|(_, state)| state.partition == partition)
                .map(|(key, state)| (key.clone(), state.clone()))
                .collect();
            let max_event_time_ms = checkpoint
                .progress
                .get(topic)
                .and_then(|parts| parts.get(&partition))
                .and_then(|progress| progress.max_event_time_ms);
            tickets.push(PartitionTicketV1 {
                v: 1,
                topic: topic.clone(),
                partition,
                commit_user: commit_user.to_string(),
                snapshot_id: checkpoint.commit_identifier,
                applied_offset,
                max_event_time_ms,
                spec: checkpoint.spec.clone(),
                keys,
            });
        }
    }
    tickets
}

impl CheckpointBody {
    /// Bytes que van al archivo. v0 no agrega claves. v1 estampa `v: 1`.
    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        match self {
            CheckpointBody::Offsets(offsets) => {
                serde_json::to_vec(offsets).map_err(|e| e.to_string())
            }
            CheckpointBody::Window(checkpoint) => {
                let mut owned = checkpoint.clone();
                owned.v = 1;
                serde_json::to_vec(&owned).map_err(|e| e.to_string())
            }
            CheckpointBody::Join(checkpoint) => {
                let mut owned = checkpoint.clone();
                owned.v = 2;
                serde_json::to_vec(&owned).map_err(|e| e.to_string())
            }
            CheckpointBody::Positions(positions) => {
                let doc = serde_json::json!({ "v": 3, "positions": positions });
                serde_json::to_vec(&doc).map_err(|e| e.to_string())
            }
        }
    }

    /// Offsets Kafka que cubre el checkpoint (el commit informativo en el
    /// broker y el resume de las fuentes de topic). En v3 salen solo las
    /// entradas `Kafka`; el resto de inputs no tiene offsets.
    pub fn applied_offsets(&self) -> SourceOffsets {
        match self {
            CheckpointBody::Offsets(offsets) => offsets.clone(),
            CheckpointBody::Window(checkpoint) => checkpoint.applied.clone(),
            CheckpointBody::Join(checkpoint) => checkpoint.applied.clone(),
            CheckpointBody::Positions(positions) => positions
                .iter()
                .filter_map(|(input, position)| match position {
                    InputPosition::Kafka(offsets) => Some((input.clone(), offsets.clone())),
                    _ => None,
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge_positions;

    fn sample_window() -> WindowCheckpointV1 {
        let mut key = Vec::new();
        key.extend_from_slice(&7i64.to_le_bytes());
        key.extend_from_slice(&1u32.to_le_bytes());
        key.push(0); // NUL dentro del utf8
        let mut keys = BTreeMap::new();
        keys.insert(
            key,
            KeyState {
                partition: 0,
                windows: BTreeMap::from([(
                    60_000,
                    Accumulators {
                        slots: vec![AggState::SumI64(1i128 << 60)],
                        present: vec![true],
                    },
                )]),
                sessions: vec![],
            },
        );
        WindowCheckpointV1 {
            v: 1,
            commit_identifier: 4,
            consumers_per_topic: 2,
            applied: BTreeMap::from([("orders".into(), BTreeMap::from([(0, 10), (1, 3)]))]),
            progress: BTreeMap::from([(
                "orders".into(),
                BTreeMap::from([(
                    0,
                    PartitionProgress {
                        max_event_time_ms: Some(70_000),
                    },
                )]),
            )]),
            instance_watermark_ms: Some(65_000),
            spec: WindowSpecId {
                input: "orders".into(),
                event_time: "event_time".into(),
                kind: WindowKind::Tumble,
                size_ms: 60_000,
                slide_ms: None,
                gap_ms: None,
                group_columns: vec!["order_id".into()],
                partial: false,
                aggs: vec![AggSpec {
                    kind: AggKind::Sum,
                    input: Some("amount".into()),
                    alias: "amount".into(),
                }],
            },
            state: OperatorState { keys },
        }
    }

    #[test]
    fn v0_stays_a_bare_offset_map() {
        let offsets = BTreeMap::from([("orders".into(), BTreeMap::from([(0, 3i64)]))]);
        let bytes = CheckpointBody::Offsets(offsets.clone())
            .to_bytes()
            .unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(!text.contains("\"v\""), "{text}");
        match parse_checkpoint(&bytes).unwrap() {
            CheckpointBody::Offsets(got) => assert_eq!(got, offsets),
            CheckpointBody::Window(_) => panic!("v0 leído como ventana"),
            CheckpointBody::Join(_) => panic!("v0 leído como join"),
            CheckpointBody::Positions(_) => panic!("v0 leído como positions"),
        }
    }

    #[test]
    fn a_ticket_keeps_only_the_keys_of_its_partition() {
        let checkpoint = sample_window();
        let tickets = partition_tickets(&checkpoint, "commit-a");
        assert_eq!(tickets.len(), 2);
        let p0 = tickets.iter().find(|t| t.partition == 0).unwrap();
        assert_eq!(p0.applied_offset, 10);
        assert_eq!(p0.keys.len(), 1);
        assert_eq!(p0.max_event_time_ms, Some(70_000));
        let p1 = tickets.iter().find(|t| t.partition == 1).unwrap();
        assert!(p1.keys.is_empty());
        assert_eq!(p1.applied_offset, 3);
        let bytes = p0.to_bytes().unwrap();
        assert_eq!(PartitionTicketV1::from_bytes(&bytes).unwrap(), *p0);
    }

    #[test]
    fn v1_roundtrips_nul_and_i128_past_2_pow_53() {
        let checkpoint = sample_window();
        let bytes = CheckpointBody::Window(checkpoint.clone())
            .to_bytes()
            .unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.contains("\"SumI64\":\"1152921504606846976\""), "{text}");
        match parse_checkpoint(&bytes).unwrap() {
            CheckpointBody::Window(got) => assert_eq!(got, checkpoint),
            CheckpointBody::Offsets(_) => panic!("v1 leído como offsets"),
            CheckpointBody::Join(_) => panic!("v1 leído como join"),
            CheckpointBody::Positions(_) => panic!("v1 leído como positions"),
        }
    }

    #[test]
    fn unknown_version_is_rejected() {
        let err = parse_checkpoint(br#"{"v":4}"#).unwrap_err();
        assert!(err.contains("desconocida"), "{err}");
    }

    #[test]
    fn v3_roundtrips_mixed_positions() {
        let mut positions = InputPositions::new();
        positions.insert(
            "orders".into(),
            InputPosition::Kafka(BTreeMap::from([(0, 10i64), (1, 3)])),
        );
        positions.insert(
            "streams".into(),
            InputPosition::Kinesis(BTreeMap::from([(
                "shardId-00000001699999999999999999999999999999999999999999999999".into(),
                "4959030000000000000000000000000000000000000000000000000000".into(),
            )])),
        );
        positions.insert("clicks".into(), InputPosition::Sqs);
        let body = CheckpointBody::Positions(positions.clone());
        let bytes = body.to_bytes().unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.contains("\"v\":3"), "{text}");
        match parse_checkpoint(&bytes).unwrap() {
            CheckpointBody::Positions(got) => assert_eq!(got, positions),
            other => panic!("v3 leído como otra cosa: {other:?}"),
        }
    }

    #[test]
    fn v3_applied_offsets_keeps_only_kafka_entries() {
        let mut positions = InputPositions::new();
        positions.insert(
            "orders".into(),
            InputPosition::Kafka(BTreeMap::from([(0, 7i64)])),
        );
        positions.insert(
            "streams".into(),
            InputPosition::Kinesis(BTreeMap::from([("shardId-000".into(), "1".into())])),
        );
        positions.insert("clicks".into(), InputPosition::Sqs);
        let offsets = CheckpointBody::Positions(positions).applied_offsets();
        assert_eq!(
            offsets,
            BTreeMap::from([("orders".into(), BTreeMap::from([(0, 7i64)]))])
        );
    }

    #[test]
    fn v3_requires_the_positions_key() {
        let err = parse_checkpoint(br#"{"v":3}"#).unwrap_err();
        assert!(err.contains("positions"), "{err}");
    }

    #[test]
    fn merge_positions_advances_per_input() {
        let mut positions = InputPositions::new();
        positions.insert("orders".into(), InputPosition::Kafka(BTreeMap::from([(0, 5i64)])));
        positions.insert(
            "streams".into(),
            InputPosition::Kinesis(BTreeMap::from([("shardId-000".into(), "100".into())])),
        );
        positions.insert("clicks".into(), InputPosition::Sqs);

        let mut update = InputPositions::new();
        update.insert("orders".into(), InputPosition::Kafka(BTreeMap::from([(0, 3i64), (1, 9)])));
        update.insert(
            "streams".into(),
            InputPosition::Kinesis(BTreeMap::from([("shardId-000".into(), "099".into())])),
        );
        merge_positions(&mut positions, &update);

        assert_eq!(
            positions.get("orders"),
            Some(&InputPosition::Kafka(BTreeMap::from([(0, 5i64), (1, 9)]))),
            "el offset nunca retrocede; la partición nueva entra"
        );
        assert_eq!(
            positions.get("streams"),
            Some(&InputPosition::Kinesis(BTreeMap::from([("shardId-000".into(), "100".into())]))),
            "el seq Kinesis nunca retrocede"
        );
        assert_eq!(positions.get("clicks"), Some(&InputPosition::Sqs));
    }
}
