//! Sidecar de checkpoint.
//!
//! v0 es el `SourceOffsets` pelado del pass-through. v1 es el estado de
//! ventanas, en el mismo path, visible solo si el snapshot de Paimon con ese
//! identifier existe. El sink decide cuál de los dos escribió.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::SourceOffsets;

/// Tope del archivo de sidecar. Por encima no se publica el rename.
pub const MAX_SIDECAR_BYTES: usize = 512 * 1024 * 1024;

/// Cuerpo que se escribe en `tachyon-offsets/<commit_user>/<id>.json`.
#[derive(Debug, Clone, PartialEq)]
pub enum CheckpointBody {
    /// JSON v0. Pass-through. El archivo es un `SourceOffsets` sin clave `v`.
    Offsets(SourceOffsets),
    /// JSON v1. Ventana.
    Window(WindowCheckpointV1),
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

/// Distingue v0 de v1. Otra forma, con el snapshot presente, es corrupción.
pub fn parse_checkpoint(bytes: &[u8]) -> Result<CheckpointBody, String> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|e| format!("sidecar no es JSON: {e}"))?;
    if let Some(version) = value.get("v").and_then(|v| v.as_u64()) {
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
        }
    }

    pub fn applied_offsets(&self) -> &SourceOffsets {
        match self {
            CheckpointBody::Offsets(offsets) => offsets,
            CheckpointBody::Window(checkpoint) => &checkpoint.applied,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_window() -> WindowCheckpointV1 {
        let mut key = Vec::new();
        key.extend_from_slice(&7i64.to_le_bytes());
        key.extend_from_slice(&1u32.to_le_bytes());
        key.push(0); // NUL dentro del utf8
        let mut keys = BTreeMap::new();
        keys.insert(
            key,
            KeyState {
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
        }
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
        }
    }

    #[test]
    fn unknown_version_is_rejected() {
        let err = parse_checkpoint(br#"{"v":2}"#).unwrap_err();
        assert!(err.contains("desconocida"), "{err}");
    }
}
