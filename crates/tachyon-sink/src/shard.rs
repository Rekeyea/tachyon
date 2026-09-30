//! Escritura en paralelo por bucket.
//!
//! Un `TableWrite` hace todo en el task que lo llama: rutear cada fila a su
//! bucket, acumularla y, en el `prepare_commit`, ordenar por primary key y
//! codificar parquet de todos los buckets, uno detrás de otro. Con un solo
//! writer, esas dos etapas eran el techo de un pipeline: un core cada una.
//!
//! Acá los buckets se reparten entre N shards (`bucket % N`). Cada shard
//! tiene su `TableWrite` en un hilo propio, así que la escritura corre en
//! paralelo, y al cerrar el epoch los N `prepare_commit` también. Los
//! mensajes de los N van al mismo snapshot bajo el mismo `commit_user`, igual
//! que los N writers de un job de Flink con un committer.
//!
//! El bucket de cada fila se calcula como Paimon: murmur3 sobre el
//! `BinaryRow` de la bucket-key (`bucket-key`, o la primary key). Un shard
//! que recibe una fila de un bucket ajeno escribiría ese bucket desde dos
//! writers (sequence numbers pisados), así que `EpochWrite::prepare_commit`
//! verifica que cada mensaje sea de un bucket del shard y, si no, falla
//! antes de publicar.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Array, Int64Array, RecordBatch, UInt32Array};
use paimon::spec::{BinaryRow, DataField, DataType as PaimonType};
use paimon::table::{CommitMessage, Table, TableWrite};

/// Cuántos batches puede tener en cola cada shard.
const SHARD_QUEUE: usize = 4;

/// A qué shard va cada fila.
#[derive(Clone)]
pub struct BucketRouter {
    buckets: i32,
    shards: usize,
    key_indices: Vec<usize>,
    fields: Arc<Vec<DataField>>,
    /// La bucket-key es una sola columna BIGINT: hash sin armar el BinaryRow.
    single_i64: Option<usize>,
}

impl BucketRouter {
    /// `None` si la tabla no se puede repartir: un bucket, buckets dinámicos
    /// o una función de bucket que no es la default de Paimon.
    pub fn for_table(table: &Table, shards: usize) -> Option<Self> {
        let schema = table.schema();
        let options = schema.options();
        let buckets: i32 = options.get("bucket")?.trim().parse().ok()?;
        if buckets < 2 || shards < 2 {
            return None;
        }
        if options
            .get("bucket-function.type")
            .is_some_and(|kind| !kind.eq_ignore_ascii_case("default"))
        {
            return None;
        }
        let fields = schema.fields().to_vec();
        let key_indices: Option<Vec<usize>> = schema
            .bucket_keys()
            .iter()
            .map(|name| fields.iter().position(|f| f.name() == name))
            .collect();
        let key_indices = key_indices.filter(|keys| !keys.is_empty())?;
        let single_i64 = match key_indices.as_slice() {
            [only] if matches!(fields[*only].data_type(), PaimonType::BigInt(_)) => Some(*only),
            _ => None,
        };
        Some(Self {
            buckets,
            shards: shards.min(buckets as usize),
            key_indices,
            fields: Arc::new(fields),
            single_i64,
        })
    }

    pub fn shards(&self) -> usize {
        self.shards
    }

    fn shard_of_bucket(&self, bucket: i32) -> usize {
        bucket as usize % self.shards
    }

    /// El bucket de Paimon de cada fila del batch.
    pub fn buckets_of(&self, batch: &RecordBatch) -> Result<Vec<i32>> {
        if let Some(index) = self.single_i64 {
            if let Some(keys) = batch.column(index).as_any().downcast_ref::<Int64Array>() {
                if keys.null_count() == 0 {
                    return Ok(keys
                        .values()
                        .iter()
                        .map(|&key| bucket_of_hash(hash_single_long(key), self.buckets))
                        .collect());
                }
            }
        }
        (0..batch.num_rows())
            .map(|row| {
                let key = BinaryRow::from_arrow(batch, row, &self.key_indices, &self.fields)
                    .context("armando la bucket-key")?;
                Ok(bucket_of_hash(key.hash_code(), self.buckets))
            })
            .collect()
    }

    /// Un sub-batch por shard (`None` si al shard no le toca nada).
    pub fn split(&self, batch: &RecordBatch) -> Result<Vec<Option<RecordBatch>>> {
        let buckets = self.buckets_of(batch)?;
        let mut rows: Vec<Vec<u32>> = vec![Vec::new(); self.shards];
        for (row, bucket) in buckets.iter().enumerate() {
            rows[self.shard_of_bucket(*bucket)].push(row as u32);
        }
        rows.into_iter()
            .map(|indices| {
                if indices.is_empty() {
                    return Ok(None);
                }
                if indices.len() == batch.num_rows() {
                    return Ok(Some(batch.clone()));
                }
                let indices = UInt32Array::from(indices);
                let columns = batch
                    .columns()
                    .iter()
                    .map(|column| arrow::compute::take(column, &indices, None))
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .context("repartiendo el batch por bucket")?;
                Ok(Some(
                    RecordBatch::try_new(batch.schema(), columns)
                        .context("armando el sub-batch de un shard")?,
                ))
            })
            .collect()
    }
}

/// `(hash % buckets).abs()`, la función default de Paimon.
fn bucket_of_hash(hash: i32, buckets: i32) -> i32 {
    (hash % buckets).wrapping_abs()
}

/// Hash de un `BinaryRow` de un solo campo BIGINT no nulo: 8 bytes de header
/// y bitmap de nulos en cero, y el valor little-endian. Es
/// `MurmurHashUtils.hashBytesByWords` de Paimon (murmur3_32, seed 42).
fn hash_single_long(value: i64) -> i32 {
    const C1: u32 = 0xcc9e2d51;
    const C2: u32 = 0x1b873593;
    fn mix(h1: u32, word: u32) -> u32 {
        let k1 = word.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
        (h1 ^ k1).rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64)
    }
    let bits = value as u64;
    let mut h1 = 42u32;
    h1 = mix(h1, 0);
    h1 = mix(h1, 0);
    h1 = mix(h1, bits as u32);
    h1 = mix(h1, (bits >> 32) as u32);
    let mut h = h1 ^ 16;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^= h >> 16;
    h as i32
}

/// Los writers de un epoch cerrado, uno por shard (o uno solo).
pub struct EpochWrite {
    writers: Vec<TableWrite>,
    /// `Some(n)` si los writers son shards de `bucket % n`.
    shards: Option<usize>,
}

impl EpochWrite {
    pub fn single(writer: TableWrite) -> Self {
        Self {
            writers: vec![writer],
            shards: None,
        }
    }

    /// Flush de todos los writers (en paralelo si son shards) y los mensajes
    /// de commit juntos. Cada mensaje tiene que ser de un bucket del shard.
    pub async fn prepare_commit(self) -> Result<Vec<CommitMessage>> {
        let Some(shards) = self.shards else {
            let mut messages = Vec::new();
            for mut writer in self.writers {
                messages.extend(writer.prepare_commit().await.context("prepare_commit")?);
            }
            return Ok(messages);
        };
        let handle = tokio::runtime::Handle::current();
        let tasks: Vec<_> = self
            .writers
            .into_iter()
            .map(|mut writer| {
                let handle = handle.clone();
                // Ordenar y codificar parquet es CPU: va al pool de blocking
                // para que los N corran a la vez sin frenar a los workers.
                tokio::task::spawn_blocking(move || {
                    handle.block_on(async move { writer.prepare_commit().await })
                })
            })
            .collect();
        let mut messages = Vec::new();
        for (shard, task) in tasks.into_iter().enumerate() {
            let shard_messages = task
                .await
                .context("el prepare de un shard terminó con error")?
                .with_context(|| format!("prepare_commit del shard {shard}"))?;
            for message in &shard_messages {
                if message.bucket as usize % shards != shard {
                    anyhow::bail!(
                        "el shard {shard} de {shards} escribió el bucket {}: el ruteo no coincide con el de Paimon",
                        message.bucket
                    );
                }
            }
            messages.extend(shard_messages);
        }
        Ok(messages)
    }
}

pub(crate) enum ShardMsg {
    Batch(RecordBatch),
    Rotate(tokio::sync::oneshot::Sender<TableWrite>),
}

/// Un shard vivo: su cola y el hilo que lo escribe.
pub(crate) struct Shard {
    pub(crate) tx: tokio::sync::mpsc::Sender<ShardMsg>,
    pub(crate) done: tokio::task::JoinHandle<Result<()>>,
}

/// Arranca un shard en un hilo del pool de blocking, con un `TableWrite`
/// propio bajo el `commit_user` del sink.
pub(crate) fn spawn_shard(
    table: Table,
    commit_user: String,
    rowkind: Option<(String, usize, crate::writer::RowkindWrite)>,
) -> Result<Shard> {
    let fresh = move || -> Result<TableWrite> {
        table
            .new_write_builder()
            .with_commit_user(&commit_user)
            .context("commit_user inválido")?
            .new_write()
            .context("new_write")
    };
    let mut active = fresh()?;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<ShardMsg>(SHARD_QUEUE);
    let handle = tokio::runtime::Handle::current();
    let done = tokio::task::spawn_blocking(move || {
        handle.block_on(async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    ShardMsg::Batch(batch) => {
                        let prepared = crate::writer::prepare_batch(&batch, &rowkind)?;
                        active
                            .write_arrow_batch(&prepared)
                            .await
                            .context("write_arrow_batch")?;
                    }
                    ShardMsg::Rotate(reply) => {
                        let full = std::mem::replace(&mut active, fresh()?);
                        let _ = reply.send(full);
                    }
                }
            }
            Ok(())
        })
    });
    Ok(Shard { tx, done })
}

/// Junta los writers rotados de los shards en un epoch.
pub(crate) fn sharded_epoch(writers: Vec<TableWrite>) -> EpochWrite {
    let shards = writers.len();
    EpochWrite {
        writers,
        shards: Some(shards),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn single_long_hash_matches_binary_row() {
        let field = DataField::new(0, "k".to_string(), PaimonType::BigInt(paimon::spec::BigIntType::new()));
        let fields = vec![field];
        let values: Vec<i64> = vec![
            0, 1, -1, 7, 42, 1_000_003, -987_654_321, i64::MAX, i64::MIN, 1 << 40, 123_456_789_012,
        ];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(values.clone()))],
        )
        .unwrap();
        for (row, value) in values.iter().enumerate() {
            let reference = BinaryRow::from_arrow(&batch, row, &[0], &fields).unwrap();
            assert_eq!(hash_single_long(*value), reference.hash_code(), "valor {value}");
        }
        for value in (-50_000i64..50_000).step_by(7) {
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)])),
                vec![Arc::new(Int64Array::from(vec![value]))],
            )
            .unwrap();
            let reference = BinaryRow::from_arrow(&batch, 0, &[0], &fields).unwrap();
            assert_eq!(hash_single_long(value), reference.hash_code(), "valor {value}");
        }
    }

    #[test]
    fn negative_hashes_land_in_range() {
        for hash in [i32::MIN, -1, -7, 0, 7, i32::MAX] {
            let bucket = bucket_of_hash(hash, 8);
            assert!((0..8).contains(&bucket), "{hash} -> {bucket}");
        }
    }
}
