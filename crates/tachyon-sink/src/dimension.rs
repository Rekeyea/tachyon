//! Dimensión de Paimon en memoria.
//!
//! El mapa es el snapshot de ahora. Se reconstruye entre batches cuando el id
//! cambia y no se guarda en el checkpoint: una fila ya escrita no se reescribe.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array, UInt32Builder,
};
use arrow::compute::take;
use arrow::datatypes::{Field, SchemaRef};
use futures::TryStreamExt;

use crate::writer::{open_table, projection_schema};

/// Clave de probe. El tipo es el de la columna, así que no se mezclan.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum LookupKey {
    I64(i64),
    I32(i32),
    Text(String),
}

impl LookupKey {
    fn show(&self) -> String {
        match self {
            LookupKey::I64(value) => value.to_string(),
            LookupKey::I32(value) => value.to_string(),
            LookupKey::Text(value) => value.clone(),
        }
    }
}

#[derive(Debug)]
struct IndexedDimension {
    index: HashMap<LookupKey, u32>,
    batch: RecordBatch,
}

impl IndexedDimension {
    fn build(name: &str, batch: RecordBatch, key: &str, budget: u64) -> Result<Self> {
        let key_index = batch.schema().index_of(key).map_err(|_| {
            anyhow::anyhow!("la dimensión {name} no tiene la columna '{key}'")
        })?;
        let keys = keys_of(batch.column(key_index))
            .with_context(|| format!("la clave '{key}' de la dimensión {name}"))?;
        let mut index = HashMap::with_capacity(keys.len());
        for (row, key_value) in keys.into_iter().enumerate() {
            let Some(key_value) = key_value else {
                anyhow::bail!("la dimensión {name} tiene una clave vacía");
            };
            if index.insert(key_value.clone(), row as u32).is_some() {
                anyhow::bail!("la dimensión {name} repite la clave {}", key_value.show());
            }
        }
        let bytes = footprint(&batch, &index);
        if bytes > budget {
            anyhow::bail!(
                "la dimensión {name} ocupa {bytes} bytes y el presupuesto es {budget}"
            );
        }
        Ok(Self { index, batch })
    }

    fn enrich(
        &self,
        facts: &RecordBatch,
        fact_key: &str,
        keep_misses: bool,
        outputs: &[(String, String)],
        schema: &SchemaRef,
    ) -> Result<RecordBatch> {
        let key_index = facts.schema().index_of(fact_key).map_err(|_| {
            anyhow::anyhow!("el stream no tiene la columna '{fact_key}'")
        })?;
        let fact_keys = keys_of(facts.column(key_index))?;
        if keep_misses {
            let mut dim_rows = UInt32Builder::with_capacity(facts.num_rows());
            for key in &fact_keys {
                match key.as_ref().and_then(|key| self.index.get(key)) {
                    Some(row) => dim_rows.append_value(*row),
                    None => dim_rows.append_null(),
                }
            }
            let dim_rows = dim_rows.finish();
            let mut columns = facts.columns().to_vec();
            for (source, _) in outputs {
                columns.push(take_column(&self.batch, source, &dim_rows)?);
            }
            return RecordBatch::try_new(schema.clone(), columns).context("batch enriquecido");
        }

        let mut fact_rows: Vec<u32> = Vec::new();
        let mut dim_rows: Vec<u32> = Vec::new();
        for (row, key) in fact_keys.iter().enumerate() {
            let Some(key) = key else { continue };
            let Some(dim_row) = self.index.get(key) else { continue };
            fact_rows.push(row as u32);
            dim_rows.push(*dim_row);
        }
        if fact_rows.is_empty() {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }
        let fact_index = UInt32Array::from(fact_rows);
        let dim_index = UInt32Array::from(dim_rows);
        let mut columns = Vec::with_capacity(schema.fields().len());
        for column in facts.columns() {
            columns.push(take_array(column, &fact_index)?);
        }
        for (source, _) in outputs {
            columns.push(take_column(&self.batch, source, &dim_index)?);
        }
        RecordBatch::try_new(schema.clone(), columns).context("batch enriquecido")
    }
}

/// Snapshot de una dimensión, listo para pegar columnas a un batch del stream.
pub struct DimensionCache {
    name: String,
    key: String,
    columns: Vec<String>,
    budget: u64,
    snapshot_id: Option<i64>,
    indexed: IndexedDimension,
    table: paimon::table::Table,
}

impl DimensionCache {
    /// Lee el snapshot actual. Si no entra en `budget`, no arranca.
    pub async fn load(
        warehouse: &str,
        database: &str,
        table_name: &str,
        name: &str,
        key: &str,
        columns: &[String],
        budget: u64,
    ) -> Result<Self> {
        let table = open_table(warehouse, database, table_name)
            .await
            .with_context(|| format!("abriendo la dimensión {name}"))?;
        let (snapshot_id, batch) = read_snapshot(&table, name, columns).await?;
        let indexed = IndexedDimension::build(name, batch, key, budget)?;
        tracing::info!(
            dimension = name,
            rows = indexed.index.len(),
            snapshot = ?snapshot_id,
            "dimensión cargada"
        );
        Ok(Self {
            name: name.to_string(),
            key: key.to_string(),
            columns: columns.to_vec(),
            budget,
            snapshot_id,
            indexed,
            table,
        })
    }

    /// Si Paimon publicó otro snapshot, el mapa pasa a ser ese. El anterior se suelta.
    pub async fn refresh(&mut self) -> Result<()> {
        let latest = self
            .table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .context("leyendo el snapshot de la dimensión")?;
        if latest == self.snapshot_id {
            return Ok(());
        }
        let (snapshot_id, batch) = read_snapshot(&self.table, &self.name, &self.columns).await?;
        self.indexed = IndexedDimension::build(&self.name, batch, &self.key, self.budget)?;
        self.snapshot_id = snapshot_id;
        tracing::info!(
            dimension = %self.name,
            rows = self.indexed.index.len(),
            snapshot = ?self.snapshot_id,
            "dimensión actualizada"
        );
        Ok(())
    }

    pub fn field(&self, name: &str) -> Result<Field> {
        self.indexed
            .batch
            .schema()
            .field_with_name(name)
            .cloned()
            .map_err(|_| anyhow::anyhow!("la dimensión {} no tiene la columna '{name}'", self.name))
    }

    pub fn len(&self) -> usize {
        self.indexed.index.len()
    }

    /// Pega las columnas de la dimensión. `keep_misses` deja la fila con nulls;
    /// si no, la fila no sale. El offset del stream lo cuenta quien llama.
    pub fn enrich(
        &self,
        facts: &RecordBatch,
        fact_key: &str,
        keep_misses: bool,
        outputs: &[(String, String)],
        schema: &SchemaRef,
    ) -> Result<RecordBatch> {
        self.indexed
            .enrich(facts, fact_key, keep_misses, outputs, schema)
    }
}

async fn read_snapshot(
    table: &paimon::table::Table,
    name: &str,
    columns: &[String],
) -> Result<(Option<i64>, RecordBatch)> {
    let known: Vec<&str> = table
        .schema()
        .fields()
        .iter()
        .map(|field| field.name())
        .collect();
    for column in columns {
        if !known.contains(&column.as_str()) {
            anyhow::bail!("la dimensión {name} no tiene la columna '{column}'");
        }
    }
    let schema = Arc::new(projection_schema(table, columns).context("schema de la dimensión")?);
    let snapshot_id = table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .context("leyendo el snapshot de la dimensión")?;
    if snapshot_id.is_none() {
        return Ok((None, RecordBatch::new_empty(schema)));
    }
    let mut builder = table.new_read_builder();
    let refs: Vec<&str> = columns.iter().map(String::as_str).collect();
    builder
        .with_projection(&refs)
        .context("proyección de la dimensión")?;
    let scan = builder.new_scan();
    let plan = scan.plan().await.context("plan de la dimensión")?;
    let read = builder.new_read().context("read de la dimensión")?;
    let stream = read
        .to_arrow(&plan.splits())
        .context("leyendo la dimensión")?;
    let batches: Vec<RecordBatch> = stream.try_collect().await.context("batches de la dimensión")?;
    let batch = if batches.is_empty() {
        RecordBatch::new_empty(schema)
    } else {
        arrow::compute::concat_batches(&schema, &batches).context("concatenando la dimensión")?
    };
    Ok((snapshot_id, batch))
}

fn take_column(batch: &RecordBatch, name: &str, indices: &UInt32Array) -> Result<ArrayRef> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| anyhow::anyhow!("la dimensión no tiene la columna '{name}'"))?;
    take_array(batch.column(index), indices)
}

fn take_array(column: &ArrayRef, indices: &UInt32Array) -> Result<ArrayRef> {
    take(column.as_ref(), indices, None).context("take")
}

fn keys_of(column: &ArrayRef) -> Result<Vec<Option<LookupKey>>> {
    if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
        return Ok((0..values.len())
            .map(|row| {
                if values.is_null(row) {
                    None
                } else {
                    Some(LookupKey::I64(values.value(row)))
                }
            })
            .collect());
    }
    if let Some(values) = column.as_any().downcast_ref::<Int32Array>() {
        return Ok((0..values.len())
            .map(|row| {
                if values.is_null(row) {
                    None
                } else {
                    Some(LookupKey::I32(values.value(row)))
                }
            })
            .collect());
    }
    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        return Ok((0..values.len())
            .map(|row| {
                if values.is_null(row) {
                    None
                } else {
                    Some(LookupKey::Text(values.value(row).to_string()))
                }
            })
            .collect());
    }
    anyhow::bail!(
        "es {}; hace falta Int64, Int32 o Utf8",
        column.data_type()
    )
}

fn footprint(batch: &RecordBatch, index: &HashMap<LookupKey, u32>) -> u64 {
    let arrays: usize = batch
        .columns()
        .iter()
        .map(|column| column.get_array_memory_size())
        .sum();
    let keys: usize = index
        .keys()
        .map(|key| match key {
            LookupKey::I64(_) | LookupKey::I32(_) => 16,
            LookupKey::Text(value) => 16 + value.len(),
        })
        .sum();
    (arrays + keys) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn customers(rows: &[(i64, Option<&str>)]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("customer_id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|row| row.1.map(str::to_string))
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("dimensión")
    }

    fn facts(ids: &[Option<i64>]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("order_id", DataType::Int64, false),
                Field::new("customer_id", DataType::Int64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(
                    ids.iter().enumerate().map(|(i, _)| i as i64 + 1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(ids.to_vec())),
            ],
        )
        .expect("hechos")
    }

    fn enriched_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("customer_id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn names(batch: &RecordBatch) -> Vec<Option<String>> {
        let column = batch
            .column(batch.schema().index_of("name").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..batch.num_rows())
            .map(|row| {
                if column.is_null(row) {
                    None
                } else {
                    Some(column.value(row).to_string())
                }
            })
            .collect()
    }

    fn outputs() -> Vec<(String, String)> {
        vec![("name".to_string(), "name".to_string())]
    }

    #[test]
    fn left_keeps_nulls_and_inner_drops_the_row() {
        let indexed = IndexedDimension::build(
            "customers",
            customers(&[(1, Some("ana")), (2, Some("bea"))]),
            "customer_id",
            u64::MAX,
        )
        .unwrap();
        let schema = enriched_schema();
        let left = indexed
            .enrich(
                &facts(&[Some(1), None, Some(9)]),
                "customer_id",
                true,
                &outputs(),
                &schema,
            )
            .unwrap();
        assert_eq!(left.num_rows(), 3);
        assert_eq!(names(&left), vec![Some("ana".to_string()), None, None]);
        let inner = indexed
            .enrich(
                &facts(&[Some(1), None, Some(9), Some(2)]),
                "customer_id",
                false,
                &outputs(),
                &schema,
            )
            .unwrap();
        assert_eq!(inner.num_rows(), 2);
        assert_eq!(
            names(&inner),
            vec![Some("ana".to_string()), Some("bea".to_string())]
        );
    }

    #[test]
    fn a_repeated_key_does_not_build() {
        let err = IndexedDimension::build(
            "customers",
            customers(&[(1, Some("ana")), (1, Some("otra"))]),
            "customer_id",
            u64::MAX,
        )
        .unwrap_err();
        assert!(err.to_string().contains("repite"), "{err}");
    }

    #[test]
    fn a_null_key_does_not_build() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("customer_id", DataType::Int64, true),
                Field::new("name", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![None])),
                Arc::new(StringArray::from(vec![Some("ana")])),
            ],
        )
        .expect("clave vacía");
        let err = IndexedDimension::build("customers", batch, "customer_id", u64::MAX).unwrap_err();
        assert!(err.to_string().contains("vacía"), "{err}");
    }

    #[test]
    fn over_budget_does_not_build() {
        let err = IndexedDimension::build(
            "customers",
            customers(&[(1, Some("ana"))]),
            "customer_id",
            1,
        )
        .unwrap_err();
        assert!(err.to_string().contains("presupuesto"), "{err}");
    }

    #[tokio::test]
    async fn a_later_snapshot_replaces_the_same_key() {
        use crate::writer::{create_test_table, PaimonSink};
        use paimon::spec::{BigIntType, DataType as PDataType, VarCharType};

        let dir = std::env::temp_dir().join(format!(
            "tachyon-dim-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("reloj")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("warehouse");
        let warehouse = dir.to_string_lossy().to_string();
        let table = create_test_table(
            &warehouse,
            "default",
            "customers",
            &[
                (
                    "customer_id",
                    PDataType::BigInt(BigIntType::with_nullable(false)),
                ),
                ("name", PDataType::VarChar(VarCharType::string_type())),
            ],
            &["customer_id"],
            1,
            None,
        )
        .await
        .expect("tabla");

        let rows = |pairs: &[(i64, &str)]| {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("customer_id", DataType::Int64, false),
                    Field::new("name", DataType::Utf8, true),
                ])),
                vec![
                    Arc::new(Int64Array::from(
                        pairs.iter().map(|row| row.0).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        pairs
                            .iter()
                            .map(|row| Some(row.1.to_string()))
                            .collect::<Vec<_>>(),
                    )),
                ],
            )
            .expect("filas")
        };

        let mut sink = PaimonSink::from_table(table.clone(), "customer_id", 1, None).expect("sink");
        sink.write(&rows(&[(1, "ana"), (2, "bea")]))
            .await
            .expect("write");
        sink.commit().await.expect("commit");

        let columns = vec!["customer_id".to_string(), "name".to_string()];
        let mut cache = DimensionCache::load(
            &warehouse,
            "default",
            "customers",
            "customers",
            "customer_id",
            &columns,
            u64::MAX,
        )
        .await
        .expect("carga");
        assert_eq!(cache.len(), 2);

        let mut sink = PaimonSink::from_table(table, "customer_id", 1, None).expect("sink");
        sink.write(&rows(&[(1, "eva")])).await.expect("update");
        sink.commit().await.expect("commit");
        cache.refresh().await.expect("refresh");
        assert_eq!(cache.len(), 2, "la misma clave sigue siendo una fila");

        let name = cache.field("name").expect("name");
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("customer_id", DataType::Int64, true),
            Field::new("name", name.data_type().clone(), true),
        ]));
        let enriched = cache
            .enrich(&facts(&[Some(1)]), "customer_id", true, &outputs(), &schema)
            .expect("enrich");
        assert_eq!(names(&enriched), vec![Some("eva".to_string())]);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
