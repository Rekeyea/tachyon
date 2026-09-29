//! `UNION ALL` de dos o más topics con el mismo esquema de salida.
//!
//! Cada rama se planifica sola, como un pass-through. El batch que sale lleva
//! el snapshot del tracker de esa rama: un prefetch de la otra no entra al
//! checkpoint. El canal de cada rama tiene capacidad 1, así que la que no se
//! está publicando adelanta un solo batch.

use anyhow::{Context, Result};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::channel::mpsc;
use futures::stream::{SelectAll, StreamExt};
use futures::SinkExt;
use tachyon_core::SourceOffsets;
use tachyon_source::OffsetTracker;

use crate::execute::{ensure_passthrough, plan_query, InputSource, StreamTableFactory};

/// Una rama lista para planificar: su `SELECT`, el topic físico y el tracker
/// que avanza cuando esa fuente emite.
pub(crate) struct UnionSource {
    pub name: String,
    pub sql: String,
    pub topic: String,
    pub schema: SchemaRef,
    pub tracker: OffsetTracker,
}

/// Fusión justa de las ramas. Cada `next` es un batch de una sola rama y los
/// offsets que ese batch cubre.
pub(crate) struct UnionFeed {
    schema: SchemaRef,
    incoming: SelectAll<mpsc::Receiver<Result<(RecordBatch, SourceOffsets)>>>,
    // Abortar las tareas al dropear el feed.
    _tasks: AbortTasks,
}

struct AbortTasks(Vec<tokio::task::JoinHandle<()>>);

impl Drop for AbortTasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

impl UnionFeed {
    pub(crate) fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub(crate) async fn next(&mut self) -> Option<Result<(RecordBatch, SourceOffsets)>> {
        self.incoming.next().await
    }

    /// Planifica cada rama, compara los esquemas y arranca el prefetch.
    pub(crate) async fn start(
        sources: &[UnionSource],
        factory: &StreamTableFactory,
    ) -> Result<Self> {
        if sources.len() < 2 {
            anyhow::bail!("UNION ALL es un SELECT de una tabla por rama");
        }
        let mut seen_topics = Vec::new();
        for source in sources {
            if seen_topics.iter().any(|topic: &String| topic == &source.topic) {
                anyhow::bail!(
                    "UNION ALL lee el topic '{}' en una sola rama",
                    source.topic
                );
            }
            seen_topics.push(source.topic.clone());
        }

        let mut planned = Vec::with_capacity(sources.len());
        for source in sources {
            let input = InputSource {
                name: source.name.clone(),
                schema: source.schema.clone(),
            };
            let (plan, task_ctx) = plan_query(&source.sql, &[input], factory)
                .await
                .with_context(|| format!("planificando la rama '{}'", source.name))?;
            ensure_passthrough(&plan)
                .with_context(|| format!("la rama '{}' no admite exactly-once", source.name))?;
            planned.push((plan, task_ctx));
        }
        let schema = planned[0].0.schema();
        for (index, (plan, _)) in planned.iter().enumerate().skip(1) {
            if !schemas_match(schema.as_ref(), plan.schema().as_ref()) {
                anyhow::bail!(
                    "las ramas de UNION ALL no escriben las mismas columnas ({} y {})",
                    sources[0].name,
                    sources[index].name
                );
            }
        }
        let schema = schema.clone();

        let mut receivers = Vec::with_capacity(planned.len());
        let mut tasks = AbortTasks(Vec::with_capacity(planned.len()));
        for ((plan, task_ctx), source) in planned.into_iter().zip(sources.iter()) {
            let stream = datafusion::physical_plan::execute_stream(plan, task_ctx)
                .with_context(|| format!("ejecutando la rama '{}'", source.name))?;
            let (tx, rx) = mpsc::channel(1);
            receivers.push(rx);
            let topic = source.topic.clone();
            let tracker = source.tracker.clone();
            tasks.0.push(tokio::spawn(async move {
                let mut stream = stream;
                let mut tx = tx;
                while let Some(item) = stream.next().await {
                    match item {
                        Ok(batch) => {
                            // Un batch vacío ya avanzó el tracker (la fila se
                            // filtró). El próximo batch con filas arrastra ese
                            // offset; no se publica solo.
                            if batch.num_rows() == 0 {
                                continue;
                            }
                            let mut covered = SourceOffsets::new();
                            covered.insert(topic.clone(), tracker.snapshot());
                            if tx.send(Ok((batch, covered))).await.is_err() {
                                break;
                            }
                        }
                        Err(err) => {
                            let _ = tx.send(Err(anyhow::anyhow!(err))).await;
                            break;
                        }
                    }
                }
            }));
        }
        Ok(Self {
            schema,
            incoming: futures::stream::select_all(receivers),
            _tasks: tasks,
        })
    }
}

/// Mismo nombre, tipo, nulabilidad y orden. La metadata del campo no cuenta.
pub(crate) fn schemas_match(left: &Schema, right: &Schema) -> bool {
    left.fields().len() == right.fields().len()
        && left
            .fields()
            .iter()
            .zip(right.fields().iter())
            .all(|(left, right)| {
                left.name() == right.name()
                    && left.data_type() == right.data_type()
                    && left.is_nullable() == right.is_nullable()
            })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::streaming::StreamingTable;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::streaming::PartitionStream;
    use datafusion::physical_plan::SendableRecordBatchStream;
    use tachyon_sql::parse_sql;

    struct OnceBatch {
        schema: SchemaRef,
        batch: RecordBatch,
    }

    impl std::fmt::Debug for OnceBatch {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("OnceBatch")
                .field("rows", &self.batch.num_rows())
                .finish()
        }
    }

    impl PartitionStream for OnceBatch {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }
        fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
            let batch = self.batch.clone();
            let schema = self.schema.clone();
            Box::pin(RecordBatchStreamAdapter::new(
                schema,
                futures::stream::iter(vec![Ok(batch)]),
            ))
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ]))
    }

    fn batch(rows: &[(i64, i64)]) -> RecordBatch {
        let (ids, amounts): (Vec<i64>, Vec<i64>) = rows.iter().copied().unzip();
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(amounts)),
            ],
        )
        .expect("batch")
    }

    fn factory(rows: HashMap<&str, &[(i64, i64)]>) -> Box<StreamTableFactory> {
        let map: HashMap<String, RecordBatch> = rows
            .into_iter()
            .map(|(name, rows)| (name.to_string(), batch(rows)))
            .collect();
        Box::new(move |name, table_schema| {
            let batch = map
                .get(name)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("input '{name}' sin batch"))?;
            let stream = Arc::new(OnceBatch {
                schema: table_schema.clone(),
                batch,
            });
            let table = StreamingTable::try_new(table_schema, vec![stream])
                .map_err(|err| anyhow::anyhow!("creando StreamingTable: {err}"))?;
            Ok(Arc::new(table))
        })
    }

    fn source(name: &str, sql: &str, topic: &str) -> UnionSource {
        UnionSource {
            name: name.to_string(),
            sql: sql.to_string(),
            topic: topic.to_string(),
            schema: schema(),
            tracker: OffsetTracker::new(),
        }
    }

    fn ids_of(batch: &RecordBatch) -> Vec<i64> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("order_id")
            .values()
            .to_vec()
    }

    #[test]
    fn schemas_match_requires_the_same_columns() {
        let left = schema();
        assert!(schemas_match(left.as_ref(), left.as_ref()));
        let swapped = Schema::new(vec![
            Field::new("amount", DataType::Int64, false),
            Field::new("order_id", DataType::Int64, false),
        ]);
        assert!(!schemas_match(left.as_ref(), &swapped));
    }

    #[tokio::test]
    async fn a_union_emits_one_branch_at_a_time_and_drops_the_filter() {
        let parsed = parse_sql(
            "INSERT INTO out \
             SELECT order_id, amount FROM web WHERE amount > 15 \
             UNION ALL \
             SELECT order_id, amount FROM app",
        )
        .expect("parse");
        let branches = parsed.union_all.expect("ramas");
        let sources = vec![
            source("web", &branches[0].sql, "web-topic"),
            source("app", &branches[1].sql, "app-topic"),
        ];
        let factory = factory(HashMap::from([
            ("web", [(1, 10), (2, 20)].as_slice()),
            ("app", [(3, 30)].as_slice()),
        ]));
        let mut feed = UnionFeed::start(&sources, &factory)
            .await
            .expect("feed");
        let mut rows = Vec::new();
        while let Some(item) = feed.next().await {
            let (batch, offsets) = item.expect("batch");
            assert_eq!(offsets.len(), 1, "el batch cubre una sola rama: {offsets:?}");
            let topic = offsets.keys().next().expect("topic").clone();
            let ids = ids_of(&batch);
            if topic == "web-topic" {
                assert!(ids.iter().all(|id| *id == 2), "{ids:?}");
            } else {
                assert_eq!(topic, "app-topic");
                assert!(ids.iter().all(|id| *id == 3), "{ids:?}");
            }
            rows.extend(ids);
        }
        rows.sort();
        assert_eq!(rows, vec![2, 3]);
    }

    #[tokio::test]
    async fn mismatched_branches_do_not_start() {
        let factory = factory(HashMap::from([
            ("web", [(1, 10)].as_slice()),
            ("app", [(3, 30)].as_slice()),
        ]));
        let sources = vec![
            source("web", "SELECT order_id, amount FROM web", "web-topic"),
            source("app", "SELECT amount, order_id FROM app", "app-topic"),
        ];
        let Err(err) = UnionFeed::start(&sources, &factory).await else {
            panic!("debería fallar");
        };
        assert!(
            err.to_string().contains("las mismas columnas"),
            "{err:#}"
        );
    }
}
