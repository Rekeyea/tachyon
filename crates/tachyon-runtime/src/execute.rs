//! Ejecución de la transformación stateless sobre DataFusion.
//!
//! Slice 3: conectar la fuente (Redpanda) a DataFusion y correr la query
//! stateless (filter, project, aggregate, join). La query se ejecuta contra
//! tablas streaming (`StreamingTable`) registradas en un `SessionContext`,
//! una por cada input de la config.
//!
//! El wiring fuente→DataFusion se decoupla del broker mediante una **factory**
//! de `StreamingTable`: en producción la factory cablea el conector Redpanda
//! (ver `tachyon-source`); en tests se inyecta una factory que produce tablas
//! sobre una fuente in-memory.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, SendableRecordBatchStream,
};
use datafusion::prelude::{SessionConfig, SessionContext};

/// Una tabla streaming (el esquema y el executor) para un input del pipeline.
pub type StreamTable = Arc<StreamingTable>;

/// Produce las tablas streaming para los inputs del pipeline.
///
/// Devuelve un mapa `nombre_lógico -> StreamingTable`. La implementación de
/// producción cablea el conector Redpanda; los tests inyectan una factory
/// in-memory.
pub type StreamTableFactory = dyn Fn(&str, SchemaRef) -> Result<StreamTable> + Send + Sync;

/// El resultado de ejecutar la transformación: un stream de `RecordBatch`
/// (la salida transformada, lista para el sink).
pub type TransformOutput = SendableRecordBatchStream;

/// Describe un input que se va a ejecutar: su nombre lógico y su esquema.
#[derive(Debug, Clone)]
pub struct InputSource {
    pub name: String,
    pub schema: SchemaRef,
}

/// Operadores que dejan pasar cada batch tal cual (sin buffering, sin tareas
/// propias): el batch de salida que recibe el runtime deriva del último batch
/// que emitió la fuente. Es la condición del tracking exacto de offsets
/// (exactly-once, ver `run.rs`).
const PASSTHROUGH_OPERATORS: &[&str] = &[
    "StreamingTableExec",
    "CooperativeExec",
    "FilterExec",
    "ProjectionExec",
    "CoalesceBatchesExec",
    "GlobalLimitExec",
    "LocalLimitExec",
];

/// Verifica que el plan sea un pipeline de pass-through de una partición.
///
/// Agregaciones, joins, sorts o repartitions retienen filas entre batches:
/// los offsets emitidos por la fuente ya no dicen qué filas llegaron al sink.
pub fn ensure_passthrough(plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
    ensure_passthrough_lanes(plan, 1)
}

/// Como `ensure_passthrough`, con `lanes` particiones de punta a punta: cada
/// carril (un consumidor) es una partición y nada las junta ni las reparte.
pub fn ensure_passthrough_lanes(plan: &Arc<dyn ExecutionPlan>, lanes: usize) -> Result<()> {
    if !PASSTHROUGH_OPERATORS.contains(&plan.name()) {
        anyhow::bail!(
            "exactly-once solo soporta transformaciones por registro (filter/project/limit); \
             el plan usa '{}', que retiene filas entre batches",
            plan.name()
        );
    }
    if plan.output_partitioning().partition_count() != lanes {
        anyhow::bail!(
            "exactly-once requiere un plan de {lanes} partición(es); '{}' tiene {}",
            plan.name(),
            plan.output_partitioning().partition_count()
        );
    }
    for child in plan.children() {
        ensure_passthrough_lanes(child, lanes)?;
    }
    Ok(())
}

/// Ejecuta la query stateless contra las tablas streaming.
///
/// Registra cada input como una tabla en un `SessionContext` nuevo y corre la
/// `select_sql` (la query SELECT ya extraída del `INSERT INTO`). Devuelve el
/// stream de salida transformada.
pub async fn execute_query(
    select_sql: &str,
    inputs: &[InputSource],
    factory: &StreamTableFactory,
) -> Result<TransformOutput> {
    let (plan, task_ctx) = plan_query(select_sql, inputs, factory).await?;
    datafusion::physical_plan::execute_stream(plan, task_ctx)
        .context("ejecutando la query de transformación")
}

/// Planifica la query (plan físico + contexto de ejecución) sin ejecutarla,
/// para poder validar el plan antes (ver `ensure_passthrough`).
pub async fn plan_query(
    select_sql: &str,
    inputs: &[InputSource],
    factory: &StreamTableFactory,
) -> Result<(Arc<dyn ExecutionPlan>, Arc<TaskContext>)> {
    // Tachyon no es distribuido: el paralelismo es por partición de Redpanda
    // entre instancias, no dentro de DataFusion. Usar `target_partitions=1`
    // evita un `RepartitionExec` que, sobre una fuente infinita de 1 partición,
    // no emite (espera más batches para repartir). 1 partición de fuente = 1
    // partición de ejecución.
    //
    // `batch_size=1` es una decisión de streaming, no un hack: los operadores
    // de DataFusion (p. ej. `FilterExec`) re-coalescen los batches en un
    // `BatchCoalescer` con objetivo `execution.batch_size` (default 8192) y
    // solo emiten al llenarlo o al terminar el stream. En un stream infinito
    // de bajo tráfico eso retiene los batches indefinidamente (latencia
    // unbounded). Con `batch_size=1`, el coalescer configura
    // `biggest_coalesce_batch_size = target/2 = 0`, y todo batch no-vacío
    // bypass (pasa tal cual): los batches que produce la fuente (batching por
    // tamaño + flush time-based en `RedpandaPartitionStream`) son la unidad
    // de flujo end-to-end.
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(1);
    let ctx = SessionContext::new_with_config(config);

    for input in inputs {
        let table = factory(&input.name, input.schema.clone())
            .with_context(|| format!("creando tabla streaming para '{}'", input.name))?;
        ctx.register_table(&input.name, table)
            .with_context(|| format!("registrando tabla '{}'", input.name))?;
    }

    let df = ctx
        .sql(select_sql)
        .await
        .context("planificando la query de transformación")?;

    // `execute_stream` (en `execute_query`) devuelve el stream de batches sin
    // materializarlo: es el camino hacia el sink (Slice 4).
    let task_ctx = Arc::new(df.task_ctx());
    let plan = df
        .create_physical_plan()
        .await
        .context("planificando el plan físico de la transformación")?;

    Ok((plan, task_ctx))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::streaming::PartitionStream;
    use futures::StreamExt;

    /// `PartitionStream` de test: emite un `RecordBatch` estático y termina.
    struct BatchPartitionStream {
        schema: SchemaRef,
        batch: RecordBatch,
    }

    impl std::fmt::Debug for BatchPartitionStream {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("BatchPartitionStream")
                .field("rows", &self.batch.num_rows())
                .finish()
        }
    }

    impl PartitionStream for BatchPartitionStream {
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

    /// Factory in-memory: para cada input, envuelve el batch proporcionado en
    /// una `StreamingTable` finita.
    fn in_memory_factory(
        batches: &std::collections::HashMap<String, RecordBatch>,
    ) -> Box<dyn Fn(&str, SchemaRef) -> Result<StreamTable> + Send + Sync> {
        let map: std::collections::HashMap<String, RecordBatch> = batches.clone();
        Box::new(move |name, schema| {
            let batch = map
                .get(name)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("input '{}' sin batch en el test", name))?;
            let ps = Arc::new(BatchPartitionStream {
                schema: schema.clone(),
                batch,
            });
            Ok(Arc::new(
                StreamingTable::try_new(schema, vec![ps])
                    .map_err(|e| anyhow::anyhow!("creando StreamingTable: {e}"))?,
            ))
        })
    }

    fn orders_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, true),
        ]))
    }

    fn orders_batch() -> RecordBatch {
        let schema = orders_schema();
        let order_id = Int64Array::from(vec![1, 2, 3, 4]);
        let status = StringArray::from(vec![
            Some("paid"),
            Some("cancelled"),
            Some("paid"),
            Some("shipped"),
        ]);
        let amount = Float64Array::from(vec![100.0, 200.0, 300.0, 400.0]);
        RecordBatch::try_new(
            schema,
            vec![Arc::new(order_id), Arc::new(status), Arc::new(amount)],
        )
        .expect("batch de orders")
    }

    async fn collect_all(stream: TransformOutput) -> Vec<RecordBatch> {
        let mut stream = stream;
        let mut out = Vec::new();
        while let Some(batch) = stream.next().await {
            out.push(batch.expect("batch sin error"));
        }
        out
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    #[tokio::test]
    async fn filter_stateless_drops_cancelled() {
        let inputs = vec![InputSource {
            name: "orders".into(),
            schema: orders_schema(),
        }];
        let factory = in_memory_factory(&std::collections::HashMap::from([(
            "orders".to_string(),
            orders_batch(),
        )]));

        let stream = execute_query(
            "SELECT order_id, amount FROM orders WHERE status <> 'cancelled'",
            &inputs,
            &factory,
        )
        .await
        .expect("query de filter");

        let batches = collect_all(stream).await;
        // 4 filas de entrada, 1 cancelled -> 3 salidas.
        assert_eq!(total_rows(&batches), 3, "el filter debe dejar 3 filas");

        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("order_id")
                    .values()
                    .to_vec()
            })
            .collect();
        let mut ids = ids;
        ids.sort();
        assert_eq!(ids, vec![1, 3, 4], "quedan los no-cancelados");
    }

    #[tokio::test]
    async fn project_stateless_selects_columns() {
        let inputs = vec![InputSource {
            name: "orders".into(),
            schema: orders_schema(),
        }];
        let factory = in_memory_factory(&std::collections::HashMap::from([(
            "orders".to_string(),
            orders_batch(),
        )]));

        let stream = execute_query("SELECT status, amount FROM orders", &inputs, &factory)
            .await
            .expect("query de project");

        let batches = collect_all(stream).await;
        assert_eq!(
            total_rows(&batches),
            4,
            "el project no cambia el nº de filas"
        );
        // El schema de salida tiene solo las columnas proyectadas.
        let out_schema = batches[0].schema();
        let names: Vec<&str> = out_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, vec!["status", "amount"], "solo columnas proyectadas");
    }

    #[tokio::test]
    async fn aggregate_stateless_groups_by_key() {
        let inputs = vec![InputSource {
            name: "orders".into(),
            schema: orders_schema(),
        }];
        let factory = in_memory_factory(&std::collections::HashMap::from([(
            "orders".to_string(),
            orders_batch(),
        )]));

        let stream = execute_query(
            "SELECT status, SUM(amount) AS total, COUNT(*) AS n \
             FROM orders GROUP BY status",
            &inputs,
            &factory,
        )
        .await
        .expect("query de aggregate");

        let batches = collect_all(stream).await;
        // 3 status distintos (paid, cancelled, shipped) -> 3 grupos.
        assert_eq!(total_rows(&batches), 3, "un grupo por status");
    }

    #[tokio::test]
    async fn join_stateless_combines_two_inputs() {
        // orders: order_id, status, amount
        let orders = orders_batch();
        // items: order_id, item_id
        let items_schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("item_id", DataType::Int64, false),
        ]));
        let items = RecordBatch::try_new(
            items_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 3])),
                Arc::new(Int64Array::from(vec![10, 11, 20])),
            ],
        )
        .expect("batch de items");

        let inputs = vec![
            InputSource {
                name: "orders".into(),
                schema: orders_schema(),
            },
            InputSource {
                name: "items".into(),
                schema: items_schema,
            },
        ];
        let factory = in_memory_factory(&std::collections::HashMap::from([
            ("orders".to_string(), orders),
            ("items".to_string(), items),
        ]));

        let stream = execute_query(
            "SELECT o.order_id, i.item_id, o.amount \
             FROM orders o JOIN items i ON o.order_id = i.order_id",
            &inputs,
            &factory,
        )
        .await
        .expect("query de join");

        let batches = collect_all(stream).await;
        // orders 1 y 3 tienen items (1->2 items, 3->1 item) -> 3 filas.
        assert_eq!(total_rows(&batches), 3, "el join inner une por order_id");
    }

    async fn plan_for(sql: &str) -> Arc<dyn ExecutionPlan> {
        let inputs = vec![InputSource {
            name: "orders".into(),
            schema: orders_schema(),
        }];
        let factory = in_memory_factory(&std::collections::HashMap::from([(
            "orders".to_string(),
            orders_batch(),
        )]));
        plan_query(sql, &inputs, &factory).await.expect("plan").0
    }

    #[tokio::test]
    async fn passthrough_plans_admit_exactly_once() {
        for sql in [
            "SELECT order_id, amount FROM orders WHERE status <> 'cancelled'",
            "SELECT order_id, amount * 2 AS doubled FROM orders",
            "SELECT order_id FROM orders LIMIT 2",
        ] {
            let plan = plan_for(sql).await;
            ensure_passthrough(&plan).unwrap_or_else(|e| panic!("{sql}: {e:#}"));
        }
    }

    #[tokio::test]
    async fn buffering_plans_are_rejected_for_exactly_once() {
        for sql in [
            "SELECT status, SUM(amount) FROM orders GROUP BY status",
            "SELECT order_id FROM orders ORDER BY amount",
        ] {
            let plan = plan_for(sql).await;
            assert!(ensure_passthrough(&plan).is_err(), "{sql} debe rechazarse");
        }
    }

    #[tokio::test]
    async fn a_rewritten_lookup_stays_passthrough() {
        use tachyon_sql::{orient_lookup, parse_sql, rewrite_lookup};

        let parsed = parse_sql(
            "INSERT INTO out \
             SELECT o.order_id, c.name AS customer_name \
             FROM orders o \
             LEFT JOIN customers c ON o.customer_id = c.customer_id \
             WHERE c.country = 'AR'",
        )
        .expect("parse");
        let join = orient_lookup(
            parsed.lookup.as_ref().expect("lookup"),
            &["customers"],
            &["orders"],
        )
        .expect("orient");
        let rewritten = rewrite_lookup(
            &parsed.select_sql,
            &join,
            &[
                "order_id".to_string(),
                "customer_id".to_string(),
                "status".to_string(),
                "amount".to_string(),
            ],
        )
        .expect("rewrite");
        assert!(
            !rewritten.sql.to_uppercase().contains("JOIN"),
            "{}",
            rewritten.sql
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("customer_id", DataType::Int64, true),
            Field::new("status", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("country", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![Some(7)])),
                Arc::new(StringArray::from(vec![Some("paid")])),
                Arc::new(Float64Array::from(vec![Some(10.0)])),
                Arc::new(StringArray::from(vec![Some("ana")])),
                Arc::new(StringArray::from(vec![Some("AR")])),
            ],
        )
        .expect("batch");
        let inputs = vec![InputSource {
            name: "orders".into(),
            schema,
        }];
        let factory = in_memory_factory(&std::collections::HashMap::from([(
            "orders".to_string(),
            batch,
        )]));
        let plan = plan_query(&rewritten.sql, &inputs, &factory)
            .await
            .expect("plan")
            .0;
        ensure_passthrough(&plan).unwrap_or_else(|err| panic!("{}: {err:#}", rewritten.sql));
    }
}
