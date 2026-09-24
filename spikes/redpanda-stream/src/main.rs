//! Spike 0.5c (parte que no requiere broker): fuente de streaming -> DataFusion.
//!
//! Valida la pieza ardua del conector fuente: que una fuente de streaming se
//! enchufa a la ejecución **Unbounded** de DataFusion mediante el trait
//! `PartitionStream` + `StreamingTable` (la primitiva que internamente DataFusion
//! traduce a `StreamingTableExec`).
//!
//! En lugar de un broker Redpanda real (que no está disponible), se usa un
//! stream sintético INFINITO que emite `RecordBatch` cada 50ms: es exactamente
//! el stand-in de lo que el consumidor rdkafka emitiría. La query usa `LIMIT`
//! para terminar sobre un stream infinito (caso real de un topic).
//!
//! El punto de intercambio con rdkafka queda marcado: basta reemplazar el
//! `futures::stream::unfold` sintético por el stream de batches del consumidor.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::streaming::StreamingTable;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use futures::stream;

/// Schema de entrada (equivalente al de `orders` en el ejemplo orders-etl).
fn input_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]))
}

/// Construye un batch de 2 filas a partir de un id de arranque.
fn make_batch(schema: &SchemaRef, id: i64) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![id, id + 1])),
            Arc::new(StringArray::from(vec!["paid", "paid"])),
            Arc::new(Float64Array::from(vec![10.0, 20.0])),
        ],
    )
    .expect("schema coincide; no puede fallar")
}

/// `PartitionStream` que emite un stream INFINITO de batches (el stand-in de
/// Redpanda). Cada `execute()` produce un stream fresco (un stream solo se
/// consume una vez).
///
/// >>> PUNTO DE INTERCAMBIO CON RD-KAFKA <<<
/// Aquí es donde el consumidor rdkafka entrará: su
/// `Stream<Item = Result<RecordBatch>>` (decodificado por partición) reemplaza
/// el `stream::unfold` sintético. El resto (StreamingTable, query, sink) no cambia.
struct RedpandaSimPartition {
    schema: SchemaRef,
}

impl std::fmt::Debug for RedpandaSimPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedpandaSimPartition").finish()
    }
}

impl PartitionStream for RedpandaSimPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let schema = self.schema.clone();
        // Emite un batch de 2 filas cada 50ms, para siempre (stream infinito).
        // El closure es FnMut (se llama por cada poll), así que clona el Arc
        // (barato) en cada iteracion para el bloque async.
        let source = stream::unfold(0i64, move |mut id| {
            let schema = schema.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let batch = make_batch(&schema, id);
                id += 2;
                Some((Ok(batch), id))
            }
        });
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), source))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let schema = input_schema();

    // 1. Fuente de streaming (1 partición sintética infinita).
    let partition = Arc::new(RedpandaSimPartition {
        schema: schema.clone(),
    });
    let table = StreamingTable::try_new(schema, vec![partition])
        .context("StreamingTable::try_new")?
        .with_infinite_table(true);

    // 2. Registrar en la sesión y correr una query sobre el stream infinito.
    //    El LIMIT hace que la query termine sin esperar a que el stream se agote.
    let ctx = SessionContext::new();
    ctx.register_table("orders_stream", Arc::new(table))
        .context("register_table")?;

    let sql = "SELECT order_id, status, amount FROM orders_stream LIMIT 4";
    let df = ctx.sql(sql).await.context("compilando SQL")?;

    // 3. Timeout de seguridad: si la ejecución Unbounded no respeta el LIMIT y
    //    intenta drenar el stream infinito, el spike falla en vez de colgar.
    let result = tokio::time::timeout(Duration::from_secs(10), df.collect())
        .await
        .context("timeout: la query no termino sobre el stream infinito")?
        .context("ejecutando plan")?;

    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    println!("OK: DataFusion ejecutó una query sobre una fuente de streaming infinita");
    println!("    filas consumidas del stream: {total_rows}");
    assert!(total_rows == 4, "se esperaban 4 filas (LIMIT 4), se obtuvieron {total_rows}");

    let formatted = datafusion::arrow::util::pretty::pretty_format_batches(&result)
        .context("formateando resultado")?;
    println!("{formatted}");
    println!("PASS: una fuente de streaming se enchufa a la ejecucion Unbounded de DataFusion");
    println!("      (el stream sintetico es donde entra el consumidor rdkafka)");
    Ok(())
}
