//! Spike 0.5b: DataFusion SQL sobre `RecordBatch` (capa de cómputo de Tachyon).
//!
//! Valida que DataFusion ejecuta una query SQL sobre un `RecordBatch` de Arrow:
//! el camino de transformación stateless del MVP:
//!   Redpanda -> RecordBatch -> SQL (DataFusion) -> RecordBatch -> Paimon
//!
//! El streaming real (Unbounded / StreamTableExec) se valida en el spike de
//! Redpanda; aquí se valida que el motor de cómputo corre la SQL y produce
//! un `RecordBatch` resultado.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Datos de entrada (simula lo que el conector Redpanda emitiria).
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
        Field::new("source_version", DataType::Int64, true),
        Field::new("amount", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 3, 3, 4])),
            Arc::new(StringArray::from(vec![
                "paid", "paid", "shipped", "paid", "cancelled", "paid",
            ])),
            Arc::new(Int64Array::from(vec![10, 11, 20, 30, 31, 40])),
            Arc::new(Float64Array::from(vec![100.0, 50.0, 200.0, 300.0, 10.0, 400.0])),
        ],
    )
    .context("construyendo RecordBatch de entrada")?;

    // 2. Registrar como tabla en memoria y abrir sesión.
    let ctx = SessionContext::new();
    ctx.register_batch("orders", batch.clone())
        .context("register_batch")?;

    // 3. La transformación stateless (equivalente al pipeline orders-etl):
    //    filtro + agregación por clave. DataFusion compila la SQL a un plan
    //    y lo ejecuta sobre Arrow.
    let sql = "SELECT order_id, SUM(amount) AS order_total, COUNT(*) AS event_count \
               FROM orders \
               WHERE status <> 'cancelled' \
               GROUP BY order_id \
               ORDER BY order_id";
    let df = ctx.sql(sql).await.context("compilando SQL")?;

    // 4. Ejecutar y coleccionar el resultado (Vec<RecordBatch>).
    //    Esperado: order 3 tiene 2 eventos (paid 300 + cancelled 10); el filtro
    //    quita solo el cancelled, así que quedan 4 ordenes (1,2,3,4):
    //      1 -> 150.0 / 2,  2 -> 200.0 / 1,  3 -> 300.0 / 1,  4 -> 400.0 / 1
    let result_batches = df.collect().await.context("ejecutando plan")?;
    let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();

    println!("OK: DataFusion ejecutó la SQL sobre el RecordBatch");
    println!("    filas de entrada: {}", batch.num_rows());
    println!("    filas de salida:  {total_rows}");
    assert!(total_rows == 4, "se esperaban 4 ordenes (1,2,3,4), se obtuvieron {total_rows}");

    // 5. Mostrar el resultado para inspección visual.
    let formatted = datafusion::arrow::util::pretty::pretty_format_batches(&result_batches)
        .context("formateando resultado")?;
    println!("{formatted}");
    println!("PASS: la capa de computo (DataFusion SQL sobre Arrow) es viable");
    Ok(())
}
