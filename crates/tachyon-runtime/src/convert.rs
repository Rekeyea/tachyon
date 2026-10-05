//! Conversor de esquema en el hot path: convierte batches de fuente a schema base.
//!
//! Se inserta entre `RedpandaPartitionStream` y `StreamingTable`. Cada batch
//! pasa por un `SchemaConverter` que reordena columnas y coerciona tipos antes
//! de que DataFusion lo vea. Es stateless: no requiere checkpoint ni estado.

use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;
use tachyon_convert::SchemaConverter;

/// Un stream convertido: envuelve un `SendableRecordBatchStream` y aplica el
/// conversor a cada batch.
pub struct ConvertedStream {
    inner: SendableRecordBatchStream,
    schema: std::sync::Arc<arrow::datatypes::Schema>,
    converter: SchemaConverter,
}

impl ConvertedStream {
    /// Crea un nuevo stream convertido.
    pub fn new(inner: SendableRecordBatchStream, converter: SchemaConverter) -> Self {
        Self {
            inner,
            schema: converter.target_schema(),
            converter,
        }
    }
}

impl Stream for ConvertedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let inner = Pin::new(&mut self.inner);
        match futures::Stream::poll_next(inner, cx) {
            Poll::Ready(Some(Ok(batch))) => match self.converter.convert(&batch) {
                Ok(converted) => Poll::Ready(Some(Ok(converted))),
                Err(err) => Poll::Ready(Some(Err(DataFusionError::External(
                    anyhow::Error::from(err).into(),
                )))),
            },
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for ConvertedStream {
    fn schema(&self) -> std::sync::Arc<arrow::datatypes::Schema> {
        self.schema.clone()
    }
}

/// Crea un stream particionado que aplica el conversor a cada partición.
pub fn make_partitioned_stream(
    inner: SendableRecordBatchStream,
    converter: SchemaConverter,
) -> SendableRecordBatchStream {
    Box::pin(ConvertedStream::new(inner, converter))
}
