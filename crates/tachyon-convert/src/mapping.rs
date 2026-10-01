//! Definición de mapeo entre un esquema fuente y un esquema base.

use std::collections::HashMap;
use std::sync::Arc;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use anyhow::{Context, Result};

/// Regla de conversión para una columna individual.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMapping {
    /// Nombre en la fuente original.
    pub source_name: String,
    /// Nombre en el esquema base (target).
    pub target_name: String,
    /// Tipo destino en el esquema base. Se usa para coerción de tipos.
    pub target_type: DataType,
}

impl ColumnMapping {
    pub fn new(source_name: &str, target_name: &str, target_type: DataType) -> Self {
        Self {
            source_name: source_name.to_string(),
            target_name: target_name.to_string(),
            target_type,
        }
    }
}

/// Mapeo completo de un esquema fuente a un esquema base.
#[derive(Debug, Clone)]
pub struct SchemaMapping {
    /// Regla por columna: source → target + tipo destino.
    pub columns: Vec<ColumnMapping>,
    /// Mapa inverso: target_name → índice en `columns`.
    target_index: HashMap<String, usize>,
}

impl SchemaMapping {
    /// Construye un mapeo validando que no haya columnas duplicadas.
    pub fn try_new(columns: Vec<ColumnMapping>) -> Result<Self> {
        let mut seen = std::collections::HashSet::new();
        for col in &columns {
            if !seen.insert(&col.target_name) {
                anyhow::bail!(
                    "duplicación de columna target '{}'",
                    col.target_name
                );
            }
        }
        let mut target_index = HashMap::with_capacity(columns.len());
        for (i, col) in columns.iter().enumerate() {
            target_index.insert(col.target_name.clone(), i);
        }
        Ok(Self {
            columns,
            target_index,
        })
    }

    /// Verifica que el esquema fuente tenga todas las columnas requeridas.
    pub fn validate_source(&self, source_schema: &Schema) -> Result<()> {
        for col in &self.columns {
            if !source_schema.field_with_name(&col.source_name).is_ok() {
                anyhow::bail!(
                    "la columna fuente '{}' no existe en el esquema",
                    col.source_name
                );
            }
        }
        Ok(())
    }

    /// Construye el schema base a partir de los mapeos.
    pub fn target_schema(&self) -> SchemaRef {
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|col| Field::new(&col.target_name, col.target_type.clone(), true))
            .collect();
        Arc::new(Schema::new(fields))
    }

    /// Busca la regla para una columna target por su nombre.
    pub fn get_target(&self, target_name: &str) -> Option<&ColumnMapping> {
        self.target_index
            .get(target_name)
            .and_then(|&i| self.columns.get(i))
    }

    /// Retorna el orden de columnas target (para reordenar el batch).
    pub fn target_order(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.target_name.clone()).collect()
    }
}

/// Conversor que aplica un `SchemaMapping` a cada batch de Arrow.
pub struct SchemaConverter {
    mapping: SchemaMapping,
}

impl SchemaConverter {
    pub fn new(mapping: SchemaMapping) -> Self {
        Self { mapping }
    }

    /// Retorna el schema objetivo.
    pub fn target_schema(&self) -> SchemaRef {
        self.mapping.target_schema()
    }

    /// Convierte un batch fuente al esquema base (reordena + coerciona tipos).
    pub fn convert(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let mut columns = Vec::with_capacity(self.mapping.columns.len());
        for col in &self.mapping.columns {
            let source_col = batch.column_by_name(&col.source_name)
                .context("columna fuente no encontrada durante conversión")?;
            let coerced = crate::coerce::coerce_array(source_col, &col.target_type)?;
            columns.push(coerced);
        }
        RecordBatch::try_new(self.mapping.target_schema(), columns)
            .context("creando batch convertido")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, Int64Array};
    use std::sync::Arc;

    #[test]
    fn schema_mapping_validates_no_duplicates() {
        let result = SchemaMapping::try_new(vec![
            ColumnMapping::new("id", "order_id", DataType::Int64),
            ColumnMapping::new("amount", "order_id", DataType::Int64),
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn schema_mapping_validates_source_columns() {
        let mapping = SchemaMapping::try_new(vec![
            ColumnMapping::new("id", "order_id", DataType::Int64),
            ColumnMapping::new("amount", "total", DataType::Int64),
        ])
        .expect("valid mapping");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("missing", DataType::Int32, false), // no 'amount'
        ]));
        assert!(mapping.validate_source(&schema).is_err());
    }

    #[test]
    fn converter_reorders_and_coerces_columns() {
        let mapping = SchemaMapping::try_new(vec![
            ColumnMapping::new("amount", "total", DataType::Int64),
            ColumnMapping::new("id", "order_id", DataType::Int64),
        ])
        .expect("valid mapping");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .expect("batch");

        let converter = SchemaConverter::new(mapping);
        let result = converter.convert(&batch).expect("convert");

        assert_eq!(result.schema().fields()[0].name(), "total");
        assert_eq!(result.schema().fields()[1].name(), "order_id");
        assert_eq!(result.num_rows(), 3);
        let total_arr = result.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64");
        assert_eq!(total_arr.values(), &[10, 20, 30]);
    }

    #[test]
    fn converter_preserves_nulls() {
        let mapping = SchemaMapping::try_new(vec![
            ColumnMapping::new("id", "order_id", DataType::Int64),
        ])
        .expect("valid mapping");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]))],
        )
        .expect("batch");

        let converter = SchemaConverter::new(mapping);
        let result = converter.convert(&batch).expect("convert");

        let arr = result.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64");
        assert!(arr.is_valid(0));
        assert!(arr.is_null(1));
        assert!(arr.is_valid(2));
    }
}
