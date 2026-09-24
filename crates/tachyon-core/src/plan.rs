//! El plan de pipeline compilado desde `pipeline.sql` + `pipeline.yaml`.
//!
//! El plan es el artefacto validado: lleva la información de I/O (inputs,
//! output, clave de particionado), la query de transformación y el binding
//! lógico de la SQL. El runtime lo valida antes de ejecutar.

use crate::types::{PartitionKey, StreamDef};

/// El plan de pipeline validado.
#[derive(Debug)]
pub struct PipelinePlan {
    pub pipeline_name: String,
    /// Streams lógicos de entrada (los schemas de columnas se pueblan en el
    /// Slice 2, al cargar los schemas Avro).
    pub inputs: Vec<StreamDef>,
    pub output: OutputDef,
    pub partition_key: PartitionKey,
    pub partitions: usize,
    /// La query SQL completa (transformación stateless en el MVP).
    pub query: String,
    /// Target lógico del `INSERT INTO <out>` (debe coincidir con `output.name`).
    pub sql_target: String,
    /// Tablas lógicas del `FROM` (cada una debe existir en `inputs`).
    pub sql_source_tables: Vec<String>,
}

/// La definición de salida (vinculada a una tabla Paimon).
#[derive(Debug)]
pub struct OutputDef {
    pub name: String,
    pub table: String,
    pub bucket: usize,
    pub sequence_field: Option<String>,
}

impl PipelinePlan {
    /// Valida el invariante de alineación de conteos (ver DESIGN.md §3.3):
    /// `partitions == output.bucket`.
    pub fn validate_alignment(&self) -> Result<(), crate::Error> {
        if self.partitions != self.output.bucket {
            return Err(crate::Error::Alignment {
                expected: self.partitions,
                found: self.output.bucket,
            });
        }
        Ok(())
    }

    /// Valida el binding lógico de la SQL contra la config:
    /// - el target del `INSERT` coincide con `output.name`.
    /// - cada tabla del `FROM` existe como un `inputs[*].name`.
    pub fn validate_sql_binding(&self) -> Result<(), crate::Error> {
        if self.sql_target != self.output.name {
            return Err(crate::Error::Sql(format!(
                "el target de la SQL '{}' no coincide con output.name '{}'",
                self.sql_target, self.output.name
            )));
        }

        let input_names: Vec<&str> = self.inputs.iter().map(|i| i.name.as_str()).collect();
        for table in &self.sql_source_tables {
            if !input_names.contains(&table.as_str()) {
                return Err(crate::Error::Sql(format!(
                    "la tabla fuente '{}' no está definida en inputs",
                    table
                )));
            }
        }
        Ok(())
    }
}
