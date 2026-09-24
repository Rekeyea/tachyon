//! Parsing de `pipeline.sql` con el parser SQL de DataFusion.
//!
//! El MVP usa una forma canónica: una única sentencia
//! `INSERT INTO <out> SELECT ... FROM <inputs> ...`. Este módulo:
//!
//! 1. **Valida la sintaxis** (si DataFusion no la parsea, es un error claro).
//! 2. **Extrae el target** lógico (el nombre de la tabla de salida, p. ej.
//!    `orders_lake`) y las **tablas fuente** lógicas del `FROM` (p. ej. `orders`).
//! 3. Deja la query completa para su ejecución posterior (Slice 3).
//!
//! La validación semántica completa (columnas, tipos) llega en los Slices 2-3,
//! cuando se cargan los schemas Avro y la query se planifica contra las tablas
//! reales. Aquí se valida la estructura y el binding lógico contra la config.

use datafusion::sql::sqlparser::ast::{
    Query, SetExpr, Statement, TableFactor, TableObject,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use tachyon_core::Error;

/// El resultado de parsear la SQL.
#[derive(Debug)]
pub struct ParsedSql {
    /// Nombre lógico del target (el `INSERT INTO <out>`). Debe coincidir con
    /// `output.name` de la config.
    pub target: String,
    /// Nombres lógicos de las tablas fuente del `FROM`. Cada una debe existir
    /// como un `inputs[*].name` de la config.
    pub source_tables: Vec<String>,
    /// La query completa (para su ejecución en el Slice 3).
    pub query: String,
    /// La query SELECT ejecutable (sin el `INSERT INTO <out>`), lista para
    /// correr en DataFusion contra las tablas streaming registradas.
    pub select_sql: String,
}

/// Parsea `pipeline.sql` y valida que sea un `INSERT INTO <out> SELECT ...`.
pub fn parse_sql(sql: &str) -> Result<ParsedSql, Error> {
    if sql.trim().is_empty() {
        return Err(Error::Sql("SQL vacía".to_string()));
    }

    let statements = Parser::parse_sql(&GenericDialect {}, sql)
        .map_err(|e| Error::Sql(format!("error de sintaxis: {e}")))?;

    if statements.len() != 1 {
        return Err(Error::Sql(format!(
            "se esperaba exactamente 1 sentencia, hay {}",
            statements.len()
        )));
    }

    let Statement::Insert(insert) = &statements[0] else {
        return Err(Error::Sql(
            "la SQL debe ser un INSERT INTO <out> SELECT ...".to_string(),
        ));
    };

    // Target: debe ser una tabla simple (no una subquery ni una función).
    let target = match &insert.table {
        TableObject::TableName(name) => table_name(name),
        _ => {
            return Err(Error::Sql(
                "el target del INSERT debe ser una tabla, no una subquery".to_string(),
            ))
        }
    };

    // Source: la query del SELECT (o INSERT ... VALUES, que no aplica aquí).
    let source = insert
        .source
        .as_ref()
        .ok_or_else(|| Error::Sql("el INSERT no tiene una query SELECT".to_string()))?;
    let source_tables = extract_source_tables(source);

    // La query SELECT ejecutable (sin el INSERT INTO <out>), serializada de
    // vuelta a string desde el AST. Esta es la que corre en DataFusion contra
    // las tablas streaming registradas.
    let select_sql = source.to_string();

    Ok(ParsedSql {
        target,
        source_tables,
        query: sql.to_string(),
        select_sql,
    })
}

/// Extrae los nombres lógicos de las tablas del `FROM` de una query.
///
/// Maneja `SELECT`, uniones (`UNION`/`EXCEPT`/`INTERSECT`) y subqueries anidadas.
/// Las tablas derivadas (subqueries en el `FROM`) se omiten: no son streams
/// lógicos y no se validan contra la config.
fn extract_source_tables(query: &Query) -> Vec<String> {
    extract_from_set_expr(&query.body)
}

fn extract_from_set_expr(se: &SetExpr) -> Vec<String> {
    match se {
        SetExpr::Select(select) => {
            let mut tables = Vec::new();
            for twj in &select.from {
                push_table(&mut tables, &twj.relation);
                // Las tablas del JOIN viven en `joins`, no en `from`.
                for join in &twj.joins {
                    push_table(&mut tables, &join.relation);
                }
            }
            tables
        }
        SetExpr::Query(inner) => extract_from_set_expr(&inner.body),
        SetExpr::SetOperation { left, right, .. } => {
            let mut tables = extract_from_set_expr(left);
            tables.extend(extract_from_set_expr(right));
            tables
        }
        _ => Vec::new(),
    }
}

/// Añade el nombre de una tabla si el `TableFactor` es una tabla simple.
/// Las subqueries y funciones de tabla no son streams lógicos.
fn push_table(tables: &mut Vec<String>, relation: &TableFactor) {
    if let TableFactor::Table { name, .. } = relation {
        tables.push(table_name(name));
    }
}

/// Devuelve el nombre lógico de una tabla (la última parte del `ObjectName`).
/// `default.orders_lake` -> `orders_lake`; `orders` -> `orders`.
fn table_name(name: &datafusion::sql::sqlparser::ast::ObjectName) -> String {
    use datafusion::sql::sqlparser::ast::ObjectNamePart;
    name.0
        .last()
        .and_then(|part| match part {
            ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
            ObjectNamePart::Function(_) => None,
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_insert_into_select() {
        let sql = "INSERT INTO orders_lake \
                   SELECT order_id, SUM(amount) AS total \
                   FROM orders WHERE status <> 'cancelled' GROUP BY order_id";
        let parsed = parse_sql(sql).unwrap();
        assert_eq!(parsed.target, "orders_lake");
        assert_eq!(parsed.source_tables, vec!["orders"]);
        assert!(parsed.query.contains("SUM(amount)"));
    }

    #[test]
    fn extracts_executable_select_sql() {
        let sql = "INSERT INTO orders_lake \
                   SELECT order_id, SUM(amount) AS total \
                   FROM orders WHERE status <> 'cancelled' GROUP BY order_id";
        let parsed = parse_sql(sql).unwrap();
        // La query SELECT ejecutable no debe contener el INSERT INTO.
        assert!(!parsed.select_sql.to_uppercase().contains("INSERT"));
        // Y debe contener el SELECT con su body.
        assert!(parsed.select_sql.to_uppercase().starts_with("SELECT"));
        assert!(parsed.select_sql.contains("SUM(amount)"));
        assert!(parsed.select_sql.contains("orders"));
    }

    #[test]
    fn extracts_multiple_source_tables_from_join() {
        let sql = "INSERT INTO out SELECT a.x FROM a JOIN b ON a.id = b.id";
        let parsed = parse_sql(sql).unwrap();
        assert_eq!(parsed.source_tables, vec!["a", "b"]);
    }

    #[test]
    fn strips_database_qualifier_from_target() {
        let sql = "INSERT INTO db.orders_lake SELECT x FROM orders";
        let parsed = parse_sql(sql).unwrap();
        assert_eq!(parsed.target, "orders_lake");
    }

    #[test]
    fn rejects_empty_sql() {
        assert!(parse_sql("   \n  ").is_err());
    }

    #[test]
    fn rejects_syntax_error() {
        assert!(parse_sql("THIS IS NOT SQL").is_err());
    }

    #[test]
    fn rejects_non_insert_statement() {
        assert!(parse_sql("SELECT 1").is_err());
    }

    #[test]
    fn rejects_multiple_statements() {
        assert!(parse_sql("SELECT 1; SELECT 2").is_err());
    }
}
