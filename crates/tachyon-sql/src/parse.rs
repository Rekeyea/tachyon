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
    DateTimeField, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    Query, SelectItem, SetExpr, Statement, TableFactor, TableObject, Value,
};
use tachyon_core::{AggKind, WindowKind};
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
    /// `Some` si el `GROUP BY` tiene `TUMBLE`, `HOP` o `SESSION`. El operador
    /// todavía no está cableado: el runtime rechaza el arranque.
    pub window: Option<WindowShape>,
}

/// Ventana reconocida en el `GROUP BY`, antes de que DataFusion vea la SQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowShape {
    pub event_time: String,
    pub kind: WindowKind,
    pub size_ms: i64,
    pub slide_ms: Option<i64>,
    pub gap_ms: Option<i64>,
    /// Columnas del `GROUP BY` que no son la llamada de ventana.
    pub group_columns: Vec<String>,
    pub aggs: Vec<WindowAgg>,
    /// `WHERE` original, si hay. El rewrite lo conserva.
    pub filter_sql: Option<String>,
    /// Tabla del `FROM`. Una sola, en una query de ventana.
    pub source: String,
}

/// Agregado del `SELECT` de una query de ventana.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowAgg {
    pub kind: AggKind,
    pub input: Option<String>,
    pub alias: String,
}

impl WindowShape {
    /// SQL que entra a DataFusion: filter/project, sin la ventana.
    /// La columna `_tachyon_partition` la agrega el stream.
    pub fn rewrite_sql(&self) -> String {
        let mut cols: Vec<&str> = Vec::new();
        for col in &self.group_columns {
            if !cols.contains(&col.as_str()) {
                cols.push(col);
            }
        }
        if !cols.contains(&self.event_time.as_str()) {
            cols.push(&self.event_time);
        }
        for agg in &self.aggs {
            if let Some(input) = &agg.input {
                if !cols.contains(&input.as_str()) {
                    cols.push(input);
                }
            }
        }
        let list = cols.join(", ");
        let mut sql = format!(
            "SELECT {list}, _tachyon_partition FROM {}",
            self.source
        );
        if let Some(filter) = &self.filter_sql {
            sql.push_str(" WHERE ");
            sql.push_str(filter);
        }
        sql
    }
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
    let mut window = window_shape(source)?;
    if let Some(shape) = window.as_mut() {
        if source_tables.len() != 1 {
            return Err(Error::Sql(
                "una query de ventana tiene un solo FROM".to_string(),
            ));
        }
        shape.source = source_tables[0].clone();
    }

    // La query SELECT ejecutable (sin el INSERT INTO <out>), serializada de
    // vuelta a string desde el AST. Esta es la que corre en DataFusion contra
    // las tablas streaming registradas. Una ventana no llega a planificarse:
    // el runtime la rechaza hasta que el operador esté cableado.
    let select_sql = source.to_string();

    Ok(ParsedSql {
        target,
        source_tables,
        query: sql.to_string(),
        select_sql,
        window,
    })
}

/// Una sola llamada de ventana en el `GROUP BY`. Cero llamadas es pass-through.
fn window_shape(query: &Query) -> Result<Option<WindowShape>, Error> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    let GroupByExpr::Expressions(exprs, _) = &select.group_by else {
        return Ok(None);
    };
    let mut window: Option<WindowShape> = None;
    let mut group_columns = Vec::new();
    for expr in exprs {
        if let Some(shape) = window_call(expr)? {
            if window.is_some() {
                return Err(Error::Sql(
                    "el GROUP BY tiene más de una ventana".to_string(),
                ));
            }
            window = Some(shape);
        } else {
            group_columns.push(column_name(expr)?);
        }
    }
    if let Some(shape) = window.as_mut() {
        shape.group_columns = group_columns;
        shape.aggs = select_aggs(&select.projection)?;
        shape.filter_sql = select.selection.as_ref().map(|expr| expr.to_string());
        if shape.aggs.is_empty() {
            return Err(Error::Sql(
                "la query de ventana no tiene agregados".to_string(),
            ));
        }
    }
    Ok(window)
}

fn select_aggs(items: &[SelectItem]) -> Result<Vec<WindowAgg>, Error> {
    let mut aggs = Vec::new();
    for item in items {
        match item {
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Some((kind, input)) = agg_call(expr)? {
                    aggs.push(WindowAgg {
                        kind,
                        input,
                        alias: alias.value.clone(),
                    });
                }
            }
            SelectItem::UnnamedExpr(expr) => {
                if agg_call(expr)?.is_some() {
                    return Err(Error::Sql(
                        "el agregado de una ventana necesita alias".to_string(),
                    ));
                }
            }
            _ => {
                return Err(Error::Sql(
                    "la query de ventana no acepta SELECT *".to_string(),
                ));
            }
        }
    }
    Ok(aggs)
}

fn agg_call(expr: &Expr) -> Result<Option<(AggKind, Option<String>)>, Error> {
    let Expr::Function(fun) = expr else {
        return Ok(None);
    };
    let name = fun.name.to_string().to_ascii_uppercase();
    let kind = match name.as_str() {
        "COUNT" => AggKind::Count,
        "SUM" => AggKind::Sum,
        "MIN" => AggKind::Min,
        "MAX" => AggKind::Max,
        "AVG" => AggKind::Avg,
        _ => return Ok(None),
    };
    if let FunctionArguments::List(list) = &fun.args {
        if list.duplicate_treatment.is_some() {
            return Err(Error::Sql(format!(
                "agregado no soportado en ventanas exactly-once: {name} DISTINCT"
            )));
        }
    }
    let input = match &fun.args {
        FunctionArguments::List(list) if list.args.is_empty() => None,
        FunctionArguments::List(list) if list.args.len() == 1 => match &list.args[0] {
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => None,
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(column_name(expr)?),
            _ => {
                return Err(Error::Sql(format!(
                    "agregado no soportado en ventanas exactly-once: {name}"
                )))
            }
        },
        FunctionArguments::None => None,
        _ => {
            return Err(Error::Sql(format!(
                "agregado no soportado en ventanas exactly-once: {name}"
            )))
        }
    };
    if kind != AggKind::Count && input.is_none() {
        return Err(Error::Sql(format!(
            "{name} necesita una columna"
        )));
    }
    Ok(Some((kind, input)))
}

fn window_call(expr: &Expr) -> Result<Option<WindowShape>, Error> {
    let Expr::Function(fun) = expr else {
        return Ok(None);
    };
    let name = fun.name.to_string().to_ascii_uppercase();
    if !matches!(name.as_str(), "TUMBLE" | "HOP" | "SESSION") {
        return Ok(None);
    }
    let args = function_args(fun)?;
    let shape = match name.as_str() {
        "TUMBLE" => {
            if args.len() != 2 {
                return Err(Error::Sql(
                    "TUMBLE(event_time, INTERVAL size) espera 2 argumentos".to_string(),
                ));
            }
            let size_ms = interval_ms(args[1])?;
            WindowShape {
                event_time: column_name(args[0])?,
                kind: WindowKind::Tumble,
                size_ms,
                slide_ms: None,
                gap_ms: None,
                group_columns: vec![],
                aggs: vec![],
                filter_sql: None,
                source: String::new(),
            }
        }
        "HOP" => {
            if args.len() != 3 {
                return Err(Error::Sql(
                    "HOP(event_time, INTERVAL slide, INTERVAL size) espera 3 argumentos"
                        .to_string(),
                ));
            }
            let slide_ms = interval_ms(args[1])?;
            let size_ms = interval_ms(args[2])?;
            if slide_ms <= 0 || size_ms % slide_ms != 0 {
                return Err(Error::Sql(format!(
                    "el slide ({slide_ms} ms) tiene que dividir al tamaño ({size_ms} ms)"
                )));
            }
            WindowShape {
                event_time: column_name(args[0])?,
                kind: WindowKind::Hop,
                size_ms,
                slide_ms: Some(slide_ms),
                gap_ms: None,
                group_columns: vec![],
                aggs: vec![],
                filter_sql: None,
                source: String::new(),
            }
        }
        "SESSION" => {
            if args.len() != 2 {
                return Err(Error::Sql(
                    "SESSION(event_time, INTERVAL gap) espera 2 argumentos".to_string(),
                ));
            }
            WindowShape {
                event_time: column_name(args[0])?,
                kind: WindowKind::Session,
                size_ms: 0,
                slide_ms: None,
                gap_ms: Some(interval_ms(args[1])?),
                group_columns: vec![],
                aggs: vec![],
                filter_sql: None,
                source: String::new(),
            }
        }
        _ => unreachable!("nombre ya filtrado"),
    };
    Ok(Some(shape))
}

fn function_args(fun: &Function) -> Result<Vec<&Expr>, Error> {
    let FunctionArguments::List(list) = &fun.args else {
        return Err(Error::Sql(
            "la ventana tiene que llamarse con argumentos".to_string(),
        ));
    };
    list.args
        .iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr),
            _ => Err(Error::Sql(
                "la ventana solo acepta argumentos posicionales".to_string(),
            )),
        })
        .collect()
}

fn column_name(expr: &Expr) -> Result<String, Error> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|ident| ident.value.clone())
            .ok_or_else(|| Error::Sql("identificador de columna vacío".to_string())),
        _ => Err(Error::Sql(format!(
            "se esperaba una columna en el GROUP BY, llegó {expr}"
        ))),
    }
}

fn interval_ms(expr: &Expr) -> Result<i64, Error> {
    let Expr::Interval(interval) = expr else {
        return Err(Error::Sql(format!(
            "el tamaño de la ventana tiene que ser INTERVAL, llegó {expr}"
        )));
    };
    let value = literal_i64(interval.value.as_ref())?;
    if value <= 0 {
        return Err(Error::Sql(
            "el intervalo de la ventana tiene que ser > 0".to_string(),
        ));
    }
    let factor: i64 = match interval.leading_field {
        Some(DateTimeField::Millisecond) => 1,
        Some(DateTimeField::Second) => 1_000,
        Some(DateTimeField::Minute) => 60_000,
        Some(DateTimeField::Hour) => 3_600_000,
        Some(DateTimeField::Day) => 86_400_000,
        Some(_) => {
            return Err(Error::Sql(
                "la ventana solo acepta intervalos de milisegundos, segundos, minutos, horas o días"
                    .to_string(),
            ))
        }
        None => {
            return Err(Error::Sql(
                "el INTERVAL de la ventana tiene que nombrar la unidad".to_string(),
            ))
        }
    };
    value.checked_mul(factor).ok_or_else(|| {
        Error::Sql("el intervalo de la ventana se pasa de i64 milisegundos".to_string())
    })
}

fn literal_i64(expr: &Expr) -> Result<i64, Error> {
    let Expr::Value(value) = expr else {
        return Err(Error::Sql(format!(
            "el valor del INTERVAL tiene que ser un literal, llegó {expr}"
        )));
    };
    match &value.value {
        Value::Number(number, _) => number.parse::<i64>().map_err(|_| {
            Error::Sql(format!("el INTERVAL '{number}' no es un entero"))
        }),
        Value::SingleQuotedString(text) | Value::DoubleQuotedString(text) => text
            .parse::<i64>()
            .map_err(|_| Error::Sql(format!("el INTERVAL '{text}' no es un entero"))),
        other => Err(Error::Sql(format!(
            "el INTERVAL '{other}' no es un entero"
        ))),
    }
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
        assert!(parsed.window.is_none());
    }

    #[test]
    fn tumble_and_hop_come_from_the_ast() {
        let tumble = parse_sql(
            "INSERT INTO out SELECT order_id, COUNT(*) AS event_count \
             FROM orders WHERE status <> 'cancelled' \
             GROUP BY order_id, TUMBLE(event_time, INTERVAL '1' MINUTE)",
        )
        .unwrap();
        let shape = tumble.window.expect("tumble");
        assert_eq!(shape.event_time, "event_time");
        assert_eq!(shape.kind, tachyon_core::WindowKind::Tumble);
        assert_eq!(shape.size_ms, 60_000);
        assert_eq!(shape.group_columns, vec!["order_id"]);
        assert_eq!(shape.aggs.len(), 1);
        assert_eq!(shape.aggs[0].alias, "event_count");
        assert!(shape.rewrite_sql().contains("_tachyon_partition"));
        assert!(shape.rewrite_sql().contains("status <> 'cancelled'"));

        let hop = parse_sql(
            "INSERT INTO out SELECT order_id, COUNT(*) AS n \
             FROM orders GROUP BY order_id, HOP(event_time, INTERVAL '5' SECOND, INTERVAL '10' SECOND)",
        )
        .unwrap();
        let shape = hop.window.expect("hop");
        assert_eq!(shape.slide_ms, Some(5_000));
        assert_eq!(shape.size_ms, 10_000);
        assert_eq!(shape.kind, tachyon_core::WindowKind::Hop);
    }

    #[test]
    fn hop_slide_must_divide_the_size() {
        let err = parse_sql(
            "INSERT INTO out SELECT order_id, COUNT(*) AS n FROM orders \
             GROUP BY order_id, HOP(event_time, INTERVAL '3' SECOND, INTERVAL '10' SECOND)",
        )
        .unwrap_err();
        assert!(err.to_string().contains("dividir"), "{err}");
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
