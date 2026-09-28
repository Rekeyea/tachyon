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
    BinaryOperator, DateTimeField, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
    GroupByExpr, JoinConstraint, JoinOperator, Query, Select, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, Statement, TableFactor, TableObject, Value,
    WildcardAdditionalOptions,
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
    /// `Some` si el `FROM` es un join por intervalo de dos streams.
    pub join: Option<IntervalJoin>,
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
    let join = interval_join(source)?;
    if join.is_some() && window.is_some() {
        return Err(Error::Sql(
            "una query no puede ser ventana y join a la vez".to_string(),
        ));
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
        join,
    })
}

/// Join por intervalo de dos streams. `None` si el `FROM` no tiene join.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntervalJoin {
    pub left: String,
    pub right: String,
    pub left_key: String,
    pub right_key: String,
    pub left_time: String,
    pub right_time: String,
    pub lower_ms: i64,
    pub upper_ms: i64,
    /// El tiempo de la tabla izquierda es la base del `BETWEEN`.
    pub base_is_left: bool,
    pub columns: Vec<JoinSelect>,
}

/// Una columna del `SELECT`, tomada de uno de los dos lados.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinSelect {
    pub side_left: bool,
    pub column: String,
    pub alias: String,
}

fn interval_join(query: &Query) -> Result<Option<IntervalJoin>, Error> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    if select.from.len() != 1 || select.from[0].joins.is_empty() {
        return Ok(None);
    }
    if select.from[0].joins.len() != 1 {
        return Err(Error::Sql(
            "el join acepta dos tablas".to_string(),
        ));
    }
    let from = &select.from[0];
    let (left, left_alias) = table_ref(&from.relation)?;
    let join = &from.joins[0];
    let (right, right_alias) = table_ref(&join.relation)?;
    let on = match &join.join_operator {
        JoinOperator::Join(JoinConstraint::On(expr))
        | JoinOperator::Inner(JoinConstraint::On(expr)) => expr,
        _ => {
            return Err(Error::Sql(
                "el join tiene que ser INNER con ON".to_string(),
            ))
        }
    };
    let predicates = split_and(on);
    if predicates.len() != 2 {
        return Err(Error::Sql(
            "el ON del join es la igualdad de la clave y un BETWEEN".to_string(),
        ));
    }
    let mut equality = None;
    let mut between = None;
    for predicate in predicates {
        if let Some(eq) = equality_of(predicate)? {
            if equality.is_some() {
                return Err(Error::Sql(
                    "el ON tiene más de una igualdad".to_string(),
                ));
            }
            equality = Some(eq);
        } else if let Some(span) = between_of(predicate)? {
            if between.is_some() {
                return Err(Error::Sql(
                    "el ON tiene más de un BETWEEN".to_string(),
                ));
            }
            between = Some(span);
        } else {
            return Err(Error::Sql(format!(
                "predicado de join no soportado: {predicate}"
            )));
        }
    }
    let (left_key, right_key) = equality.ok_or_else(|| {
        Error::Sql("el ON no iguala las claves".to_string())
    })?;
    let span = between.ok_or_else(|| Error::Sql("el ON no tiene BETWEEN".to_string()))?;
    if span.lower_ms > span.upper_ms {
        return Err(Error::Sql(
            "el límite bajo del intervalo es mayor que el alto".to_string(),
        ));
    }
    let left_name = left_alias.as_deref().unwrap_or(left.as_str());
    let right_name = right_alias.as_deref().unwrap_or(right.as_str());
    let (left_key, right_key) = assign_pair(left_name, right_name, left_key, right_key)?;
    let base_is_left = span.base_table == left_name;
    let probe_is_right = span.probe_table == right_name;
    let base_is_right = span.base_table == right_name;
    let probe_is_left = span.probe_table == left_name;
    if !((base_is_left && probe_is_right) || (base_is_right && probe_is_left)) {
        return Err(Error::Sql(
            "el BETWEEN tiene que comparar el tiempo de las dos tablas".to_string(),
        ));
    }
    let (left_time, right_time) = if base_is_left {
        (span.base_column, span.probe_column)
    } else {
        (span.probe_column, span.base_column)
    };
    let columns = join_select(&select.projection, left_name, right_name)?;
    Ok(Some(IntervalJoin {
        left,
        right,
        left_key,
        right_key,
        left_time,
        right_time,
        lower_ms: span.lower_ms,
        upper_ms: span.upper_ms,
        base_is_left,
        columns,
    }))
}

struct TimeSpan {
    probe_table: String,
    probe_column: String,
    base_table: String,
    base_column: String,
    lower_ms: i64,
    upper_ms: i64,
}

fn split_and(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::Nested(inner) => split_and(inner),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut parts = split_and(left);
            parts.extend(split_and(right));
            parts
        }
        other => vec![other],
    }
}

fn equality_of(expr: &Expr) -> Result<Option<(Qual, Qual)>, Error> {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = expr
    else {
        return Ok(None);
    };
    Ok(Some((qualified(left)?, qualified(right)?)))
}

fn between_of(expr: &Expr) -> Result<Option<TimeSpan>, Error> {
    let Expr::Between {
        expr,
        negated,
        low,
        high,
    } = expr
    else {
        return Ok(None);
    };
    if *negated {
        return Err(Error::Sql(
            "el intervalo no acepta NOT BETWEEN".to_string(),
        ));
    }
    let (probe_table, probe_column) = require_table(qualified(expr)?)?;
    let (low_table, low_column, lower_ms) = time_point(low)?;
    let (high_table, high_column, upper_ms) = time_point(high)?;
    if low_table != high_table || low_column != high_column {
        return Err(Error::Sql(
            "los dos extremos del BETWEEN tienen que ser la misma columna de tiempo".to_string(),
        ));
    }
    Ok(Some(TimeSpan {
        probe_table,
        probe_column,
        base_table: low_table,
        base_column: low_column,
        lower_ms,
        upper_ms,
    }))
}

fn time_point(expr: &Expr) -> Result<(String, String, i64), Error> {
    match expr {
        Expr::Nested(inner) => time_point(inner),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Plus,
            right,
        } => {
            let (table, column) = require_table(qualified(left)?)?;
            Ok((table, column, interval_ms(right)?))
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Minus,
            right,
        } => {
            let (table, column) = require_table(qualified(left)?)?;
            Ok((table, column, -interval_ms(right)?))
        }
        other => {
            let (table, column) = require_table(qualified(other)?)?;
            Ok((table, column, 0))
        }
    }
}

struct Qual {
    table: String,
    column: String,
}

fn qualified(expr: &Expr) -> Result<Qual, Error> {
    match expr {
        Expr::Nested(inner) => qualified(inner),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => Ok(Qual {
            table: parts[0].value.clone(),
            column: parts[1].value.clone(),
        }),
        _ => Err(Error::Sql(format!(
            "se esperaba alias.columna, llegó {expr}"
        ))),
    }
}

fn require_table(qual: Qual) -> Result<(String, String), Error> {
    Ok((qual.table, qual.column))
}

fn assign_pair(
    left: &str,
    right: &str,
    a: Qual,
    b: Qual,
) -> Result<(String, String), Error> {
    let left_col = if a.table == left {
        Some(a.column.clone())
    } else if b.table == left {
        Some(b.column.clone())
    } else {
        None
    };
    let right_col = if a.table == right {
        Some(a.column.clone())
    } else if b.table == right {
        Some(b.column.clone())
    } else {
        None
    };
    match (left_col, right_col) {
        (Some(left_col), Some(right_col)) => Ok((left_col, right_col)),
        _ => Err(Error::Sql(
            "la igualdad tiene que ser la clave de las dos tablas".to_string(),
        )),
    }
}

fn join_select(
    items: &[SelectItem],
    left: &str,
    right: &str,
) -> Result<Vec<JoinSelect>, Error> {
    if items.is_empty() {
        return Err(Error::Sql("el SELECT del join está vacío".to_string()));
    }
    let mut columns = Vec::new();
    for item in items {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            _ => {
                return Err(Error::Sql(
                    "el join no acepta SELECT *".to_string(),
                ))
            }
        };
        let qual = qualified(expr)?;
        let side_left = if qual.table == left {
            true
        } else if qual.table == right {
            false
        } else {
            return Err(Error::Sql(format!(
                "la columna {}.{} no es de las dos tablas del join",
                qual.table, qual.column
            )));
        };
        let alias = alias.unwrap_or_else(|| qual.column.clone());
        if columns.iter().any(|col: &JoinSelect| col.alias == alias) {
            return Err(Error::Sql(format!(
                "la columna de salida '{alias}' está repetida; hace falta un alias"
            )));
        }
        columns.push(JoinSelect {
            side_left,
            column: qual.column,
            alias,
        });
    }
    Ok(columns)
}

fn table_ref(factor: &TableFactor) -> Result<(String, Option<String>), Error> {
    match factor {
        TableFactor::Table { name, alias, .. } => Ok((
            table_name(name),
            alias.as_ref().map(|alias| alias.name.value.clone()),
        )),
        _ => Err(Error::Sql(
            "el join solo acepta tablas, no subqueries".to_string(),
        )),
    }
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

/// Columnas que un `SELECT` plano pide de una tabla.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableColumns {
    /// `SELECT *` o `SELECT tabla.*`.
    Star,
    /// Nombres de columna, en el orden del `SELECT`.
    Names(Vec<String>),
}

/// Un `SELECT` que solo nombra columnas de una tabla.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSelect {
    pub source: String,
    pub columns: TableColumns,
}

/// Acepta `SELECT col, ... FROM tabla` o `SELECT * FROM tabla`.
///
/// Un `WHERE`, un join o un agregado se rechazan: el cursor de snapshots
/// no puede avanzar dentro de un filtro que tire del lote siguiente.
pub fn table_select(sql: &str) -> Result<TableSelect, Error> {
    let statements = Parser::parse_sql(&GenericDialect {}, sql)
        .map_err(|e| Error::Sql(format!("error de sintaxis: {e}")))?;
    if statements.len() != 1 {
        return Err(Error::Sql(format!(
            "se esperaba exactamente 1 sentencia, hay {}",
            statements.len()
        )));
    }
    let Statement::Query(query) = &statements[0] else {
        return Err(Error::Sql(
            "leer una tabla es un SELECT de columnas".to_string(),
        ));
    };
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Err(Error::Sql(
            "leer una tabla es un SELECT de columnas, sin orden ni límite".to_string(),
        ));
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(Error::Sql(
            "leer una tabla es un SELECT de columnas".to_string(),
        ));
    };
    plain_select(select)?;
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err(Error::Sql(
            "leer una tabla no acepta JOIN".to_string(),
        ));
    }
    let (source, alias) = table_ref(&select.from[0].relation)?;
    let columns = plain_projection(&select.projection, &source, alias.as_deref())?;
    Ok(TableSelect { source, columns })
}

fn plain_select(select: &Select) -> Result<(), Error> {
    if select.selection.is_some() || select.prewhere.is_some() {
        return Err(Error::Sql(
            "leer una tabla no acepta WHERE".to_string(),
        ));
    }
    if select.having.is_some() || !group_is_empty(&select.group_by) {
        return Err(Error::Sql(
            "leer una tabla no acepta GROUP BY".to_string(),
        ));
    }
    if select.distinct.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || !select.lateral_views.is_empty()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
    {
        return Err(Error::Sql(
            "leer una tabla es un SELECT de columnas".to_string(),
        ));
    }
    Ok(())
}

fn group_is_empty(group: &GroupByExpr) -> bool {
    match group {
        GroupByExpr::Expressions(exprs, modifiers) => exprs.is_empty() && modifiers.is_empty(),
        GroupByExpr::All(_) => false,
    }
}

fn plain_projection(
    items: &[SelectItem],
    table: &str,
    alias: Option<&str>,
) -> Result<TableColumns, Error> {
    if items.is_empty() {
        return Err(Error::Sql("el SELECT está vacío".to_string()));
    }
    if items.len() == 1 {
        match &items[0] {
            SelectItem::Wildcard(options) => {
                plain_star(options)?;
                return Ok(TableColumns::Star);
            }
            SelectItem::QualifiedWildcard(kind, options) => {
                plain_star(options)?;
                let SelectItemQualifiedWildcardKind::ObjectName(name) = kind else {
                    return Err(Error::Sql(
                        "el asterisco tiene que ser de la tabla".to_string(),
                    ));
                };
                let qualifier = table_name(name);
                if qualifier != table && Some(qualifier.as_str()) != alias {
                    return Err(Error::Sql(format!(
                        "el asterisco de '{qualifier}' no es de '{table}'"
                    )));
                }
                return Ok(TableColumns::Star);
            }
            _ => {}
        }
    }
    let mut names = Vec::with_capacity(items.len());
    for item in items {
        let expr = match item {
            SelectItem::UnnamedExpr(expr) => expr,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                return Err(Error::Sql(
                    "el asterisco va solo en el SELECT".to_string(),
                ));
            }
            _ => {
                return Err(Error::Sql(
                    "el SELECT de una tabla nombra columnas, sin alias".to_string(),
                ));
            }
        };
        let name = plain_column(expr, table, alias)?;
        if names.iter().any(|have| have == &name) {
            return Err(Error::Sql(format!(
                "la columna '{name}' está repetida"
            )));
        }
        names.push(name);
    }
    Ok(TableColumns::Names(names))
}

fn plain_star(options: &WildcardAdditionalOptions) -> Result<(), Error> {
    if options.opt_exclude.is_some()
        || options.opt_except.is_some()
        || options.opt_ilike.is_some()
        || options.opt_rename.is_some()
        || options.opt_replace.is_some()
        || options.opt_alias.is_some()
    {
        return Err(Error::Sql(
            "el SELECT de una tabla no filtra el asterisco".to_string(),
        ));
    }
    Ok(())
}

fn plain_column(expr: &Expr, table: &str, alias: Option<&str>) -> Result<String, Error> {
    match expr {
        Expr::Nested(inner) => plain_column(inner, table, alias),
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let qualifier = parts[0].value.as_str();
            if qualifier != table && Some(qualifier) != alias {
                return Err(Error::Sql(format!(
                    "la columna {qualifier}.{} no es de '{table}'",
                    parts[1].value
                )));
            }
            Ok(parts[1].value.clone())
        }
        other => Err(Error::Sql(format!(
            "el SELECT de una tabla nombra columnas, llegó {other}"
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
    fn a_plain_select_keeps_the_columns() {
        let parsed = table_select("SELECT order_id, paid.amount FROM paid").unwrap();
        assert_eq!(parsed.source, "paid");
        assert_eq!(
            parsed.columns,
            TableColumns::Names(vec!["order_id".to_string(), "amount".to_string()])
        );
        let star = table_select("SELECT * FROM paid").unwrap();
        assert_eq!(star.columns, TableColumns::Star);
    }

    #[test]
    fn a_filtered_select_is_not_a_table_tail() {
        let err = table_select("SELECT order_id FROM paid WHERE amount > 0").unwrap_err();
        assert!(err.to_string().contains("WHERE"), "{err}");
        let err = table_select("SELECT order_id AS id FROM paid").unwrap_err();
        assert!(err.to_string().contains("alias"), "{err}");
        let err = table_select("SELECT a.order_id FROM a JOIN b ON a.order_id = b.order_id")
            .unwrap_err();
        assert!(err.to_string().contains("JOIN"), "{err}");
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
    fn an_interval_join_keeps_the_bounds_and_the_select() {
        let sql = "INSERT INTO paid_orders \
            SELECT o.order_id, o.amount, p.payment_id \
            FROM orders AS o \
            JOIN payments AS p \
              ON o.order_id = p.order_id \
             AND p.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '1' HOUR";
        let parsed = parse_sql(sql).unwrap();
        let join = parsed.join.expect("join");
        assert_eq!(join.left, "orders");
        assert_eq!(join.right, "payments");
        assert_eq!(join.left_key, "order_id");
        assert_eq!(join.right_key, "order_id");
        assert_eq!(join.left_time, "event_time");
        assert_eq!(join.right_time, "event_time");
        assert!(join.base_is_left);
        assert_eq!(join.lower_ms, 0);
        assert_eq!(join.upper_ms, 3_600_000);
        assert_eq!(
            join.columns
                .iter()
                .map(|col| col.alias.as_str())
                .collect::<Vec<_>>(),
            vec!["order_id", "amount", "payment_id"]
        );
    }

    #[test]
    fn a_join_without_an_interval_does_not_parse() {
        let sql = "INSERT INTO out SELECT a.x FROM a JOIN b ON a.id = b.id";
        let err = parse_sql(sql).unwrap_err();
        assert!(err.to_string().contains("BETWEEN"), "{err}");
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
