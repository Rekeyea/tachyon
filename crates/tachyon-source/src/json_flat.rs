//! Decoder JSON de objetos planos, directo a buffers Arrow.
//!
//! Es el camino de cada mensaje JSON cuyo schema solo tiene `Int64`,
//! `Float64`, `Utf8` y `Boolean`. Recorre el objeto una vez y escribe cada
//! valor en su columna: sin DOM intermedio, sin buscar la columna en un
//! `HashMap` por campo y sin un `String` por valor (los strings van a un solo
//! buffer por columna). El decode era la mitad del CPU de un ETL; este camino
//! cuesta una fracción de simd-json + DOM.
//!
//! Semántica (la misma que el fast path anterior sobre simd-json):
//! - un objeto JSON por mensaje; cualquier otra cosa es un error;
//! - campo ausente o `null` -> null de Arrow;
//! - un campo que no está en el schema se saltea (aunque sea anidado);
//! - clave repetida en el mismo objeto: gana la última;
//! - `Int64` acepta enteros; un número con `.` o exponente es un error;
//! - `Float64` acepta enteros y decimales;
//! - un tipo incompatible (string en una columna numérica, objeto en una
//!   columna escalar, ...) es un error con la fila y la columna.

use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;

/// Tipos que sabe materializar.
pub(crate) fn supported(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int64 | DataType::Float64 | DataType::Utf8 | DataType::Boolean
    )
}

#[derive(Clone)]
pub(crate) struct FlatJson {
    schema: SchemaRef,
    names: Vec<Vec<u8>>,
}

impl FlatJson {
    /// `schema` solo con tipos `supported`.
    pub(crate) fn new(schema: SchemaRef) -> Self {
        let names = schema
            .fields()
            .iter()
            .map(|f| f.name().as_bytes().to_vec())
            .collect();
        Self { schema, names }
    }

    pub(crate) fn decode(&self, values: &[&[u8]]) -> Result<RecordBatch> {
        let rows = values.len();
        let mut cols: Vec<Col> = self
            .schema
            .fields()
            .iter()
            .map(|f| Col::new(f.data_type(), rows))
            .collect();
        // `stamp[c] == fila + 1`: la columna ya recibió valor en esta fila.
        let mut stamp = vec![0usize; cols.len()];
        let mut scratch = Vec::new();
        for (row, payload) in values.iter().enumerate() {
            if simdutf8::basic::from_utf8(payload).is_err() {
                bail!("parseando JSON (fila {row}): el mensaje no es UTF-8");
            }
            let mark = row + 1;
            let mut p = Parser::new(payload);
            self.object(&mut p, &mut cols, &mut stamp, mark, &mut scratch)
                .map_err(|e| match e {
                    Fault::Syntax(msg) => anyhow!("parseando JSON (fila {row}): {msg}"),
                    Fault::Column(ci, msg) => anyhow!(
                        "decodificando la columna '{}' (fila {row}): {msg}",
                        self.schema.field(ci).name()
                    ),
                })?;
            for (ci, col) in cols.iter_mut().enumerate() {
                if stamp[ci] != mark {
                    col.push_null();
                }
            }
        }
        let arrays: Vec<ArrayRef> = cols.into_iter().map(Col::finish).collect();
        RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|e| anyhow!("construyendo RecordBatch JSON: {e}"))
    }

    fn object(
        &self,
        p: &mut Parser<'_>,
        cols: &mut [Col],
        stamp: &mut [usize],
        mark: usize,
        scratch: &mut Vec<u8>,
    ) -> Result<(), Fault> {
        p.ws();
        if p.next() != Some(b'{') {
            return Err(Fault::Syntax("se esperaba un objeto JSON por mensaje".into()));
        }
        p.ws();
        if p.peek() == Some(b'}') {
            p.pos += 1;
        } else {
            // Los productores suelen emitir los campos siempre en el mismo
            // orden: se prueba primero el que sigue al último encontrado.
            let mut hint = 0usize;
            loop {
                if p.next() != Some(b'"') {
                    return Err(Fault::Syntax("se esperaba una clave".into()));
                }
                let column = match p.string(scratch)? {
                    Text::Raw(start, end) => self.lookup(&p.buf[start..end], &mut hint),
                    Text::Scratch => self.lookup(scratch, &mut hint),
                };
                p.ws();
                if p.next() != Some(b':') {
                    return Err(Fault::Syntax("se esperaba ':' después de la clave".into()));
                }
                p.ws();
                match column {
                    Some(ci) => {
                        if stamp[ci] == mark {
                            cols[ci].pop();
                        }
                        stamp[ci] = mark;
                        cols[ci].value(p, scratch).map_err(|e| match e {
                            Fault::Syntax(msg) => Fault::Column(ci, msg),
                            other => other,
                        })?;
                    }
                    None => p.skip_value(scratch)?,
                }
                p.ws();
                match p.next() {
                    Some(b',') => p.ws(),
                    Some(b'}') => break,
                    _ => return Err(Fault::Syntax("se esperaba ',' o '}'".into())),
                }
            }
        }
        p.ws();
        if p.pos != p.buf.len() {
            return Err(Fault::Syntax("hay bytes después del objeto".into()));
        }
        Ok(())
    }

    fn lookup(&self, key: &[u8], hint: &mut usize) -> Option<usize> {
        if let Some(name) = self.names.get(*hint) {
            if name.as_slice() == key {
                *hint += 1;
                return Some(*hint - 1);
            }
        }
        let found = self.names.iter().position(|name| name.as_slice() == key)?;
        *hint = found + 1;
        Some(found)
    }
}

enum Fault {
    Syntax(String),
    Column(usize, String),
}

/// Un string ya leído: un rango sin escapes del buffer original, o el
/// contenido desescapado en `scratch`.
enum Text {
    Raw(usize, usize),
    Scratch,
}

struct Parser<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    #[inline]
    fn peek(&self) -> Option<u8> {
        self.buf.get(self.pos).copied()
    }

    #[inline]
    fn next(&mut self) -> Option<u8> {
        let byte = self.peek()?;
        self.pos += 1;
        Some(byte)
    }

    #[inline]
    fn ws(&mut self) {
        while let Some(b' ' | b'\n' | b'\r' | b'\t') = self.peek() {
            self.pos += 1;
        }
    }

    fn literal(&mut self, word: &[u8]) -> Result<(), Fault> {
        if self.buf[self.pos..].starts_with(word) {
            self.pos += word.len();
            Ok(())
        } else {
            Err(Fault::Syntax("literal JSON inválido".into()))
        }
    }

    /// Después de la comilla de apertura. Sin escapes devuelve el rango;
    /// con escapes desescapa en `scratch` (el input ya es UTF-8 válido).
    fn string(&mut self, scratch: &mut Vec<u8>) -> Result<Text, Fault> {
        let start = self.pos;
        let rest = &self.buf[start..];
        let Some(hit) = memchr::memchr2(b'"', b'\\', rest) else {
            return Err(Fault::Syntax("string sin cerrar".into()));
        };
        if rest[hit] == b'"' {
            self.pos = start + hit + 1;
            return Ok(Text::Raw(start, start + hit));
        }
        scratch.clear();
        scratch.extend_from_slice(&rest[..hit]);
        self.pos = start + hit;
        loop {
            match self.next() {
                None => return Err(Fault::Syntax("string sin cerrar".into())),
                Some(b'"') => return Ok(Text::Scratch),
                Some(b'\\') => self.escape(scratch)?,
                Some(byte) if byte < 0x20 => {
                    return Err(Fault::Syntax("carácter de control sin escapar en un string".into()))
                }
                Some(byte) => scratch.push(byte),
            }
        }
    }

    fn escape(&mut self, out: &mut Vec<u8>) -> Result<(), Fault> {
        let byte = self
            .next()
            .ok_or_else(|| Fault::Syntax("escape sin terminar".into()))?;
        let decoded = match byte {
            b'"' => b'"',
            b'\\' => b'\\',
            b'/' => b'/',
            b'b' => 0x08,
            b'f' => 0x0c,
            b'n' => b'\n',
            b'r' => b'\r',
            b't' => b'\t',
            b'u' => {
                let high = self.hex4()?;
                let code = if (0xD800..0xDC00).contains(&high) {
                    if self.next() != Some(b'\\') || self.next() != Some(b'u') {
                        return Err(Fault::Syntax("surrogate alto sin su par".into()));
                    }
                    let low = self.hex4()?;
                    if !(0xDC00..0xE000).contains(&low) {
                        return Err(Fault::Syntax("surrogate bajo inválido".into()));
                    }
                    0x10000 + ((high - 0xD800) << 10) + (low - 0xDC00)
                } else {
                    high
                };
                let ch = char::from_u32(code)
                    .ok_or_else(|| Fault::Syntax("escape \\u inválido".into()))?;
                let mut utf8 = [0u8; 4];
                out.extend_from_slice(ch.encode_utf8(&mut utf8).as_bytes());
                return Ok(());
            }
            _ => return Err(Fault::Syntax("escape inválido".into())),
        };
        out.push(decoded);
        Ok(())
    }

    fn hex4(&mut self) -> Result<u32, Fault> {
        let digits = self
            .buf
            .get(self.pos..self.pos + 4)
            .ok_or_else(|| Fault::Syntax("escape \\u incompleto".into()))?;
        let mut code = 0u32;
        for &digit in digits {
            let nibble = (digit as char)
                .to_digit(16)
                .ok_or_else(|| Fault::Syntax("escape \\u inválido".into()))?;
            code = code * 16 + nibble;
        }
        self.pos += 4;
        Ok(code)
    }

    /// Extensión de un número: `[-+0-9.eE]+`.
    fn number_span(&mut self) -> Result<&'a [u8], Fault> {
        let start = self.pos;
        while let Some(b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E') = self.peek() {
            self.pos += 1;
        }
        if self.pos == start {
            return Err(Fault::Syntax("se esperaba un número".into()));
        }
        Ok(&self.buf[start..self.pos])
    }

    fn skip_value(&mut self, scratch: &mut Vec<u8>) -> Result<(), Fault> {
        match self.peek() {
            Some(b'"') => {
                self.pos += 1;
                self.string(scratch).map(|_| ())
            }
            Some(b'{' | b'[') => self.skip_nested(scratch),
            Some(b't') => self.literal(b"true"),
            Some(b'f') => self.literal(b"false"),
            Some(b'n') => self.literal(b"null"),
            Some(_) => self.number_span().map(|_| ()),
            None => Err(Fault::Syntax("valor faltante".into())),
        }
    }

    fn skip_nested(&mut self, scratch: &mut Vec<u8>) -> Result<(), Fault> {
        let mut depth = 0usize;
        loop {
            match self.next() {
                None => return Err(Fault::Syntax("objeto o array sin cerrar".into())),
                Some(b'{' | b'[') => depth += 1,
                Some(b'}' | b']') => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(());
                    }
                }
                Some(b'"') => {
                    self.string(scratch)?;
                }
                Some(_) => {}
            }
        }
    }

    /// Qué hay en la posición actual, para el mensaje de tipo incompatible.
    fn describe(&self) -> &'static str {
        match self.peek() {
            Some(b'"') => "un string",
            Some(b'{') => "un objeto",
            Some(b'[') => "un array",
            Some(b't' | b'f') => "un booleano",
            Some(b'0'..=b'9' | b'-') => "un número",
            _ => "un valor inválido",
        }
    }
}

fn parse_i64(span: &[u8]) -> Result<i64, Fault> {
    let (negative, digits) = match span.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, span),
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return Err(Fault::Syntax(format!(
            "valor JSON incompatible con la columna Int64: {}",
            String::from_utf8_lossy(span)
        )));
    }
    let mut value: i64 = 0;
    for &digit in digits {
        let d = (digit - b'0') as i64;
        value = value
            .checked_mul(10)
            .and_then(|v| if negative { v.checked_sub(d) } else { v.checked_add(d) })
            .ok_or_else(|| Fault::Syntax("entero JSON fuera de rango para Int64".into()))?;
    }
    Ok(value)
}

fn parse_f64(span: &[u8]) -> Result<f64, Fault> {
    if let Some(value) = fast_decimal(span) {
        return Ok(value);
    }
    // SAFETY: `number_span` solo junta bytes ASCII (`[-+0-9.eE]`).
    let text = unsafe { std::str::from_utf8_unchecked(span) };
    text.parse::<f64>()
        .map_err(|_| Fault::Syntax(format!("número JSON inválido: {text}")))
}

/// Camino rápido de Clinger: un decimal sin exponente cuya mantisa entera
/// entra exacta en un f64 (< 2^53) y con hasta 22 decimales es `m / 10^k`
/// con los dos operandos exactos, y la división IEEE redondea al más
/// cercano: el mismo resultado que `str::parse`. Si no, `None`.
fn fast_decimal(span: &[u8]) -> Option<f64> {
    const POW10: [f64; 23] = [
        1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15,
        1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
    ];
    let (negative, digits) = match span.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, span),
    };
    let mut mantissa: u64 = 0;
    let mut decimals = 0usize;
    let mut seen_point = false;
    let mut seen_digit = false;
    for &byte in digits {
        match byte {
            b'0'..=b'9' => {
                mantissa = mantissa.checked_mul(10)?.checked_add(u64::from(byte - b'0'))?;
                seen_digit = true;
                if seen_point {
                    decimals += 1;
                }
            }
            b'.' if !seen_point => seen_point = true,
            _ => return None,
        }
    }
    if !seen_digit || mantissa >= (1u64 << 53) || decimals > 22 {
        return None;
    }
    let value = mantissa as f64 / POW10[decimals];
    Some(if negative { -value } else { value })
}

/// Columna en construcción. La validez es un `Vec<bool>` (se empaqueta al
/// final) para poder deshacer el último valor de una clave repetida.
enum Col {
    I64(Vec<i64>, Vec<bool>),
    F64(Vec<f64>, Vec<bool>),
    Utf8 {
        offsets: Vec<i32>,
        data: Vec<u8>,
        valid: Vec<bool>,
    },
    Bool(Vec<bool>, Vec<bool>),
}

impl Col {
    fn new(dt: &DataType, rows: usize) -> Self {
        match dt {
            DataType::Int64 => Col::I64(Vec::with_capacity(rows), Vec::with_capacity(rows)),
            DataType::Float64 => Col::F64(Vec::with_capacity(rows), Vec::with_capacity(rows)),
            DataType::Utf8 => {
                let mut offsets = Vec::with_capacity(rows + 1);
                offsets.push(0);
                Col::Utf8 {
                    offsets,
                    data: Vec::with_capacity(rows * 8),
                    valid: Vec::with_capacity(rows),
                }
            }
            DataType::Boolean => Col::Bool(Vec::with_capacity(rows), Vec::with_capacity(rows)),
            other => unreachable!("tipo no soportado por el decoder JSON plano: {other:?}"),
        }
    }

    fn push_null(&mut self) {
        match self {
            Col::I64(values, valid) => {
                values.push(0);
                valid.push(false);
            }
            Col::F64(values, valid) => {
                values.push(0.0);
                valid.push(false);
            }
            Col::Utf8 { offsets, data, valid } => {
                offsets.push(data.len() as i32);
                valid.push(false);
            }
            Col::Bool(values, valid) => {
                values.push(false);
                valid.push(false);
            }
        }
    }

    /// Deshace el valor de esta fila (clave repetida: gana la última).
    fn pop(&mut self) {
        match self {
            Col::I64(values, valid) => {
                values.pop();
                valid.pop();
            }
            Col::F64(values, valid) => {
                values.pop();
                valid.pop();
            }
            Col::Utf8 { offsets, data, valid } => {
                offsets.pop();
                data.truncate(*offsets.last().expect("offset inicial") as usize);
                valid.pop();
            }
            Col::Bool(values, valid) => {
                values.pop();
                valid.pop();
            }
        }
    }

    fn value(&mut self, p: &mut Parser<'_>, scratch: &mut Vec<u8>) -> Result<(), Fault> {
        if p.peek() == Some(b'n') {
            p.literal(b"null")?;
            self.push_null();
            return Ok(());
        }
        match self {
            Col::I64(values, valid) => {
                if !matches!(p.peek(), Some(b'0'..=b'9' | b'-')) {
                    return Err(Fault::Syntax(format!(
                        "valor JSON incompatible con la columna Int64: {}",
                        p.describe()
                    )));
                }
                values.push(parse_i64(p.number_span()?)?);
                valid.push(true);
            }
            Col::F64(values, valid) => {
                if !matches!(p.peek(), Some(b'0'..=b'9' | b'-')) {
                    return Err(Fault::Syntax(format!(
                        "valor JSON incompatible con la columna Float64: {}",
                        p.describe()
                    )));
                }
                values.push(parse_f64(p.number_span()?)?);
                valid.push(true);
            }
            Col::Utf8 { offsets, data, valid } => {
                if p.peek() != Some(b'"') {
                    return Err(Fault::Syntax(format!(
                        "valor JSON incompatible con la columna Utf8: {}",
                        p.describe()
                    )));
                }
                p.pos += 1;
                match p.string(scratch)? {
                    Text::Raw(start, end) => data.extend_from_slice(&p.buf[start..end]),
                    Text::Scratch => data.extend_from_slice(scratch),
                }
                let end = i32::try_from(data.len())
                    .map_err(|_| Fault::Syntax("columna Utf8 de más de 2 GiB en un lote".into()))?;
                offsets.push(end);
                valid.push(true);
            }
            Col::Bool(values, valid) => {
                match p.peek() {
                    Some(b't') => {
                        p.literal(b"true")?;
                        values.push(true);
                    }
                    Some(b'f') => {
                        p.literal(b"false")?;
                        values.push(false);
                    }
                    _ => {
                        return Err(Fault::Syntax(format!(
                            "valor JSON incompatible con la columna Boolean: {}",
                            p.describe()
                        )))
                    }
                }
                valid.push(true);
            }
        }
        Ok(())
    }

    fn finish(self) -> ArrayRef {
        fn nulls(valid: Vec<bool>) -> Option<NullBuffer> {
            // Sin nulos no hace falta empaquetar el bitmap.
            if valid.iter().all(|v| *v) {
                return None;
            }
            Some(NullBuffer::from(valid))
        }
        match self {
            Col::I64(values, valid) => {
                Arc::new(Int64Array::new(ScalarBuffer::from(values), nulls(valid)))
            }
            Col::F64(values, valid) => {
                Arc::new(Float64Array::new(ScalarBuffer::from(values), nulls(valid)))
            }
            Col::Utf8 { offsets, data, valid } => {
                // SAFETY: los offsets son crecientes por construcción (cada
                // push agrega al final de `data`) y `data` es UTF-8 válido:
                // cada mensaje se validó entero con simdutf8 y los escapes se
                // desescapan con `char::encode_utf8`.
                let offsets = unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(offsets)) };
                Arc::new(unsafe {
                    StringArray::new_unchecked(offsets, Buffer::from_vec(data), nulls(valid))
                })
            }
            Col::Bool(values, valid) => Arc::new(BooleanArray::new(
                BooleanBuffer::from(values),
                nulls(valid),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use arrow::datatypes::{Field, Schema};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("price", DataType::Float64, true),
            Field::new("ok", DataType::Boolean, true),
        ]))
    }

    fn decode(rows: &[&str]) -> Result<RecordBatch> {
        let values: Vec<&[u8]> = rows.iter().map(|r| r.as_bytes()).collect();
        FlatJson::new(schema()).decode(&values)
    }

    fn col<T: 'static>(batch: &RecordBatch, i: usize) -> &T {
        batch.column(i).as_any().downcast_ref::<T>().unwrap()
    }

    #[test]
    fn decodes_all_types_nulls_and_missing_fields() {
        let batch = decode(&[
            r#"{"id":1,"name":"a","price":2.5,"ok":true}"#,
            r#" { "ok" : false , "id" : -7 } "#,
            r#"{"id":null,"name":null,"price":3,"ok":null}"#,
            r#"{}"#,
        ])
        .unwrap();
        let ids = col::<Int64Array>(&batch, 0);
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), -7);
        assert!(ids.is_null(2) && ids.is_null(3));
        let names = col::<StringArray>(&batch, 1);
        assert_eq!(names.value(0), "a");
        assert!(names.is_null(1) && names.is_null(2));
        let prices = col::<Float64Array>(&batch, 2);
        assert_eq!(prices.value(0), 2.5);
        assert_eq!(prices.value(2), 3.0);
        let oks = col::<BooleanArray>(&batch, 3);
        assert!(oks.value(0));
        assert!(!oks.value(1));
        assert!(oks.is_null(2));
    }

    #[test]
    fn unescapes_strings_and_unicode() {
        let batch = decode(&[
            r#"{"name":"a\"b\\c\/d\n\t"}"#,
            r#"{"name":"é中😀"}"#,
            r#"{"name":"ñandú"}"#,
        ])
        .unwrap();
        let names = col::<StringArray>(&batch, 1);
        assert_eq!(names.value(0), "a\"b\\c/d\n\t");
        assert_eq!(names.value(1), "é中😀");
        assert_eq!(names.value(2), "ñandú");
    }

    #[test]
    fn skips_unknown_fields_even_nested() {
        let batch = decode(&[
            r#"{"extra":{"a":[1,{"b":"}]"}],"c":"x"},"id":5,"more":[true,null],"z":-1.5e3}"#,
        ])
        .unwrap();
        assert_eq!(col::<Int64Array>(&batch, 0).value(0), 5);
    }

    #[test]
    fn repeated_key_keeps_the_last_value() {
        let batch = decode(&[r#"{"name":"first","id":1,"name":"second","id":2}"#, r#"{"name":"x"}"#])
            .unwrap();
        assert_eq!(col::<Int64Array>(&batch, 0).value(0), 2);
        let names = col::<StringArray>(&batch, 1);
        assert_eq!(names.value(0), "second");
        assert_eq!(names.value(1), "x");
    }

    #[test]
    fn int64_edges() {
        let batch = decode(&[
            r#"{"id":9223372036854775807}"#,
            r#"{"id":-9223372036854775808}"#,
        ])
        .unwrap();
        let ids = col::<Int64Array>(&batch, 0);
        assert_eq!(ids.value(0), i64::MAX);
        assert_eq!(ids.value(1), i64::MIN);
        let err = decode(&[r#"{"id":9223372036854775808}"#]).unwrap_err();
        assert!(format!("{err:#}").contains("fuera de rango"), "{err:#}");
        let err = decode(&[r#"{"id":1.5}"#]).unwrap_err();
        assert!(format!("{err:#}").contains("columna 'id'"), "{err:#}");
    }

    #[test]
    fn floats_match_the_standard_parser() {
        let texts = ["0.1", "-2.5e-3", "1E10", "123456789.123456789", "5e-324"];
        let rows: Vec<String> = texts.iter().map(|t| format!(r#"{{"price":{t}}}"#)).collect();
        let refs: Vec<&str> = rows.iter().map(String::as_str).collect();
        let batch = decode(&refs).unwrap();
        let prices = col::<Float64Array>(&batch, 2);
        for (i, text) in texts.iter().enumerate() {
            assert_eq!(prices.value(i), text.parse::<f64>().unwrap(), "{text}");
        }
    }

    #[test]
    fn fast_decimals_are_bit_identical_to_the_standard_parser() {
        let mut texts: Vec<String> = [
            "0", "-0", "0.0", "1", "-1", "0.1", "0.2", "0.3", "1.5", "100.5", "-2.25", "3.14159",
            "123456789.123456789", "9007199254740991", "9007199254740993", "0.000000000000000000001",
            "1.0000000000000002", "4503599627370497.5", "12.", ".5",
        ]
        .iter()
        .map(|t| t.to_string())
        .collect();
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        for _ in 0..20_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let int = x % 10_000_000;
            let frac_digits = (x >> 40) % 12;
            let frac = (x >> 20) % 10u64.pow(frac_digits as u32).max(1);
            texts.push(format!("{int}.{frac:0width$}", width = frac_digits as usize));
        }
        for text in &texts {
            if let Some(fast) = fast_decimal(text.as_bytes()) {
                let reference: f64 = text.parse().unwrap();
                assert_eq!(fast.to_bits(), reference.to_bits(), "{text}");
            }
        }
        assert!(fast_decimal(b"1e5").is_none());
        assert!(fast_decimal(b"9007199254740993").is_none(), "mantisa >= 2^53");
    }

    #[test]
    fn errors_name_the_row_and_column() {
        let cases = [
            (r#"{"id":"7"}"#, "columna 'id'"),
            (r#"{"name":3}"#, "columna 'name'"),
            (r#"{"ok":1}"#, "columna 'ok'"),
            (r#"{"price":{}}"#, "columna 'price'"),
            (r#"[1,2]"#, "objeto JSON"),
            (r#"{"id":1} x"#, "después del objeto"),
            (r#"{"id":1"#, "','"),
            (r#"{"name":"abc}"#, "sin cerrar"),
        ];
        for (text, expected) in cases {
            let err = decode(&[r#"{"id":1}"#, text]).unwrap_err();
            let msg = format!("{err:#}");
            assert!(msg.contains(expected), "{text}: {msg}");
            assert!(msg.contains("fila 1"), "{text}: {msg}");
        }
        let invalid: Vec<&[u8]> = vec![b"{\"name\":\"\xff\"}"];
        let err = FlatJson::new(schema()).decode(&invalid).unwrap_err();
        assert!(format!("{err:#}").contains("UTF-8"), "{err:#}");
    }

    #[test]
    fn a_non_nullable_column_rejects_a_missing_value() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let err = FlatJson::new(schema)
            .decode(&[b"{}".as_slice()])
            .unwrap_err();
        assert!(format!("{err:#}").contains("null"), "{err:#}");
    }

    #[test]
    fn matches_arrow_json_on_generated_rows() {
        // Diferencial contra arrow-json (el camino general): mismos valores.
        let mut rows = Vec::new();
        for i in 0..500i64 {
            let name = match i % 4 {
                0 => format!(r#""n{i}""#),
                1 => r#""es\"cápe""#.to_string(),
                2 => "null".to_string(),
                _ => format!(r#""{}""#, "x".repeat((i % 13) as usize)),
            };
            let price = if i % 5 == 0 { "null".to_string() } else { format!("{}.{}", i, i % 7) };
            let ok = if i % 3 == 0 { "true" } else { "false" };
            let row = if i % 6 == 0 {
                format!(r#"{{"price":{price},"id":{i},"ok":{ok}}}"#)
            } else {
                format!(r#"{{"id":{},"name":{name},"price":{price},"ok":{ok},"skip":[1,2]}}"#, i * 1_000_003)
            };
            rows.push(row);
        }
        let values: Vec<&[u8]> = rows.iter().map(|r| r.as_bytes()).collect();
        let flat = FlatJson::new(schema()).decode(&values).unwrap();
        let mut ndjson = Vec::new();
        for r in &rows {
            ndjson.extend_from_slice(r.as_bytes());
            ndjson.push(b'\n');
        }
        let reference = arrow_json::ReaderBuilder::new(schema())
            .with_batch_size(1_000)
            .build(std::io::BufReader::new(ndjson.as_slice()))
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(flat, reference);
    }
}
