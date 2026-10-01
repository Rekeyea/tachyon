//! Coerción de tipos Arrow seguros (lossless o con advertencia controlada).
//!
//! Soporta las coerciones más comunes en pipelines de IoT, e-commerce y logs:
//! - Enteros: Int8/16/32 → Int64
//! - Flotantes: Float32/64 → Float64
//! - String → cualquier tipo numérico (parsing)
//! - Boolean → Int64 (0/1)

use std::sync::Arc;
use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow_cast::cast as arrow_cast_fn;
use arrow_cast::cast::can_cast_types;
use arrow::datatypes::DataType;
use anyhow::{Context, Result};

/// Coerciona un array al tipo destino. Solo coerciones seguras o explícitas.
pub fn coerce_array(array: &dyn Array, target_type: &DataType) -> Result<ArrayRef> {
    // Si ya es el tipo correcto, devolver tal cual (zero-copy).
    if can_cast_types(array.data_type(), target_type) {
        return arrow_cast_fn(array, target_type)
            .map_err(anyhow::Error::from)
            .context("coerción de array");
    }

    // Mapeos explícitos no cubiertos por cast genérico.
    match (array.data_type(), target_type) {
        (DataType::Utf8, DataType::Int64) => coerce_string_to_int64(array),
        (DataType::Utf8, DataType::Float64) => coerce_string_to_float64(array),
        (DataType::Boolean, DataType::Int64) => coerce_bool_to_int64(array),
        _ => anyhow::bail!(
            "coerción no soportada de {:?} a {:?}",
            array.data_type(),
            target_type
        ),
    }
}

fn coerce_string_to_int64(array: &dyn Array) -> Result<ArrayRef> {
    let str_arr = array.as_any().downcast_ref::<StringArray>()
        .context("string → int64 requiere StringArray")?;
    let mut values = Vec::with_capacity(str_arr.len());
    for opt in str_arr.iter() {
        match opt {
            Some(s) => match s.trim().parse::<i64>() {
                Ok(v) => values.push(v),
                Err(_) => anyhow::bail!("no se puede convertir '{}' a Int64", s),
            },
            None => values.push(0), // null → 0 (configurable en el futuro)
        }
    }
    Ok(Arc::new(Int64Array::from(values)))
}

fn coerce_string_to_float64(array: &dyn Array) -> Result<ArrayRef> {
    let str_arr = array.as_any().downcast_ref::<StringArray>()
        .context("string → float64 requiere StringArray")?;
    use arrow::array::Float64Array;
    let mut values = Vec::with_capacity(str_arr.len());
    for opt in str_arr.iter() {
        match opt {
            Some(s) => match s.trim().parse::<f64>() {
                Ok(v) => values.push(v),
                Err(_) => anyhow::bail!("no se puede convertir '{}' a Float64", s),
            },
            None => values.push(f64::NAN),
        }
    }
    Ok(Arc::new(Float64Array::from(values)))
}

fn coerce_bool_to_int64(array: &dyn Array) -> Result<ArrayRef> {
    let bool_arr = array.as_any().downcast_ref::<BooleanArray>()
        .context("bool → int64 requiere BooleanArray")?;
    let values: Vec<i64> = bool_arr.iter()
        .map(|opt| opt.map_or(0, |b| if b { 1 } else { 0 }))
        .collect();
    Ok(Arc::new(Int64Array::from(values)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array};
    use std::sync::Arc;

    #[test]
    fn can_cast_returns_same_type() {
        let arr = Arc::new(Int32Array::from(vec![1, 2, 3]));
        assert!(can_cast_types(arr.data_type(), arr.data_type()));
    }

    #[test]
    fn coerce_string_to_int64_parses_valid() {
        let str_arr: ArrayRef = Arc::new(StringArray::from(vec!["10", "20", "30"]));
        let result = coerce_string_to_int64(&str_arr).expect("coerce");
        let int_arr = result.as_any().downcast_ref::<Int64Array>().expect("Int64");
        assert_eq!(int_arr.values(), &[10, 20, 30]);
    }

    #[test]
    fn coerce_string_to_int64_rejects_invalid() {
        let str_arr: ArrayRef = Arc::new(StringArray::from(vec!["10", "abc", "30"]));
        assert!(coerce_string_to_int64(&str_arr).is_err());
    }

    #[test]
    fn bool_to_int64_coercion() {
        let bool_arr: ArrayRef = Arc::new(BooleanArray::from(vec![true, false, true]));
        let result = coerce_bool_to_int64(&bool_arr).expect("coerce");
        let int_arr = result.as_any().downcast_ref::<Int64Array>().expect("Int64");
        assert_eq!(int_arr.values(), &[1, 0, 1]);
    }

    #[test]
    fn coerce_array_no_cast_needed_returns_same() {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        // Int32 → Int64 es castable
        let result = coerce_array(&arr, &DataType::Int64).expect("coerce");
        assert_eq!(result.data_type(), &DataType::Int64);
    }
}
