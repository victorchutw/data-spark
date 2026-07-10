//! Inferred-schema deep module: the single home for the "type story" of a load.
//!
//! A column's type is decided once here by inference over the observed values
//! and the same decision drives materialization into an Arrow [`RecordBatch`].
//! CSV cells arrive as text ([`from_text_columns`]) and JSONL cells arrive as
//! typed [`Value`]s ([`from_json_columns`]); both fold observations through the
//! [`InferredType`] lattice, so the two formats produce schemas the same way.
//! Everything type-related — the lattice, observation rules, materialization,
//! and the `schema_decision` shape — is private behind these two entry points.

use crate::LoadFailure;
use arrow_array::builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use serde_json::{json, Value};
use std::sync::Arc;

/// A materialized load: the typed Arrow batch plus the `schema_decision` shape
/// that the load report echoes back to the caller.
///
/// Rejected records (#8) will arrive here later as an **additive** `rejected`
/// field, so callers pattern-match by name rather than positionally. That
/// promise is why this is a struct today even though materialization currently
/// never rejects a row — do not collapse it back to a bare `RecordBatch`.
pub(crate) struct Materialized {
    pub(crate) batch: RecordBatch,
    pub(crate) schema_decision: Value,
}

/// The narrowest type observed across a column's values, before it is widened
/// to an Arrow [`DataType`]. `Null` is the identity for an all-absent column.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum InferredType {
    Null,
    Boolean,
    Int64,
    Float64,
    Utf8,
}

/// Materializes CSV columns, whose cells arrive untyped as text and are typed
/// by how they parse.
pub(crate) fn from_text_columns(
    field_names: Vec<String>,
    records: Vec<Vec<Option<String>>>,
) -> Result<Materialized, LoadFailure> {
    // CSV fields arrive untyped, so their types are inferred from the text values.
    let inferred_types = infer_text_types(field_names.len(), &records);
    let columns = inferred_types
        .iter()
        .enumerate()
        .map(|(column_index, inferred_type)| {
            build_text_array(*inferred_type, &records, column_index)
        })
        .collect::<Result<Vec<_>, _>>()?;
    materialize(field_names, inferred_types, columns)
}

/// Materializes JSONL columns from their parsed [`Value`]s directly, owning the
/// field projection (`None` / [`Value::Null`] → null) so a JSON string like
/// `"01234"` stays text instead of being round-tripped through a re-parse.
pub(crate) fn from_json_columns(
    field_names: Vec<String>,
    objects: Vec<serde_json::Map<String, Value>>,
) -> Result<Materialized, LoadFailure> {
    // JSON values carry their own type, so a field's type is inferred from the
    // observed JSON kinds rather than by re-parsing stringified values. This keeps
    // JSON strings like "01234" as text instead of retyping them as numbers.
    let mut inferred_types = vec![InferredType::Null; field_names.len()];
    for object in &objects {
        for (column_index, field_name) in field_names.iter().enumerate() {
            if let Some(value) = object.get(field_name) {
                inferred_types[column_index] =
                    inferred_types[column_index].merge(infer_json_type(value));
            }
        }
    }
    let inferred_types = inferred_types
        .into_iter()
        .map(default_null_to_text)
        .collect::<Vec<_>>();

    let columns = field_names
        .iter()
        .zip(inferred_types.iter())
        .enumerate()
        .map(|(column_index, (field_name, inferred_type))| {
            build_json_array(*inferred_type, &objects, field_name, column_index)
        })
        .collect::<Result<Vec<_>, _>>()?;
    materialize(field_names, inferred_types, columns)
}

/// Assembles a schema from the inferred types and the pre-built columns into a
/// [`RecordBatch`], deriving the `schema_decision` from the same schema so the
/// report can never disagree with the batch that was written.
fn materialize(
    field_names: Vec<String>,
    inferred_types: Vec<InferredType>,
    columns: Vec<ArrayRef>,
) -> Result<Materialized, LoadFailure> {
    let schema = Arc::new(Schema::new(
        field_names
            .iter()
            .zip(inferred_types.iter())
            .map(|(name, inferred_type)| Field::new(name, inferred_type.data_type(), true))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema, columns).map_err(|error| LoadFailure {
        code: "record_batch_creation_failed",
        message: format!("failed to create Arrow record batch: {error}"),
    })?;
    let schema_decision = inferred_schema_decision(batch.schema().as_ref());

    Ok(Materialized {
        batch,
        schema_decision,
    })
}

fn infer_text_types(field_count: usize, records: &[Vec<Option<String>>]) -> Vec<InferredType> {
    let mut inferred_types = vec![InferredType::Null; field_count];
    for record in records {
        for (column_index, value) in record.iter().enumerate() {
            if let Some(value) = value {
                inferred_types[column_index] =
                    inferred_types[column_index].merge(infer_text_type(value));
            }
        }
    }

    inferred_types
        .into_iter()
        .map(default_null_to_text)
        .collect()
}

fn default_null_to_text(inferred_type: InferredType) -> InferredType {
    if inferred_type == InferredType::Null {
        InferredType::Utf8
    } else {
        inferred_type
    }
}

fn build_text_array(
    inferred_type: InferredType,
    records: &[Vec<Option<String>>],
    column_index: usize,
) -> Result<ArrayRef, LoadFailure> {
    match inferred_type {
        InferredType::Null | InferredType::Utf8 => {
            let mut builder = StringBuilder::new();
            for record in records {
                match &record[column_index] {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        InferredType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for record in records {
                match &record[column_index] {
                    Some(value) => builder.append_value(
                        parse_bool(value)
                            .ok_or_else(|| coercion_failure(column_index, value, "boolean"))?,
                    ),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        InferredType::Int64 => {
            let mut builder = Int64Builder::new();
            for record in records {
                match &record[column_index] {
                    Some(value) => builder.append_value(
                        value
                            .parse::<i64>()
                            .map_err(|_| coercion_failure(column_index, value, "int64"))?,
                    ),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        InferredType::Float64 => {
            let mut builder = Float64Builder::new();
            for record in records {
                match &record[column_index] {
                    Some(value) => builder.append_value(
                        value
                            .parse::<f64>()
                            .map_err(|_| coercion_failure(column_index, value, "float64"))?,
                    ),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

/// Materializes a JSONL column straight from the parsed [`Value`]s, healing the
/// old JSON → String → re-parse round-trip: integers come from `as_i64`, floats
/// from `as_f64`, booleans are taken directly, and text columns reuse
/// [`json_scalar_to_string`]. The coercion arms are unreachable on an inferred
/// schema (inference only picks a type every cell already carries) but return a
/// clean failure rather than panicking if a pinned schema (#7) ever supplies one.
fn build_json_array(
    inferred_type: InferredType,
    objects: &[serde_json::Map<String, Value>],
    field_name: &str,
    column_index: usize,
) -> Result<ArrayRef, LoadFailure> {
    match inferred_type {
        InferredType::Null | InferredType::Utf8 => {
            let mut builder = StringBuilder::new();
            for object in objects {
                match json_scalar_to_string(object.get(field_name)) {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        InferredType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for object in objects {
                match object.get(field_name) {
                    None | Some(Value::Null) => builder.append_null(),
                    Some(Value::Bool(flag)) => builder.append_value(*flag),
                    Some(other) => {
                        return Err(coercion_failure(
                            column_index,
                            &other.to_string(),
                            "boolean",
                        ))
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        InferredType::Int64 => {
            let mut builder = Int64Builder::new();
            for object in objects {
                match object.get(field_name) {
                    None | Some(Value::Null) => builder.append_null(),
                    Some(Value::Number(number)) => match number.as_i64() {
                        Some(value) => builder.append_value(value),
                        None => {
                            return Err(coercion_failure(
                                column_index,
                                &number.to_string(),
                                "int64",
                            ))
                        }
                    },
                    Some(other) => {
                        return Err(coercion_failure(column_index, &other.to_string(), "int64"))
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        InferredType::Float64 => {
            let mut builder = Float64Builder::new();
            for object in objects {
                match object.get(field_name) {
                    None | Some(Value::Null) => builder.append_null(),
                    Some(Value::Number(number)) => match number.as_f64() {
                        Some(value) => builder.append_value(value),
                        None => {
                            return Err(coercion_failure(
                                column_index,
                                &number.to_string(),
                                "float64",
                            ))
                        }
                    },
                    Some(other) => {
                        return Err(coercion_failure(
                            column_index,
                            &other.to_string(),
                            "float64",
                        ))
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

/// Renders a JSON scalar as the text an inferred Utf8 column stores: strings
/// keep their value, other scalars stringify, and nested arrays/objects degrade
/// to their JSON representation. Absent or null values become an Arrow null.
fn json_scalar_to_string(value: Option<&Value>) -> Option<String> {
    match value {
        None | Some(Value::Null) => None,
        Some(Value::String(text)) => Some(text.clone()),
        Some(Value::Bool(flag)) => Some(flag.to_string()),
        Some(Value::Number(number)) => Some(number.to_string()),
        Some(other) => Some(other.to_string()),
    }
}

fn inferred_schema_decision(schema: &Schema) -> Value {
    json!({
        "mode": "inferred",
        "fields": schema
            .fields()
            .iter()
            .map(|field| {
                json!({
                    "name": field.name(),
                    "type": data_type_name(field.data_type()),
                    "nullable": field.is_nullable()
                })
            })
            .collect::<Vec<_>>()
    })
}

fn data_type_name(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Boolean => "boolean",
        DataType::Int64 => "int64",
        DataType::Float64 => "float64",
        DataType::Utf8 => "utf8",
        _ => "unsupported",
    }
}

fn coercion_failure(column_index: usize, value: &str, data_type: &str) -> LoadFailure {
    LoadFailure {
        code: "schema_coercion_failed",
        message: format!(
            "failed to coerce column {} value {:?} to {data_type}",
            column_index + 1,
            value
        ),
    }
}

impl InferredType {
    /// Widens two observed types to the narrowest type that can hold both.
    ///
    /// `Null` is the identity (an absent or null value constrains nothing),
    /// integers widen to floats when mixed, and any other disagreement falls
    /// back to text. Both source readers share this lattice so CSV and JSONL
    /// produce schemas the same way.
    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (InferredType::Null, other) => other,
            (current, InferredType::Null) => current,
            (current, other) if current == other => current,
            (InferredType::Int64, InferredType::Float64)
            | (InferredType::Float64, InferredType::Int64) => InferredType::Float64,
            _ => InferredType::Utf8,
        }
    }

    fn data_type(self) -> DataType {
        match self {
            InferredType::Null | InferredType::Utf8 => DataType::Utf8,
            InferredType::Boolean => DataType::Boolean,
            InferredType::Int64 => DataType::Int64,
            InferredType::Float64 => DataType::Float64,
        }
    }
}

/// Observes the type carried by a text value, as CSV fields have no other type
/// information than how they parse. Float64 requires a finite parse: text that
/// parses non-finite (the `inf` / `infinity` / `nan` spellings and overflow
/// that saturates to ±infinity) observes as text per ADR-0031.
fn infer_text_type(value: &str) -> InferredType {
    if parse_bool(value).is_some() {
        InferredType::Boolean
    } else if value.parse::<i64>().is_ok() {
        InferredType::Int64
    } else if value.parse::<f64>().is_ok_and(f64::is_finite) {
        InferredType::Float64
    } else {
        InferredType::Utf8
    }
}

/// Observes the type a JSON value already declares, so JSON strings stay text
/// even when they look numeric and nested values degrade to text.
fn infer_json_type(value: &Value) -> InferredType {
    match value {
        Value::Null => InferredType::Null,
        Value::Bool(_) => InferredType::Boolean,
        Value::Number(number) => {
            if number.is_i64() {
                InferredType::Int64
            } else {
                InferredType::Float64
            }
        }
        Value::String(_) | Value::Array(_) | Value::Object(_) => InferredType::Utf8,
    }
}

fn parse_bool(value: &str) -> Option<bool> {
    match value {
        "true" | "TRUE" | "True" => Some(true),
        "false" | "FALSE" | "False" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};

    const ALL: [InferredType; 5] = [
        InferredType::Null,
        InferredType::Boolean,
        InferredType::Int64,
        InferredType::Float64,
        InferredType::Utf8,
    ];

    // ---- InferredType lattice ----

    #[test]
    fn merge_is_commutative() {
        for a in ALL {
            for b in ALL {
                assert_eq!(
                    a.merge(b),
                    b.merge(a),
                    "merge not commutative for {a:?}, {b:?}"
                );
            }
        }
    }

    #[test]
    fn merge_is_associative() {
        for a in ALL {
            for b in ALL {
                for c in ALL {
                    assert_eq!(
                        a.merge(b).merge(c),
                        a.merge(b.merge(c)),
                        "merge not associative for {a:?}, {b:?}, {c:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn merge_treats_null_as_the_identity() {
        for a in ALL {
            assert_eq!(InferredType::Null.merge(a), a);
            assert_eq!(a.merge(InferredType::Null), a);
        }
    }

    #[test]
    fn merge_widens_by_the_type_lattice() {
        use InferredType::*;
        // Same observation is idempotent.
        for a in ALL {
            assert_eq!(a.merge(a), a);
        }
        // Int and Float widen to Float; every other disagreement falls to text.
        assert_eq!(Int64.merge(Float64), Float64);
        assert_eq!(Boolean.merge(Int64), Utf8);
        assert_eq!(Boolean.merge(Float64), Utf8);
        assert_eq!(Boolean.merge(Utf8), Utf8);
        assert_eq!(Int64.merge(Utf8), Utf8);
        assert_eq!(Float64.merge(Utf8), Utf8);
    }

    // ---- Observation rules (pinned as-is; policy call tracked in #22) ----

    #[test]
    fn infer_text_type_reads_the_narrowest_type_the_text_parses_into() {
        use InferredType::*;
        // Migrated smoke cases.
        assert_eq!(infer_text_type("true"), Boolean);
        assert_eq!(infer_text_type("42"), Int64);
        assert_eq!(infer_text_type("3.14"), Float64);
        assert_eq!(infer_text_type("data-spark"), Utf8);

        // Reverse-behaviour table: unintuitive but pinned to keep the refactor
        // behaviour-preserving. #22 tracks leading zeros.
        assert_eq!(infer_text_type("007"), Int64); // leading zeros survive as Int64
        assert_eq!(infer_text_type("+42"), Int64); // leading sign parses as Int64
        assert_eq!(infer_text_type("1e10"), Float64); // scientific notation is float
        assert_eq!(infer_text_type("99999999999999999999999"), Float64); // i64 overflow -> float
        assert_eq!(infer_text_type(" 42"), Utf8); // whitespace padding is not trimmed
        assert_eq!(infer_text_type("42 "), Utf8);

        // ADR-0031: only a finite parse observes Float64. Non-finite spellings
        // (any casing, optional sign) and f64-saturating overflow observe as text.
        assert_eq!(infer_text_type("inf"), Utf8);
        assert_eq!(infer_text_type("-inf"), Utf8);
        assert_eq!(infer_text_type("infinity"), Utf8);
        assert_eq!(infer_text_type("NaN"), Utf8);
        assert_eq!(infer_text_type("NAN"), Utf8);
        assert_eq!(infer_text_type("1e400"), Utf8); // saturates to +infinity
        assert_eq!(infer_text_type("1e-400"), Float64); // underflows to the finite 0.0
    }

    #[test]
    fn infer_text_type_treats_only_the_six_exact_bool_spellings_as_boolean() {
        use InferredType::*;
        for text in ["true", "false", "TRUE", "True", "FALSE", "False"] {
            assert_eq!(infer_text_type(text), Boolean, "{text} should be boolean");
        }
        // Near-misses are not boolean.
        assert_eq!(infer_text_type("TrUe"), Utf8);
        assert_eq!(infer_text_type("yes"), Utf8);
        assert_eq!(infer_text_type("t"), Utf8);
        assert_eq!(infer_text_type("1"), Int64); // numeric, not boolean
    }

    #[test]
    fn infer_json_type_reads_the_type_the_json_value_declares() {
        use InferredType::*;
        assert_eq!(infer_json_type(&json!(null)), Null);
        assert_eq!(infer_json_type(&json!(true)), Boolean);
        assert_eq!(infer_json_type(&json!(42)), Int64);
        assert_eq!(infer_json_type(&json!(-7)), Int64);
        assert_eq!(infer_json_type(&json!(3.5)), Float64);
        // u64 beyond i64::MAX is not an i64, so it observes as Float64.
        assert_eq!(infer_json_type(&json!(18446744073709551615_u64)), Float64);
        // A JSON string that looks numeric stays text — the JSONL heal's whole point.
        assert_eq!(infer_json_type(&json!("42")), Utf8);
        assert_eq!(infer_json_type(&json!("01234")), Utf8);
        // Nested composites degrade to text.
        assert_eq!(infer_json_type(&json!([1, 2])), Utf8);
        assert_eq!(infer_json_type(&json!({"k": "v"})), Utf8);
    }

    #[test]
    fn default_null_to_text_promotes_only_all_null_columns() {
        use InferredType::*;
        assert_eq!(default_null_to_text(Null), Utf8);
        assert_eq!(default_null_to_text(Boolean), Boolean);
        assert_eq!(default_null_to_text(Int64), Int64);
        assert_eq!(default_null_to_text(Float64), Float64);
        assert_eq!(default_null_to_text(Utf8), Utf8);
    }

    // ---- Materialization ----

    #[test]
    fn from_text_columns_infers_types_values_and_schema_decision() {
        let materialized = from_text_columns(
            names(&["id", "name", "total"]),
            vec![
                row(&[Some("1"), Some("Ada"), Some("42.50")]),
                row(&[Some("2"), Some("Grace"), Some("7.25")]),
            ],
        )
        .expect("materialize");
        let batch = &materialized.batch;

        assert_eq!(
            schema_types(batch),
            vec![DataType::Int64, DataType::Utf8, DataType::Float64]
        );
        assert_eq!(ints(batch, 0).value(0), 1);
        assert_eq!(ints(batch, 0).value(1), 2);
        assert_eq!(strings(batch, 1).value(0), "Ada");
        assert_eq!(strings(batch, 1).value(1), "Grace");
        assert_eq!(floats(batch, 2).value(0), 42.50);
        assert_eq!(floats(batch, 2).value(1), 7.25);

        assert_eq!(
            materialized.schema_decision,
            json!({
                "mode": "inferred",
                "fields": [
                    {"name": "id", "type": "int64", "nullable": true},
                    {"name": "name", "type": "utf8", "nullable": true},
                    {"name": "total", "type": "float64", "nullable": true}
                ]
            })
        );
    }

    #[test]
    fn from_text_columns_widens_mixed_columns_and_defaults_empty_columns_to_text() {
        let materialized = from_text_columns(
            names(&["mixed", "widened", "empty"]),
            vec![
                row(&[Some("1"), Some("1"), None]),
                row(&[Some("x"), Some("2.5"), None]),
            ],
        )
        .expect("materialize");
        let batch = &materialized.batch;

        // mixed Int + text -> Utf8; Int + Float -> Float64; all-empty -> Utf8 nulls.
        assert_eq!(
            schema_types(batch),
            vec![DataType::Utf8, DataType::Float64, DataType::Utf8]
        );
        // 'mixed' keeps the original text rather than a re-parsed value.
        assert_eq!(strings(batch, 0).value(0), "1");
        assert_eq!(strings(batch, 0).value(1), "x");
        // 'widened' materializes the integer cell as a float.
        assert_eq!(floats(batch, 1).value(0), 1.0);
        assert_eq!(floats(batch, 1).value(1), 2.5);
        // 'empty' is an all-null text column.
        assert_eq!(strings(batch, 2).len(), 2);
        assert!(strings(batch, 2).is_null(0));
        assert!(strings(batch, 2).is_null(1));
    }

    #[test]
    fn from_text_columns_keeps_a_column_with_non_finite_numeric_text_as_text() {
        // ADR-0031: `inf` observes as text, so a column mixing it with finite
        // numbers falls to Utf8 under the disagreements-fall-to-text merge rule
        // and stores the original strings verbatim.
        let materialized = from_text_columns(
            names(&["reading"]),
            vec![row(&[Some("1.5")]), row(&[Some("inf")])],
        )
        .expect("materialize");
        let batch = &materialized.batch;

        assert_eq!(schema_types(batch), vec![DataType::Utf8]);
        assert_eq!(strings(batch, 0).value(0), "1.5");
        assert_eq!(strings(batch, 0).value(1), "inf");
    }

    #[test]
    fn from_json_columns_keeps_numeric_strings_as_text_and_maps_absent_to_null() {
        // `zip` is a JSON string of digits: it must stay text (the heal), not be
        // retyped as a number. `active` is missing on the second row -> null.
        let objects = vec![
            json_object(r#"{"zip": "01234", "balance": 10, "active": true}"#),
            json_object(r#"{"zip": "00987", "balance": 5}"#),
        ];
        let materialized =
            from_json_columns(names(&["zip", "balance", "active"]), objects).expect("materialize");
        let batch = &materialized.batch;

        assert_eq!(
            schema_types(batch),
            vec![DataType::Utf8, DataType::Int64, DataType::Boolean]
        );
        assert_eq!(strings(batch, 0).value(0), "01234");
        assert_eq!(strings(batch, 0).value(1), "00987");
        assert_eq!(ints(batch, 1).value(0), 10);
        assert_eq!(ints(batch, 1).value(1), 5);
        assert!(bools(batch, 2).value(0));
        assert!(bools(batch, 2).is_null(1)); // missing field -> null
    }

    #[test]
    fn from_json_columns_heals_the_float_round_trip_bit_for_bit() {
        // A column mixing a JSON integer and a JSON float widens to Float64. The
        // healed path reads f64 straight from the number; assert the exact bits
        // the old String -> re-parse hop produced.
        let objects = vec![
            json_object(r#"{"amount": 10}"#),
            json_object(r#"{"amount": 42.5}"#),
        ];
        let materialized = from_json_columns(names(&["amount"]), objects).expect("materialize");
        let batch = &materialized.batch;

        assert_eq!(schema_types(batch), vec![DataType::Float64]);
        let amounts = floats(batch, 0);
        // Old path: Number(10).to_string() = "10" -> "10".parse::<f64>() = 10.0.
        assert_eq!(amounts.value(0).to_bits(), 10.0_f64.to_bits());
        // Old path: Number(42.5).to_string() = "42.5" -> parse = 42.5.
        assert_eq!(amounts.value(1).to_bits(), 42.5_f64.to_bits());
    }

    #[test]
    fn from_json_columns_stringifies_composites_and_reports_schema_decision() {
        let objects = vec![json_object(r#"{"tags": ["a", "b"], "meta": {"k": 1}}"#)];
        let materialized =
            from_json_columns(names(&["tags", "meta"]), objects).expect("materialize");
        let batch = &materialized.batch;

        assert_eq!(schema_types(batch), vec![DataType::Utf8, DataType::Utf8]);
        // Composites degrade to their compact JSON representation.
        assert_eq!(strings(batch, 0).value(0), "[\"a\",\"b\"]");
        assert_eq!(strings(batch, 1).value(0), "{\"k\":1}");

        assert_eq!(
            materialized.schema_decision,
            json!({
                "mode": "inferred",
                "fields": [
                    {"name": "tags", "type": "utf8", "nullable": true},
                    {"name": "meta", "type": "utf8", "nullable": true}
                ]
            })
        );
    }

    #[test]
    fn from_json_columns_defaults_all_null_columns_to_text() {
        let objects = vec![json_object(r#"{"note": null}"#), json_object("{}")];
        let materialized = from_json_columns(names(&["note"]), objects).expect("materialize");
        let batch = &materialized.batch;

        assert_eq!(schema_types(batch), vec![DataType::Utf8]);
        assert!(strings(batch, 0).is_null(0));
        assert!(strings(batch, 0).is_null(1));
    }

    // ---- Test helpers ----

    fn names(field_names: &[&str]) -> Vec<String> {
        field_names.iter().map(|name| name.to_string()).collect()
    }

    fn row(cells: &[Option<&str>]) -> Vec<Option<String>> {
        cells.iter().map(|cell| cell.map(str::to_string)).collect()
    }

    fn json_object(text: &str) -> serde_json::Map<String, Value> {
        match serde_json::from_str::<Value>(text).expect("valid json") {
            Value::Object(map) => map,
            _ => panic!("expected a JSON object"),
        }
    }

    fn schema_types(batch: &RecordBatch) -> Vec<DataType> {
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect()
    }

    fn strings(batch: &RecordBatch, index: usize) -> &StringArray {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 column")
    }

    fn ints(batch: &RecordBatch, index: usize) -> &Int64Array {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column")
    }

    fn floats(batch: &RecordBatch, index: usize) -> &Float64Array {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float64 column")
    }

    fn bools(batch: &RecordBatch, index: usize) -> &BooleanArray {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean column")
    }
}
