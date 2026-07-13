//! Schema deep module: the single home for the "type story" of a load.
//!
//! A column's type is decided once here — by inference over the observed
//! values, or by validating those observations against a pinned schema
//! ([`SchemaDirective`]) — and the same decision drives materialization into an
//! Arrow [`RecordBatch`]. CSV cells arrive as text ([`from_text_columns`]) and
//! JSONL cells arrive as typed [`Value`]s ([`from_json_columns`]); both fold
//! observations through the [`InferredType`] lattice, so the two formats
//! produce and validate schemas the same way: a value fits a pinned field iff
//! its observed type widens to the pinned type under that lattice (ADR-0034).
//! Source shape problems — a missing pinned field, an added field the drift
//! policy does not allow, duplicate source field names — fail the whole load
//! as `schema_drift`, while value fit is judged per record: a record whose
//! cell misfits its pinned type or leaves a `nullable: false` field null
//! becomes a rejected record instead of failing the load (ADR-0035).
//! Everything type-related — the lattice, observation rules, the pinned schema
//! file contract ([`PinnedSchema`], ADR-0033), drift comparison, per-record
//! validation, materialization, and the `schema_decision` shape — is private
//! behind these two entry points.

use crate::rejection::{self, RejectedRecord};
use crate::{ExecutionFailure, LoadFailure};
use arrow_array::builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const PINNED_SCHEMA_VERSION: u64 = 1;

/// A materialized load: the typed Arrow batch of the surviving records, the
/// `schema_decision` shape that the load report echoes back to the caller,
/// the pinned schema file write the caller performs when this load produces
/// or extends a pin (ADR-0033), and the records per-record validation
/// rejected (ADR-0035) — the caller owns the artifact write and the reject
/// threshold, so this module stays types-only.
pub(crate) struct Materialized {
    pub(crate) batch: RecordBatch,
    pub(crate) schema_decision: Value,
    pub(crate) pinned_schema_write: Option<PinnedSchemaWrite>,
    pub(crate) rejected: Vec<RejectedRecord>,
}

/// A pinned schema file write the load must perform: the path the load
/// definition named and the YAML text to persist there. Produced only when a
/// load creates the pin (first pin-requesting load) or extends it (additive
/// drift), so the path always travels with the text it belongs to.
pub(crate) struct PinnedSchemaWrite {
    pub(crate) pinned_path: String,
    pub(crate) yaml: String,
}

/// One CSV record positioned at its source line, with one cell per observed
/// field in header order. The line travels with the record so a rejection can
/// point back into the source file.
pub(crate) struct TextRecord {
    pub(crate) line: u64,
    pub(crate) cells: Vec<Option<String>>,
}

/// One JSONL record positioned at its source line, as the parsed JSON object.
pub(crate) struct JsonRecord {
    pub(crate) line: u64,
    pub(crate) object: serde_json::Map<String, Value>,
}

/// How a load decides its dataset schema: infer it from observed records,
/// infer it and persist it as the new pinned schema (the first pin-requesting
/// load, ADR-0033), or validate observed records against an existing pinned
/// schema (ADR-0034). `pinned_path` is the display path the schema decision
/// reports; file I/O stays with the caller.
pub(crate) enum SchemaDirective {
    Inferred,
    PinInferred {
        pinned_path: String,
    },
    Pinned {
        pinned_path: String,
        pin: PinnedSchema,
        drift_policy: DriftPolicy,
    },
}

/// The rule that decides whether a load may continue when schema drift is
/// detected against a pinned schema: fail fast by default, or allow additive
/// nullable drift when the load definition explicitly permits it (ADR-0007).
pub(crate) enum DriftPolicy {
    Fail,
    AllowAdditiveNullable,
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

/// A pinned schema: the dataset schema a load definition reuses across loads to
/// keep a BI-ready dataset stable (ADR-0033). Parsed from and serialized to the
/// versioned YAML pinned schema file named by `schema.pinned_path`.
#[derive(Debug, PartialEq)]
pub(crate) struct PinnedSchema {
    fields: Vec<PinnedField>,
}

/// One field of a pinned schema. A `nullable: false` field is a required
/// field: a record that leaves it null is rejected per record (ADR-0035).
#[derive(Debug, PartialEq)]
struct PinnedField {
    name: String,
    field_type: InferredType,
    nullable: bool,
}

/// The raw serde shape of a pinned schema file, kept separate from
/// [`PinnedSchema`] so contract validation happens in one place after parsing.
#[derive(Deserialize, Serialize)]
struct PinnedSchemaFile {
    version: Option<u64>,
    fields: Option<Vec<PinnedFieldFile>>,
}

#[derive(Deserialize, Serialize)]
struct PinnedFieldFile {
    name: String,
    #[serde(rename = "type")]
    field_type: String,
    nullable: Option<bool>,
}

impl PinnedSchema {
    /// Parses a pinned schema file's YAML text, validating the contract:
    /// `version: 1`, at least one field, unique field names, and known field
    /// types. `nullable` defaults to true; a `nullable: false` field declares
    /// a required field.
    pub(crate) fn from_yaml(text: &str) -> Result<Self, LoadFailure> {
        let file = serde_yaml::from_str::<PinnedSchemaFile>(text).map_err(|error| {
            invalid_pinned_schema(format!("failed to parse pinned schema: {error}"))
        })?;
        match file.version {
            Some(PINNED_SCHEMA_VERSION) => {}
            Some(version) => {
                return Err(invalid_pinned_schema(format!(
                    "unsupported pinned schema version: {version}"
                )))
            }
            None => {
                return Err(invalid_pinned_schema(
                    "pinned schema version is required".to_string(),
                ))
            }
        }

        let raw_fields = file.fields.unwrap_or_default();
        if raw_fields.is_empty() {
            return Err(invalid_pinned_schema(
                "pinned schema must declare at least one field".to_string(),
            ));
        }

        let mut seen_names = HashSet::new();
        let mut fields = Vec::with_capacity(raw_fields.len());
        for raw_field in raw_fields {
            if !seen_names.insert(raw_field.name.clone()) {
                return Err(invalid_pinned_schema(format!(
                    "pinned schema field {:?} is declared more than once",
                    raw_field.name
                )));
            }
            let field_type = parse_type_name(&raw_field.field_type).ok_or_else(|| {
                invalid_pinned_schema(format!(
                    "unsupported pinned schema field type: {}",
                    raw_field.field_type
                ))
            })?;
            fields.push(PinnedField {
                name: raw_field.name,
                field_type,
                nullable: raw_field.nullable.unwrap_or(true),
            });
        }

        Ok(PinnedSchema { fields })
    }

    /// Renders the pinned schema as the YAML text the pinned schema file
    /// persists.
    pub(crate) fn to_yaml(&self) -> String {
        let file = PinnedSchemaFile {
            version: Some(PINNED_SCHEMA_VERSION),
            fields: Some(
                self.fields
                    .iter()
                    .map(|field| PinnedFieldFile {
                        name: field.name.clone(),
                        field_type: data_type_name(&field.field_type.data_type()).to_string(),
                        nullable: Some(field.nullable),
                    })
                    .collect(),
            ),
        };
        serde_yaml::to_string(&file).expect("pinned schema serializes to yaml")
    }
}

fn invalid_pinned_schema(message: String) -> LoadFailure {
    LoadFailure {
        code: "invalid_pinned_schema",
        message,
    }
}

fn parse_type_name(name: &str) -> Option<InferredType> {
    match name {
        "boolean" => Some(InferredType::Boolean),
        "int64" => Some(InferredType::Int64),
        "float64" => Some(InferredType::Float64),
        "utf8" => Some(InferredType::Utf8),
        _ => None,
    }
}

/// Materializes CSV columns, whose cells arrive untyped as text and are typed
/// by how they parse, under the load's schema directive.
pub(crate) fn from_text_columns(
    directive: &SchemaDirective,
    field_names: Vec<String>,
    records: Vec<TextRecord>,
) -> Result<Materialized, ExecutionFailure> {
    match directive {
        SchemaDirective::Inferred => {
            let plan = inferred_text_plan(&field_names, &records, None);
            build_text(plan, &records, Vec::new())
        }
        SchemaDirective::PinInferred { pinned_path } => {
            let plan = inferred_text_plan(&field_names, &records, Some(pinned_path));
            build_text(plan, &records, Vec::new())
        }
        SchemaDirective::Pinned {
            pinned_path,
            pin,
            drift_policy,
        } => {
            let ShapeMatch { matched, added } =
                match_shape(pinned_path, pin, drift_policy, &field_names)?;
            let mut survivors = Vec::with_capacity(records.len());
            let mut rejected = Vec::new();
            for record in records {
                match validate_text_record(&record, &matched, &field_names) {
                    Some(rejection) => rejected.push(rejection),
                    None => survivors.push(record),
                }
            }
            // Added fields take their types from the surviving records only,
            // so a rejected record's values never shape the destination.
            let survivor_types = observe_text_types(field_names.len(), &survivors);
            let added = planned_added_fields(added, &survivor_types);
            let plan = assemble_pinned_plan(pinned_path, matched, added);
            build_text(plan, &survivors, rejected)
        }
    }
}

/// Materializes JSONL columns from their parsed [`Value`]s directly, owning the
/// field projection (`None` / [`Value::Null`] → null) so a JSON string like
/// `"01234"` stays text instead of being round-tripped through a re-parse.
pub(crate) fn from_json_columns(
    directive: &SchemaDirective,
    field_names: Vec<String>,
    records: Vec<JsonRecord>,
) -> Result<Materialized, ExecutionFailure> {
    match directive {
        SchemaDirective::Inferred => {
            let plan = inferred_json_plan(&field_names, &records, None);
            build_json(plan, &records, Vec::new())
        }
        SchemaDirective::PinInferred { pinned_path } => {
            let plan = inferred_json_plan(&field_names, &records, Some(pinned_path));
            build_json(plan, &records, Vec::new())
        }
        SchemaDirective::Pinned {
            pinned_path,
            pin,
            drift_policy,
        } => {
            let ShapeMatch { matched, added } =
                match_shape(pinned_path, pin, drift_policy, &field_names)?;
            let mut survivors = Vec::with_capacity(records.len());
            let mut rejected = Vec::new();
            for record in records {
                match validate_json_record(&record, &matched) {
                    Some(rejection) => rejected.push(rejection),
                    None => survivors.push(record),
                }
            }
            let survivor_types = observe_json_types(&field_names, &survivors);
            let added = planned_added_fields(added, &survivor_types);
            let plan = assemble_pinned_plan(pinned_path, matched, added);
            build_json(plan, &survivors, rejected)
        }
    }
}

/// One field the load will materialize: its output name, the type its column is
/// built as (never `Null`), whether its values may be null, and the index of
/// the observed source column it reads from.
struct PlannedField {
    name: String,
    materialized_type: InferredType,
    nullable: bool,
    observed_index: usize,
}

/// The resolved field plan for a load: the fields to materialize in output
/// order, the `schema_decision` the report echoes, and the pinned schema file
/// write to perform when this load produces or extends a pin.
struct FieldPlan {
    fields: Vec<PlannedField>,
    decision: Value,
    pinned_schema_write: Option<PinnedSchemaWrite>,
}

/// Plans an inference-driven CSV load: every observed column keeps its
/// observed type. Inference derives types from the records themselves, so an
/// inference-driven load never rejects a record.
fn inferred_text_plan(
    field_names: &[String],
    records: &[TextRecord],
    pinned_path: Option<&str>,
) -> FieldPlan {
    let observed_types = observe_text_types(field_names.len(), records);
    inferred_plan(field_names, &observed_types, pinned_path)
}

/// Plans an inference-driven JSONL load; see [`inferred_text_plan`].
fn inferred_json_plan(
    field_names: &[String],
    records: &[JsonRecord],
    pinned_path: Option<&str>,
) -> FieldPlan {
    let observed_types = observe_json_types(field_names, records);
    inferred_plan(field_names, &observed_types, pinned_path)
}

/// Plans an inference-driven load: every observed column keeps its observed
/// type (all-null columns default to text). With a `pinned_path`, the inferred
/// schema is also rendered for persistence as the new pin (ADR-0033).
fn inferred_plan(
    observed_names: &[String],
    observed_types: &[InferredType],
    pinned_path: Option<&str>,
) -> FieldPlan {
    let fields = observed_names
        .iter()
        .zip(observed_types)
        .enumerate()
        .map(|(observed_index, (name, observed_type))| PlannedField {
            name: name.clone(),
            materialized_type: default_null_to_text(*observed_type),
            nullable: true,
            observed_index,
        })
        .collect::<Vec<_>>();

    let mut decision = json!({
        "mode": "inferred",
        "fields": fields_json(&fields),
        "drift_status": "not_applicable",
    });
    let pinned_schema_write = pinned_path.map(|path| {
        decision["pinned_schema_path"] = json!(path);
        decision["pinned_schema_persisted"] = json!(true);
        PinnedSchemaWrite {
            pinned_path: path.to_string(),
            yaml: pin_yaml(&fields),
        }
    });

    FieldPlan {
        fields,
        decision,
        pinned_schema_write,
    }
}

/// The outcome of matching observed source fields against the pin by name:
/// the pin's fields planned in pin order, and the added fields (name and
/// observed column index) awaiting their survivor-observed types.
struct ShapeMatch {
    matched: Vec<PlannedField>,
    added: Vec<(String, usize)>,
}

/// Matches the observed source fields against the pinned schema by name and
/// fails with `schema_drift` on shape drift: duplicate source field names, a
/// pinned field absent from every record, or an added field the drift policy
/// does not allow (ADR-0034). Value fit is not judged here — that is
/// per-record work (ADR-0035).
fn match_shape(
    pinned_path: &str,
    pin: &PinnedSchema,
    drift_policy: &DriftPolicy,
    observed_names: &[String],
) -> Result<ShapeMatch, ExecutionFailure> {
    // Observed columns match pin fields by name, so duplicate observed names
    // are unmatchable shape drift.
    let mut observed_indexes = HashMap::with_capacity(observed_names.len());
    for (index, name) in observed_names.iter().enumerate() {
        if observed_indexes.insert(name.as_str(), index).is_some() {
            return Err(drift_failure(
                pinned_path,
                pin,
                format!("source field {name:?} appears more than once, so records cannot be validated against the pinned schema"),
                json!({ "duplicate_fields": [name] }),
            ));
        }
    }

    let mut missing_fields = Vec::new();
    let mut matched = Vec::new();
    for pin_field in &pin.fields {
        match observed_indexes.get(pin_field.name.as_str()) {
            None => missing_fields.push(pin_field.name.clone()),
            Some(&observed_index) => matched.push(PlannedField {
                name: pin_field.name.clone(),
                materialized_type: pin_field.field_type,
                nullable: pin_field.nullable,
                observed_index,
            }),
        }
    }
    let pinned_names = pin
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<HashSet<_>>();
    let added = observed_names
        .iter()
        .enumerate()
        .filter(|(_, name)| !pinned_names.contains(name.as_str()))
        .map(|(observed_index, name)| (name.clone(), observed_index))
        .collect::<Vec<_>>();

    let additive_allowed = matches!(drift_policy, DriftPolicy::AllowAdditiveNullable);
    if !missing_fields.is_empty() || (!added.is_empty() && !additive_allowed) {
        let added_names = added
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>();
        let mut segments = Vec::new();
        if !missing_fields.is_empty() {
            segments.push(format!("missing fields: {}", missing_fields.join(", ")));
        }
        if !added_names.is_empty() {
            segments.push(format!("added fields: {}", added_names.join(", ")));
        }
        return Err(drift_failure(
            pinned_path,
            pin,
            segments.join("; "),
            json!({
                "missing_fields": missing_fields,
                "added_fields": added_names,
            }),
        ));
    }

    Ok(ShapeMatch { matched, added })
}

/// Validates one CSV record against the matched pinned fields, in pin order:
/// the first null cell in a non-nullable field or the first cell whose
/// observed type does not widen to its pinned type rejects the record
/// (ADR-0035).
fn validate_text_record(
    record: &TextRecord,
    matched: &[PlannedField],
    field_names: &[String],
) -> Option<RejectedRecord> {
    for planned in matched {
        match record.cells[planned.observed_index].as_deref() {
            None => {
                if !planned.nullable {
                    return Some(required_field_rejection(
                        record.line,
                        planned,
                        text_record_json(field_names, &record.cells),
                    ));
                }
            }
            Some(value) => {
                if !fits_pinned_type(infer_text_type(value), planned.materialized_type) {
                    return Some(type_rejection(
                        record.line,
                        planned,
                        json!(value),
                        text_record_json(field_names, &record.cells),
                    ));
                }
            }
        }
    }
    None
}

/// Validates one JSONL record against the matched pinned fields; see
/// [`validate_text_record`]. A field absent from the record reads as null.
fn validate_json_record(record: &JsonRecord, matched: &[PlannedField]) -> Option<RejectedRecord> {
    for planned in matched {
        match record.object.get(&planned.name) {
            None | Some(Value::Null) => {
                if !planned.nullable {
                    return Some(required_field_rejection(
                        record.line,
                        planned,
                        Value::Object(record.object.clone()),
                    ));
                }
            }
            Some(value) => {
                if !fits_pinned_type(infer_json_type(value), planned.materialized_type) {
                    return Some(type_rejection(
                        record.line,
                        planned,
                        value.clone(),
                        Value::Object(record.object.clone()),
                    ));
                }
            }
        }
    }
    None
}

/// A value fits a pinned field iff its observed type widens to the pinned
/// type under the inference lattice — the per-cell restriction of the
/// ADR-0034 column rule (ADR-0035). Building a surviving record's cell with
/// its pinned type can then never fail per value.
fn fits_pinned_type(observed: InferredType, pinned: InferredType) -> bool {
    observed.merge(pinned) == pinned
}

fn required_field_rejection(line: u64, planned: &PlannedField, record: Value) -> RejectedRecord {
    RejectedRecord {
        line,
        code: rejection::MISSING_REQUIRED_FIELD,
        field: Some(planned.name.clone()),
        message: format!("required field {:?} is null", planned.name),
        record,
    }
}

fn type_rejection(
    line: u64,
    planned: &PlannedField,
    value: Value,
    record: Value,
) -> RejectedRecord {
    RejectedRecord {
        line,
        code: rejection::TYPE_COERCION_FAILED,
        field: Some(planned.name.clone()),
        message: format!(
            "value {value} does not fit pinned type {} for field {:?}",
            planned.materialized_type.name(),
            planned.name
        ),
        record,
    }
}

/// Renders a CSV record as the JSON object a rejection's artifact line
/// carries: header names to cell text, with an empty cell as null.
fn text_record_json(field_names: &[String], cells: &[Option<String>]) -> Value {
    Value::Object(
        field_names
            .iter()
            .zip(cells)
            .map(|(name, cell)| {
                (
                    name.clone(),
                    cell.as_ref()
                        .map_or(Value::Null, |value| Value::String(value.clone())),
                )
            })
            .collect(),
    )
}

/// Types the added fields a pinned load appends under the additive policy
/// from the observed types of the surviving records (all-null defaults to
/// text). Added fields are always nullable: inference cannot prove more.
fn planned_added_fields(
    added_names: Vec<(String, usize)>,
    survivor_types: &[InferredType],
) -> Vec<PlannedField> {
    added_names
        .into_iter()
        .map(|(name, observed_index)| PlannedField {
            name,
            materialized_type: default_null_to_text(survivor_types[observed_index]),
            nullable: true,
            observed_index,
        })
        .collect()
}

/// Assembles the plan of a pinned load that passed shape validation: matched
/// fields materialize in pin order with pinned types, and added fields are
/// appended with the persisted pin extended to carry them (ADR-0033), so a
/// field that later disappears again is caught as drift.
fn assemble_pinned_plan(
    pinned_path: &str,
    matched: Vec<PlannedField>,
    added: Vec<PlannedField>,
) -> FieldPlan {
    if added.is_empty() {
        let decision = json!({
            "mode": "pinned",
            "fields": fields_json(&matched),
            "drift_status": "none",
            "pinned_schema_path": pinned_path,
        });
        return FieldPlan {
            fields: matched,
            decision,
            pinned_schema_write: None,
        };
    }

    let added_json = fields_json(&added);
    let mut fields = matched;
    fields.extend(added);
    let decision = json!({
        "mode": "pinned",
        "fields": fields_json(&fields),
        "drift_status": "additive_fields_added",
        "added_fields": added_json,
        "pinned_schema_path": pinned_path,
        "pinned_schema_persisted": true,
    });
    let pinned_schema_write = Some(PinnedSchemaWrite {
        pinned_path: pinned_path.to_string(),
        yaml: pin_yaml(&fields),
    });
    FieldPlan {
        fields,
        decision,
        pinned_schema_write,
    }
}

/// Builds the `schema_drift` failure whose report echoes the pinned expectation
/// and the observed drift, so a drift-failed load's report still records the
/// schema decision.
fn drift_failure(
    pinned_path: &str,
    pin: &PinnedSchema,
    detail: String,
    drift: Value,
) -> ExecutionFailure {
    let decision = json!({
        "mode": "pinned",
        "fields": pinned_fields_json(pin),
        "drift_status": "failed_on_drift",
        "drift": drift,
        "pinned_schema_path": pinned_path,
    });
    ExecutionFailure {
        failure: LoadFailure {
            code: "schema_drift",
            message: format!("schema drift against pinned schema {pinned_path}: {detail}"),
        },
        schema_decision: Some(Box::new(decision)),
        source_rows: None,
        rejected: Vec::new(),
    }
}

/// Renders a pinned schema's fields as the `fields` array of a schema decision.
fn pinned_fields_json(pin: &PinnedSchema) -> Value {
    Value::Array(
        pin.fields
            .iter()
            .map(|field| {
                json!({
                    "name": field.name,
                    "type": field.field_type.name(),
                    "nullable": field.nullable
                })
            })
            .collect(),
    )
}

/// Renders planned fields as the `fields` array of a schema decision.
fn fields_json(fields: &[PlannedField]) -> Value {
    Value::Array(
        fields
            .iter()
            .map(|planned| {
                json!({
                    "name": planned.name,
                    "type": planned.materialized_type.name(),
                    "nullable": planned.nullable
                })
            })
            .collect(),
    )
}

/// Renders planned fields as the pinned schema YAML to persist.
fn pin_yaml(fields: &[PlannedField]) -> String {
    PinnedSchema {
        fields: fields
            .iter()
            .map(|planned| PinnedField {
                name: planned.name.clone(),
                field_type: planned.materialized_type,
                nullable: planned.nullable,
            })
            .collect(),
    }
    .to_yaml()
}

/// Builds the planned columns over the surviving CSV records and assembles
/// the batch.
fn build_text(
    plan: FieldPlan,
    records: &[TextRecord],
    rejected: Vec<RejectedRecord>,
) -> Result<Materialized, ExecutionFailure> {
    let columns = plan
        .fields
        .iter()
        .map(|planned| build_text_array(planned.materialized_type, records, planned.observed_index))
        .collect::<Result<Vec<_>, _>>()?;
    materialize(plan, columns, rejected)
}

/// Builds the planned columns over the surviving JSONL records and assembles
/// the batch.
fn build_json(
    plan: FieldPlan,
    records: &[JsonRecord],
    rejected: Vec<RejectedRecord>,
) -> Result<Materialized, ExecutionFailure> {
    let columns = plan
        .fields
        .iter()
        .enumerate()
        .map(|(column_index, planned)| {
            build_json_array(
                planned.materialized_type,
                records,
                &planned.name,
                column_index,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    materialize(plan, columns, rejected)
}

/// Assembles the planned fields and their pre-built columns into a
/// [`RecordBatch`]. The batch schema and the reported `schema_decision` derive
/// from the same plan, so the report can never disagree with the batch that was
/// written.
fn materialize(
    plan: FieldPlan,
    columns: Vec<ArrayRef>,
    rejected: Vec<RejectedRecord>,
) -> Result<Materialized, ExecutionFailure> {
    let schema = Arc::new(Schema::new(
        plan.fields
            .iter()
            .map(|planned| {
                Field::new(
                    &planned.name,
                    planned.materialized_type.data_type(),
                    planned.nullable,
                )
            })
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema, columns).map_err(|error| LoadFailure {
        code: "record_batch_creation_failed",
        message: format!("failed to create Arrow record batch: {error}"),
    })?;

    Ok(Materialized {
        batch,
        schema_decision: plan.decision,
        pinned_schema_write: plan.pinned_schema_write,
        rejected,
    })
}

fn observe_text_types(field_count: usize, records: &[TextRecord]) -> Vec<InferredType> {
    let mut observed_types = vec![InferredType::Null; field_count];
    for record in records {
        for (column_index, value) in record.cells.iter().enumerate() {
            if let Some(value) = value {
                observed_types[column_index] =
                    observed_types[column_index].merge(infer_text_type(value));
            }
        }
    }
    observed_types
}

fn observe_json_types(field_names: &[String], records: &[JsonRecord]) -> Vec<InferredType> {
    let mut observed_types = vec![InferredType::Null; field_names.len()];
    for record in records {
        for (column_index, field_name) in field_names.iter().enumerate() {
            if let Some(value) = record.object.get(field_name) {
                observed_types[column_index] =
                    observed_types[column_index].merge(infer_json_type(value));
            }
        }
    }
    observed_types
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
    records: &[TextRecord],
    column_index: usize,
) -> Result<ArrayRef, LoadFailure> {
    match inferred_type {
        InferredType::Null | InferredType::Utf8 => {
            let mut builder = StringBuilder::new();
            for record in records {
                match &record.cells[column_index] {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        InferredType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for record in records {
                match &record.cells[column_index] {
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
                match &record.cells[column_index] {
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
                match &record.cells[column_index] {
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
/// [`json_scalar_to_string`]. The coercion arms are unreachable: inference only
/// picks a type every cell already carries, and a pinned load builds only over
/// surviving records, whose cells per-record validation proved to fit
/// (ADR-0035). They return a clean failure rather than panicking if that
/// invariant is ever broken.
fn build_json_array(
    inferred_type: InferredType,
    records: &[JsonRecord],
    field_name: &str,
    column_index: usize,
) -> Result<ArrayRef, LoadFailure> {
    match inferred_type {
        InferredType::Null | InferredType::Utf8 => {
            let mut builder = StringBuilder::new();
            for record in records {
                match json_scalar_to_string(record.object.get(field_name)) {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        InferredType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for record in records {
                match record.object.get(field_name) {
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
            for record in records {
                match record.object.get(field_name) {
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
            for record in records {
                match record.object.get(field_name) {
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

    /// The stable name this type carries in schema decisions and pinned schema
    /// files.
    fn name(self) -> &'static str {
        data_type_name(&self.data_type())
    }
}

/// Observes the type carried by a text value, as CSV fields have no other type
/// information than how they parse. A numeric reading that would lose
/// information is not taken: zero-padded text observes as text per ADR-0032,
/// and Float64 requires a finite parse — text that parses non-finite (the
/// `inf` / `infinity` / `nan` spellings and overflow that saturates to
/// ±infinity) observes as text per ADR-0031.
fn infer_text_type(value: &str) -> InferredType {
    if parse_bool(value).is_some() {
        InferredType::Boolean
    } else if is_zero_padded(value) {
        InferredType::Utf8
    } else if value.parse::<i64>().is_ok() {
        InferredType::Int64
    } else if value.parse::<f64>().is_ok_and(f64::is_finite) {
        InferredType::Float64
    } else {
        InferredType::Utf8
    }
}

/// True when the text's integer part is zero-padded: after an optional leading
/// sign, a `0` immediately followed by another ASCII digit, as in `007`,
/// `0042`, or `007.5`. A numeric reading of such text would drop the leading
/// zeros, so per ADR-0032 it observes as text.
fn is_zero_padded(value: &str) -> bool {
    let unsigned = value.strip_prefix(['+', '-']).unwrap_or(value);
    matches!(unsigned.as_bytes(), [b'0', next, ..] if next.is_ascii_digit())
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

    // ---- Observation rules ----

    #[test]
    fn infer_text_type_reads_the_narrowest_type_that_loses_nothing() {
        use InferredType::*;
        // Migrated smoke cases.
        assert_eq!(infer_text_type("true"), Boolean);
        assert_eq!(infer_text_type("42"), Int64);
        assert_eq!(infer_text_type("3.14"), Float64);
        assert_eq!(infer_text_type("data-spark"), Utf8);

        // Reverse-behaviour table: unintuitive but pinned to keep the refactor
        // behaviour-preserving.
        assert_eq!(infer_text_type("+42"), Int64); // leading sign parses as Int64
        assert_eq!(infer_text_type("1e10"), Float64); // scientific notation is float
        assert_eq!(infer_text_type("99999999999999999999999"), Float64); // i64 overflow -> float
        assert_eq!(infer_text_type(" 42"), Utf8); // whitespace padding is not trimmed
        assert_eq!(infer_text_type("42 "), Utf8);

        // ADR-0032: a zero-padded integer part (an optional sign, then `0`
        // immediately followed by another digit) observes as text, because its
        // numeric reading would drop the leading zeros.
        assert_eq!(infer_text_type("007"), Utf8);
        assert_eq!(infer_text_type("00"), Utf8);
        assert_eq!(infer_text_type("0042"), Utf8);
        assert_eq!(infer_text_type("+007"), Utf8);
        assert_eq!(infer_text_type("-007"), Utf8);
        assert_eq!(infer_text_type("007.5"), Utf8);
        // Unpadded zeros keep their numeric reading.
        assert_eq!(infer_text_type("0"), Int64);
        assert_eq!(infer_text_type("-0"), Int64);
        assert_eq!(infer_text_type("0.5"), Float64);

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
            &SchemaDirective::Inferred,
            names(&["id", "name", "total"]),
            vec![
                record(2, &[Some("1"), Some("Ada"), Some("42.50")]),
                record(3, &[Some("2"), Some("Grace"), Some("7.25")]),
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

        // Inference derives types from the records themselves, so an
        // inference-driven load never rejects a record.
        assert!(materialized.rejected.is_empty());
        assert_eq!(
            materialized.schema_decision,
            json!({
                "mode": "inferred",
                "fields": [
                    {"name": "id", "type": "int64", "nullable": true},
                    {"name": "name", "type": "utf8", "nullable": true},
                    {"name": "total", "type": "float64", "nullable": true}
                ],
                "drift_status": "not_applicable"
            })
        );
    }

    #[test]
    fn from_text_columns_widens_mixed_columns_and_defaults_empty_columns_to_text() {
        let materialized = from_text_columns(
            &SchemaDirective::Inferred,
            names(&["mixed", "widened", "empty"]),
            vec![
                record(2, &[Some("1"), Some("1"), None]),
                record(3, &[Some("x"), Some("2.5"), None]),
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
            &SchemaDirective::Inferred,
            names(&["reading"]),
            vec![record(2, &[Some("1.5")]), record(3, &[Some("inf")])],
        )
        .expect("materialize");
        let batch = &materialized.batch;

        assert_eq!(schema_types(batch), vec![DataType::Utf8]);
        assert_eq!(strings(batch, 0).value(0), "1.5");
        assert_eq!(strings(batch, 0).value(1), "inf");
    }

    #[test]
    fn from_text_columns_keeps_a_column_mixing_zero_padded_and_plain_integers_as_text() {
        // ADR-0032: `007` observes as text, so a column mixing it with plain
        // integers falls to Utf8 under the disagreements-fall-to-text merge rule
        // and stores the original strings verbatim.
        let materialized = from_text_columns(
            &SchemaDirective::Inferred,
            names(&["account"]),
            vec![record(2, &[Some("007")]), record(3, &[Some("1234")])],
        )
        .expect("materialize");
        let batch = &materialized.batch;

        assert_eq!(schema_types(batch), vec![DataType::Utf8]);
        assert_eq!(strings(batch, 0).value(0), "007");
        assert_eq!(strings(batch, 0).value(1), "1234");
    }

    #[test]
    fn from_json_columns_keeps_numeric_strings_as_text_and_maps_absent_to_null() {
        // `zip` is a JSON string of digits: it must stay text (the heal), not be
        // retyped as a number. `active` is missing on the second row -> null.
        let records = vec![
            json_record(1, r#"{"zip": "01234", "balance": 10, "active": true}"#),
            json_record(2, r#"{"zip": "00987", "balance": 5}"#),
        ];
        let materialized = from_json_columns(
            &SchemaDirective::Inferred,
            names(&["zip", "balance", "active"]),
            records,
        )
        .expect("materialize");
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
        assert!(materialized.rejected.is_empty());
    }

    #[test]
    fn from_json_columns_heals_the_float_round_trip_bit_for_bit() {
        // A column mixing a JSON integer and a JSON float widens to Float64. The
        // healed path reads f64 straight from the number; assert the exact bits
        // the old String -> re-parse hop produced.
        let records = vec![
            json_record(1, r#"{"amount": 10}"#),
            json_record(2, r#"{"amount": 42.5}"#),
        ];
        let materialized =
            from_json_columns(&SchemaDirective::Inferred, names(&["amount"]), records)
                .expect("materialize");
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
        let records = vec![json_record(1, r#"{"tags": ["a", "b"], "meta": {"k": 1}}"#)];
        let materialized = from_json_columns(
            &SchemaDirective::Inferred,
            names(&["tags", "meta"]),
            records,
        )
        .expect("materialize");
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
                ],
                "drift_status": "not_applicable"
            })
        );
    }

    #[test]
    fn from_json_columns_defaults_all_null_columns_to_text() {
        let records = vec![json_record(1, r#"{"note": null}"#), json_record(2, "{}")];
        let materialized = from_json_columns(&SchemaDirective::Inferred, names(&["note"]), records)
            .expect("materialize");
        let batch = &materialized.batch;

        assert_eq!(schema_types(batch), vec![DataType::Utf8]);
        assert!(strings(batch, 0).is_null(0));
        assert!(strings(batch, 0).is_null(1));
    }

    // ---- Pinned schema contract (ADR-0033) ----

    const PIN_YAML: &str = "version: 1\n\
                            fields:\n\
                            - name: customer_id\n\
                            \x20 type: int64\n\
                            \x20 nullable: true\n\
                            - name: name\n\
                            \x20 type: utf8\n\
                            \x20 nullable: true\n";

    #[test]
    fn pinned_schema_parses_versioned_yaml_fields() {
        let pin = PinnedSchema::from_yaml(PIN_YAML).expect("parse pinned schema");

        assert_eq!(
            pin,
            PinnedSchema {
                fields: vec![
                    PinnedField {
                        name: "customer_id".to_string(),
                        field_type: InferredType::Int64,
                        nullable: true,
                    },
                    PinnedField {
                        name: "name".to_string(),
                        field_type: InferredType::Utf8,
                        nullable: true,
                    },
                ],
            }
        );
    }

    #[test]
    fn pinned_schema_defaults_omitted_nullable_to_true() {
        // A hand-written pin may omit `nullable`; an omitted contract stays
        // permissive.
        let pin = PinnedSchema::from_yaml("version: 1\nfields:\n- name: id\n  type: int64\n")
            .expect("parse pinned schema without nullable");
        assert_eq!(pin.fields[0].name, "id");
        assert_eq!(pin.fields[0].field_type, InferredType::Int64);
        assert!(pin.fields[0].nullable);
    }

    #[test]
    fn pinned_schema_accepts_non_nullable_fields_and_round_trips_them() {
        // ADR-0035: `nullable: false` declares a required field with
        // per-record semantics, so the pin contract now accepts it — and the
        // persisted YAML keeps it, so a rewrite cannot silently relax a
        // required field.
        let yaml = "version: 1\n\
                    fields:\n\
                    - name: id\n\
                    \x20 type: int64\n\
                    \x20 nullable: false\n\
                    - name: note\n\
                    \x20 type: utf8\n\
                    \x20 nullable: true\n";
        let pin = PinnedSchema::from_yaml(yaml).expect("non-nullable pin parses");
        assert!(!pin.fields[0].nullable);
        assert!(pin.fields[1].nullable);
        assert_eq!(pin.to_yaml(), yaml);
    }

    #[test]
    fn pinned_schema_rejects_contract_violations() {
        let violations = [
            (
                "fields:\n- name: id\n  type: int64\n",
                "pinned schema version is required",
            ),
            (
                "version: 2\nfields:\n- name: id\n  type: int64\n",
                "unsupported pinned schema version: 2",
            ),
            (
                "version: 1\nfields:\n- name: id\n  type: date\n",
                "unsupported pinned schema field type: date",
            ),
            ("version: 1\nfields: []\n", "at least one field"),
            ("version: 1\n", "at least one field"),
            (
                "version: 1\nfields:\n- name: id\n  type: int64\n- name: id\n  type: utf8\n",
                "pinned schema field \"id\" is declared more than once",
            ),
            ("version: [\n", "failed to parse pinned schema"),
        ];

        for (yaml, expected_message_part) in violations {
            let error = PinnedSchema::from_yaml(yaml)
                .err()
                .unwrap_or_else(|| panic!("pinned schema {yaml:?} accepted"));
            assert_eq!(error.code, "invalid_pinned_schema", "code for {yaml:?}");
            assert!(
                error.message.contains(expected_message_part),
                "message {:?} misses {expected_message_part:?}",
                error.message
            );
        }
    }

    // ---- Pinned materialization and drift (ADR-0034) ----

    fn pinned_directive(pin_yaml: &str, drift_policy: DriftPolicy) -> SchemaDirective {
        SchemaDirective::Pinned {
            pinned_path: "customers.schema.yml".to_string(),
            pin: PinnedSchema::from_yaml(pin_yaml).expect("test pin parses"),
            drift_policy,
        }
    }

    #[test]
    fn from_text_columns_materializes_matching_records_in_pinned_order_and_types() {
        // The source arrives with reordered columns and `total` observes as
        // int64 this batch; the pin still materializes pin order and widens
        // total to the pinned float64.
        let directive = pinned_directive(
            "version: 1\n\
             fields:\n\
             - name: id\n\
             \x20 type: int64\n\
             - name: total\n\
             \x20 type: float64\n\
             - name: name\n\
             \x20 type: utf8\n",
            DriftPolicy::Fail,
        );
        let materialized = from_text_columns(
            &directive,
            names(&["name", "id", "total"]),
            vec![
                record(2, &[Some("Ada"), Some("1"), Some("42")]),
                record(3, &[Some("Grace"), Some("2"), Some("7")]),
            ],
        )
        .expect("materialize");
        let batch = &materialized.batch;

        assert_eq!(
            schema_types(batch),
            vec![DataType::Int64, DataType::Float64, DataType::Utf8]
        );
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect::<Vec<_>>(),
            vec!["id", "total", "name"]
        );
        assert_eq!(ints(batch, 0).value(0), 1);
        assert_eq!(floats(batch, 1).value(0), 42.0);
        assert_eq!(floats(batch, 1).value(1), 7.0);
        assert_eq!(strings(batch, 2).value(1), "Grace");

        // A matching load rejects nothing, persists nothing, and reports the
        // pinned posture.
        assert!(materialized.rejected.is_empty());
        assert!(materialized.pinned_schema_write.is_none());
        assert_eq!(
            materialized.schema_decision,
            json!({
                "mode": "pinned",
                "fields": [
                    {"name": "id", "type": "int64", "nullable": true},
                    {"name": "total", "type": "float64", "nullable": true},
                    {"name": "name", "type": "utf8", "nullable": true}
                ],
                "drift_status": "none",
                "pinned_schema_path": "customers.schema.yml"
            })
        );
    }

    #[test]
    fn from_text_columns_fails_on_missing_and_added_fields_before_validating_records() {
        // `name` is missing and `nickname` is new: shape drift fails the load
        // before any record is validated, echoing the pinned expectation
        // (ADR-0034); per-record value fit never runs (ADR-0035).
        let directive = pinned_directive(
            "version: 1\n\
             fields:\n\
             - name: id\n\
             \x20 type: int64\n\
             - name: name\n\
             \x20 type: utf8\n\
             - name: total\n\
             \x20 type: float64\n",
            DriftPolicy::Fail,
        );
        let error = from_text_columns(
            &directive,
            names(&["id", "total", "nickname"]),
            vec![record(2, &[Some("abc"), Some("1.5"), Some("Ada")])],
        )
        .err()
        .expect("drift rejected");

        assert_eq!(error.failure.code, "schema_drift");
        assert_eq!(
            error.failure.message,
            "schema drift against pinned schema customers.schema.yml: \
             missing fields: name; added fields: nickname"
        );
        assert!(error.rejected.is_empty());
        assert_eq!(
            *error
                .schema_decision
                .expect("drift failure carries decision"),
            json!({
                "mode": "pinned",
                "fields": [
                    {"name": "id", "type": "int64", "nullable": true},
                    {"name": "name", "type": "utf8", "nullable": true},
                    {"name": "total", "type": "float64", "nullable": true}
                ],
                "drift_status": "failed_on_drift",
                "drift": {
                    "missing_fields": ["name"],
                    "added_fields": ["nickname"]
                },
                "pinned_schema_path": "customers.schema.yml"
            })
        );
    }

    #[test]
    fn from_text_columns_fails_on_added_fields_under_the_default_drift_policy() {
        let directive = pinned_directive(
            "version: 1\nfields:\n- name: id\n  type: int64\n",
            DriftPolicy::Fail,
        );
        let error = from_text_columns(
            &directive,
            names(&["id", "extra"]),
            vec![record(2, &[Some("1"), Some("x")])],
        )
        .err()
        .expect("added field rejected by default");

        assert_eq!(error.failure.code, "schema_drift");
        assert_eq!(
            error.failure.message,
            "schema drift against pinned schema customers.schema.yml: added fields: extra"
        );
    }

    #[test]
    fn from_text_columns_rejects_records_whose_cells_misfit_the_pinned_type() {
        // ADR-0035: a value misfit rejects only its record; the surviving
        // records materialize and the schema decision reports no drift.
        let directive = pinned_directive(
            "version: 1\n\
             fields:\n\
             - name: id\n\
             \x20 type: int64\n\
             - name: name\n\
             \x20 type: utf8\n",
            DriftPolicy::Fail,
        );
        let materialized = from_text_columns(
            &directive,
            names(&["id", "name"]),
            vec![
                record(2, &[Some("1"), Some("Ada")]),
                record(3, &[Some("abc"), Some("Bad")]),
                record(4, &[Some("3"), Some("Cara")]),
            ],
        )
        .expect("misfits reject records, not the load");
        let batch = &materialized.batch;

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(ints(batch, 0).value(0), 1);
        assert_eq!(ints(batch, 0).value(1), 3);
        assert_eq!(strings(batch, 1).value(1), "Cara");

        assert_eq!(materialized.rejected.len(), 1);
        let rejected = &materialized.rejected[0];
        assert_eq!(rejected.line, 3);
        assert_eq!(rejected.code, "type_coercion_failed");
        assert_eq!(rejected.field.as_deref(), Some("id"));
        assert_eq!(
            rejected.message,
            "value \"abc\" does not fit pinned type int64 for field \"id\""
        );
        assert_eq!(rejected.record, json!({ "id": "abc", "name": "Bad" }));

        // Rejections are threshold business, not drift: the decision stays
        // clean.
        assert_eq!(materialized.schema_decision["drift_status"], "none");
    }

    #[test]
    fn from_text_columns_rejects_lossy_numeric_text_under_a_pin() {
        // The per-cell fit uses observation, not a raw parse: `007` parses as
        // an i64 but observes as text (ADR-0032), and `inf` parses as an f64
        // but observes as text (ADR-0031) — both would lose information under
        // their pinned numeric reading, so both records are rejected. This
        // also pins that a load whose records are all rejected materializes an
        // empty batch of the pinned fields.
        let directive = pinned_directive(
            "version: 1\n\
             fields:\n\
             - name: account\n\
             \x20 type: int64\n\
             - name: reading\n\
             \x20 type: float64\n",
            DriftPolicy::Fail,
        );
        let materialized = from_text_columns(
            &directive,
            names(&["account", "reading"]),
            vec![
                record(2, &[Some("007"), Some("1.5")]),
                record(3, &[Some("42"), Some("inf")]),
            ],
        )
        .expect("lossy numeric text rejects records, not the load");

        assert_eq!(materialized.batch.num_rows(), 0);
        assert_eq!(
            schema_types(&materialized.batch),
            vec![DataType::Int64, DataType::Float64]
        );
        assert_eq!(materialized.rejected.len(), 2);
        assert_eq!(materialized.rejected[0].line, 2);
        assert_eq!(materialized.rejected[0].field.as_deref(), Some("account"));
        assert_eq!(
            materialized.rejected[0].message,
            "value \"007\" does not fit pinned type int64 for field \"account\""
        );
        assert_eq!(materialized.rejected[1].line, 3);
        assert_eq!(materialized.rejected[1].field.as_deref(), Some("reading"));
        assert_eq!(
            materialized.rejected[1].message,
            "value \"inf\" does not fit pinned type float64 for field \"reading\""
        );
    }

    #[test]
    fn from_text_columns_rejects_null_in_a_non_nullable_pinned_field() {
        // ADR-0035: `nullable: false` is a required field; an empty CSV cell
        // reads as null and rejects the record. The materialized Arrow field
        // is non-nullable, which the surviving records satisfy by
        // construction.
        let directive = pinned_directive(
            "version: 1\n\
             fields:\n\
             - name: id\n\
             \x20 type: int64\n\
             \x20 nullable: false\n\
             - name: name\n\
             \x20 type: utf8\n",
            DriftPolicy::Fail,
        );
        let materialized = from_text_columns(
            &directive,
            names(&["id", "name"]),
            vec![
                record(2, &[Some("1"), Some("Ada")]),
                record(3, &[None, Some("Bad")]),
            ],
        )
        .expect("required-field violations reject records, not the load");
        let batch = &materialized.batch;

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(ints(batch, 0).value(0), 1);
        assert!(!batch.schema().field(0).is_nullable());
        assert!(batch.schema().field(1).is_nullable());

        assert_eq!(materialized.rejected.len(), 1);
        let rejected = &materialized.rejected[0];
        assert_eq!(rejected.line, 3);
        assert_eq!(rejected.code, "missing_required_field");
        assert_eq!(rejected.field.as_deref(), Some("id"));
        assert_eq!(rejected.message, "required field \"id\" is null");
        assert_eq!(rejected.record, json!({ "id": null, "name": "Bad" }));

        // The schema decision echoes the required field.
        assert_eq!(
            materialized.schema_decision["fields"][0],
            json!({"name": "id", "type": "int64", "nullable": false})
        );
    }

    #[test]
    fn from_text_columns_types_added_fields_from_surviving_records_only() {
        // The rejected record carries text in the added `extra` column; its
        // values must not widen the added field, which types from survivors.
        let directive = pinned_directive(
            "version: 1\nfields:\n- name: id\n  type: int64\n",
            DriftPolicy::AllowAdditiveNullable,
        );
        let materialized = from_text_columns(
            &directive,
            names(&["id", "extra"]),
            vec![
                record(2, &[Some("1"), Some("7")]),
                record(3, &[Some("abc"), Some("hello")]),
            ],
        )
        .expect("materialize");
        let batch = &materialized.batch;

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(schema_types(batch), vec![DataType::Int64, DataType::Int64]);
        assert_eq!(materialized.rejected.len(), 1);
        assert_eq!(
            materialized.schema_decision["added_fields"],
            json!([{"name": "extra", "type": "int64", "nullable": true}])
        );
        assert_eq!(
            materialized.pinned_schema_write.expect("extended pin").yaml,
            "version: 1\n\
             fields:\n\
             - name: id\n\
             \x20 type: int64\n\
             \x20 nullable: true\n\
             - name: extra\n\
             \x20 type: int64\n\
             \x20 nullable: true\n"
        );
    }

    #[test]
    fn from_text_columns_appends_added_nullable_fields_under_the_additive_policy() {
        let directive = pinned_directive(
            "version: 1\n\
             fields:\n\
             - name: id\n\
             \x20 type: int64\n\
             - name: name\n\
             \x20 type: utf8\n",
            DriftPolicy::AllowAdditiveNullable,
        );
        let materialized = from_text_columns(
            &directive,
            names(&["id", "name", "vip"]),
            vec![
                record(2, &[Some("1"), Some("Ada"), Some("true")]),
                record(3, &[Some("2"), Some("Grace"), None]),
            ],
        )
        .expect("additive drift allowed");
        let batch = &materialized.batch;

        assert_eq!(
            schema_types(batch),
            vec![DataType::Int64, DataType::Utf8, DataType::Boolean]
        );
        assert!(bools(batch, 2).value(0));
        assert!(bools(batch, 2).is_null(1));

        assert_eq!(
            materialized.schema_decision,
            json!({
                "mode": "pinned",
                "fields": [
                    {"name": "id", "type": "int64", "nullable": true},
                    {"name": "name", "type": "utf8", "nullable": true},
                    {"name": "vip", "type": "boolean", "nullable": true}
                ],
                "drift_status": "additive_fields_added",
                "added_fields": [
                    {"name": "vip", "type": "boolean", "nullable": true}
                ],
                "pinned_schema_path": "customers.schema.yml",
                "pinned_schema_persisted": true
            })
        );
        // The persisted pin now carries the added field, so a later
        // disappearance is caught as drift (ADR-0033).
        assert_eq!(
            materialized.pinned_schema_write.expect("extended pin").yaml,
            "version: 1\n\
             fields:\n\
             - name: id\n\
             \x20 type: int64\n\
             \x20 nullable: true\n\
             - name: name\n\
             \x20 type: utf8\n\
             \x20 nullable: true\n\
             - name: vip\n\
             \x20 type: boolean\n\
             \x20 nullable: true\n"
        );
    }

    #[test]
    fn from_text_columns_persists_the_inferred_schema_as_the_new_pin_on_first_load() {
        let materialized = from_text_columns(
            &SchemaDirective::PinInferred {
                pinned_path: "customers.schema.yml".to_string(),
            },
            names(&["id", "total"]),
            vec![record(2, &[Some("1"), Some("42.5")])],
        )
        .expect("materialize");

        assert_eq!(
            materialized.schema_decision,
            json!({
                "mode": "inferred",
                "fields": [
                    {"name": "id", "type": "int64", "nullable": true},
                    {"name": "total", "type": "float64", "nullable": true}
                ],
                "drift_status": "not_applicable",
                "pinned_schema_path": "customers.schema.yml",
                "pinned_schema_persisted": true
            })
        );
        let pinned_schema_write = materialized.pinned_schema_write.expect("bootstrap pin");
        assert_eq!(pinned_schema_write.pinned_path, "customers.schema.yml");
        assert_eq!(
            pinned_schema_write.yaml,
            "version: 1\n\
             fields:\n\
             - name: id\n\
             \x20 type: int64\n\
             \x20 nullable: true\n\
             - name: total\n\
             \x20 type: float64\n\
             \x20 nullable: true\n"
        );
    }

    #[test]
    fn from_text_columns_matches_all_null_columns_against_any_pinned_type() {
        // `score` never carries a value in this batch: a null cell fits any
        // nullable pinned field, so no record is rejected and the column
        // materializes as an all-null int64 column.
        let directive = pinned_directive(
            "version: 1\n\
             fields:\n\
             - name: id\n\
             \x20 type: int64\n\
             - name: score\n\
             \x20 type: int64\n",
            DriftPolicy::Fail,
        );
        let materialized = from_text_columns(
            &directive,
            names(&["id", "score"]),
            vec![record(2, &[Some("1"), None]), record(3, &[Some("2"), None])],
        )
        .expect("all-null column matches");
        let batch = &materialized.batch;

        assert_eq!(schema_types(batch), vec![DataType::Int64, DataType::Int64]);
        assert!(ints(batch, 1).is_null(0));
        assert!(ints(batch, 1).is_null(1));
        assert!(materialized.rejected.is_empty());
        assert_eq!(materialized.schema_decision["drift_status"], "none");
    }

    #[test]
    fn from_text_columns_fails_on_duplicate_source_field_names_under_a_pin() {
        let directive = pinned_directive(
            "version: 1\nfields:\n- name: id\n  type: int64\n",
            DriftPolicy::Fail,
        );
        let error = from_text_columns(
            &directive,
            names(&["id", "id"]),
            vec![record(2, &[Some("1"), Some("2")])],
        )
        .err()
        .expect("duplicate names rejected");

        assert_eq!(error.failure.code, "schema_drift");
        assert!(error.failure.message.contains(
            "source field \"id\" appears more than once, so records cannot be validated"
        ));
        assert_eq!(
            error.schema_decision.expect("decision")["drift"],
            json!({ "duplicate_fields": ["id"] })
        );
    }

    #[test]
    fn from_json_columns_validates_and_widens_against_a_pinned_schema() {
        // `zip` is a JSON string that merely looks numeric: pinned utf8 keeps
        // it text. `balance` observes as int64 and widens to the pinned float64.
        let directive = pinned_directive(
            "version: 1\n\
             fields:\n\
             - name: zip\n\
             \x20 type: utf8\n\
             - name: balance\n\
             \x20 type: float64\n",
            DriftPolicy::Fail,
        );
        let records = vec![
            json_record(1, r#"{"zip": "01234", "balance": 10}"#),
            json_record(2, r#"{"zip": "00987", "balance": 5}"#),
        ];
        let materialized = from_json_columns(&directive, names(&["zip", "balance"]), records)
            .expect("materialize");
        let batch = &materialized.batch;

        assert_eq!(schema_types(batch), vec![DataType::Utf8, DataType::Float64]);
        assert_eq!(strings(batch, 0).value(0), "01234");
        assert_eq!(floats(batch, 1).value(0), 10.0);
        assert!(materialized.rejected.is_empty());
        assert_eq!(materialized.schema_decision["mode"], "pinned");
        assert_eq!(materialized.schema_decision["drift_status"], "none");
    }

    #[test]
    fn from_json_columns_rejects_a_json_string_against_a_pinned_numeric_field() {
        // ADR-0035: a JSON string that merely looks numeric misfits a pinned
        // int64 per record — the record is rejected, not the load.
        let directive = pinned_directive(
            "version: 1\nfields:\n- name: balance\n  type: int64\n",
            DriftPolicy::Fail,
        );
        let records = vec![
            json_record(1, r#"{"balance": 7}"#),
            json_record(2, r#"{"balance": "10"}"#),
        ];
        let materialized = from_json_columns(&directive, names(&["balance"]), records)
            .expect("string vs pinned int64 rejects the record");

        assert_eq!(materialized.batch.num_rows(), 1);
        assert_eq!(ints(&materialized.batch, 0).value(0), 7);
        assert_eq!(materialized.rejected.len(), 1);
        let rejected = &materialized.rejected[0];
        assert_eq!(rejected.line, 2);
        assert_eq!(rejected.code, "type_coercion_failed");
        assert_eq!(rejected.field.as_deref(), Some("balance"));
        assert_eq!(
            rejected.message,
            "value \"10\" does not fit pinned type int64 for field \"balance\""
        );
        assert_eq!(rejected.record, json!({ "balance": "10" }));
    }

    #[test]
    fn from_json_columns_rejects_null_and_absent_in_a_non_nullable_pinned_field() {
        // A JSON null and an absent field both read as null (ADR-0034), so
        // both violate a `nullable: false` pinned field per record.
        let directive = pinned_directive(
            "version: 1\n\
             fields:\n\
             - name: id\n\
             \x20 type: int64\n\
             - name: note\n\
             \x20 type: utf8\n\
             \x20 nullable: false\n",
            DriftPolicy::Fail,
        );
        let records = vec![
            json_record(1, r#"{"id": 1, "note": "x"}"#),
            json_record(2, r#"{"id": 2, "note": null}"#),
            json_record(3, r#"{"id": 3}"#),
        ];
        let materialized = from_json_columns(&directive, names(&["id", "note"]), records)
            .expect("required-field violations reject records, not the load");

        assert_eq!(materialized.batch.num_rows(), 1);
        assert_eq!(materialized.rejected.len(), 2);
        for (rejected, line, record) in [
            (
                &materialized.rejected[0],
                2,
                json!({ "id": 2, "note": null }),
            ),
            (&materialized.rejected[1], 3, json!({ "id": 3 })),
        ] {
            assert_eq!(rejected.line, line);
            assert_eq!(rejected.code, "missing_required_field");
            assert_eq!(rejected.field.as_deref(), Some("note"));
            assert_eq!(rejected.message, "required field \"note\" is null");
            assert_eq!(rejected.record, record);
        }
    }

    #[test]
    fn from_json_columns_treats_a_batch_wide_absent_pinned_field_as_missing_field_drift() {
        // A field absent from one record reads as null, but a pinned field
        // absent from every record is missing-field drift under every policy
        // (ADR-0034): a silently renamed source field must not quietly become
        // an all-null column.
        let directive = pinned_directive(
            "version: 1\n\
             fields:\n\
             - name: id\n\
             \x20 type: int64\n\
             - name: note\n\
             \x20 type: utf8\n",
            DriftPolicy::AllowAdditiveNullable,
        );
        let records = vec![
            json_record(1, r#"{"id": 1}"#),
            json_record(2, r#"{"id": 2}"#),
        ];
        let error = from_json_columns(&directive, names(&["id"]), records)
            .err()
            .expect("batch-wide absence rejected");

        assert_eq!(error.failure.code, "schema_drift");
        assert!(error.failure.message.contains("missing fields: note"));
    }

    #[test]
    fn from_json_columns_appends_an_all_null_added_field_as_text_under_the_additive_policy() {
        let directive = pinned_directive(
            "version: 1\nfields:\n- name: id\n  type: int64\n",
            DriftPolicy::AllowAdditiveNullable,
        );
        let records = vec![
            json_record(1, r#"{"id": 1, "note": null}"#),
            json_record(2, r#"{"id": 2}"#),
        ];
        let materialized =
            from_json_columns(&directive, names(&["id", "note"]), records).expect("materialize");
        let batch = &materialized.batch;

        assert_eq!(schema_types(batch), vec![DataType::Int64, DataType::Utf8]);
        assert!(strings(batch, 1).is_null(0));
        assert!(strings(batch, 1).is_null(1));
        assert_eq!(
            materialized.schema_decision["added_fields"],
            json!([{"name": "note", "type": "utf8", "nullable": true}])
        );
        assert_eq!(
            materialized.pinned_schema_write.expect("extended pin").yaml,
            "version: 1\n\
             fields:\n\
             - name: id\n\
             \x20 type: int64\n\
             \x20 nullable: true\n\
             - name: note\n\
             \x20 type: utf8\n\
             \x20 nullable: true\n"
        );
    }

    #[test]
    fn pinned_schema_serializes_to_the_persisted_yaml_form() {
        let pin = PinnedSchema::from_yaml(PIN_YAML).expect("parse pinned schema");

        // The exact persisted form is part of the pinned schema file contract.
        assert_eq!(pin.to_yaml(), PIN_YAML);
        // And the round trip is lossless.
        assert_eq!(
            PinnedSchema::from_yaml(&pin.to_yaml()).expect("reparse"),
            pin
        );
    }

    // ---- Test helpers ----

    fn names(field_names: &[&str]) -> Vec<String> {
        field_names.iter().map(|name| name.to_string()).collect()
    }

    fn record(line: u64, cells: &[Option<&str>]) -> TextRecord {
        TextRecord {
            line,
            cells: cells.iter().map(|cell| cell.map(str::to_string)).collect(),
        }
    }

    fn json_record(line: u64, text: &str) -> JsonRecord {
        match serde_json::from_str::<Value>(text).expect("valid json") {
            Value::Object(object) => JsonRecord { line, object },
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
