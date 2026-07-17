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
//! A load definition may override selected inferred fields
//! ([`SchemaOverrides`], ADR-0038): an override rewrites inference wherever
//! it decides a field's shape and never rewrites a pin — an override naming a
//! field absent from the observed source shape fails the load as
//! `unknown_override_field` before any pin comparison, and one contradicting
//! an existing pinned field fails it as `schema_override_conflict` before
//! drift comparison. Overridden fields validate per record exactly like
//! pinned fields.
//! A load definition may also declare a structural transform
//! ([`SchemaTransform`], ADR-0039): field selection evaluates first against
//! the observed source names and fixes the dataset field order, then the
//! rename mapping applies simultaneously over the selected fields. The
//! transform runs before everything above (ADR-0040), so overrides, pins,
//! drift, and per-record validation all speak the transformed dataset names
//! while rejections keep the original source content — a transform naming an
//! unobserved field fails the load as `unknown_transform_field`, and a rename
//! target colliding with another dataset field fails it as
//! `transform_name_collision`, both before any override or pin comparison.
//! Everything type-related — the lattice, observation rules, the pinned schema
//! file contract ([`PinnedSchema`], ADR-0033), drift comparison, per-record
//! validation, materialization, and the `schema_decision` shape — is private
//! behind these two entry points.

use crate::connector::DestinationWriteFacts;
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
/// schema (ADR-0034). Every variant carries the definition's structural
/// transform (ADR-0039), which reshapes the observed source fields into the
/// dataset fields everything downstream speaks, and the definition's
/// per-field overrides (ADR-0038), which rewrite the inference-decided parts
/// of the schema — everything on an inference-driven load, the bootstrapped
/// pin, and the added fields an additive drift policy admits — and must agree
/// with any field the pin already governs. `pinned_path` is the display path
/// the schema decision reports; file I/O stays with the caller.
pub(crate) enum SchemaDirective {
    Inferred {
        transform: SchemaTransform,
        overrides: SchemaOverrides,
    },
    PinInferred {
        pinned_path: String,
        transform: SchemaTransform,
        overrides: SchemaOverrides,
    },
    Pinned {
        pinned_path: String,
        pin: PinnedSchema,
        drift_policy: DriftPolicy,
        transform: SchemaTransform,
        overrides: SchemaOverrides,
    },
}

impl SchemaDirective {
    /// The inference directive with no transform and no overrides configured
    /// — the posture of a load definition without `transform` and `schema`
    /// blocks.
    #[cfg(test)]
    pub(crate) fn inferred() -> Self {
        SchemaDirective::Inferred {
            transform: SchemaTransform::none(),
            overrides: SchemaOverrides::none(),
        }
    }

    fn transform(&self) -> &SchemaTransform {
        match self {
            SchemaDirective::Inferred { transform, .. }
            | SchemaDirective::PinInferred { transform, .. }
            | SchemaDirective::Pinned { transform, .. } => transform,
        }
    }

    fn overrides(&self) -> &SchemaOverrides {
        match self {
            SchemaDirective::Inferred { overrides, .. }
            | SchemaDirective::PinInferred { overrides, .. }
            | SchemaDirective::Pinned { overrides, .. } => overrides,
        }
    }

    /// Adds the directive echoes to a schema decision: every decision this
    /// module reports — success and failure paths alike — carries the
    /// transform and the overrides the definition configured, and neither
    /// when it configured neither.
    fn stamp(&self, decision: Value) -> Value {
        self.transform().stamp(self.overrides().stamp(decision))
    }
}

/// The rule that decides whether a load may continue when schema drift is
/// detected against a pinned schema: fail fast by default, or allow additive
/// nullable drift when the load definition explicitly permits it (ADR-0007).
pub(crate) enum DriftPolicy {
    Fail,
    AllowAdditiveNullable,
}

/// One `schema.overrides` entry as written in a load definition (ADR-0038):
/// the field it names and the inferred properties it replaces. Part of the
/// versioned load-definition contract, so unknown keys inside an entry are
/// rejected at parse time (ADR-0037).
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OverrideEntry {
    name: String,
    #[serde(rename = "type")]
    field_type: Option<String>,
    nullable: Option<bool>,
}

/// The validated per-field overrides of a load definition, in declaration
/// order (ADR-0038). Empty when the definition configures none, so every
/// [`SchemaDirective`] can carry one without an optional wrapper.
pub(crate) struct SchemaOverrides {
    overrides: Vec<FieldOverride>,
}

/// One validated override: the field it names and the properties it
/// replaces — at least one of them is set.
struct FieldOverride {
    name: String,
    field_type: Option<InferredType>,
    nullable: Option<bool>,
}

impl SchemaOverrides {
    /// No overrides configured.
    pub(crate) fn none() -> Self {
        SchemaOverrides {
            overrides: Vec::new(),
        }
    }

    /// Validates the `schema.overrides` entries of a load definition before
    /// any data is read: field names must be unique and every entry must
    /// override at least one property (`invalid_schema_config`), with `type`
    /// drawn from the pinned-schema type vocabulary
    /// (`unsupported_override_type`, mirroring `unsupported_drift_policy`).
    pub(crate) fn from_entries(entries: &[OverrideEntry]) -> Result<Self, LoadFailure> {
        let mut seen_names = HashSet::new();
        let mut overrides = Vec::with_capacity(entries.len());
        for entry in entries {
            if !seen_names.insert(entry.name.as_str()) {
                return Err(LoadFailure {
                    code: "invalid_schema_config",
                    message: format!(
                        "schema override for field {:?} is declared more than once",
                        entry.name
                    ),
                });
            }
            if entry.field_type.is_none() && entry.nullable.is_none() {
                return Err(LoadFailure {
                    code: "invalid_schema_config",
                    message: format!(
                        "schema override for field {:?} must set at least one of type or nullable",
                        entry.name
                    ),
                });
            }
            let field_type = entry
                .field_type
                .as_deref()
                .map(|type_name| {
                    parse_type_name(type_name).ok_or_else(|| LoadFailure {
                        code: "unsupported_override_type",
                        message: format!(
                            "unsupported schema override type for field {:?}: {type_name}",
                            entry.name
                        ),
                    })
                })
                .transpose()?;
            overrides.push(FieldOverride {
                name: entry.name.clone(),
                field_type,
                nullable: entry.nullable,
            });
        }
        Ok(SchemaOverrides { overrides })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.overrides.is_empty()
    }

    fn get(&self, name: &str) -> Option<&FieldOverride> {
        self.overrides
            .iter()
            .find(|override_| override_.name == name)
    }

    /// The override-named fields absent from the dataset shape — the observed
    /// source shape after the structural transform — in declaration order.
    fn unknown_names(&self, dataset_names: &HashSet<&str>) -> Vec<&str> {
        self.overrides
            .iter()
            .map(|override_| override_.name.as_str())
            .filter(|name| !dataset_names.contains(name))
            .collect()
    }

    /// Renders the overrides as the `schema_decision.overrides` echo: the
    /// directive as written, with unspecified properties omitted.
    fn echo(&self) -> Value {
        Value::Array(
            self.overrides
                .iter()
                .map(|override_| {
                    let mut entry = serde_json::Map::new();
                    entry.insert("name".to_string(), json!(override_.name));
                    if let Some(field_type) = override_.field_type {
                        entry.insert("type".to_string(), json!(field_type.name()));
                    }
                    if let Some(nullable) = override_.nullable {
                        entry.insert("nullable".to_string(), json!(nullable));
                    }
                    Value::Object(entry)
                })
                .collect(),
        )
    }

    /// Adds the echo to a schema decision. Every decision the schema module
    /// reports — success and failure paths alike — carries the overrides the
    /// definition configured, and none when it configured none.
    fn stamp(&self, mut decision: Value) -> Value {
        if !self.is_empty() {
            decision["overrides"] = self.echo();
        }
        decision
    }
}

/// The `transform` block of a load definition as written (ADR-0039): the
/// source fields to keep, in dataset order, and the source-to-dataset rename
/// mapping. Part of the versioned load-definition contract, so unknown keys
/// inside the block are rejected at parse time (ADR-0037).
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TransformConfig {
    select: Option<Vec<String>>,
    rename: Option<RenameMap>,
}

/// The `transform.rename` mapping as written: source field name to dataset
/// field name, in declaration order. Deserialized through its own visitor so
/// a duplicate source key fails YAML parsing — serde's default map handling
/// would silently keep the last entry — and so the echo preserves the
/// declaration order.
#[derive(Debug)]
struct RenameMap(Vec<(String, String)>);

impl<'de> Deserialize<'de> for RenameMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct RenameMapVisitor;

        impl<'de> serde::de::Visitor<'de> for RenameMapVisitor {
            type Value = RenameMap;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a map of source field name to dataset field name")
            }

            fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut entries: Vec<(String, String)> =
                    Vec::with_capacity(access.size_hint().unwrap_or(0));
                let mut seen_sources = HashSet::new();
                while let Some((source, target)) = access.next_entry::<String, String>()? {
                    if !seen_sources.insert(source.clone()) {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate transform.rename key {source:?}"
                        )));
                    }
                    entries.push((source, target));
                }
                Ok(RenameMap(entries))
            }
        }

        deserializer.deserialize_map(RenameMapVisitor)
    }
}

impl Serialize for RenameMap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_map(self.0.iter().map(|(source, target)| (source, target)))
    }
}

/// The validated structural transform of a load definition (ADR-0039): field
/// selection evaluates first, against observed source names, with the select
/// list order fixing the dataset field order; the rename mapping evaluates
/// second, applied simultaneously over the selected (or, without `select`,
/// full observed) field set, so swaps are legal and unmapped fields pass
/// through under their source names. Empty when the definition configures
/// none, so every [`SchemaDirective`] can carry one without an optional
/// wrapper.
pub(crate) struct SchemaTransform {
    select: Option<Vec<String>>,
    rename: Vec<(String, String)>,
}

impl SchemaTransform {
    /// No transform configured.
    pub(crate) fn none() -> Self {
        SchemaTransform {
            select: None,
            rename: Vec::new(),
        }
    }

    /// Validates the `transform` block of a load definition before any data
    /// is read (`invalid_transform_config`): the block must transform
    /// something, select entries must be unique, and every rename must map an
    /// actual name change onto a usable, unique target drawn from the select
    /// list when one is declared — no implicit selection and no lenient
    /// no-ops (ADR-0039). With `select`, the dataset shape is fully
    /// config-determined, so a rename target colliding with another selected
    /// field's dataset name is also rejected here; without `select`, the
    /// pass-through field set is only known at read time and collisions
    /// surface there as `transform_name_collision`.
    pub(crate) fn from_config(config: &TransformConfig) -> Result<Self, LoadFailure> {
        let invalid = |message: String| LoadFailure {
            code: "invalid_transform_config",
            message,
        };
        if config.select.is_none() && config.rename.is_none() {
            return Err(invalid(
                "a transform block must set transform.select or transform.rename".to_string(),
            ));
        }
        if let Some(select) = &config.select {
            if select.is_empty() {
                return Err(invalid(
                    "transform.select must name at least one field".to_string(),
                ));
            }
            let mut seen_entries = HashSet::new();
            for name in select {
                if !seen_entries.insert(name.as_str()) {
                    return Err(invalid(format!(
                        "transform.select names field {name:?} more than once"
                    )));
                }
            }
        }
        let rename = match &config.rename {
            None => Vec::new(),
            Some(rename) if rename.0.is_empty() => {
                return Err(invalid(
                    "transform.rename must map at least one field".to_string(),
                ))
            }
            Some(rename) => rename.0.clone(),
        };
        let mut seen_targets = HashSet::new();
        for (source, target) in &rename {
            if target.trim().is_empty() {
                return Err(invalid(format!(
                    "transform.rename target for field {source:?} must not be empty"
                )));
            }
            if source == target {
                return Err(invalid(format!(
                    "transform.rename maps field {source:?} to itself"
                )));
            }
            if !seen_targets.insert(target.as_str()) {
                return Err(invalid(format!(
                    "transform.rename maps more than one field to {target:?}"
                )));
            }
            if let Some(select) = &config.select {
                if !select.contains(source) {
                    return Err(invalid(format!(
                        "transform.rename key {source:?} is not in transform.select"
                    )));
                }
            }
        }
        if let Some(select) = &config.select {
            let mut seen_dataset_names = HashSet::new();
            for source in select {
                let dataset_name = rename
                    .iter()
                    .find(|(key, _)| key == source)
                    .map(|(_, target)| target.as_str())
                    .unwrap_or(source.as_str());
                if !seen_dataset_names.insert(dataset_name) {
                    return Err(invalid(format!(
                        "transform.select and transform.rename map more than one field \
                         to the dataset name {dataset_name:?}"
                    )));
                }
            }
        }
        Ok(SchemaTransform {
            select: config.select.clone(),
            rename,
        })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.select.is_none() && self.rename.is_empty()
    }

    /// The dataset field name a selected source field materializes under: its
    /// rename target, or the source name passed through.
    fn dataset_name(&self, source_name: &str) -> String {
        self.rename
            .iter()
            .find(|(source, _)| source == source_name)
            .map(|(_, target)| target.clone())
            .unwrap_or_else(|| source_name.to_string())
    }

    /// Renders the transform as the `schema_decision.transform` echo: the
    /// block as written, with unset keys omitted.
    fn echo(&self) -> Value {
        let mut entry = serde_json::Map::new();
        if let Some(select) = &self.select {
            entry.insert("select".to_string(), json!(select));
        }
        if !self.rename.is_empty() {
            entry.insert(
                "rename".to_string(),
                Value::Object(
                    self.rename
                        .iter()
                        .map(|(source, target)| (source.clone(), json!(target)))
                        .collect(),
                ),
            );
        }
        Value::Object(entry)
    }

    /// Adds the echo to a schema decision. Every decision the schema module
    /// reports — success and failure paths alike — carries the transform the
    /// definition configured, and none when it configured none.
    fn stamp(&self, mut decision: Value) -> Value {
        if !self.is_empty() {
            decision["transform"] = self.echo();
        }
        decision
    }
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
/// Parsing rejects unknown keys at the top level and in each field entry
/// (ADR-0037), so a hand edit that misspells a key fails instead of silently
/// relaxing the pin.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PinnedSchemaFile {
    version: Option<u64>,
    fields: Option<Vec<PinnedFieldFile>>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
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
    stamped(
        directive,
        text_materialization(directive, field_names, records),
    )
}

/// Materializes JSONL columns from their parsed [`Value`]s directly, owning
/// how absent and [`Value::Null`] values become Arrow nulls, so a JSON string
/// like `"01234"` stays text instead of being round-tripped through a
/// re-parse.
pub(crate) fn from_json_columns(
    directive: &SchemaDirective,
    field_names: Vec<String>,
    records: Vec<JsonRecord>,
) -> Result<Materialized, ExecutionFailure> {
    stamped(
        directive,
        json_materialization(directive, field_names, records),
    )
}

/// Adds the directive echoes to whichever schema decision a materialization
/// produced — the one place every decision passes through, so success and
/// failure decisions alike carry the configured transform and overrides.
fn stamped(
    directive: &SchemaDirective,
    result: Result<Materialized, ExecutionFailure>,
) -> Result<Materialized, ExecutionFailure> {
    match result {
        Ok(mut materialized) => {
            materialized.schema_decision = directive.stamp(materialized.schema_decision);
            Ok(materialized)
        }
        Err(mut failure) => {
            if let Some(decision) = failure.schema_decision.take() {
                failure.schema_decision = Some(Box::new(directive.stamp(*decision)));
            }
            Err(failure)
        }
    }
}

fn text_materialization(
    directive: &SchemaDirective,
    field_names: Vec<String>,
    records: Vec<TextRecord>,
) -> Result<Materialized, ExecutionFailure> {
    let columns = resolve_transform(directive, &field_names)?;
    check_override_names(directive, &columns)?;
    match directive {
        SchemaDirective::Inferred { overrides, .. } => {
            inferred_text(&columns, &field_names, records, None, overrides)
        }
        SchemaDirective::PinInferred {
            pinned_path,
            overrides,
            ..
        } => inferred_text(
            &columns,
            &field_names,
            records,
            Some(pinned_path),
            overrides,
        ),
        SchemaDirective::Pinned {
            pinned_path,
            pin,
            drift_policy,
            overrides,
            ..
        } => {
            check_override_conflicts(pinned_path, pin, overrides)?;
            let ShapeMatch { matched, added } =
                match_shape(pinned_path, pin, drift_policy, &columns)?;
            let mut checks = pinned_checks(&matched);
            checks.extend(override_checks(overrides, added.iter()));
            let (survivors, rejected) = partition_text(records, &checks, &field_names);
            // Added fields take their types from the surviving records only,
            // so a rejected record's values never shape the destination.
            let survivor_types = observe_text_types(field_names.len(), &survivors);
            let added = planned_added_fields(added, &survivor_types, overrides);
            let plan = assemble_pinned_plan(pinned_path, matched, added);
            build_text(plan, &survivors, rejected)
        }
    }
}

fn json_materialization(
    directive: &SchemaDirective,
    field_names: Vec<String>,
    records: Vec<JsonRecord>,
) -> Result<Materialized, ExecutionFailure> {
    let columns = resolve_transform(directive, &field_names)?;
    check_override_names(directive, &columns)?;
    match directive {
        SchemaDirective::Inferred { overrides, .. } => {
            inferred_json(&columns, &field_names, records, None, overrides)
        }
        SchemaDirective::PinInferred {
            pinned_path,
            overrides,
            ..
        } => inferred_json(
            &columns,
            &field_names,
            records,
            Some(pinned_path),
            overrides,
        ),
        SchemaDirective::Pinned {
            pinned_path,
            pin,
            drift_policy,
            overrides,
            ..
        } => {
            check_override_conflicts(pinned_path, pin, overrides)?;
            let ShapeMatch { matched, added } =
                match_shape(pinned_path, pin, drift_policy, &columns)?;
            let mut checks = pinned_checks(&matched);
            checks.extend(override_checks(overrides, added.iter()));
            let (survivors, rejected) = partition_json(records, &checks);
            let survivor_types = observe_json_types(&field_names, &survivors);
            let added = planned_added_fields(added, &survivor_types, overrides);
            let plan = assemble_pinned_plan(pinned_path, matched, added);
            build_json(plan, &survivors, rejected)
        }
    }
}

/// One field of the dataset a load materializes, produced by resolving the
/// structural transform against the observed source shape: the dataset name
/// the field writes under, the source field it reads from, and that source
/// field's observed column index. Without a transform the dataset view is the
/// observed source shape itself.
#[derive(Clone)]
struct DatasetColumn {
    dataset_name: String,
    source_name: String,
    observed_index: usize,
}

/// Resolves the directive's structural transform against the observed source
/// names into the dataset columns everything downstream operates on
/// (ADR-0040). Fails with `unknown_transform_field` when a select entry or
/// rename key names no observed field — reported as the user wrote them —
/// and with `transform_name_collision` when a rename target collides with a
/// pass-through field name, which is reachable only without `select`: with
/// one, the dataset shape is config-determined and collisions were already
/// rejected at directive resolution. Duplicate observed names resolve to
/// their first occurrence; purely pass-through duplicates keep their
/// pre-transform meaning (drift under a pin) rather than becoming a
/// transform failure.
fn resolve_transform(
    directive: &SchemaDirective,
    observed_names: &[String],
) -> Result<Vec<DatasetColumn>, ExecutionFailure> {
    let transform = directive.transform();
    let mut observed_indexes: HashMap<&str, usize> = HashMap::with_capacity(observed_names.len());
    for (index, name) in observed_names.iter().enumerate() {
        observed_indexes.entry(name.as_str()).or_insert(index);
    }

    let mut unknown: Vec<&str> = Vec::new();
    for name in transform
        .select
        .iter()
        .flatten()
        .chain(transform.rename.iter().map(|(source, _)| source))
    {
        if !observed_indexes.contains_key(name.as_str()) && !unknown.contains(&name.as_str()) {
            unknown.push(name);
        }
    }
    if !unknown.is_empty() {
        return Err(pre_materialization_failure(
            LoadFailure {
                code: "unknown_transform_field",
                message: format!(
                    "transform selects or renames fields absent from the observed source shape: {}",
                    unknown.join(", ")
                ),
            },
            configured_posture_decision(directive),
        ));
    }

    let columns = match &transform.select {
        Some(select) => select
            .iter()
            .map(|source_name| DatasetColumn {
                dataset_name: transform.dataset_name(source_name),
                source_name: source_name.clone(),
                observed_index: observed_indexes[source_name.as_str()],
            })
            .collect::<Vec<_>>(),
        None => observed_names
            .iter()
            .enumerate()
            .map(|(observed_index, source_name)| DatasetColumn {
                dataset_name: transform.dataset_name(source_name),
                source_name: source_name.clone(),
                observed_index,
            })
            .collect(),
    };

    // A dataset name produced by more than one column is a collision only
    // when a rename put it there; identity renames are config-invalid, so a
    // renamed column is exactly one whose names differ. Columns are grouped
    // once by dataset name, then reported at the first colliding column in
    // dataset order, with its sources in dataset order too.
    let mut columns_by_dataset_name: HashMap<&str, Vec<&DatasetColumn>> = HashMap::new();
    for column in &columns {
        columns_by_dataset_name
            .entry(column.dataset_name.as_str())
            .or_default()
            .push(column);
    }
    for column in &columns {
        let colliding = &columns_by_dataset_name[column.dataset_name.as_str()];
        if colliding.len() > 1
            && colliding
                .iter()
                .any(|other| other.dataset_name != other.source_name)
        {
            let sources = colliding
                .iter()
                .map(|other| other.source_name.as_str())
                .collect::<Vec<_>>();
            return Err(pre_materialization_failure(
                LoadFailure {
                    code: "transform_name_collision",
                    message: format!(
                        "transform rename collides on dataset field {:?}: source fields {} map to the same name",
                        column.dataset_name,
                        sources.join(", ")
                    ),
                },
                configured_posture_decision(directive),
            ));
        }
    }

    Ok(columns)
}

/// The schema decision a failure echoes when the load failed before any
/// schema decision could be completed: the configured posture, with no drift
/// comparison run.
fn configured_posture_decision(directive: &SchemaDirective) -> Value {
    match directive {
        SchemaDirective::Inferred { .. } => json!({
            "mode": "inferred",
            "drift_status": "not_applicable",
        }),
        SchemaDirective::PinInferred { pinned_path, .. } => json!({
            "mode": "inferred",
            "drift_status": "not_applicable",
            "pinned_schema_path": pinned_path,
        }),
        SchemaDirective::Pinned {
            pinned_path, pin, ..
        } => json!({
            "mode": "pinned",
            "fields": pinned_fields_json(pin),
            "drift_status": "not_applicable",
            "pinned_schema_path": pinned_path,
        }),
    }
}

/// Materializes an inference-driven or pin-bootstrapping CSV load: overridden
/// fields validate per record like pinned fields (ADR-0038), and the
/// surviving records alone shape every property no override sets, so a
/// rejected record's values never shape the destination. `field_names` stays
/// the observed source shape — record width and rejection content — while
/// `columns` is the dataset view the load materializes.
fn inferred_text(
    columns: &[DatasetColumn],
    field_names: &[String],
    records: Vec<TextRecord>,
    pinned_path: Option<&str>,
    overrides: &SchemaOverrides,
) -> Result<Materialized, ExecutionFailure> {
    let checks = override_checks(overrides, columns.iter());
    let (survivors, rejected) = partition_text(records, &checks, field_names);
    let survivor_types = observe_text_types(field_names.len(), &survivors);
    let plan = inferred_plan(columns, &survivor_types, pinned_path, overrides);
    build_text(plan, &survivors, rejected)
}

/// Materializes an inference-driven or pin-bootstrapping JSONL load; see
/// [`inferred_text`].
fn inferred_json(
    columns: &[DatasetColumn],
    field_names: &[String],
    records: Vec<JsonRecord>,
    pinned_path: Option<&str>,
    overrides: &SchemaOverrides,
) -> Result<Materialized, ExecutionFailure> {
    let checks = override_checks(overrides, columns.iter());
    let (survivors, rejected) = partition_json(records, &checks);
    let survivor_types = observe_json_types(field_names, &survivors);
    let plan = inferred_plan(columns, &survivor_types, pinned_path, overrides);
    build_json(plan, &survivors, rejected)
}

/// Fails the load with `unknown_override_field` when an override names a
/// field absent from the dataset shape — the CSV header, or the union of the
/// JSONL batch's record keys, where a batch-wide-absent field is absent
/// (ADR-0038), after the structural transform, so overrides speak dataset
/// names and one naming a dropped source field is unknown (ADR-0040).
/// Checked as soon as the dataset names are known and before any pin
/// comparison, so a misspelled override never reads as drift.
fn check_override_names(
    directive: &SchemaDirective,
    columns: &[DatasetColumn],
) -> Result<(), ExecutionFailure> {
    let dataset_names = columns
        .iter()
        .map(|column| column.dataset_name.as_str())
        .collect::<HashSet<_>>();
    let unknown = directive.overrides().unknown_names(&dataset_names);
    if unknown.is_empty() {
        return Ok(());
    }

    // The load failed before any schema decision could be completed, so the
    // decision echoes the configured posture — and no drift comparison ran.
    Err(pre_materialization_failure(
        LoadFailure {
            code: "unknown_override_field",
            message: format!(
                "schema overrides name fields absent from the observed source shape: {}",
                unknown.join(", ")
            ),
        },
        configured_posture_decision(directive),
    ))
}

/// Fails the load with `schema_override_conflict` when an override
/// contradicts the pinned field it names on a property it sets (ADR-0038). A
/// field the pin already governs takes nothing from an override, so a
/// contradiction is a broken definition regardless of what this batch looks
/// like — which is why it is checked before drift comparison.
fn check_override_conflicts(
    pinned_path: &str,
    pin: &PinnedSchema,
    overrides: &SchemaOverrides,
) -> Result<(), ExecutionFailure> {
    for override_ in &overrides.overrides {
        let Some(pin_field) = pin.fields.iter().find(|field| field.name == override_.name) else {
            continue;
        };
        let mut segments = Vec::new();
        let mut override_json = serde_json::Map::new();
        if let Some(field_type) = override_.field_type {
            override_json.insert("type".to_string(), json!(field_type.name()));
            if field_type != pin_field.field_type {
                segments.push(format!(
                    "pinned type {}, override type {}",
                    pin_field.field_type.name(),
                    field_type.name()
                ));
            }
        }
        if let Some(nullable) = override_.nullable {
            override_json.insert("nullable".to_string(), json!(nullable));
            if nullable != pin_field.nullable {
                segments.push(format!(
                    "pinned nullable {}, override nullable {nullable}",
                    pin_field.nullable
                ));
            }
        }
        if segments.is_empty() {
            continue;
        }

        let decision = json!({
            "mode": "pinned",
            "fields": pinned_fields_json(pin),
            "drift_status": "not_applicable",
            "conflict": {
                "field": override_.name,
                "pinned": {
                    "type": pin_field.field_type.name(),
                    "nullable": pin_field.nullable,
                },
                "override": Value::Object(override_json),
            },
            "pinned_schema_path": pinned_path,
        });
        return Err(pre_materialization_failure(
            LoadFailure {
                code: "schema_override_conflict",
                message: format!(
                    "schema override for field {:?} contradicts pinned schema {pinned_path}: {}",
                    override_.name,
                    segments.join("; ")
                ),
            },
            decision,
        ));
    }
    Ok(())
}

/// An execution failure raised before any record was validated or written:
/// only the schema decision travels with it — no source count, no
/// rejections, no destination write.
fn pre_materialization_failure(failure: LoadFailure, decision: Value) -> ExecutionFailure {
    ExecutionFailure {
        failure,
        schema_decision: Some(Box::new(decision)),
        source_rows: None,
        written_records: 0,
        rejected: Vec::new(),
        destination_write: Box::new(DestinationWriteFacts::not_applicable()),
    }
}

/// One field the load will materialize: its dataset name, the source field it
/// reads from — by observed column index for CSV cells and by source name for
/// JSONL objects — the type its column is built as (never `Null`), and
/// whether its values may be null. The two names differ exactly when a rename
/// mapping changed the field's name (ADR-0039).
struct PlannedField {
    name: String,
    source_name: String,
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

/// Plans an inference-driven load: every dataset column keeps its observed
/// type (all-null columns default to text) and stays nullable, unless a
/// schema override — named in the dataset namespace — replaces either
/// property (ADR-0038, ADR-0040). With a `pinned_path`, the resulting —
/// overridden — schema is also rendered for persistence as the new pin
/// (ADR-0033). `observed_types` spans the full observed source shape and is
/// read through each column's observed index.
fn inferred_plan(
    columns: &[DatasetColumn],
    observed_types: &[InferredType],
    pinned_path: Option<&str>,
    overrides: &SchemaOverrides,
) -> FieldPlan {
    let fields = columns
        .iter()
        .map(|column| {
            let override_ = overrides.get(&column.dataset_name);
            PlannedField {
                name: column.dataset_name.clone(),
                source_name: column.source_name.clone(),
                materialized_type: override_
                    .and_then(|override_| override_.field_type)
                    .unwrap_or_else(|| default_null_to_text(observed_types[column.observed_index])),
                nullable: override_
                    .and_then(|override_| override_.nullable)
                    .unwrap_or(true),
                observed_index: column.observed_index,
            }
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

/// The outcome of matching the dataset columns against the pin by name: the
/// pin's fields planned in pin order, and the added dataset columns awaiting
/// their survivor-observed types.
struct ShapeMatch {
    matched: Vec<PlannedField>,
    added: Vec<DatasetColumn>,
}

/// Matches the dataset columns — the observed source fields after the
/// structural transform (ADR-0040) — against the pinned schema by name and
/// fails with `schema_drift` on shape drift: duplicate dataset field names, a
/// pinned field absent from every record, or an added field the drift policy
/// does not allow (ADR-0034). Value fit is not judged here — that is
/// per-record work (ADR-0035).
fn match_shape(
    pinned_path: &str,
    pin: &PinnedSchema,
    drift_policy: &DriftPolicy,
    columns: &[DatasetColumn],
) -> Result<ShapeMatch, ExecutionFailure> {
    // Dataset columns match pin fields by name, so duplicate names — only
    // pass-through source duplicates survive the transform's collision check,
    // where dataset and source names coincide — are unmatchable shape drift.
    let mut columns_by_name: HashMap<&str, &DatasetColumn> = HashMap::with_capacity(columns.len());
    for column in columns {
        if columns_by_name
            .insert(column.dataset_name.as_str(), column)
            .is_some()
        {
            return Err(drift_failure(
                pinned_path,
                pin,
                format!("source field {:?} appears more than once, so records cannot be validated against the pinned schema", column.dataset_name),
                json!({ "duplicate_fields": [column.dataset_name] }),
            ));
        }
    }

    let mut missing_fields = Vec::new();
    let mut matched = Vec::new();
    for pin_field in &pin.fields {
        match columns_by_name.get(pin_field.name.as_str()) {
            None => missing_fields.push(pin_field.name.clone()),
            Some(column) => matched.push(PlannedField {
                name: pin_field.name.clone(),
                source_name: column.source_name.clone(),
                materialized_type: pin_field.field_type,
                nullable: pin_field.nullable,
                observed_index: column.observed_index,
            }),
        }
    }
    let pinned_names = pin
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<HashSet<_>>();
    let added = columns
        .iter()
        .filter(|column| !pinned_names.contains(column.dataset_name.as_str()))
        .cloned()
        .collect::<Vec<_>>();

    let additive_allowed = matches!(drift_policy, DriftPolicy::AllowAdditiveNullable);
    if !missing_fields.is_empty() || (!added.is_empty() && !additive_allowed) {
        let added_names = added
            .iter()
            .map(|column| column.dataset_name.as_str())
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

/// One per-record value check (ADR-0035, ADR-0038): the dataset field it
/// guards, the source field it reads — by observed column index for CSV and
/// by source name for JSONL — the type its values must widen to — if any —
/// with the wording its rejections carry, and whether a null value rejects
/// the record. Pinned fields check everything the pin declares; overridden
/// fields check exactly the properties their override sets.
struct FieldCheck {
    name: String,
    source_name: String,
    observed_index: usize,
    expected_type: Option<(InferredType, TypeOrigin)>,
    required: bool,
}

impl FieldCheck {
    /// The source field a rejection names alongside the dataset field — set
    /// only when a rename mapping changed the rejected field's name
    /// (ADR-0039).
    fn renamed_source(&self) -> Option<String> {
        (self.name != self.source_name).then(|| self.source_name.clone())
    }
}

/// Where a field's expected type came from, for rejection messages: the
/// pinned schema (ADR-0035) or a schema override (ADR-0038).
#[derive(Clone, Copy)]
enum TypeOrigin {
    Pinned,
    Overridden,
}

impl TypeOrigin {
    fn wording(self) -> &'static str {
        match self {
            TypeOrigin::Pinned => "pinned",
            TypeOrigin::Overridden => "overridden",
        }
    }
}

/// The checks the matched pinned fields impose on every record, in pin order
/// (ADR-0035).
fn pinned_checks(matched: &[PlannedField]) -> Vec<FieldCheck> {
    matched
        .iter()
        .map(|planned| FieldCheck {
            name: planned.name.clone(),
            source_name: planned.source_name.clone(),
            observed_index: planned.observed_index,
            expected_type: Some((planned.materialized_type, TypeOrigin::Pinned)),
            required: !planned.nullable,
        })
        .collect()
}

/// The checks the overrides impose on the given dataset columns, in column
/// order: the overridden type must hold per value, and an override to
/// `nullable: false` makes the field required (ADR-0038). Columns without an
/// override — and properties an override leaves unset — check nothing.
fn override_checks<'a>(
    overrides: &SchemaOverrides,
    columns: impl Iterator<Item = &'a DatasetColumn>,
) -> Vec<FieldCheck> {
    columns
        .filter_map(|column| {
            overrides
                .get(&column.dataset_name)
                .map(|override_| FieldCheck {
                    name: column.dataset_name.clone(),
                    source_name: column.source_name.clone(),
                    observed_index: column.observed_index,
                    expected_type: override_
                        .field_type
                        .map(|field_type| (field_type, TypeOrigin::Overridden)),
                    required: override_.nullable == Some(false),
                })
        })
        .collect()
}

/// Splits CSV records into the survivors and the records the checks rejected
/// (ADR-0035).
fn partition_text(
    records: Vec<TextRecord>,
    checks: &[FieldCheck],
    field_names: &[String],
) -> (Vec<TextRecord>, Vec<RejectedRecord>) {
    let mut survivors = Vec::with_capacity(records.len());
    let mut rejected = Vec::new();
    for record in records {
        match validate_text_record(&record, checks, field_names) {
            Some(rejection) => rejected.push(rejection),
            None => survivors.push(record),
        }
    }
    (survivors, rejected)
}

/// Splits JSONL records into the survivors and the records the checks
/// rejected (ADR-0035).
fn partition_json(
    records: Vec<JsonRecord>,
    checks: &[FieldCheck],
) -> (Vec<JsonRecord>, Vec<RejectedRecord>) {
    let mut survivors = Vec::with_capacity(records.len());
    let mut rejected = Vec::new();
    for record in records {
        match validate_json_record(&record, checks) {
            Some(rejection) => rejected.push(rejection),
            None => survivors.push(record),
        }
    }
    (survivors, rejected)
}

/// Validates one CSV record against the field checks, in check order: the
/// first null cell in a required field or the first cell whose observed type
/// does not widen to its expected type rejects the record (ADR-0035). The
/// rejection names the dataset field while `record` keeps the original
/// source content under source names (ADR-0039).
fn validate_text_record(
    record: &TextRecord,
    checks: &[FieldCheck],
    field_names: &[String],
) -> Option<RejectedRecord> {
    for check in checks {
        match record.cells[check.observed_index].as_deref() {
            None => {
                if check.required {
                    return Some(required_field_rejection(
                        record.line,
                        check,
                        text_record_json(field_names, &record.cells),
                    ));
                }
            }
            Some(value) => {
                if let Some((expected_type, origin)) = check.expected_type {
                    if !fits_expected_type(infer_text_type(value), expected_type) {
                        return Some(type_rejection(
                            record.line,
                            check,
                            expected_type,
                            origin,
                            json!(value),
                            text_record_json(field_names, &record.cells),
                        ));
                    }
                }
            }
        }
    }
    None
}

/// Validates one JSONL record against the field checks; see
/// [`validate_text_record`]. Values are read under their source names; a
/// field absent from the record reads as null.
fn validate_json_record(record: &JsonRecord, checks: &[FieldCheck]) -> Option<RejectedRecord> {
    for check in checks {
        match record.object.get(&check.source_name) {
            None | Some(Value::Null) => {
                if check.required {
                    return Some(required_field_rejection(
                        record.line,
                        check,
                        Value::Object(record.object.clone()),
                    ));
                }
            }
            Some(value) => {
                if let Some((expected_type, origin)) = check.expected_type {
                    if !fits_expected_type(infer_json_type(value), expected_type) {
                        return Some(type_rejection(
                            record.line,
                            check,
                            expected_type,
                            origin,
                            value.clone(),
                            Value::Object(record.object.clone()),
                        ));
                    }
                }
            }
        }
    }
    None
}

/// A value fits a pinned or overridden field iff its observed type widens to
/// the expected type under the inference lattice — the per-cell restriction
/// of the ADR-0034 column rule (ADR-0035, ADR-0038). Building a surviving
/// record's cell with its expected type can then never fail per value.
fn fits_expected_type(observed: InferredType, expected: InferredType) -> bool {
    observed.merge(expected) == expected
}

fn required_field_rejection(line: u64, check: &FieldCheck, record: Value) -> RejectedRecord {
    RejectedRecord {
        line,
        code: rejection::MISSING_REQUIRED_FIELD,
        field: Some(check.name.clone()),
        source_field: check.renamed_source(),
        message: format!("required field {:?} is null", check.name),
        record,
    }
}

fn type_rejection(
    line: u64,
    check: &FieldCheck,
    expected_type: InferredType,
    origin: TypeOrigin,
    value: Value,
    record: Value,
) -> RejectedRecord {
    RejectedRecord {
        line,
        code: rejection::TYPE_COERCION_FAILED,
        field: Some(check.name.clone()),
        source_field: check.renamed_source(),
        message: format!(
            "value {value} does not fit {} type {} for field {:?}",
            origin.wording(),
            expected_type.name(),
            check.name
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
/// text). Added fields default to nullable — inference cannot prove more —
/// but an override naming an added field is explicit intent and wins over
/// both defaults, including `nullable: false` (ADR-0038); the rewritten pin
/// then records the overridden properties.
fn planned_added_fields(
    added: Vec<DatasetColumn>,
    survivor_types: &[InferredType],
    overrides: &SchemaOverrides,
) -> Vec<PlannedField> {
    added
        .into_iter()
        .map(|column| {
            let override_ = overrides.get(&column.dataset_name);
            PlannedField {
                materialized_type: override_
                    .and_then(|override_| override_.field_type)
                    .unwrap_or_else(|| default_null_to_text(survivor_types[column.observed_index])),
                nullable: override_
                    .and_then(|override_| override_.nullable)
                    .unwrap_or(true),
                name: column.dataset_name,
                source_name: column.source_name,
                observed_index: column.observed_index,
            }
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
    pre_materialization_failure(
        LoadFailure {
            code: "schema_drift",
            message: format!("schema drift against pinned schema {pinned_path}: {detail}"),
        },
        decision,
    )
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

/// Builds the planned columns over the surviving JSONL records — read under
/// their source names — and assembles the batch.
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
                &planned.source_name,
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
/// picks a type every cell already carries, and pinned and overridden fields
/// build only over surviving records, whose cells per-record validation proved
/// to fit (ADR-0035, ADR-0038). They return a clean failure rather than
/// panicking if that invariant is ever broken.
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
            &SchemaDirective::inferred(),
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
            &SchemaDirective::inferred(),
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
            &SchemaDirective::inferred(),
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
            &SchemaDirective::inferred(),
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
            &SchemaDirective::inferred(),
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
            from_json_columns(&SchemaDirective::inferred(), names(&["amount"]), records)
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
            &SchemaDirective::inferred(),
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
        let materialized =
            from_json_columns(&SchemaDirective::inferred(), names(&["note"]), records)
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

    #[test]
    fn pinned_schema_rejects_unknown_keys_at_top_level_and_in_field_entries() {
        // Strict contract (ADR-0037): a key the pin contract does not declare
        // is a parse failure naming the key, never a silently ignored no-op.
        for (yaml, unknown_field) in [
            (
                "version: 1\nowner: bi-team\nfields:\n- name: id\n  type: int64\n",
                "owner",
            ),
            (
                "version: 1\nfields:\n- name: id\n  type: int64\n  description: primary key\n",
                "description",
            ),
        ] {
            let error = PinnedSchema::from_yaml(yaml)
                .err()
                .unwrap_or_else(|| panic!("pinned schema {yaml:?} accepted"));
            assert_eq!(error.code, "invalid_pinned_schema", "code for {yaml:?}");
            assert!(
                error
                    .message
                    .contains(&format!("unknown field `{unknown_field}`")),
                "message {:?} misses the rejected field {unknown_field:?}",
                error.message
            );
        }
    }

    // ---- Pinned materialization and drift (ADR-0034) ----

    fn pinned_directive(pin_yaml: &str, drift_policy: DriftPolicy) -> SchemaDirective {
        overridden_pinned_directive(pin_yaml, drift_policy, SchemaOverrides::none())
    }

    fn overridden_pinned_directive(
        pin_yaml: &str,
        drift_policy: DriftPolicy,
        overrides: SchemaOverrides,
    ) -> SchemaDirective {
        transformed_pinned_directive(pin_yaml, drift_policy, SchemaTransform::none(), overrides)
    }

    fn transformed_pinned_directive(
        pin_yaml: &str,
        drift_policy: DriftPolicy,
        transform: SchemaTransform,
        overrides: SchemaOverrides,
    ) -> SchemaDirective {
        SchemaDirective::Pinned {
            pinned_path: "customers.schema.yml".to_string(),
            pin: PinnedSchema::from_yaml(pin_yaml).expect("test pin parses"),
            drift_policy,
            transform,
            overrides,
        }
    }

    /// Builds validated overrides from `(name, type, nullable)` triples.
    fn overrides(entries: &[(&str, Option<&str>, Option<bool>)]) -> SchemaOverrides {
        SchemaOverrides::from_entries(
            &entries
                .iter()
                .map(|(name, field_type, nullable)| OverrideEntry {
                    name: name.to_string(),
                    field_type: field_type.map(str::to_string),
                    nullable: *nullable,
                })
                .collect::<Vec<_>>(),
        )
        .expect("test overrides validate")
    }

    fn overridden_inferred_directive(
        entries: &[(&str, Option<&str>, Option<bool>)],
    ) -> SchemaDirective {
        SchemaDirective::Inferred {
            transform: SchemaTransform::none(),
            overrides: overrides(entries),
        }
    }

    /// Builds a validated transform from the `transform` block's YAML text.
    fn transform(yaml: &str) -> SchemaTransform {
        SchemaTransform::from_config(
            &serde_yaml::from_str::<TransformConfig>(yaml).expect("test transform parses"),
        )
        .expect("test transform validates")
    }

    fn transformed_inferred_directive(yaml: &str) -> SchemaDirective {
        SchemaDirective::Inferred {
            transform: transform(yaml),
            overrides: SchemaOverrides::none(),
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
                transform: SchemaTransform::none(),
                overrides: SchemaOverrides::none(),
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

    // ---- Schema overrides (ADR-0038) ----

    #[test]
    fn from_text_columns_applies_overrides_to_inferred_fields_and_rejects_misfits() {
        // `account` infers utf8 because of "n/a"; the override corrects it to
        // int64 — the ADR-0038 core scenario — so the dirty record is
        // rejected per record exactly like a pinned misfit and the survivors
        // materialize under the overridden type.
        let materialized = from_text_columns(
            &overridden_inferred_directive(&[("account", Some("int64"), None)]),
            names(&["account", "note"]),
            vec![
                record(2, &[Some("42"), Some("a")]),
                record(3, &[Some("n/a"), Some("b")]),
                record(4, &[Some("7"), Some("c")]),
            ],
        )
        .expect("override misfits reject records, not the load");
        let batch = &materialized.batch;

        assert_eq!(schema_types(batch), vec![DataType::Int64, DataType::Utf8]);
        assert_eq!(ints(batch, 0).value(0), 42);
        assert_eq!(ints(batch, 0).value(1), 7);

        assert_eq!(materialized.rejected.len(), 1);
        let rejected = &materialized.rejected[0];
        assert_eq!(rejected.line, 3);
        assert_eq!(rejected.code, "type_coercion_failed");
        assert_eq!(rejected.field.as_deref(), Some("account"));
        assert_eq!(
            rejected.message,
            "value \"n/a\" does not fit overridden type int64 for field \"account\""
        );
        assert_eq!(rejected.record, json!({ "account": "n/a", "note": "b" }));

        assert_eq!(
            materialized.schema_decision,
            json!({
                "mode": "inferred",
                "fields": [
                    {"name": "account", "type": "int64", "nullable": true},
                    {"name": "note", "type": "utf8", "nullable": true}
                ],
                "drift_status": "not_applicable",
                "overrides": [
                    {"name": "account", "type": "int64"}
                ]
            })
        );
    }

    #[test]
    fn from_text_columns_shapes_unoverridden_fields_from_surviving_records_only() {
        // The record rejected by the `account` override carries the only text
        // in `score`: its value must not widen the surviving column, which
        // types from survivors alone.
        let materialized = from_text_columns(
            &overridden_inferred_directive(&[("account", Some("int64"), None)]),
            names(&["account", "score"]),
            vec![
                record(2, &[Some("1"), Some("10")]),
                record(3, &[Some("bad"), Some("high")]),
            ],
        )
        .expect("materialize");

        assert_eq!(
            schema_types(&materialized.batch),
            vec![DataType::Int64, DataType::Int64]
        );
        assert_eq!(materialized.rejected.len(), 1);
    }

    #[test]
    fn from_text_columns_overrides_an_all_null_column_to_the_overridden_type() {
        // An all-null column defaults to text under inference; the override
        // decides the type instead, and null cells still fit a nullable
        // overridden field.
        let materialized = from_text_columns(
            &overridden_inferred_directive(&[("score", Some("int64"), None)]),
            names(&["score"]),
            vec![record(2, &[None]), record(3, &[None])],
        )
        .expect("materialize");

        assert_eq!(schema_types(&materialized.batch), vec![DataType::Int64]);
        assert!(ints(&materialized.batch, 0).is_null(0));
        assert!(materialized.rejected.is_empty());
    }

    #[test]
    fn from_json_columns_rejects_null_and_absent_under_a_non_nullable_override() {
        // A `nullable: false` override makes the field required (ADR-0038)
        // with pinned-field per-record semantics (ADR-0035): a JSON null and
        // an absent field both reject their record. The type stays inferred
        // from the survivors, and the materialized Arrow field is
        // non-nullable.
        let materialized = from_json_columns(
            &SchemaDirective::Inferred {
                transform: SchemaTransform::none(),
                overrides: overrides(&[("email", None, Some(false))]),
            },
            names(&["id", "email"]),
            vec![
                json_record(1, r#"{"id": 1, "email": "a@example.com"}"#),
                json_record(2, r#"{"id": 2, "email": null}"#),
                json_record(3, r#"{"id": 3}"#),
            ],
        )
        .expect("required-field violations reject records, not the load");
        let batch = &materialized.batch;

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(schema_types(batch), vec![DataType::Int64, DataType::Utf8]);
        assert!(!batch.schema().field(1).is_nullable());
        assert_eq!(materialized.rejected.len(), 2);
        for (rejected, line) in [
            (&materialized.rejected[0], 2),
            (&materialized.rejected[1], 3),
        ] {
            assert_eq!(rejected.line, line);
            assert_eq!(rejected.code, "missing_required_field");
            assert_eq!(rejected.field.as_deref(), Some("email"));
            assert_eq!(rejected.message, "required field \"email\" is null");
        }
        assert_eq!(
            materialized.schema_decision["overrides"],
            json!([{"name": "email", "nullable": false}])
        );
    }

    #[test]
    fn from_json_columns_rejects_a_json_string_against_an_overridden_numeric_field() {
        // A JSON string that merely looks numeric misfits an overridden int64
        // exactly like it misfits a pinned int64: the value's declared type
        // does not widen to the override.
        let materialized = from_json_columns(
            &overridden_inferred_directive(&[("balance", Some("int64"), None)]),
            names(&["balance"]),
            vec![
                json_record(1, r#"{"balance": 7}"#),
                json_record(2, r#"{"balance": "10"}"#),
            ],
        )
        .expect("string vs overridden int64 rejects the record");

        assert_eq!(materialized.batch.num_rows(), 1);
        assert_eq!(ints(&materialized.batch, 0).value(0), 7);
        assert_eq!(materialized.rejected.len(), 1);
        assert_eq!(
            materialized.rejected[0].message,
            "value \"10\" does not fit overridden type int64 for field \"balance\""
        );
    }

    #[test]
    fn from_json_columns_widens_values_into_an_overridden_wider_type() {
        // Overridden fields build like pinned fields: integer cells widen
        // into an overridden float64 column per the lattice.
        let materialized = from_json_columns(
            &overridden_inferred_directive(&[("amount", Some("float64"), None)]),
            names(&["amount"]),
            vec![json_record(1, r#"{"amount": 10}"#)],
        )
        .expect("materialize");

        assert_eq!(schema_types(&materialized.batch), vec![DataType::Float64]);
        assert_eq!(floats(&materialized.batch, 0).value(0), 10.0);
        assert!(materialized.rejected.is_empty());
    }

    #[test]
    fn from_text_columns_persists_the_overridden_schema_as_the_new_pin() {
        // The bootstrap load persists the overridden schema (ADR-0038): the
        // pin records the effective schema, with no override annotation.
        let materialized = from_text_columns(
            &SchemaDirective::PinInferred {
                pinned_path: "customers.schema.yml".to_string(),
                transform: SchemaTransform::none(),
                overrides: overrides(&[("customer_id", Some("utf8"), Some(false))]),
            },
            names(&["customer_id", "total"]),
            vec![record(2, &[Some("1"), Some("42.5")])],
        )
        .expect("materialize");

        assert_eq!(
            materialized.schema_decision,
            json!({
                "mode": "inferred",
                "fields": [
                    {"name": "customer_id", "type": "utf8", "nullable": false},
                    {"name": "total", "type": "float64", "nullable": true}
                ],
                "drift_status": "not_applicable",
                "pinned_schema_path": "customers.schema.yml",
                "pinned_schema_persisted": true,
                "overrides": [
                    {"name": "customer_id", "type": "utf8", "nullable": false}
                ]
            })
        );
        assert_eq!(
            materialized
                .pinned_schema_write
                .expect("bootstrap pin")
                .yaml,
            "version: 1\n\
             fields:\n\
             - name: customer_id\n\
             \x20 type: utf8\n\
             \x20 nullable: false\n\
             - name: total\n\
             \x20 type: float64\n\
             \x20 nullable: true\n"
        );
    }

    #[test]
    fn from_text_columns_fails_on_an_override_naming_an_unobserved_field() {
        // An override names the observed source shape — for CSV, the header.
        // A name the source does not carry fails the load before anything is
        // materialized, echoing the configured posture.
        let error = from_text_columns(
            &overridden_inferred_directive(&[("vip", Some("boolean"), None)]),
            names(&["id", "name"]),
            vec![record(2, &[Some("1"), Some("Ada")])],
        )
        .err()
        .expect("unknown override field rejected");

        assert_eq!(error.failure.code, "unknown_override_field");
        assert_eq!(
            error.failure.message,
            "schema overrides name fields absent from the observed source shape: vip"
        );
        assert!(error.rejected.is_empty());
        assert_eq!(
            *error.schema_decision.expect("decision echoes the posture"),
            json!({
                "mode": "inferred",
                "drift_status": "not_applicable",
                "overrides": [
                    {"name": "vip", "type": "boolean"}
                ]
            })
        );
    }

    #[test]
    fn from_json_columns_treats_a_batch_wide_absent_override_target_as_unknown() {
        // JSONL's observed shape is the union of the batch's record keys, so
        // a field absent from every record is absent from the shape — an
        // override naming it is unknown, mirroring missing-field drift's
        // batch-wide rule (ADR-0034).
        let error = from_json_columns(
            &overridden_inferred_directive(&[("email", None, Some(false))]),
            names(&["id"]),
            vec![json_record(1, r#"{"id": 1}"#)],
        )
        .err()
        .expect("batch-wide absent override target rejected");

        assert_eq!(error.failure.code, "unknown_override_field");
    }

    #[test]
    fn unknown_override_fields_fail_before_missing_field_drift() {
        // The override names a pinned field the source batch does not carry:
        // both an unknown override name and missing-field drift are present,
        // and the unknown override wins — it is checked as soon as the
        // observed names are known, before any pin comparison (ADR-0038).
        let directive = overridden_pinned_directive(
            "version: 1\n\
             fields:\n\
             - name: id\n\
             \x20 type: int64\n\
             - name: name\n\
             \x20 type: utf8\n",
            DriftPolicy::Fail,
            overrides(&[("name", None, Some(false))]),
        );
        let error = from_text_columns(&directive, names(&["id"]), vec![record(2, &[Some("1")])])
            .err()
            .expect("unknown override field rejected");

        assert_eq!(error.failure.code, "unknown_override_field");
        assert_eq!(
            *error.schema_decision.expect("decision echoes the pin"),
            json!({
                "mode": "pinned",
                "fields": [
                    {"name": "id", "type": "int64", "nullable": true},
                    {"name": "name", "type": "utf8", "nullable": true}
                ],
                "drift_status": "not_applicable",
                "pinned_schema_path": "customers.schema.yml",
                "overrides": [
                    {"name": "name", "nullable": false}
                ]
            })
        );
    }

    #[test]
    fn overrides_conflicting_with_the_pin_fail_as_override_conflicts() {
        // A field the pin governs takes nothing from an override, but the
        // override must agree with it: a contradiction on a set property
        // fails the load with the conflict detail (ADR-0038).
        let directive = overridden_pinned_directive(
            "version: 1\nfields:\n- name: id\n  type: int64\n",
            DriftPolicy::Fail,
            overrides(&[("id", Some("utf8"), None)]),
        );
        let error = from_text_columns(&directive, names(&["id"]), vec![record(2, &[Some("1")])])
            .err()
            .expect("conflicting override rejected");

        assert_eq!(error.failure.code, "schema_override_conflict");
        assert_eq!(
            error.failure.message,
            "schema override for field \"id\" contradicts pinned schema \
             customers.schema.yml: pinned type int64, override type utf8"
        );
        assert_eq!(
            *error
                .schema_decision
                .expect("decision carries the conflict"),
            json!({
                "mode": "pinned",
                "fields": [
                    {"name": "id", "type": "int64", "nullable": true}
                ],
                "drift_status": "not_applicable",
                "conflict": {
                    "field": "id",
                    "pinned": {"type": "int64", "nullable": true},
                    "override": {"type": "utf8"}
                },
                "pinned_schema_path": "customers.schema.yml",
                "overrides": [
                    {"name": "id", "type": "utf8"}
                ]
            })
        );
    }

    #[test]
    fn override_conflicts_fail_before_drift_comparison() {
        // A definition contradicting its pin is broken regardless of what the
        // batch looks like, so the conflict wins over the added-field drift
        // also present in this source.
        let directive = overridden_pinned_directive(
            "version: 1\nfields:\n- name: id\n  type: int64\n",
            DriftPolicy::Fail,
            overrides(&[("id", None, Some(false))]),
        );
        let error = from_text_columns(
            &directive,
            names(&["id", "extra"]),
            vec![record(2, &[Some("1"), Some("x")])],
        )
        .err()
        .expect("conflicting override rejected");

        assert_eq!(error.failure.code, "schema_override_conflict");
        assert_eq!(
            error.failure.message,
            "schema override for field \"id\" contradicts pinned schema \
             customers.schema.yml: pinned nullable true, override nullable false"
        );
    }

    #[test]
    fn overrides_agreeing_with_the_pin_change_nothing() {
        // An override that restates what the pin declares is consistent — the
        // load validates exactly as without it, still wording rejections as
        // pinned misfits — and the decision still echoes the directive.
        let directive = overridden_pinned_directive(
            "version: 1\nfields:\n- name: id\n  type: int64\n",
            DriftPolicy::Fail,
            overrides(&[("id", Some("int64"), None)]),
        );
        let materialized = from_text_columns(
            &directive,
            names(&["id"]),
            vec![record(2, &[Some("1")]), record(3, &[Some("abc")])],
        )
        .expect("agreeing override loads");

        assert_eq!(materialized.batch.num_rows(), 1);
        assert_eq!(materialized.rejected.len(), 1);
        assert_eq!(
            materialized.rejected[0].message,
            "value \"abc\" does not fit pinned type int64 for field \"id\""
        );
        assert_eq!(materialized.schema_decision["drift_status"], "none");
        assert_eq!(
            materialized.schema_decision["overrides"],
            json!([{"name": "id", "type": "int64"}])
        );
    }

    #[test]
    fn from_text_columns_applies_overrides_to_added_fields_and_the_extended_pin() {
        // Under the additive policy an override naming the added field is
        // explicit intent: it beats the policy's nullable default and the
        // survivor-observed type, the required field rejects the record that
        // leaves it null, and the rewritten pin records the overridden
        // properties (ADR-0038).
        let directive = overridden_pinned_directive(
            "version: 1\nfields:\n- name: id\n  type: int64\n",
            DriftPolicy::AllowAdditiveNullable,
            overrides(&[("vip", Some("utf8"), Some(false))]),
        );
        let materialized = from_text_columns(
            &directive,
            names(&["id", "vip"]),
            vec![
                record(2, &[Some("1"), Some("true")]),
                record(3, &[Some("2"), None]),
            ],
        )
        .expect("materialize");
        let batch = &materialized.batch;

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(schema_types(batch), vec![DataType::Int64, DataType::Utf8]);
        assert!(!batch.schema().field(1).is_nullable());
        assert_eq!(strings(batch, 1).value(0), "true");

        assert_eq!(materialized.rejected.len(), 1);
        assert_eq!(materialized.rejected[0].code, "missing_required_field");
        assert_eq!(materialized.rejected[0].field.as_deref(), Some("vip"));

        assert_eq!(
            materialized.schema_decision,
            json!({
                "mode": "pinned",
                "fields": [
                    {"name": "id", "type": "int64", "nullable": true},
                    {"name": "vip", "type": "utf8", "nullable": false}
                ],
                "drift_status": "additive_fields_added",
                "added_fields": [
                    {"name": "vip", "type": "utf8", "nullable": false}
                ],
                "pinned_schema_path": "customers.schema.yml",
                "pinned_schema_persisted": true,
                "overrides": [
                    {"name": "vip", "type": "utf8", "nullable": false}
                ]
            })
        );
        assert_eq!(
            materialized.pinned_schema_write.expect("extended pin").yaml,
            "version: 1\n\
             fields:\n\
             - name: id\n\
             \x20 type: int64\n\
             \x20 nullable: true\n\
             - name: vip\n\
             \x20 type: utf8\n\
             \x20 nullable: false\n"
        );
    }

    #[test]
    fn drift_failures_echo_the_configured_overrides() {
        // The overrides echo rides every decision the schema module reports,
        // failure paths included.
        let directive = overridden_pinned_directive(
            "version: 1\nfields:\n- name: id\n  type: int64\n",
            DriftPolicy::Fail,
            overrides(&[("extra", Some("utf8"), None)]),
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
            error.schema_decision.expect("decision")["overrides"],
            json!([{"name": "extra", "type": "utf8"}])
        );
    }

    // ---- Structural transforms (ADR-0039, ADR-0040) ----

    fn batch_field_names(batch: &RecordBatch) -> Vec<String> {
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect()
    }

    #[test]
    fn from_text_columns_selects_fields_in_select_order_and_renames_them() {
        // The select list fixes the dataset order (total before id), the
        // rename maps id → customer_id, and the unselected `note` column
        // vanishes. The decision echoes the transform as written.
        let materialized = from_text_columns(
            &transformed_inferred_directive("select: [total, id]\nrename: {id: customer_id}"),
            names(&["id", "note", "total"]),
            vec![
                record(2, &[Some("1"), Some("a"), Some("42.5")]),
                record(3, &[Some("2"), Some("b"), Some("7.25")]),
            ],
        )
        .expect("materialize");
        let batch = &materialized.batch;

        assert_eq!(batch_field_names(batch), ["total", "customer_id"]);
        assert_eq!(
            schema_types(batch),
            vec![DataType::Float64, DataType::Int64]
        );
        assert_eq!(floats(batch, 0).value(0), 42.5);
        assert_eq!(ints(batch, 1).value(1), 2);
        assert!(materialized.rejected.is_empty());
        assert_eq!(
            materialized.schema_decision,
            json!({
                "mode": "inferred",
                "fields": [
                    {"name": "total", "type": "float64", "nullable": true},
                    {"name": "customer_id", "type": "int64", "nullable": true}
                ],
                "drift_status": "not_applicable",
                "transform": {
                    "select": ["total", "id"],
                    "rename": {"id": "customer_id"}
                }
            })
        );
    }

    #[test]
    fn from_json_columns_applies_rename_swaps_simultaneously() {
        // {a: b, b: a} is a legal swap: rename keys are source names
        // evaluated at once, values are still read under their source names,
        // and the unmapped `c` passes through.
        let materialized = from_json_columns(
            &transformed_inferred_directive("rename: {a: b, b: a}"),
            names(&["a", "b", "c"]),
            vec![json_record(1, r#"{"a": 1, "b": "x", "c": true}"#)],
        )
        .expect("materialize");
        let batch = &materialized.batch;

        assert_eq!(batch_field_names(batch), ["b", "a", "c"]);
        assert_eq!(
            schema_types(batch),
            vec![DataType::Int64, DataType::Utf8, DataType::Boolean]
        );
        // Dataset b reads source a and dataset a reads source b.
        assert_eq!(ints(batch, 0).value(0), 1);
        assert_eq!(strings(batch, 1).value(0), "x");
        assert!(bools(batch, 2).value(0));
        assert_eq!(
            materialized.schema_decision["transform"],
            json!({ "rename": {"a": "b", "b": "a"} })
        );
    }

    #[test]
    fn from_text_columns_fails_on_a_transform_naming_an_unobserved_field() {
        // A select entry the source does not carry fails the load before
        // anything is materialized, named as the user wrote it, with the
        // decision echoing the configured posture.
        let error = from_text_columns(
            &transformed_inferred_directive("select: [id, vip]\nrename: {id: customer_id}"),
            names(&["id", "name"]),
            vec![record(2, &[Some("1"), Some("Ada")])],
        )
        .err()
        .expect("unknown transform field rejected");

        assert_eq!(error.failure.code, "unknown_transform_field");
        assert_eq!(
            error.failure.message,
            "transform selects or renames fields absent from the observed source shape: vip"
        );
        assert!(error.rejected.is_empty());
        assert_eq!(
            *error.schema_decision.expect("decision echoes the posture"),
            json!({
                "mode": "inferred",
                "drift_status": "not_applicable",
                "transform": {
                    "select": ["id", "vip"],
                    "rename": {"id": "customer_id"}
                }
            })
        );
    }

    #[test]
    fn from_json_columns_treats_a_batch_wide_absent_transform_field_as_unknown() {
        // JSONL's observed shape is the union of the batch's record keys, so
        // a rename key absent from every record is unknown, mirroring the
        // override and missing-field-drift batch-wide rules (ADR-0034).
        let error = from_json_columns(
            &transformed_inferred_directive("rename: {email: contact_email}"),
            names(&["id"]),
            vec![json_record(1, r#"{"id": 1}"#)],
        )
        .err()
        .expect("batch-wide absent transform field rejected");

        assert_eq!(error.failure.code, "unknown_transform_field");
        assert_eq!(
            error.failure.message,
            "transform selects or renames fields absent from the observed source shape: email"
        );
    }

    #[test]
    fn from_text_columns_fails_when_a_rename_target_collides_with_a_pass_through_field() {
        // Without select, every unmapped field passes through: renaming
        // legacy_id onto the still-present id collides on the final name.
        let error = from_text_columns(
            &transformed_inferred_directive("rename: {legacy_id: id}"),
            names(&["id", "legacy_id"]),
            vec![record(2, &[Some("1"), Some("2")])],
        )
        .err()
        .expect("collision rejected");

        assert_eq!(error.failure.code, "transform_name_collision");
        assert_eq!(
            error.failure.message,
            "transform rename collides on dataset field \"id\": \
             source fields id, legacy_id map to the same name"
        );
        assert_eq!(
            *error.schema_decision.expect("decision echoes the posture"),
            json!({
                "mode": "inferred",
                "drift_status": "not_applicable",
                "transform": {
                    "rename": {"legacy_id": "id"}
                }
            })
        );
    }

    #[test]
    fn unknown_transform_fields_fail_before_unknown_override_fields() {
        // Both the transform and the overrides name unobserved fields: the
        // transform resolves first — overrides speak the dataset namespace it
        // produces — so its code wins.
        let directive = SchemaDirective::Inferred {
            transform: transform("select: [ghost]"),
            overrides: overrides(&[("phantom", Some("int64"), None)]),
        };
        let error = from_text_columns(&directive, names(&["id"]), vec![record(2, &[Some("1")])])
            .err()
            .expect("unknown transform field rejected");

        assert_eq!(error.failure.code, "unknown_transform_field");
    }

    #[test]
    fn unknown_transform_fields_fail_before_missing_field_drift() {
        // The pin misses a field and the transform names a ghost: the
        // transform resolves before any pin comparison, and the posture
        // decision echoes the pin without a drift comparison.
        let directive = transformed_pinned_directive(
            "version: 1\n\
             fields:\n\
             - name: id\n\
             \x20 type: int64\n\
             - name: name\n\
             \x20 type: utf8\n",
            DriftPolicy::Fail,
            transform("select: [ghost]"),
            SchemaOverrides::none(),
        );
        let error = from_text_columns(&directive, names(&["id"]), vec![record(2, &[Some("1")])])
            .err()
            .expect("unknown transform field rejected");

        assert_eq!(error.failure.code, "unknown_transform_field");
        assert_eq!(
            *error.schema_decision.expect("decision echoes the pin"),
            json!({
                "mode": "pinned",
                "fields": [
                    {"name": "id", "type": "int64", "nullable": true},
                    {"name": "name", "type": "utf8", "nullable": true}
                ],
                "drift_status": "not_applicable",
                "pinned_schema_path": "customers.schema.yml",
                "transform": {
                    "select": ["ghost"]
                }
            })
        );
    }

    #[test]
    fn overrides_name_dataset_fields_after_the_transform() {
        // The override names the renamed dataset field and rewrites it; the
        // decision carries both echoes.
        let directive = SchemaDirective::Inferred {
            transform: transform("select: [id]\nrename: {id: customer_id}"),
            overrides: overrides(&[("customer_id", Some("utf8"), None)]),
        };
        let materialized = from_text_columns(
            &directive,
            names(&["id", "note"]),
            vec![record(2, &[Some("1"), Some("x")])],
        )
        .expect("materialize");

        assert_eq!(batch_field_names(&materialized.batch), ["customer_id"]);
        assert_eq!(schema_types(&materialized.batch), vec![DataType::Utf8]);
        assert_eq!(strings(&materialized.batch, 0).value(0), "1");
        assert_eq!(
            materialized.schema_decision["overrides"],
            json!([{"name": "customer_id", "type": "utf8"}])
        );
        assert_eq!(
            materialized.schema_decision["transform"],
            json!({
                "select": ["id"],
                "rename": {"id": "customer_id"}
            })
        );
    }

    #[test]
    fn overrides_naming_a_dropped_or_pre_rename_source_field_are_unknown() {
        // Overrides speak dataset names (ADR-0040): the dropped `note` and
        // the pre-rename `id` are both absent from the dataset shape.
        for override_name in ["note", "id"] {
            let directive = SchemaDirective::Inferred {
                transform: transform("select: [id]\nrename: {id: customer_id}"),
                overrides: overrides(&[(override_name, Some("utf8"), None)]),
            };
            let error = from_text_columns(
                &directive,
                names(&["id", "note"]),
                vec![record(2, &[Some("1"), Some("x")])],
            )
            .err()
            .expect("override outside the dataset namespace rejected");

            assert_eq!(
                error.failure.code, "unknown_override_field",
                "code for override {override_name:?}"
            );
        }
    }

    #[test]
    fn from_text_columns_persists_the_transformed_schema_as_the_new_pin() {
        // The bootstrap pin records the dataset shape: dataset names, in
        // select order (ADR-0040).
        let materialized = from_text_columns(
            &SchemaDirective::PinInferred {
                pinned_path: "customers.schema.yml".to_string(),
                transform: transform("select: [total, id]\nrename: {id: customer_id}"),
                overrides: SchemaOverrides::none(),
            },
            names(&["id", "note", "total"]),
            vec![record(2, &[Some("1"), Some("x"), Some("42.5")])],
        )
        .expect("materialize");

        assert_eq!(
            materialized.schema_decision,
            json!({
                "mode": "inferred",
                "fields": [
                    {"name": "total", "type": "float64", "nullable": true},
                    {"name": "customer_id", "type": "int64", "nullable": true}
                ],
                "drift_status": "not_applicable",
                "pinned_schema_path": "customers.schema.yml",
                "pinned_schema_persisted": true,
                "transform": {
                    "select": ["total", "id"],
                    "rename": {"id": "customer_id"}
                }
            })
        );
        assert_eq!(
            materialized
                .pinned_schema_write
                .expect("bootstrap pin")
                .yaml,
            "version: 1\n\
             fields:\n\
             - name: total\n\
             \x20 type: float64\n\
             \x20 nullable: true\n\
             - name: customer_id\n\
             \x20 type: int64\n\
             \x20 nullable: true\n"
        );
    }

    #[test]
    fn unselected_source_fields_are_invisible_to_drift() {
        // The pin governs the selected dataset shape: a new unselected source
        // field yields no drift even under the fail policy (ADR-0040).
        let directive = transformed_pinned_directive(
            "version: 1\nfields:\n- name: customer_id\n  type: int64\n",
            DriftPolicy::Fail,
            transform("select: [id]\nrename: {id: customer_id}"),
            SchemaOverrides::none(),
        );
        let materialized = from_text_columns(
            &directive,
            names(&["id", "surprise"]),
            vec![record(2, &[Some("1"), Some("x")])],
        )
        .expect("unselected fields are shielded from drift");

        assert_eq!(batch_field_names(&materialized.batch), ["customer_id"]);
        assert_eq!(materialized.schema_decision["drift_status"], "none");
        assert!(materialized.pinned_schema_write.is_none());
    }

    #[test]
    fn rename_only_transforms_leave_additive_drift_behaving_as_today() {
        // Without select, a new source field passes through and additive
        // drift extends the pin exactly as before the transform existed.
        let directive = transformed_pinned_directive(
            "version: 1\nfields:\n- name: customer_id\n  type: int64\n",
            DriftPolicy::AllowAdditiveNullable,
            transform("rename: {id: customer_id}"),
            SchemaOverrides::none(),
        );
        let materialized = from_text_columns(
            &directive,
            names(&["id", "vip"]),
            vec![record(2, &[Some("1"), Some("true")])],
        )
        .expect("additive drift allowed");

        assert_eq!(
            materialized.schema_decision["drift_status"],
            "additive_fields_added"
        );
        assert_eq!(
            materialized.schema_decision["added_fields"],
            json!([{"name": "vip", "type": "boolean", "nullable": true}])
        );
        assert_eq!(
            materialized.pinned_schema_write.expect("extended pin").yaml,
            "version: 1\n\
             fields:\n\
             - name: customer_id\n\
             \x20 type: int64\n\
             \x20 nullable: true\n\
             - name: vip\n\
             \x20 type: boolean\n\
             \x20 nullable: true\n"
        );
    }

    #[test]
    fn a_pin_recorded_before_the_transform_fails_as_schema_drift() {
        // The old pin records source names; the transform now renames them,
        // so the dataset shape misses the pinned name and adds the new one —
        // drift under the fail policy, with no pin migration (ADR-0040).
        let directive = transformed_pinned_directive(
            "version: 1\nfields:\n- name: id\n  type: int64\n",
            DriftPolicy::Fail,
            transform("rename: {id: customer_id}"),
            SchemaOverrides::none(),
        );
        let error = from_text_columns(&directive, names(&["id"]), vec![record(2, &[Some("1")])])
            .err()
            .expect("pre-transform pin drifts");

        assert_eq!(error.failure.code, "schema_drift");
        assert_eq!(
            error.failure.message,
            "schema drift against pinned schema customers.schema.yml: \
             missing fields: id; added fields: customer_id"
        );
        assert_eq!(
            error.schema_decision.expect("decision")["transform"],
            json!({ "rename": {"id": "customer_id"} })
        );
    }

    #[test]
    fn rejections_on_renamed_fields_carry_the_dataset_and_source_names() {
        // A rejection on a renamed field names the dataset field, points back
        // at the source field, and keeps the record under source names.
        let directive = SchemaDirective::Inferred {
            transform: transform("rename: {id: customer_id}"),
            overrides: overrides(&[("customer_id", Some("int64"), None)]),
        };
        let materialized = from_text_columns(
            &directive,
            names(&["id", "note"]),
            vec![
                record(2, &[Some("1"), Some("a")]),
                record(3, &[Some("n/a"), Some("b")]),
            ],
        )
        .expect("override misfits reject records, not the load");

        assert_eq!(materialized.rejected.len(), 1);
        let rejected = &materialized.rejected[0];
        assert_eq!(rejected.line, 3);
        assert_eq!(rejected.code, "type_coercion_failed");
        assert_eq!(rejected.field.as_deref(), Some("customer_id"));
        assert_eq!(rejected.source_field.as_deref(), Some("id"));
        assert_eq!(
            rejected.message,
            "value \"n/a\" does not fit overridden type int64 for field \"customer_id\""
        );
        assert_eq!(rejected.record, json!({ "id": "n/a", "note": "b" }));
    }

    #[test]
    fn rejections_on_unrenamed_fields_carry_no_source_field() {
        // Selection alone changes no names: a rejection on a selected but
        // unrenamed field leaves source_field unset.
        let directive = SchemaDirective::Inferred {
            transform: transform("select: [id, email]"),
            overrides: overrides(&[("email", None, Some(false))]),
        };
        let materialized = from_json_columns(
            &directive,
            names(&["id", "email"]),
            vec![
                json_record(1, r#"{"id": 1, "email": "a@example.com"}"#),
                json_record(2, r#"{"id": 2}"#),
            ],
        )
        .expect("required-field violations reject records, not the load");

        assert_eq!(materialized.rejected.len(), 1);
        let rejected = &materialized.rejected[0];
        assert_eq!(rejected.field.as_deref(), Some("email"));
        assert_eq!(rejected.source_field, None);
    }

    #[test]
    fn json_checks_and_columns_read_values_under_source_names() {
        // JSONL records only know source names: a required check on a renamed
        // field reads the source key, and the surviving column materializes
        // from it.
        let directive = SchemaDirective::Inferred {
            transform: transform("rename: {email: contact_email}"),
            overrides: overrides(&[("contact_email", None, Some(false))]),
        };
        let materialized = from_json_columns(
            &directive,
            names(&["email"]),
            vec![
                json_record(1, r#"{"email": "a@example.com"}"#),
                json_record(2, r#"{"email": null}"#),
            ],
        )
        .expect("required-field violations reject records, not the load");
        let batch = &materialized.batch;

        assert_eq!(batch_field_names(batch), ["contact_email"]);
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(strings(batch, 0).value(0), "a@example.com");
        assert!(!batch.schema().field(0).is_nullable());

        assert_eq!(materialized.rejected.len(), 1);
        let rejected = &materialized.rejected[0];
        assert_eq!(rejected.code, "missing_required_field");
        assert_eq!(rejected.field.as_deref(), Some("contact_email"));
        assert_eq!(rejected.source_field.as_deref(), Some("email"));
        assert_eq!(rejected.message, "required field \"contact_email\" is null");
    }

    #[test]
    fn pinned_rejections_on_renamed_fields_carry_the_dataset_and_source_names() {
        // The pin speaks dataset names; a pinned misfit on a renamed field
        // still points back at the source field it was read from.
        let directive = transformed_pinned_directive(
            "version: 1\nfields:\n- name: customer_id\n  type: int64\n",
            DriftPolicy::Fail,
            transform("rename: {id: customer_id}"),
            SchemaOverrides::none(),
        );
        let materialized = from_text_columns(
            &directive,
            names(&["id"]),
            vec![record(2, &[Some("1")]), record(3, &[Some("abc")])],
        )
        .expect("pinned misfits reject records, not the load");

        assert_eq!(materialized.rejected.len(), 1);
        let rejected = &materialized.rejected[0];
        assert_eq!(rejected.field.as_deref(), Some("customer_id"));
        assert_eq!(rejected.source_field.as_deref(), Some("id"));
        assert_eq!(
            rejected.message,
            "value \"abc\" does not fit pinned type int64 for field \"customer_id\""
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
