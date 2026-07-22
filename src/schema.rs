//! Schema deep module: the single home for the "type story" of a load.
//!
//! A column's type is decided once here — by inference over the observed
//! values, or by validating those observations against a pinned schema
//! ([`SchemaDirective`]) — and the same decision drives how records
//! materialize into Arrow [`RecordBatch`] chunks. The module splits along the
//! two source passes of ADR-0045: a streaming pass-1 observer per format
//! ([`TextObserver`], [`JsonObserver`]) folds one record at a time into
//! bounded state — the per-column observed-type lattice and the per-record
//! check outcomes — and resolves the whole-input schema decision at end of
//! input ([`Resolution`]); pass 2 then materializes fixed-size chunks of
//! surviving records against the resolved plan ([`ChunkPlan`]). CSV cells
//! arrive as text and JSONL cells arrive as typed [`Value`]s; both fold
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
//! Three field types are declared-only ([`FieldType`], ADR-0042): the two
//! timestamps split by offset discipline (ADR-0043) and the parameterized
//! decimal (ADR-0044) enter a schema only through overrides and the pins a
//! declared load bootstraps — never inference, whose lattice they extend
//! without touching. Their per-value fit is parse-based instead of
//! lattice-based, their rejections name the concrete cause, and a pinned
//! declared-type field without its re-declaring override fails the load as
//! `schema_drift`, because the load definition stays the declaration of
//! record.
//! A load definition may also declare a structural transform
//! ([`SchemaTransform`], ADR-0039, ADR-0041): the flatten mapping evaluates
//! first, against the observed source shape, adding one path-extracted
//! dataset field per declared source path after the observed fields; field
//! selection then evaluates against the post-flatten names and fixes the
//! dataset field order, and the rename mapping applies simultaneously over
//! the selected fields. The transform runs before everything above
//! (ADR-0040), so overrides, pins, drift, and per-record validation all
//! speak the transformed dataset names while rejections keep the original
//! source content — a transform naming an unobserved field (or a flatten
//! path whose first segment names none) fails the load as
//! `unknown_transform_field`, and a rename target colliding with another
//! dataset field or a flatten output shadowing an observed source field
//! fails it as `transform_name_collision`, both before any override or pin
//! comparison. Flatten extraction is total and never rejects a record: a
//! scalar leaf feeds the inference lattice, an object or array leaf
//! materializes as its compact JSON text, and a missing, null, or non-object
//! step anywhere on the path yields null (ADR-0041).
//! Everything type-related — the lattice, observation rules, the pinned schema
//! file contract ([`PinnedSchema`], ADR-0033), drift comparison, per-record
//! validation, materialization, and the `schema_decision` shape — is private
//! behind the observers, the resolution they finish into, and the chunk
//! builder that materializes against it.

use crate::connector::DestinationWriteFacts;
use crate::rejection::{self, RejectedRecord};
use crate::{ExecutionFailure, LoadFailure};
use arrow_array::builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, Decimal128Array, RecordBatch, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const PINNED_SCHEMA_VERSION: u64 = 1;

/// A whole-input materialization outcome, kept as the test-only convenience
/// over the streaming observer + chunk builder machinery: the typed Arrow
/// batch of the surviving records, the `schema_decision` shape the load
/// report echoes, the pinned schema file write the caller performs when the
/// load produces or extends a pin (ADR-0033), and the records per-record
/// validation rejected (ADR-0035). Production reads stream instead
/// (ADR-0045), so only tests materialize a whole input in one call.
#[cfg(test)]
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
#[derive(Clone, Copy)]
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
    field_type: Option<FieldType>,
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

/// The `transform` block of a load definition as written (ADR-0039,
/// ADR-0041): the source paths to flatten into added dataset fields, the
/// source fields to keep, in dataset order, and the source-to-dataset rename
/// mapping. Part of the versioned load-definition contract, so unknown keys
/// inside the block are rejected at parse time (ADR-0037).
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TransformConfig {
    flatten: Option<FlattenMap>,
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
        deserialize_ordered_unique_map(
            deserializer,
            "transform.rename",
            "a map of source field name to dataset field name",
        )
        .map(RenameMap)
    }
}

/// The `transform.flatten` mapping as written (ADR-0041): source path to
/// dataset field name, in declaration order. Deserialized like [`RenameMap`]
/// so a duplicate path key fails YAML parsing and the echo preserves the
/// declaration order.
#[derive(Debug)]
struct FlattenMap(Vec<(String, String)>);

impl<'de> Deserialize<'de> for FlattenMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserialize_ordered_unique_map(
            deserializer,
            "transform.flatten",
            "a map of source path to dataset field name",
        )
        .map(FlattenMap)
    }
}

impl Serialize for FlattenMap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serialize_ordered_map(&self.0, serializer)
    }
}

/// Deserializes a YAML map into its entries in declaration order, failing on
/// a duplicate key — serde's default map handling would silently keep the
/// last entry. `map_label` names the load-definition key in the error, e.g.
/// `transform.rename`; `expecting` describes the map in the caller's terms.
fn deserialize_ordered_unique_map<'de, D>(
    deserializer: D,
    map_label: &'static str,
    expecting: &'static str,
) -> Result<Vec<(String, String)>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct OrderedUniqueMapVisitor {
        map_label: &'static str,
        expecting: &'static str,
    }

    impl<'de> serde::de::Visitor<'de> for OrderedUniqueMapVisitor {
        type Value = Vec<(String, String)>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str(self.expecting)
        }

        fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::MapAccess<'de>,
        {
            let mut entries: Vec<(String, String)> =
                Vec::with_capacity(access.size_hint().unwrap_or(0));
            let mut seen_keys = HashSet::new();
            while let Some((key, value)) = access.next_entry::<String, String>()? {
                if !seen_keys.insert(key.clone()) {
                    return Err(serde::de::Error::custom(format!(
                        "duplicate {} key {key:?}",
                        self.map_label
                    )));
                }
                entries.push((key, value));
            }
            Ok(entries)
        }
    }

    deserializer.deserialize_map(OrderedUniqueMapVisitor {
        map_label,
        expecting,
    })
}

impl Serialize for RenameMap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serialize_ordered_map(&self.0, serializer)
    }
}

/// Serializes declaration-ordered map entries back as a map, so a definition
/// echo renders the mapping exactly as written.
fn serialize_ordered_map<S>(entries: &[(String, String)], serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.collect_map(entries.iter().map(|(key, value)| (key, value)))
}

/// The validated structural transform of a load definition (ADR-0039,
/// ADR-0041): the flatten mapping evaluates first, against the observed
/// source shape, adding one dataset field per declared source path — in
/// declaration order, after the observed fields; field selection evaluates
/// second, against the post-flatten names, with the select list order fixing
/// the dataset field order; the rename mapping evaluates last, applied
/// simultaneously over the selected (or, without `select`, full post-flatten)
/// field set, so swaps are legal and unmapped fields pass through under
/// their post-flatten names. Empty when the definition configures none, so
/// every [`SchemaDirective`] can carry one without an optional wrapper.
pub(crate) struct SchemaTransform {
    flatten: Vec<FlattenEntry>,
    select: Option<Vec<String>>,
    rename: Vec<(String, String)>,
}

/// One validated `transform.flatten` entry (ADR-0041): the declared source
/// path split into its dot-notation segments — at least two, all non-empty —
/// and the dataset field name its extracted values materialize under.
struct FlattenEntry {
    segments: Vec<String>,
    output: String,
}

impl FlattenEntry {
    /// The declared source path as the user wrote it. Segments never contain
    /// dots — a source key with a literal dot is unaddressable (ADR-0041) —
    /// so rejoining reproduces the declaration exactly.
    fn path(&self) -> String {
        self.segments.join(".")
    }
}

impl SchemaTransform {
    /// No transform configured.
    pub(crate) fn none() -> Self {
        SchemaTransform {
            flatten: Vec::new(),
            select: None,
            rename: Vec::new(),
        }
    }

    /// Validates the `transform` block of a load definition before any data
    /// is read (`invalid_transform_config`): the block must transform
    /// something, flatten paths must spell at least two non-empty segments
    /// onto usable, unique output names — on a JSONL source only, since CSV
    /// cells hold no addressable structure (ADR-0041) — select entries must
    /// be unique, and every rename must map an actual name change onto a
    /// usable, unique target drawn from the select list when one is declared
    /// — no implicit selection and no lenient no-ops (ADR-0039). Flatten
    /// outputs are ordinary fields to selection and renaming, so a declared
    /// select list must also list every output (no no-op extraction), and
    /// every config-determined dataset name — a select-resolved name with
    /// `select`, a rename target or flatten output's final name without —
    /// must stay unique here; without `select`, the pass-through field set
    /// is only known at read time and collisions with it surface there as
    /// `transform_name_collision`.
    pub(crate) fn from_config(
        config: &TransformConfig,
        source_format: &str,
    ) -> Result<Self, LoadFailure> {
        let invalid = |message: String| LoadFailure {
            code: "invalid_transform_config",
            message,
        };
        if config.flatten.is_none() && config.select.is_none() && config.rename.is_none() {
            return Err(invalid(
                "a transform block must set transform.flatten, transform.select, \
                 or transform.rename"
                    .to_string(),
            ));
        }
        let flatten = match &config.flatten {
            None => Vec::new(),
            Some(_) if source_format == "csv" => {
                return Err(invalid(
                    "transform.flatten requires a JSONL source format; \
                     the resolved source format is csv"
                        .to_string(),
                ))
            }
            Some(flatten) if flatten.0.is_empty() => {
                return Err(invalid(
                    "transform.flatten must map at least one path".to_string(),
                ))
            }
            Some(flatten) => {
                let mut entries = Vec::with_capacity(flatten.0.len());
                let mut seen_outputs = HashSet::new();
                for (path, output) in &flatten.0 {
                    let segments: Vec<String> = path.split('.').map(str::to_string).collect();
                    if segments.len() < 2 {
                        return Err(invalid(format!(
                            "transform.flatten path {path:?} must have at least two \
                             dot-separated segments"
                        )));
                    }
                    if segments.iter().any(|segment| segment.is_empty()) {
                        return Err(invalid(format!(
                            "transform.flatten path {path:?} must not contain empty segments"
                        )));
                    }
                    if output.trim().is_empty() {
                        return Err(invalid(format!(
                            "transform.flatten output name for path {path:?} must not be empty"
                        )));
                    }
                    if !seen_outputs.insert(output.as_str()) {
                        return Err(invalid(format!(
                            "transform.flatten maps more than one path to {output:?}"
                        )));
                    }
                    entries.push(FlattenEntry {
                        segments,
                        output: output.clone(),
                    });
                }
                entries
            }
        };
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
        // The final dataset name a post-flatten field materializes under: its
        // rename target, or the field name passed through.
        fn final_dataset_name<'a>(rename: &'a [(String, String)], name: &'a str) -> &'a str {
            rename
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, target)| target.as_str())
                .unwrap_or(name)
        }
        if let Some(select) = &config.select {
            // No no-op extraction: with a declared select list, a flatten
            // output outside it would be extracted and dropped, matching the
            // rename rule above (ADR-0041).
            for entry in &flatten {
                if !select.contains(&entry.output) {
                    return Err(invalid(format!(
                        "transform.flatten output {:?} is not in transform.select",
                        entry.output
                    )));
                }
            }
            let mut seen_dataset_names = HashSet::new();
            for source in select {
                let dataset_name = final_dataset_name(&rename, source);
                if !seen_dataset_names.insert(dataset_name) {
                    return Err(invalid(format!(
                        "transform.select and transform.rename map more than one field \
                         to the dataset name {dataset_name:?}"
                    )));
                }
            }
        } else if !flatten.is_empty() {
            // Without `select`, the config-determined dataset names are the
            // rename targets and the flatten outputs' final names; a rename
            // whose key is a flatten output renames that output, so its
            // target is the output's final name, not a separate one.
            let flatten_outputs: HashSet<&str> =
                flatten.iter().map(|entry| entry.output.as_str()).collect();
            let mut seen_config_names: HashSet<&str> = rename
                .iter()
                .filter(|(source, _)| !flatten_outputs.contains(source.as_str()))
                .map(|(_, target)| target.as_str())
                .collect();
            for entry in &flatten {
                let dataset_name = final_dataset_name(&rename, &entry.output);
                if !seen_config_names.insert(dataset_name) {
                    return Err(invalid(format!(
                        "transform.flatten and transform.rename map more than one field \
                         to the dataset name {dataset_name:?}"
                    )));
                }
            }
        }
        Ok(SchemaTransform {
            flatten,
            select: config.select.clone(),
            rename,
        })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.flatten.is_empty() && self.select.is_none() && self.rename.is_empty()
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
        if !self.flatten.is_empty() {
            entry.insert(
                "flatten".to_string(),
                Value::Object(
                    self.flatten
                        .iter()
                        .map(|flatten_entry| (flatten_entry.path(), json!(flatten_entry.output)))
                        .collect(),
                ),
            );
        }
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

/// A dataset field type: the vocabulary schema overrides, pinned schema
/// files, and schema decisions name, and the type a materialized column is
/// built as. The four inference-reachable types mirror the observation
/// lattice one-to-one; the declared-only types (ADR-0042) — the two
/// timestamps split by offset discipline (ADR-0043) and the parameterized
/// decimal (ADR-0044) — enter a schema only through explicit declaration,
/// never inference, so they extend this vocabulary while [`InferredType`]
/// and its merge lattice stay untouched.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum FieldType {
    Boolean,
    Int64,
    Float64,
    Utf8,
    Timestamp,
    Timestamptz,
    Decimal { precision: u8, scale: u8 },
}

impl FieldType {
    fn data_type(self) -> DataType {
        match self {
            FieldType::Boolean => DataType::Boolean,
            FieldType::Int64 => DataType::Int64,
            FieldType::Float64 => DataType::Float64,
            FieldType::Utf8 => DataType::Utf8,
            FieldType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            FieldType::Timestamptz => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            FieldType::Decimal { precision, scale } => DataType::Decimal128(precision, scale as i8),
        }
    }

    /// The stable name this type carries in schema decisions, pinned schema
    /// files, and failure messages: byte-identical to the accepted
    /// declaration, so a report or pin file prints a declared type exactly as
    /// it was written (ADR-0042).
    fn name(self) -> String {
        match self {
            FieldType::Boolean => "boolean".to_string(),
            FieldType::Int64 => "int64".to_string(),
            FieldType::Float64 => "float64".to_string(),
            FieldType::Utf8 => "utf8".to_string(),
            FieldType::Timestamp => "timestamp".to_string(),
            FieldType::Timestamptz => "timestamptz".to_string(),
            FieldType::Decimal { precision, scale } => format!("decimal({precision},{scale})"),
        }
    }

    /// The observation-lattice type this field type corresponds to, for the
    /// four inference-reachable types. The declared-only types have no
    /// lattice image — no widening involves them in either direction
    /// (ADR-0042) — so their value-level fit is parse-based instead.
    fn lattice_type(self) -> Option<InferredType> {
        match self {
            FieldType::Boolean => Some(InferredType::Boolean),
            FieldType::Int64 => Some(InferredType::Int64),
            FieldType::Float64 => Some(InferredType::Float64),
            FieldType::Utf8 => Some(InferredType::Utf8),
            FieldType::Timestamp | FieldType::Timestamptz | FieldType::Decimal { .. } => None,
        }
    }
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
    field_type: FieldType,
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
                        field_type: field.field_type.name(),
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

fn parse_type_name(name: &str) -> Option<FieldType> {
    match name {
        "boolean" => Some(FieldType::Boolean),
        "int64" => Some(FieldType::Int64),
        "float64" => Some(FieldType::Float64),
        "utf8" => Some(FieldType::Utf8),
        "timestamp" => Some(FieldType::Timestamp),
        "timestamptz" => Some(FieldType::Timestamptz),
        _ => parse_decimal_type_name(name),
    }
}

/// Parses the canonical `decimal(p,s)` spelling — both parameters mandatory,
/// no spaces, no signs, no leading zeros — with `1 <= p <= 38` and
/// `0 <= s <= p` (ADR-0044). Only the canonical spelling is accepted so the
/// name a report or pin file prints back is byte-identical to the accepted
/// declaration.
fn parse_decimal_type_name(name: &str) -> Option<FieldType> {
    let parameters = name.strip_prefix("decimal(")?.strip_suffix(')')?;
    let (precision_text, scale_text) = parameters.split_once(',')?;
    let precision = parse_canonical_u8(precision_text)?;
    let scale = parse_canonical_u8(scale_text)?;
    if !(1..=38).contains(&precision) || scale > precision {
        return None;
    }
    Some(FieldType::Decimal { precision, scale })
}

/// Parses a `decimal(p,s)` parameter written canonically: ASCII digits only,
/// with no leading zero unless the parameter is `0` itself.
fn parse_canonical_u8(text: &str) -> Option<u8> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    if text.len() > 1 && text.starts_with('0') {
        return None;
    }
    text.parse().ok()
}

/// The resolved outcome of pass 1 (ADR-0045): the fixed plan pass 2
/// materializes chunks against, the per-record checks both passes agree on,
/// the `schema_decision` the load report echoes, and the pinned schema file
/// write the caller performs when the load produces or extends a pin. The
/// plan is final — inference, transform resolution, overrides, pin
/// comparison, and drift are all decided against the whole input before any
/// chunk is built.
pub(crate) struct Resolution {
    pub(crate) plan: ChunkPlan,
    pub(crate) checks: Vec<FieldCheck>,
    pub(crate) decision: Value,
    pub(crate) pinned_schema_write: Option<PinnedSchemaWrite>,
}

impl Resolution {
    /// Renders the rejection a JSONL record spilled during pass 1 carries in
    /// the artifact, now that the resolved checks fix which violated check
    /// names it (ADR-0045). `None` only if the record no longer violates any
    /// check, which pass-1/pass-2 check agreement rules out.
    pub(crate) fn rejection_for(&self, record: &JsonRecord) -> Option<RejectedRecord> {
        validate_json_record(record, &self.checks)
    }
}

/// The streaming pass-1 observer for CSV records: folds each record into the
/// whole-input and surviving-records type lattices and, when the schema
/// verdict is already known to be healthy from the header, judges the record
/// against the per-record checks (ADR-0045). CSV shape verdicts — transform
/// resolution, override names, pin conflicts, drift — depend only on the
/// header, so a pending verdict skips per-record validation exactly like
/// today's whole-input path, and [`TextObserver::finish`] re-derives the
/// failure with the whole-input observations its message needs.
pub(crate) struct TextObserver<'a> {
    directive: &'a SchemaDirective,
    field_names: &'a [String],
    checks: Option<Vec<FieldCheck>>,
    all_types: Vec<InferredType>,
    survivor_types: Vec<InferredType>,
}

impl<'a> TextObserver<'a> {
    pub(crate) fn new(directive: &'a SchemaDirective, field_names: &'a [String]) -> Self {
        // Probe the verdict with placeholder observations: only the checks
        // matter here, and they do not depend on observed types. A failing
        // probe leaves the observer in observation-only mode, and finish()
        // re-derives the same failure with the real observations.
        let placeholder = vec![InferredType::Null; field_names.len()];
        let checks = resolve_plan(directive, field_names, &placeholder, &placeholder)
            .ok()
            .map(|resolution| resolution.checks);
        TextObserver {
            directive,
            field_names,
            checks,
            all_types: vec![InferredType::Null; field_names.len()],
            survivor_types: vec![InferredType::Null; field_names.len()],
        }
    }

    /// Folds one record into the observer: the whole-input lattice always
    /// advances, and under a healthy verdict the record is judged against the
    /// per-record checks — returning its rejection, or feeding the
    /// surviving-records lattice that shapes inference and added fields.
    /// Under a pending verdict every record returns `None` unjudged, matching
    /// the whole-input path, where a shape failure precedes validation.
    pub(crate) fn observe(&mut self, record: &TextRecord) -> Option<RejectedRecord> {
        merge_text_observations(&mut self.all_types, record);
        let checks = self.checks.as_ref()?;
        if let Some(rejection) = validate_text_record(record, checks, self.field_names) {
            return Some(rejection);
        }
        merge_text_observations(&mut self.survivor_types, record);
        None
    }

    /// Resolves the whole-input schema decision: the fixed plan and checks on
    /// success, or the schema failure — re-derived with the whole-input
    /// observations, so a declared-type drift message names the real
    /// effective types.
    pub(crate) fn finish(self) -> Result<Resolution, ExecutionFailure> {
        resolve_plan(
            self.directive,
            self.field_names,
            &self.all_types,
            &self.survivor_types,
        )
    }
}

fn merge_text_observations(types: &mut [InferredType], record: &TextRecord) {
    for (column_index, cell) in record.cells.iter().enumerate() {
        if let Some(value) = cell {
            types[column_index] = types[column_index].merge(infer_text_type(value));
        }
    }
}

/// The streaming pass-1 observer for JSONL records. The JSONL dataset shape —
/// the union of keys across all records — is only known at end of input, so
/// unlike CSV no shape verdict exists while streaming: every record is judged
/// against the directive-derived checks (the pinned fields and the overrides
/// outside the pin, whose set equals the resolved checks whenever the verdict
/// passes), and the caller spills rejected records until
/// [`JsonObserver::finish`] delivers the verdict (ADR-0045). Outcomes are
/// judged as "violates any check", which is order-free, so the unresolved
/// dataset column order cannot change them.
pub(crate) struct JsonObserver<'a> {
    directive: &'a SchemaDirective,
    checks: Vec<FieldCheck>,
    all_types: HashMap<String, InferredType>,
    survivor_types: HashMap<String, InferredType>,
    flatten_all: Vec<InferredType>,
    flatten_survivors: Vec<InferredType>,
}

/// The pass-1 outcome of one JSONL record: survived, or rejected by a
/// per-record check — with the rejection itself deferred to the resolved
/// checks, since which violated check names it depends on the dataset column
/// order only end of input fixes.
pub(crate) enum JsonOutcome {
    Survived,
    Rejected,
}

impl<'a> JsonObserver<'a> {
    pub(crate) fn new(directive: &'a SchemaDirective) -> Self {
        let flatten_count = directive.transform().flatten.len();
        JsonObserver {
            directive,
            checks: directive_checks(directive),
            all_types: HashMap::new(),
            survivor_types: HashMap::new(),
            flatten_all: vec![InferredType::Null; flatten_count],
            flatten_survivors: vec![InferredType::Null; flatten_count],
        }
    }

    /// Folds one record into the observer: the whole-input lattices always
    /// advance, and the record is judged against the directive-derived
    /// checks. A surviving record also feeds the surviving-records lattices
    /// that shape inference and added fields.
    pub(crate) fn observe(&mut self, record: &JsonRecord) -> JsonOutcome {
        merge_json_observations(
            &mut self.all_types,
            &mut self.flatten_all,
            self.directive,
            record,
        );
        if json_record_violates(record, &self.checks) {
            return JsonOutcome::Rejected;
        }
        merge_json_observations(
            &mut self.survivor_types,
            &mut self.flatten_survivors,
            self.directive,
            record,
        );
        JsonOutcome::Survived
    }

    /// Resolves the whole-input schema decision against the observed key
    /// union, exactly like the whole-input path: transform resolution,
    /// override names, pin conflicts, shape drift, and declared-type checks
    /// all judge the union `field_names` spells, so a batch-wide-absent
    /// pinned field is missing-field drift.
    pub(crate) fn finish(self, field_names: &[String]) -> Result<Resolution, ExecutionFailure> {
        let width = field_names.len() + self.flatten_all.len();
        let mut all_types = vec![InferredType::Null; width];
        let mut survivor_types = vec![InferredType::Null; width];
        for (column_index, field_name) in field_names.iter().enumerate() {
            if let Some(observed) = self.all_types.get(field_name) {
                all_types[column_index] = *observed;
            }
            if let Some(observed) = self.survivor_types.get(field_name) {
                survivor_types[column_index] = *observed;
            }
        }
        for (position, observed) in self.flatten_all.iter().enumerate() {
            all_types[field_names.len() + position] = *observed;
        }
        for (position, observed) in self.flatten_survivors.iter().enumerate() {
            survivor_types[field_names.len() + position] = *observed;
        }
        resolve_plan(self.directive, field_names, &all_types, &survivor_types)
    }
}

fn merge_json_observations(
    types: &mut HashMap<String, InferredType>,
    flatten_types: &mut [InferredType],
    directive: &SchemaDirective,
    record: &JsonRecord,
) {
    for (field_name, value) in &record.object {
        let observed = types
            .entry(field_name.clone())
            .or_insert(InferredType::Null);
        *observed = observed.merge(infer_json_type(value));
    }
    for (position, entry) in directive.transform().flatten.iter().enumerate() {
        if let Some(value) = json_path_value(&entry.segments, &record.object) {
            flatten_types[position] = flatten_types[position].merge(infer_json_type(value));
        }
    }
}

/// The per-record checks derivable from the directive alone, before the JSONL
/// key union is known: every pinned field (a pin field missing from the union
/// is missing-field drift, so on any load that validates records the pin is
/// fully matched) plus every override outside the pin (an override naming no
/// dataset field is `unknown_override_field`, another verdict that precedes
/// validation). Whenever the end-of-input verdict passes, this set equals the
/// resolved checks, so pass-1 outcomes agree with the resolved plan; when the
/// verdict fails, the outcomes are discarded with the spill.
fn directive_checks(directive: &SchemaDirective) -> Vec<FieldCheck> {
    let transform = directive.transform();
    let overrides = directive.overrides();
    let mut checks = Vec::new();
    let mut pin_named: HashSet<&str> = HashSet::new();
    if let SchemaDirective::Pinned { pin, .. } = directive {
        for pin_field in &pin.fields {
            pin_named.insert(pin_field.name.as_str());
            checks.push(FieldCheck {
                name: pin_field.name.clone(),
                source: json_source_address(transform, &pin_field.name),
                observed_index: 0,
                expected_type: Some((pin_field.field_type, TypeOrigin::Pinned)),
                required: !pin_field.nullable,
            });
        }
    }
    for override_ in &overrides.overrides {
        if pin_named.contains(override_.name.as_str()) {
            continue;
        }
        checks.push(FieldCheck {
            name: override_.name.clone(),
            source: json_source_address(transform, &override_.name),
            observed_index: 0,
            expected_type: override_
                .field_type
                .map(|field_type| (field_type, TypeOrigin::Overridden)),
            required: override_.nullable == Some(false),
        });
    }
    checks
}

/// The source address a dataset field name reads from in a JSONL record,
/// derived from the transform configuration alone: the declared path when the
/// name is a flatten output's dataset name (ADR-0041), the rename key when a
/// rename mapping produced the name (ADR-0039), and the name itself
/// otherwise. Rename targets and flatten outputs are config-unique, so the
/// reverse lookup is unambiguous.
fn json_source_address(transform: &SchemaTransform, dataset_name: &str) -> SourceAddress {
    if let Some(entry) = transform
        .flatten
        .iter()
        .find(|entry| transform.dataset_name(&entry.output) == dataset_name)
    {
        return SourceAddress::Path(entry.segments.clone());
    }
    if let Some((source_name, _)) = transform
        .rename
        .iter()
        .find(|(_, target)| target == dataset_name)
    {
        return SourceAddress::Field(source_name.clone());
    }
    SourceAddress::Field(dataset_name.to_string())
}

/// Whether a JSONL record violates any of the checks: the order-free
/// restriction of [`validate_json_record`], used while the check order is
/// still unresolved during pass 1 — and by pass 2, whose outcomes must be
/// the same pure function of (record, checks). A record violates iff
/// `validate` would reject it, whatever the check order.
pub(crate) fn json_record_violates(record: &JsonRecord, checks: &[FieldCheck]) -> bool {
    checks
        .iter()
        .any(|check| match check.source.json_value(&record.object) {
            None | Some(Value::Null) => check.required,
            Some(value) => check
                .expected_type
                .is_some_and(|(expected_type, _)| !json_value_fits(value, expected_type)),
        })
}

/// Resolves the whole-input schema decision shared by both formats: the
/// transform against the observed shape, override names, and — under a pin —
/// conflicts, shape drift, and declared-type re-declarations, in exactly the
/// whole-input order, then plans every field's materialized type from the
/// whole-input observations. `all_types` spans the observed source shape
/// plus the flatten columns and feeds only failure messages; added fields
/// and inference take their types from `survivor_types`, so a rejected
/// record's values never shape the destination.
fn resolve_plan(
    directive: &SchemaDirective,
    field_names: &[String],
    all_types: &[InferredType],
    survivor_types: &[InferredType],
) -> Result<Resolution, ExecutionFailure> {
    stamped_resolution(directive, || {
        let columns = resolve_transform(directive, field_names)?;
        check_override_names(directive, &columns)?;
        match directive {
            SchemaDirective::Inferred { overrides, .. } => Ok(inferred_resolution(
                &columns,
                survivor_types,
                None,
                overrides,
            )),
            SchemaDirective::PinInferred {
                pinned_path,
                overrides,
                ..
            } => Ok(inferred_resolution(
                &columns,
                survivor_types,
                Some(pinned_path),
                overrides,
            )),
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
                check_declared_type_overrides(pinned_path, pin, &matched, overrides, || {
                    all_types.to_vec()
                })?;
                let mut checks = pinned_checks(&matched);
                checks.extend(override_checks(overrides, added.iter()));
                // Added fields take their types from the surviving records
                // only, so a rejected record's values never shape the
                // destination.
                let added = planned_added_fields(added, survivor_types, overrides);
                let plan = assemble_pinned_plan(pinned_path, matched, added);
                Ok(Resolution {
                    checks,
                    plan: ChunkPlan::new(plan.fields),
                    decision: plan.decision,
                    pinned_schema_write: plan.pinned_schema_write,
                })
            }
        }
    })
}

/// Assembles the resolution of an inference-driven or pin-bootstrapping load:
/// overridden fields validate per record like pinned fields (ADR-0038), and
/// the surviving records alone shape every property no override sets.
fn inferred_resolution(
    columns: &[DatasetColumn],
    survivor_types: &[InferredType],
    pinned_path: Option<&str>,
    overrides: &SchemaOverrides,
) -> Resolution {
    let checks = override_checks(overrides, columns.iter());
    let plan = inferred_plan(columns, survivor_types, pinned_path, overrides);
    Resolution {
        checks,
        plan: ChunkPlan::new(plan.fields),
        decision: plan.decision,
        pinned_schema_write: plan.pinned_schema_write,
    }
}

/// Adds the directive echoes to whichever schema decision a resolution
/// produced — the one place every decision passes through, so success and
/// failure decisions alike carry the configured transform and overrides.
fn stamped_resolution(
    directive: &SchemaDirective,
    resolve: impl FnOnce() -> Result<Resolution, ExecutionFailure>,
) -> Result<Resolution, ExecutionFailure> {
    match resolve() {
        Ok(mut resolution) => {
            resolution.decision = directive.stamp(resolution.decision);
            Ok(resolution)
        }
        Err(mut failure) => {
            if let Some(decision) = failure.schema_decision.take() {
                failure.schema_decision = Some(Box::new(directive.stamp(*decision)));
            }
            Err(failure)
        }
    }
}

/// One field of the dataset a load materializes, produced by resolving the
/// structural transform against the observed source shape: the dataset name
/// the field writes under, the source address it reads from, and its column
/// index into the observed types — a virtual index past the observed fields
/// for a flatten column (ADR-0041). Without a transform the dataset view is
/// the observed source shape itself.
#[derive(Clone)]
struct DatasetColumn {
    dataset_name: String,
    source: SourceAddress,
    observed_index: usize,
}

impl DatasetColumn {
    /// Whether the transform changed this column's name: a renamed source
    /// field, or a flatten column, whose dataset name never spells its
    /// source path.
    fn name_changed(&self) -> bool {
        match &self.source {
            SourceAddress::Field(source_name) => &self.dataset_name != source_name,
            SourceAddress::Path(_) => true,
        }
    }
}

/// How a dataset column reads its value from a source record: a top-level
/// source field — by observed column index for CSV cells and by source name
/// for JSONL objects — or a flatten path into a top-level field's nested
/// JSON structure (ADR-0041). Paths reach only JSONL loads: a flatten
/// declaration on a CSV source is rejected at config time.
#[derive(Clone)]
enum SourceAddress {
    Field(String),
    Path(Vec<String>),
}

impl SourceAddress {
    /// The source name reports and rejections carry: the top-level field
    /// name, or the declared source path as the user wrote it.
    fn as_written(&self) -> String {
        match self {
            SourceAddress::Field(name) => name.clone(),
            SourceAddress::Path(segments) => segments.join("."),
        }
    }

    /// Reads the addressed value from a JSONL record: a top-level lookup, or
    /// the total path walk of ADR-0041 — `None` (an Arrow null downstream)
    /// for a missing key or a null or non-object value anywhere before the
    /// leaf, and the leaf value itself otherwise, so an object or array leaf
    /// stays a [`Value`] that materializes as its compact JSON text.
    fn json_value<'a>(&self, object: &'a serde_json::Map<String, Value>) -> Option<&'a Value> {
        match self {
            SourceAddress::Field(name) => object.get(name),
            SourceAddress::Path(segments) => json_path_value(segments, object),
        }
    }
}

/// The total flatten-path walk of ADR-0041 over a record's object, shared by
/// the path-addressed [`SourceAddress`] and the streaming flatten observation.
fn json_path_value<'a>(
    segments: &[String],
    object: &'a serde_json::Map<String, Value>,
) -> Option<&'a Value> {
    let mut value = object.get(&segments[0])?;
    for segment in &segments[1..] {
        value = value.as_object()?.get(segment)?;
    }
    Some(value)
}

/// Resolves the directive's structural transform against the observed source
/// names into the dataset columns everything downstream operates on
/// (ADR-0040). The flatten mapping resolves first (ADR-0041): each entry
/// becomes one path-addressed column — placed by the select list when one is
/// declared, appended after the observed fields in declaration order
/// otherwise — whose observed-type slot is a virtual index past the observed
/// fields. Fails with `unknown_transform_field` when a select entry or
/// rename key names no post-flatten field, or a flatten path's first segment
/// names no observed field — reported as the user wrote them — and with
/// `transform_name_collision` when a flatten output shadows an observed
/// source field (even one selection would drop, or the post-flatten names
/// select and rename resolve against would turn ambiguous) or a rename
/// target collides with a pass-through field name, which is reachable only
/// without `select`: with one, the dataset shape is config-determined and
/// collisions were already rejected at directive resolution. Duplicate
/// observed names resolve to their first occurrence; purely pass-through
/// duplicates keep their pre-transform meaning (drift under a pin) rather
/// than becoming a transform failure.
fn resolve_transform(
    directive: &SchemaDirective,
    observed_names: &[String],
) -> Result<Vec<DatasetColumn>, ExecutionFailure> {
    let transform = directive.transform();
    let mut observed_indexes: HashMap<&str, usize> = HashMap::with_capacity(observed_names.len());
    for (index, name) in observed_names.iter().enumerate() {
        observed_indexes.entry(name.as_str()).or_insert(index);
    }
    let flatten_positions: HashMap<&str, usize> = transform
        .flatten
        .iter()
        .enumerate()
        .map(|(position, entry)| (entry.output.as_str(), position))
        .collect();

    // Select entries and rename keys naming flatten outputs are known
    // fields; a flatten path is unknown when its first segment names no
    // observed field, whatever deeper segments spell — a deeper absence is a
    // null value, never a failure (ADR-0041).
    let mut unknown: Vec<String> = Vec::new();
    for name in transform
        .select
        .iter()
        .flatten()
        .chain(transform.rename.iter().map(|(source, _)| source))
    {
        if !observed_indexes.contains_key(name.as_str())
            && !flatten_positions.contains_key(name.as_str())
            && !unknown.contains(name)
        {
            unknown.push(name.clone());
        }
    }
    for entry in &transform.flatten {
        let path = entry.path();
        if !observed_indexes.contains_key(entry.segments[0].as_str()) && !unknown.contains(&path) {
            unknown.push(path);
        }
    }
    if !unknown.is_empty() {
        return Err(pre_materialization_failure(
            LoadFailure {
                code: "unknown_transform_field",
                message: format!(
                    "transform selects, renames, or flattens fields absent from the observed source shape: {}",
                    unknown.join(", ")
                ),
            },
            configured_posture_decision(directive),
        ));
    }

    for entry in &transform.flatten {
        if observed_indexes.contains_key(entry.output.as_str()) {
            return Err(pre_materialization_failure(
                LoadFailure {
                    code: "transform_name_collision",
                    message: format!(
                        "transform flatten collides on dataset field {:?}: source path {} shadows an observed source field",
                        entry.output,
                        entry.path()
                    ),
                },
                configured_posture_decision(directive),
            ));
        }
    }

    let flatten_column = |position: usize| {
        let entry = &transform.flatten[position];
        DatasetColumn {
            dataset_name: transform.dataset_name(&entry.output),
            source: SourceAddress::Path(entry.segments.clone()),
            observed_index: observed_names.len() + position,
        }
    };
    let columns = match &transform.select {
        Some(select) => select
            .iter()
            .map(
                |source_name| match flatten_positions.get(source_name.as_str()) {
                    Some(&position) => flatten_column(position),
                    None => DatasetColumn {
                        dataset_name: transform.dataset_name(source_name),
                        source: SourceAddress::Field(source_name.clone()),
                        observed_index: observed_indexes[source_name.as_str()],
                    },
                },
            )
            .collect::<Vec<_>>(),
        None => observed_names
            .iter()
            .enumerate()
            .map(|(observed_index, source_name)| DatasetColumn {
                dataset_name: transform.dataset_name(source_name),
                source: SourceAddress::Field(source_name.clone()),
                observed_index,
            })
            .chain((0..transform.flatten.len()).map(flatten_column))
            .collect(),
    };

    // A dataset name produced by more than one column is a collision only
    // when the transform changed a name to put it there; identity renames
    // are config-invalid and a flatten column shadowing an observed field
    // was rejected above, so the remaining reachable case is a name renamed
    // onto a pass-through field. Columns are grouped once by dataset name,
    // then reported at the first colliding column in dataset order, with its
    // sources in dataset order too.
    let mut columns_by_dataset_name: HashMap<&str, Vec<&DatasetColumn>> = HashMap::new();
    for column in &columns {
        columns_by_dataset_name
            .entry(column.dataset_name.as_str())
            .or_default()
            .push(column);
    }
    for column in &columns {
        let colliding = &columns_by_dataset_name[column.dataset_name.as_str()];
        if colliding.len() > 1 && colliding.iter().any(|other| other.name_changed()) {
            let sources = colliding
                .iter()
                .map(|other| other.source.as_written())
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
        rejected_count: 0,
        committed_execution: None,
        destination_write: Box::new(DestinationWriteFacts::not_applicable()),
    }
}

/// One field the load will materialize: its dataset name, the source address
/// it reads from ([`SourceAddress`]), the type its column is built as (never
/// `Null`), and whether its values may be null. The dataset name differs
/// from the source when a rename mapping changed the field's name (ADR-0039)
/// or the field is a flatten output (ADR-0041).
struct PlannedField {
    name: String,
    source: SourceAddress,
    materialized_type: FieldType,
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
                source: column.source.clone(),
                materialized_type: override_
                    .and_then(|override_| override_.field_type)
                    .unwrap_or_else(|| observed_types[column.observed_index].field_type()),
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
                source: column.source.clone(),
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
/// guards, the source address it reads ([`SourceAddress`]; CSV cells read by
/// observed column index, meaningless for JSONL checks), the type its values
/// must widen to — if any — with the wording its rejections carry, and
/// whether a null value rejects the record. Pinned fields check everything
/// the pin declares; overridden fields check exactly the properties their
/// override sets. Check outcomes are a pure function of (record, checks), so
/// the two source passes of ADR-0045 agree on them.
pub(crate) struct FieldCheck {
    name: String,
    source: SourceAddress,
    observed_index: usize,
    expected_type: Option<(FieldType, TypeOrigin)>,
    required: bool,
}

impl FieldCheck {
    /// The source a rejection names alongside the dataset field: the source
    /// field name when a rename mapping changed the rejected field's name
    /// (ADR-0039), or the declared source path as written when the rejected
    /// field is a flatten output (ADR-0041).
    fn source_field(&self) -> Option<String> {
        match &self.source {
            SourceAddress::Field(source_name) => {
                (&self.name != source_name).then(|| source_name.clone())
            }
            SourceAddress::Path(_) => Some(self.source.as_written()),
        }
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
            source: planned.source.clone(),
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
                    source: column.source.clone(),
                    observed_index: column.observed_index,
                    expected_type: override_
                        .field_type
                        .map(|field_type| (field_type, TypeOrigin::Overridden)),
                    required: override_.nullable == Some(false),
                })
        })
        .collect()
}

/// Validates one CSV record against the field checks, in check order: the
/// first null cell in a required field or the first cell whose observed type
/// does not widen to its expected type rejects the record (ADR-0035). The
/// rejection names the dataset field while `record` keeps the original
/// source content under source names (ADR-0039).
pub(crate) fn validate_text_record(
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
                    if !text_value_fits(value, expected_type) {
                        return Some(type_rejection(
                            record.line,
                            check,
                            expected_type,
                            origin,
                            json!(value),
                            text_misfit_cause(value, expected_type),
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
/// [`validate_text_record`]. Values are read through their source addresses
/// — a top-level name, or a flatten path walk (ADR-0041) — and an absent
/// address reads as null.
fn validate_json_record(record: &JsonRecord, checks: &[FieldCheck]) -> Option<RejectedRecord> {
    for check in checks {
        match check.source.json_value(&record.object) {
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
                    if !json_value_fits(value, expected_type) {
                        return Some(type_rejection(
                            record.line,
                            check,
                            expected_type,
                            origin,
                            value.clone(),
                            json_misfit_cause(value, expected_type),
                            Value::Object(record.object.clone()),
                        ));
                    }
                }
            }
        }
    }
    None
}

/// A value fits a pinned or overridden lattice-typed field iff its observed
/// type widens to the expected type under the inference lattice — the
/// per-cell restriction of the ADR-0034 column rule (ADR-0035, ADR-0038).
/// The declared-only types have no lattice image, so no observation ever
/// fits them here — their fit is parse-based on the value itself
/// ([`text_value_fits`], [`json_value_fits`], ADR-0042). Building a
/// surviving record's cell with its expected type can then never fail per
/// value.
fn fits_expected_type(observed: InferredType, expected: FieldType) -> bool {
    match expected.lattice_type() {
        Some(lattice_expected) => observed.merge(lattice_expected) == lattice_expected,
        None => false,
    }
}

/// Whether a CSV cell fits an expected type: lattice-typed fields by
/// observation widening, declared-type fields by parsing the text itself
/// under the strict menus (ADR-0043, ADR-0044).
fn text_value_fits(value: &str, expected: FieldType) -> bool {
    match expected {
        FieldType::Timestamp => parse_timestamp_micros(value).is_some(),
        FieldType::Timestamptz => parse_timestamptz_micros(value).is_some(),
        FieldType::Decimal { precision, scale } => {
            parse_decimal_scaled(value, precision, scale).is_some()
        }
        _ => fits_expected_type(infer_text_type(value), expected),
    }
}

/// Whether a JSONL value fits an expected type. Only JSON strings can spell
/// a timestamp — numbers would be epoch guessing (ADR-0043) — and a decimal
/// takes strings and integers, whose exact digits survive, but never floats,
/// whose digits were already lost to IEEE parsing (ADR-0044).
fn json_value_fits(value: &Value, expected: FieldType) -> bool {
    match expected {
        FieldType::Timestamp => {
            matches!(value, Value::String(text) if parse_timestamp_micros(text).is_some())
        }
        FieldType::Timestamptz => {
            matches!(value, Value::String(text) if parse_timestamptz_micros(text).is_some())
        }
        FieldType::Decimal { precision, scale } => match value {
            Value::String(text) => parse_decimal_scaled(text, precision, scale).is_some(),
            Value::Number(number) => json_integer(number).is_some_and(|integer| {
                rescale_decimal_integer(integer, precision, scale).is_some()
            }),
            _ => false,
        },
        _ => fits_expected_type(infer_json_type(value), expected),
    }
}

/// The exact integer a JSON number spells, when it spells one: floats return
/// `None` — their exact digits were already lost to IEEE parsing before this
/// load saw them (ADR-0044).
fn json_integer(number: &serde_json::Number) -> Option<i128> {
    if let Some(value) = number.as_i64() {
        Some(i128::from(value))
    } else {
        number.as_u64().map(i128::from)
    }
}

/// Names the concrete cause a CSV cell missed a declared type, appended to
/// its `type_coercion_failed` message (ADR-0043, ADR-0044). Lattice-typed
/// misfits keep their established wording and carry no cause. Both timestamp
/// arms probe the clock prefix first, so offset-discipline causes name the
/// offset even when it is malformed rather than falling to the generic menu
/// wording.
fn text_misfit_cause(value: &str, expected: FieldType) -> Option<String> {
    match expected {
        FieldType::Timestamp => Some(match parse_datetime_prefix(value) {
            Some((_, offset_text)) if !offset_text.is_empty() => {
                if parse_offset_seconds(offset_text).is_some() {
                    "the text carries a UTC offset, which wall-clock timestamp text must not"
                        .to_string()
                } else {
                    "the text continues after the clock reading, which wall-clock timestamp \
                     text must not"
                        .to_string()
                }
            }
            _ => clock_misfit_cause(value),
        }),
        FieldType::Timestamptz => Some(match parse_datetime_prefix(value) {
            Some((_, "")) => {
                "the text is missing its mandatory UTC offset (Z or ±hh:mm)".to_string()
            }
            Some((_, offset_text)) if parse_offset_seconds(offset_text).is_none() => {
                "the UTC offset is malformed; an instant timestamp needs Z, z, or ±hh:mm"
                    .to_string()
            }
            _ => clock_misfit_cause(value),
        }),
        FieldType::Decimal { precision, scale } => {
            Some(decimal_misfit_cause(value, precision, scale))
        }
        _ => None,
    }
}

/// The clock-menu causes shared by both timestamp types, probed from the
/// rejection menu of ADR-0043: epoch numbers, date-only text, an
/// over-length fraction, then the generic menu wording.
fn clock_misfit_cause(value: &str) -> String {
    if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
        return "epoch numbers are not accepted as timestamp text".to_string();
    }
    if parse_timestamp_micros(&format!("{value} 00:00:00")).is_some() {
        return "date-only text has no time part".to_string();
    }
    if fraction_exceeds_six_digits(value) {
        return "the fraction has more than 6 digits and is never truncated".to_string();
    }
    "the text does not match YYYY-MM-DD HH:MM:SS with an optional fraction of 1 to 6 digits"
        .to_string()
}

/// Whether otherwise-valid clock text fails only by an over-length fraction:
/// the fixed-width menu puts the fraction point at byte 19 (after
/// `YYYY-MM-DD HH:MM:SS`), so the probe re-reads the prefix at that offset
/// and counts the digits the strict parser refused to truncate.
fn fraction_exceeds_six_digits(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.get(19) == Some(&b'.')
        && bytes[20..]
            .iter()
            .take_while(|byte| byte.is_ascii_digit())
            .count()
            > 6
        && parse_timestamp_micros(&value[..19]).is_some()
}

/// The decimal-text causes, probed from the rejection menu of ADR-0044:
/// exponent notation, thousands separators, then — for text that is plain
/// decimal syntax — the over-scale and precision-overflow rules.
fn decimal_misfit_cause(value: &str, precision: u8, scale: u8) -> String {
    if let Some((_, _, fraction_digits)) = split_plain_decimal(value) {
        if fraction_digits.len() > usize::from(scale) {
            return format!(
                "the value has {} fractional digits, more than scale {scale} allows; \
                 values are never rounded",
                fraction_digits.len()
            );
        }
        return decimal_overflow_cause(precision, scale);
    }
    if value.contains(['e', 'E']) && value.parse::<f64>().is_ok() {
        return "exponent notation is not accepted as decimal text".to_string();
    }
    if value.contains(',') && split_plain_decimal(&value.replace(',', "")).is_some() {
        return "thousands separators are not accepted as decimal text".to_string();
    }
    "the text is not plain decimal digits with an optional sign and decimal point".to_string()
}

fn decimal_overflow_cause(precision: u8, scale: u8) -> String {
    format!(
        "the value overflows decimal({precision},{scale}): \
         the integer part allows at most {} digits",
        precision - scale
    )
}

/// Names the concrete cause a JSONL value missed a declared type; see
/// [`text_misfit_cause`].
fn json_misfit_cause(value: &Value, expected: FieldType) -> Option<String> {
    match expected {
        FieldType::Timestamp | FieldType::Timestamptz => Some(match value {
            Value::String(text) => text_misfit_cause(text, expected)?,
            Value::Number(_) => format!(
                "JSON numbers do not fit a declared {} field: \
                 epoch numbers are not accepted",
                expected.name()
            ),
            other => format!(
                "JSON {} values do not fit a declared {} field; \
                 only JSON strings parse as timestamps",
                json_kind(other),
                expected.name()
            ),
        }),
        FieldType::Decimal { precision, scale } => Some(match value {
            Value::String(text) => decimal_misfit_cause(text, precision, scale),
            Value::Number(number) => match json_integer(number) {
                Some(_) => decimal_overflow_cause(precision, scale),
                None => "JSON floats do not fit a declared decimal field: \
                         their exact digits were already lost to IEEE parsing"
                    .to_string(),
            },
            other => format!(
                "JSON {} values do not fit a declared decimal field; \
                 only JSON strings and integers fit",
                json_kind(other)
            ),
        }),
        _ => None,
    }
}

fn json_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn required_field_rejection(line: u64, check: &FieldCheck, record: Value) -> RejectedRecord {
    RejectedRecord {
        line,
        code: rejection::MISSING_REQUIRED_FIELD,
        field: Some(check.name.clone()),
        source_field: check.source_field(),
        message: format!("required field {:?} is null", check.name),
        record,
    }
}

fn type_rejection(
    line: u64,
    check: &FieldCheck,
    expected_type: FieldType,
    origin: TypeOrigin,
    value: Value,
    cause: Option<String>,
    record: Value,
) -> RejectedRecord {
    let cause_suffix = cause.map(|cause| format!(": {cause}")).unwrap_or_default();
    RejectedRecord {
        line,
        code: rejection::TYPE_COERCION_FAILED,
        field: Some(check.name.clone()),
        source_field: check.source_field(),
        message: format!(
            "value {value} does not fit {} type {} for field {:?}{cause_suffix}",
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
                    .unwrap_or_else(|| survivor_types[column.observed_index].field_type()),
                nullable: override_
                    .and_then(|override_| override_.nullable)
                    .unwrap_or(true),
                name: column.dataset_name,
                source: column.source,
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

/// Fails a pinned load as `schema_drift` when a pinned declared-type field
/// has no override re-declaring its type (ADR-0042): declared types enter a
/// schema only through declaration, so the load definition stays the
/// declaration of record and a pin alone cannot resurrect one. Without the
/// override the field's effective type falls back to the observed type,
/// which no lattice rule widens to a declared type — a definition-level
/// omission, failed here as one drift, after shape matching (missing-field
/// drift wins) and before per-record validation (never a flood of
/// rejections). `observe` runs only on the failure path, over all records,
/// to name the effective type a rejection-free inference would produce.
fn check_declared_type_overrides(
    pinned_path: &str,
    pin: &PinnedSchema,
    matched: &[PlannedField],
    overrides: &SchemaOverrides,
    observe: impl FnOnce() -> Vec<InferredType>,
) -> Result<(), ExecutionFailure> {
    let undeclared: Vec<&PlannedField> = matched
        .iter()
        .filter(|planned| planned.materialized_type.lattice_type().is_none())
        .filter(|planned| {
            overrides
                .get(&planned.name)
                .and_then(|override_| override_.field_type)
                .is_none()
        })
        .collect();
    if undeclared.is_empty() {
        return Ok(());
    }

    let observed_types = observe();
    let effective_name =
        |planned: &PlannedField| observed_types[planned.observed_index].field_type().name();
    let detail = format!(
        "{}; declared types take effect only through schema.overrides — the override may be missing",
        undeclared
            .iter()
            .map(|planned| {
                format!(
                    "field {:?} is pinned as {} but its effective type is {}",
                    planned.name,
                    planned.materialized_type.name(),
                    effective_name(planned),
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
    );
    let drift = json!({
        "undeclared_fields": undeclared
            .iter()
            .map(|planned| {
                json!({
                    "name": planned.name,
                    "pinned_type": planned.materialized_type.name(),
                    "effective_type": effective_name(planned),
                })
            })
            .collect::<Vec<_>>(),
    });
    Err(drift_failure(pinned_path, pin, detail, drift))
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

/// The fixed materialization plan pass 2 builds chunks against (ADR-0045):
/// the planned fields in output order and the Arrow schema they spell. The
/// batch schema and the reported `schema_decision` derive from the same
/// resolution, so the report can never disagree with the chunks that were
/// written, and every chunk of a load carries the identical schema.
pub(crate) struct ChunkPlan {
    fields: Vec<PlannedField>,
    schema: Arc<Schema>,
}

impl ChunkPlan {
    fn new(fields: Vec<PlannedField>) -> Self {
        let schema = Arc::new(Schema::new(
            fields
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
        ChunkPlan { fields, schema }
    }

    /// Builds one chunk over surviving CSV records. An empty record run
    /// builds the zero-row batch that materializes the schema alone.
    pub(crate) fn build_text_chunk(
        &self,
        records: &[TextRecord],
    ) -> Result<RecordBatch, LoadFailure> {
        let columns = self
            .fields
            .iter()
            .map(|planned| {
                build_text_array(planned.materialized_type, records, planned.observed_index)
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.assemble(columns)
    }

    /// Builds one chunk over surviving JSONL records, read through their
    /// source addresses.
    pub(crate) fn build_json_chunk(
        &self,
        records: &[JsonRecord],
    ) -> Result<RecordBatch, LoadFailure> {
        let columns = self
            .fields
            .iter()
            .enumerate()
            .map(|(column_index, planned)| {
                build_json_array(
                    planned.materialized_type,
                    records,
                    &planned.source,
                    column_index,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.assemble(columns)
    }

    fn assemble(&self, columns: Vec<ArrayRef>) -> Result<RecordBatch, LoadFailure> {
        RecordBatch::try_new(self.schema.clone(), columns).map_err(|error| LoadFailure {
            code: "record_batch_creation_failed",
            message: format!("failed to create Arrow record batch: {error}"),
        })
    }
}

fn build_text_array(
    field_type: FieldType,
    records: &[TextRecord],
    column_index: usize,
) -> Result<ArrayRef, LoadFailure> {
    match field_type {
        FieldType::Utf8 => {
            let mut builder = StringBuilder::new();
            for record in records {
                match &record.cells[column_index] {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        FieldType::Boolean => {
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
        FieldType::Int64 => {
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
        FieldType::Float64 => {
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
        FieldType::Timestamp | FieldType::Timestamptz => {
            let mut values: Vec<Option<i64>> = Vec::with_capacity(records.len());
            for record in records {
                values.push(match &record.cells[column_index] {
                    Some(value) => {
                        Some(parse_declared_timestamp(value, field_type).ok_or_else(|| {
                            coercion_failure(column_index, value, &field_type.name())
                        })?)
                    }
                    None => None,
                });
            }
            timestamp_array(values, field_type)
        }
        FieldType::Decimal { precision, scale } => {
            let mut values: Vec<Option<i128>> = Vec::with_capacity(records.len());
            for record in records {
                values.push(match &record.cells[column_index] {
                    Some(value) => Some(parse_decimal_scaled(value, precision, scale).ok_or_else(
                        || coercion_failure(column_index, value, &field_type.name()),
                    )?),
                    None => None,
                });
            }
            decimal_array(values, precision, scale, column_index)
        }
    }
}

/// The microseconds a declared-timestamp cell stores, under whichever of the
/// two timestamp types the column declares (ADR-0043).
fn parse_declared_timestamp(value: &str, field_type: FieldType) -> Option<i64> {
    match field_type {
        FieldType::Timestamp => parse_timestamp_micros(value),
        FieldType::Timestamptz => parse_timestamptz_micros(value),
        _ => None,
    }
}

/// Assembles a microsecond-timestamp column, stamped UTC for the instant
/// type so the Arrow schema matches [`FieldType::data_type`] (ADR-0043).
fn timestamp_array(
    values: Vec<Option<i64>>,
    field_type: FieldType,
) -> Result<ArrayRef, LoadFailure> {
    let array = TimestampMicrosecondArray::from(values);
    Ok(match field_type {
        FieldType::Timestamptz => Arc::new(array.with_timezone("UTC")),
        _ => Arc::new(array),
    })
}

/// Assembles a `Decimal128(precision, scale)` column from pre-scaled values.
/// The parameters were validated at declaration time, so Arrow rejecting
/// them is an invariant breach reported as a clean failure.
fn decimal_array(
    values: Vec<Option<i128>>,
    precision: u8,
    scale: u8,
    column_index: usize,
) -> Result<ArrayRef, LoadFailure> {
    Decimal128Array::from(values)
        .with_precision_and_scale(precision, scale as i8)
        .map(|array| Arc::new(array) as ArrayRef)
        .map_err(|error| {
            coercion_failure(
                column_index,
                &error.to_string(),
                &FieldType::Decimal { precision, scale }.name(),
            )
        })
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
    field_type: FieldType,
    records: &[JsonRecord],
    source: &SourceAddress,
    column_index: usize,
) -> Result<ArrayRef, LoadFailure> {
    match field_type {
        FieldType::Utf8 => {
            let mut builder = StringBuilder::new();
            for record in records {
                match json_scalar_to_string(source.json_value(&record.object)) {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        FieldType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for record in records {
                match source.json_value(&record.object) {
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
        FieldType::Int64 => {
            let mut builder = Int64Builder::new();
            for record in records {
                match source.json_value(&record.object) {
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
        FieldType::Float64 => {
            let mut builder = Float64Builder::new();
            for record in records {
                match source.json_value(&record.object) {
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
        FieldType::Timestamp | FieldType::Timestamptz => {
            let mut values: Vec<Option<i64>> = Vec::with_capacity(records.len());
            for record in records {
                values.push(match source.json_value(&record.object) {
                    None | Some(Value::Null) => None,
                    Some(Value::String(text)) => {
                        Some(parse_declared_timestamp(text, field_type).ok_or_else(|| {
                            coercion_failure(column_index, text, &field_type.name())
                        })?)
                    }
                    Some(other) => {
                        return Err(coercion_failure(
                            column_index,
                            &other.to_string(),
                            &field_type.name(),
                        ))
                    }
                });
            }
            timestamp_array(values, field_type)
        }
        FieldType::Decimal { precision, scale } => {
            let mut values: Vec<Option<i128>> = Vec::with_capacity(records.len());
            for record in records {
                values.push(match source.json_value(&record.object) {
                    None | Some(Value::Null) => None,
                    Some(Value::String(text)) => {
                        Some(parse_decimal_scaled(text, precision, scale).ok_or_else(|| {
                            coercion_failure(column_index, text, &field_type.name())
                        })?)
                    }
                    Some(Value::Number(number)) => Some(
                        json_integer(number)
                            .and_then(|integer| rescale_decimal_integer(integer, precision, scale))
                            .ok_or_else(|| {
                                coercion_failure(
                                    column_index,
                                    &number.to_string(),
                                    &field_type.name(),
                                )
                            })?,
                    ),
                    Some(other) => {
                        return Err(coercion_failure(
                            column_index,
                            &other.to_string(),
                            &field_type.name(),
                        ))
                    }
                });
            }
            decimal_array(values, precision, scale, column_index)
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

    /// The field type an observed column materializes as when no declaration
    /// replaces it: the observation itself, with an all-null column
    /// defaulting to text. Inference can only reach the four lattice types —
    /// the declared-only types have no observation to come from (ADR-0042).
    fn field_type(self) -> FieldType {
        match self {
            InferredType::Null | InferredType::Utf8 => FieldType::Utf8,
            InferredType::Boolean => FieldType::Boolean,
            InferredType::Int64 => FieldType::Int64,
            InferredType::Float64 => FieldType::Float64,
        }
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

/// Parses wall-clock timestamp text (ADR-0043) into the microseconds an Arrow
/// `Timestamp(Microsecond, None)` column stores: the strict
/// `YYYY-MM-DD[ Tt]HH:MM:SS[.ffffff]` menu with nothing after it — wall-clock
/// text carrying an offset contradicts its own type and rejects.
fn parse_timestamp_micros(text: &str) -> Option<i64> {
    let (micros, offset_text) = parse_datetime_prefix(text)?;
    offset_text.is_empty().then_some(micros)
}

/// Parses instant timestamp text (ADR-0043): the same clock menu followed by
/// a mandatory `Z`/`z` or `±hh:mm` offset, normalized to the microseconds of
/// the UTC instant it spells.
fn parse_timestamptz_micros(text: &str) -> Option<i64> {
    let (micros, offset_text) = parse_datetime_prefix(text)?;
    let offset_seconds = parse_offset_seconds(offset_text)?;
    micros.checked_sub(offset_seconds.checked_mul(1_000_000)?)
}

/// Lexes the strict clock menu shared by both timestamp types —
/// `YYYY-MM-DD`, one space or `T`/`t` separator, `HH:MM:SS`, an optional
/// fraction of 1 to 6 digits — into epoch microseconds plus the unconsumed
/// offset text. Fields are fixed-width zero-padded and the year is four
/// digits, so nothing locale-dependent can parse. More than 6 fractional
/// digits reject rather than truncate. Calendar validity (month lengths,
/// leap years) and the 00-59 second range (leap-second text rejects) come
/// from chrono's `Naive` types.
fn parse_datetime_prefix(text: &str) -> Option<(i64, &str)> {
    let bytes = text.as_bytes();
    if bytes.len() < 19 {
        return None;
    }
    let year = parse_fixed_digits(&bytes[0..4])?;
    let month = parse_fixed_digits(&bytes[5..7])?;
    let day = parse_fixed_digits(&bytes[8..10])?;
    let hour = parse_fixed_digits(&bytes[11..13])?;
    let minute = parse_fixed_digits(&bytes[14..16])?;
    let second = parse_fixed_digits(&bytes[17..19])?;
    if bytes[4] != b'-'
        || bytes[7] != b'-'
        || !matches!(bytes[10], b' ' | b'T' | b't')
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }

    let (fraction_micros, offset_start) = if bytes.get(19) == Some(&b'.') {
        let digit_count = bytes[20..]
            .iter()
            .take_while(|byte| byte.is_ascii_digit())
            .count();
        if digit_count == 0 || digit_count > 6 {
            return None;
        }
        let fraction = parse_fixed_digits(&bytes[20..20 + digit_count])?;
        (
            fraction * 10_u32.pow(6 - digit_count as u32),
            20 + digit_count,
        )
    } else {
        (0, 19)
    };

    let date = chrono::NaiveDate::from_ymd_opt(year as i32, month, day)?;
    let time = chrono::NaiveTime::from_hms_micro_opt(hour, minute, second, fraction_micros)?;
    Some((
        date.and_time(time).and_utc().timestamp_micros(),
        &text[offset_start..],
    ))
}

/// Parses the mandatory instant-timestamp offset — `Z`/`z`, or `±hh:mm` with
/// zero-padded fields — as signed seconds east of UTC.
fn parse_offset_seconds(text: &str) -> Option<i64> {
    if text == "Z" || text == "z" {
        return Some(0);
    }
    let bytes = text.as_bytes();
    if bytes.len() != 6 || bytes[3] != b':' {
        return None;
    }
    let sign = match bytes[0] {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let hours = parse_fixed_digits(&bytes[1..3])?;
    let minutes = parse_fixed_digits(&bytes[4..6])?;
    if hours > 23 || minutes > 59 {
        return None;
    }
    Some(sign * i64::from(hours * 3600 + minutes * 60))
}

/// Parses a fixed-width zero-padded clock field: every byte an ASCII digit.
fn parse_fixed_digits(bytes: &[u8]) -> Option<u32> {
    let mut value: u32 = 0;
    for byte in bytes {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value * 10 + u32::from(byte - b'0');
    }
    Some(value)
}

/// Parses decimal text (ADR-0044) into the scaled integer a
/// `Decimal128(precision, scale)` column stores: an optional sign, then plain
/// base-10 digits with at most one decimal point — no exponent notation, no
/// thousands separators, no whitespace. Fewer fractional digits than the
/// scale rescale losslessly; more reject, because rounding never happens. A
/// zero-padded integer part parses: ADR-0032 guards inference, not
/// declared-type parsing.
fn parse_decimal_scaled(text: &str, precision: u8, scale: u8) -> Option<i128> {
    let (negative, integer_digits, fraction_digits) = split_plain_decimal(text)?;
    if fraction_digits.len() > usize::from(scale) {
        return None;
    }

    let mut scaled: i128 = 0;
    for byte in integer_digits.bytes().chain(fraction_digits.bytes()) {
        scaled = scaled
            .checked_mul(10)?
            .checked_add(i128::from(byte - b'0'))?;
    }
    for _ in fraction_digits.len()..usize::from(scale) {
        scaled = scaled.checked_mul(10)?;
    }
    if negative {
        scaled = -scaled;
    }
    bounded_decimal(scaled, precision)
}

/// Splits text into plain decimal syntax — an optional sign, then base-10
/// digits with at most one decimal point carrying at least one digit on each
/// side — or `None` for anything else (ADR-0044). Shared by the parser and
/// the cause classifier so both agree on what "plain decimal text" means.
fn split_plain_decimal(text: &str) -> Option<(bool, &str, &str)> {
    let (negative, unsigned) = match text.as_bytes().first()? {
        b'-' => (true, &text[1..]),
        b'+' => (false, &text[1..]),
        _ => (false, text),
    };
    let (integer_digits, fraction_digits) = match unsigned.split_once('.') {
        Some((_, "")) => return None,
        Some(split) => split,
        None => (unsigned, ""),
    };
    if integer_digits.is_empty()
        || !integer_digits.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction_digits.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    Some((negative, integer_digits, fraction_digits))
}

/// Rescales a JSON integer into a declared decimal's scaled representation
/// (ADR-0044). An integer's exact digits were never lost, so only the
/// precision bound can reject it.
fn rescale_decimal_integer(value: i128, precision: u8, scale: u8) -> Option<i128> {
    let scaled = value.checked_mul(10_i128.checked_pow(u32::from(scale))?)?;
    bounded_decimal(scaled, precision)
}

/// A scaled decimal value fits its declared precision iff its magnitude
/// spells at most `precision` digits.
fn bounded_decimal(scaled: i128, precision: u8) -> Option<i128> {
    let bound = 10_i128.pow(u32::from(precision));
    (-bound < scaled && scaled < bound).then_some(scaled)
}

/// Materializes a whole CSV input in one call — the streaming observer, the
/// resolution, and one chunk over every survivor — as the test-only
/// equivalence harness for the pass-based machinery: the observed behavior
/// of this wrapper is the pre-chunking whole-input contract.
#[cfg(test)]
pub(crate) fn from_text_columns(
    directive: &SchemaDirective,
    field_names: Vec<String>,
    records: Vec<TextRecord>,
) -> Result<Materialized, ExecutionFailure> {
    let mut observer = TextObserver::new(directive, &field_names);
    let mut survivors = Vec::new();
    let mut rejected = Vec::new();
    for record in records {
        match observer.observe(&record) {
            Some(rejection) => rejected.push(rejection),
            None => survivors.push(record),
        }
    }
    let resolution = observer.finish()?;
    let batch = resolution.plan.build_text_chunk(&survivors)?;
    Ok(Materialized {
        batch,
        schema_decision: resolution.decision,
        pinned_schema_write: resolution.pinned_schema_write,
        rejected,
    })
}

/// Materializes a whole JSONL input in one call; see [`from_text_columns`].
/// Rejections spill as records during observation and render against the
/// resolved checks, exactly like the streaming artifact path.
#[cfg(test)]
pub(crate) fn from_json_columns(
    directive: &SchemaDirective,
    field_names: Vec<String>,
    records: Vec<JsonRecord>,
) -> Result<Materialized, ExecutionFailure> {
    let mut observer = JsonObserver::new(directive);
    let mut survivors = Vec::new();
    let mut spilled = Vec::new();
    for record in records {
        match observer.observe(&record) {
            JsonOutcome::Rejected => spilled.push(record),
            JsonOutcome::Survived => survivors.push(record),
        }
    }
    let resolution = observer.finish(&field_names)?;
    let rejected = spilled
        .iter()
        .filter_map(|record| resolution.rejection_for(record))
        .collect();
    let batch = resolution.plan.build_json_chunk(&survivors)?;
    Ok(Materialized {
        batch,
        schema_decision: resolution.decision,
        pinned_schema_write: resolution.pinned_schema_write,
        rejected,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        Array, BooleanArray, Decimal128Array, Float64Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    };

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
    fn observed_field_types_promote_only_all_null_columns_to_text() {
        use InferredType::*;
        assert_eq!(Null.field_type(), FieldType::Utf8);
        assert_eq!(Boolean.field_type(), FieldType::Boolean);
        assert_eq!(Int64.field_type(), FieldType::Int64);
        assert_eq!(Float64.field_type(), FieldType::Float64);
        assert_eq!(Utf8.field_type(), FieldType::Utf8);
    }

    // ---- Declared-type value parsing (ADR-0043, ADR-0044) ----

    #[test]
    fn parse_timestamp_micros_reads_the_strict_wall_clock_menu() {
        // Worked example: 2026-07-14 17:00:00 UTC is epoch second
        // 1_784_048_400 (computed independently with `date -u`).
        const BASE: i64 = 1_784_048_400_000_000;
        assert_eq!(parse_timestamp_micros("2026-07-14 17:00:00"), Some(BASE));
        assert_eq!(parse_timestamp_micros("2026-07-14T17:00:00"), Some(BASE));
        assert_eq!(parse_timestamp_micros("2026-07-14t17:00:00"), Some(BASE));
        // Fractions of 1 to 6 digits scale to microseconds.
        assert_eq!(
            parse_timestamp_micros("2026-07-14 17:00:00.5"),
            Some(BASE + 500_000)
        );
        assert_eq!(
            parse_timestamp_micros("2026-07-14 17:00:00.123456"),
            Some(BASE + 123_456)
        );
        // The epoch and its pre-epoch side.
        assert_eq!(parse_timestamp_micros("1970-01-01 00:00:00"), Some(0));
        assert_eq!(
            parse_timestamp_micros("1969-12-31 23:59:59"),
            Some(-1_000_000)
        );
        // 2000 is a leap year (divisible by 400): Feb 29 is a real date.
        assert_eq!(
            parse_timestamp_micros("2000-02-29 12:30:45"),
            Some(951_827_445_000_000)
        );
    }

    #[test]
    fn parse_timestamp_micros_rejects_offsets_and_non_menu_text() {
        for text in [
            "2026-07-14T17:00:00Z", // wall-clock text must not carry an offset
            "2026-07-14T17:00:00+08:00",
            "2026-07-14",                  // date-only text
            "17:00:00",                    // time-only text
            "2026-07-14 17:00:00.1234567", // more than 6 fractional digits
            "2026-07-14 17:00:00.",        // a point needs digits
            "2026-7-14 17:00:00",          // fields are fixed-width zero-padded
            "26-07-14 17:00:00",           // year is four digits
            "2026-07-14  17:00:00",        // exactly one separator
            "2026-02-30 00:00:00",         // calendar validity
            "2001-02-29 00:00:00",         // 2001 is not a leap year
            "2026-13-01 00:00:00",
            "2026-00-01 00:00:00",
            "2026-07-14 24:00:00",
            "2026-07-14 17:60:00",
            "2026-07-14 23:59:60", // leap-second text parses strictly
            "14/07/2026 17:00:00", // no locale-dependent forms
            "July 14, 2026 17:00",
            "1784048400", // no epoch numbers
            " 2026-07-14 17:00:00",
            "2026-07-14 17:00:00 ",
            "",
        ] {
            assert_eq!(parse_timestamp_micros(text), None, "{text:?} should reject");
        }
    }

    #[test]
    fn parse_timestamptz_micros_requires_an_offset_and_normalizes_to_utc() {
        const INSTANT: i64 = 1_784_048_400_000_000; // 2026-07-14T17:00:00Z
        assert_eq!(
            parse_timestamptz_micros("2026-07-14T17:00:00Z"),
            Some(INSTANT)
        );
        assert_eq!(
            parse_timestamptz_micros("2026-07-14t17:00:00z"),
            Some(INSTANT)
        );
        // The same instant spelled from other offsets normalizes to it.
        assert_eq!(
            parse_timestamptz_micros("2026-07-15T01:00:00+08:00"),
            Some(INSTANT)
        );
        assert_eq!(
            parse_timestamptz_micros("2026-07-14 12:00:00-05:00"),
            Some(INSTANT)
        );
        assert_eq!(
            parse_timestamptz_micros("2026-07-14T17:00:00+00:00"),
            Some(INSTANT)
        );
        assert_eq!(
            parse_timestamptz_micros("2026-07-14T17:00:00.000001Z"),
            Some(INSTANT + 1)
        );
    }

    #[test]
    fn parse_timestamptz_micros_rejects_missing_and_malformed_offsets() {
        for text in [
            "2026-07-14T17:00:00",      // an instant needs its offset
            "2026-07-14T17:00:00+08",   // minutes are mandatory
            "2026-07-14T17:00:00+8:00", // fixed-width zero-padded
            "2026-07-14T17:00:00+08:0",
            "2026-07-14T17:00:00+08:60",
            "2026-07-14T17:00:00+24:00",
            "2026-07-14T17:00:00 Z",
            "2026-07-14T17:00:00ZZ",
            "2026-07-14T17:00:00UTC",
        ] {
            assert_eq!(
                parse_timestamptz_micros(text),
                None,
                "{text:?} should reject"
            );
        }
    }

    #[test]
    fn parse_decimal_scaled_rescales_losslessly_and_never_rounds() {
        assert_eq!(parse_decimal_scaled("1.2", 10, 2), Some(120));
        assert_eq!(parse_decimal_scaled("1.20", 10, 2), Some(120));
        assert_eq!(parse_decimal_scaled("42", 10, 2), Some(4_200));
        assert_eq!(parse_decimal_scaled("+42", 10, 2), Some(4_200));
        assert_eq!(parse_decimal_scaled("-3.75", 10, 2), Some(-375));
        assert_eq!(parse_decimal_scaled("0", 1, 0), Some(0));
        // Zero-padded integer parts parse: ADR-0032 guards inference, not
        // declared-type parsing.
        assert_eq!(parse_decimal_scaled("007.50", 10, 2), Some(750));
        // The full-precision boundary on both sides of zero.
        assert_eq!(
            parse_decimal_scaled("99999999.99", 10, 2),
            Some(9_999_999_999)
        );
        assert_eq!(
            parse_decimal_scaled("-99999999.99", 10, 2),
            Some(-9_999_999_999)
        );
        assert_eq!(
            parse_decimal_scaled("99999999999999999999999999999999999999", 38, 0),
            Some(99_999_999_999_999_999_999_999_999_999_999_999_999)
        );
        // With scale equal to precision only magnitudes below one fit.
        assert_eq!(parse_decimal_scaled("0.5", 1, 1), Some(5));
        assert_eq!(parse_decimal_scaled("1.5", 1, 1), None);
    }

    #[test]
    fn parse_decimal_scaled_rejects_over_scale_overflow_and_non_menu_text() {
        for (text, precision, scale) in [
            ("1.234", 10, 2),        // over-scale fractions reject, never round
            ("0.001", 10, 2),        // even when rounding would reach zero
            ("100000000.00", 10, 2), // integer digits beyond p - s overflow
            ("1e3", 10, 2),          // no exponent notation
            ("1E3", 10, 2),
            ("1,000", 10, 2), // no thousands separators
            ("1 000", 10, 2), // no whitespace
            (" 1.2", 10, 2),
            ("1.2 ", 10, 2),
            ("1.", 10, 2),    // a point needs fraction digits
            (".5", 10, 2),    // and an integer part
            ("1.2.3", 10, 2), // at most one decimal point
            ("--1", 10, 2),
            ("+-1", 10, 2),
            ("abc", 10, 2),
            ("", 10, 2),
            ("NaN", 10, 2),
            ("0x10", 10, 2),
        ] {
            assert_eq!(
                parse_decimal_scaled(text, precision, scale),
                None,
                "{text:?} under decimal({precision},{scale}) should reject"
            );
        }
    }

    #[test]
    fn rescale_decimal_integer_scales_json_integers_into_the_precision_bound() {
        assert_eq!(rescale_decimal_integer(42, 10, 2), Some(4_200));
        assert_eq!(rescale_decimal_integer(-7, 10, 2), Some(-700));
        assert_eq!(rescale_decimal_integer(0, 1, 0), Some(0));
        assert_eq!(
            rescale_decimal_integer(99_999_999, 10, 2),
            Some(9_999_999_900)
        );
        // One more digit than p - s allows overflows.
        assert_eq!(rescale_decimal_integer(100_000_000, 10, 2), None);
        // A u64 beyond i64 still rescales exactly: its digits were never lost
        // to IEEE parsing.
        assert_eq!(
            rescale_decimal_integer(18_446_744_073_709_551_615, 38, 0),
            Some(18_446_744_073_709_551_615)
        );
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

    // ---- Declared-type materialization (ADR-0042, ADR-0043, ADR-0044) ----

    #[test]
    fn from_text_columns_materializes_declared_types_under_overrides() {
        let materialized = from_text_columns(
            &overridden_inferred_directive(&[
                ("created_at", Some("timestamp"), None),
                ("settled_at", Some("timestamptz"), None),
                ("amount", Some("decimal(10,2)"), None),
            ]),
            names(&["created_at", "settled_at", "amount"]),
            vec![
                record(
                    2,
                    &[
                        Some("2026-07-14 17:00:00"),
                        Some("2026-07-14T17:00:00Z"),
                        Some("1.2"),
                    ],
                ),
                record(
                    3,
                    &[
                        Some("2026-07-14T17:00:00.5"),
                        Some("2026-07-15t01:00:00+08:00"),
                        Some("007.50"),
                    ],
                ),
                record(4, &[None, None, None]),
            ],
        )
        .expect("declared overrides materialize");
        let batch = &materialized.batch;

        const INSTANT: i64 = 1_784_048_400_000_000; // 2026-07-14T17:00:00Z
        assert_eq!(
            schema_types(batch),
            vec![
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                DataType::Decimal128(10, 2),
            ]
        );
        let created = timestamps(batch, 0);
        assert_eq!(created.value(0), INSTANT);
        assert_eq!(created.value(1), INSTANT + 500_000);
        assert!(created.is_null(2));
        // Different offset spellings of one instant store equal microseconds.
        let settled = timestamps(batch, 1);
        assert_eq!(settled.value(0), INSTANT);
        assert_eq!(settled.value(1), INSTANT);
        assert!(settled.is_null(2));
        let amounts = decimals(batch, 2);
        assert_eq!(amounts.value(0), 120); // "1.2" stores as 1.20
        assert_eq!(amounts.value(1), 750); // zero-padded text parses
        assert!(amounts.is_null(2));

        assert!(materialized.rejected.is_empty());
        assert_eq!(
            materialized.schema_decision["fields"],
            json!([
                {"name": "created_at", "type": "timestamp", "nullable": true},
                {"name": "settled_at", "type": "timestamptz", "nullable": true},
                {"name": "amount", "type": "decimal(10,2)", "nullable": true}
            ])
        );
    }

    #[test]
    fn from_text_columns_rejects_declared_type_misfits_naming_the_cause() {
        for (field_type, value, cause_part) in [
            ("timestamp", "2026-07-14T17:00:00Z", "carries a UTC offset"),
            (
                "timestamp",
                "2026-07-14T17:00:00UTC",
                "continues after the clock reading",
            ),
            ("timestamp", "2026-07-14", "date-only"),
            ("timestamp", "1784048400", "epoch numbers"),
            ("timestamp", "2026-07-14 17:00:00.1234567", "more than 6"),
            (
                "timestamptz",
                "2026-07-14T17:00:00",
                "missing its mandatory UTC offset",
            ),
            (
                "timestamptz",
                "2026-07-14T17:00:00+8:00",
                "UTC offset is malformed",
            ),
            (
                "timestamptz",
                "2026-07-14T17:00:00ZZ",
                "UTC offset is malformed",
            ),
            ("decimal(10,2)", "1.234", "more than scale 2"),
            ("decimal(10,2)", "100000000.00", "overflows decimal(10,2)"),
            ("decimal(10,2)", "1e3", "exponent notation"),
            ("decimal(10,2)", "1,000", "thousands separators"),
        ] {
            let materialized = from_text_columns(
                &overridden_inferred_directive(&[("v", Some(field_type), None)]),
                names(&["v"]),
                vec![record(2, &[Some(value)])],
            )
            .expect("a misfit rejects the record, not the load");

            assert_eq!(materialized.batch.num_rows(), 0, "{value:?} written");
            assert_eq!(materialized.rejected.len(), 1, "{value:?} rejected");
            let rejected = &materialized.rejected[0];
            assert_eq!(rejected.code, "type_coercion_failed", "{value:?}");
            assert_eq!(rejected.field.as_deref(), Some("v"));
            assert!(
                rejected
                    .message
                    .contains(&format!("does not fit overridden type {field_type}")),
                "message {:?} misses the type {field_type:?}",
                rejected.message
            );
            assert!(
                rejected.message.contains(cause_part),
                "message {:?} misses the cause {cause_part:?}",
                rejected.message
            );
        }
    }

    #[test]
    fn from_json_columns_materializes_declared_types_from_strings_and_integers() {
        let materialized = from_json_columns(
            &overridden_inferred_directive(&[
                ("created_at", Some("timestamp"), None),
                ("settled_at", Some("timestamptz"), None),
                ("amount", Some("decimal(10,2)"), None),
            ]),
            names(&["created_at", "settled_at", "amount"]),
            vec![
                json_record(
                    1,
                    "{\"created_at\": \"2026-07-14T17:00:00\", \
                      \"settled_at\": \"2026-07-15T01:00:00+08:00\", \
                      \"amount\": \"1.2\"}",
                ),
                // A JSON integer rescales under decimal; JSON null stays null.
                json_record(
                    2,
                    "{\"created_at\": null, \"settled_at\": null, \"amount\": 42}",
                ),
            ],
        )
        .expect("declared overrides materialize from json");
        let batch = &materialized.batch;

        const INSTANT: i64 = 1_784_048_400_000_000; // 2026-07-14T17:00:00Z
        assert_eq!(
            schema_types(batch),
            vec![
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                DataType::Decimal128(10, 2),
            ]
        );
        assert_eq!(timestamps(batch, 0).value(0), INSTANT);
        assert!(timestamps(batch, 0).is_null(1));
        assert_eq!(timestamps(batch, 1).value(0), INSTANT);
        assert!(timestamps(batch, 1).is_null(1));
        assert_eq!(decimals(batch, 2).value(0), 120);
        assert_eq!(decimals(batch, 2).value(1), 4_200);
        assert!(materialized.rejected.is_empty());
    }

    #[test]
    fn from_json_columns_rejects_json_shapes_that_do_not_fit_declared_types() {
        for (field_type, value_json, cause_part) in [
            // Epoch numbers stay unaccepted in JSON exactly like in text.
            ("timestamp", "1784048400", "epoch numbers"),
            ("timestamp", "true", "only JSON strings"),
            (
                "timestamptz",
                "\"2026-07-14T17:00:00\"",
                "missing its mandatory UTC offset",
            ),
            // A JSON float's exact digits were already lost to IEEE parsing.
            ("decimal(10,2)", "1.2", "JSON float"),
            ("decimal(10,2)", "100000000", "overflows decimal(10,2)"),
            ("decimal(10,2)", "[1]", "only JSON strings and integers"),
        ] {
            let materialized = from_json_columns(
                &overridden_inferred_directive(&[("v", Some(field_type), None)]),
                names(&["v"]),
                vec![json_record(1, &format!("{{\"v\": {value_json}}}"))],
            )
            .expect("a misfit rejects the record, not the load");

            assert_eq!(materialized.batch.num_rows(), 0, "{value_json:?} written");
            assert_eq!(materialized.rejected.len(), 1, "{value_json:?} rejected");
            let rejected = &materialized.rejected[0];
            assert_eq!(rejected.code, "type_coercion_failed", "{value_json:?}");
            assert!(
                rejected.message.contains(cause_part),
                "message {:?} misses the cause {cause_part:?}",
                rejected.message
            );
        }
    }

    #[test]
    fn a_pinned_declared_type_field_without_its_override_fails_as_drift() {
        // ADR-0042: the load definition stays the declaration of record — a
        // pin alone cannot resurrect a declared type. Omitting the override
        // (or overriding only nullable) makes the effective type fall back
        // to the observed type, and the load fails as drift under both
        // policies, naming both types and the likely fix.
        for drift_policy in [DriftPolicy::Fail, DriftPolicy::AllowAdditiveNullable] {
            // The second configuration overrides only nullable — agreeing
            // with the pin, so no conflict fires — and still omits the type.
            for overrides_config in [
                SchemaOverrides::none(),
                overrides(&[("created_at", None, Some(true))]),
            ] {
                let error = from_text_columns(
                    &overridden_pinned_directive(
                        "version: 1\nfields:\n- name: created_at\n  type: timestamp\n",
                        drift_policy,
                        overrides_config,
                    ),
                    names(&["created_at"]),
                    vec![record(2, &[Some("2026-07-14 17:00:00")])],
                )
                .err()
                .expect("a pinned declared type without its override is drift");

                assert_eq!(error.failure.code, "schema_drift");
                assert!(
                    error.failure.message.contains("pinned as timestamp"),
                    "message {:?} misses the pinned type",
                    error.failure.message
                );
                assert!(
                    error.failure.message.contains("effective type is utf8"),
                    "message {:?} misses the effective type",
                    error.failure.message
                );
                assert!(
                    error.failure.message.contains("override may be missing"),
                    "message {:?} misses the hint",
                    error.failure.message
                );
                let decision = error.schema_decision.expect("decision echoed");
                assert_eq!(decision["drift_status"], "failed_on_drift");
                assert_eq!(
                    decision["drift"]["undeclared_fields"],
                    json!([{
                        "name": "created_at",
                        "pinned_type": "timestamp",
                        "effective_type": "utf8"
                    }])
                );
            }
        }
    }

    #[test]
    fn a_pinned_declared_type_field_without_its_override_fails_as_drift_for_json() {
        // The JSONL leg of the rule above: the effective type names what the
        // batch's JSON values observe — an integer column here.
        let error = from_json_columns(
            &pinned_directive(
                "version: 1\nfields:\n- name: amount\n  type: decimal(10,2)\n",
                DriftPolicy::Fail,
            ),
            names(&["amount"]),
            vec![json_record(1, "{\"amount\": 42}")],
        )
        .err()
        .expect("a pinned declared type without its override is drift");

        assert_eq!(error.failure.code, "schema_drift");
        assert!(
            error.failure.message.contains("pinned as decimal(10,2)"),
            "message {:?} misses the pinned type",
            error.failure.message
        );
        assert!(
            error.failure.message.contains("effective type is int64"),
            "message {:?} misses the effective type",
            error.failure.message
        );
    }

    #[test]
    fn pinned_declared_types_with_their_overrides_validate_per_record() {
        // The declaration of record is present on both sides: the pin and the
        // overrides agree, records validate parse-based per record, and the
        // misfit rejects without disturbing the drift-free load.
        let materialized = from_text_columns(
            &overridden_pinned_directive(
                "version: 1\n\
                 fields:\n\
                 - name: created_at\n\
                 \x20 type: timestamp\n\
                 - name: note\n\
                 \x20 type: utf8\n",
                DriftPolicy::Fail,
                overrides(&[("created_at", Some("timestamp"), None)]),
            ),
            names(&["created_at", "note"]),
            vec![
                record(2, &[Some("2026-07-14 17:00:00"), Some("a")]),
                record(3, &[Some("2026-07-14T17:00:00Z"), Some("b")]),
            ],
        )
        .expect("declared pin with its override materializes");

        assert_eq!(
            schema_types(&materialized.batch),
            vec![
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Utf8,
            ]
        );
        assert_eq!(materialized.batch.num_rows(), 1);
        assert_eq!(materialized.rejected.len(), 1);
        assert_eq!(materialized.rejected[0].line, 3);
        assert_eq!(materialized.rejected[0].code, "type_coercion_failed");
        assert_eq!(materialized.schema_decision["drift_status"], "none");
    }

    #[test]
    fn declared_type_parameter_disagreements_fail_as_override_conflicts() {
        // ADR-0042: pin comparison treats declared types as exactly equal or
        // different — decimal(10,2) vs decimal(12,2) is a contradiction, not
        // widening, and surfaces through the established conflict check
        // before any drift comparison, under either policy.
        for (pin_type, override_type) in [
            ("decimal(10,2)", "decimal(12,2)"),
            ("timestamp", "timestamptz"),
            ("utf8", "timestamp"),
        ] {
            let error = from_text_columns(
                &overridden_pinned_directive(
                    &format!("version: 1\nfields:\n- name: v\n  type: {pin_type}\n"),
                    DriftPolicy::AllowAdditiveNullable,
                    overrides(&[("v", Some(override_type), None)]),
                ),
                names(&["v"]),
                vec![record(2, &[Some("x")])],
            )
            .err()
            .expect("a declared-type contradiction fails");

            assert_eq!(error.failure.code, "schema_override_conflict");
            assert!(
                error.failure.message.contains(&format!(
                    "pinned type {pin_type}, override type {override_type}"
                )),
                "message {:?} misses the contradiction",
                error.failure.message
            );
        }
    }

    #[test]
    fn an_added_declared_type_field_extends_the_pin_under_the_additive_policy() {
        // ADR-0042: a newly added nullable declared-type field is legal
        // additive drift — the override shapes it and the rewritten pin
        // records the declared name.
        let materialized = from_text_columns(
            &overridden_pinned_directive(
                "version: 1\nfields:\n- name: id\n  type: int64\n",
                DriftPolicy::AllowAdditiveNullable,
                overrides(&[("settled_at", Some("timestamptz"), None)]),
            ),
            names(&["id", "settled_at"]),
            vec![record(2, &[Some("1"), Some("2026-07-14T17:00:00Z")])],
        )
        .expect("an added declared-type field is additive drift");

        assert_eq!(
            schema_types(&materialized.batch),
            vec![
                DataType::Int64,
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ]
        );
        assert_eq!(
            materialized.schema_decision["drift_status"],
            "additive_fields_added"
        );
        let pin_write = materialized
            .pinned_schema_write
            .expect("additive drift rewrites the pin");
        assert!(
            pin_write.yaml.contains("timestamptz"),
            "rewritten pin {:?} carries the declared type",
            pin_write.yaml
        );
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
                        field_type: FieldType::Int64,
                        nullable: true,
                    },
                    PinnedField {
                        name: "name".to_string(),
                        field_type: FieldType::Utf8,
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
        assert_eq!(pin.fields[0].field_type, FieldType::Int64);
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
    fn pinned_schema_accepts_declared_types_and_round_trips_them() {
        // ADR-0042: the declared-only types are pin vocabulary like any other
        // field type, and the persisted YAML prints them exactly as declared.
        let yaml = "version: 1\n\
                    fields:\n\
                    - name: created_at\n\
                    \x20 type: timestamp\n\
                    \x20 nullable: true\n\
                    - name: settled_at\n\
                    \x20 type: timestamptz\n\
                    \x20 nullable: false\n\
                    - name: amount\n\
                    \x20 type: decimal(10,2)\n\
                    \x20 nullable: true\n";
        let pin = PinnedSchema::from_yaml(yaml).expect("declared-type pin parses");
        assert_eq!(pin.fields[0].field_type, FieldType::Timestamp);
        assert_eq!(pin.fields[1].field_type, FieldType::Timestamptz);
        assert_eq!(
            pin.fields[2].field_type,
            FieldType::Decimal {
                precision: 10,
                scale: 2
            }
        );

        let round_tripped = pin.to_yaml();
        assert!(
            round_tripped.contains("decimal(10,2)"),
            "persisted pin {round_tripped:?} prints the decimal exactly as declared"
        );
        assert_eq!(
            PinnedSchema::from_yaml(&round_tripped).expect("round-tripped pin parses"),
            pin
        );
    }

    #[test]
    fn pinned_schema_rejects_malformed_declared_type_strings() {
        // ADR-0042/0044: a malformed declaration is a broken contract file
        // failing in the existing invalid-declaration style. `decimal` needs
        // the canonical `decimal(p,s)` spelling — both parameters, no spaces,
        // no leading zeros — with 1 <= p <= 38 and 0 <= s <= p.
        for type_name in [
            "decimal",
            "decimal()",
            "decimal(10)",
            "decimal(10,)",
            "decimal(,2)",
            "decimal(0,0)",
            "decimal(39,2)",
            "decimal(10,11)",
            "decimal(10, 2)",
            "decimal(010,2)",
            "decimal(+10,2)",
            "timestamp(3)",
            "timestamptz(utc)",
            "date",
        ] {
            let yaml = format!("version: 1\nfields:\n- name: v\n  type: \"{type_name}\"\n");
            let error = PinnedSchema::from_yaml(&yaml)
                .err()
                .unwrap_or_else(|| panic!("pinned schema type {type_name:?} accepted"));
            assert_eq!(
                error.code, "invalid_pinned_schema",
                "code for {type_name:?}"
            );
            assert!(
                error.message.contains(&format!(
                    "unsupported pinned schema field type: {type_name}"
                )),
                "message {:?} misses {type_name:?}",
                error.message
            );
        }
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

    /// Builds a validated transform from the `transform` block's YAML text,
    /// under the JSONL source format so flatten declarations validate.
    fn transform(yaml: &str) -> SchemaTransform {
        SchemaTransform::from_config(
            &serde_yaml::from_str::<TransformConfig>(yaml).expect("test transform parses"),
            "jsonl",
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
        assert_eq!(error.rejected_count, 0);
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
    fn schema_overrides_accept_the_declared_type_vocabulary() {
        // ADR-0042: overrides share the pin's type vocabulary, so the three
        // declared-only names — decimal across its whole parameter range —
        // validate like any existing type name.
        for (type_name, expected) in [
            ("timestamp", FieldType::Timestamp),
            ("timestamptz", FieldType::Timestamptz),
            (
                "decimal(1,0)",
                FieldType::Decimal {
                    precision: 1,
                    scale: 0,
                },
            ),
            (
                "decimal(38,38)",
                FieldType::Decimal {
                    precision: 38,
                    scale: 38,
                },
            ),
        ] {
            let validated = overrides(&[("v", Some(type_name), None)]);
            assert_eq!(
                validated
                    .get("v")
                    .and_then(|override_| override_.field_type),
                Some(expected),
                "override type {type_name:?}"
            );
        }
    }

    #[test]
    fn schema_overrides_reject_malformed_declared_type_strings() {
        // Same invalid-declaration style as any unsupported override type:
        // the load fails before any data is read (ADR-0042).
        for type_name in ["decimal", "decimal(39,2)", "decimal(2,3)", "decimal(10, 2)"] {
            let error = SchemaOverrides::from_entries(&[OverrideEntry {
                name: "v".to_string(),
                field_type: Some(type_name.to_string()),
                nullable: None,
            }])
            .err()
            .unwrap_or_else(|| panic!("override type {type_name:?} accepted"));
            assert_eq!(error.code, "unsupported_override_type");
            assert!(
                error.message.contains(type_name),
                "message {:?} misses {type_name:?}",
                error.message
            );
        }
    }

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
        assert_eq!(error.rejected_count, 0);
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
            "transform selects, renames, or flattens fields absent from the observed source shape: vip"
        );
        assert_eq!(error.rejected_count, 0);
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
            "transform selects, renames, or flattens fields absent from the observed source shape: email"
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

    // ---- Flatten mapping (ADR-0041) ----

    #[test]
    fn from_json_columns_flattens_declared_paths_into_added_dataset_fields() {
        // Without select, flatten outputs append after the observed fields in
        // declaration order; the parent keeps materializing as compact JSON
        // text, and the decision echoes the mapping as written.
        let directive = transformed_inferred_directive(
            "flatten: {customer.name: customer_name, customer.age: customer_age}",
        );
        let materialized = from_json_columns(
            &directive,
            names(&["id", "customer"]),
            vec![
                json_record(1, r#"{"id": 1, "customer": {"name": "Ada", "age": 36}}"#),
                json_record(2, r#"{"id": 2, "customer": {"name": "Bo", "age": 52}}"#),
            ],
        )
        .expect("flatten materializes");
        let batch = &materialized.batch;

        assert_eq!(
            batch_field_names(batch),
            ["id", "customer", "customer_name", "customer_age"]
        );
        assert_eq!(
            schema_types(batch),
            vec![
                DataType::Int64,
                DataType::Utf8,
                DataType::Utf8,
                DataType::Int64
            ]
        );
        assert_eq!(strings(batch, 1).value(0), r#"{"name":"Ada","age":36}"#);
        assert_eq!(strings(batch, 2).value(0), "Ada");
        assert_eq!(strings(batch, 2).value(1), "Bo");
        assert_eq!(ints(batch, 3).value(0), 36);
        assert_eq!(ints(batch, 3).value(1), 52);
        assert_eq!(
            materialized.schema_decision["transform"],
            json!({
                "flatten": {"customer.name": "customer_name", "customer.age": "customer_age"}
            })
        );
    }

    #[test]
    fn flatten_extraction_yields_null_for_missing_null_and_non_object_steps() {
        // The extraction table of ADR-0041: a missing leaf key, a null
        // parent, a non-object intermediate, a null leaf, and an absent
        // parent all yield null — never a rejection — while the parent
        // column keeps its own materialization.
        let directive = transformed_inferred_directive("flatten: {customer.name: customer_name}");
        let materialized = from_json_columns(
            &directive,
            names(&["id", "customer"]),
            vec![
                json_record(1, r#"{"id": 1, "customer": {"name": "Ada"}}"#),
                json_record(2, r#"{"id": 2, "customer": {"vip": true}}"#),
                json_record(3, r#"{"id": 3, "customer": null}"#),
                json_record(4, r#"{"id": 4, "customer": "opaque"}"#),
                json_record(5, r#"{"id": 5, "customer": {"name": null}}"#),
                json_record(6, r#"{"id": 6}"#),
            ],
        )
        .expect("extraction is total and never rejects");
        let batch = &materialized.batch;

        assert!(materialized.rejected.is_empty());
        assert_eq!(batch.num_rows(), 6);
        let flattened = strings(batch, 2);
        assert_eq!(flattened.value(0), "Ada");
        for row in 1..6 {
            assert!(flattened.is_null(row), "row {row} extracts to null");
        }
        // The parent column is untouched by extraction.
        let parents = strings(batch, 1);
        assert_eq!(parents.value(1), r#"{"vip":true}"#);
        assert!(parents.is_null(2));
        assert_eq!(parents.value(3), "opaque");
    }

    #[test]
    fn flatten_columns_widen_mixed_leaf_types_and_default_all_null_paths_to_text() {
        // Extracted scalars feed the same inference lattice as top-level
        // values: int64 and float64 widen to float64, disagreement falls
        // back to text, and a path that never yields a value widens to utf8
        // exactly like an all-absent field.
        let directive = transformed_inferred_directive("flatten: {m.a: a, m.b: b, m.c: c}");
        let materialized = from_json_columns(
            &directive,
            names(&["m"]),
            vec![
                json_record(1, r#"{"m": {"a": 1, "b": 7}}"#),
                json_record(2, r#"{"m": {"a": 2.5, "b": "seven"}}"#),
            ],
        )
        .expect("mixed flatten types widen");
        let batch = &materialized.batch;

        assert_eq!(
            schema_types(batch),
            vec![
                DataType::Utf8,
                DataType::Float64,
                DataType::Utf8,
                DataType::Utf8
            ]
        );
        assert_eq!(floats(batch, 1).value(0), 1.0);
        assert_eq!(floats(batch, 1).value(1), 2.5);
        assert_eq!(strings(batch, 2).value(0), "7");
        assert_eq!(strings(batch, 2).value(1), "seven");
        assert!(strings(batch, 3).is_null(0));
        assert!(strings(batch, 3).is_null(1));
        assert_eq!(
            materialized.schema_decision["fields"][3],
            json!({"name": "c", "type": "utf8", "nullable": true})
        );
    }

    #[test]
    fn flatten_materializes_object_and_array_leaves_as_compact_json_text() {
        // An object or array leaf stays JSON text, and a deeper path on the
        // same parent coexists as its own typed column (ADR-0041).
        let directive = transformed_inferred_directive(
            "flatten: {customer.address: address, customer.address.city: city, customer.tags: tags}",
        );
        let materialized = from_json_columns(
            &directive,
            names(&["customer"]),
            vec![json_record(
                1,
                r#"{"customer": {"address": {"city": "Taipei", "zip": "100"}, "tags": [1, 2]}}"#,
            )],
        )
        .expect("structured leaves materialize as text");
        let batch = &materialized.batch;

        assert_eq!(
            batch_field_names(batch),
            ["customer", "address", "city", "tags"]
        );
        assert_eq!(
            strings(batch, 1).value(0),
            r#"{"city":"Taipei","zip":"100"}"#
        );
        assert_eq!(strings(batch, 2).value(0), "Taipei");
        assert_eq!(strings(batch, 3).value(0), "[1,2]");
    }

    #[test]
    fn select_places_flatten_outputs_and_rename_maps_them() {
        // Flatten outputs are ordinary fields to the later steps: select
        // places them anywhere in the dataset order and rename maps them,
        // with select entries and rename keys speaking the output name.
        let directive = transformed_inferred_directive(
            "flatten: {customer.name: customer_name}\n\
             select: [customer_name, id]\n\
             rename: {customer_name: contact}",
        );
        let materialized = from_json_columns(
            &directive,
            names(&["id", "customer"]),
            vec![json_record(1, r#"{"id": 1, "customer": {"name": "Ada"}}"#)],
        )
        .expect("flatten output selects and renames");
        let batch = &materialized.batch;

        assert_eq!(batch_field_names(batch), ["contact", "id"]);
        assert_eq!(strings(batch, 0).value(0), "Ada");
        assert_eq!(ints(batch, 1).value(0), 1);
        assert_eq!(
            materialized.schema_decision["transform"],
            json!({
                "flatten": {"customer.name": "customer_name"},
                "select": ["customer_name", "id"],
                "rename": {"customer_name": "contact"}
            })
        );
    }

    #[test]
    fn flatten_paths_with_unobserved_first_segments_are_unknown_transform_fields() {
        // The first segment resolves against the batch-wide observed source
        // fields; a miss reports the full path as the user wrote it. Deeper
        // segments are never checked here — their absence is a null value.
        let directive = transformed_inferred_directive("flatten: {ghost.name: contact}");
        let error = from_json_columns(
            &directive,
            names(&["id"]),
            vec![json_record(1, r#"{"id": 1}"#)],
        )
        .err()
        .expect("unobserved first segment rejected");

        assert_eq!(error.failure.code, "unknown_transform_field");
        assert_eq!(
            error.failure.message,
            "transform selects, renames, or flattens fields absent from the observed source shape: ghost.name"
        );
        assert_eq!(
            error.schema_decision.expect("decision")["transform"],
            json!({ "flatten": {"ghost.name": "contact"} })
        );
    }

    #[test]
    fn flatten_outputs_may_never_shadow_observed_fields() {
        // A flatten output equal to an observed source field name would turn
        // the post-flatten namespace ambiguous, so it fails — even when a
        // select list would drop the shadowed field (ADR-0041).
        for transform_yaml in [
            "flatten: {customer.name: id}",
            "flatten: {customer.name: id}\nselect: [id]",
        ] {
            let directive = transformed_inferred_directive(transform_yaml);
            let error = from_json_columns(
                &directive,
                names(&["id", "customer"]),
                vec![json_record(1, r#"{"id": 1, "customer": {"name": "Ada"}}"#)],
            )
            .err()
            .expect("shadowing flatten output rejected");

            assert_eq!(
                error.failure.code, "transform_name_collision",
                "code for {transform_yaml:?}"
            );
            assert_eq!(
                error.failure.message,
                "transform flatten collides on dataset field \"id\": \
                 source path customer.name shadows an observed source field"
            );
        }
    }

    #[test]
    fn pinned_flatten_outputs_reject_misfits_with_the_declared_path() {
        // Strictness composes through the pin (ADR-0035): a pinned flatten
        // output whose extracted value misfits rejects that record, naming
        // the dataset field and the declared source path, with the record
        // content staying the original source shape.
        let directive = transformed_pinned_directive(
            "version: 1\n\
             fields:\n\
             - name: customer\n\
             \x20 type: utf8\n\
             - name: customer_name\n\
             \x20 type: int64\n",
            DriftPolicy::Fail,
            transform("flatten: {customer.name: customer_name}"),
            SchemaOverrides::none(),
        );
        let materialized = from_json_columns(
            &directive,
            names(&["customer"]),
            vec![
                json_record(1, r#"{"customer": {"name": 7}}"#),
                json_record(2, r#"{"customer": {"name": "Ada"}}"#),
            ],
        )
        .expect("pinned misfits reject records, not the load");
        let batch = &materialized.batch;

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(ints(batch, 1).value(0), 7);
        assert_eq!(materialized.rejected.len(), 1);
        let rejected = &materialized.rejected[0];
        assert_eq!(rejected.line, 2);
        assert_eq!(rejected.code, "type_coercion_failed");
        assert_eq!(rejected.field.as_deref(), Some("customer_name"));
        assert_eq!(rejected.source_field.as_deref(), Some("customer.name"));
        assert_eq!(
            rejected.message,
            "value \"Ada\" does not fit pinned type int64 for field \"customer_name\""
        );
        assert_eq!(rejected.record, json!({ "customer": {"name": "Ada"} }));
    }

    #[test]
    fn bootstrap_pins_record_flatten_outputs_in_dataset_order() {
        // The first pin-requesting load records flatten outputs like any
        // dataset field: by output name, with their inferred types, in
        // dataset order (ADR-0033, ADR-0041).
        let directive = SchemaDirective::PinInferred {
            pinned_path: "customers.schema.yml".to_string(),
            transform: transform("flatten: {customer.name: customer_name}"),
            overrides: SchemaOverrides::none(),
        };
        let materialized = from_json_columns(
            &directive,
            names(&["id", "customer"]),
            vec![json_record(1, r#"{"id": 1, "customer": {"name": "Ada"}}"#)],
        )
        .expect("bootstrap materializes");

        assert_eq!(
            materialized.pinned_schema_write.expect("new pin").yaml,
            "version: 1\n\
             fields:\n\
             - name: id\n\
             \x20 type: int64\n\
             \x20 nullable: true\n\
             - name: customer\n\
             \x20 type: utf8\n\
             \x20 nullable: true\n\
             - name: customer_name\n\
             \x20 type: utf8\n\
             \x20 nullable: true\n"
        );
    }

    #[test]
    fn a_declared_flatten_output_never_triggers_missing_field_drift() {
        // A declared flatten output always exists in the post-transform
        // shape — its values may be all null — so a batch where the path
        // never yields a value is not missing-field drift (ADR-0041).
        let directive = transformed_pinned_directive(
            "version: 1\n\
             fields:\n\
             - name: customer\n\
             \x20 type: utf8\n\
             - name: customer_name\n\
             \x20 type: utf8\n",
            DriftPolicy::Fail,
            transform("flatten: {customer.name: customer_name}"),
            SchemaOverrides::none(),
        );
        let materialized = from_json_columns(
            &directive,
            names(&["customer"]),
            vec![json_record(1, r#"{"customer": {"vip": true}}"#)],
        )
        .expect("an all-null flatten output is not drift");
        let batch = &materialized.batch;

        assert_eq!(materialized.schema_decision["drift_status"], "none");
        assert_eq!(batch_field_names(batch), ["customer", "customer_name"]);
        assert!(strings(batch, 1).is_null(0));
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

    fn timestamps(batch: &RecordBatch, index: usize) -> &TimestampMicrosecondArray {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("microsecond timestamp column")
    }

    fn decimals(batch: &RecordBatch, index: usize) -> &Decimal128Array {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal128 column")
    }
}
