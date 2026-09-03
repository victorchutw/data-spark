//! The SQL Server type mapping (ADR-0062): the created shape a dataset
//! schema takes as a `CREATE TABLE` statement ([`create_table_ddl`]), and
//! the encoding of one chunk's records into the tiberius bulk rows every
//! live load mode consumes ([`BulkRowPlan`], ADR-0064). Pure and offline —
//! nothing here opens a connection, and nothing here preflights the
//! driver's representability caps: an over-long `NVARCHAR` value reaches
//! the wire and fails there as a chunk write failure. The one value the
//! encoder refuses itself is a timestamp outside the `DATETIME2` year
//! range 0001–9999. The lower bound is forced — tiberius asserts on a
//! pre-0001 day count instead of reporting it, and a panic is not the
//! write failure ADR-0062 assigns that sliver; the upper bound is the
//! same rule applied symmetrically, so the refused range is exactly the
//! sliver ADR-0062 enumerates rather than the driver's own limit, and
//! every out-of-range value fails with one message.

use crate::LoadFailure;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Float64Array, Int64Array, RecordBatch,
    StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use std::borrow::Cow;
use tiberius::numeric::Numeric;
use tiberius::time::{Date, DateTime2, Time};
use tiberius::{ColumnData, TokenRow};

/// Every failure this module raises is a write-phase failure: the dataset
/// schema and the load rules were honored, and the destination side could
/// not take the shape or the value.
fn write_failure(message: String) -> LoadFailure {
    LoadFailure {
        code: "destination_write_failed",
        message,
    }
}

/// Renders the created shape of a dataset schema (ADR-0062) as one
/// `CREATE TABLE [schema].[table] (...)` statement.
pub(crate) fn create_table_ddl(
    dataset: &Schema,
    schema_name: &str,
    table_name: &str,
) -> Result<String, LoadFailure> {
    if dataset.fields().is_empty() {
        return Err(write_failure(format!(
            "cannot create SQL Server table {}.{}: the dataset schema has no fields",
            quote_identifier(schema_name),
            quote_identifier(table_name)
        )));
    }
    let columns = dataset
        .fields()
        .iter()
        .map(|field| column_ddl(field))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!(
        "CREATE TABLE {}.{} ({})",
        quote_identifier(schema_name),
        quote_identifier(table_name),
        columns.join(", ")
    ))
}

/// One column of the created shape: the bracket-quoted field name, its
/// exact-fit column type, and the nullability the field declares.
fn column_ddl(field: &Field) -> Result<String, LoadFailure> {
    let column_type = ColumnType::from_arrow(field)?;
    let nullability = if field.is_nullable() {
        "NULL"
    } else {
        "NOT NULL"
    };
    Ok(format!(
        "{} {} {nullability}",
        quote_identifier(field.name()),
        column_type.ddl()
    ))
}

/// Bracket-quotes a T-SQL identifier, doubling any closing bracket the name
/// itself contains — the only character bracket quoting has to escape.
fn quote_identifier(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

/// How one chunk of a dataset becomes tiberius bulk rows for one destination
/// table: every table column, in table column order, reading exactly one
/// dataset field. The order is load-bearing — the TDS bulk path carries no
/// column list, so rows ride in the order the server's metadata states, and
/// same-typed columns in the wrong order would land transposed without an
/// error (#115). A plan is built once per write session and encodes every
/// chunk of the session through [`BulkRowPlan::rows`].
///
/// This slice plans full-schema rows: the table columns and the dataset
/// fields must match one-to-one by exact name. Extra destination columns —
/// the nullable, `IDENTITY`, or defaulted columns ADR-0065 admits — need
/// the introspected column type to choose their placeholder, so they join
/// the plan with the Accept Family slices. The Accept Family validates an
/// existing table's shape before a plan is built, listing every violation
/// as `incompatible_destination_table`; a mismatch caught here is
/// therefore an invariant breach between two already-validated shapes,
/// and it fails loudly rather than being reported as table validation.
#[derive(Debug)]
pub(crate) struct BulkRowPlan {
    dataset: Schema,
    columns: Vec<PlannedColumn>,
}

/// One table column of a [`BulkRowPlan`]: which chunk column it reads and
/// the column type that fixes the wire encoding.
#[derive(Debug)]
struct PlannedColumn {
    name: String,
    field_index: usize,
    column_type: ColumnType,
}

impl BulkRowPlan {
    /// Plans the emission order for `table_columns` — the destination
    /// table's column names in table order — against the dataset schema.
    /// Every table column must name a dataset field exactly once, and every
    /// dataset field must have a table column; anything else is a write
    /// failure naming the offending column, never a silently partial row.
    pub(crate) fn new(dataset: &Schema, table_columns: &[String]) -> Result<Self, LoadFailure> {
        let mut planned = vec![false; dataset.fields().len()];
        let mut columns = Vec::with_capacity(table_columns.len());
        for name in table_columns {
            let field_index = dataset.index_of(name).map_err(|_| {
                write_failure(format!(
                    "SQL Server table column {name} has no dataset field of that name"
                ))
            })?;
            if std::mem::replace(&mut planned[field_index], true) {
                return Err(write_failure(format!(
                    "SQL Server table column {name} is listed more than once"
                )));
            }
            columns.push(PlannedColumn {
                name: name.clone(),
                field_index,
                column_type: ColumnType::from_arrow(dataset.field(field_index))?,
            });
        }
        if let Some(index) = planned.iter().position(|planned| !planned) {
            return Err(write_failure(format!(
                "dataset field {} has no SQL Server table column",
                dataset.field(index).name()
            )));
        }
        Ok(BulkRowPlan {
            dataset: dataset.clone(),
            columns,
        })
    }

    /// Encodes one chunk as bulk rows: one [`TokenRow`] per record, in
    /// record order, each holding the planned columns in table order. Values
    /// borrow from the chunk, so the rows live no longer than it. The chunk
    /// must carry the planned dataset schema; a differing chunk fails before
    /// any row is produced.
    pub(crate) fn rows<'a>(&self, batch: &'a RecordBatch) -> Result<BulkRows<'a>, LoadFailure> {
        if batch.schema().fields() != self.dataset.fields() {
            return Err(write_failure(
                "chunk schema differs from the dataset schema the SQL Server bulk rows were \
                 planned for"
                    .to_string(),
            ));
        }
        let readers = self
            .columns
            .iter()
            .map(|column| ColumnReader::new(column, batch.column(column.field_index)))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(BulkRows {
            readers,
            next: 0,
            len: batch.num_rows(),
        })
    }
}

/// The bulk rows of one chunk (see [`BulkRowPlan::rows`]). A record whose
/// value the wire cannot carry — a timestamp outside the `DATETIME2` range
/// — yields the write failure in its place and ends the rows, so a chunk
/// is either fully encodable or fails at the first offending record.
pub(crate) struct BulkRows<'a> {
    readers: Vec<ColumnReader<'a>>,
    next: usize,
    len: usize,
}

impl<'a> Iterator for BulkRows<'a> {
    type Item = Result<TokenRow<'a>, LoadFailure>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.len {
            return None;
        }
        let record = self.next;
        self.next += 1;
        let mut row = TokenRow::with_capacity(self.readers.len());
        for reader in &self.readers {
            match reader.value(record) {
                Ok(value) => row.push(value),
                Err(failure) => {
                    self.next = self.len;
                    return Some(Err(failure));
                }
            }
        }
        Some(Ok(row))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len - self.next;
        (remaining, Some(remaining))
    }
}

/// One planned column bound to its chunk column: the typed Arrow array the
/// values are read from, resolved once per chunk so per-record encoding
/// never downcasts.
enum ColumnReader<'a> {
    BigInt(&'a Int64Array),
    Float53(&'a Float64Array),
    Bit(&'a BooleanArray),
    NvarcharMax(&'a StringArray),
    DateTime2Micros {
        name: String,
        array: &'a TimestampMicrosecondArray,
    },
    Numeric {
        scale: u8,
        array: &'a Decimal128Array,
    },
}

impl<'a> ColumnReader<'a> {
    fn new(column: &PlannedColumn, array: &'a ArrayRef) -> Result<Self, LoadFailure> {
        fn typed<'a, T: 'static>(
            column: &PlannedColumn,
            array: &'a ArrayRef,
        ) -> Result<&'a T, LoadFailure> {
            array.as_any().downcast_ref::<T>().ok_or_else(|| {
                write_failure(format!(
                    "chunk column {} is not the {} array its dataset field materializes as",
                    column.name,
                    column.column_type.ddl()
                ))
            })
        }
        Ok(match column.column_type {
            ColumnType::BigInt => ColumnReader::BigInt(typed(column, array)?),
            ColumnType::Float53 => ColumnReader::Float53(typed(column, array)?),
            ColumnType::Bit => ColumnReader::Bit(typed(column, array)?),
            ColumnType::NvarcharMax => ColumnReader::NvarcharMax(typed(column, array)?),
            ColumnType::DateTime2Micros => ColumnReader::DateTime2Micros {
                name: column.name.clone(),
                array: typed(column, array)?,
            },
            ColumnType::Decimal { scale, .. } => ColumnReader::Numeric {
                scale,
                array: typed(column, array)?,
            },
        })
    }

    /// The wire value of one record: `None` inside the variant for a null
    /// slot — an empty string is a present value, never a null — and the
    /// exact value otherwise.
    fn value(&self, record: usize) -> Result<ColumnData<'a>, LoadFailure> {
        Ok(match self {
            &ColumnReader::BigInt(array) => {
                ColumnData::I64(array.is_valid(record).then(|| array.value(record)))
            }
            &ColumnReader::Float53(array) => {
                ColumnData::F64(array.is_valid(record).then(|| array.value(record)))
            }
            &ColumnReader::Bit(array) => {
                ColumnData::Bit(array.is_valid(record).then(|| array.value(record)))
            }
            &ColumnReader::NvarcharMax(array) => ColumnData::String(
                array
                    .is_valid(record)
                    .then(|| Cow::Borrowed(array.value(record))),
            ),
            ColumnReader::DateTime2Micros { name, array } => {
                ColumnData::DateTime2(if array.is_valid(record) {
                    let micros = array.value(record);
                    Some(
                        datetime2_from_micros(micros)
                            .ok_or_else(|| datetime2_out_of_range(name, record, micros))?,
                    )
                } else {
                    None
                })
            }
            // The scale is the dataset field's declared scale, which the
            // created shape carries verbatim and the Accept Family
            // (ADR-0065) requires of an existing column — tiberius encodes
            // a value only at the column's own scale.
            &ColumnReader::Numeric { scale, array } => ColumnData::Numeric(
                array
                    .is_valid(record)
                    .then(|| Numeric::new_with_scale(array.value(record), scale)),
            ),
        })
    }
}

const MICROS_PER_DAY: i64 = 86_400_000_000;
/// Days from 0001-01-01, the `DATETIME2` day origin, to 1970-01-01, the
/// origin of Arrow's microsecond timestamps.
const DAYS_FROM_DATETIME2_ORIGIN_TO_UNIX_EPOCH: i64 = 719_162;
/// 9999-12-31 counted from 0001-01-01: the last day `DATETIME2` holds.
const LAST_DATETIME2_DAY: i64 = 3_652_058;

/// Converts an Arrow microsecond timestamp — a wall-clock reading, or an
/// instant already normalized to UTC (ADR-0043) — to the `DATETIME2(6)`
/// wire value carrying the same microsecond exactly: the day count from
/// 0001-01-01 plus the microseconds into that day at scale 6, so tiberius
/// writes the value as-is with no rescaling. `None` when the day lies
/// outside 0001-01-01..=9999-12-31, the `DATETIME2` range: SQL Server
/// cannot store such a value, and tiberius asserts on a negative day
/// count rather than reporting it, so the whole range is checked here —
/// the upper bound by the same rule, although the driver itself would
/// only assert from day 2^24 (year 45,941) on and let the server reject
/// the years between.
fn datetime2_from_micros(micros_since_epoch: i64) -> Option<DateTime2> {
    let day =
        micros_since_epoch.div_euclid(MICROS_PER_DAY) + DAYS_FROM_DATETIME2_ORIGIN_TO_UNIX_EPOCH;
    if !(0..=LAST_DATETIME2_DAY).contains(&day) {
        return None;
    }
    let micros_of_day = micros_since_epoch.rem_euclid(MICROS_PER_DAY);
    Some(DateTime2::new(
        Date::new(day as u32),
        Time::new(micros_of_day as u64, 6),
    ))
}

/// The write failure for a timestamp outside the `DATETIME2` range: the
/// second representability sliver of ADR-0062, a write failure rather than
/// a Rejected Record because the record satisfies the dataset schema. The
/// value is spelled in the dataset's own timestamp notation with all six
/// fractional digits; a value beyond what a calendar can even name falls
/// back to its raw microsecond count.
fn datetime2_out_of_range(column: &str, record: usize, micros_since_epoch: i64) -> LoadFailure {
    use chrono::{Datelike, Timelike};

    let rendered = chrono::DateTime::from_timestamp_micros(micros_since_epoch)
        .map(|instant| {
            let value = instant.naive_utc();
            format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}",
                value.year(),
                value.month(),
                value.day(),
                value.hour(),
                value.minute(),
                value.second(),
                value.nanosecond() / 1_000
            )
        })
        .unwrap_or_else(|| format!("{micros_since_epoch} microseconds since 1970-01-01"));
    write_failure(format!(
        "record {record} of the chunk holds timestamp {rendered} in dataset field {column}, \
         outside the SQL Server DATETIME2 range 0001-01-01 to 9999-12-31"
    ))
}

/// The exact-fit SQL Server column type of one dataset field type
/// (ADR-0062): the narrowest type that holds every value of the field
/// exactly. Both timestamp types land as `DATETIME2(6)` — the instant
/// type's UTC normalization already happened at parse (ADR-0043), so the
/// column holds the UTC wall-clock reading — and a declared decimal keeps
/// its precision and scale verbatim, the declaration cap of 38 (ADR-0044)
/// being SQL Server's own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColumnType {
    BigInt,
    Float53,
    Bit,
    NvarcharMax,
    DateTime2Micros,
    Decimal { precision: u8, scale: u8 },
}

impl ColumnType {
    /// Maps one materialized dataset field to its column type. The dataset
    /// schema only ever materializes the seven field types of
    /// `schema::FieldType`, so an Arrow type outside that vocabulary is a
    /// contract breach surfaced as a write failure naming the field rather
    /// than a silent approximate mapping.
    fn from_arrow(field: &Field) -> Result<ColumnType, LoadFailure> {
        let unmapped = || {
            write_failure(format!(
                "dataset field {} has no SQL Server column type mapping: {}",
                field.name(),
                field.data_type()
            ))
        };
        match field.data_type() {
            DataType::Int64 => Ok(ColumnType::BigInt),
            DataType::Float64 => Ok(ColumnType::Float53),
            DataType::Boolean => Ok(ColumnType::Bit),
            DataType::Utf8 => Ok(ColumnType::NvarcharMax),
            DataType::Timestamp(TimeUnit::Microsecond, None) => Ok(ColumnType::DateTime2Micros),
            DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) if zone.as_ref() == "UTC" => {
                Ok(ColumnType::DateTime2Micros)
            }
            DataType::Decimal128(precision, scale) => {
                let precision = *precision;
                let scale = u8::try_from(*scale).map_err(|_| unmapped())?;
                if (1..=38).contains(&precision) && scale <= precision {
                    Ok(ColumnType::Decimal { precision, scale })
                } else {
                    Err(unmapped())
                }
            }
            _ => Err(unmapped()),
        }
    }

    fn ddl(self) -> String {
        match self {
            ColumnType::BigInt => "BIGINT".to_string(),
            ColumnType::Float53 => "FLOAT(53)".to_string(),
            ColumnType::Bit => "BIT".to_string(),
            ColumnType::NvarcharMax => "NVARCHAR(MAX)".to_string(),
            ColumnType::DateTime2Micros => "DATETIME2(6)".to_string(),
            ColumnType::Decimal { precision, scale } => format!("DECIMAL({precision},{scale})"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        BooleanArray, Decimal128Array, Float64Array, Int64Array, RecordBatch, StringArray,
        TimestampMicrosecondArray,
    };
    use arrow_schema::{DataType, Field, TimeUnit};
    use std::sync::Arc;

    // ---- Created-shape DDL ----

    #[test]
    fn ddl_renders_one_required_bigint_column_in_a_bracket_quoted_table() {
        let dataset = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

        let ddl = create_table_ddl(&dataset, "dbo", "customers").expect("mapped schema");

        assert_eq!(ddl, "CREATE TABLE [dbo].[customers] ([id] BIGINT NOT NULL)");
    }

    /// Every dataset field type beside the exact-fit column type ADR-0062
    /// assigns it; the decimal cases span the declaration range's edges.
    fn type_mapping() -> Vec<(DataType, &'static str)> {
        vec![
            (DataType::Int64, "BIGINT"),
            (DataType::Float64, "FLOAT(53)"),
            (DataType::Boolean, "BIT"),
            (DataType::Utf8, "NVARCHAR(MAX)"),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                "DATETIME2(6)",
            ),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                "DATETIME2(6)",
            ),
            (DataType::Decimal128(18, 4), "DECIMAL(18,4)"),
            (DataType::Decimal128(1, 0), "DECIMAL(1,0)"),
            (DataType::Decimal128(38, 38), "DECIMAL(38,38)"),
        ]
    }

    #[test]
    fn ddl_renders_every_type_and_nullability_combination_exactly() {
        for (data_type, column_type) in type_mapping() {
            for (nullable, nullability) in [(true, "NULL"), (false, "NOT NULL")] {
                let dataset = Schema::new(vec![Field::new("v", data_type.clone(), nullable)]);

                let ddl = create_table_ddl(&dataset, "dbo", "t").expect("mapped schema");

                assert_eq!(
                    ddl,
                    format!("CREATE TABLE [dbo].[t] ([v] {column_type} {nullability})"),
                    "{data_type} nullable={nullable}"
                );
            }
        }
    }

    #[test]
    fn ddl_keeps_the_dataset_field_order() {
        let dataset = Schema::new(vec![
            Field::new("amount", DataType::Decimal128(10, 2), true),
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let ddl = create_table_ddl(&dataset, "sales", "orders").expect("mapped schema");

        assert_eq!(
            ddl,
            "CREATE TABLE [sales].[orders] ([amount] DECIMAL(10,2) NULL, [id] BIGINT NOT NULL, \
             [name] NVARCHAR(MAX) NULL)"
        );
    }

    #[test]
    fn ddl_doubles_closing_brackets_inside_every_identifier() {
        let dataset = Schema::new(vec![Field::new("odd]name", DataType::Utf8, true)]);

        let ddl = create_table_ddl(&dataset, "sch]ema", "tab]le").expect("mapped schema");

        assert_eq!(
            ddl,
            "CREATE TABLE [sch]]ema].[tab]]le] ([odd]]name] NVARCHAR(MAX) NULL)"
        );
    }

    #[test]
    fn ddl_never_carries_a_collate_clause() {
        let fields = type_mapping()
            .into_iter()
            .enumerate()
            .map(|(index, (data_type, _))| Field::new(format!("c{index}"), data_type, true))
            .collect::<Vec<_>>();

        let ddl = create_table_ddl(&Schema::new(fields), "dbo", "t").expect("mapped schema");

        assert!(
            !ddl.to_ascii_uppercase().contains("COLLATE"),
            "columns inherit the database collation: {ddl}"
        );
    }

    #[test]
    fn ddl_fails_for_a_schema_without_fields() {
        let error = create_table_ddl(&Schema::empty(), "dbo", "t").expect_err("no columns");

        assert_eq!(error.code, "destination_write_failed");
        assert_eq!(
            error.message,
            "cannot create SQL Server table [dbo].[t]: the dataset schema has no fields"
        );
    }

    #[test]
    fn ddl_fails_for_an_arrow_type_outside_the_dataset_vocabulary() {
        let unmapped = [
            DataType::Int32,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, Some("+08:00".into())),
            DataType::Decimal128(39, 0),
            DataType::Decimal128(10, -1),
            DataType::Decimal128(0, 0),
            DataType::Decimal128(5, 6),
            DataType::Decimal256(10, 2),
            DataType::LargeUtf8,
        ];
        for data_type in unmapped {
            let dataset = Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("v", data_type.clone(), true),
            ]);

            let error = create_table_ddl(&dataset, "dbo", "t").expect_err("unmapped type");

            assert_eq!(error.code, "destination_write_failed", "{data_type}");
            assert_eq!(
                error.message,
                format!("dataset field v has no SQL Server column type mapping: {data_type}"),
            );
        }
    }

    // ---- Bulk rows ----

    fn names(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    /// Materializes every row of a chunk under a plan, each row as its
    /// column values in emission order.
    fn rows_of(plan: &BulkRowPlan, batch: &RecordBatch) -> Vec<Vec<ColumnData<'static>>> {
        plan.rows(batch)
            .expect("chunk matches the plan")
            .map(|row| {
                row.expect("encodable record")
                    .into_iter()
                    .map(owned)
                    .collect()
            })
            .collect()
    }

    /// Detaches a row value from the chunk it borrows so rows can outlive
    /// the batch inside assertions.
    fn owned(value: ColumnData<'_>) -> ColumnData<'static> {
        match value {
            ColumnData::String(text) => {
                ColumnData::String(text.map(|text| text.into_owned().into()))
            }
            ColumnData::I64(v) => ColumnData::I64(v),
            ColumnData::F64(v) => ColumnData::F64(v),
            ColumnData::Bit(v) => ColumnData::Bit(v),
            ColumnData::Numeric(v) => ColumnData::Numeric(v),
            ColumnData::DateTime2(v) => ColumnData::DateTime2(v),
            other => panic!("the mapping never emits {other:?}"),
        }
    }

    /// The dataset schema of the row tests: one field per column type, in
    /// an order the table orders differently.
    fn every_type_dataset() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(18, 4), true),
            Field::new("active", DataType::Boolean, true),
            Field::new("ratio", DataType::Float64, true),
            Field::new(
                "seen_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ])
    }

    /// Epoch microseconds of a UTC wall-clock reading, the value both
    /// timestamp columns store (ADR-0043).
    fn micros(year: i32, month: u32, day: u32, h: u32, m: u32, s: u32, micro: u32) -> i64 {
        chrono::NaiveDate::from_ymd_opt(year, month, day)
            .expect("calendar date")
            .and_hms_micro_opt(h, m, s, micro)
            .expect("wall-clock time")
            .and_utc()
            .timestamp_micros()
    }

    /// The `DATETIME2(6)` wire value of a day count from 0001-01-01 and the
    /// microseconds into that day — the literal shape the tests expect.
    fn datetime2(days: u32, micros_of_day: u64) -> ColumnData<'static> {
        ColumnData::DateTime2(Some(DateTime2::new(
            Date::new(days),
            Time::new(micros_of_day, 6),
        )))
    }

    fn numeric(value: i128, scale: u8) -> ColumnData<'static> {
        ColumnData::Numeric(Some(Numeric::new_with_scale(value, scale)))
    }

    fn text(value: &str) -> ColumnData<'static> {
        ColumnData::String(Some(Cow::Owned(value.to_string())))
    }

    #[test]
    fn rows_carry_int64_values_and_nulls_one_token_row_per_record() {
        let dataset = Schema::new(vec![Field::new("id", DataType::Int64, true)]);
        let batch = RecordBatch::try_new(
            Arc::new(dataset.clone()),
            vec![Arc::new(Int64Array::from(vec![
                Some(1),
                None,
                Some(i64::MAX),
            ]))],
        )
        .expect("batch");
        let plan = BulkRowPlan::new(&dataset, &names(&["id"])).expect("plan");

        assert_eq!(
            rows_of(&plan, &batch),
            vec![
                vec![ColumnData::I64(Some(1))],
                vec![ColumnData::I64(None)],
                vec![ColumnData::I64(Some(i64::MAX))],
            ]
        );
    }

    #[test]
    fn rows_emit_every_type_in_table_column_order_not_dataset_order() {
        let dataset = every_type_dataset();
        let batch = RecordBatch::try_new(
            Arc::new(dataset.clone()),
            vec![
                Arc::new(Int64Array::from(vec![Some(7), Some(8), Some(9)])),
                Arc::new(StringArray::from(vec![Some("Ada"), None, Some("")])),
                Arc::new(
                    Decimal128Array::from(vec![Some(-1), None, Some(0)])
                        .with_precision_and_scale(18, 4)
                        .expect("decimal(18,4)"),
                ),
                Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
                Arc::new(Float64Array::from(vec![Some(0.5), None, Some(0.0)])),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![
                        Some(micros(2024, 2, 29, 12, 34, 56, 1)),
                        None,
                        Some(micros(1970, 1, 1, 0, 0, 0, 0)),
                    ])
                    .with_timezone("UTC"),
                ),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(micros(2001, 9, 9, 1, 46, 40, 999_999)),
                    None,
                    Some(micros(1969, 12, 31, 23, 59, 59, 999_999)),
                ])),
            ],
        )
        .expect("batch");
        let table_order = names(&[
            "created_at",
            "active",
            "id",
            "amount",
            "name",
            "seen_at",
            "ratio",
        ]);
        let plan = BulkRowPlan::new(&dataset, &table_order).expect("plan");

        // 2024-02-29 is day 738,944 from 0001-01-01 (719,162 to the Unix
        // epoch plus 19,782 days); 2001-09-09 is day 730,736.
        assert_eq!(
            rows_of(&plan, &batch),
            vec![
                vec![
                    datetime2(730_736, 6_400_999_999),
                    ColumnData::Bit(Some(true)),
                    ColumnData::I64(Some(7)),
                    numeric(-1, 4),
                    text("Ada"),
                    datetime2(738_944, 45_296_000_001),
                    ColumnData::F64(Some(0.5)),
                ],
                vec![
                    ColumnData::DateTime2(None),
                    ColumnData::Bit(None),
                    ColumnData::I64(Some(8)),
                    ColumnData::Numeric(None),
                    ColumnData::String(None),
                    ColumnData::DateTime2(None),
                    ColumnData::F64(None),
                ],
                vec![
                    datetime2(719_161, 86_399_999_999),
                    ColumnData::Bit(Some(false)),
                    ColumnData::I64(Some(9)),
                    numeric(0, 4),
                    text(""),
                    datetime2(719_162, 0),
                    ColumnData::F64(Some(0.0)),
                ],
            ]
        );
    }

    #[test]
    fn rows_keep_an_empty_string_distinct_from_null() {
        let dataset = Schema::new(vec![Field::new("name", DataType::Utf8, true)]);
        let batch = RecordBatch::try_new(
            Arc::new(dataset.clone()),
            vec![Arc::new(StringArray::from(vec![Some(""), None]))],
        )
        .expect("batch");
        let plan = BulkRowPlan::new(&dataset, &names(&["name"])).expect("plan");

        let rows = rows_of(&plan, &batch);

        assert_eq!(rows[0], vec![text("")]);
        assert_eq!(rows[1], vec![ColumnData::String(None)]);
        assert_ne!(rows[0], rows[1]);
    }

    #[test]
    fn rows_carry_the_numeric_boundaries_exactly() {
        let dataset = Schema::new(vec![
            Field::new("i", DataType::Int64, false),
            Field::new("f", DataType::Float64, false),
            Field::new("d", DataType::Decimal128(38, 0), false),
        ]);
        let max_decimal = 10i128.pow(38) - 1;
        let batch = RecordBatch::try_new(
            Arc::new(dataset.clone()),
            vec![
                Arc::new(Int64Array::from(vec![i64::MAX, i64::MIN, 0])),
                Arc::new(Float64Array::from(vec![
                    f64::MAX,
                    f64::MIN,
                    f64::MIN_POSITIVE,
                ])),
                Arc::new(
                    Decimal128Array::from(vec![max_decimal, -max_decimal, -1])
                        .with_precision_and_scale(38, 0)
                        .expect("decimal(38,0)"),
                ),
            ],
        )
        .expect("batch");
        let plan = BulkRowPlan::new(&dataset, &names(&["i", "f", "d"])).expect("plan");

        assert_eq!(
            rows_of(&plan, &batch),
            vec![
                vec![
                    ColumnData::I64(Some(i64::MAX)),
                    ColumnData::F64(Some(f64::MAX)),
                    numeric(max_decimal, 0),
                ],
                vec![
                    ColumnData::I64(Some(i64::MIN)),
                    ColumnData::F64(Some(f64::MIN)),
                    numeric(-max_decimal, 0),
                ],
                vec![
                    ColumnData::I64(Some(0)),
                    ColumnData::F64(Some(f64::MIN_POSITIVE)),
                    numeric(-1, 0),
                ],
            ]
        );
    }

    #[test]
    fn rows_carry_negative_decimals_with_the_declared_scale() {
        let dataset = Schema::new(vec![Field::new("d", DataType::Decimal128(10, 3), false)]);
        let batch = RecordBatch::try_new(
            Arc::new(dataset.clone()),
            vec![Arc::new(
                Decimal128Array::from(vec![-1, -1_000, -9_999_999_999])
                    .with_precision_and_scale(10, 3)
                    .expect("decimal(10,3)"),
            )],
        )
        .expect("batch");
        let plan = BulkRowPlan::new(&dataset, &names(&["d"])).expect("plan");

        let rows = rows_of(&plan, &batch);

        // -0.001, -1.000, -9999999.999: value and scale both preserved.
        assert_eq!(
            rows,
            vec![
                vec![numeric(-1, 3)],
                vec![numeric(-1_000, 3)],
                vec![numeric(-9_999_999_999, 3)],
            ]
        );
        for row in rows {
            match &row[0] {
                ColumnData::Numeric(Some(value)) => assert_eq!(value.scale(), 3),
                other => panic!("expected a numeric, got {other:?}"),
            }
        }
    }

    #[test]
    fn rows_carry_the_datetime2_range_edges_and_microseconds_exactly() {
        let dataset = Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]);
        let batch = RecordBatch::try_new(
            Arc::new(dataset.clone()),
            vec![Arc::new(TimestampMicrosecondArray::from(vec![
                micros(1, 1, 1, 0, 0, 0, 0),
                micros(9999, 12, 31, 23, 59, 59, 999_999),
                micros(2024, 6, 15, 8, 30, 0, 1),
                micros(2024, 6, 15, 8, 30, 0, 999_999),
            ]))],
        )
        .expect("batch");
        let plan = BulkRowPlan::new(&dataset, &names(&["t"])).expect("plan");

        // 2024-06-15 is day 739,051 from 0001-01-01; 08:30:00 is
        // 30,600 seconds into the day.
        assert_eq!(
            rows_of(&plan, &batch),
            vec![
                vec![datetime2(0, 0)],
                vec![datetime2(3_652_058, 86_399_999_999)],
                vec![datetime2(739_051, 30_600_000_001)],
                vec![datetime2(739_051, 30_600_999_999)],
            ]
        );
    }

    /// Cross-checks the direct `DATETIME2` construction against tiberius's
    /// own chrono conversion — the path the spike round-tripped through a
    /// real server — across the range edges, leap days, and both signs of
    /// the Unix epoch: the same day count, and increments scaled 7 → 6 by
    /// exactly ten.
    #[test]
    fn rows_agree_with_the_tiberius_chrono_conversion() {
        use tiberius::IntoSql;

        let samples = [
            (1, 1, 1, 0, 0, 0, 0),
            (1, 1, 1, 0, 0, 0, 1),
            (1600, 2, 29, 23, 59, 59, 999_999),
            (1900, 3, 1, 0, 0, 0, 0),
            (1969, 12, 31, 23, 59, 59, 999_999),
            (1970, 1, 1, 0, 0, 0, 0),
            (2000, 2, 29, 12, 0, 0, 500_000),
            (2038, 1, 19, 3, 14, 8, 0),
            (9999, 12, 31, 0, 0, 0, 0),
            (9999, 12, 31, 23, 59, 59, 999_999),
        ];
        for (year, month, day, h, m, s, micro) in samples {
            let naive = chrono::NaiveDate::from_ymd_opt(year, month, day)
                .expect("calendar date")
                .and_hms_micro_opt(h, m, s, micro)
                .expect("wall-clock time");
            let expected = match naive.into_sql() {
                ColumnData::DateTime2(Some(value)) => value,
                other => panic!("chrono maps to datetime2, got {other:?}"),
            };

            let actual = datetime2_from_micros(naive.and_utc().timestamp_micros())
                .unwrap_or_else(|| panic!("{naive:?} is inside the DATETIME2 range"));

            assert_eq!(
                actual.date().days(),
                expected.date().days(),
                "{naive:?} day"
            );
            assert_eq!(actual.time().scale(), 6, "{naive:?} scale");
            assert_eq!(expected.time().scale(), 7, "tiberius chrono scale");
            assert_eq!(
                actual.time().increments() * 10,
                expected.time().increments(),
                "{naive:?} increments"
            );
        }
    }

    /// The #134 regression seed (ADR-0069 fork rung, #156): the scaled
    /// value `-1` of a `decimal(38,38)` field — `-0.000…01` with 38
    /// fractional digits — becomes a Numeric of value `-1` at scale 38 that
    /// tiberius describes as `numeric(38,38)`, deterministically and
    /// without rounding.
    #[test]
    fn rows_convert_decimal_38_38_scaled_minus_one_to_a_scale_38_numeric() {
        let dataset = Schema::new(vec![Field::new("d", DataType::Decimal128(38, 38), false)]);
        let batch = RecordBatch::try_new(
            Arc::new(dataset.clone()),
            vec![Arc::new(
                Decimal128Array::from(vec![-1])
                    .with_precision_and_scale(38, 38)
                    .expect("decimal(38,38)"),
            )],
        )
        .expect("batch");
        let plan = BulkRowPlan::new(&dataset, &names(&["d"])).expect("plan");

        let first = rows_of(&plan, &batch);
        let second = rows_of(&plan, &batch);

        assert_eq!(first, second, "encoding is deterministic");
        assert_eq!(first, vec![vec![numeric(-1, 38)]]);
        match &first[0][0] {
            ColumnData::Numeric(Some(value)) => {
                assert_eq!(value.value(), -1);
                assert_eq!(value.scale(), 38);
                assert_eq!(
                    value.precision(),
                    38,
                    "numeric(38,38), never numeric(39,38)"
                );
            }
            other => panic!("expected a numeric, got {other:?}"),
        }
    }

    #[test]
    fn rows_fail_instead_of_panicking_for_timestamps_outside_the_datetime2_range() {
        let dataset = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "t",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]);
        // Year 0000 is reachable: `0001-01-01T00:00:00+01:00` normalizes
        // to 0000-12-31T23:00:00Z (ADR-0043); the far ends guard the
        // arithmetic itself.
        let cases = [
            (
                micros(0, 12, 31, 23, 0, 0, 0),
                "record 1 of the chunk holds timestamp 0000-12-31T23:00:00.000000 in dataset \
                 field t, outside the SQL Server DATETIME2 range 0001-01-01 to 9999-12-31",
            ),
            (
                micros(10_000, 1, 1, 0, 0, 0, 1),
                "record 1 of the chunk holds timestamp 10000-01-01T00:00:00.000001 in dataset \
                 field t, outside the SQL Server DATETIME2 range 0001-01-01 to 9999-12-31",
            ),
            (
                i64::MIN,
                "record 1 of the chunk holds timestamp -9223372036854775808 microseconds since \
                 1970-01-01 in dataset field t, outside the SQL Server DATETIME2 range \
                 0001-01-01 to 9999-12-31",
            ),
            (
                i64::MAX,
                "record 1 of the chunk holds timestamp 9223372036854775807 microseconds since \
                 1970-01-01 in dataset field t, outside the SQL Server DATETIME2 range \
                 0001-01-01 to 9999-12-31",
            ),
        ];
        for (value, message) in cases {
            let batch = RecordBatch::try_new(
                Arc::new(dataset.clone()),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                    Arc::new(
                        TimestampMicrosecondArray::from(vec![Some(0), Some(value), Some(0)])
                            .with_timezone("UTC"),
                    ),
                ],
            )
            .expect("batch");
            let plan = BulkRowPlan::new(&dataset, &names(&["id", "t"])).expect("plan");

            let mut rows = plan.rows(&batch).expect("chunk matches the plan");

            let first = rows.next().expect("record 0").expect("record 0 encodes");
            assert_eq!(first.len(), 2);
            let error = rows
                .next()
                .expect("record 1")
                .expect_err("record 1 is out of range");
            assert_eq!(error.code, "destination_write_failed");
            assert_eq!(error.message, message);
            assert!(
                rows.next().is_none(),
                "the failing record ends the chunk's rows"
            );
        }
    }

    #[test]
    fn plan_requires_table_columns_and_dataset_fields_to_match_one_to_one() {
        let dataset = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let cases: [(&[&str], &str); 4] = [
            (
                &["id", "Name"],
                "SQL Server table column Name has no dataset field of that name",
            ),
            (
                &["id", "name", "extra"],
                "SQL Server table column extra has no dataset field of that name",
            ),
            (
                &["id", "name", "id"],
                "SQL Server table column id is listed more than once",
            ),
            (&["name"], "dataset field id has no SQL Server table column"),
        ];
        for (table_columns, message) in cases {
            let error = BulkRowPlan::new(&dataset, &names(table_columns))
                .err()
                .unwrap_or_else(|| panic!("{table_columns:?} must not plan"));

            assert_eq!(error.code, "destination_write_failed");
            assert_eq!(error.message, message);
        }
    }

    #[test]
    fn plan_fails_for_an_arrow_type_outside_the_dataset_vocabulary() {
        let dataset = Schema::new(vec![Field::new("v", DataType::Int32, true)]);

        let error = BulkRowPlan::new(&dataset, &names(&["v"])).expect_err("unmapped type");

        assert_eq!(error.code, "destination_write_failed");
        assert_eq!(
            error.message,
            "dataset field v has no SQL Server column type mapping: Int32"
        );
    }

    #[test]
    fn rows_fail_for_a_chunk_carrying_a_different_schema() {
        let dataset = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let other = Schema::new(vec![Field::new("id", DataType::Int64, true)]);
        let batch = RecordBatch::try_new(
            Arc::new(other),
            vec![Arc::new(Int64Array::from(vec![Some(1)]))],
        )
        .expect("batch");
        let plan = BulkRowPlan::new(&dataset, &names(&["id"])).expect("plan");

        let error = plan.rows(&batch).err().expect("schema mismatch");

        assert_eq!(error.code, "destination_write_failed");
        assert_eq!(
            error.message,
            "chunk schema differs from the dataset schema the SQL Server bulk rows were \
             planned for"
        );
    }

    #[test]
    fn rows_of_an_empty_chunk_are_empty_and_sized_exactly() {
        let dataset = every_type_dataset();
        let batch = RecordBatch::new_empty(Arc::new(dataset.clone()));
        let order = dataset
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        let plan = BulkRowPlan::new(&dataset, &order).expect("plan");

        let rows = plan.rows(&batch).expect("chunk matches the plan");

        assert_eq!(rows.size_hint(), (0, Some(0)));
        assert_eq!(rows.count(), 0);
    }

    #[test]
    fn datetime2_day_constants_match_the_proleptic_gregorian_calendar() {
        let origin = chrono::NaiveDate::from_ymd_opt(1, 1, 1).expect("0001-01-01");
        let unix_epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("1970-01-01");
        let last_day = chrono::NaiveDate::from_ymd_opt(9999, 12, 31).expect("9999-12-31");

        assert_eq!(
            unix_epoch.signed_duration_since(origin).num_days(),
            DAYS_FROM_DATETIME2_ORIGIN_TO_UNIX_EPOCH
        );
        assert_eq!(
            last_day.signed_duration_since(origin).num_days(),
            LAST_DATETIME2_DAY
        );
    }
}
