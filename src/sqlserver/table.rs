//! Offline existing-table validation and catalog decoding (ADR-0065).

use super::{quote_identifier, write_failure, ColumnType};
use crate::{LoadFailure, LoadMode};
use arrow_schema::Schema;
use tiberius::ColumnData;

/// The projection is the decoding contract of `from_catalog_rows`.
/// Resolve aliases to their system type, retaining unknown/CLR types with
/// a LEFT JOIN so no column silently disappears. OBJECT_ID takes a Unicode
/// string containing the bracket-quoted name, so both quoting layers apply.
/// An empty result does not distinguish a missing table from invisible
/// metadata; the live session must decide existence before validating.
pub(crate) fn introspection_query(schema_name: &str, table_name: &str) -> String {
    let object_name = format!(
        "{}.{}",
        quote_identifier(schema_name),
        quote_identifier(table_name)
    )
    .replace('\'', "''");
    format!(
        "SELECT c.name, COALESCE(t.name, TYPE_NAME(c.user_type_id)), \
         c.precision, c.scale, c.max_length, c.is_nullable, c.is_identity, \
         CAST(CASE WHEN c.default_object_id <> 0 THEN 1 ELSE 0 END AS bit) \
         FROM sys.columns AS c \
         LEFT JOIN sys.types AS t ON t.user_type_id = c.system_type_id \
         WHERE c.object_id = OBJECT_ID(N'{object_name}', N'U') \
         ORDER BY c.column_id"
    )
}

/// Catalog columns in ordinal order. Names match dataset fields exactly,
/// as in `BulkRowPlan`; database collation does not change this contract.
#[derive(Debug)]
pub(crate) struct TableShape {
    pub(crate) columns: Vec<TableColumn>,
}

/// SQL Server catalog metadata, retained even for extra/unaccepted types so
/// subsequent bulk planning can choose typed placeholders without a query.
#[derive(Debug)]
pub(crate) struct TableColumn {
    pub(crate) name: String,
    pub(crate) type_name: String,
    pub(crate) precision: u8,
    pub(crate) scale: u8,
    /// Catalog byte length, not character count; -1 means MAX.
    pub(crate) max_length: i16,
    pub(crate) nullable: bool,
    pub(crate) identity: bool,
    pub(crate) has_default: bool,
}

impl TableShape {
    /// Consumes catalog rows (including `tiberius::Row` via IntoIterator)
    /// in query order. Reject malformed/NULL metadata instead of guessing.
    pub(crate) fn from_catalog_rows<R>(
        rows: impl IntoIterator<Item = R>,
    ) -> Result<Self, LoadFailure>
    where
        R: IntoIterator<Item = ColumnData<'static>>,
    {
        let columns = rows.into_iter().map(|row| {
            let cells: Vec<_> = row.into_iter().collect();
            match cells.as_slice() {
                [ColumnData::String(Some(name)), ColumnData::String(Some(type_name)),
                 ColumnData::U8(Some(precision)), ColumnData::U8(Some(scale)),
                 ColumnData::I16(Some(max_length)), ColumnData::Bit(Some(nullable)),
                 ColumnData::Bit(Some(identity)), ColumnData::Bit(Some(has_default))] => Ok(TableColumn {
                    name: name.to_string(), type_name: type_name.to_string(),
                    precision: *precision, scale: *scale, max_length: *max_length,
                    nullable: *nullable, identity: *identity, has_default: *has_default,
                }),
                _ => Err(write_failure("invalid SQL Server catalog column metadata".into())),
            }
        }).collect::<Result<Vec<_>, _>>()?;
        Ok(Self { columns })
    }

    pub(crate) fn validate(
        &self,
        dataset: &Schema,
        mode: LoadMode,
        merge_keys: &[String],
        accept_datetime_rounding: bool,
    ) -> Result<(), LoadFailure> {
        let mut violations = Vec::new();
        for field in dataset.fields() {
            let Some(column) = self
                .columns
                .iter()
                .find(|column| column.name == *field.name())
            else {
                violations.push(format!(
                    "column {}: missing destination column (names must match exactly)",
                    field.name()
                ));
                continue;
            };
            let accepted = match ColumnType::from_arrow(field) {
                Ok(ColumnType::BigInt) => matches!(
                    column.type_name.as_str(),
                    "bigint" | "int" | "smallint" | "tinyint"
                ),
                Ok(ColumnType::Float53) => column.type_name == "float" && column.precision == 53,
                Ok(ColumnType::Bit) => column.type_name == "bit",
                Ok(ColumnType::NvarcharMax) => column.type_name == "nvarchar",
                Ok(ColumnType::Decimal { scale, .. }) => {
                    matches!(column.type_name.as_str(), "decimal" | "numeric")
                        && column.scale == scale
                }
                Ok(ColumnType::DateTime2Micros) => match column.type_name.as_str() {
                    "datetime2" => column.scale >= 6 || accept_datetime_rounding,
                    "datetime" => accept_datetime_rounding,
                    _ => false,
                },
                Err(_) => false,
            };
            if !accepted {
                violations.push(format!("column {}: {} is incompatible with {} (precision={}, scale={}, max_length={}; accept_datetime_rounding={accept_datetime_rounding})", column.name, column.type_name, field.data_type(), column.precision, column.scale, column.max_length));
            }
            if field.is_nullable() && !column.nullable {
                violations.push(format!(
                    "column {}: NOT NULL cannot receive a nullable dataset field",
                    column.name
                ));
            }
            if column.identity && !(mode == LoadMode::Merge && merge_keys.contains(field.name())) {
                violations.push(format!(
                    "column {}: mapped IDENTITY is allowed only as a merge key in merge mode",
                    column.name
                ));
            }
            if column.has_default && field.is_nullable() && mode != LoadMode::Merge {
                violations.push(format!(
                    "column {}: DEFAULT would replace explicit NULL in append or full refresh",
                    column.name
                ));
            }
        }
        for column in &self.columns {
            if dataset.index_of(&column.name).is_err()
                && !column.nullable
                && !column.identity
                && !column.has_default
            {
                violations.push(format!(
                    "column {}: extra NOT NULL column has neither IDENTITY nor DEFAULT",
                    column.name
                ));
            }
        }
        if violations.is_empty() {
            Ok(())
        } else {
            Err(LoadFailure {
                code: "incompatible_destination_table",
                message: violations.join("; "),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};

    fn column(type_name: &str, precision: u8, scale: u8) -> TableColumn {
        TableColumn {
            name: "value".into(),
            type_name: type_name.into(),
            precision,
            scale,
            max_length: 8,
            nullable: true,
            identity: false,
            has_default: false,
        }
    }

    #[test]
    fn datetime_rounding_is_the_only_opt_in_type_family() {
        use arrow_schema::TimeUnit;
        for zone in [None, Some("UTC".into())] {
            for mode in [LoadMode::FullRefresh, LoadMode::Append, LoadMode::Merge] {
                for nullable in [false, true] {
                    for table_nullable in [false, true] {
                        for rounding in [false, true] {
                            let dataset = Schema::new(vec![Field::new(
                                "value",
                                DataType::Timestamp(TimeUnit::Microsecond, zone.clone()),
                                nullable,
                            )]);
                            let cases = (0..=7)
                                .map(|scale| ("datetime2", scale, scale >= 6 || rounding))
                                .chain([
                                    ("datetime", 3, rounding),
                                    ("smalldatetime", 0, false),
                                    ("date", 0, false),
                                    ("time", 7, false),
                                    ("datetimeoffset", 7, false),
                                    ("bigint", 0, false),
                                ]);
                            for (name, scale, accepted) in cases {
                                let mut col = column(name, 0, scale);
                                col.nullable = table_nullable;
                                assert_eq!(
                                    TableShape { columns: vec![col] }
                                        .validate(&dataset, mode, &[], rounding)
                                        .is_ok(),
                                    accepted && (!nullable || table_nullable),
                                    "{name}({scale}), rounding={rounding}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn all_violations_are_reported_together_with_column_names() {
        let dataset = Schema::new(vec![
            Field::new("value", DataType::Decimal128(18, 4), true),
            Field::new("Missing", DataType::Int64, false),
        ]);
        let mut col = column("decimal", 18, 3);
        col.nullable = false;
        col.identity = true;
        col.has_default = true;
        let mut extra = column("int", 10, 0);
        extra.name = "missing".into();
        extra.nullable = false;
        let failure = TableShape {
            columns: vec![col, extra],
        }
        .validate(&dataset, LoadMode::Append, &[], false)
        .unwrap_err();
        assert_eq!(failure.code, "incompatible_destination_table");
        assert_eq!(failure.message.split("; column ").count(), 6);
        for expected in [
            "column value: decimal is incompatible",
            "scale=3",
            "column value: NOT NULL",
            "column value: mapped IDENTITY",
            "column value: DEFAULT",
            "column Missing: missing destination column",
            "column missing: extra NOT NULL",
        ] {
            assert!(
                failure.message.contains(expected),
                "{} lacks {expected}",
                failure.message
            );
        }
    }

    #[test]
    fn max_text_and_decimal_boundaries_are_accepted() {
        for (data_type, col) in [
            (
                DataType::Utf8,
                TableColumn {
                    max_length: -1,
                    ..column("nvarchar", 0, 0)
                },
            ),
            (DataType::Decimal128(38, 38), column("decimal", 38, 38)),
            (DataType::Decimal128(1, 0), column("numeric", 1, 0)),
        ] {
            let dataset = Schema::new(vec![Field::new("value", data_type, true)]);
            let table = TableShape { columns: vec![col] };
            for mode in [LoadMode::FullRefresh, LoadMode::Append, LoadMode::Merge] {
                for rounding in [false, true] {
                    table.validate(&dataset, mode, &[], rounding).unwrap();
                }
            }
        }
    }

    #[test]
    fn catalog_query_quotes_names_and_decodes_columns_in_order() {
        let query = introspection_query("o'br]ien", "t']; DROP TABLE x;--");
        assert!(query.contains("OBJECT_ID(N'[o''br]]ien].[t'']]; DROP TABLE x;--]', N'U')"));
        assert!(query.contains("ORDER BY c.column_id"));
        assert!(query.contains("t.user_type_id = c.system_type_id"));
        let rows = vec![
            catalog_row("second", "numeric", 38, 4, 17, false, true, true),
            catalog_row("first", "nvarchar", 0, 0, -1, true, false, false),
        ];
        let table = TableShape::from_catalog_rows(rows).unwrap();
        let col = &table.columns[0];
        assert_eq!(
            (
                &*col.name,
                &*col.type_name,
                col.precision,
                col.scale,
                col.max_length,
                col.nullable,
                col.identity,
                col.has_default
            ),
            ("second", "numeric", 38, 4, 17, false, true, true)
        );
        assert_eq!(table.columns[1].name, "first");
        assert_eq!(table.columns[1].max_length, -1);
        assert!(table.columns[1].nullable);
        assert!(!table.columns[1].identity);
        assert!(!table.columns[1].has_default);
        assert!(
            TableShape::from_catalog_rows(Vec::<Vec<tiberius::ColumnData<'static>>>::new())
                .unwrap()
                .columns
                .is_empty()
        );
        for malformed in [vec![], vec![tiberius::ColumnData::String(None)]] {
            assert_eq!(
                TableShape::from_catalog_rows(vec![malformed])
                    .unwrap_err()
                    .code,
                "destination_write_failed"
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn catalog_row(
        name: &str,
        type_name: &str,
        precision: u8,
        scale: u8,
        length: i16,
        nullable: bool,
        identity: bool,
        has_default: bool,
    ) -> Vec<tiberius::ColumnData<'static>> {
        use tiberius::ColumnData::*;
        vec![
            String(Some(name.to_owned().into())),
            String(Some(type_name.to_owned().into())),
            U8(Some(precision)),
            U8(Some(scale)),
            I16(Some(length)),
            Bit(Some(nullable)),
            Bit(Some(identity)),
            Bit(Some(has_default)),
        ]
    }

    #[test]
    fn mapped_constraints_and_extra_columns_follow_each_load_mode() {
        for mode in [LoadMode::FullRefresh, LoadMode::Append, LoadMode::Merge] {
            for nullable in [false, true] {
                for identity in [false, true] {
                    for has_default in [false, true] {
                        for is_key in [false, true] {
                            let mut col = column("bigint", 19, 0);
                            col.identity = identity;
                            col.has_default = has_default;
                            let dataset =
                                Schema::new(vec![Field::new("value", DataType::Int64, nullable)]);
                            let keys = if is_key { vec!["value".into()] } else { vec![] };
                            let expected = (!identity || (mode == LoadMode::Merge && is_key))
                                && (!has_default || !nullable || mode == LoadMode::Merge);
                            assert_eq!(
                                TableShape { columns: vec![col] }
                                    .validate(&dataset, mode, &keys, false)
                                    .is_ok(),
                                expected
                            );
                        }
                        let mut extra = column("int", 10, 0);
                        extra.nullable = nullable;
                        extra.identity = identity;
                        extra.has_default = has_default;
                        assert_eq!(
                            TableShape {
                                columns: vec![extra]
                            }
                            .validate(&Schema::empty(), mode, &[], false)
                            .is_ok(),
                            nullable || identity || has_default
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn accept_family_preserves_types_across_modes_nullability_and_opt_in() {
        let cases = [
            (DataType::Int64, "bigint", 19, 0, true, true),
            (DataType::Int64, "int", 10, 0, true, true),
            (DataType::Int64, "smallint", 5, 0, true, true),
            (DataType::Int64, "tinyint", 3, 0, true, true),
            (DataType::Float64, "float", 53, 0, true, true),
            (DataType::Float64, "float", 24, 0, false, false),
            (DataType::Float64, "real", 24, 0, false, false),
            (DataType::Boolean, "bit", 1, 0, true, true),
            (DataType::Utf8, "nvarchar", 0, 0, true, true),
            (DataType::Utf8, "varchar", 0, 0, false, false),
            (DataType::Decimal128(18, 4), "decimal", 18, 4, true, true),
            (DataType::Decimal128(18, 4), "numeric", 38, 4, true, true),
            (DataType::Decimal128(18, 4), "decimal", 6, 4, true, true),
            (DataType::Decimal128(18, 4), "decimal", 18, 3, false, false),
            (DataType::Decimal128(18, 4), "decimal", 18, 5, false, false),
            (DataType::Int64, "nvarchar", 0, 0, false, false),
            (DataType::Utf8, "bigint", 19, 0, false, false),
        ];
        for (data_type, sql_type, precision, scale, normal, opted_in) in cases {
            for mode in [LoadMode::FullRefresh, LoadMode::Append, LoadMode::Merge] {
                for nullable in [false, true] {
                    for table_nullable in [false, true] {
                        for rounding in [false, true] {
                            let dataset =
                                Schema::new(vec![Field::new("value", data_type.clone(), nullable)]);
                            let mut col = column(sql_type, precision, scale);
                            col.nullable = table_nullable;
                            let expected = (if rounding { opted_in } else { normal })
                                && (!nullable || table_nullable);
                            assert_eq!(TableShape { columns: vec![col] }.validate(&dataset, mode, &[], rounding).is_ok(), expected, "{data_type:?} -> {sql_type}, nullable={nullable}/{table_nullable}, rounding={rounding}");
                        }
                    }
                }
            }
        }
    }
}
