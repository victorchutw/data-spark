//! Direct smoke test of the pinned Tiberius dependency (#156, ADR-0069).
//!
//! Data Spark's declared Decimal contract admits `decimal(38,38)`
//! (ADR-0044), and the SQL Server mapping carries that declaration
//! verbatim (ADR-0062). Tiberius 0.12.3 from crates.io panics on scale 38
//! and describes a fraction-only scale-38 value as `numeric(39,38)`, so
//! the manifest pins a maintainer-controlled fork carrying the fix. This
//! test proves the pinned revision honors the complete scale-38 invariant
//! without opening a connection; the live round trip lives in the fork's
//! own test suite.

use tiberius::numeric::Numeric;
use tiberius::{ColumnData, ToSql};

/// The #134 regression seed: Arrow `Decimal128(38,38)` scaled value `-1`.
const SCALED_MINUS_ONE: i128 = -1;

#[test]
fn pinned_tiberius_builds_scale_38_numeric_deterministically_with_precision_38() {
    let first = Numeric::new_with_scale(SCALED_MINUS_ONE, 38);
    let second = Numeric::new_with_scale(SCALED_MINUS_ONE, 38);

    for numeric in [first, second] {
        assert_eq!(numeric.value(), SCALED_MINUS_ONE);
        assert_eq!(numeric.scale(), 38);
        assert_eq!(numeric.int_part(), 0);
        assert_eq!(numeric.dec_part(), SCALED_MINUS_ONE);
        assert_eq!(
            numeric.precision(),
            38,
            "a fraction-only scale-38 value is numeric(38,38), never numeric(39,38)"
        );
    }

    assert_eq!(first, second);
    assert_eq!(format!("{first:?}"), format!("{second:?}"));
    assert_eq!(first.to_sql(), ColumnData::Numeric(Some(second)));
}

#[test]
fn pinned_tiberius_reports_precision_38_across_the_scale_38_range() {
    let max_magnitude = 10i128.pow(38) - 1;

    for value in [0, 1, max_magnitude, -max_magnitude] {
        let numeric = Numeric::new_with_scale(value, 38);
        assert_eq!(numeric.value(), value);
        assert_eq!(numeric.scale(), 38);
        assert_eq!(numeric.precision(), 38, "precision of {value} at scale 38");
    }
}

#[test]
fn pinned_tiberius_still_rejects_scale_39() {
    let outcome = std::panic::catch_unwind(|| Numeric::new_with_scale(1, 39));

    assert!(
        outcome.is_err(),
        "scale 39 exceeds SQL Server's cap and must keep panicking"
    );
}
