// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

/**
 * A run-time parameter whose placeholder each database types by its DECLARED Pure type (docs/GOALS_OF_RECORD_2026_11_07.md
 * D1, ruled 2026-11-07: "Yes perfect"; P4 of the total wall, 2026-11-09): a Float's, a Decimal's, a Number's, a Date's
 * and a DateTime's. The dialect writes the placeholder for the declared type, as upstream's database extension writes
 * its template for the declared type when the plan is made (legend-engine's {@code processPlaceHolder}); nothing at run
 * writes a type into the statement, and its text is the same on every call (a number bound bare is typed by the
 * database from the class the driver passes, as upstream's bare literal is by its digits: the re-audit's B-4,
 * 2026-11-09). Until P4 the runner wrote a type
 * chosen by the value into a hole in the statement.
 */
public enum DeclaredKind {
    /** A Float: bare where the database types a bound value as its literal; H2's {@code FLOAT}, upstream's own. */
    FLOAT,
    /** A Decimal: bare where the database types a bound value as its literal; H2's {@code DECFLOAT} (exact). */
    DECIMAL,
    /** A Number: as a Decimal. */
    NUMBER,
    /** A Date (a StrictDate or a DateTime value): a timestamp, as upstream writes it on almost every database (a
     *  StrictDate value a midnight timestamp); BigQuery's a date. */
    DATE,
    /** A DateTime: a timestamp at the database's finest precision. */
    DATE_TIME
}
