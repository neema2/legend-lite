// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.executionplan;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * ONE OPERAND OF A RAISE SITE, AS THE PLAN STATES IT (stage B's B1, errors as records: the user's rulings of 2026-11-20
 * ~16:40, "we should do B to keep the PCT", and ~17:00, the plan carries the error's facts and no words): a literal the
 * statement holds, by its Pure kind, or {@link Computed} where the database computes the value. The plan never carries
 * a computed value; each audience's formatter words a site from its kind, its where and these operands after the run,
 * in Java (the product's sentence names the kind and the where; legend-pure's prints the literals, and a computed
 * operand as {@code <computed>}, register row S389). One JSON key an operand, its value a string where a JSON number
 * would lose digits.
 */
public sealed interface Operand {

    /** The operand's one JSON key. */
    String key();

    /** A raise's operand as the statement holds it ({@code sql.dialect.RaiseSite#operands}): a literal by its Pure kind,
     *  any other expression (a column, a call, a cast, a parameter) {@link Computed}, its value the database's. */
    static Operand of(com.legend.sql.SqlExpr e) {
        if (e instanceof com.legend.sql.SqlExpr.StringLit s) {
            return new Text(s.value());
        }
        if (e instanceof com.legend.sql.SqlExpr.IntLit i) {
            return new Int(i.value());
        }
        if (e instanceof com.legend.sql.SqlExpr.FloatLit f) {
            return new Flt(f.value());
        }
        if (e instanceof com.legend.sql.SqlExpr.DecimalLit d) {
            return new Dec(d.value());
        }
        if (e instanceof com.legend.sql.SqlExpr.BoolLit b) {
            return new Bool(b.value());
        }
        if (e instanceof com.legend.sql.SqlExpr.DateLit d) {
            return new Day(d.iso());
        }
        if (e instanceof com.legend.sql.SqlExpr.TimestampLit t) {
            return new Instant(t.iso());
        }
        return Computed.IT;
    }

    /** A text literal: a label, a type's name, a bound, a value the query wrote. */
    record Text(String value) implements Operand {
        public Text {
            Objects.requireNonNull(value, "a text operand's value");
        }

        @Override
        public String key() {
            return "string";
        }
    }

    /** An Integer literal. */
    record Int(long value) implements Operand {
        @Override
        public String key() {
            return "integer";
        }
    }

    /** A Float literal, written as Java's shortest round-trip text (never a JSON number). */
    record Flt(double value) implements Operand {
        @Override
        public String key() {
            return "float";
        }
    }

    /** A Decimal literal, its digits and scale as written. */
    record Dec(BigDecimal value) implements Operand {
        public Dec {
            Objects.requireNonNull(value, "a decimal operand's value");
        }

        @Override
        public String key() {
            return "decimal";
        }
    }

    /** A Boolean literal. */
    record Bool(boolean value) implements Operand {
        @Override
        public String key() {
            return "boolean";
        }
    }

    /** A date literal, ISO {@code yyyy-MM-dd}. */
    record Day(String iso) implements Operand {
        public Day {
            Objects.requireNonNull(iso, "a date operand's text");
        }

        @Override
        public String key() {
            return "date";
        }
    }

    /** A timestamp literal, as the statement holds it. */
    record Instant(String iso) implements Operand {
        public Instant {
            Objects.requireNonNull(iso, "a date-time operand's text");
        }

        @Override
        public String key() {
            return "dateTime";
        }
    }

    /** A value the database computes: its position kept, its value never carried by the plan. */
    record Computed() implements Operand {
        /** The one computed operand. */
        public static final Computed IT = new Computed();

        @Override
        public String key() {
            return "computed";
        }
    }
}
