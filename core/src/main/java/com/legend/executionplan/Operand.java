// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.executionplan;

import com.legend.sql.SqlExpr;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * ONE OPERAND OF A RAISE SITE, AS TYPED DATA (H13, the error-records spike, 2026-11-19; slice 1.2 of the compatibility
 * program; the user's rulings A3 and CP-7): what a raise was built from where the planner KNEW it -- a literal of the
 * query, typed -- or {@link Computed}, which keeps the operand's position and says nothing of its value (a column's, a
 * parameter's, a cast's: a run-time value never enters a message, A3). Product data: {@code PlanJson} writes it
 * natively, with no codec. No words: each audience's formatter words a site from its kind, its where and these.
 *
 * <p>SPIKE: scratch, never proposed for merge.
 */
public sealed interface Operand {

    /** A text the planner knew ({@code SqlExpr.StringLit}): a label, a type's name, a bound, or a String literal. */
    record Text(String value) implements Operand {
        public Text {
            Objects.requireNonNull(value, "a text operand");
        }
    }

    /** An Integer literal ({@code SqlExpr.IntLit}). */
    record Int(long value) implements Operand {
    }

    /** A Float literal ({@code SqlExpr.FloatLit}), the double itself. */
    record Flt(double value) implements Operand {
    }

    /** A Decimal literal ({@code SqlExpr.DecimalLit}), its digits and scale as written. */
    record Dec(BigDecimal value) implements Operand {
        public Dec {
            Objects.requireNonNull(value, "a decimal operand");
        }
    }

    /** A Boolean literal ({@code SqlExpr.BoolLit}). */
    record Bool(boolean value) implements Operand {
    }

    /** A date literal's ISO day ({@code SqlExpr.DateLit}). */
    record Day(String iso) implements Operand {
        public Day {
            Objects.requireNonNull(iso, "a date operand");
        }
    }

    /** A date-time literal's ISO text ({@code SqlExpr.TimestampLit}). */
    record Instant(String iso) implements Operand {
        public Instant {
            Objects.requireNonNull(iso, "a date-time operand");
        }
    }

    /** An operand the planner did not know: its position kept, its value never carried (A3). */
    record Computed() implements Operand {
    }

    /** The one {@link Computed}. */
    Operand COMPUTED = new Computed();

    /** H13 measure switch: {@code /home/user/h13log/seeThroughCasts} exists. */
    boolean LOOSE = java.nio.file.Files.exists(java.nio.file.Path.of("/home/user/h13log/seeThroughCasts"));

    /** {@code written}, an operand a render wrote into a raise site, as the plan's data: a literal typed, any other
     *  expression {@link #COMPUTED}. Nothing is evaluated, folded or looked through: a cast of a literal is computed
     *  (the measure's strict form; H13 counts what a looser rule would recover). */
    static Operand of(SqlExpr written) {
        return switch (written) {
            case SqlExpr.StringLit t -> new Text(t.value());
            case SqlExpr.IntLit n -> new Int(n.value());
            case SqlExpr.FloatLit f -> new Flt(f.value());
            case SqlExpr.DecimalLit d -> new Dec(d.value());
            case SqlExpr.BoolLit b -> new Bool(b.value());
            case SqlExpr.DateLit d -> new Day(d.iso());
            case SqlExpr.TimestampLit ts -> new Instant(ts.iso());
            // H13 MEASURE ONLY (on where the marker file exists): a cast of a literal read as that literal. NOT a
            // rule to land: a cast can change what a value prints as; the real fix is the lowering handing the raise
            // the literal it was given.
            case SqlExpr.Cast c when LOOSE && of(c.value()) != COMPUTED -> of(c.value());
            default -> COMPUTED;
        };
    }
}
