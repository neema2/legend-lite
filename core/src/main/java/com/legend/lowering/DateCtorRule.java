// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.List;

/**
 * The {@code date(...)} constructor's SQL spelling (extracted from
 * {@link Scalars} at the file guardrail): component RANGES validate with
 * real pure's messages — {@code date(2016, 13)} raises
 * {@code 'Invalid month: 13'}; the DAY check is MONTH-AWARE
 * ({@code DateFunctions.validateDay}: a too-large day speaks the
 * attempted y-m-d, {@code 'Invalid day: 2016-12-32'}, month un-padded).
 * Literal components fold to a constant error; RUNTIME components wrap
 * in the SQL guard — the same message either way (the audit found the
 * runtime half missing: DuckDB's own make_date message leaked instead
 * of pure's). Partial-precision forms build the string carrier.
 */
final class DateCtorRule {

    private DateCtorRule() {
    }

    static SqlExpr lower(TypedNativeCall n, List<SqlExpr> args) {
        String[] comps = {null, "month", "day", "hour", "minute", "second"};
        long[][] ranges = {null, {1, 12}, {1, 31}, {0, 23}, {0, 59}, {0, 59}};
        List<SqlExpr> guarded = new ArrayList<>(args);
        for (int i = 1; i < Math.min(args.size(), 6); i++) {
            if (args.get(i) instanceof SqlExpr.IntLit lit) {
                // DAY is MONTH-AWARE (real DateFunctions.validateDay):
                // day < 1 speaks the day alone; day > daysInMonth
                // speaks the attempted y-m-d ("Invalid day: 2016-12-32",
                // month un-padded)
                if (i == 2 && lit.value() >= 1
                        && args.get(0) instanceof SqlExpr.IntLit yl
                        && args.get(1) instanceof SqlExpr.IntLit ml
                        && ml.value() >= 1 && ml.value() <= 12
                        && lit.value() > daysInMonth(yl.value(), ml.value())) {
                    return PureSql.raise(
                            new SqlExpr.StringLit("Invalid day: "
                            + yl.value() + "-" + ml.value() + "-"
                            + lit.value()), n.pos());
                }
                if (lit.value() < ranges[i][0] || lit.value() > ranges[i][1]) {
                    return PureSql.raise(
                            new SqlExpr.StringLit(
                            "Invalid " + comps[i] + ": " + lit.value()), n.pos());
                }
            } else if (args.get(i) instanceof SqlExpr.FloatLit
                    || args.get(i) instanceof SqlExpr.DecimalLit) {
                // a LITERAL fractional seconds component validates
                // statically: [0, 60) — exclusive top (59.999 legal)
                java.math.BigDecimal v = args.get(i) instanceof SqlExpr.FloatLit fl
                        ? java.math.BigDecimal.valueOf(fl.value())
                        : ((SqlExpr.DecimalLit) args.get(i)).value();
                if (v.signum() < 0
                        || v.compareTo(java.math.BigDecimal.valueOf(60)) >= 0) {
                    return PureSql.raise(
                            new SqlExpr.StringLit(
                            "Invalid " + comps[i] + ": " + v.toPlainString()), n.pos());
                }
            } else {
                // FRACTIONAL seconds are legal up to (not including)
                // 60 — the integer ranges guard integers; a
                // fractional bound is exclusive at the top
                boolean fractionalSeconds = i == 5
                        && n.args().get(i).info().type() != Type.Primitive.INTEGER;
                SqlExpr tooHigh = fractionalSeconds
                        ? SqlExpr.Call.of(SqlFn.GREATER_EQUAL, args.get(i),
                                new SqlExpr.IntLit(60))
                        : SqlExpr.Call.of(SqlFn.GREATER, args.get(i),
                                new SqlExpr.IntLit(ranges[i][1]));
                guarded.set(i, Scalars.guarded(
                        SqlExpr.Call.of(SqlFn.OR,
                                SqlExpr.Call.of(SqlFn.LESS, args.get(i),
                                        new SqlExpr.IntLit(ranges[i][0])),
                                tooHigh),
                        Scalars.cat(new SqlExpr.StringLit("Invalid " + comps[i] + ": "),
                                Scalars.str(args.get(i))),
                        args.get(i)));
            }
        }
        args = guarded;
        if (args.size() == 3) {
            return new SqlExpr.Call(SqlFn.MAKE_DATE, args);
        }
        if (args.size() == 6) {
            // FLOAT seconds = SUB-SECOND precision: real pure prints
            // the ISO form with the fraction trimmed to its minimal
            // digits (11.0, not 11.000) — the string carrier again.
            if (n.args().get(5).info().type() == Type.Primitive.FLOAT
                    || n.args().get(5).info().type() == Type.Primitive.DECIMAL) {
                args = new ArrayList<>(args);
                args.set(5, keptSecond(args.get(5), n));
                SqlExpr iso = SqlExpr.Call.of(SqlFn.STRFTIME,
                        new SqlExpr.Call(SqlFn.MAKE_TIMESTAMP, args),
                        // %f = MICROseconds — %g's milliseconds
                        // silently truncated 59.999999 (audit); the
                        // zero-trim below reduces to minimal digits.
                        new SqlExpr.FormatLit(com.legend.sql.DateFmt.ISO_MICRO));
                SqlExpr trimmed = SqlExpr.Call.of(SqlFn.RTRIM, iso,
                        new SqlExpr.StringLit("0"));
                // TEMPORAL_TEXT-stamped (§4bZ-V B3): the marker cast
                // never renders; the slot says temporal-in-text
                return new SqlExpr.Cast(new SqlExpr.Case(
                        List.of(new SqlExpr.Case.When(
                                SqlExpr.Call.of(SqlFn.ENDS_WITH, trimmed,
                                        new SqlExpr.StringLit(".")),
                                SqlExpr.Call.of(SqlFn.CONCAT, trimmed,
                                        new SqlExpr.StringLit("0")))),
                        trimmed, SqlExpr.Reason.OPERATOR), SqlType.Scalar.TEMPORAL_TEXT, SqlExpr.Reason.OPERATOR);
            }
            return new SqlExpr.Call(SqlFn.MAKE_TIMESTAMP, args);
        }
        String[] seps = {"", "-", "-", "T", ":"};
        int[] widths = {4, 2, 2, 2, 2};
        SqlExpr out = null;
        for (int i = 0; i < args.size(); i++) {
            SqlExpr part = SqlExpr.Call.of(SqlFn.LPAD,
                    new SqlExpr.Cast(args.get(i),
                            SqlType.Scalar.VARCHAR, SqlExpr.Reason.OPERATOR),
                    new SqlExpr.IntLit(widths[i]), new SqlExpr.StringLit("0"));
            out = out == null ? part
                    : SqlExpr.Call.of(SqlFn.CONCAT, SqlExpr.Call.of(SqlFn.CONCAT,
                            out, new SqlExpr.StringLit(seps[i])), part);
        }
        // the signature requires at least the year component; the
        // partial print is TEMPORAL_TEXT-stamped (§4bZ-V B3 — the
        // marker cast never renders)
        return new SqlExpr.Cast(java.util.Objects.requireNonNull(out,
                "date() with no components"), SqlType.Scalar.TEMPORAL_TEXT, SqlExpr.Reason.OPERATOR);
    }

    /** The digits of a second the fractional {@code date()}'s text carrier writes ({@code DateFmt.ISO_MICRO}). */
    static final int CARRIED_DIGITS = 6;

    /**
     * {@code second}, a fractional {@code date()}'s seconds, as the text carrier keeps it: a second with more fraction
     * digits than the carrier writes ({@link #CARRIED_DIGITS}) is REFUSED, never cut (S181, the user's ruling of
     * 2026-11-10; the second audit's S5: {@code MAKE_TIMESTAMP} and the carrier's microsecond format cut a Float second
     * of 9 digits to 6). A known second is refused at planning, by name; a computed one by the statement's own raise
     * when its value has more (its value as a DECIMAL(38, 9) differs from itself rounded to the carrier's digits).
     */
    private static SqlExpr keptSecond(SqlExpr second, TypedNativeCall n) {
        java.math.BigDecimal known = second instanceof SqlExpr.FloatLit fl ? java.math.BigDecimal.valueOf(fl.value())
                : second instanceof SqlExpr.DecimalLit dl ? dl.value() : null;
        if (known != null) {
            int digits = Math.max(0, known.stripTrailingZeros().scale());
            if (digits > CARRIED_DIGITS) {
                throw new com.legend.sql.DialectCapability("the second " + known.toPlainString() + " has " + digits
                        + " fraction digits; a date()'s fractional second keeps " + CARRIED_DIGITS
                        + ": refused, never cut (S181)");
            }
            return second;
        }
        SqlExpr exact = new SqlExpr.Cast(second, new SqlType.Decimal(38, 9), SqlExpr.Reason.OPERATOR);
        return Scalars.guarded(SqlExpr.Call.of(SqlFn.NOT_EQUAL, exact,
                        SqlExpr.Call.of(SqlFn.ROUND, exact, new SqlExpr.IntLit(CARRIED_DIGITS))),
                Scalars.cat(new SqlExpr.StringLit("the second "), Scalars.str(second),
                        new SqlExpr.StringLit(" has more than " + CARRIED_DIGITS
                                + " fraction digits, which a date()'s fractional second keeps: refused, never cut"
                                + " (S181)")),
                second, n.pos());
    }

    /** Real pure's month length (DateFunctions.getDaysInMonth — proleptic
     * Gregorian leap rule). Callers range-check the month FIRST; an
     * out-of-range month here is a caller bug (C2.4 — throw, never
     * fabricate). */
    private static long daysInMonth(long year, long month) {
        return switch ((int) month) {
            case 1, 3, 5, 7, 8, 10, 12 -> 31;
            case 4, 6, 9, 11 -> 30;
            case 2 -> (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))
                    ? 29 : 28;
            default -> throw new IllegalStateException(
                    "daysInMonth over unvalidated month: " + month);
        };
    }
}
