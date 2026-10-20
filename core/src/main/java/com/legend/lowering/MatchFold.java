// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedMatchRuntime;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedSubst;
import com.legend.error.NotImplementedException;
import com.legend.sql.SqlExpr;
import com.legend.values.PureDateLiteral;

/**
 * STATIC-DISPATCH fold for a runtime match in scalar position: the
 * input's compile-time type picks the FIRST conforming arm (Pure match
 * order), whose body inlines with the parameter bound to the input —
 * the milestoning date-projection helpers spell StrictDate/DateTime
 * arms over a typed date column (the getAllForEachDate family). A
 * genuinely polymorphic input (class hierarchies) stays a loud wall.
 */
final class MatchFold {

    private MatchFold() {
    }

    static TypedSpec fold(TypedMatchRuntime mr) {
        if (mr.dynamicArms().isPresent()) {
            throw new NotImplementedException("scalar match: the arm collection has a"
                    + " non-literal prefix (extension-contributed arms) that did not fold"
                    + " to [] — the lowering has no runtime arm list");
        }
        for (TypedMatchRuntime.Arm arm : mr.arms()) {
            if (staticConforms(mr.input().info().type(), arm.typeFqn())) {
                // capture-avoiding: a binder in the arm body that the input
                // reads free is renamed (rebuild W0.6 push 1)
                return TypedSubst.apply(arm.body(), java.util.Map.of(arm.param(), mr.input()));
            }
        }
        throw new NotImplementedException("scalar match: no arm statically"
                + " accepts input type "
                + mr.input().info().type().typeName());
    }

    /** Exact primitive FQN, the temporal ladder under Date, the numeric
     * ladder under Number, and Any; class inputs answer false. */
    private static boolean staticConforms(Type t, String armFqn) {
        if (com.legend.compiler.element.type.PlatformTypes.ANY.equals(armFqn)) {
            return true;
        }
        if (!(t instanceof Type.Primitive p)) {
            return false;
        }
        if (p.qualifiedName().equals(armFqn)) {
            return true;
        }
        if ("meta::pure::metamodel::type::Date".equals(armFqn)) {
            return p.isTemporal() && p != Type.Primitive.STRICT_TIME;
        }
        return "meta::pure::metamodel::type::Number".equals(armFqn)
                && p.isNumeric();
    }

    /** A time-of-day literal: a TIME, its minute form padded with its seconds ({@code %10:30} is 10:30:00). */
    static SqlExpr timeLit(com.legend.values.PureTimeLiteral t) {
        return new SqlExpr.TimeLit(t instanceof com.legend.values.PureTimeLiteral.TimeWithMinute
                ? t.toEngineString() + ":00" : t.toEngineString());
    }

    /** Date literals: full dates/timestamps render typed; PARTIAL dates
     * (year / year-month) compare as STRINGS (master's pinned
     * semantics); HOUR/MINUTE-precision timestamps PAD to the full
     * shape SQL demands. Exhaustive — a new precision variant demands a
     * decision here. */
    static SqlExpr dateLit(PureDateLiteral d) {
        return dateLit(d, null);
    }

    /** {@code zone}: the connection's dbTimeZone — a TIME-BEARING literal
     * (a pure DateTime is an instant, UTC-based) spells in that zone, the
     * engine's convertDateToSqlString rule (extensionDefaults.pure:144:
     * {@code format('%t{[zone]yyyy-MM-dd HH:mm:ss}', $date)}); date-only
     * and partial literals are untouched. Null = GMT, the identity. */
    static SqlExpr dateLit(PureDateLiteral d, @com.legend.base.Nullable String zone) {
        if (zone != null && !zone.equals("GMT") && !zone.equals("UTC")) {
            String iso = LiteralSpelling.isoTimestamp(d);
            if (iso != null) {
                return new SqlExpr.TimestampLit(LiteralSpelling.inZone(iso, zone));
            }
        }
        return switch (d) {
            case PureDateLiteral.StrictDate sd ->
                    new SqlExpr.DateLit(iso(sd.toEngineString(), sd.year()));
            // Partials carry as the TEMPORAL_TEXT-stamped print form
            // (§4bZ-V B3): the marker cast never renders — same SQL,
            // and the slot's contract says temporal-in-text-carriage
            case PureDateLiteral.Year y -> new SqlExpr.Cast(
                    new SqlExpr.StringLit(y.toEngineString()),
                    com.legend.sql.SqlType.Scalar.TEMPORAL_TEXT);
            case PureDateLiteral.YearMonth ym -> new SqlExpr.Cast(
                    new SqlExpr.StringLit(ym.toEngineString()),
                    com.legend.sql.SqlType.Scalar.TEMPORAL_TEXT);
                // Every time-bearing precision is a TIMESTAMP — exhaustive,
                // so a new precision variant demands a decision here.
                // HOUR/MINUTE-precision literals PAD to the full timestamp
                // shape SQL demands (%2015-04-15T17 is 17:00:00); second-level
                // precision is already full.
            case PureDateLiteral.DateWithHour h ->
                    new SqlExpr.TimestampLit(iso(h.toEngineString(), h.year()) + ":00:00");
            case PureDateLiteral.DateWithMinute mi ->
                    new SqlExpr.TimestampLit(iso(mi.toEngineString(), mi.year()) + ":00");
            case PureDateLiteral.DateWithSecond se ->
                    new SqlExpr.TimestampLit(iso(se.toEngineString(), se.year()));
            case PureDateLiteral.DateWithSubsecond su ->
                    new SqlExpr.TimestampLit(iso(su.toEngineString(), su.year()));
        };
    }

    /** A date literal's text with its year in ISO's four digits: Pure spells the year 1 {@code %1-01-01}, and Postgres
     *  reads a leading field of fewer than three digits as no year of four ({@code '1-01-01'} as 2001-01-01,
     *  {@code '10-01-01'} as 2001-10-01, measured 2026-10-18), where {@code 0001-01-01} is the year 1 on every
     *  database. A year outside 0 to 9999 keeps Pure's spelling. */
    static String iso(String text, int year) {
        return year >= 0 && year < 1000
                ? String.format(java.util.Locale.ROOT, "%04d", year) + text.substring(Integer.toString(year).length())
                : text;
    }
}
