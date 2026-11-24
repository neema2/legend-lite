// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.spec.typed.TypedCDate;
import com.legend.compiler.spec.typed.TypedCDecimal;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlType;
import com.legend.values.PureDateLiteral;

/**
 * KIND-FRAGILE literals at the egress ROOT (D-arc + X-audit): a plain
 * SQL spelling would evaporate the pure kind on the wire, so the ROOT
 * projection swaps to a kind-carrying form — and ONLY the root
 * (measured: a blanket renderer cast truncated VALUES columns and
 * re-kinded 2-arg divide; inside expressions SQL type unification
 * governs).
 *
 * <ul>
 *   <li>Temporal literals with hour/minute/subsecond precision: a SQL
 *       TIMESTAMP round-trip destroys written precision — project the
 *       WRITTEN spelling under the TIMESTAMP-stamped output (the
 *       precision-faithful VARCHAR convention).</li>
 *   <li>Scale&le;0 Decimal literals: the dotless spelling ('2') types
 *       INTEGER — project with an explicit DECIMAL cast.</li>
 * </ul>
 */
final class RootLiterals {

    private RootLiterals() {
    }

    /** The root swap, or null when {@code spec} is not a fragile
     * literal (the caller lowers normally). */
    static @com.legend.base.Nullable SqlExpr swap(TypedSpec spec) {
        if (spec instanceof TypedCDate cd
                && fragileTemporalPrecision(cd.value())) {
            // TEMPORAL_TEXT-stamped (§4bZ-V B3): the marker cast never
            // renders; the slot's contract says temporal-in-text. A date-time with a nonzero fraction digit past the
            // ninth is refused by name, never cut: no database lite runs on keeps more (S181, the user's ruling of
            // 2026-11-10; the precision audit's M1, 2026-11-22: the text carrier passed it to a judge that cut it)
            String text = cd.value().canonicalText();
            if (significantFractionDigits(text) > 9) {
                throw new com.legend.sql.DialectCapability("the date-time " + text + " has "
                        + significantFractionDigits(text) + " fraction digits; no database keeps more than 9: refused,"
                        + " never cut (S181)");
            }
            return new SqlExpr.Cast(
                    new SqlExpr.StringLit(text),
                    SqlType.Scalar.TEMPORAL_TEXT, SqlExpr.Reason.DECLARED);
        }
        if (spec instanceof TypedCDecimal cdec
                && cdec.value().scale() <= 0) {
            return new SqlExpr.Cast(new SqlExpr.DecimalLit(cdec.value()),
                    new SqlType.Decimal(38, 0), SqlExpr.Reason.DECLARED);
        }
        return null;
    }

    /** The digits of a second {@code text} (a date-time's canonical text) writes, its trailing zeros no digit. */
    private static int significantFractionDigits(String text) {
        int dot = text.indexOf('.');
        if (dot < 0) {
            return 0;
        }
        int end = dot + 1;
        while (end < text.length() && Character.isDigit(text.charAt(end))) {
            end++;
        }
        int n = end - dot - 1;
        while (n > 0 && text.charAt(dot + n) == '0') {
            n--;
        }
        return n;
    }

    /** Precisions a SQL TIMESTAMP round-trip cannot reproduce: padded
     * HOUR/MINUTE forms, and any written subsecond block — digit count
     * is per-value data. Year/YearMonth ride StringLit already;
     * StrictDate/DateWithSecond round-trip exactly. */
    private static boolean fragileTemporalPrecision(PureDateLiteral d) {
        return switch (d) {
            case PureDateLiteral.DateWithHour h -> true;
            case PureDateLiteral.DateWithMinute m -> true;
            case PureDateLiteral.DateWithSubsecond s -> true;
            default -> false;
        };
    }
}
