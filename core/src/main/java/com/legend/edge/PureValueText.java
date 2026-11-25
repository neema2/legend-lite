// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.error.NotImplementedException;
import com.legend.executionplan.ExecutionPlan;
import com.legend.values.PureDateLiteral;
import com.legend.values.PureTimeLiteral;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * LEGEND-PURE'S TEXT OF A VALUE, WRITTEN IN JAVA FROM LITE'S TYPED VALUE: THE EDGE'S ONE VALUE PRINTER (stage B's B2b
 * S6a, legend-pure's door, 2026-11-25; the orchestrator's agreement of the same day, G1: one printer, never a second
 * Float printer -- the midpoint audit's item, B2b's record). Two readers: a relation's cell in the table printer
 * ({@link #cell}, the rules {@link PureRelationText} held until S6a, moved here unchanged), and a Float whose
 * {@code toString} is a program's last step, lifted at legend-pure's door ({@link #value}; {@link PrintedRelations}),
 * where the edge's SQL print pass writes legend-engine's in-query cast (on H2 {@code 1.3421E-8}, where legend-pure
 * prints {@code 0.000000013421}). legend-engine's door, and the passes' deletion, wait for the user's ruling (the
 * orchestrator's relay of 2026-11-25). A printer only: the database computed every value, and this class chooses only
 * how each is written (tenet 3; G6, small Java formatting at the edge); a value it has no rule for is refused by name
 * (AGENTS.md invariant 4).
 */
final class PureValueText {

    private PureValueText() {
    }

    /** The column types whose cell is never a text. */
    private static final java.util.Set<String> NOT_TEXT = java.util.Set.of("Integer", "Float", "Decimal", "Number",
            "Boolean", "Date", "StrictDate", "DateTime", "StrictTime");

    /**
     * legend-pure's {@code toString()} of a relation's cell, by the value lite's typed row carries and its column's Pure
     * type:
     * <ul>
     *   <li>an Integer's digits;</li>
     *   <li>a Float column's cell as the double it is declared -- whatever kind the database computed it in (an integer,
     *       a decimal an aggregate collapsed to), as the edge's SQL composition cast it to DOUBLE until B2b and
     *       legend-engine's TDS reads it -- written by legend-pure's Float print ({@link #pureFloat});</li>
     *   <li>a Number column's cell by its own value: an integer's digits (legend-pure's print of an Integer), a
     *       fraction-kind value (a double, or a decimal an aggregate collapsed to) as a Float -- the rule the edge's SQL
     *       composition kept until B2b (witness PCT's {@code testMax_Numbers_Relation_Aggregate}: {@code 81.18}, never
     *       {@code 81.180});</li>
     *   <li>a Decimal at its own scale ({@code toPlainString}, legend-pure's Decimal print);</li>
     *   <li>a Boolean {@code true}/{@code false}; an enumeration's value, which lite carries as its name;</li>
     *   <li>a date ({@link #tdsDate}); a StrictTime as written.</li>
     * </ul>
     * Anything else -- a list, a map, a structure -- is refused by name.
     */
    static String cell(Object v, ExecutionPlan.Column c) {
        String type = c.type();
        if (type.equals("Float")) {
            return asFloat(v, c, "toString");
        }
        return switch (v) {
            case Long l -> Long.toString(l);
            case Integer i -> Integer.toString(i);
            case BigInteger i -> i.toString();
            case Double d -> pureFloat(d);
            case BigDecimal d -> type.equals("Number") ? pureFloat(d.doubleValue()) : d.toPlainString();
            case Boolean x -> x.toString();
            // a text where a column holds text (a String, an enumeration's name, Any, a Variant); under a number, a
            // Boolean or a date it is no value this printer has a rule for (B2b's midpoint audit, 2026-11-24)
            case String s when !NOT_TEXT.contains(type) -> s;
            case PureDateLiteral d -> tdsDate(d);
            case PureTimeLiteral t -> t.canonicalText();
            default -> throw refused("toString", v, c);
        };
    }

    /** legend-pure's {@code toString()} of a Float a program's last step prints, lifted at legend-pure's door
     *  ({@link PrintedRelations#liftValue}): the double it is declared, by legend-pure's print ({@link #pureFloat}).
     *  Anything else is refused by name: the door lifts a Float's print alone. */
    static String value(Object v, ExecutionPlan.Column c) {
        if (!c.type().equals("Float")) {
            throw refused("value toString", v, c);
        }
        return asFloat(v, c, "value toString");
    }

    /** {@code v} under a Float type, as the double it is declared (an integer or a decimal the database held it in). */
    private static String asFloat(Object v, ExecutionPlan.Column c, String what) {
        return switch (v) {
            case Double d -> pureFloat(d);
            case Long l -> pureFloat(l.doubleValue());
            case Integer i -> pureFloat(i.doubleValue());
            case BigInteger i -> pureFloat(i.doubleValue());
            case BigDecimal d -> pureFloat(d.doubleValue());
            default -> throw refused(what, v, c);
        };
    }

    /** legend-pure's print of a Float (legend-pure 5.99.0's {@code CompiledSupport}, lines 130-137 and 1271-1274):
     *  {@code 0.0} for a zero of either sign, else {@code DecimalFormat("0.0")} in English with up to 340 fraction
     *  digits -- a point always, never an exponent; an infinity {@code ∞}, as that format writes it. */
    static String pureFloat(double d) {
        if (d == 0.0d) {
            return "0.0";
        }
        // a format per value: a DecimalFormat is not safe to share, and the printer holds no static state
        // (DanglingStateGuardTest); NaN prints NaN and the infinities the infinity sign, signed, as legend-pure's do
        java.text.DecimalFormat format = new java.text.DecimalFormat("0.0",
                java.text.DecimalFormatSymbols.getInstance(java.util.Locale.ENGLISH));
        format.setMaximumFractionDigits(340);
        return format.format(d);
    }

    /**
     * A date cell. A value with no time of day is its day. One with a time is the TDS text's DateTime -- its seconds,
     * at least three fraction digits (the first three, then any finer ones without their trailing zeros), and
     * {@code +0000}: the rule the edge's SQL composition wrote until B2b ({@code dateTimeTextTds}, measured on 35
     * goldens of channel B's relation and standard suites, 2026-10-18; legend-engine's {@code testDayOfWeek_Relation}
     * prints a TDS literal's {@code 1985-01-12T06:30:00Z} as {@code 1985-01-12T06:30:00.000+0000}). A known divergence
     * it keeps, as the SQL did: legend-engine's TestTDS reads a date to the millisecond
     * ({@code DateFunctions.fromDate(new Date(nanos / 1000000))}), so a finer value prints {@code .123+0000} there and
     * {@code .123456+0000} here; the goldens hold whole seconds and milliseconds, which both rules print alike (the
     * independent re-review of 2026-11-24; recorded in docs/records/compat-b2-progress.md for B2b's register rows).
     */
    static String tdsDate(PureDateLiteral d) {
        if (!d.precision().atLeast(PureDateLiteral.Precision.HOUR)) {
            return d.canonicalText();
        }
        String nine = d.toNineDigits();
        int point = nine.length() - 10;
        String digits = nine.substring(point + 1);
        String finer = digits.substring(3);
        int end = finer.length();
        while (end > 0 && finer.charAt(end - 1) == '0') {
            end--;
        }
        return nine.substring(0, point) + "." + digits.substring(0, 3) + finer.substring(0, end) + "+0000";
    }

    static NotImplementedException refused(String what, Object v, ExecutionPlan.Column c) {
        return new NotImplementedException("legend-pure's " + what + " of a " + v.getClass().getSimpleName()
                + " cell under the " + c.type() + " column '" + c.name() + "' is not written at the edge");
    }
}
