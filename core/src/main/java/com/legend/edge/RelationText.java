// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.result.CellText;
import com.legend.result.RowDecode;
import com.legend.values.PureDateLiteral;
import com.legend.values.PureTimeLiteral;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

/**
 * SPIKE (H11, docs/records/compat-homework/H11-relation-text-spike.md; never proposed for merge as is): LEGEND-PURE'S
 * TEXT OF A RELATION, WRITTEN IN JAVA over typed rows after the run -- what {@link RelationTextLowering} composes in
 * SQL today. The rules are read at the pins (legend-engine 4.145.0, legend-pure 5.99.0), each named where it is used.
 * A printer only: it chooses the text of a value it holds whole, converts no kind and defaults nothing; a cell it has
 * no rule for is refused by name.
 */
final class RelationText {

    private RelationText() {
    }

    /** A column as the printer reads it: its name and its Pure type in the plan's spelling. */
    record Col(String name, String type) {
    }

    // ---- toString: the #TDS text (legend-engine's toString.pure:24-35) ---------------------------------------------

    /** {@code meta::pure::functions::relation::toString(rel)}: {@code #TDS}, the names line, one line per row, {@code #}.
     *  No rows leaves the rows segment empty ({@code joinStrings('\n')} of nothing). */
    static String tds(List<Col> cols, List<? extends List<?>> rows) {
        StringBuilder b = new StringBuilder(64 + rows.size() * 32).append("#TDS\n   ");
        for (int i = 0; i < cols.size(); i++) {
            b.append(i == 0 ? "" : ",").append(tdsName(cols.get(i).name()));
        }
        b.append('\n');
        for (int r = 0; r < rows.size(); r++) {
            List<?> row = rows.get(r);
            if (row.size() != cols.size()) {
                throw new IllegalArgumentException("a row of " + row.size() + " cells under " + cols.size() + " columns");
            }
            b.append(r == 0 ? "" : "\n").append("   ");
            for (int i = 0; i < cols.size(); i++) {
                b.append(i == 0 ? "" : ",").append(tdsCell(row.get(i), cols.get(i).type()));
            }
        }
        return b.append("\n#").toString();
    }

    /** A column's name in the header (toString.pure:27-30): trimmed, and quoted unless it already starts with a quote
     *  or is {@code ^[a-zA-Z0-9_]+$}. */
    static String tdsName(String raw) {
        String name = raw.trim();
        boolean simple = name.startsWith("'") || name.matches("^[a-zA-Z0-9_]+$");
        return simple ? name : "'" + name + "'";
    }

    /** A cell, legend-engine's {@code s(value, type)} (s.pure:23-38): an empty one {@code null} ({@code 'null'} under a
     *  Variant column); a Variant quoted; a String holding <code>{</code> or {@code [} quoted; anything else its
     *  {@code toString()} ({@link #value}). */
    static String tdsCell(@com.legend.base.Nullable Object v, String type) {
        met("tds", v, type);
        boolean variant = type.equals(RowDecode.VARIANT);
        if (v == null) {
            return variant ? "'null'" : "null";
        }
        if (variant) {
            return quoted(value(v, type));
        }
        if (v instanceof String s) {
            return s.contains("{") || s.contains("[") ? quoted(s) : s;
        }
        return value(v, type);
    }

    /** {@code '} + the text with {@code \} and {@code '} escaped + {@code '} (s.pure:33-34). */
    private static String quoted(String s) {
        return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'";
    }

    // ---- toCSV (legend-engine's helperFunctions.pure:198-250) and a TDS's csv ---------------------------------------

    /** {@code meta::relational::tests::csv::toCSV(tds, renderTdsNull)} (helperFunctions.pure:208-212): the names, each
     *  through the cell's escape, joined {@code ,} and a newline; then the rows, each its cells joined {@code ,},
     *  {@code joinStrings('', '\n', '\n')}: a newline between rows and one after the last (no rows: the one). */
    static String csv(List<Col> cols, List<? extends List<?>> rows, boolean renderTdsNull) {
        return csv(cols, rows, renderTdsNull, ",", true);
    }

    /** A TDS's {@code csv} property (legend-pure's tds.pure:19) as the relational corpus's one golden spells it
     *  (testEnumerationMapping.pure:162-166, {@code testEnumInRelation}): names joined {@code ", "}, an empty cell
     *  {@code TDSNull}, the rows joined by a newline and none after the last. */
    static String tdsCsvProperty(List<Col> cols, List<? extends List<?>> rows) {
        return csv(cols, rows, true, ", ", false);
    }

    private static String csv(List<Col> cols, List<? extends List<?>> rows, boolean renderTdsNull, String headerSep,
            boolean trailingNewline) {
        StringBuilder b = new StringBuilder(64 + rows.size() * 32);
        for (int i = 0; i < cols.size(); i++) {
            b.append(i == 0 ? "" : headerSep).append(escapeCsv(cols.get(i).name()));
        }
        b.append('\n');
        for (int r = 0; r < rows.size(); r++) {
            List<?> row = rows.get(r);
            if (row.size() != cols.size()) {
                throw new IllegalArgumentException("a row of " + row.size() + " cells under " + cols.size() + " columns");
            }
            b.append(r == 0 ? "" : "\n");
            for (int i = 0; i < cols.size(); i++) {
                b.append(i == 0 ? "" : ",").append(csvCell(row.get(i), cols.get(i).type(), renderTdsNull));
            }
        }
        return trailingNewline ? b.append('\n').toString() : b.toString();
    }

    /** A cell, legend-engine's {@code toCSVString} (helperFunctions.pure:229-243): an empty one nothing, or
     *  {@code TDSNull} when asked; a date with a time of day ({@code hasHour}) as {@code %t{yyyy-MM-dd HH:mm:ss}},
     *  escaped, one without as {@code %t{yyyy-MM-dd}}; anything else its {@code toString()}, escaped. */
    static String csvCell(@com.legend.base.Nullable Object v, String type, boolean renderTdsNull) {
        met("csv", v, type);
        if (v == null) {
            return renderTdsNull ? Presenter.TDS_NULL : "";
        }
        if (v instanceof PureDateLiteral d) {
            if (!d.precision().atLeast(PureDateLiteral.Precision.HOUR)) {
                return d.canonicalText();
            }
            // SimpleDateTimeFormat (dateExtension.pure:384-387): seconds, a space for the T, no fraction, no zone
            return escapeCsv(d.toNineDigits().substring(0, d.toNineDigits().length() - 10).replace('T', ' '));
        }
        return escapeCsv(value(v, type));
    }

    /** {@code escapeCSVString} (helperFunctions.pure:245-250): quoted when the text holds a comma, a quote, a line feed
     *  or a carriage return, its quotes doubled. */
    static String escapeCsv(String s) {
        return s.contains(",") || s.contains("\"") || s.contains("\n") || s.contains("\r")
                ? "\"" + s.replace("\"", "\"\"") + "\"" : s;
    }

    // ---- format's %r of a String (legend-pure's toRepresentation.pure:43) -----------------------------------------

    /** legend-pure's {@code toRepresentation} of a String: quoted, {@code \}, {@code '} and a NEWLINE escaped (the
     *  audit's EDGE-3: the SQL form leaves the newline raw). */
    static String representation(String s) {
        return "'" + s.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n") + "'";
    }

    // ---- a value's toString, as legend-pure prints the value a TDS holds ----------------------------------------

    /**
     * legend-pure's {@code toString()} of a cell's value. What a TDS holds is legend-engine's {@code TestTDS}'s
     * (TestTDS.java:180-367): an Integer a long; a Float or a Number a double (a whole number cast to one); a Decimal
     * a BigDecimal; a date kind an instant read back at three subsecond digits; a Boolean; a text. Its print is the
     * interpreter's name of that value (ToString.java; SimpleFloatCoreInstance, SimpleDecimalCoreInstance:
     * {@code toPlainString}).
     */
    static String value(Object v, String type) {
        return switch (v) {
            // a whole number under a Float column is a Float (TestTDS.java:246-253: "(double) bVal")
            case Long l -> floatTyped(type) ? CellText.carriedFloat((double) l) : Long.toString(l);
            case Integer i -> floatTyped(type) ? CellText.carriedFloat((double) i) : Integer.toString(i);
            case BigInteger i -> floatTyped(type) ? CellText.carriedFloat(new BigDecimal(i)) : i.toString();
            // FloatCoreInstance.canonicalizeBigDecimal(BigDecimal.valueOf(d)).toPlainString(): the shortest digits, a
            // point always, never an exponent -- CellText.carriedFloat's rule
            case Double d -> CellText.carriedFloat(d);
            // a decimal the database computed under a Float or Number column prints as the Float it is declared
            // (81.180 is 81.18); a Decimal column's at its own scale (SimpleDecimalCoreInstance: toPlainString)
            case BigDecimal d -> floatTyped(type) || type.equals("Number") ? CellText.carriedFloat(d) : d.toPlainString();
            case Boolean x -> x.toString();
            case String s -> s;
            case PureDateLiteral d -> date(d);
            case PureTimeLiteral t -> t.canonicalText();
            default -> throw new IllegalArgumentException("legend-pure's text of a " + v.getClass().getSimpleName()
                    + " cell under a " + type + " column is not defined at the edge (" + v + ")");
        };
    }

    /** The spike's census: each (column type, carrier) pair the printer met, printed once per JVM. */
    private static final java.util.Set<String> MET = java.util.concurrent.ConcurrentHashMap.newKeySet();

    private static void met(String where, @com.legend.base.Nullable Object v, String type) {
        String carrier = v == null ? "null" : v instanceof PureDateLiteral d ? "PureDateLiteral/" + d.precision()
                : v.getClass().getSimpleName();
        String shown = type.matches("[A-Z][A-Za-z]*(\\(.*\\))?") || type.equals(RowDecode.VARIANT) ? type : "<a path>";
        if (MET.add(where + " " + shown + " " + carrier)) {
            System.out.println("[H11-cell] " + where + " type=" + shown + " carrier=" + carrier + " first="
                    + System.getProperty("legend.test.current"));
        }
    }

    private static boolean floatTyped(String type) {
        return type.equals("Float");
    }

    /**
     * A date cell. A value with no time of day is its day. A time-bearing one is legend-pure's print of the value a
     * TDS holds: {@code DateFunctions.fromDate(new Date(nanos / 1000000))} (TestTDS.java:209), which is
     * {@code fromInstant(instant, 3)} (DateFunctions.java:267): EXACTLY three subsecond digits, the finer ones dropped,
     * then {@code +0000}.
     */
    static String date(PureDateLiteral d) {
        if (!d.precision().atLeast(PureDateLiteral.Precision.HOUR)) {
            return d.canonicalText();
        }
        String nine = d.toNineDigits();
        return nine.substring(0, nine.length() - 6) + "+0000";
    }
}
