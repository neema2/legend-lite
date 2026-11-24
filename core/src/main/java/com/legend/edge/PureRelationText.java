// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.executionplan.ExecutionPlan;
import com.legend.result.CellText;
import com.legend.result.RowDecode;
import com.legend.values.PureDateLiteral;
import com.legend.values.PureTimeLiteral;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

/**
 * LEGEND-PURE'S TEXT OF A RELATION, WRITTEN IN JAVA FROM LITE'S TYPED ROWS (stage B's B2, B2b S1, 2026-11-24; the
 * plan of record's B2, the user's rulings of 2026-11-19 ~18:12 "Yes record it" and ~18:36 "Null is good"; the H11
 * prototype's printer, claude/compat-relation-text-spike, byte-identical on PCT's 431 DuckDB tables): a relation's
 * {@code toString} (its {@code #TDS} text), its {@code toString(rel, true)} (with each column's type and multiplicity),
 * legend-engine's {@code toCSV} of a TDS and a TDS's {@code csv} property, and the literal {@code replace} calls a
 * program applies to the printed text (the user, 2026-11-24: "also do the replace in java too it's really just more
 * string functions on the CSV"). Compatibility re-spells lite's typed output after the run (rule 3, tenet 3; G6: small
 * Java formatting at the edge): the database computed every value, and this class chooses only how each is written.
 *
 * <p>Every rule is read from the pinned sources (legend-engine 4.145.0, the relation functions' {@code toString.pure},
 * {@code s.pure}, and the relational store's {@code helperFunctions.pure}), named where it is used. A printer only: it
 * chooses the text of a value it holds whole, converts no kind, and defaults nothing; a cell it has no rule for is
 * refused by name (AGENTS.md invariant 4).
 */
public final class PureRelationText {

    private PureRelationText() {
    }

    /** A literal {@code replace(target, replacement)} applied to the printed text, in the program's order. */
    public record Replace(String target, String replacement) {
        public Replace {
            java.util.Objects.requireNonNull(target, "target");
            java.util.Objects.requireNonNull(replacement, "replacement");
        }
    }

    /** {@code text} with each of {@code replaces} applied in order, as Pure's {@code replace} does: every occurrence of
     *  the target, left to right, non-overlapping ({@code String.replace}, which legend-pure's {@code replace} native
     *  calls). An empty target is refused by name: Java and the databases disagree on what replacing nothing means. */
    public static String replaced(String text, List<Replace> replaces) {
        String out = text;
        for (Replace r : replaces) {
            if (r.target().isEmpty()) {
                throw new IllegalArgumentException("a replace of the empty string in a printed relation is not"
                        + " defined at the edge");
            }
            out = out.replace(r.target(), r.replacement());
        }
        return out;
    }

    // ---- toString: the #TDS text (toString.pure:24-35) -----------------------------------------------------------

    /** {@code meta::pure::functions::relation::toString(rel)}, which is {@code toString(rel, false)}. */
    public static String tds(List<ExecutionPlan.Column> columns, List<? extends List<?>> rows) {
        return tds(columns, rows, false);
    }

    /** {@code meta::pure::functions::relation::toString(rel, typesAndMuls)} (toString.pure:24-35): {@code #TDS}, a
     *  newline, three spaces and the names joined {@code ,} (each with {@code :Type[mul]} when {@code typesAndMuls}),
     *  a newline, each row as three spaces and its cells joined {@code ,}, the rows joined by a newline, a newline and
     *  {@code #}. No rows leaves the rows segment empty ({@code joinStrings('\n')} of nothing). */
    public static String tds(List<ExecutionPlan.Column> columns, List<? extends List<?>> rows, boolean typesAndMuls) {
        StringBuilder b = new StringBuilder(64 + rows.size() * 32).append("#TDS\n   ");
        for (int i = 0; i < columns.size(); i++) {
            ExecutionPlan.Column c = columns.get(i);
            b.append(i == 0 ? "" : ",").append(tdsName(c.name()));
            if (typesAndMuls) {
                b.append(':').append(printedType(c)).append('[').append(printedMultiplicity(c.multiplicity()))
                        .append(']');
            }
        }
        b.append('\n');
        for (int r = 0; r < rows.size(); r++) {
            List<?> row = checked(rows.get(r), columns);
            b.append(r == 0 ? "" : "\n").append("   ");
            for (int i = 0; i < columns.size(); i++) {
                b.append(i == 0 ? "" : ",").append(tdsCell(row.get(i), columns.get(i)));
            }
        }
        return b.append("\n#").toString();
    }

    /** A column's name in the header (toString.pure:27-30): trimmed, and quoted with {@code '} unless it starts with a
     *  quote or is {@code ^[a-zA-Z0-9_]+$}. */
    static String tdsName(String raw) {
        String name = raw.trim();
        boolean simple = name.startsWith("'") || name.matches("^[a-zA-Z0-9_]+$");
        return simple ? name : "'" + name + "'";
    }

    /** A column's type in the header (toString.pure:30, {@code _printGenericType}, toString.pure:37-44): the type's
     *  path, which for a primitive is its name. A type with type-variable values (a precise primitive's
     *  {@code Varchar(200)}) or a class is refused by name: the plan's column carries its Pure type's name, not the
     *  generic type legend-pure prints. */
    private static String printedType(ExecutionPlan.Column c) {
        return switch (c.type()) {
            case "Integer", "Float", "Decimal", "Number", "String", "Boolean", "Date", "StrictDate", "DateTime",
                    "StrictTime" -> c.type();
            default -> throw new IllegalArgumentException("legend-pure's toString(rel, true) of a column of type "
                    + c.type() + " ('" + c.name() + "') is not defined at the edge");
        };
    }

    /** legend-pure's {@code printMul} of a multiplicity: {@code 1}, {@code 0..1}, {@code *}, {@code 1..*}, {@code 2..5}. */
    static String printedMultiplicity(ExecutionPlan.Multiplicity m) {
        Integer upper = m.upper();
        if (upper == null) {
            return m.lower() == 0 ? "*" : m.lower() + "..*";
        }
        return m.lower() == upper ? Integer.toString(upper) : m.lower() + ".." + upper;
    }

    /** A cell, legend-engine's {@code s(value, type)} (s.pure:21-38): an empty one {@code null}, or {@code 'null'}
     *  under a Variant column; a Variant its text quoted with {@code '}, {@code \} and {@code '} escaped; a String
     *  holding <code>{</code> or {@code [} quoted the same way, any other String as it is; anything else its
     *  {@code toString()} ({@link #value}). */
    static String tdsCell(@com.legend.base.Nullable Object v, ExecutionPlan.Column c) {
        boolean variant = c.type().equals(RowDecode.VARIANT);
        if (v == null) {
            return variant ? "'null'" : "null";
        }
        if (variant) {
            if (!(v instanceof String json)) {
                throw refused("toString", v, c);
            }
            return quoted(json);
        }
        if (v instanceof String s) {
            return s.contains("{") || s.contains("[") ? quoted(s) : s;
        }
        return value(v, c);
    }

    /** {@code '} + the text with {@code \} and then {@code '} escaped by a backslash + {@code '} (s.pure:33-34). */
    private static String quoted(String s) {
        return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'";
    }

    // ---- toCSV (helperFunctions.pure:198-250) and a TDS's csv property --------------------------------------------

    /** {@code meta::relational::tests::csv::toCSV(tds)}, which is {@code toCSV(tds, false)} (helperFunctions.pure:198-
     *  201): {@link #csv(List, List, boolean)} with an empty cell written as nothing. */
    public static String csv(List<ExecutionPlan.Column> columns, List<? extends List<?>> rows) {
        return csv(columns, rows, false);
    }

    /** {@code toCSV(tds, renderTdsNull)} (helperFunctions.pure:203-212, with legend-engine's {@code SimpleDateTimeFormat}
     *  and {@code ISO8601DateFormat}): the names, each a cell's text ({@link #csvText}), joined {@code ,}, a newline;
     *  then each row's cells joined {@code ,}, the rows joined by {@code joinStrings('', '\n', '\n')} -- a newline
     *  between rows and one after the last, and with no rows the one newline. */
    public static String csv(List<ExecutionPlan.Column> columns, List<? extends List<?>> rows, boolean renderTdsNull) {
        StringBuilder b = new StringBuilder(64 + rows.size() * 32);
        for (int i = 0; i < columns.size(); i++) {
            b.append(i == 0 ? "" : ",").append(escapeCsv(columns.get(i).name()));
        }
        b.append('\n');
        for (int r = 0; r < rows.size(); r++) {
            List<?> row = checked(rows.get(r), columns);
            b.append(r == 0 ? "" : "\n");
            for (int i = 0; i < columns.size(); i++) {
                b.append(i == 0 ? "" : ",").append(csvCell(row.get(i), columns.get(i), renderTdsNull));
            }
        }
        return b.append('\n').toString();
    }

    /** A TDS's {@code csv} property (legend-pure's tds.pure:19) as the relational corpus's golden spells it
     *  (testEnumerationMapping.pure:162-166, {@code testEnumInRelation}; the edge's SQL composition until B2b, whose
     *  rule this keeps): the names joined {@code ", "}, an empty cell {@code TDSNull}, the rows joined by a newline,
     *  none after the last. */
    public static String tdsCsvProperty(List<ExecutionPlan.Column> columns, List<? extends List<?>> rows) {
        StringBuilder b = new StringBuilder(64 + rows.size() * 32);
        for (int i = 0; i < columns.size(); i++) {
            b.append(i == 0 ? "" : ", ").append(escapeCsv(columns.get(i).name()));
        }
        b.append('\n');
        for (int r = 0; r < rows.size(); r++) {
            List<?> row = checked(rows.get(r), columns);
            b.append(r == 0 ? "" : "\n");
            for (int i = 0; i < columns.size(); i++) {
                b.append(i == 0 ? "" : ",").append(csvCell(row.get(i), columns.get(i), true));
            }
        }
        return b.toString();
    }

    /** A cell, legend-engine's {@code toCSVString(value, dateTimeFormat, dateFormat, renderTdsNull)}
     *  (helperFunctions.pure:229-243): an empty one {@code TDSNull} when asked, else nothing; a date
     *  ({@code formatDateTime}, :238-243) with a time of day ({@code hasHour}) formatted {@code yyyy-MM-dd HH:mm:ss}
     *  and escaped, one without {@code yyyy-MM-dd} (not escaped); anything else its {@code toString()}, escaped. */
    static String csvCell(@com.legend.base.Nullable Object v, ExecutionPlan.Column c, boolean renderTdsNull) {
        if (v == null) {
            return renderTdsNull ? Presenter.TDS_NULL : "";
        }
        if (v instanceof PureDateLiteral d) {
            if (!d.precision().atLeast(PureDateLiteral.Precision.HOUR)) {
                return d.canonicalText();
            }
            // SimpleDateTimeFormat, 'yyyy-MM-dd HH:mm:ss' (dateExtension.pure): the seconds, a space for the T, no
            // fraction and no zone -- the nine-digit text without its point and nine digits (its year is unpadded,
            // so the cut is from the end)
            String nine = d.toNineDigits();
            return escapeCsv(nine.substring(0, nine.length() - 10).replace('T', ' '));
        }
        return escapeCsv(csvText(v, c));
    }

    /** A non-date cell's {@code toString()} for the CSV: a String as it is, anything else {@link #value}. */
    private static String csvText(Object v, ExecutionPlan.Column c) {
        return v instanceof String s ? s : value(v, c);
    }

    /** {@code escapeCSVString} (helperFunctions.pure:245-250): quoted with {@code "} when the text holds a comma, a
     *  quote, a line feed or a carriage return, its quotes doubled. */
    static String escapeCsv(String s) {
        return s.contains(",") || s.contains("\"") || s.contains("\n") || s.contains("\r")
                ? "\"" + s.replace("\"", "\"\"") + "\"" : s;
    }

    // ---- a value's toString, as legend-pure prints the value a TDS holds -----------------------------------------

    /**
     * legend-pure's {@code toString()} of a cell's value, by the value lite's typed row carries and its column's Pure
     * type. What a TDS holds is legend-engine's {@code TestTDS}'s (an Integer a long; a Float a double; a Decimal a
     * BigDecimal; a date an instant read back at three subsecond digits; a Boolean; a text), and its print is the
     * interpreter's name of that value:
     * <ul>
     *   <li>an Integer's digits;</li>
     *   <li>a Float, or a number the database computed under a Float column (an integer or a decimal it held), as a
     *       double: the shortest digits, a point always, never an exponent ({@link CellText#carriedFloat}, the one
     *       rule {@link Presenter#legendPure} prints a Float by);</li>
     *   <li>a Number column's cell by its own carrier: an integer's digits, a fraction-kind value (a double, or a
     *       decimal an aggregate collapsed to) as a Float -- the rule the edge's SQL composition kept until B2b
     *       (witness PCT's {@code testMax_Numbers_Relation_Aggregate}: {@code 81.18}, never {@code 81.180});</li>
     *   <li>a Decimal at its own scale ({@code toPlainString});</li>
     *   <li>a Boolean {@code true}/{@code false}; an enumeration's value, which lite carries as its name;</li>
     *   <li>a date ({@link #date}); a StrictTime as written.</li>
     * </ul>
     * Anything else -- a list, a map, a structure, a mixed column's tagged cell -- is refused by name.
     */
    static String value(Object v, ExecutionPlan.Column c) {
        String type = c.type();
        boolean asFloat = type.equals("Float");
        return switch (v) {
            case Long l -> asFloat ? CellText.carriedFloat((double) l) : Long.toString(l);
            case Integer i -> asFloat ? CellText.carriedFloat((double) i) : Integer.toString(i);
            case BigInteger i -> asFloat ? CellText.carriedFloat(new BigDecimal(i)) : i.toString();
            case Double d -> CellText.carriedFloat(d);
            case BigDecimal d -> asFloat || type.equals("Number") ? CellText.carriedFloat(d) : d.toPlainString();
            case Boolean x -> x.toString();
            case String s -> s;
            case PureDateLiteral d -> date(d);
            case PureTimeLiteral t -> t.canonicalText();
            default -> throw refused("toString", v, c);
        };
    }

    /**
     * A date cell. A value with no time of day is its day. One with a time is legend-pure's print of the value a TDS
     * holds, which re-reads it at the millisecond ({@code DateFunctions.fromDate(new Date(nanos / 1000000))}, legend-
     * engine's TestTDS; {@code fromInstant(instant, 3)}): exactly three subsecond digits, the finer ones dropped, then
     * {@code +0000} -- the edge's SQL composition's rule until B2b ({@code dateTimeTextTds}).
     */
    static String date(PureDateLiteral d) {
        if (!d.precision().atLeast(PureDateLiteral.Precision.HOUR)) {
            return d.canonicalText();
        }
        String nine = d.toNineDigits();
        return nine.substring(0, nine.length() - 6) + "+0000";
    }

    // ---- shared ---------------------------------------------------------------------------------------------------

    private static List<?> checked(List<?> row, List<ExecutionPlan.Column> columns) {
        if (row.size() != columns.size()) {
            throw new IllegalArgumentException("a row of " + row.size() + " cells under " + columns.size()
                    + " columns");
        }
        return row;
    }

    private static IllegalArgumentException refused(String what, Object v, ExecutionPlan.Column c) {
        return new IllegalArgumentException("legend-pure's " + what + " of a " + v.getClass().getSimpleName()
                + " cell under the " + c.type() + " column '" + c.name() + "' is not defined at the edge");
    }
}
