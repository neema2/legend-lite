// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.error.NotImplementedException;
import com.legend.executionplan.ExecutionPlan;
import com.legend.result.RowDecode;
import com.legend.values.PureDateLiteral;
import com.legend.values.PureTimeLiteral;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

/**
 * LEGEND-PURE'S TEXT OF A RELATION, WRITTEN IN JAVA FROM LITE'S TYPED ROWS (stage B's B2, B2b S1, 2026-11-24; the
 * plan of record's B2, the user's rulings of 2026-11-19 ~18:12 "Yes record it" and ~18:36 "Null is good"; the H11
 * prototype's printer, claude/compat-relation-text-spike): a relation's {@code toString} (its {@code #TDS} text),
 * legend-engine's {@code toCSV} of a TDS, a TDS's {@code csv} property, and the literal {@code replace} calls a program
 * applies to the printed text (the user, 2026-11-24: "also do the replace in java too it's really just more string
 * functions on the CSV"). Compatibility re-spells lite's typed output after the run (rule 3, tenet 3; G6: small Java
 * formatting at the edge): the database computed every value, and this class chooses only how each is written.
 *
 * <p>Every rule is read from the pinned sources (legend-engine 4.145.0's relation functions {@code toString.pure} and
 * {@code s.pure}, its relational store's {@code helperFunctions.pure}; legend-pure 5.99.0's {@code CompiledSupport}),
 * named where it is used. A printer only: it chooses the text of a value it holds whole, converts no kind but the one
 * its column declares (a Float cell is a double), and defaults nothing; a column or a cell it has no rule for is
 * refused by name (AGENTS.md invariant 4). {@code toString(rel, true)} is not written: lite's plan types a column by its
 * values (a {@code #TDS} column with no empty cell is {@code [1]}), where legend-pure prints {@code [0..1]}, and a
 * store's column as its plain Pure type, where legend-engine prints the precise primitive -- so its header would not be
 * legend-pure's; the door leaves it to the run, which refuses it by name as before.
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

    /** {@code text} with each of {@code replaces} applied in order, as Pure's {@code replace} does: Java's
     *  {@code String.replace}, which legend-pure's native calls in both its runtimes ({@code CompiledSupport.replace},
     *  the interpreter's {@code Replace}) -- every occurrence, left to right, non-overlapping. */
    public static String replaced(String text, List<Replace> replaces) {
        String out = text;
        for (Replace r : replaces) {
            out = out.replace(r.target(), r.replacement());
        }
        return out;
    }

    /** The columns refused before a run's rows are read: a mixed column (its cells tagged by kind, which a typed
     *  batch hands over without the tag) has no rule here. */
    public static void requirePrintable(List<ExecutionPlan.Column> columns) {
        for (ExecutionPlan.Column c : columns) {
            if (c.slot() != null && RowDecode.slotWord(c.slot()).equals("TAGGED")) {
                throw new NotImplementedException("legend-pure's text of a relation whose column '" + c.name()
                        + "' mixes kinds is not written at the edge");
            }
        }
    }

    // ---- toString: the #TDS text (toString.pure:19-35) -----------------------------------------------------------

    /** {@code meta::pure::functions::relation::toString(rel)}, which is {@code toString(rel, false)}
     *  (toString.pure:19-35): {@code #TDS}, a newline, three spaces and the names joined {@code ,}, a newline, each row
     *  as three spaces and its cells joined {@code ,}, the rows joined by a newline, a newline and {@code #}. No rows
     *  leaves the rows segment empty ({@code joinStrings('\n')} of nothing). */
    public static String tds(List<ExecutionPlan.Column> columns, List<? extends List<?>> rows) {
        requirePrintable(columns);
        StringBuilder b = new StringBuilder(64 + rows.size() * 32).append("#TDS\n   ");
        for (int i = 0; i < columns.size(); i++) {
            b.append(i == 0 ? "" : ",").append(tdsName(columns.get(i).name()));
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

    /** A cell, legend-engine's {@code s(value, type)} (s.pure:21-38): an empty one {@code null}, or {@code 'null'}
     *  under a Variant column; a Variant its text quoted with {@code '}, {@code \} and {@code '} escaped; a String --
     *  the value's runtime type, so a String column's, never an enumeration's, whose value lite carries as its name --
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
        if (v instanceof String s && c.type().equals("String")) {
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
     *  and {@code ISO8601DateFormat}): the names, each a cell's text, joined {@code ,}, a newline; then each row's cells
     *  joined {@code ,}, the rows joined by {@code joinStrings('', '\n', '\n')} -- a newline between rows and one after
     *  the last, and with no rows the one newline. */
    public static String csv(List<ExecutionPlan.Column> columns, List<? extends List<?>> rows, boolean renderTdsNull) {
        requirePrintable(columns);
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

    /** A TDS's {@code csv} property (legend-pure's tds.pure:19) as the relational corpus's one golden spells it
     *  (testEnumerationMapping.pure:162-166, {@code testEnumInRelation}): the names joined {@code ", "}, an empty cell
     *  {@code TDSNull}, each cell as {@code toCSV}'s, the rows joined by a newline and none after the last. */
    public static String tdsCsvProperty(List<ExecutionPlan.Column> columns, List<? extends List<?>> rows) {
        requirePrintable(columns);
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
     *  and escaped, one without {@code yyyy-MM-dd} (not escaped); anything else its {@code toString()}, escaped. A date
     *  the format cannot write (a year, a month, a time without its seconds: legend-pure's {@code format} raises on a
     *  missing component) is refused by name. */
    static String csvCell(@com.legend.base.Nullable Object v, ExecutionPlan.Column c, boolean renderTdsNull) {
        if (v == null) {
            return renderTdsNull ? Presenter.TDS_NULL : "";
        }
        if (v instanceof PureDateLiteral d) {
            return switch (d) {
                case PureDateLiteral.StrictDate day -> day.canonicalText();
                // SimpleDateTimeFormat, 'yyyy-MM-dd HH:mm:ss': the seconds, a space for the T, no fraction and no zone
                // -- the nine-digit text without its point and nine digits (yyyy writes the year as it is, unpadded)
                case PureDateLiteral.DateWithSecond s -> escapeCsv(withoutFraction(s));
                case PureDateLiteral.DateWithSubsecond s -> escapeCsv(withoutFraction(s));
                default -> throw refused("toCSV", v, c);
            };
        }
        return escapeCsv(v instanceof String s ? s : value(v, c));
    }

    private static String withoutFraction(PureDateLiteral d) {
        String nine = d.toNineDigits();
        return nine.substring(0, nine.length() - 10).replace('T', ' ');
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
     *   <li>a date ({@link #date}); a StrictTime as written.</li>
     * </ul>
     * Anything else -- a list, a map, a structure -- is refused by name.
     */
    static String value(Object v, ExecutionPlan.Column c) {
        String type = c.type();
        if (type.equals("Float")) {
            return switch (v) {
                case Double d -> pureFloat(d);
                case Long l -> pureFloat(l.doubleValue());
                case Integer i -> pureFloat(i.doubleValue());
                case BigInteger i -> pureFloat(i.doubleValue());
                case BigDecimal d -> pureFloat(d.doubleValue());
                default -> throw refused("toString", v, c);
            };
        }
        return switch (v) {
            case Long l -> Long.toString(l);
            case Integer i -> Integer.toString(i);
            case BigInteger i -> i.toString();
            case Double d -> pureFloat(d);
            case BigDecimal d -> type.equals("Number") ? pureFloat(d.doubleValue()) : d.toPlainString();
            case Boolean x -> x.toString();
            case String s -> s;
            case PureDateLiteral d -> date(d);
            case PureTimeLiteral t -> t.canonicalText();
            default -> throw refused("toString", v, c);
        };
    }

    /** legend-pure's print of a Float (legend-pure 5.99.0's {@code CompiledSupport}, lines 130-137 and 1271-1274):
     *  {@code 0.0} for a zero of either sign, else {@code DecimalFormat("0.0")} in English with up to 340 fraction
     *  digits -- a point always, never an exponent; an infinity {@code ∞}, as that format writes it. */
    static String pureFloat(double d) {
        if (d == 0.0d) {
            return "0.0";
        }
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
     * prints a TDS literal's {@code 1985-01-12T06:30:00Z} as {@code 1985-01-12T06:30:00.000+0000}).
     */
    static String date(PureDateLiteral d) {
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

    // ---- shared ---------------------------------------------------------------------------------------------------

    private static List<?> checked(List<?> row, List<ExecutionPlan.Column> columns) {
        if (row.size() != columns.size()) {
            throw new IllegalArgumentException("a row of " + row.size() + " cells under " + columns.size()
                    + " columns");
        }
        return row;
    }

    private static NotImplementedException refused(String what, Object v, ExecutionPlan.Column c) {
        return new NotImplementedException("legend-pure's " + what + " of a " + v.getClass().getSimpleName()
                + " cell under the " + c.type() + " column '" + c.name() + "' is not written at the edge");
    }
}
