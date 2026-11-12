// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.executionplan.ExecutionPlan;
import com.legend.model.DatabaseDefinition;
import com.legend.model.RelationalDataType;
import com.legend.sql.dialect.HostOperation;
import com.legend.sql.dialect.SqlDialect;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * THE TEST-DATA GENERATOR'S SYNTHESIZE, its simple slice (the user's ruling of 2026-11-12: "Let's do the most simple
 * thing that will work on duckdb for now - can extend later"; register S303; the roadmap is
 * docs/data-delivery-homework/SYNTHESIZE_DESIGN_2026_11_12.md). {@code count} rows the database generates into a table
 * the model declares, as a plan: ONE {@link ExecutionPlan.Load.Cadence#HOST_SESSION} load whose steps make the declared
 * table, empty, and fill it with one statement -- each column's value a formula of the row's number
 * ({@link HostOperation.Formula}), computed by the database. No row is in the plan.
 *
 * <p>Each column's formula, by its declared type, unless a {@link Knob} names it:
 * <ul>
 *   <li>a text: {@code <column>_1} .. {@code <column>_5}, in turn; a knob's values in turn;</li>
 *   <li>a whole number: 0..9999 (a TINYINT 0..99), spread; a knob's range;</li>
 *   <li>a decimal: 0..9999 (or less, as its precision holds) at its scale, spread; a FLOAT, DOUBLE or REAL at two
 *       places; a knob's range at its scale;</li>
 *   <li>a date: the five years ending 2025-12-31 (a fixed window, never today), spread; a timestamp, the same to the
 *       second; a knob's range of ISO dates;</li>
 *   <li>a BIT: true and false in turn;</li>
 *   <li>a knob's values for any of them, in turn.</li>
 * </ul>
 * The columns taken in turn (a list's values, a text's five, a BIT's two) are the row number's digits in mixed radix,
 * in declaration order: the first's index is {@code i mod k1}, the next's {@code (i / k1) mod k2}, and so on, so every
 * combination of their values occurs once the count reaches the product of their sizes (the orchestrator's fix of
 * 2026-11-12: one index for all of them paired the demo's four quarters with its four books, four pairs of sixteen).
 * A spread column's offset is the row number times its own multiplier modulo the prime 2^31 - 1, then modulo its
 * range's size, so no spread column moves with another or with a turn column. A range of more values than that prime,
 * and more than 2^32 rows, are refused by name.
 * No column is made NULL in this slice, a nullable one included. Any other type (binary, JSON, an array, a time) is
 * refused by name; so is a knob for a column the table does not declare, and any database but DuckDB (its host
 * operations refuse {@link HostOperation.Synthesize} by name).
 */
public final class Synthesize {

    private Synthesize() {
    }

    /** A column's knob, from the request: its values in turn, or a range (numbers, or ISO dates) at a scale. */
    public sealed interface Knob {
        /** The values in turn, each as written (a number's digits, or a text). */
        record Values(List<String> values) implements Knob {
            public Values {
                values = List.copyOf(values);
            }
        }

        /** The most values one column's list may hold: a knob describes a column, it does not carry its data (the B
         *  close's audit, SHOULD-FIX 4, 2026-11-12: a list of a million values would be rows in the plan's text, where
         *  data enters only by a load; DataCube's knobs hold ten at most). */
        int MOST_VALUES = 1_000;

        /** A range {@code [lo, hi]}: numbers at {@code scale} places (none: the column's), or ISO dates. */
        record Range(String lo, String hi, @com.legend.base.Nullable Integer scale) implements Knob {
        }
    }

    /** The first day of the default five-year window, and its last. */
    static final String FROM = "2021-01-01";
    static final String TO = "2025-12-31";

    /**
     * {@code count} rows generated into {@code table} on a database of {@code type}, written in {@code dialect}: the
     * plan of one {@link ExecutionPlan.Load.Cadence#HOST_SESSION} load (the table made as the model declares it, empty,
     * then filled by one statement), {@code knobs} by column name.
     */
    public static ExecutionPlan plan(SqlDialect dialect, com.legend.executionplan.DatabaseType type,
            LoadPlanner.DeclaredTable table, long count, Map<String, Knob> knobs) {
        DatabaseDefinition.TableDefinition def = table.ctx().findTableDefinition(table.database(),
                "default".equals(table.schema()) ? table.table() : table.schema() + "." + table.table())
                .orElseThrow(() -> new IllegalArgumentException("synthesize: the Database " + table.database()
                        + " declares no table " + table.schema() + "." + table.table()));
        for (String named : knobs.keySet()) {
            if (def.columns().stream().noneMatch(c -> c.name().equals(named))) {
                throw new IllegalArgumentException("synthesize: a knob for the column " + named + ", which the table "
                        + table.table() + " does not declare");
            }
        }
        if (count > HostOperation.Formula.MOST_ROWS) {
            throw new IllegalArgumentException("synthesize: " + count + " rows, more than the "
                    + HostOperation.Formula.MOST_ROWS + " one fill makes");
        }
        List<HostOperation.Formula> formulas = new ArrayList<>(def.columns().size());
        // the turn columns are the row number's digits in mixed radix, in declaration order: radix is the product of
        // the earlier ones' sizes (at most MOST_ROWS: a digit worth more is 0 on every row); each spread column takes
        // the next multiplier
        long radix = 1;
        int spreads = 0;
        for (DatabaseDefinition.ColumnDefinition c : def.columns()) {
            HostOperation.Formula f = formula(c, knobs.get(c.name()), table.table(), radix, multiplier(spreads));
            int k = switch (f) {
                case HostOperation.Formula.Cycle cycle -> cycle.values().size();
                case HostOperation.Formula.Numbered numbered -> numbered.k();
                case HostOperation.Formula.Alternate alternate -> 2;
                case HostOperation.Formula.Spread spread -> 1;
                case HostOperation.Formula.Days days -> 1;
            };
            if (f instanceof HostOperation.Formula.Spread || f instanceof HostOperation.Formula.Days) {
                spreads++;
            }
            radix = radix > HostOperation.Formula.MOST_ROWS / k ? HostOperation.Formula.MOST_ROWS : radix * k;
            formulas.add(f);
        }
        HostOperation.Synthesize fill = new HostOperation.Synthesize(
                HostOperation.Name.declared(table.schema(), table.table()), count, formulas);
        // the database's statement first: a database whose host operations do not write it refuses here, by name
        List<String> generating = com.legend.sql.dialect.HostOperations.of(dialect).statements(fill);
        // the table as the model declares it, empty: the seed's statements for its header alone (as a host's file is)
        com.legend.setup.LoadSteps made = com.legend.setup.CsvSeed.planned(com.legend.setup.CsvSeed.steps(
                table.schema() + "\n" + table.table() + "\n" + String.join(",", def.columns().stream()
                        .map(DatabaseDefinition.ColumnDefinition::name).toList()),
                table.database(), table.ctx(), dialect), dialect);
        // then the one statement filling it, by the planner's maker of statements
        com.legend.setup.LoadSteps.Builder filled = new com.legend.setup.LoadSteps.Builder();
        for (String sql : generating) {
            filled.statement(sql);
        }
        List<ExecutionPlan.LoadStep> steps = made.then(filled.build()).steps();
        // the target as the dialect writes it: its session's statements and its read-back (C2; the C checkpoint's code
        // audit, SHOULD-FIX 3, 2026-11-12: until then a target with no read-back, whatever the database)
        ExecutionPlan.Target target = Targets.of(new ExecutionPlan.Database.Platform(type, null), dialect);
        return ExecutionPlan.writing(List.of(), LoadPlanner.load(ExecutionPlan.Load.Cadence.HOST_SESSION, steps,
                target));
    }

    /** A primitive root of {@link HostOperation.Formula#MODULUS} (Fishman and Moore's first): the spread column at
     *  position j (0, 1, ...) multiplies by its (j + 1)-th power, so no two spread columns share a multiplier, and none
     *  lies near a small fraction of the modulus (a multiplier near one moves with the row number for millions of
     *  rows, as the primes just below 2^31 do). */
    private static final long ROOT = 742_938_285L;

    private static long multiplier(int position) {
        return java.math.BigInteger.valueOf(ROOT).modPow(java.math.BigInteger.valueOf(position + 1L),
                java.math.BigInteger.valueOf(HostOperation.Formula.MODULUS)).longValueExact();
    }

    /** {@code c}'s formula: its knob's, or its declared type's default; a turn column's digit worth {@code every}, a
     *  spread column's {@code multiplier}. */
    private static HostOperation.Formula formula(DatabaseDefinition.ColumnDefinition c,
            @com.legend.base.Nullable Knob knob, String table, long every, long multiplier) {
        String name = c.name();
        RelationalDataType t = c.dataType();
        Kind kind = kind(t, name, table);
        if (knob instanceof Knob.Values v) {
            if (v.values().isEmpty() || v.values().size() > Knob.MOST_VALUES) {
                throw new IllegalArgumentException("synthesize: the column " + name + "'s values number "
                        + v.values().size() + "; a list holds 1 to " + Knob.MOST_VALUES);
            }
            List<HostOperation.Literal> values = new ArrayList<>(v.values().size());
            for (String value : v.values()) {
                // each value read as its column's type here, at planning, never first by the database at run (the B
                // close's audit, SHOULD-FIX 3, 2026-11-12: only a number column's values were checked)
                switch (kind) {
                    case NUMBER -> number(value, name);
                    case DATE -> date(value, name);
                    case TIMESTAMP -> timestamp(value, name);
                    case BOOLEAN -> {
                        if (!value.equals("true") && !value.equals("false")) {
                            throw new IllegalArgumentException("synthesize: the boolean column " + name
                                    + "'s values are true and false, not '" + value + "'");
                        }
                    }
                    case TEXT -> { }
                }
                values.add(new HostOperation.Literal(value, kind == Kind.NUMBER));
            }
            return new HostOperation.Formula.Cycle(name, values, every);
        }
        if (knob instanceof Knob.Range r) {
            return switch (kind) {
                case NUMBER -> {
                    BigDecimal lo = number(r.lo(), name);
                    BigDecimal hi = number(r.hi(), name);
                    if (lo.compareTo(hi) > 0) {
                        throw new IllegalArgumentException("synthesize: the range of " + name + " runs from " + lo
                                + " down to " + hi);
                    }
                    int scale = r.scale() != null ? r.scale() : scale(t);
                    if (scale < 0 || scale > 0 && wholeNumber(t)) {
                        throw new IllegalArgumentException("synthesize: the whole-number column " + name + " at scale "
                                + scale);
                    }
                    within(hi.subtract(lo).movePointRight(scale), name);
                    yield new HostOperation.Formula.Spread(name, lo, hi, scale, multiplier);
                }
                case DATE, TIMESTAMP -> {
                    if (r.scale() != null) {
                        throw new IllegalArgumentException("synthesize: a scale for the " + kind.name()
                                .toLowerCase(java.util.Locale.ROOT) + " column " + name + ": a scale is a number's");
                    }
                    java.time.LocalDate from = date(r.lo(), name);
                    java.time.LocalDate to = date(r.hi(), name);
                    if (from.isAfter(to)) {
                        throw new IllegalArgumentException("synthesize: the range of " + name + " runs from " + from
                                + " back to " + to);
                    }
                    within(BigDecimal.valueOf(java.time.temporal.ChronoUnit.DAYS.between(from, to) + 1)
                            .multiply(BigDecimal.valueOf(kind == Kind.TIMESTAMP ? 86_400 : 1)).subtract(BigDecimal.ONE),
                            name);
                    yield new HostOperation.Formula.Days(name, from.toString(), to.toString(), kind == Kind.TIMESTAMP,
                            multiplier);
                }
                case TEXT, BOOLEAN -> throw new IllegalArgumentException("synthesize: a range for the " + kind.name()
                        .toLowerCase(java.util.Locale.ROOT) + " column " + name + ": give its values");
            };
        }
        return switch (kind) {
            case TEXT -> new HostOperation.Formula.Numbered(name, name + "_", 5, every);
            case NUMBER -> new HostOperation.Formula.Spread(name, BigDecimal.ZERO, defaultHigh(t), scale(t), multiplier);
            case DATE -> new HostOperation.Formula.Days(name, FROM, TO, false, multiplier);
            case TIMESTAMP -> new HostOperation.Formula.Days(name, FROM, TO, true, multiplier);
            case BOOLEAN -> new HostOperation.Formula.Alternate(name, every);
        };
    }

    /** What a declared type is made as. */
    private enum Kind { TEXT, NUMBER, DATE, TIMESTAMP, BOOLEAN }

    private static Kind kind(RelationalDataType t, String column, String table) {
        return switch (t) {
            case RelationalDataType.Varchar v -> Kind.TEXT;
            case RelationalDataType.Char_ ch -> Kind.TEXT;
            case RelationalDataType.BigInt b -> Kind.NUMBER;
            case RelationalDataType.Integer_ i -> Kind.NUMBER;
            case RelationalDataType.SmallInt s -> Kind.NUMBER;
            case RelationalDataType.TinyInt ti -> Kind.NUMBER;
            case RelationalDataType.Float_ f -> Kind.NUMBER;
            case RelationalDataType.Double_ d -> Kind.NUMBER;
            case RelationalDataType.Real r -> Kind.NUMBER;
            case RelationalDataType.Decimal d -> Kind.NUMBER;
            case RelationalDataType.Numeric n -> Kind.NUMBER;
            case RelationalDataType.Date_ d -> Kind.DATE;
            case RelationalDataType.Timestamp ts -> Kind.TIMESTAMP;
            case RelationalDataType.Bit b -> Kind.BOOLEAN;
            default -> throw new IllegalArgumentException("synthesize: the column " + table + "." + column + " of type "
                    + t.getClass().getSimpleName().replace("_", "").toUpperCase(java.util.Locale.ROOT) + " is not made by"
                    + " this slice (a text, a number, a date, a timestamp or a BIT is)");
        };
    }

    private static boolean wholeNumber(RelationalDataType t) {
        return t instanceof RelationalDataType.BigInt || t instanceof RelationalDataType.Integer_
                || t instanceof RelationalDataType.SmallInt || t instanceof RelationalDataType.TinyInt;
    }

    /** A number column's default scale: a whole number's none, a decimal's own, a float's two places; any other type
     *  has none, refused (a caller asks for a number column's alone). */
    private static int scale(RelationalDataType t) {
        return switch (t) {
            case RelationalDataType.BigInt b -> 0;
            case RelationalDataType.Integer_ i -> 0;
            case RelationalDataType.SmallInt s -> 0;
            case RelationalDataType.TinyInt ti -> 0;
            case RelationalDataType.Decimal d -> d.scale();
            case RelationalDataType.Numeric n -> n.scale();
            case RelationalDataType.Float_ f -> 2;
            case RelationalDataType.Double_ d -> 2;
            case RelationalDataType.Real r -> 2;
            default -> throw new IllegalStateException("synthesize: a scale asked of a non-number type " + t);
        };
    }

    /** A number column's default top: 9999 (a TINYINT 99; a decimal no more than its whole digits hold), at its
     *  scale's last step. */
    private static BigDecimal defaultHigh(RelationalDataType t) {
        int scale = scale(t);
        BigDecimal top = t instanceof RelationalDataType.TinyInt ? BigDecimal.valueOf(99) : BigDecimal.valueOf(9999);
        // a decimal holds no more than its whole digits
        int whole = t instanceof RelationalDataType.Decimal d ? d.precision() - d.scale()
                : t instanceof RelationalDataType.Numeric n ? n.precision() - n.scale() : Integer.MAX_VALUE;
        if (whole < 4) {
            top = BigDecimal.TEN.pow(Math.max(whole, 0)).subtract(BigDecimal.ONE);
        }
        // the top whole value and its last step: 9999.99 at two places
        return top.add(BigDecimal.ONE).subtract(BigDecimal.ONE.movePointLeft(scale)).max(BigDecimal.ZERO);
    }

    /** A range's steps past its first, refused by name beyond the modulus (its offsets lie below it). */
    private static BigDecimal within(BigDecimal steps, String column) {
        if (steps.compareTo(BigDecimal.valueOf(HostOperation.Formula.MODULUS - 1)) > 0) {
            throw new IllegalArgumentException("synthesize: the range of " + column + " has " + steps.add(BigDecimal.ONE)
                    .toPlainString() + " values, more than the " + HostOperation.Formula.MODULUS + " one column spreads");
        }
        return steps;
    }

    private static BigDecimal number(String text, String column) {
        try {
            return new BigDecimal(text);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("synthesize: the number column " + column + " takes no value '" + text
                    + "'");
        }
    }

    private static java.time.LocalDate date(String text, String column) {
        try {
            return java.time.LocalDate.parse(text);
        } catch (java.time.format.DateTimeParseException e) {
            throw new IllegalArgumentException("synthesize: the date column " + column + " takes ISO dates,"
                    + " not '" + text + "'");
        }
    }

    /** A timestamp column's value: an ISO date, or an ISO date and time (a space or a T between). */
    private static void timestamp(String text, String column) {
        try {
            if (text.length() <= 10) {
                java.time.LocalDate.parse(text);
            } else {
                java.time.LocalDateTime.parse(text.replaceFirst(" ", "T"));
            }
        } catch (java.time.format.DateTimeParseException e) {
            throw new IllegalArgumentException("synthesize: the timestamp column " + column + " takes ISO dates and"
                    + " times, not '" + text + "'");
        }
    }
}
