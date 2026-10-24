// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;

import java.io.IOException;
import java.math.BigDecimal;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * THE RUNNER (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 3): an {@link ExecutionPlan} run with a caller's
 * parameter values, with no model, no compiler and no dialect — what runs is the plan's own statement, reviewed when the
 * plan was made. It checks and converts the caller's values ({@link PlanParameters}), takes the session for the
 * statement's target ({@link PlanSessions}: shared, set up once, by the target's content), checks it is the database and
 * server version the statement is written for, runs the target's session statements, binds each slot to its
 * parameter's value as the slot says (writing a value's type where the statement leaves it to the value, H2's), and
 * passes on the text the database writes. A plan of several steps (a write plan, docs/WRITES_DESIGN_2026_10_18.md §8,
 * leg 7) runs them all on one session, in one transaction where one changes the database, its answer held until they
 * have run.
 */
public final class PlanRunner {

    private PlanRunner() {
    }

    /**
     * Runs {@code plan} with {@code values} (by parameter name, each as legend-engine's execute API makes it from a
     * request's value), on a session of {@code sessions}, writing the result's text to {@code out}, which is flushed
     * and never closed.
     */
    public static void run(ExecutionPlan plan, Map<String, ?> values, PlanSessions.Source sessions, Writer out)
            throws IOException {
        Map<String, PlanParameters.Checked> checked = PlanParameters.check(plan.parameters(), values);
        switch (plan.root()) {
            case ExecutionPlan.TextResult text -> text(text, checked, sessions, out);
            case ExecutionPlan.TdsResult tds -> throw tdsResult();
            case ExecutionPlan.Sequence sequence -> sequence(sequence, checked, sessions, out);
            case ExecutionPlan.Update update -> throw new IllegalStateException("a plan whose answer is an update: a"
                    + " plan's answer is a result, its text");
            case ExecutionPlan.Check check -> throw new IllegalStateException("a plan whose answer is a check: a"
                    + " plan's answer is a result, its text");
        }
    }

    private static com.legend.error.NotImplementedException tdsResult() {
        return new com.legend.error.NotImplementedException("a plan whose rows the runner decodes (a TDS result) is"
                + " not run yet: phase 3 of the execution plan program");
    }

    private static void text(ExecutionPlan.TextResult text, Map<String, PlanParameters.Checked> checked,
            PlanSessions.Source sessions, Writer out) throws IOException {
        ExecutionPlan.Sql sql = text.sql();
        nullTypes(sql);
        try (Sessions.Session session = sessions.open(sql.target())) {
            Connection c = session.connection();
            begin(c, sql.target());
            writeText(text, checked, c, out);
        } catch (SQLException e) {
            throw PlanSessions.dataError(e);
        }
    }

    /**
     * A plan of several steps (docs/WRITES_DESIGN_2026_10_18.md §8, leg 7): every step on one session, in order;
     * where a step changes the database, all of them in one transaction ({@link Executor#inTransaction}: a caller's
     * open one joined), each change's count kept under its name for the slots after it. Its answer is its one result
     * step's text, held until the steps have all run (and their changes committed), then written to {@code out}.
     */
    private static void sequence(ExecutionPlan.Sequence sequence, Map<String, PlanParameters.Checked> checked,
            PlanSessions.Source sessions, Writer out) throws IOException {
        ExecutionPlan.Target target = null;
        int results = 0;
        boolean changes = false;
        for (ExecutionPlan.Node step : sequence.steps()) {
            ExecutionPlan.Sql sql = switch (step) {
                case ExecutionPlan.Update u -> {
                    changes = true;
                    if (u.count() != null && checked.containsKey(u.count())) {
                        throw new IllegalStateException("a write's count is named '" + u.count() + "', as a declared"
                                + " parameter is");
                    }
                    yield u.sql();
                }
                case ExecutionPlan.Check k -> k.sql();
                case ExecutionPlan.TextResult t -> {
                    results++;
                    yield t.sql();
                }
                case ExecutionPlan.TdsResult t -> throw tdsResult();
                case ExecutionPlan.Sequence s -> throw new IllegalStateException("a sequence inside a sequence: a"
                        + " plan's steps are one list");
            };
            nullTypes(sql);
            if (target == null) {
                target = sql.target();
            } else if (!target.equals(sql.target())) {
                throw new com.legend.error.NotImplementedException("a plan whose steps name different targets: its"
                        + " steps run on one session, in one transaction");
            }
        }
        if (results != 1) {
            throw new IllegalStateException("a sequence of " + results + " result steps: a plan's answer is one");
        }
        ExecutionPlan.Target on = java.util.Objects.requireNonNull(target, "a sequence has a step");
        boolean writes = changes;
        try (Sessions.Session session = writes ? sessions.openToChange(on) : sessions.open(on)) {
            Connection c = session.connection();
            begin(c, on);
            java.io.StringWriter held = new java.io.StringWriter();
            Executor.inTransaction(c, () -> {
                try {
                    steps(sequence, checked, c, held);
                } catch (SQLException e) {
                    throw dataError(e, c);
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
                return null;
            });
            out.write(held.toString());
            out.flush();
        } catch (SQLException e) {
            throw PlanSessions.dataError(e);
        }
    }

    /** The steps of {@code sequence} run on {@code c}, the result's text written to {@code held}. */
    private static void steps(ExecutionPlan.Sequence sequence, Map<String, PlanParameters.Checked> checked,
            Connection c, Writer held) throws SQLException, IOException {
        Map<String, PlanParameters.Checked> values = new java.util.HashMap<>(checked);
        for (ExecutionPlan.Node step : sequence.steps()) {
            switch (step) {
                case ExecutionPlan.Update u -> {
                    long count;
                    String statement = statement(u.sql(), values);
                    try (PreparedStatement st = c.prepareStatement(statement)) {
                        bound(c, st, u.sql(), statement, values);
                        count = st.executeUpdate();
                    }
                    if (u.count() != null) {
                        // (the database's count: a value it produced, bound where a later step reads it)
                        values.put(u.count(), new PlanParameters.One(count));
                    }
                }
                case ExecutionPlan.Check k -> {
                    String statement = statement(k.sql(), values);
                    try (PreparedStatement st = c.prepareStatement(statement)) {
                        bound(c, st, k.sql(), statement, values);
                        try (ResultSet rs = st.executeQuery()) {
                            while (rs.next()) {
                                // each row read, so the database computes what it may raise; its values are dropped
                            }
                        }
                    }
                }
                case ExecutionPlan.TextResult t -> writeText(t, values, c, held);
                case ExecutionPlan.TdsResult t -> throw tdsResult();
                case ExecutionPlan.Sequence s -> throw new IllegalStateException("a sequence inside a sequence");
            }
        }
    }

    /** A database's failure on {@code session}, its raise unwrapped (one the session holds: Postgres's). */
    private static com.legend.error.DataError dataError(SQLException e, Connection session) {
        SQLException un = RaisedErrors.unwrapped(e, session);
        return new com.legend.error.DataError(String.valueOf(un.getMessage()), un);
    }

    /** Every one-value slot's null type checked, before any session is opened. */
    private static void nullTypes(ExecutionPlan.Sql sql) {
        for (ExecutionPlan.Slot slot : sql.slots()) {
            if (slot.binding() instanceof ExecutionPlan.Binding.One one) {
                nullType(one, slot);
            }
        }
    }

    /** The session checked against the target, and the target's session statements run on it. */
    private static void begin(Connection c, ExecutionPlan.Target target) throws SQLException {
        check(c, target);
        try (var origin = StatementOrigin.enter(StatementOrigin.SESSION); Statement st = c.createStatement()) {
            for (String s : target.session()) {
                StatementOrigin.sent(s);
                st.execute(s);
            }
        }
    }

    /** {@code st}, prepared from {@code sql}'s {@code statement}, each slot bound to its parameter's value, then
     *  marked sent. */
    private static void bound(Connection c, PreparedStatement st, ExecutionPlan.Sql sql, String statement,
            Map<String, PlanParameters.Checked> values) throws SQLException {
        List<ExecutionPlan.Slot> slots = sql.slots();
        for (int i = 0; i < slots.size(); i++) {
            bind(c, st, i + 1, slots.get(i), value(values, slots.get(i), i + 1));
        }
        StatementOrigin.sent(statement);
    }

    /** A text result's statement run on {@code c}, its text written to {@code out}. */
    private static void writeText(ExecutionPlan.TextResult text, Map<String, PlanParameters.Checked> checked,
            Connection c, Writer out) throws SQLException, IOException {
        String statement = statement(text.sql(), checked);
        try (PreparedStatement st = c.prepareStatement(statement)) {
            bound(c, st, text.sql(), statement, checked);
            writeRows(text, st, out);
        }
    }

    /** The text result's rows, as {@code st} answers them, written to {@code out}. */
    private static void writeRows(ExecutionPlan.TextResult text, PreparedStatement st, Writer out)
            throws SQLException, IOException {
        try (ResultSet rs = st.executeQuery()) {
            switch (text.format()) {
                case CSV, JSON -> {
                    if (!rs.next()) {
                        throw new IllegalStateException("the plan's statement wrote no text: no row");
                    }
                    String whole = text(rs);
                    out.write(whole == null ? "" : whole);
                }
                case JSON_PER_ROW -> {
                    // one JSON object per row: the array's brackets and commas are the runner's
                    out.write('[');
                    boolean first = true;
                    while (rs.next()) {
                        if (!first) {
                            out.write(',');
                        }
                        first = false;
                        String row = text(rs);
                        out.write(row != null ? row : "null");
                        out.flush();
                    }
                    out.write(']');
                }
            }
            out.flush();
        }
    }

    /** The text the database wrote in the row's one column: carried, never read as a value (Charter C1.2). */
    private static @com.legend.base.Nullable String text(ResultSet rs) throws SQLException {
        return rs.getString(1);
    }

    /** The session is the database the statement is written for, and — where the statement's spelling depends on the
     *  server's version — of a version it is written for; anything else is refused by name, never run. */
    private static void check(Connection c, ExecutionPlan.Target target) {
        Sessions.check(c, target.database().type());
        switch (target.servers()) {
            case ExecutionPlan.Servers.Every every -> {
                // the spelling depends on no version
            }
            case ExecutionPlan.Servers.Versions versions -> {
                String version = Sessions.version(c);
                if (versions.prefixes().stream().noneMatch(version::startsWith)) {
                    throw new com.legend.error.NotImplementedException("the plan's statements are written for "
                            + target.database().type() + " " + versions.prefixes() + ", and the session is "
                            + target.database().type() + " " + version + ": not run");
                }
            }
        }
    }

    /** {@code slot}'s parameter's checked value. */
    private static PlanParameters.Checked value(Map<String, PlanParameters.Checked> checked, ExecutionPlan.Slot slot,
            int index) {
        PlanParameters.Checked value = checked.get(slot.parameter());
        if (value == null) {
            throw new IllegalStateException("slot " + index + " names parameter '" + slot.parameter()
                    + "', which the plan does not declare");
        }
        return value;
    }

    /** The statement to prepare: the plan's, with each type hole filled with its value's type (a statement with none is
     *  the plan's as it is). */
    private static String statement(ExecutionPlan.Sql sql, Map<String, PlanParameters.Checked> checked) {
        StringBuilder out = new StringBuilder();
        int from = 0;
        List<ExecutionPlan.Slot> slots = sql.slots();
        for (int i = 0; i < slots.size(); i++) {
            ExecutionPlan.Slot slot = slots.get(i);
            if (slot.binding() instanceof ExecutionPlan.Binding.One one && one.hole() != null) {
                ExecutionPlan.TypeHole hole = one.hole();
                out.append(sql.statement(), from, hole.at()).append(typeOf(hole, value(checked, slot, i + 1), slot));
                from = hole.at();
            }
        }
        return out.append(sql.statement().substring(from)).toString();
    }

    /** The type a hole takes for {@code value}: the spelling of its kind, with its own digits where the spelling takes
     *  them; an absent value's, its type's name alone (a null has no digits to keep). */
    private static String typeOf(ExecutionPlan.TypeHole hole, PlanParameters.Checked value, ExecutionPlan.Slot slot) {
        Object v = switch (value) {
            case PlanParameters.One one -> one.value();
            case PlanParameters.None none -> null;
            case PlanParameters.Many many -> throw new IllegalStateException("slot of '" + slot.parameter()
                    + "' binds one value, given a list");
        };
        ExecutionPlan.TypeSpelling spelling = spelling(hole, v == null ? hole.absent() : kindOf(v, slot), slot);
        if (v == null) {
            return spelling.name();
        }
        BigDecimal digits = v instanceof Double d ? BigDecimal.valueOf(d) : v instanceof BigDecimal bd ? bd : null;
        return switch (spelling.digits()) {
            case NONE -> spelling.name();
            case PRECISION -> spelling.name() + "(" + digitsOf(digits, slot).precision() + ")";
            case PRECISION_AND_SCALE -> spelling.name() + "(" + digitsOf(digits, slot).precision() + ","
                    + digitsOf(digits, slot).scale() + ")";
        };
    }

    private static ExecutionPlan.TypeSpelling spelling(ExecutionPlan.TypeHole hole, ExecutionPlan.ValueKind kind,
            ExecutionPlan.Slot slot) {
        ExecutionPlan.TypeSpelling spelling = hole.types().get(kind);
        if (spelling == null) {
            throw new IllegalStateException("slot of '" + slot.parameter() + "' is given a value of kind " + kind
                    + ", and its type hole spells only " + hole.types().keySet());
        }
        return spelling;
    }

    private static final java.time.format.DateTimeFormatter DATE_TIME_TEXT =
            java.time.format.DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSSSSS", java.util.Locale.ROOT);

    /** {@code t} as the text its cast reads: to the nanosecond, or to {@code fraction} digits of a second, the finer ones
     *  cut, as its literal's are. */
    private static String dateTimeText(LocalDateTime t, @com.legend.base.Nullable Integer fraction) {
        int unit = 1;
        for (int digits = fraction == null ? 9 : fraction; digits < 9; digits++) {
            unit *= 10;
        }
        return DATE_TIME_TEXT.format(t.withNano(t.getNano() - t.getNano() % unit));
    }

    private static BigDecimal digitsOf(@com.legend.base.Nullable BigDecimal digits, ExecutionPlan.Slot slot) {
        if (digits == null) {
            throw new IllegalStateException("slot of '" + slot.parameter() + "' spells a type with a value's digits,"
                    + " given a value with none");
        }
        return digits;
    }

    /** The kind of a checked value (one {@link PlanParameters} makes). */
    private static ExecutionPlan.ValueKind kindOf(Object v, ExecutionPlan.Slot slot) {
        return switch (v) {
            case Long l -> ExecutionPlan.ValueKind.INTEGER;
            case BigDecimal d -> ExecutionPlan.ValueKind.DECIMAL;
            case Double d -> ExecutionPlan.ValueKind.FLOATING;
            case LocalDate d -> ExecutionPlan.ValueKind.DATE;
            case LocalDateTime t -> t.getNano() % 1000 == 0 ? ExecutionPlan.ValueKind.DATE_TIME
                    : ExecutionPlan.ValueKind.DATE_TIME_NANOS;
            default -> throw new IllegalStateException("slot of '" + slot.parameter() + "' is typed by its value,"
                    + " given " + v.getClass().getSimpleName() + ", which is of no value kind");
        };
    }

    /** Slot {@code index} bound to its parameter's checked value, as the slot says: one value (an absent one a null of
     *  the slot's type), or a list as one array of the slot's element type (an absent one an empty array). */
    private static void bind(Connection c, PreparedStatement st, int index, ExecutionPlan.Slot slot,
            PlanParameters.Checked value) throws SQLException {
        switch (slot.binding()) {
            case ExecutionPlan.Binding.Array array -> {
                List<Object> items = switch (value) {
                    case PlanParameters.Many many -> many.values();
                    case PlanParameters.None none -> List.of();
                    case PlanParameters.One one -> List.of(one.value());
                };
                st.setArray(index, c.createArrayOf(array.elementSqlType(),
                        items.stream().map(PlanRunner::arrayElement).toArray()));
            }
            case ExecutionPlan.Binding.One one -> {
                switch (value) {
                    case PlanParameters.One v -> {
                        ExecutionPlan.TypeHole hole = one.hole();
                        if (hole != null && v.value() instanceof LocalDateTime t) {
                            // a date-time in a cast is passed as its text: the drivers pass no digit finer than a
                            // microsecond alike (DuckDB's cuts it, Postgres's rounds it; probes/timestamp-results.txt)
                            st.setString(index, dateTimeText(t, spelling(hole, kindOf(t, slot), slot).fraction()));
                        } else {
                            st.setObject(index, v.value());
                        }
                    }
                    case PlanParameters.None none -> st.setNull(index, nullType(one, slot));
                    case PlanParameters.Many many -> throw new IllegalStateException("slot " + index + " binds one"
                            + " value of '" + slot.parameter() + "', given a list");
                }
            }
        }
    }

    /** The JDBC type number of {@code one}'s null: its type's name is a {@code java.sql.JDBCType}'s, or the plan is refused
     *  by name (checked for every slot before any session is opened). */
    private static int nullType(ExecutionPlan.Binding.One one, ExecutionPlan.Slot slot) {
        try {
            return java.sql.JDBCType.valueOf(one.nullType()).getVendorTypeNumber();
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("slot of '" + slot.parameter() + "': its null type '" + one.nullType()
                    + "' is no JDBC type", e);
        }
    }

        /** An array element as the drivers take it: a timestamp as {@code java.sql.Timestamp} (measured,
     *  probes/list-results.txt), every other value as it is. */
    private static Object arrayElement(Object v) {
        return v instanceof LocalDateTime t ? java.sql.Timestamp.valueOf(t) : v;
    }
}
