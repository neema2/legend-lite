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
 * passes on the text the database writes.
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
            case ExecutionPlan.TdsResult tds -> throw new com.legend.error.NotImplementedException("a plan whose rows"
                    + " the runner decodes (a TDS result) is not run yet: phase 3 of the execution plan program");
            case ExecutionPlan.Sequence sequence -> throw new com.legend.error.NotImplementedException("a plan of"
                    + " several steps is not run yet: phase 2 of the execution plan program");
        }
    }

    private static void text(ExecutionPlan.TextResult text, Map<String, PlanParameters.Checked> checked,
            PlanSessions.Source sessions, Writer out) throws IOException {
        ExecutionPlan.Sql sql = text.sql();
        try (Sessions.Session session = sessions.open(sql.target())) {
            Connection c = session.connection();
            check(c, sql.target());
            try (var origin = StatementOrigin.enter(StatementOrigin.SESSION); Statement st = c.createStatement()) {
                for (String s : sql.target().session()) {
                    StatementOrigin.sent(s);
                    st.execute(s);
                }
            }
            String statement = statement(sql, checked);
            try (PreparedStatement st = c.prepareStatement(statement)) {
                List<ExecutionPlan.Slot> slots = sql.slots();
                for (int i = 0; i < slots.size(); i++) {
                    bind(c, st, i + 1, slots.get(i), value(checked, slots.get(i), i + 1));
                }
                StatementOrigin.sent(statement);
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
        } catch (SQLException e) {
            throw PlanSessions.dataError(e);
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
                    case PlanParameters.None none -> st.setNull(index, java.sql.JDBCType.valueOf(one.nullType())
                            .getVendorTypeNumber());
                    case PlanParameters.Many many -> throw new IllegalStateException("slot " + index + " binds one"
                            + " value of '" + slot.parameter() + "', given a list");
                }
            }
        }
    }

    /** An array element as the drivers take it: a timestamp as {@code java.sql.Timestamp} (measured,
     *  probes/list-results.txt), every other value as it is. */
    private static Object arrayElement(Object v) {
        return v instanceof LocalDateTime t ? java.sql.Timestamp.valueOf(t) : v;
    }
}
