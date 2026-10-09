// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;

import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * THE RUNNER (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 3): an {@link ExecutionPlan} run with a caller's
 * parameter values, with no model, no compiler and no dialect — what runs is the plan's own statement, reviewed when the
 * plan was made. It checks and converts the values as legend-engine does ({@link PlanParameters}), takes the session
 * for the statement's target ({@link PlanSessions}: shared, set up once, by the target's content), checks it is the
 * database and server version the statement is written for, runs the target's session statements, binds each slot to
 * its parameter's value, and passes on the text the database writes.
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
            case ExecutionPlan.TextResult text -> text(text, plan.parameters(), checked, sessions, out);
            case ExecutionPlan.TdsResult tds -> throw new com.legend.error.NotImplementedException("a plan whose rows"
                    + " the runner decodes (a TDS result) is not run yet: phase 3 of the execution plan program");
            case ExecutionPlan.Sequence sequence -> throw new com.legend.error.NotImplementedException("a plan of"
                    + " several steps is not run yet: phase 2 of the execution plan program");
        }
    }

    private static void text(ExecutionPlan.TextResult text, List<ExecutionPlan.Parameter> declared,
            Map<String, PlanParameters.Checked> checked, PlanSessions.Source sessions, Writer out) throws IOException {
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
            try (PreparedStatement st = c.prepareStatement(sql.statement())) {
                List<ExecutionPlan.Slot> slots = sql.slots();
                for (int i = 0; i < slots.size(); i++) {
                    bind(c, st, i + 1, slots.get(i), declared, checked);
                }
                StatementOrigin.sent(sql.statement());
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

    /** Slot {@code index} bound to its parameter's checked value: one value, a list as one array of the slot's element
     *  type, or none (a typed null; an empty array). */
    private static void bind(Connection c, PreparedStatement st, int index, ExecutionPlan.Slot slot,
            List<ExecutionPlan.Parameter> declared, Map<String, PlanParameters.Checked> checked) throws SQLException {
        PlanParameters.Checked value = checked.get(slot.parameter());
        if (value == null) {
            throw new IllegalStateException("slot " + index + " names parameter '" + slot.parameter()
                    + "', which the plan does not declare");
        }
        String element = slot.arrayElementSqlType();
        if (element != null) {
            List<Object> items = switch (value) {
                case PlanParameters.Many many -> many.values();
                case PlanParameters.None none -> List.of();
                case PlanParameters.One one -> List.of(one.value());
            };
            st.setArray(index, c.createArrayOf(element, items.stream().map(PlanRunner::arrayElement).toArray()));
            return;
        }
        switch (value) {
            case PlanParameters.One one -> st.setObject(index, one.value());
            case PlanParameters.None none -> st.setNull(index, nullType(declared, slot.parameter()));
            case PlanParameters.Many many -> throw new IllegalStateException("slot " + index + " binds one value of '"
                    + slot.parameter() + "', given a list");
        }
    }

    /** An array element as the drivers take it: a timestamp as {@code java.sql.Timestamp} (measured,
     *  probes/list-results.txt), every other value as it is. */
    private static Object arrayElement(Object v) {
        return v instanceof LocalDateTime t ? java.sql.Timestamp.valueOf(t) : v;
    }

    /** The JDBC type of an absent value of the parameter {@code name}. */
    private static int nullType(List<ExecutionPlan.Parameter> declared, String name) {
        ExecutionPlan.Parameter p = declared.stream().filter(d -> d.name().equals(name)).findFirst()
                .orElseThrow(() -> new IllegalStateException("parameter '" + name + "' is not declared"));
        if (!p.enumValues().isEmpty()) {
            return java.sql.Types.VARCHAR;
        }
        return switch (p.type()) {
            case "Integer" -> java.sql.Types.BIGINT;
            case "Float", "Decimal" -> java.sql.Types.DECIMAL;
            case "String" -> java.sql.Types.VARCHAR;
            case "Boolean" -> java.sql.Types.BOOLEAN;
            case "StrictDate", "Date" -> java.sql.Types.DATE;
            case "DateTime" -> java.sql.Types.TIMESTAMP;
            default -> throw new IllegalStateException("an absent value of " + p.type() + " has no JDBC type");
        };
    }
}
