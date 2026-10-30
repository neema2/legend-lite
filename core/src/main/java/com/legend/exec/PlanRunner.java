// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;

import java.io.IOException;
import java.math.BigDecimal;
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
 * hands its typed rows to a sink ({@link #stream}; {@link #rows} collects them): each cell read by the session from
 * the driver by the plan's column (docs/PLAN_AND_RESULT_MODEL_2026_10_18.md §3, the plan path program's step 7). A plan
 * of several steps (a write plan, docs/WRITES_DESIGN_2026_10_18.md §8, leg 7) runs them all on one session, in one
 * transaction where one changes the database, its answer held until they have run.
 */
public final class PlanRunner {

    private PlanRunner() {
    }

    /** A plan's typed rows: the columns, with their three type facts, and each row's cells decoded by them. */
    public record Rows(List<ExecutionPlan.Column> columns, List<List<Object>> rows) {
        public Rows {
            columns = List.copyOf(columns);
            rows = List.copyOf(rows);
        }
    }

    /**
     * Runs {@code plan}, whose answer is typed rows, with {@code values}, on a session of {@code sessions}, and
     * collects the whole answer ({@link com.legend.result.Collector}: the judges', the harnesses' and the tests'
     * writer, docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md R8): each row's cells read by the session's typed method
     * ({@link Statements#rows}: from the driver by the plan's columns).
     */
    public static Rows rows(ExecutionPlan plan, Map<String, ?> values, PlanSessions.Source sessions)
            throws IOException {
        com.legend.result.Collector collector = new com.legend.result.Collector();
        stream(plan, values, sessions, collector);
        com.legend.result.Collector.Collected c = collector.collected();
        return new Rows(c.columns(), c.rows());
    }

    /** {@link #stream(ExecutionPlan, Map, PlanSessions.Source, com.legend.result.ResultSink, int)} in batches of
     *  {@link com.legend.result.Batcher#DEFAULT_CAPACITY} rows. */
    public static void stream(ExecutionPlan plan, Map<String, ?> values, PlanSessions.Source sessions,
            com.legend.result.ResultSink sink) throws IOException {
        stream(plan, values, sessions, sink, com.legend.result.Batcher.DEFAULT_CAPACITY);
    }

    /**
     * Runs {@code plan}, whose answer is TYPED ROWS, with {@code values}, on a session of {@code sessions}: its columns
     * to {@code sink} once the statement has answered, then its rows in batches of up to {@code capacity} as they
     * arrive, then the end (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md R8). A tabular result streams; a keyed plan's
     * body streams once its keys are read; a plan of several steps holds its answer until its changes have committed,
     * then hands it on (no answer leaves for a change rolled back).
     */
    public static void stream(ExecutionPlan plan, Map<String, ?> values, PlanSessions.Source sessions,
            com.legend.result.ResultSink sink, int capacity) throws IOException {
        Map<String, PlanParameters.Checked> checked = PlanParameters.check(plan.parameters(), values);
        switch (plan.root()) {
            case ExecutionPlan.TdsResult tds -> {
                Streamed streamed = new Streamed(tds.columns(), sink, capacity);
                send(tds.sql(), checked, sessions, (st, statement, bound) ->
                        st.rows(statement, tds.sql().slots(), bound, tds.columns(), streamed::row));
                streamed.end();
            }
            case ExecutionPlan.Keyed keyed -> keyed(keyed, checked, sessions, new KeyedStream(sink, capacity));
            case ExecutionPlan.Sequence sequence -> {
                Held held = new Held();
                sequence(sequence, checked, sessions, held);
                Streamed streamed = new Streamed(held.columns, sink, capacity);
                for (List<Object> row : held.rows) {
                    streamed.row(row);
                }
                streamed.end();
            }
            case ExecutionPlan.Update update -> throw new IllegalStateException("a plan whose answer is an update: a"
                    + " plan's answer is a result");
            case ExecutionPlan.Check check -> throw new IllegalStateException("a plan whose answer is a check: a"
                    + " plan's answer is a result");
            case ExecutionPlan.Effect effect -> throw new IllegalStateException("a plan whose answer is an effect: a"
                    + " plan's answer is a result (an effect alone is sent by effect(...))");
        }
    }

    /**
     * The body driver's one change (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md, phase 5: every statement a body sends
     * is a plan's): {@code update} -- a store write, a staged table made or dropped -- on a session of
     * {@code sessions}, checked against its target and given the target's session statements as any session is; the
     * count of rows the database reports. Its transaction is the caller's (the body's, {@link Sessions#inTransaction}).
     */
    public static long update(ExecutionPlan.Update update, PlanSessions.Source sessions) throws IOException {
        long[] count = {0};
        send(update.sql(), Map.of(), sessions, (st, statement, values) ->
                count[0] = st.update(statement, update.sql().slots(), values));
        return count[0];
    }

    /** The body driver's one effect: {@code effect} -- a raw statement a program wrote, a DDL, a script, a seed's
     *  rows -- on a session of {@code sessions} as {@link #update}'s; whether it answered rows (not read). */
    public static boolean effect(ExecutionPlan.Effect effect, PlanSessions.Source sessions) throws IOException {
        if (!effect.sql().slots().isEmpty()) {
            throw new IllegalStateException("an effect binds no slot: its text is the statement");
        }
        boolean[] rows = {false};
        send(effect.sql(), Map.of(), sessions, (st, statement, values) -> rows[0] = st.execute(statement));
        return rows[0];
    }

    /** A statement's columns as the database reports them, the statement RUN for them and no row read (the probes'
     *  LIMIT-0 way, {@link Statements#columns}): on a session of {@code sessions}, checked against the statement's
     *  target and given the target's session statements as any statement is (the doors, 2026-10-29: a probe is a plan
     *  statement; until then each probe held the session's statements itself). A probe binds no slot. A schema read
     *  is not counted as a statement sent ({@link StatementOrigin#sent}): the probes that count themselves do so by
     *  name, as before. */
    public static List<Statements.Reported> columns(ExecutionPlan.Sql sql, PlanSessions.Source sessions) {
        return schema(sql, sessions, Statements::columns);
    }

    /** A statement's columns as the database DESCRIBES them, the statement prepared and never run
     *  ({@link Statements#describe}); as {@link #columns} otherwise. */
    public static List<Statements.Reported> describe(ExecutionPlan.Sql sql, PlanSessions.Source sessions) {
        return schema(sql, sessions, Statements::describe);
    }

    private static List<Statements.Reported> schema(ExecutionPlan.Sql sql, PlanSessions.Source sessions,
            java.util.function.BiFunction<Statements, String, List<Statements.Reported>> read) {
        if (!sql.slots().isEmpty()) {
            throw new IllegalStateException("a schema probe binds no slot: its text is the statement");
        }
        try (Sessions.Session session = sessions.open(sql.target())) {
            return read.apply(opened(session, sql.target()), sql.statement());
        }
    }

    /** How a setup step went, for a caller that records the steps it asked for (the referee's ledger): the step's
     *  index and whether its statement answered rows. */
    @FunctionalInterface
    public interface Ran {
        void ran(int step, boolean answeredRows);
    }

    /**
     * A session's setup, step by step, on a session of {@code sessions} for {@code target}: a statement step as an
     * effect ({@link #effect}); a rows step through the session's bulk loader ({@link Statements#load}), which the plan
     * side chose where the database has one ({@code SqlDialect.loadsRowsInBulk}; a session whose database has none is
     * a mismatch, refused by name, as {@link PlanSessions}'s own setup refuses it). The body driver's seeds (a
     * context's declared test data), a program's session statements and the system metamodel store's rows come
     * through here (the doors, 2026-10-29; until then {@code SetupRunner} ran them on the session's statements
     * itself); a session the host opens is set up by its owner ({@link PlanSessions#setUp}). Counted under the
     * caller's statement origin. No step streams to a sink, so none raises an {@code IOException} of its own.
     */
    public static void setUp(List<ExecutionPlan.SetupStep> steps, ExecutionPlan.Target target,
            PlanSessions.Source sessions, @com.legend.base.Nullable Ran ran) {
        for (int i = 0; i < steps.size(); i++) {
            boolean answeredRows = switch (steps.get(i)) {
                case ExecutionPlan.SetupStep.Statement s -> {
                    try {
                        yield effect(new ExecutionPlan.Effect(ExecutionPlan.Sql.of(s.sql(), target)), sessions);
                    } catch (IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                }
                case ExecutionPlan.SetupStep.Rows r -> {
                    try (Sessions.Session session = sessions.open(target)) {
                        if (!opened(session, target).load(r)) {
                            throw new IllegalStateException("the setup has rows for a bulk loader, and the session's"
                                    + " database has none (" + r.stagingTable() + ")");
                        }
                    }
                    yield false;
                }
            };
            if (ran != null) {
                ran.ran(i, answeredRows);
            }
        }
    }

    /** A typed answer on its way to a sink: the columns once the statement has answered (its first row, or its
     *  end), so a statement the database refuses has written nothing to the sink. */
    private static final class Streamed {
        private final com.legend.result.Batcher batcher;
        private boolean started;

        Streamed(List<ExecutionPlan.Column> columns, com.legend.result.ResultSink sink, int capacity) {
            this.batcher = new com.legend.result.Batcher(columns, sink, capacity);
        }

        void row(List<Object> cells) throws IOException {
            if (!started) {
                started = true;
                batcher.start();
            }
            batcher.row(cells);
        }

        void end() throws IOException {
            if (!started) {
                started = true;
                batcher.start();
            }
            batcher.end();
        }
    }

    /** A keyed plan's typed rows, streamed: its answered columns (the per-key ones named by the keys) once the keys
     *  are read, then the body's rows. */
    private static final class KeyedStream implements KeyedSink {
        private final com.legend.result.ResultSink sink;
        private final int capacity;
        private @com.legend.base.Nullable Streamed streamed;

        KeyedStream(com.legend.result.ResultSink sink, int capacity) {
            this.sink = sink;
            this.capacity = capacity;
        }

        @Override
        public void columns(List<ExecutionPlan.Column> columns) {
            streamed = new Streamed(columns, sink, capacity);
        }

        @Override
        public void row(List<Object> cells) throws IOException {
            java.util.Objects.requireNonNull(streamed, "a keyed row before its columns").row(cells);
        }

        @Override
        public void end() throws IOException {
            java.util.Objects.requireNonNull(streamed, "a keyed end before its columns").end();
        }
    }

    /** A plan of several steps' answer, held until its changes have committed: its typed rows. */
    private static final class Held {
        private List<ExecutionPlan.Column> columns = List.of();
        private final List<List<Object>> rows = new java.util.ArrayList<>();
    }

    // ---- a keyed plan (a dynamic pivot's probe plan, 2026-10-20; docs/PLAN_PATH_PROGRAM_2026_10_17.md §9) ----------

    /** Where a keyed plan's answer goes, once its keys are known: its columns, then each row's cells. */
    private interface KeyedSink {
        void columns(List<ExecutionPlan.Column> columns) throws IOException;

        void row(List<Object> cells) throws IOException;

        void end() throws IOException;
    }

    /**
     * A plan of several steps (main's write plans, docs/WRITES_DESIGN_2026_10_18.md §8, leg 7; merged 2026-10-26):
     * every step on one session, in order, through the session's own statements ({@link Statements}) as every other
     * plan's are; where a step changes the database, all of them in one transaction ({@link Statements#inTransaction}:
     * a caller's open one joined), each change's count kept under its name for the slots after it. Its answer is its
     * one result step's typed rows, held until the steps have all run (and their changes committed), then handed on.
     */
    private static void sequence(ExecutionPlan.Sequence sequence, Map<String, PlanParameters.Checked> checked,
            PlanSessions.Source sessions, Held held) throws IOException {
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
                case ExecutionPlan.Effect e -> {
                    changes = true;
                    yield e.sql();
                }
                case ExecutionPlan.TdsResult t -> {
                    results++;
                    yield t.sql();
                }
                case ExecutionPlan.Keyed k -> throw new com.legend.error.NotImplementedException("a keyed result"
                        + " inside a plan of several steps is not run yet");
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
        try (Sessions.Session session = changes ? sessions.openToChange(on) : sessions.open(on)) {
            Statements st = opened(session, on);
            try {
                st.inTransaction(() -> {
                    try {
                        steps(sequence, checked, st, held);
                    } catch (IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                    return null;
                });
            } catch (java.io.UncheckedIOException e) {
                throw e.getCause();
            }
        }
    }

    /** The steps, each at its turn on {@code st}: a change's count bound under its name for the slots after it; a
     *  check's rows read and dropped (the database computes what it may raise); the result's typed rows to
     *  {@code held}. */
    private static void steps(ExecutionPlan.Sequence sequence, Map<String, PlanParameters.Checked> checked,
            Statements st, Held held) throws IOException {
        Map<String, PlanParameters.Checked> values = new java.util.HashMap<>(checked);
        for (ExecutionPlan.Node step : sequence.steps()) {
            switch (step) {
                case ExecutionPlan.Update u -> {
                    String statement = statement(u.sql(), values);
                    StatementOrigin.sent(statement);
                    long count = st.update(statement, u.sql().slots(), valuesOf(u.sql(), values));
                    if (u.count() != null) {
                        // (the database's count: a value it produced, bound where a later step reads it)
                        values.put(u.count(), new PlanParameters.One(count));
                    }
                }
                case ExecutionPlan.Check k -> {
                    String statement = statement(k.sql(), values);
                    StatementOrigin.sent(statement);
                    st.query(statement, k.sql().slots(), valuesOf(k.sql(), values), row -> { });
                }
                case ExecutionPlan.Effect e -> {
                    String statement = statement(e.sql(), values);
                    StatementOrigin.sent(statement);
                    st.execute(statement);
                }
                case ExecutionPlan.TdsResult t -> {
                    String statement = statement(t.sql(), values);
                    StatementOrigin.sent(statement);
                    held.columns = t.columns();
                    st.rows(statement, t.sql().slots(), valuesOf(t.sql(), values), t.columns(), held.rows::add);
                }
                case ExecutionPlan.Keyed k -> throw new com.legend.error.NotImplementedException("a keyed result"
                        + " inside a plan of several steps is not run yet");
                case ExecutionPlan.Sequence s -> throw new IllegalStateException("a sequence inside a sequence");
            }
        }
    }

    /** Every null's type checked before any session is opened: a plan whose null is of no type is refused whole. */
    private static void nullTypes(ExecutionPlan.Sql sql) {
        for (ExecutionPlan.Slot slot : sql.slots()) {
            if (slot.binding() instanceof ExecutionPlan.Binding.One one) {
                JdbcStatements.nullType(one, slot);
            }
        }
    }

    /** Each slot's value, in the statement's order. */
    private static List<PlanParameters.Checked> valuesOf(ExecutionPlan.Sql sql,
            Map<String, PlanParameters.Checked> checked) {
        List<PlanParameters.Checked> values = new java.util.ArrayList<>(sql.slots().size());
        for (int i = 0; i < sql.slots().size(); i++) {
            values.add(value(checked, sql.slots().get(i), i + 1));
        }
        return values;
    }

    /**
     * Runs a keyed plan on one session, inside its snapshot: the keys probe, then the body written for those keys
     * (each per-key region once per key, in the probe's order, each copy binding its key's text), its columns and
     * rows handed to {@code sink}. The runner writes only text the plan carries and the keys' positions; every value
     * and every name is the database's.
     */
    private static void keyed(ExecutionPlan.Keyed keyed, Map<String, PlanParameters.Checked> checked,
            PlanSessions.Source sessions, KeyedSink sink) throws IOException {
        for (ExecutionPlan.Sql sql : List.of(keyed.keys(), keyed.body())) {
            for (ExecutionPlan.Slot slot : sql.slots()) {
                if (slot.binding() instanceof ExecutionPlan.Binding.One one) {
                    JdbcStatements.nullType(one, slot);
                }
            }
        }
        int perKey = (int) keyed.outputs().stream().filter(o -> o instanceof ExecutionPlan.Keyed.PerKey).count();
        // each statement's cells read by the names it writes them under (a session reading by name, MongoDB's, needs
        // them; JDBC reads by place): the probe's keyFields, a fixed output's field, a per-key output's
        // "_pk<position>_<field>" (ExecutionPlan.Keyed.PerKey)
        List<ExecutionPlan.Column> keyColumns = new java.util.ArrayList<>();
        for (int i = 0; i <= perKey; i++) {
            keyColumns.add(new ExecutionPlan.Column(keyed.keyFields().get(i), "String",
                    new ExecutionPlan.Multiplicity(1, 1), "VARCHAR", null, null));
        }
        try (Sessions.Session session = sessions.open(keyed.keys().target())) {
            Statements st = opened(session, keyed.keys().target());
            st.inSnapshot(keyed.snapshot(), () -> {
                List<List<Object>> keys = new java.util.ArrayList<>();
                ExecutionPlan.Sql probe = keyed.keys();
                List<PlanParameters.Checked> probeValues = new java.util.ArrayList<>(probe.slots().size());
                for (int i = 0; i < probe.slots().size(); i++) {
                    probeValues.add(value(checked, probe.slots().get(i), i + 1));
                }
                st.rows(statement(probe, checked), probe.slots(), probeValues, keyColumns, keys::add);
                int fixed = keyed.outputs().size() - perKey;
                int columns = fixed + keys.size() * perKey;
                if (keyed.maxColumns() != null && columns > keyed.maxColumns()) {
                    throw new com.legend.error.NotImplementedException("a dynamic pivot of " + keys.size() + " keys answers "
                            + columns + " columns, more than the database's " + keyed.maxColumns());
                }
                List<ExecutionPlan.Column> answered = new java.util.ArrayList<>(columns);
                List<ExecutionPlan.Column> read = new java.util.ArrayList<>(columns);
                for (int i = 0; i < keyed.outputs().size(); i++) {
                    if (keyed.outputs().get(i) instanceof ExecutionPlan.Keyed.Fixed f) {
                        answered.add(f.column());
                        read.add(named(f.column(), f.field()));
                    }
                }
                for (int k = 0; k < keys.size(); k++) {
                    for (int i = 0; i < keyed.outputs().size(); i++) {
                        if (keyed.outputs().get(i) instanceof ExecutionPlan.Keyed.PerKey p) {
                            String name = cellText(keys.get(k), p.name());
                            ExecutionPlan.Column c = p.column();
                            ExecutionPlan.Column a = new ExecutionPlan.Column(name, c.type(), c.multiplicity(),
                                    c.slot(), c.store(), c.precision());
                            answered.add(a);
                            read.add(named(a, "_pk" + k + "_" + p.field()));
                        }
                    }
                }
                // the answer in the relation's own order: the first perKeyAt fixed columns, the per-key ones, then the
                // rest of the fixed ones -- read in the body's order (its fixed columns first), each cell placed
                int at = keyed.perKeyAt();
                int[] place = new int[columns];
                for (int i = 0; i < columns; i++) {
                    place[i] = i < at ? i : i < fixed ? i + (columns - fixed) : i - (fixed - at);
                }
                List<ExecutionPlan.Column> inOrder = new java.util.ArrayList<>(java.util.Collections.nCopies(columns,
                        (ExecutionPlan.Column) null));
                for (int i = 0; i < columns; i++) {
                    inOrder.set(place[i], answered.get(i));
                }
                Expanded body = expand(keyed.body(), keys, checked);
                sink.columns(inOrder);
                StatementOrigin.sent(body.statement());
                st.rows(body.statement(), body.slots(), body.values(), read, cells -> {
                    List<Object> placed = new java.util.ArrayList<>(java.util.Collections.nCopies(columns, null));
                    for (int i = 0; i < columns; i++) {
                        placed.set(place[i], cells.get(i));
                    }
                    sink.row(placed);
                });
                sink.end();
            });
        }
    }

    /** {@code c} read under the name its statement writes it: the column's facts, that name. */
    private static ExecutionPlan.Column named(ExecutionPlan.Column c, String name) {
        return new ExecutionPlan.Column(name, c.type(), c.multiplicity(), c.slot(), c.store(), c.precision());
    }

    private static String cellText(List<Object> key, int cell) {
        if (cell >= key.size() || !(key.get(cell) instanceof String s)) {
            throw new IllegalStateException("a keys row's cell " + cell + " is " + (cell < key.size() ? key.get(cell)
                    : "absent") + ": the probe writes each key's text and names");
        }
        return s;
    }

    /** A body written for its keys: the statement, and each placeholder's slot and value in placeholder order. */
    private record Expanded(String statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values) {
    }

    /**
     * {@code sql} written for {@code keys}: the text outside its regions as it is, its type holes filled; each region
     * once per key ({@code lead} before the first copy, {@code separator} between copies, the key's position at each
     * counter; with no key, {@code empty}, or the statement refused where it has no form without one); each slot
     * outside a region bound once, each inside one bound in every copy, a key's cell to that key's text.
     */
    private static Expanded expand(ExecutionPlan.Sql sql, List<List<Object>> keys,
            Map<String, PlanParameters.Checked> checked) {
        String text = sql.statement();
        // the type holes, each outside every region (the writer refuses one inside): their places and types, in order
        List<int[]> holes = new java.util.ArrayList<>();
        List<String> holeTypes = new java.util.ArrayList<>();
        for (int i = 0; i < sql.slots().size(); i++) {
            ExecutionPlan.Slot each = sql.slots().get(i);
            if (each.binding() instanceof ExecutionPlan.Binding.One one && one.hole() != null) {
                holes.add(new int[] {one.hole().at()});
                holeTypes.add(typeOf(one.hole(), value(checked, each, i + 1), each));
            }
        }
        StringBuilder out = new StringBuilder(text.length() + keys.size() * 64);
        List<ExecutionPlan.Slot> slots = new java.util.ArrayList<>();
        List<PlanParameters.Checked> values = new java.util.ArrayList<>();
        int[] hole = {0};
        int from = 0;
        int slot = 0;
        for (ExecutionPlan.Region r : sql.regions()) {
            outside(out, text, from, r.start(), holes, holeTypes, hole);
            for (; slot < r.firstSlot(); slot++) {
                bindOnce(sql, slot, checked, slots, values);
            }
            if (keys.isEmpty()) {
                if (r.empty() == null) {
                    throw new com.legend.error.NotImplementedException("a dynamic pivot with no fixed column found no key: its"
                            + " relation has no column");
                }
                out.append(r.empty());
            } else {
                out.append(r.lead());
                for (int k = 0; k < keys.size(); k++) {
                    if (k > 0) {
                        out.append(r.separator());
                    }
                    int at = r.start();
                    for (int counter : r.counters()) {
                        out.append(text, at, counter).append(k);
                        at = counter;
                    }
                    out.append(text, at, r.end());
                    for (int s = r.firstSlot(); s < r.firstSlot() + r.slotCount(); s++) {
                        ExecutionPlan.Slot each = sql.slots().get(s);
                        if (each.binding() instanceof ExecutionPlan.Binding.Key key) {
                            slots.add(new ExecutionPlan.Slot("", new ExecutionPlan.Binding.One("VARCHAR", null)));
                            values.add(new PlanParameters.One(cellText(keys.get(k), key.cell())));
                        } else {
                            slots.add(each);
                            values.add(value(checked, each, s + 1));
                        }
                    }
                }
            }
            slot = r.firstSlot() + r.slotCount();
            from = r.end();
        }
        outside(out, text, from, text.length(), holes, holeTypes, hole);
        for (; slot < sql.slots().size(); slot++) {
            bindOnce(sql, slot, checked, slots, values);
        }
        return new Expanded(out.toString(), slots, values);
    }

    /** The statement's text {@code [from, to)} outside its regions, each type hole up to {@code to} filled (one at
     *  {@code to} itself before the region that starts there); {@code next} the first hole not yet written. */
    private static void outside(StringBuilder out, String text, int from, int to, List<int[]> holes,
            List<String> holeTypes, int[] next) {
        int at = from;
        while (next[0] < holes.size() && holes.get(next[0])[0] <= to) {
            int place = holes.get(next[0])[0];
            out.append(text, at, place).append(holeTypes.get(next[0]));
            at = place;
            next[0]++;
        }
        out.append(text, at, to);
    }

    private static void bindOnce(ExecutionPlan.Sql sql, int slot, Map<String, PlanParameters.Checked> checked,
            List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values) {
        ExecutionPlan.Slot each = sql.slots().get(slot);
        slots.add(each);
        values.add(value(checked, each, slot + 1));
    }

    /** What a statement's session is asked, once the session is checked and its own statements have run: the
     *  statement with its type holes filled, and the slots' checked values in slot order. */
    private interface Ask {
        void of(Statements st, String statement, List<PlanParameters.Checked> values) throws IOException;
    }

    /** {@code sql} sent on a session of {@code sessions}: the slots bound to the checked values, the type holes
     *  filled, the session asked ({@code ask}: each row's text, or each row's typed cells). */
    private static void send(ExecutionPlan.Sql sql, Map<String, PlanParameters.Checked> checked,
            PlanSessions.Source sessions, Ask ask) throws IOException {
        // every null type checked before any session is opened: a plan whose null is of no type is refused whole
        for (ExecutionPlan.Slot slot : sql.slots()) {
            if (slot.binding() instanceof ExecutionPlan.Binding.One one) {
                JdbcStatements.nullType(one, slot);
            }
        }
        List<PlanParameters.Checked> values = new java.util.ArrayList<>(sql.slots().size());
        for (int i = 0; i < sql.slots().size(); i++) {
            values.add(value(checked, sql.slots().get(i), i + 1));
        }
        // the session's own statements (JDBC's, or a connector's: Statements), 2026-10-17, docs/MONGODB_PLAN_2026_10_16.md
        // D12: the runner holds the plan and the punctuation; the session carries text in and text out
        try (Sessions.Session session = sessions.open(sql.target())) {
            Statements st = opened(session, sql.target());
            String statement = statement(sql, checked);
            // counted here alone, for every session kind (a session sends, the runner counts: JdbcStatements counted
            // each statement a second time, 2026-10-21)
            StatementOrigin.sent(statement);
            ask.of(st, statement, values);
        }
    }

    /** {@code session}'s statements for {@code target}: checked against it (the database, the server versions the
     *  statements are written for), the target's session statements run -- once per connection (the session's memo,
     *  {@link SessionState#needsSessionStatements}; D66, 2026-10-30: until then they ran before every statement), and
     *  again after a rollback the seam made. Inside a transaction the CALLER holds (autocommit off before the session
     *  was handed in) they run before every statement, as before: a caller's rollback undoes what a session statement
     *  set there (Postgres's {@code SET} is transactional) and the memo cannot see it. The one place a session's
     *  statements are taken (the doors, 2026-10-29: {@code ArchitectureTest.theRunnerIsTheOneCallerOfTheSessionsStatements}),
     *  on every path: a statement, a schema read, a setup step, a sequence, a keyed plan. */
    private static Statements opened(Sessions.Session session, ExecutionPlan.Target target) {
        Statements st = session.statements();
        st.check(target);
        if (!target.session().isEmpty()) {
            boolean memo = st.ownsTransaction();
            if (!memo || session.state().needsSessionStatements(target.session())) {
                try (var origin = StatementOrigin.enter(StatementOrigin.SESSION)) {
                    for (String s : target.session()) {
                        StatementOrigin.sent(s);
                        st.execute(s);
                    }
                }
                if (memo) {
                    // recorded after every statement ran: one that failed leaves the list to run again
                    session.state().sessionStatementsRan(target.session());
                }
            }
        }
        return st;
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

    static ExecutionPlan.TypeSpelling spelling(ExecutionPlan.TypeHole hole, ExecutionPlan.ValueKind kind,
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
    static String dateTimeText(LocalDateTime t, @com.legend.base.Nullable Integer fraction) {
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
    static ExecutionPlan.ValueKind kindOf(Object v, ExecutionPlan.Slot slot) {
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
}
