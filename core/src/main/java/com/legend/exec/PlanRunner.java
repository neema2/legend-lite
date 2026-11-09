// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.base.Nullable;
import com.legend.error.DataError;
import com.legend.error.NotImplementedException;
import com.legend.executionplan.ColumnFacts;
import com.legend.executionplan.ExecutionPlan;
import com.legend.result.Batcher;
import com.legend.result.Collector;
import com.legend.result.ResultSink;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

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
        Collector collector = new Collector();
        stream(plan, values, sessions, collector);
        Collector.Collected c = collector.collected();
        return new Rows(c.columns(), c.rows());
    }

    /** {@link #stream(ExecutionPlan, Map, PlanSessions.Source, com.legend.result.ResultSink, int)} in batches of
     *  {@link com.legend.result.Batcher#DEFAULT_CAPACITY} rows. */
    public static void stream(ExecutionPlan plan, Map<String, ?> values, PlanSessions.Source sessions,
            ResultSink sink) throws IOException {
        stream(plan, values, sessions, sink, Batcher.DEFAULT_CAPACITY);
    }

    /**
     * Runs {@code plan}, whose answer is TYPED ROWS, with {@code values}, on a session of {@code sessions}: its columns
     * to {@code sink} once the statement has answered, then its rows in batches of up to {@code capacity} as they
     * arrive, then the end (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md R8). A tabular result streams; a keyed plan's
     * body streams once its keys are read; a plan of several steps holds its answer until its changes have committed,
     * then hands it on (no answer leaves for a change rolled back).
     */
    public static void stream(ExecutionPlan plan, Map<String, ?> values, PlanSessions.Source sessions,
            ResultSink sink, int capacity) throws IOException {
        // the runtime wall (the deep audit, 2026-11-05): while the plan runs, nothing on this thread plans
        try (var run = com.legend.base.RunScope.enter()) {
            streamRun(plan, values, sessions, sink, capacity);
        }
    }

    private static void streamRun(ExecutionPlan plan, Map<String, ?> values, PlanSessions.Source sessions,
            ResultSink sink, int capacity) throws IOException {
        Map<String, PlanParameters.Checked> checked = PlanParameters.check(plan.parameters(), values);
        switch (plan.root()) {
            case ExecutionPlan.TdsResult tds -> {
                if (reported(tds)) {
                    // a relation of no declared column: the columns its result reports, read from the real run
                    Streamed[] streamed = {null};
                    send(tds.sql(), checked, sessions, (st, statement, bound) ->
                            st.reportedRows(statement, tds.sql().slots(), bound,
                                    columns -> streamed[0] = new Streamed(columns, sink, capacity),
                                    cells -> streamed[0].row(cells)));
                    Objects.requireNonNull(streamed[0], "the session handed no reported columns").end();
                    return;
                }
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
            case ExecutionPlan.Constant constant -> {
                // a value the program wrote: answered with nothing sent, no session opened
                Streamed streamed = new Streamed(constant.columns(), sink, capacity);
                for (List<Object> row : constantRows(constant)) {
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
            case ExecutionPlan.Load seeds -> throw new IllegalStateException("a plan whose root is a seed step: a"
                    + " program's step, run by the program runner (ProgramRunner.run)");
        }
    }

    /** A seed step on the graph's system database is the system store's own ({@link ExecutionPlan.Load#SYSTEM_STORE},
     *  the planner's), never a body's test data: refused by name otherwise. */
    static void systemStore(ExecutionPlan.Load seeds) {
        if (!seeds.identity().equals(ExecutionPlan.Load.SYSTEM_STORE)) {
            throw new IllegalStateException("a seed step on the graph's system database that is not the system store's"
                    + " own: the database holds the graph's metamodel and nothing else");
        }
    }

    /** The program's held answer streamed to {@code sink}, once its steps have run (and its changes committed). */
    static void streamHeld(Held held, ResultSink sink) throws IOException {
        Streamed streamed = new Streamed(held.columns, sink, Batcher.DEFAULT_CAPACITY);
        for (List<Object> row : held.rows) {
            streamed.row(row);
        }
        streamed.end();
    }

    static KeyedSink heldSink(Held held) {
        return new KeyedSink() {
            @Override
            public void columns(List<ExecutionPlan.Column> columns) {
                held.columns = columns;
            }

            @Override
            public void row(List<Object> cells) {
                held.rows.add(cells);
            }

            @Override
            public void end() {
            }
        };
    }

    /**
     * The body driver's one change (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md, phase 5: every statement a body sends
     * is a plan's): {@code update} -- a store write, a staged table made or dropped -- on a session of
     * {@code sessions}, checked against its target and given the target's session statements as any session is; the
     * count of rows the database reports. Its transaction is the caller's (the body's, {@link Sessions#inTransaction}).
     */
    public static long update(ExecutionPlan.Update update, PlanSessions.Source sessions) throws IOException {
        // a run of one statement (THE WALL, 2026-11-06: every statement is a run's; none started inside a run)
        try (var run = com.legend.base.RunScope.enter()) {
            return updateIn(update, sessions);
        }
    }

    private static long updateIn(ExecutionPlan.Update update, PlanSessions.Source sessions) throws IOException {
        long[] count = {0};
        send(update.sql(), Map.of(), sessions, (st, statement, values) ->
                count[0] = st.update(statement, update.sql().slots(), values));
        return count[0];
    }

    /** The body driver's one effect: {@code effect} -- a raw statement a program wrote, a DDL, a script, a seed's
     *  rows -- on a session of {@code sessions} as {@link #update}'s; whether it answered rows (not read). */
    public static boolean effect(ExecutionPlan.Effect effect, PlanSessions.Source sessions) throws IOException {
        // a run of one statement (THE WALL, 2026-11-06)
        try (var run = com.legend.base.RunScope.enter()) {
            return effectIn(effect, sessions);
        }
    }

    private static boolean effectIn(ExecutionPlan.Effect effect, PlanSessions.Source sessions) throws IOException {
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
            BiFunction<Statements, String, List<Statements.Reported>> read) {
        if (!sql.slots().isEmpty()) {
            throw new IllegalStateException("a schema probe binds no slot: its text is the statement");
        }
        // a run of one statement (THE WALL, 2026-11-06)
        try (var run = com.legend.base.RunScope.enter(); Sessions.Session session = sessions.open(sql.target())) {
            return read.apply(opened(session, sql.target(), sessions.mark()), sql.statement());
        }
    }

    /**
     * A TARGET'S LOAD established ({@link ExecutionPlan.Load.Cadence#PER_ACQUISITION}; docs/DATA_IN_THE_PLAN_2026_11_09.md
     * L2, 2026-11-09): run on a session of {@code sessions} for its target, through the one deliverer every load takes
     * ({@link #seed}: a statement step sent, a rows step through the session's bulk loader -- refused by name where the
     * database has none -- a file step by its reference), as a run of its own (THE WALL: a statement is sent only inside
     * a run). Counted under the SEED mark. {@code PlanSessions} establishes a target's loads so when it acquires a session
     * for it; a caller that holds a load of its own (a test's fixture) establishes it here -- test harnesses alone, M15
     * residue (docs/DATA_IN_THE_PLAN_2026_11_09.md), narrowed when L8 moves their seeding into plans. Until L2 a target's
     * setup steps ran by a path of their own.
     */
    public static void establish(ExecutionPlan.Load load, PlanSessions.Source sessions) {
        try (var run = com.legend.base.RunScope.enter()) {
            establishIn(load, sessions);
        }
    }

    /** {@link #establish} inside the run in progress. */
    static void establishIn(ExecutionPlan.Load load, PlanSessions.Source sessions) {
        try (Sessions.Session session = sessions.open(load.target())) {
            establishOn(load, session);
        }
    }

    /** {@code load} established on {@code session}, inside the run in progress: a target's loads hold statements and
     *  rows, never a file (they are given no store; a file step is refused by name). */
    static void establishOn(ExecutionPlan.Load load, Sessions.Session session) {
        seed(load, session, opened(session, load.target(), StatementOrigin.SEED),
                com.legend.executionplan.DataStores.none(), answered -> { });
    }

    /** A typed answer on its way to a sink: the columns once the statement has answered (its first row, or its
     *  end), so a statement the database refuses has written nothing to the sink. */
    private static final class Streamed {
        private final Batcher batcher;
        private boolean started;

        Streamed(List<ExecutionPlan.Column> columns, ResultSink sink, int capacity) {
            this.batcher = new Batcher(columns, sink, capacity);
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
        private final ResultSink sink;
        private final int capacity;
        private @Nullable Streamed streamed;

        KeyedStream(ResultSink sink, int capacity) {
            this.sink = sink;
            this.capacity = capacity;
        }

        @Override
        public void columns(List<ExecutionPlan.Column> columns) {
            streamed = new Streamed(columns, sink, capacity);
        }

        @Override
        public void row(List<Object> cells) throws IOException {
            Objects.requireNonNull(streamed, "a keyed row before its columns").row(cells);
        }

        @Override
        public void end() throws IOException {
            Objects.requireNonNull(streamed, "a keyed end before its columns").end();
        }
    }

    /** {@code held}'s rows as {@link #rows} hands them: through the result's batches and the collector, so a cell
     *  reaches a judge as it reaches every other reader of a typed result (a tagged column's value, not its carrier;
     *  the seam program's S2f, 2026-10-30: the held rows of a verdict's sides and of the fused statement go this way). */
    static Rows collected(Held held) throws IOException {
        Collector collector = new Collector();
        Streamed streamed = new Streamed(held.columns, collector, Batcher.DEFAULT_CAPACITY);
        for (List<Object> row : held.rows) {
            streamed.row(row);
        }
        streamed.end();
        Collector.Collected c = collector.collected();
        return new Rows(c.columns(), c.rows());
    }

    /** A plan of several steps' answer, held until its changes have committed: its typed rows. */
    static final class Held {
        List<ExecutionPlan.Column> columns = List.of();
        final List<List<Object>> rows = new ArrayList<>();
    }

    // ---- a keyed plan (a dynamic pivot's probe plan, 2026-10-20; docs/PLAN_PATH_PROGRAM_2026_10_17.md §9) ----------

    /** Where a keyed plan's answer goes, once its keys are known: its columns, then each row's cells. */
    interface KeyedSink {
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
        ExecutionPlan.Keyed.Snapshot snapshot = null;
        for (ExecutionPlan.Node step : sequence.steps()) {
            if (step instanceof ExecutionPlan.Constant) {
                // a value the program wrote: the answer, with nothing sent
                results++;
                continue;
            }
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
                case ExecutionPlan.Load u -> throw new NotImplementedException("a seed step in a"
                        + " plan the query door runs: seeds are the test door's (the program runner's, ProgramRunner.run)");
                case ExecutionPlan.TdsResult t -> {
                    results++;
                    yield t.sql();
                }
                case ExecutionPlan.Keyed k -> {
                    // a keyed result among the steps (the seam program's S1a, 2026-10-30; refused by name until then):
                    // its two statements run on the sequence's session, its snapshot the sequence's one transaction,
                    // opened at the strictest isolation a keyed step asks (below); the body's null types checked here,
                    // the keys' as the step's statement; one target for both is the record's own rule
                    results++;
                    nullTypes(k.body());
                    snapshot = stricter(snapshot, k.snapshot());
                    yield k.keys();
                }
                case ExecutionPlan.Constant c -> throw new IllegalStateException("not reached: a constant step sends"
                        + " nothing");
                case ExecutionPlan.Sequence s -> throw new IllegalStateException("a sequence inside a sequence: a"
                        + " plan's steps are one list");
            };
            nullTypes(sql);
            if (target == null) {
                target = sql.target();
            } else if (!target.equals(sql.target())) {
                throw new NotImplementedException("a plan whose steps name different targets: its"
                        + " steps run on one session, in one transaction");
            }
        }
        if (results != 1) {
            throw new IllegalStateException("a sequence of " + results + " result steps: a plan's answer is one");
        }
        ExecutionPlan.Target on = Objects.requireNonNull(target, "a sequence has a step");
        try (Sessions.Session session = changes ? sessions.openToChange(on) : sessions.open(on)) {
            Statements st = opened(session, on, sessions.mark());
            if (snapshot != null && snapshot != ExecutionPlan.Keyed.Snapshot.NONE && st.ownsTransaction()) {
                // a keyed step's snapshot is the sequence's own transaction, at the isolation the step asks, set
                // before its first statement (Postgres refuses an isolation changed mid-transaction); a transaction a
                // caller holds is joined as it is below, and is the snapshot then
                st.inSnapshot(snapshot, () -> steps(sequence, checked, st, held));
                return;
            }
            try {
                st.inTransaction(() -> {
                    try {
                        steps(sequence, checked, st, held);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    return null;
                });
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
        }
    }

    /** The stricter of two keyed snapshots ({@code a} null: none asked yet): a serializable transaction over a
     *  repeatable-read one, that over the database's own, that over none. */
    static ExecutionPlan.Keyed.Snapshot stricter(ExecutionPlan.Keyed.@Nullable Snapshot a,
            ExecutionPlan.Keyed.Snapshot b) {
        if (a == null) {
            return b;
        }
        return rank(b) > rank(a) ? b : a;
    }

    private static int rank(ExecutionPlan.Keyed.Snapshot s) {
        return switch (s) {
            case NONE -> 0;
            case TRANSACTION -> 1;
            case REPEATABLE_READ -> 2;
            case SERIALIZABLE -> 3;
        };
    }

    /** The steps, each at its turn on {@code st}: a change's count bound under its name for the slots after it; a
     *  check's rows read and dropped (the database computes what it may raise); the result's typed rows to
     *  {@code held}. */
    private static void steps(ExecutionPlan.Sequence sequence, Map<String, PlanParameters.Checked> checked,
            Statements st, Held held) throws IOException {
        Map<String, PlanParameters.Checked> values = new HashMap<>(checked);
        for (ExecutionPlan.Node step : sequence.steps()) {
            switch (step) {
                case ExecutionPlan.Update u -> update(u, values, st);
                case ExecutionPlan.Check k -> check(k, values, st);
                case ExecutionPlan.Effect e -> effect(e, values, st);
                case ExecutionPlan.Load u -> throw new IllegalStateException("a seed step in a plan's steps: the"
                        + " program runner's (ProgramRunner.run)");
                case ExecutionPlan.TdsResult t -> result(t, values, st, held);
                case ExecutionPlan.Constant c -> constant(c, held);
                case ExecutionPlan.Keyed k -> keyedOn(k, values, st, heldSink(held));
                case ExecutionPlan.Sequence s -> throw new IllegalStateException("a sequence inside a sequence");
            }
        }
    }

    /** A change's statement on {@code st}, its count bound under its name for the slots after it. */
    static void update(ExecutionPlan.Update u, Map<String, PlanParameters.Checked> values, Statements st)
            throws IOException {
        unstamped(u.sql());   // a write is sent with no trace comment (JdbcStatements.update): it binds none
        String statement = u.sql().statement();
        StatementOrigin.sent(st.mark(), statement);
        long count = st.update(statement, u.sql().slots(), valuesOf(u.sql(), values));
        if (u.count() != null) {
            // (the database's count: a value it produced, bound where a later step reads it)
            values.put(u.count(), new PlanParameters.One(count));
        }
    }

    /** A check's rows read and dropped (the database computes what it may raise). */
    static void check(ExecutionPlan.Check k, Map<String, PlanParameters.Checked> values, Statements st)
            throws IOException {
        stamp(k.sql(), values, st);
        String statement = k.sql().statement();
        StatementOrigin.sent(st.mark(), statement);
        st.query(statement, k.sql().slots(), valuesOf(k.sql(), values), row -> { });
    }

    /** An effect sent for what it does. */
    private static void effect(ExecutionPlan.Effect e, Map<String, PlanParameters.Checked> values, Statements st) {
        unstamped(e.sql());
        String statement = e.sql().statement();
        StatementOrigin.sent(st.mark(), statement);
        st.execute(statement);
    }

    /** A program's effect step (the test-is-the-product plan's B12): a script in its dialect's bracket where the
     *  session owns its transaction and none is open (the bracket's abort sent before a failure is raised), its
     *  statements bare otherwise; a single statement as {@link #effect}. */
    static void script(ExecutionPlan.Effect e, Map<String, PlanParameters.Checked> values, Statements st,
            boolean inTransaction) {
        unstamped(e.sql());
        ExecutionPlan.Effect.Script script = e.script();
        // the script is the body's statement, whatever mark is in force (a fixture's seeding); its abort is not
        Statements body = Statements.marked(st, StatementOrigin.BODY);
        if (script == null || inTransaction) {
            effect(e, values, body);
            return;
        }
        try {
            sendStatement(script.bracketed(), body);
        } catch (DataError failed) {
            if (script.abort() != null) {
                sendStatement(script.abort(), st);
            }
            throw failed;
        }
    }

    /** A statement the plan wrote as text (a script, a savepoint's), sent for what it does. */
    static void sendStatement(String statement, Statements st) {
        StatementOrigin.sent(st.mark(), statement);
        st.execute(statement);
    }

    /** The graph's system store's seed step ({@link ExecutionPlan.Load#SYSTEM_STORE}) on {@code system}, the graph's
     *  system database's session, once (the seeds' memo): the body driver's walk reaching that session runs the program
     *  road's own step through here, so whichever road comes first writes the store, once. Deleted with the walk. */
    public static void seedSystemStore(ExecutionPlan.Load seeds, Sessions.Session system) {
        if (!(seeds.target().database() instanceof ExecutionPlan.Database.System)) {
            throw new IllegalStateException("the system store's seeds name the graph's system database");
        }
        systemStore(seeds);
        // a run of the seed step (THE WALL, 2026-11-06)
        try (var run = com.legend.base.RunScope.enter()) {
            // the system store's load holds rows and statements, never a file: it is given no store
            seed(seeds, system, opened(system, seeds.target()), com.legend.executionplan.DataStores.none(),
                    answered -> { });
        }
    }

    /** A program's load (B12; docs/DATA_IN_THE_PLAN_2026_11_09.md 2.2): a {@code BEFORE_STEP} load (the seeds) skipped
     *  where the session holds its identity and has run no write since, and its identity recorded once run; an
     *  {@code AT_STEP} load (a program's own, {@code loadCsvToDbTable}) run every time, a write, recording nothing. Its
     *  steps run as a target's setup's are, a file's through {@link DataLoads} over {@code stores}. {@code ran} is told
     *  which of its steps ran, and whether each answered rows -- on a failure, those before it, as the walk records a
     *  setup's statements as each runs (the M5 audit's S1). The system store's load is sent under the SYSTEM mark, as
     *  its holder sent it; every other under SEED. */
    static void seed(ExecutionPlan.Load seeds, Sessions.Session session, Statements st,
            com.legend.executionplan.DataStores stores, java.util.function.Consumer<List<Boolean>> ran) {
        // a seed (BEFORE_STEP) is memoed; a program's own load and a target's (AT_STEP, PER_ACQUISITION) write
        boolean memo = seeds.cadence() == ExecutionPlan.Load.Cadence.BEFORE_STEP;
        verifyIdentity(seeds);
        SeedKey key = new SeedKey(seeds.identity());
        if (memo && session.state().established(key)) {
            return;
        }
        if (!memo) {
            // a program's own load writes, as an effect does: a seed established before it is established no more
            session.state().markWriting();
        }
        List<Boolean> answered = new ArrayList<>(seeds.steps().size());
        boolean system = seeds.target().database() instanceof ExecutionPlan.Database.System;
        Statements seeding = Statements.marked(st, system ? StatementOrigin.SYSTEM : StatementOrigin.SEED);
        try {
            for (ExecutionPlan.LoadStep each : seeds.steps()) {
                switch (each) {
                    case ExecutionPlan.LoadStep.Statement s -> {
                        StatementOrigin.sent(seeding.mark(), s.sql());
                        answered.add(seeding.execute(s.sql()));
                    }
                    case ExecutionPlan.LoadStep.Rows r -> {
                        if (!seeding.load(r)) {
                            throw new IllegalStateException("the seeds have rows for a bulk loader, and the session's"
                                    + " database has none (" + r.stagingTable() + ")");
                        }
                        // the bulk load's one statement, counted under the seeds' mark
                        StatementOrigin.count(seeding.mark());
                        answered.add(false);
                    }
                    case ExecutionPlan.LoadStep.File f -> {
                        // the file by its reference: its verified bytes, delivered as the plan says (DataLoads)
                        byte[] bytes = DataLoads.verified(f.ref(), stores);
                        switch (f.delivery()) {
                            case ExecutionPlan.LoadStep.Delivery.Streamed streamed -> {
                                List<List<String>> cells = DataLoads.cells(f.ref(), bytes, streamed.columns(),
                                        f.sizeLimitMegabytes());
                                if (!cells.isEmpty()) {
                                    seeding.streams(streamed.insert(), cells);
                                }
                            }
                            case ExecutionPlan.LoadStep.Delivery.Bulk bulk -> {
                                // the session's bulk loader, its staging statements the plan's (finding 8)
                                List<List<String>> cells = DataLoads.cells(f.ref(), bytes, bulk.columns(),
                                        f.sizeLimitMegabytes());
                                if (!cells.isEmpty() && !seeding.bulk(bulk.stagingTable(), bulk.createStaging(),
                                        bulk.copy(), bulk.dropStaging(), cells)) {
                                    throw new IllegalStateException("the plan loads a file through a bulk loader, and"
                                            + " the session's database has none (" + f.ref().name() + ")");
                                }
                            }
                        }
                        // the load's one statement (its batch), counted under the seeds' mark
                        StatementOrigin.count(seeding.mark());
                        answered.add(false);
                    }
                }
            }
        } finally {
            ran.accept(List.copyOf(answered));
        }
        if (memo) {
            session.state().establish(key);
        }
    }

    /** A seed step's key as the session's seeds' memo holds it. */
    private record SeedKey(String key) {
    }

    /** A constant answer's rows into {@code held}. */
    static void constant(ExecutionPlan.Constant c, Held held) {
        held.columns = c.columns();
        held.rows.addAll(constantRows(c));
    }

    /** A constant answer's TEXT cells read back by its column's declared type: a String the text, a Boolean
     *  {@code true} or {@code false}; any other type refused by name (the planner writes no other constant). */
    static List<List<Object>> constantRows(ExecutionPlan.Constant c) {
        String type = c.columns().get(0).type();
        List<List<Object>> rows = new ArrayList<>(c.cells().size());
        for (List<String> row : c.cells()) {
            String text = row.get(0);
            Object cell = text == null ? null : switch (type) {
                case "String" -> text;
                case "Boolean" -> switch (text) {
                    case "true" -> Boolean.TRUE;
                    case "false" -> Boolean.FALSE;
                    default -> throw new IllegalStateException("a Boolean constant's text is true or false, not '"
                            + text + "'");
                };
                default -> throw new NotImplementedException("a constant answer of " + type
                        + ": the planner writes a String or a Boolean");
            };
            List<Object> one = new ArrayList<>(1);
            one.add(cell);
            rows.add(one);
        }
        return rows;
    }

    /** A typed result's rows into {@code held}. */
    static void result(ExecutionPlan.TdsResult t, Map<String, PlanParameters.Checked> values, Statements st,
            Held held) throws IOException {
        stamp(t.sql(), values, st);
        String statement = t.sql().statement();
        StatementOrigin.sent(st.mark(), statement);
        if (reported(t)) {
            st.reportedRows(statement, t.sql().slots(), valuesOf(t.sql(), values), columns -> held.columns = columns,
                    held.rows::add);
            return;
        }
        held.columns = t.columns();
        st.rows(statement, t.sql().slots(), valuesOf(t.sql(), values), t.columns(), held.rows::add);
    }

    /** A relation of no declared column: the columns its result reports (a raw grid's, the seam program's S3c). */
    private static boolean reported(ExecutionPlan.TdsResult t) {
        return t.shape() == ExecutionPlan.TdsResult.Shape.RELATION && t.columns().isEmpty();
    }

    /** Every null's type checked before any session is opened: a plan whose null is of no type is refused whole. */
    static void nullTypes(ExecutionPlan.Sql sql) {
        for (ExecutionPlan.Slot slot : sql.slots()) {
            if (slot.binding() instanceof ExecutionPlan.Binding.One one) {
                JdbcStatements.nullType(one, slot);
            }
        }
    }

    /** A statement whose own stamp the runner does not bind (a keyed plan's, a write's, an effect's: none is sent
     *  under a comment the runner mints): one that binds it is a planner defect, refused by name. */
    private static void unstamped(ExecutionPlan.Sql sql) {
        if (sql.stamp() != null) {
            throw new IllegalStateException("a keyed, write or effect statement binds its own execution-trace stamp ('"
                    + sql.stamp() + "'): only a query or a check binds one (the planner's)");
        }
    }

    /** {@code sql}'s own execution-trace stamp, where the plan reads it ({@link ExecutionPlan.Sql#stamp}): the comment
     *  the session will send it under, minted now and bound under its name (R17 C4). */
    private static void stamp(ExecutionPlan.Sql sql, Map<String, PlanParameters.Checked> values, Statements st) {
        if (sql.stamp() != null) {
            values.put(sql.stamp(), new PlanParameters.One(st.nextStamp()));
        }
    }

    /** Each slot's value, in the statement's order. */
    private static List<PlanParameters.Checked> valuesOf(ExecutionPlan.Sql sql,
            Map<String, PlanParameters.Checked> checked) {
        List<PlanParameters.Checked> values = new ArrayList<>(sql.slots().size());
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
        nullTypes(keyed.keys());
        nullTypes(keyed.body());
        try (Sessions.Session session = sessions.open(keyed.keys().target())) {
            Statements st = opened(session, keyed.keys().target(), sessions.mark());
            st.inSnapshot(keyed.snapshot(), () -> keyedOn(keyed, checked, st, sink));
        }
    }

    /**
     * A keyed plan's two statements on {@code st}, inside their snapshot already (a keyed plan's own, or the
     * sequence's transaction where the keyed result is a step: the seam program's S1a, 2026-10-30): the keys probe
     * read as typed rows, the body written for those keys and its rows handed to {@code sink}.
     */
    static void keyedOn(ExecutionPlan.Keyed keyed, Map<String, PlanParameters.Checked> checked,
            Statements st, KeyedSink sink) throws IOException {
        int perKey = (int) keyed.outputs().stream().filter(o -> o instanceof ExecutionPlan.Keyed.PerKey).count();
        // each statement's cells read by the names it writes them under (a session reading by name, MongoDB's, needs
        // them; JDBC reads by place): the probe's keyFields, a fixed output's field, a per-key output's
        // "_pk<position>_<field>" (ExecutionPlan.Keyed.PerKey)
        List<ExecutionPlan.Column> keyColumns = new ArrayList<>();
        for (int i = 0; i <= perKey; i++) {
            keyColumns.add(new ExecutionPlan.Column(keyed.keyFields().get(i), "String",
                    new ExecutionPlan.Multiplicity(1, 1), "VARCHAR", null, null));
        }
        List<List<Object>> keys = new ArrayList<>();
        unstamped(keyed.keys());
        unstamped(keyed.body());
        ExecutionPlan.Sql probe = keyed.keys();
        if (keyed.keysOf() == ExecutionPlan.Keyed.KeysOf.COLUMNS) {
            // a raw grid's: each column its result reports a key, its label the key's one cell (the seam program's
            // S3c, 2026-10-30) -- read when the plan runs, never at planning
            for (Statements.Reported r : st.columns(probe.statement())) {
                if (!keyed.admitted().isEmpty()) {
                    String slot = r.type() == null ? null : ColumnFacts.slot(r.type());
                    if (slot == null || !keyed.admitted().contains(slot)) {
                        throw new NotImplementedException("a raw grid's column '" + r.name()
                                + "' is reported as " + r.driverType() + ": its cells are spelled here exactly only"
                                + " in " + keyed.admitted() + " (never approximately)");
                    }
                }
                keys.add(List.of(r.name()));
            }
        } else {
            List<PlanParameters.Checked> probeValues = new ArrayList<>(probe.slots().size());
            for (int i = 0; i < probe.slots().size(); i++) {
                probeValues.add(value(checked, probe.slots().get(i), i + 1));
            }
            st.rows(probe.statement(), probe.slots(), probeValues, keyColumns, keys::add);
        }
        int fixed = keyed.outputs().size() - perKey;
        int columns = fixed + keys.size() * perKey;
        if (keyed.maxColumns() != null && columns > keyed.maxColumns()) {
            throw new NotImplementedException((keyed.keysOf() == ExecutionPlan.Keyed.KeysOf.COLUMNS
                    ? "a raw grid of " + keys.size() + " columns answers " : "a dynamic pivot of " + keys.size()
                    + " keys answers ") + columns + " columns, more than the database's " + keyed.maxColumns());
        }
        List<ExecutionPlan.Column> answered = new ArrayList<>(columns);
        List<ExecutionPlan.Column> read = new ArrayList<>(columns);
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
        List<ExecutionPlan.Column> inOrder = new ArrayList<>(Collections.nCopies(columns,
                (ExecutionPlan.Column) null));
        for (int i = 0; i < columns; i++) {
            inOrder.set(place[i], answered.get(i));
        }
        Expanded body = expand(keyed.body(), keys, checked);
        sink.columns(inOrder);
        StatementOrigin.sent(st.mark(), body.statement());
        st.rows(body.statement(), body.slots(), body.values(), read, cells -> {
            List<Object> placed = new ArrayList<>(Collections.nCopies(columns, null));
            for (int i = 0; i < columns; i++) {
                placed.set(place[i], cells.get(i));
            }
            sink.row(placed);
        });
        sink.end();
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
     * {@code sql} written for {@code keys}: the text outside its regions as it is; each region
     * once per key ({@code lead} before the first copy, {@code separator} between copies, the key's position at each
     * counter; with no key, {@code empty}, or the statement refused where it has no form without one); each slot
     * outside a region bound once, each inside one bound in every copy, a key's cell to that key's text.
     */
    private static Expanded expand(ExecutionPlan.Sql sql, List<List<Object>> keys,
            Map<String, PlanParameters.Checked> checked) {
        String text = sql.statement();
        StringBuilder out = new StringBuilder(text.length() + keys.size() * 64);
        List<ExecutionPlan.Slot> slots = new ArrayList<>();
        List<PlanParameters.Checked> values = new ArrayList<>();
        int from = 0;
        int slot = 0;
        for (ExecutionPlan.Region r : sql.regions()) {
            out.append(text, from, r.start());
            for (; slot < r.firstSlot(); slot++) {
                bindOnce(sql, slot, checked, slots, values);
            }
            if (keys.isEmpty()) {
                if (r.empty() == null) {
                    throw new NotImplementedException("a dynamic pivot with no fixed column found no key: its"
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
        out.append(text, from, text.length());
        for (; slot < sql.slots().size(); slot++) {
            bindOnce(sql, slot, checked, slots, values);
        }
        return new Expanded(out.toString(), slots, values);
    }

    private static void bindOnce(ExecutionPlan.Sql sql, int slot, Map<String, PlanParameters.Checked> checked,
            List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values) {
        ExecutionPlan.Slot each = sql.slots().get(slot);
        slots.add(each);
        values.add(value(checked, each, slot + 1));
    }

    /** What a statement's session is asked, once the session is checked and its own statements have run: the
     *  statement as the plan holds it (P4: no text is written at run), and the slots' checked values in slot order. */
    private interface Ask {
        void of(Statements st, String statement, List<PlanParameters.Checked> values) throws IOException;
    }

    /** {@code sql} sent on a session of {@code sessions}: the slots bound to the checked values, the statement the
     *  plan's own, the session asked ({@code ask}: each row's text, or each row's typed cells). */
    private static void send(ExecutionPlan.Sql sql, Map<String, PlanParameters.Checked> checked,
            PlanSessions.Source sessions, Ask ask) throws IOException {
        // every null type checked before any session is opened: a plan whose null is of no type is refused whole
        for (ExecutionPlan.Slot slot : sql.slots()) {
            if (slot.binding() instanceof ExecutionPlan.Binding.One one) {
                JdbcStatements.nullType(one, slot);
            }
        }
        // the session's own statements (JDBC's, or a connector's: Statements), 2026-10-17, docs/MONGODB_PLAN_2026_10_16.md
        // D12: the runner holds the plan and the punctuation; the session carries text in and text out
        try (Sessions.Session session = sessions.open(sql.target())) {
            Statements st = opened(session, sql.target(), sessions.mark());
            Map<String, PlanParameters.Checked> bound = checked;
            if (sql.stamp() != null) {
                bound = new HashMap<>(checked);
                stamp(sql, bound, st);
            }
            List<PlanParameters.Checked> values = new ArrayList<>(sql.slots().size());
            for (int i = 0; i < sql.slots().size(); i++) {
                values.add(value(bound, sql.slots().get(i), i + 1));
            }
            String statement = sql.statement();
            // counted here alone, for every session kind (a session sends, the runner counts: JdbcStatements counted
            // each statement a second time, 2026-10-21)
            StatementOrigin.sent(st.mark(), statement);
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
    static Statements opened(Sessions.Session session, ExecutionPlan.Target target) {
        return opened(session, target, StatementOrigin.OTHER);
    }

    /** {@link #opened(Sessions.Session, ExecutionPlan.Target)}, the statements counted under {@code mark}. */
    static Statements opened(Sessions.Session session, ExecutionPlan.Target target, StatementOrigin mark) {
        Statements st = Statements.marked(session.statements(), mark);
        st.check(target);
        if (!target.session().isEmpty()) {
            boolean memo = st.ownsTransaction();
            if (!memo || session.state().needsSessionStatements(target.session())) {
                for (String s : target.session()) {
                    StatementOrigin.sent(StatementOrigin.SESSION, s);
                    st.execute(s);
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

    private static final DateTimeFormatter DATE_TIME_TEXT =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSSSSS", Locale.ROOT);

    /** {@code t} as the text its cast reads: to the nanosecond, or to {@code fraction} digits of a second, the finer ones
     *  cut, as its literal's are. */
    static String dateTimeText(LocalDateTime t, @Nullable Integer fraction) {
        int unit = 1;
        for (int digits = fraction == null ? 9 : fraction; digits < 9; digits++) {
            unit *= 10;
        }
        return DATE_TIME_TEXT.format(t.withNano(t.getNano() - t.getNano() % unit));
    }

    /** {@code load} refused by name unless its identity is its content's digest (ExecutionPlan.Load.canonical): the
     *  seeds' memo and a shared database's key trust it (the M1 tenets audit's note, 2026-11-09). The system store's load
     *  is named by its constant until the design's L4. */
    static void verifyIdentity(ExecutionPlan.Load load) {
        if (load.identity().equals(ExecutionPlan.Load.SYSTEM_STORE)) {
            return;
        }
        String content = ExecutionPlan.Load.IDENTITY_PREFIX + com.legend.cache.Hash.ofUtf8(
                ExecutionPlan.Load.canonical(load.cadence(), load.steps())).hex();
        if (!content.equals(load.identity())) {
            throw new IllegalStateException("a load whose identity (" + load.identity() + ") is not its content's ("
                    + content + "): refused, never run under another's name");
        }
    }
}
