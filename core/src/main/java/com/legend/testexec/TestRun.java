// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.base.Nullable;
import com.legend.error.DataError;
import com.legend.exec.PlanParameters;
import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.exec.ProgramRunner;
import com.legend.exec.StatementOrigin;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testexecutionplan.TestPlan;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

/**
 * A TEST PLAN RUN (docs/PLAN_AS_DATA_2026_11_07.md 2.8, the design's step 3, 2026-11-08): the test's half of the one
 * walker. The plan is lowered to the runner's {@link ProgramRunner.Program} -- each product step a node, a frame run a
 * node under LET, each verdict group and branch a callback declaring every statement it may read -- and at each
 * callback this reads the step's statements through the open program as its judge asks, and hands the rows to the
 * judges ({@link Judges}). The runner judges nothing and knows no verdict; this decides nothing either: the judges do.
 * In the driver until the design's step 10 makes it {@code testexec}.
 */
final class TestRun {

    private TestRun() {
    }

    /** Where a verdict step's rows go: the judges, as the plan's judge asks. */
    interface Judges {

        /** The DATABASE judge's row for {@code verdict}: the verdict schema's cells (the fused statement's index cell
         *  removed); null where the verdict has no wrapper. In the group's order. */
        void verdict(TestPlan.Verdict verdict, @Nullable List<Object> row) throws IOException;

        /** The group's fused statement ran ({@code rejected} null) or the database rejected it (the run then fails by
         *  name, nothing retried). Once per group that has one, before its verdicts. */
        void fused(TestPlan.Verdicts group, @Nullable DataError rejected) throws IOException;

        /** The HOST judge's sides for {@code verdict}: each base side's typed rows in the base's order. */
        void sides(TestPlan.Verdict verdict, List<PlanRunner.Rows> sides) throws IOException;

        /** The HOST judge's sides for {@code verdict} could not be read ({@code failure}): judged as rows are. */
        void sidesFailed(TestPlan.Verdict verdict, RuntimeException failure);

        /** The program's reader, from its first verdict step on (null once the program has ended): a verdict's
         *  message read on demand, where the verdict fails. */
        void reader(@Nullable SideReader reader);
    }

    /** {@code plan} run: its steps in order on the program's session, its verdict steps judged by {@code judges}, its
     *  answer and its steps' doings to {@code sink}. {@code textOrigin}: the origin a verdict step at a place reads
     *  our text under (an engine-text assert's, M10c item 8), or null. */
    static void run(TestPlan plan, Map<String, ?> values, PlanSessions.Source sessions,
            com.legend.executionplan.DataStores stores, IntFunction<@Nullable StatementOrigin> textOrigin,
            Judges judges, ProgramRunner.ProgramSink sink) throws IOException {
        Handler handler = new Handler(plan, judges);
        try {
            ProgramRunner.run(program(plan, textOrigin), values, sessions, stores, handler, sink);
        } finally {
            judges.reader(null);
        }
    }

    /** {@code plan}'s release, sent when closed ({@link ProgramRunner#release}): opened before the run, closed once the
     *  run and its appeals are over, whatever their outcome -- a body's failure stands, a failed release suppressed in
     *  it (B2a, 2026-11-11). */
    static Released releasing(TestPlan plan, PlanSessions.Source sessions) {
        return () -> ProgramRunner.release(plan.release(), sessions, plan.session());
    }

    /** A plan's release, sent when closed. */
    interface Released extends AutoCloseable {
        @Override
        void close();
    }

    /** {@code plan} as the runner's program: a product step a node, a frame let's eager run a node under LET (M9),
     *  a verdict group or a branch a callback at its place, declaring its statements, under BODY where the database
     *  judge alone judges (its fused statements, as the walk's flush sent them, M7) -- else a verdict step under the
     *  origin its engine-text check reads our text by. */
    static ProgramRunner.Program program(TestPlan plan, IntFunction<@Nullable StatementOrigin> textOrigin) {
        List<ProgramRunner.Step> steps = new ArrayList<>(plan.steps().size());
        boolean databaseAlone = plan.judge() == TestPlan.Judge.DATABASE;
        for (int i = 0; i < plan.steps().size(); i++) {
            steps.add(switch (plan.steps().get(i)) {
                case TestPlan.Run r -> new ProgramRunner.Node(r.node(), null);
                case TestPlan.FrameRun f -> new ProgramRunner.Node(f.check(), StatementOrigin.LET);
                case TestPlan.Verdicts v -> new ProgramRunner.Callback(i, TestPlan.statements(v),
                        databaseAlone ? StatementOrigin.BODY : textOrigin.apply(i));
                case TestPlan.Branch b -> new ProgramRunner.Callback(i, TestPlan.statements(b),
                        databaseAlone ? StatementOrigin.BODY : null);
            });
        }
        return new ProgramRunner.Program(plan.parameters(), steps);
    }

    /** The callbacks: each verdict step at its place. */
    private static final class Handler implements ProgramRunner.Callbacks {
        private final TestPlan plan;
        private final Judges judges;
        private boolean reading;

        Handler(TestPlan plan, Judges judges) {
            this.plan = plan;
            this.judges = judges;
        }

        @Override
        public void step(int id, ProgramRunner.OpenProgram open) throws IOException {
            if (!reading) {
                // the program's reader, handed once: the open program is the same at every callback
                reading = true;
                judges.reader(side -> sideRows(side, open));
            }
            switch (plan.steps().get(id)) {
                case TestPlan.Verdicts v -> verdicts(v, open);
                case TestPlan.Branch b -> branch(b, open);
                case TestPlan.Run r -> throw new IllegalStateException("a callback at a product step");
                case TestPlan.FrameRun f -> throw new IllegalStateException("a callback at a frame run");
            }
        }

        /** A branch: its condition read -- a statement of the body, as a value statement is (a mark in force stands)
         *  -- then the steps of the side it names, and only those: the side not taken is never sent. */
        private void branch(TestPlan.Branch b, ProgramRunner.OpenProgram open) throws IOException {
            PlanRunner.Rows condition = b.condition() instanceof TestPlan.Side.Constant c ? constantRows(c)
                    : open.read(statement(b.condition()), StatementOrigin.STATEMENT, Map.of());
            for (TestPlan.TestStep inner : taken(condition) ? b.then() : b.otherwise()) {
                switch (inner) {
                    case TestPlan.Verdicts v -> verdicts(v, open);
                    case TestPlan.Branch ib -> branch(ib, open);
                    case TestPlan.Run r -> throw new IllegalStateException("a branch's side holds verdict groups and"
                            + " branches");
                    case TestPlan.FrameRun f -> throw new IllegalStateException("a branch's side holds verdict groups"
                            + " and branches");
                }
            }
        }

        /** A verdict group as the plan's judge asks: the DATABASE judge's rows from the fused statement (each
         *  verdict's own wrapper where the plan fused none; a rejected fused statement fails the program) and the HOST
         *  judge's sides, each verdict's rows to the judges in the group's order. */
        private void verdicts(TestPlan.Verdicts group, ProgramRunner.OpenProgram open) throws IOException {
            if (group.verdicts().stream().anyMatch(TestPlan.Verdict::raises)) {
                raisingVerdict(group, open);
                return;
            }
            TestPlan.Judge judge = plan.judge();
            boolean hostAll = judge == TestPlan.Judge.HOST || judge == TestPlan.Judge.AGREEMENT;
            // a HOST-JUDGED verdict in a database-judged program: its sides and no statement of the database's (no
            // wrapper, no fused statement) -- an assertError's step (ruling N3: Java compares the error in both judge
            // modes)
            boolean hostOnly = !hostAll && group.fused() == null
                    && group.verdicts().stream().anyMatch(v -> v.wrapper() == null && !v.base().isEmpty());
            if (hostAll || hostOnly) {
                // F11 for the host's reads: inside the program's transaction a side whose failure the program may
                // survive (an assertError's) is read under the group's savepoint, so the transaction stays live for
                // the steps after it
                ProgramRunner.Savepoint guard = guard(group, open);
                for (TestPlan.Verdict each : group.verdicts()) {
                    if (!hostAll && (each.wrapper() != null || each.base().isEmpty())) {
                        continue;
                    }
                    // the read's own failure is the verdict's (its sides could not be read); a savepoint statement's
                    // is the program's
                    List<PlanRunner.Rows> sides = open.guarded(guard, () -> hostSides(each, open), e -> {
                        judges.sidesFailed(each, e instanceof IOException io ? new UncheckedIOException(io)
                                : (RuntimeException) e);
                        return null;
                    });
                    if (sides != null) {
                        judges.sides(each, sides);
                    }
                }
            }
            if (judge == TestPlan.Judge.DATABASE || judge == TestPlan.Judge.AGREEMENT) {
                databaseVerdicts(group, open);
            }
        }

        /**
         * An assertError's verdict (the user's ruling S135, 2026-11-05: the host catches, the database judges): its
         * base read once under the group's savepoint; what reading it RAISED (a database's error or a deferred guard's,
         * the errors assertError takes) is the outcome, any other exception a wall raised as it was. The host judge is
         * handed the sides or the error; the database judge's statement is run with the outcome bound to its
         * parameters (each absent where the base raised nothing, the position absent where the raise carried none).
         */
        private void raisingVerdict(TestPlan.Verdicts group, ProgramRunner.OpenProgram open) throws IOException {
            if (group.fused() != null || group.verdicts().size() != 1) {
                throw new IllegalStateException("a raising verdict is a group of its own: its statement reads what its"
                        + " own base raised");
            }
            TestPlan.Verdict each = group.verdicts().get(0);
            RuntimeException[] caught = new RuntimeException[1];
            List<PlanRunner.Rows> sides = open.guarded(guard(group, open), () -> hostSides(each, open), e -> {
                if (e instanceof DataError || e instanceof com.legend.error.ModelException) {
                    // the errors assertError takes; any other is a wall, raised as it was -- never judged as "no error"
                    caught[0] = (RuntimeException) e;
                    return null;
                }
                if (e instanceof IOException io) {
                    throw io;
                }
                throw (RuntimeException) e;
            });
            RuntimeException raised = caught[0];
            TestPlan.Judge judge = plan.judge();
            if (judge == TestPlan.Judge.HOST || judge == TestPlan.Judge.AGREEMENT) {
                if (raised != null) {
                    judges.sidesFailed(each, raised);
                } else {
                    judges.sides(each, java.util.Objects.requireNonNull(sides));
                }
            }
            if (judge == TestPlan.Judge.DATABASE || judge == TestPlan.Judge.AGREEMENT) {
                Map<String, PlanParameters.Checked> bound = new HashMap<>();
                PlanParameters.Checked none = new PlanParameters.None();
                RuntimeException error = raised;
                bound.put(TestPlan.Verdict.RAISED_MESSAGE,
                        error != null ? new PlanParameters.One(String.valueOf(error.getMessage())) : none);
                com.legend.exec.RaisedErrors.Positioned at = error instanceof DataError d
                        && d.getCause() instanceof com.legend.exec.RaisedErrors.Positioned p ? p : null;
                bound.put(TestPlan.Verdict.RAISED_LINE, at != null ? new PlanParameters.One((long) at.line()) : none);
                bound.put(TestPlan.Verdict.RAISED_COLUMN,
                        at != null ? new PlanParameters.One((long) at.column()) : none);
                if (!(each.wrapper() instanceof ExecutionPlan.TdsResult t)) {
                    throw new IllegalStateException(each.name() + ": a raising verdict's statement is a typed result");
                }
                PlanRunner.Rows single = open.read(t, null, bound);
                if (single.rows().size() != 1) {
                    throw new IllegalStateException(each.name() + ": the verdict statement answered "
                            + single.rows().size() + " rows");
                }
                judges.verdict(each, single.rows().get(0));
            }
        }

        /** The DATABASE judge's rows for {@code group}: its fused statement, else each verdict's own wrapper (as the
         *  plan holds them), each row handed to the judges in the group's order. A fused statement the database
         *  rejects fails the program with the database's error, named: nothing is retried another way (P1.2,
         *  2026-11-07, G4 and G7). */
        private void databaseVerdicts(TestPlan.Verdicts group, ProgramRunner.OpenProgram open) throws IOException {
            Map<Integer, List<Object>> rows = new HashMap<>();
            ExecutionPlan.TdsResult fused = group.fused();
            if (fused != null) {
                // no savepoint guards it: a rejection fails the program, whose transaction is rolled back whole
                try {
                    for (List<Object> row : open.read(fused, null, Map.of()).rows()) {
                        if (row.isEmpty() || !(row.get(0) instanceof Number index)) {
                            throw new IllegalStateException("a fused verdict row's first cell is the verdict's index:"
                                    + " " + (row.isEmpty() ? "no cell" : row.get(0)));
                        }
                        rows.put(index.intValue(), row.subList(1, row.size()));
                    }
                    if (rows.size() != group.verdicts().size()) {
                        throw new IllegalStateException("the fused verdict statement answered " + rows.size()
                                + " rows for " + group.verdicts().size() + " verdicts");
                    }
                    judges.fused(group, null);
                } catch (DataError e) {
                    judges.fused(group, e);
                    // the database's own error first, as the host judge reports it and as legend-engine does, then the
                    // verdicts the rejected statement held, by name (round 25, 2026-11-09: a prefix before the
                    // database's text moved the DATABASE judge's message off the host judge's, and off Snowflake's
                    // channel B ledger)
                    throw new DataError(e.getMessage() + " (the database rejected the verdict statement of "
                            + group.verdicts().stream().map(TestPlan.Verdict::name).toList() + ")", e);
                }
            }
            for (int i = 0; i < group.verdicts().size(); i++) {
                TestPlan.Verdict each = group.verdicts().get(i);
                List<Object> row = rows.get(i);
                ExecutionPlan.Node wrapper = each.wrapper();
                if (row == null && wrapper != null) {
                    // a group planned with no fused statement runs its wrappers under its caller's origin (the batch's
                    // keyed verdicts as the body's: the planner-driver seam program's S3c, 2026-10-31)
                    PlanRunner.Rows single = open.read(wrapper, null, Map.of());
                    if (single.rows().size() != 1) {
                        throw new IllegalStateException(each.name() + ": the verdict statement answered "
                                + single.rows().size() + " rows");
                    }
                    row = single.rows().get(0);
                }
                judges.verdict(each, row);
            }
        }
    }

    /** {@code group}'s savepoint where the program's session is inside a transaction; else none. */
    private static ProgramRunner.@Nullable Savepoint guard(TestPlan.Verdicts group, ProgramRunner.OpenProgram open) {
        TestPlan.Verdicts.Savepoint sp = group.savepoint();
        return sp != null && open.insideATransaction()
                ? new ProgramRunner.Savepoint(sp.set(), sp.rollbackTo(), sp.release()) : null;
    }

    /** {@code each}'s base sides read for the host judge, in the base's order. */
    private static List<PlanRunner.Rows> hostSides(TestPlan.Verdict each, ProgramRunner.OpenProgram open)
            throws IOException {
        List<PlanRunner.Rows> sides = new ArrayList<>(each.base().size());
        for (TestPlan.Side side : each.base()) {
            sides.add(sideRows(side, open));
        }
        return sides;
    }

    /** A side's typed rows: a statement's or a keyed result's read through the open program, a constant's its TEXT
     *  cells as the program wrote them. */
    private static PlanRunner.Rows sideRows(TestPlan.Side side, ProgramRunner.OpenProgram open) throws IOException {
        return side instanceof TestPlan.Side.Constant c ? constantRows(c) : open.read(statement(side), null, Map.of());
    }

    /** The statement a side that sends one reads. */
    private static ExecutionPlan.Node statement(TestPlan.Side side) {
        return switch (side) {
            case TestPlan.Side.Rows r -> r.result();
            case TestPlan.Side.KeyedRows k -> k.keyed();
            case TestPlan.Side.Constant c -> throw new IllegalStateException("a constant side sends no statement");
        };
    }

    /** Which side of a branch runs: its condition's one Boolean, as the database answered it or the program wrote it
     *  (a constant's TEXT cell, its Pure spelling). An absent or many-valued condition is refused by name: Pure's
     *  {@code if} takes a {@code Boolean[1]}. */
    private static boolean taken(PlanRunner.Rows condition) {
        if (condition.rows().size() != 1 || condition.rows().get(0).size() != 1) {
            throw new IllegalStateException("a branch's condition answered " + condition.rows().size() + " rows: an"
                    + " if's condition is one Boolean");
        }
        Object cell = condition.rows().get(0).get(0);
        return switch (cell) {
            case Boolean v -> v;
            case String text when text.equals("true") || text.equals("false") -> Boolean.parseBoolean(text);
            case null -> throw new IllegalStateException("a branch's condition answered no value: an if's condition"
                    + " is one Boolean");
            default -> throw new IllegalStateException("a branch's condition answered a "
                    + cell.getClass().getSimpleName() + " (" + cell + "): an if's condition is one Boolean");
        };
    }

    /** A constant side's rows: its columns and its TEXT cells as the program wrote them. */
    static PlanRunner.Rows constantRows(TestPlan.Side.Constant c) {
        List<List<Object>> rows = new ArrayList<>(c.cells().size());
        for (List<String> row : c.cells()) {
            rows.add(new ArrayList<Object>(row));
        }
        return new PlanRunner.Rows(c.columns(), rows);
    }
}
