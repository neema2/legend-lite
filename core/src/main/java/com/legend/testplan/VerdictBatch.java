// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.testexecutionplan.Failure;


import com.legend.testexecutionplan.Row;

import com.legend.diagnostics.Census;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testexecutionplan.RootLoop;
import com.legend.testexecutionplan.TestPlan;

import com.legend.sql.SqlQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** LEG 3.4 — ONE VERDICT STATEMENT PER TEST BODY (docs/DATABASE_MODE_HOMEWORK
 * _2026_09_18.md §3 D1 stage B, the V12 spike's ratified rung). In database
 * mode a statement-root assert no longer sends its verdict statement at
 * the assert: the statement is DEFERRED into this per-body batch, keyed by
 * the assert's position, and the batch is sent as ONE statement (the
 * lowering's {@link Fusion}, a {@code UNION ALL} of the deferred
 * statements) when the body reaches a statement that is not an assert, or
 * ends. Verdicts are then reported in body order and the FIRST failure
 * raises exactly as before (the runner's first-failure sequencing and gate
 * 11's per-assert ledger keys are unchanged); a root's own steps (an
 * unrolled quantified assert is several statements, a static kind gate is
 * a decided outcome) decide in their order. A FUSED STATEMENT THE DATABASE
 * REJECTS at run (a side it raises on) fails the body by name, the
 * database's own error first, then the verdicts the statement held: nothing
 * is retried another way (P1.2, d67cd85c5, 2026-11-07: the run-time split
 * rung, which judged the batch statement by statement, is deleted). The one
 * split that remains is at PLANNING: a fused form the dialect cannot spell,
 * each member then its own statement, or refused alone (see {@link #plan}).
 * This class plans
 * and orders; the SQL shape is the lowering's, the judgment of a row is
 * the verdict arm's ({@link RootLoops.Judge}), and each statement is sent as a PLAN
 * by the {@link Runner} (the plan path program's step 6, 2026-10-18: a
 * verdict statement is a typed-rows plan whose rows the runner decodes;
 * this class holds no dialect, no shape and no executor). */
public final class VerdictBatch {

    /** The lowering's fused statement over the deferred statements, with
     * the frame definitions the sides reference at its head. */
    @FunctionalInterface
    public interface Fusion {
        /** {@code members}' rows as one statement, {@code frames} at its head; each member's {@code columns} its own,
         *  under its prefix (item 3's S4: one row). */
        SqlQuery fuse(List<SqlQuery> members, List<List<String>> columns, Map<String, SqlQuery> frames);
    }

    // (Either, the lowering's one row over a generic verdict row and its ruled row -- 2026-11-06, VerdictSql.either --
    // deleted with the ruled comparison, A4 of the compatibility program, stage A ratified by the user 2026-11-19: its
    // one caller, the rendered-text rule's rows fallback, left the database judge with legend-pure's comparison rules)

    /** The lowering's self-contained form of a verdict row (M8b): the assert's polarity and its failure message
     *  composed in the statement, so the judge reads holds and message and the plan carries nothing about the
     *  assert. */
    @FunctionalInterface
    public interface SelfContained {
        SqlQuery wrap(SqlQuery row, String name, boolean wantEqual);
    }

    /** The plan side's PLANNER for a verdict statement (the planner-driver seam program's S2e, 2026-10-30; until then
     *  its runner): {@code statement} as a typed-rows plan of the verdict schema ({@code fused}: the batch schema, an
     *  index column first), rendered for the body's session. The batch runs the plans itself, as a verdict group
     *  through the plan runner ({@code PlanRunner.program}); a wrapper the dialect cannot spell raises here. */
    public interface Runner {
        /** A typed result, or a keyed one where the statement reads a raw grid's column names or cells (S3c), its
         *  target the database of the session {@code on} it runs on (the body's, or the graph's system database beside
         *  it: per-statement routing, 2026-11-03 -- until then a verdict over the system store on a server engine was
         *  planned against the body's target, so the database judge left such a body to the walk). */
        ExecutionPlan.Node plan(SqlQuery statement, com.legend.compiler.element.type.Type.RelationType schema,
                ExecutionPlan.Target on);

        /** Whether {@code statement} reads a raw grid's report, so its wrapper is keyed and never fused (the keys are
         *  one grid's columns; the planner-driver seam program's S3c, 2026-10-30). */
        boolean keyed(SqlQuery statement);

        /** The statement's rendered length (the prepare trace's census). */
        int chars(SqlQuery statement);

        /** The most characters a fused statement may have on the body's database ({@code SqlDialect
         *  .fusedStatementBudget}); a longer group is planned as several fused statements. */
        default int fusedBudget() {
            return Integer.MAX_VALUE;
        }

        /** The savepoint that guards a fused statement inside a transaction where the body's database aborts one on a
         *  failing statement (F11, the test-is-the-product plan's B12), or null where none is needed or none exists
         *  ({@code SqlDialect.verdictSavepoint}). */
        TestPlan.Verdicts.@com.legend.base.Nullable Savepoint savepoint();
    }

    /** The arm a verdict's name spells: the assert function's simple name (the name is its callee's FQN, or the
     *  arm's own word where there is none). */
    static String armOf(String name) {
        int i = name.lastIndexOf("::");
        String simple = i < 0 ? name : name.substring(i + 2);
        int u = simple.indexOf('_');
        return u > 0 ? simple.substring(0, u) : simple;
    }

    /** A deferred verdict statement, or an outcome the arm decided
     * without one (a static kind gate, an UNJUDGED decline, a side that
     * errored while planning). */
    private sealed interface Step permits Pending, Resolved, Judged {
    }

    /** A verdict the arm decided at planning, counted by the census at the root's turn ({@link #judged}). */
    private record Judged() implements Step {
    }

    /** A deferred verdict row. (Its ruled comparison {@code orElse}, its appeal and its decline went with A4 of the compatibility program, stage A ratified by the user 2026-11-19:
     *  the rendered-text rule's rows fallback and the edge's engine-text rows left the database judge.) */
    /** A deferred verdict row: its statement, its columns' schema (the seven-column verdict schema, or a typed
     *  member's own), and a typed member's report (item 3's S4; null for a seven-column row). */
    private record Pending(int ix, String name, SqlQuery query, ExecutionPlan.Target on,
            com.legend.compiler.element.type.Type.RelationType schema, RootLoop.@com.legend.base.Nullable Report report,
            @com.legend.base.Nullable SqlQuery message, ExecutionPlan.@com.legend.base.Nullable Target messageOn,
            com.legend.testexecutionplan.@com.legend.base.Nullable Failure unplanned)
            implements Step {
    }

    /** A root resolved before any row: a static decision, an unjudged raise, a planned raise ({@code reported}: the
     *  listener hears its verdict), or a WALL the arm left through ({@code reported} false: the walk discards the root
     *  and surfaces the wall after the roots before it; the program road raises it at its turn the same way). */
    private record Resolved(com.legend.testexecutionplan.Failure failure, boolean reported) implements Step {
    }

    private static final class Root {
        final String listenerName;
        final List<Step> steps = new ArrayList<>();
        /** The caller's message where it READS ({@link #message}): its row, sent only where the root fails. */
        @com.legend.base.Nullable SqlQuery message;
        ExecutionPlan.@com.legend.base.Nullable Target messageOn;
        /** The caller's message's error where computing it raises ({@link #unplanned}). */
        com.legend.testexecutionplan.@com.legend.base.Nullable Failure unplanned;

        Root(String listenerName) {
            this.listenerName = listenerName;
        }
    }

    /** CENSUS: frames built under a batch, by how the asserts read them —
     * {@code cte} (a relation-rooted frame of static schema, planned once),
     * {@code pasted} (relation-rooted but late-bound or not eager), {@code
     * class} (a class- or scalar-rooted frame: the chain pastes). */
    public enum FrameRead { CTE, PASTED, CLASS, CLASS_CTE }

    public static void frame(FrameRead how) {
        switch (how) {
            case CTE -> Census.inc(Census.Key.FRAME_CTE);
            case PASTED -> Census.inc(Census.Key.FRAME_PASTED);
            case CLASS -> Census.inc(Census.Key.FRAME_CLASS);
            case CLASS_CTE -> Census.inc(Census.Key.FRAME_CLASS_CTE);
        }
    }

    public static String frameCensus() {
        return "cte=" + Census.count(Census.Key.FRAME_CTE) + " pasted=" + Census.count(Census.Key.FRAME_PASTED)
                + " class=" + Census.count(Census.Key.FRAME_CLASS) + " class-cte=" + Census.count(Census.Key.FRAME_CLASS_CTE);
    }

    private final Fusion fusion;
    /** The body's verdict ids: each verdict this batch plans takes the next. */
    private final VerdictIds ids;
    private final SelfContained selfContained;
    private final Runner runner;
    private final List<Root> roots = new ArrayList<>();
    /** The sentence frame the rows deferred now are planned under (lite's own rules): an unrolled quantifier's element
     *  index (-1 for none) and whether they are {@code assertIsNot}'s (#28). One field, set and restored by scope. */
    private record Scope(int elementIndex, boolean isNot) {
    }

    private Scope scope = new Scope(-1, false);

    /** Rows deferred until the returned restore runs are {@code assertIsNot}'s. */
    public Runnable asIsNot() {
        Scope before = scope;
        scope = new Scope(before.elementIndex(), true);
        return () -> scope = before;
    }

    /** Rows deferred until the returned scope closes are an unrolled quantifier's element {@code index}: lite's
     *  sentence names it ({@code FrameKind.AT_INDEX}). */
    public Runnable atElement(int index) {
        Scope before = scope;
        scope = new Scope(index, before.isNot());
        return () -> {
            scope = before;
            if (elementMessage != null || elementUnplanned != null) {
                // an element whose message no row of it carried (decided at planning, or a row S4 has not typed)
                elementMessage = null;
                elementUnplanned = null;
                throw new com.legend.error.NotImplementedException("an unrolled element's caller's message that reads"
                        + " or raises, over an element no typed row judges: not planned");
            }
        };
    }

    /** The element in scope's caller's message where it READS (its row, its session), or where computing it raises
     *  (its error): carried by the element's own row, read or raised only where THAT element fails (item 3's S4 step 3,
     *  the user's decision 4, 2026-11-22). */
    private @com.legend.base.Nullable SqlQuery elementMessage;
    private ExecutionPlan.@com.legend.base.Nullable Target elementMessageOn;
    private com.legend.testexecutionplan.@com.legend.base.Nullable Failure elementUnplanned;
    private @com.legend.base.Nullable Root current;
    /** The frames the body's asserts read by reference: name → the frame's
     * plan (leg 3.4 step 2), defined once per body. */
    private final Map<String, SqlQuery> frames = new LinkedHashMap<>();


    /** The batch's words of a failure (census J29): an unrolled element named as they name it. */
    private final com.legend.lowering.VerdictWords words;

    public VerdictBatch(Fusion fusion, SelfContained selfContained, Runner runner,
            VerdictIds ids, com.legend.lowering.VerdictWords words) {
        this.ids = ids;
        this.words = words;
        this.fusion = fusion;
        this.selfContained = selfContained;
        this.runner = runner;
    }

    /** A statement-root assert begins: its steps accrue until {@link #close}. */
    public void open(String listenerName) {
        if (current != null) {
            throw new IllegalStateException("verdict batch: a root is already open");
        }
        current = new Root(listenerName);
        roots.add(current);
    }

    public boolean active() {
        return current != null;
    }

    public void defineFrame(String name, SqlQuery plan) {
        // the body's aliases under the frame's own prefix: the fused statement
        // keeps the renderers' invariant that an alias is unique statement-wide
        plan = com.legend.sql.AliasPrefix.apply(com.legend.sql.AliasPrefix.frameBody(name), plan);
        SqlQuery prev = frames.putIfAbsent(name, plan);
        if (prev != null && !prev.equals(plan)) {
            throw new IllegalStateException("verdict batch: two plans under the frame '" + name + "'");
        }
    }

    public Map<String, SqlQuery> frames() {
        return java.util.Collections.unmodifiableMap(frames);
    }

    /** The open root's caller's message where it READS (a let, a query): {@code row}, its one value as a verdict row
     *  ({@code VerdictSql.messageRow}), on {@code on} -- sent by the root loop at the root's place only where the root
     *  fails, its value the failure's text; a raise reading it is the failure (upstream evaluates the message only
     *  where the assert fails, and its evaluation's error is the test's; the assert bar, 2026-11-04). */
    public void message(SqlQuery row, ExecutionPlan.Target on) {
        Root r = java.util.Objects.requireNonNull(current, "verdict batch: no open root");
        if (scope.elementIndex() >= 0) {
            // an unrolled element's own message, its row's (decision 4)
            elementMessage = row;
            elementMessageOn = on;
            return;
        }
        if (r.unplanned != null || r.message != null && !r.message.equals(row)) {
            // a root carries ONE caller's message: an unrolled quantifier's elements whose messages differ, or one that
            // raises beside one that reads, are each element's own (S4 carries a message per step; the S2+S3 fixes'
            // audit, 2026-11-21: until then the last element's message silently stood for every element's)
            throw oneMessagePerRoot(r);
        }
        r.message = row;
        r.messageOn = on;
    }

    /** The open root's caller's message raises {@code error} where it is computed, an error upstream raises at run:
     *  raised in place of the root's failure where the root fails, and only there (item 3, 2026-11-21, the user's lazy
     *  rule; until then lite's sentence around the error's text as the verdict row's message). */
    public void unplanned(com.legend.testexecutionplan.Failure error) {
        Root r = java.util.Objects.requireNonNull(current, "verdict batch: no open root");
        if (scope.elementIndex() >= 0) {
            // an unrolled element's message's error: raised only where THAT element fails, by its own row (decision 4;
            // the S2+S3 fixes' audit, 2026-11-21: one slot per root replaced a later element's failure)
            elementUnplanned = error;
            return;
        }
        if (r.unplanned != null || r.message != null) {
            throw oneMessagePerRoot(r);
        }
        r.unplanned = error;
    }

    /** The refusal of a second caller's message on one root, by name: the body is refused at planning. */
    private static com.legend.error.NotImplementedException oneMessagePerRoot(Root r) {
        return new com.legend.error.NotImplementedException("the database judge carries one caller's message per"
                + " assert: " + r.listenerName + "'s elements each have a message that reads or raises");
    }

    /** {@code query}, the open root's verdict row. (Its overload with an appeal and a decline, the edge's engine-text
     *  rows', went with A4 of the compatibility program, stage A ratified by the user 2026-11-19.) */
    public void defer(String name, boolean wantEqual, SqlQuery query, ExecutionPlan.Target on) {
        Root r = java.util.Objects.requireNonNull(current, "verdict batch: no open root");
        int ix = 0;
        for (Root x : roots) {
            for (Step s : x.steps) {
                if (s instanceof Pending) {
                    ix++;
                }
            }
        }
        // the row self-contained (M8b): its polarity and its message in the statement, nothing of the assert kept
        SqlQuery row = selfContained.wrap(scope.isNot() ? com.legend.lowering.VerdictSql.withIsNotHint(query) : query, name,
                wantEqual);
        if (scope.elementIndex() >= 0) {
            // an unrolled quantifier's element, named as the words name it (lite's: by its zero-based index)
            row = com.legend.lowering.VerdictSql.atIndex(row, scope.elementIndex(), words);
        }
        if (elementMessage != null || elementUnplanned != null) {
            // a seven-column row reads no element message (its words are in its statement): not planned
            elementMessage = null;
            elementUnplanned = null;
            throw new com.legend.error.NotImplementedException(name + ": an unrolled element's caller's message that"
                    + " reads or raises, over a row S4 has not typed: not planned");
        }
        r.steps.add(new Pending(ix, name, row, on, com.legend.lowering.VerdictSql.schema(), null, null, null, null));
    }

    /**
     * A TYPED verdict member (item 3's S4, 2026-11-21, the user's Q-A): {@code row}, one row of the columns
     * {@code schema} declares -- its verdict, under the assert's polarity, then the typed values its {@code report}'s
     * family reads, written where the verdict fails -- deferred on the open root. Nothing wraps it: the run records its
     * failure from its values (RootLoops, by the report). An unrolled quantifier's element frames the report at its
     * index.
     */
    public void deferTyped(String name, SqlQuery row, com.legend.compiler.element.type.Type.RelationType schema,
            RootLoop.Report report, ExecutionPlan.Target on) {
        Root r = java.util.Objects.requireNonNull(current, "verdict batch: no open root");
        if (scope.isNot()) {
            // assertIsNot's (#28): its failure the IS_NOT record of the actual side, as the host's negated judgment's
            report = new RootLoop.Report(RootLoop.Family.IS_NOT, report.want(), report.e(), report.a(), report.fixed(),
                    report.index());
        }
        int ix = 0;
        for (Root x : roots) {
            for (Step s : x.steps) {
                if (s instanceof Pending) {
                    ix++;
                }
            }
        }
        r.steps.add(new Pending(ix, name, row, on, schema,
                scope.elementIndex() >= 0 ? report.atElement(scope.elementIndex()) : report, elementMessage,
                elementMessageOn, elementUnplanned));
        elementMessage = null;
        elementMessageOn = null;
        elementUnplanned = null;
    }

    /** The batch's one root as ONE statement (the user's ruling S135, 2026-11-05: an assert inside assertError is a
     *  statement that raises its own failure): its one verdict row, self-contained, with the batch's frames at its head
     *  and the session it reads; or the failure it was decided with at planning; null where the batch holds no root,
     *  several, a root of several steps, a caller's message that reads, or a verdict row whose message raises
     *  (no one row decides it alone). */
    public @com.legend.base.Nullable Sole sole() {
        // a static verdict's count (Judged) is no step of the one statement: the statement is the assert's, its census
        // the enclosing assertError's
        List<Step> steps = roots.size() != 1 ? List.of()
                : roots.get(0).steps.stream().filter(s -> !(s instanceof Judged)).toList();
        if (roots.size() != 1 || steps.size() != 1 || roots.get(0).message != null) {
            return null;
        }
        Step step = steps.get(0);
        com.legend.testexecutionplan.@com.legend.base.Nullable Failure unplanned = roots.get(0).unplanned;
        if (step instanceof Pending p) {
            // a row whose failure is its message's error raises that error where it fails, which a raising row cannot
            // spell: no one row decides it, so the assertError is refused by name (the S2+S3 fixes' audit, HIGH,
            // 2026-11-21: until then the row raised its own failure and the error was dropped)
            // (an unrolled element's own message, decision 4, is likewise no one row's: refused the same way)
            return unplanned != null || p.message() != null || p.unplanned() != null ? null
                    : new Sole.Row(com.legend.sql.FrameCtes.attach(p.query(), frames()), p.on(), p.schema(),
                            p.report());
        }
        if (step instanceof Resolved r && r.reported() && r.failure().kind() == com.legend.testexecutionplan.Failure.Kind.ASSERT_FAILED
                && r.failure().unjudgedReason() == null) {
            // the assert fails at planning, so its message's error, where computing it raises, is the outcome (the
            // user's lazy rule, as RootLoops raises it)
            return new Sole.Decided(unplanned != null ? unplanned : r.failure());
        }
        return null;
    }

    /** {@link #sole}'s answer: a verdict row the database computes, or a failure decided at planning. */
    public sealed interface Sole permits Sole.Row, Sole.Decided {
        /** The verdict row ({@code VerdictSql.schema}'s columns) and the session it reads. */
        record Row(SqlQuery query, ExecutionPlan.Target on, com.legend.compiler.element.type.Type.RelationType schema,
                RootLoop.@com.legend.base.Nullable Report report) implements Sole {
        }

        /** The assert's failure, decided at planning. */
        record Decided(com.legend.testexecutionplan.Failure failure) implements Sole {
        }
    }

    // (orElse, a row's RULED comparison planned with it -- 2026-11-04, the core program; one row with it since
    // 2026-11-06 -- deleted with A4 of the compatibility program, stage A ratified by the user 2026-11-19: its one
    // caller was the rendered-text rule's rows fallback, compatibility mode's S131/S132, which left the database judge
    // with legend-pure's comparison rules)

    /** THE FRAGMENT MAP (block-compiler stage 2, 2026-09-21): every frame name and
     * verdict branch index → the let / assert it came from, so a database error
     * on the fused statement names the statement that owns it. */
    private final Map<String, String> fragments = new java.util.LinkedHashMap<>();

    public void fragments(Map<String, String> map) {
        fragments.putAll(map);
    }

    /** The open root's arm decided its verdict at planning (a static decision): a step the runner counts
     *  at the root's turn (J39, 2026-11-18, the orchestrator's ruling (b): the census counted it at planning). */
    public void judged() {
        Root r = java.util.Objects.requireNonNull(current, "verdict batch: no open root for a static verdict");
        r.steps.add(new Judged());
    }

    public void resolve(com.legend.testexecutionplan.Failure failure) {
        java.util.Objects.requireNonNull(current, "verdict batch: no open root")
                .steps.add(new Resolved(failure, true));
    }

    /** The open root left through a WALL (not a verdict, not a data error): raised at its turn after the roots before
     *  it are judged, told to no listener -- as the walk's {@code batched} discards it and surfaces it after the flush
     *  (the test-is-the-product plan's M7: the program road plans the raise at the root's place; its exit is counted
     *  where it was planned, as the walk counts it at the arm). */
    public void wall(com.legend.testexecutionplan.Failure failure) {
        java.util.Objects.requireNonNull(current, "verdict batch: no open root")
                .steps.add(new Resolved(failure, false));
    }

    public void close() {
        current = null;
    }

    /** The open root was not a verdict after all (the arm answered null,
     * or left through a wall): dropped with its steps. */
    public void discard() {
        java.util.Objects.requireNonNull(current, "verdict batch: no open root");
        roots.remove(roots.size() - 1);
        current = null;
    }

    public boolean isEmpty() {
        return roots.isEmpty();
    }

    /** One verdict group of a planned batch: its step, the session it runs on, whether it is the keyed members' group
     *  (each verdict its keyed wrapper, no fused statement; a rejection fails the program, P1.2, 2026-11-07). */
    public record Group(TestPlan.Verdicts step, ExecutionPlan.Target on, boolean keyed) {
    }

    /**
     * THE BATCH PLANNED (the test-is-the-product plan's M7): the deferred verdicts as verdict groups -- one fused
     * statement per session over the batch's frames (where the dialect cannot spell it, each member its own statement:
     * the split rung at planning), the keyed members a group of their own -- and the judgment over the rows the groups answer
     * ({@link #judge}: the root loop, in body order, the first failure raising). The walk's flush (deleted 2026-11-07) ran the
     * groups through the plan runner between the two; the program road places the groups as steps of the body's plan
     * and judges at the last of them. Null where nothing was deferred (no root).
     */
    public @com.legend.base.Nullable Planned plan() {
        if (roots.isEmpty()) {
            return null;
        }
        List<Root> batch = new ArrayList<>(roots);
        roots.clear();
        current = null;
        // (ONE verdict row for a generic row and its ruled row, planned here since 2026-11-06: deleted with the ruled
        // comparison, A4 of the compatibility program, stage A ratified by the user 2026-11-19)
        Map<ExecutionPlan.Target, List<Pending>> groups = new LinkedHashMap<>();
        for (Root r : batch) {
            for (Step s : r.steps) {
                if (s instanceof Pending p) {
                    groups.computeIfAbsent(p.on(), k -> new ArrayList<>()).add(p);
                }
            }
        }
        // a caller's message that READS, planned now as a side of the plan (P1.2, the goals of record, 2026-11-07: exec
        // runs only the plan's statements): on its root's first verdict in the plan (Verdict.message, attached once
        // the groups are made: carried), read through the run's reader only where the root fails; a message the
        // dialect cannot spell refuses the body at planning (D, the user's ruling of 2026-11-21: a dialect's refusal is
        // never an error upstream raises at run; the BE audit 2's nit 2: until then a map of refusals that stayed empty)
        // (by the root's place in the batch: the step 5 of 2026-11-08 deleted the batch's maps keyed by identity)
        Map<Integer, TestPlan.Side> messages = new HashMap<>();
        for (int k = 0; k < batch.size(); k++) {
            Root r = batch.get(k);
            if (r.message == null) {
                continue;
            }
            Ahead a = planAhead(r.listenerName, r.message, com.legend.lowering.VerdictSql.schema(),
                    java.util.Objects.requireNonNull(r.messageOn));
            if (a.refused() != null) {
                throw a.refused();
            }
            messages.put(k, sideOf(java.util.Objects.requireNonNull(a.plan())));
        }
        List<Group> out = new ArrayList<>();
        Map<Integer, Pending> members = new java.util.HashMap<>();
        // the members the dialect cannot spell (the split rung at planning): each refused at its place, nothing sent
        // for it (2026-11-04, the core program)
        List<Pending> unspelled = new ArrayList<>();
        for (var g : groups.entrySet()) {
            List<Pending> all = g.getValue();
            // the group as a plan value (S2e): the fused statement its wrapper over wrappers, each verdict a step with
            // no wrapper of its own here
            // a member whose statement reads a raw grid's report is keyed: its wrapper's keys are its grid's columns,
            // which no fused statement shares, so the keyed members are a group of their own, each its wrapper
            // (a member is keyed by its statement with the batch's frames, as it is planned: a frame may read a report)
            Group keyedGroup = ownStatements(all.stream().filter(this::keyed).toList(), g.getKey(), members,
                    unspelled, true);
            if (keyedGroup != null) {
                out.add(keyedGroup);
            }
            List<Pending> ps = all.stream().filter(p -> !keyed(p)).toList();
            if (ps.isEmpty()) {
                continue;
            }
            // a new fused group at each member that may raise (the compatibility owner's (beta), 2026-11-19, G2): a
            // database that rejects a fused statement names no verdict, so its rejection is its group's FIRST verdict's
            // failure (TestRun, RootLoops) -- exact only where no later member of the group may raise. Until then one
            // statement held every member, and Postgres's refusal of testAdjustByMinutesBigNumber's second expected
            // literal was blamed on the first assert, which the host held
            for (List<Pending> run : raisingRuns(ps)) {
                ExecutionPlan.TdsResult fusedPlan;
                try {
                    fusedPlan = fusedOf(run, g.getKey());
                } catch (com.legend.sql.DialectCapability e) {
                    // the split rung at planning (P1, 2026-11-07, the milestone audit's 1): the fused statement the
                    // dialect cannot spell -- each member its own statement, a group with no fused statement as the
                    // keyed members' (the runner runs each verdict's wrapper); a member whose own statement the dialect
                    // cannot spell is refused at its place in the root loop, nothing sent. Until the audit the members
                    // the dialect spelled alone were dropped from every group, and the root loop found no row for them
                    com.legend.diagnostics.Census.verdictFallback(e, fragments);
                    Group own = ownStatements(run, g.getKey(), members, unspelled, false);
                    if (own != null) {
                        out.add(own);
                    }
                    continue;
                }
                for (Map.Entry<List<Pending>, ExecutionPlan.TdsResult> part : withinBudget(run, fusedPlan,
                        g.getKey())) {
                    List<TestPlan.Verdict> verdicts = new ArrayList<>(part.getKey().size());
                    for (Pending p : part.getKey()) {
                        TestPlan.Verdict v = new TestPlan.Verdict(ids.next(), p.name(), armOf(p.name()), List.of(),
                                null);
                        verdicts.add(v);
                        members.put(v.id(), p);
                    }
                    out.add(new Group(new TestPlan.Verdicts(verdicts, part.getValue(), runner.savepoint()),
                            g.getKey(), false));
                }
            }
        }
        // a member the dialect cannot spell refuses the body at planning, nothing sent (D, 2026-11-21, as above; P1.2,
        // 2026-11-07: the root loop sends nothing outside the plan -- the split rung's run-time retry and its
        // statements planned outside the plan are deleted)
        for (Pending u : unspelled) {
            com.legend.sql.DialectCapability refused = planAhead(u.name(), u.query(), u.schema(), u.on()).refused();
            if (refused == null) {
                throw new IllegalStateException(u.name() + ": a member left out of its fused statement as unspelled"
                        + " whose own statement the dialect spells");
            }
            throw refused;
        }
        // an unrolled element's own message that READS (decision 4): planned as its row's side, carried on its verdict
        Map<Integer, TestPlan.Side> rowMessages = new HashMap<>();   // by the member's ix
        for (Root r : batch) {
            for (Step st : r.steps) {
                if (st instanceof Pending p && p.message() != null) {
                    Ahead a = planAhead(p.name(), p.message(), com.legend.lowering.VerdictSql.schema(),
                            java.util.Objects.requireNonNull(p.messageOn()));
                    if (a.refused() != null) {
                        throw a.refused();
                    }
                    rowMessages.put(p.ix(), sideOf(java.util.Objects.requireNonNull(a.plan())));
                }
            }
        }
        List<Group> placed = carried(batch, out, members, messages, rowMessages);
        return new Planned(placed, loop(batch, placed, members, messages, rowMessages));
    }

    /** The segment's roots as the plan's data (the plan-as-data design's step 5, 2026-11-08): each root's rows by the
     *  verdict that reads them, its decided outcomes, its caller's message -- what {@link RootLoops} judges at run. A
     *  row's and a message's refusal slots are empty since D (a dialect's refusal refuses the body at planning). */
    private RootLoop loop(List<Root> batch, List<Group> groups, Map<Integer, Pending> members,
            Map<Integer, TestPlan.Side> messages, Map<Integer, TestPlan.Side> rowMessages) {
        Map<Integer, Integer> verdictOf = new HashMap<>();   // a member's ix -> the id of the verdict reading its row
        members.forEach((id, p) -> verdictOf.put(p.ix(), id));
        List<RootLoop.Root> roots = new ArrayList<>(batch.size());
        for (int k = 0; k < batch.size(); k++) {
            Root r = batch.get(k);
            List<RootLoop.Step> steps = new ArrayList<>(r.steps.size());
            for (Step s : r.steps) {
                steps.add(switch (s) {
                    case Pending p -> {
                        Integer id = verdictOf.get(p.ix());
                        TestPlan.Side own = rowMessages.get(p.ix());
                        yield new RootLoop.Row(p.name(), java.util.Objects.requireNonNull(id,
                                () -> p.name() + ": a member with no verdict reading its row"), p.report(),
                                own == null ? null : new RootLoop.Message(own, groups.stream()
                                        .flatMap(g -> g.step().verdicts().stream())
                                        .anyMatch(v -> own.equals(v.message()))), p.unplanned());
                    }
                    case Resolved rv -> new RootLoop.Decided(rv.failure(), rv.reported());
                    case Judged j -> new RootLoop.Judged();
                });
            }
            RootLoop.Message message = null;
            if (r.message != null) {
                TestPlan.Side side = java.util.Objects.requireNonNull(messages.get(k), () -> r.listenerName
                        + ": a caller's message with no planned side");
                message = new RootLoop.Message(side, groups.stream().flatMap(g -> g.step().verdicts().stream())
                        .anyMatch(v -> side.equals(v.message())));
            }
            roots.add(new RootLoop.Root(r.listenerName, steps, message, r.unplanned));
        }
        return new RootLoop(roots, fragments);
    }

    /** {@code groups} with each root's caller's message on the root's first verdict in them (the milestone audit's 3,
     *  2026-11-07: until then the message rode the root's first pending member, which a refused member or a root
     *  decided at planning left in no group); {@code members} keyed by the verdicts as rebuilt. */
    private static List<Group> carried(List<Root> batch, List<Group> groups, Map<Integer, Pending> members,
            Map<Integer, TestPlan.Side> messages, Map<Integer, TestPlan.Side> rowMessages) {
        Map<Integer, TestPlan.Side> carry = new HashMap<>();   // by the member's ix
        java.util.Set<Integer> grouped = new java.util.HashSet<>();
        for (Pending p : members.values()) {
            grouped.add(p.ix());
        }
        // an element's own message rides its own verdict (decision 4)
        rowMessages.forEach((ix, side) -> {
            if (grouped.contains(ix)) {
                carry.put(ix, side);
            }
        });
        for (int k = 0; k < batch.size(); k++) {
            TestPlan.Side side = messages.get(k);
            if (side == null) {
                continue;
            }
            for (Step step : batch.get(k).steps) {
                if (step instanceof Pending p && grouped.contains(p.ix())) {
                    if (carry.containsKey(p.ix())) {
                        // a verdict carries one message: the root's and its first element's own, refused by name
                        throw new com.legend.error.NotImplementedException(batch.get(k).listenerName
                                + ": an assert's caller's message and its first element's own message, both reading"
                                + " the database, on one verdict");
                    }
                    carry.put(p.ix(), side);
                    break;
                }
            }
        }
        if (carry.isEmpty()) {
            return groups;
        }
        List<Group> out = new ArrayList<>(groups.size());
        for (Group g : groups) {
            List<TestPlan.Verdict> verdicts = new ArrayList<>(g.step().verdicts().size());
            for (TestPlan.Verdict v : g.step().verdicts()) {
                Pending p = members.get(v.id());
                TestPlan.Side side = p == null ? null : carry.get(p.ix());
                if (side == null) {
                    verdicts.add(v);
                    continue;
                }
                TestPlan.Verdict withMessage = new TestPlan.Verdict(v.id(), v.name(), v.arm(), v.base(), v.wrapper(),
                        side);
                // the same verdict with its message: the same id, so its member stands
                verdicts.add(withMessage);
            }
            out.add(new Group(new TestPlan.Verdicts(verdicts, g.step().fused(), g.step().savepoint()),
                    g.on(), g.keyed()));
        }
        return out;
    }

    /** A statement planned at planning: its plan, or the dialect's refusal of it. */
    private record Ahead(ExecutionPlan.@com.legend.base.Nullable Node plan,
            com.legend.sql.@com.legend.base.Nullable DialectCapability refused) {
    }

    private Ahead planAhead(String name, SqlQuery query, com.legend.compiler.element.type.Type.RelationType schema,
            ExecutionPlan.Target on) {
        try {
            return new Ahead(runner.plan(com.legend.sql.FrameCtes.attach(query, frames), schema, on), null);
        } catch (com.legend.sql.DialectCapability e) {
            return new Ahead(null, e);
        }
    }

    /** THE PLANNED BATCH, AS DATA (the plan-as-data design's step 5, 2026-11-08): its verdict groups, in the order they
     *  run (a session's keyed group, then its fused group, per session), and its root loop -- the judgment's data,
     *  judged at run from the plan by {@link RootLoops}. Until this step it held the members, the refusals and the
     *  messages in maps keyed by object identity and judged the rows itself. */
    public record Planned(List<Group> groups, RootLoop loop) {
        public Planned {
            groups = List.copyOf(groups);
        }
    }

    /** A planned verdict statement as a side of the plan the run's reader reads. */
    private static TestPlan.Side sideOf(ExecutionPlan.Node node) {
        return switch (node) {
            case ExecutionPlan.TdsResult t -> new TestPlan.Side.Rows(t);
            case ExecutionPlan.Keyed k -> new TestPlan.Side.KeyedRows(k);
            default -> throw new IllegalStateException("a verdict statement is a typed or keyed result");
        };
    }

    /** Whether {@code p}'s statement, with the batch's frames as it is planned, reads a raw grid's report. */
    private boolean keyed(Pending p) {
        return runner.keyed(com.legend.sql.FrameCtes.attach(p.query(), frames));
    }

    /** Members as a verdict group of their own, each verdict its own wrapper, no fused statement: the keyed members
     *  (the planner-driver seam program's S3c, 2026-10-31: a raw grid's report, which no fused statement shares), and
     *  the members of a fused statement the dialect cannot spell (the split rung at planning, P1, 2026-11-07). A
     *  wrapper the dialect cannot spell leaves its member out, unspelled: the root loop raises its refusal at its place,
     *  nothing sent. Null where no member is spelled. */
    private @com.legend.base.Nullable Group ownStatements(List<Pending> keyed, ExecutionPlan.Target on,
            Map<Integer, Pending> members, List<Pending> unspelled, boolean isKeyed) {
        List<TestPlan.Verdict> verdicts = new ArrayList<>(keyed.size());
        for (Pending p : keyed) {
            try {
                TestPlan.Verdict v = new TestPlan.Verdict(ids.next(), p.name(), armOf(p.name()), List.of(),
                        runner.plan(com.legend.sql.FrameCtes.attach(p.query(), frames), p.schema(), on));
                verdicts.add(v);
                members.put(v.id(), p);
            } catch (com.legend.sql.DialectCapability e) {
                unspelled.add(p);
            }
        }
        return verdicts.isEmpty() ? null : new Group(new TestPlan.Verdicts(verdicts, null), on,
                isKeyed);
    }

    /** {@code ps} in runs, in order, a new run at each member whose statement may raise ({@link MayRaise}): within a run
     *  only its first member may, so a rejection of a run's fused statement -- or of a half of it, the budget's split
     *  keeps the order -- is that first member's (the compatibility owner's (beta), 2026-11-19). The gap, named: what
     *  a database refuses beside no raising function -- a literal past its range (a date's), or a cast reading text as
     *  a number it does not spell (casts are left out of SqlExpr.raisesItself, measured) -- is not seen, and stays
     *  blamed on its run's first member. */
    private static List<List<Pending>> raisingRuns(List<Pending> ps) {
        List<List<Pending>> out = new ArrayList<>();
        List<Pending> run = new ArrayList<>();
        for (Pending p : ps) {
            if (!run.isEmpty() && MayRaise.of(p.query())) {
                out.add(run);
                run = new ArrayList<>();
            }
            run.add(p);
        }
        out.add(run);
        return out;
    }

    /** Whether any expression of a statement, at any depth, may raise an error itself ({@code SqlExpr.raisesItself}):
     *  a walk that reads, the statement unchanged. */
    private static final class MayRaise extends com.legend.sql.SqlRewriter {
        /** The expressions the walk met that may raise (as HostSideBatch's RowBound collects what it meets). */
        private final List<com.legend.sql.SqlExpr> met = new ArrayList<>();

        static boolean of(SqlQuery q) {
            MayRaise walk = new MayRaise();
            walk.rewrite(q);
            return !walk.met.isEmpty();
        }

        @Override
        protected com.legend.sql.SqlExpr expr(com.legend.sql.SqlExpr e) {
            if (com.legend.sql.SqlExpr.raisesItself(e)) {
                met.add(e);
            }
            return e;
        }
    }

    /** A fused statement's plan: one typed result (a member that reads a raw grid's report never joins one). */
    /** The most members one fused statement joins: its wide row joins one derived table per member, and SQLite joins at
     *  most 64 tables, MariaDB and MySQL 61 (the S4 audit's MEDIUM 2, 2026-11-22: the row's width, which the
     *  character budget does not see); a larger group is halved as an over-budget one is. */
    static final int FUSED_MEMBERS = 60;

    /** {@code ps}'s fused statement {@code fused}, or -- where its text passes the database's budget
     *  ({@link Runner#fusedBudget}) -- {@code ps} halved, each half fused and halved again until it fits or is one
     *  member: the same members in their order, each part a fused group of its own, all planned here (2026-11-05, PCT
     *  round 22: ClickHouse's analyzer over 50 asserts of nested instances, 1.3M characters). */
    private List<Map.Entry<List<Pending>, ExecutionPlan.TdsResult>> withinBudget(List<Pending> ps,
            ExecutionPlan.TdsResult fused, ExecutionPlan.Target on) {
        if (ps.size() == 1 || ps.size() <= FUSED_MEMBERS && fused.sql().statement().length() <= runner.fusedBudget()) {
            return List.of(Map.entry(ps, fused));
        }
        List<Map.Entry<List<Pending>, ExecutionPlan.TdsResult>> out = new ArrayList<>();
        int half = ps.size() / 2;
        for (List<Pending> part : List.of(ps.subList(0, half), ps.subList(half, ps.size()))) {
            out.addAll(withinBudget(part, fusedOf(part, on), on));
        }
        return out;
    }

    /** {@code ps}'s fused statement. */
    private ExecutionPlan.TdsResult fusedOf(List<Pending> ps, ExecutionPlan.Target on) {
        List<SqlQuery> ms = new ArrayList<>(ps.size());
        List<List<String>> columns = new ArrayList<>(ps.size());
        List<com.legend.compiler.element.type.Type.RelationType> schemas = new ArrayList<>(ps.size());
        for (Pending p : ps) {
            ms.add(p.query());
            columns.add(p.schema().columns().stream()
                    .map(com.legend.compiler.element.type.Type.RelationType.Column::name).toList());
            schemas.add(p.schema());
        }
        return typed(runner.plan(fusion.fuse(ms, columns, frames), com.legend.lowering.VerdictSql.batchSchema(schemas),
                on));
    }

    private static ExecutionPlan.TdsResult typed(ExecutionPlan.Node plan) {
        if (plan instanceof ExecutionPlan.TdsResult t) {
            return t;
        }
        throw new IllegalStateException("a fused verdict statement planned as " + plan.getClass().getSimpleName()
                + ": a keyed member is judged at its place");
    }


}
