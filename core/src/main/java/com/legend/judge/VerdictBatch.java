// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.judge;

import com.legend.exec.Census;
import com.legend.exec.ExecutionTrace;
import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.exec.PrepTrace;
import com.legend.exec.Sessions;
import com.legend.executionplan.ExecutionPlan;

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
 * a decided outcome) decide in their order. THE SPLIT RUNG: when the fused
 * statement itself errors (a side the database rejects), the batch is
 * judged statement by statement — today's path, so the error lands on the
 * assert that owns it — counted, never a verdict change. This class sends
 * and orders; the SQL shape is the lowering's, the judgment of a row is
 * the verdict arm's ({@link Judge}), and each statement is sent as a PLAN
 * by the {@link Runner} (the plan path program's step 6, 2026-10-18: a
 * verdict statement is a typed-rows plan whose rows the runner decodes;
 * this class holds no dialect, no shape and no executor). */
public final class VerdictBatch {

    /** The lowering's fused statement over the deferred statements, with
     * the frame definitions the sides reference at its head. */
    @FunctionalInterface
    public interface Fusion {
        SqlQuery fuse(List<SqlQuery> statements, Map<String, SqlQuery> frames);
    }

    /** The verdict arm's reading of one row ({@code verdict, expected,
     * actual, unjudged, lenient, holds, message}): returns where the row
     * holds, raises the row's own message otherwise (the test-is-the-product
     * plan's M8b: the polarity and the message are the statement's). */
    @FunctionalInterface
    public interface Judge {
        void judge(String name, List<Object> row);
    }

    /** The lowering's ONE row over a generic verdict row and its ruled row (2026-11-06,
     *  {@code VerdictSql.either}): the generic row's columns where it holds, else the ruled row's. */
    @FunctionalInterface
    public interface Either {
        SqlQuery either(SqlQuery generic, SqlQuery ruled);
    }

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
     *  through the plan runner ({@link PlanRunner#program}); a wrapper the dialect cannot spell raises here. */
    public interface Runner {
        /** A typed result, or a keyed one where the statement reads a raw grid's column names or cells (S3c), its
         *  target the database of the session {@code on} it runs on (the body's, or the graph's system database beside
         *  it: per-statement routing, 2026-11-03 -- until then a verdict over the system store on a server engine was
         *  planned against the body's target, so the database judge left such a body to the walk). */
        ExecutionPlan.Node plan(SqlQuery statement, boolean fused, Sessions.Session on);

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
        ExecutionPlan.Verdicts.@com.legend.base.Nullable Savepoint savepoint();

        /** The rules the batch's verdicts are judged by (the run's, stamped on every group it plans: the explicit
         *  rule, 2026-11-04) -- the database judge planned their statements under them. */
        ExecutionPlan.JudgingRules rules();
    }

    /** The arm a verdict's name spells: the assert function's simple name (the name is its callee's FQN, or the
     *  arm's own word where there is none). */
    static String armOf(String name) {
        int i = name.lastIndexOf("::");
        String simple = i < 0 ? name : name.substring(i + 2);
        int u = simple.indexOf('_');
        return u > 0 ? simple.substring(0, u) : simple;
    }

    /** A program sink for a verdict group alone: no answer, no host sides (a verdict group answers no rows and the
     *  host judge is not asked here); the rows and the fused outcome are the batch's to take. */
    private abstract static class VerdictSink implements PlanRunner.ProgramSink {
        @Override
        public void columns(List<ExecutionPlan.Column> columns) {
            throw new IllegalStateException("a verdict group answers no rows");
        }

        @Override
        public void batch(com.legend.result.Batch batch) {
            throw new IllegalStateException("a verdict group answers no rows");
        }

        @Override
        public void end() {
            throw new IllegalStateException("a verdict group answers no rows");
        }

        @Override
        public void sides(ExecutionPlan.Verdict verdict, List<PlanRunner.Rows> sides) {
            throw new IllegalStateException("the host judge is not asked through the batch");
        }
    }

    /** A deferred verdict statement, or an outcome the arm decided
     * without one (a static kind gate, an UNJUDGED decline, a side that
     * errored while planning). */
    private sealed interface Step permits Pending, Resolved {
    }

    /** {@code orElse}: the row's RULED comparison (2026-11-04, the core program: every assert on its own merit
     *  generically first), as {@link #defer} records it -- a ruled row, which {@link #plan} makes part of the generic
     *  row's own statement (2026-11-06, never a statement of its own sent after a failure) -- or, once planned, what
     *  decided it at planning (the dialect's refusal of the ruled row, a failure its planning raised), raised where the
     *  generic row fails. */
    private record Pending(int ix, String name, SqlQuery query,
            Sessions.Session on, @com.legend.base.Nullable Appeal appeal, @com.legend.base.Nullable Step orElse)
            implements Step {
    }

    /** A verdict row's APPEAL (block-compiler rung 2a): run at the flush when the
     * row FAILED — returns on a pass by another judgment (the SQL-text referee's
     * rows), raises its own failure otherwise. Never consulted on a held row. */
    public interface Appeal {
        /** The failed {@code row} ({@code VerdictSql.schema}'s columns) decided: a pass returns, a failure raises. */
        TypedSide appeal(List<Object> row);
    }

    /** A root resolved before any row: a static decision, an unjudged raise, a planned raise ({@code reported}: the
     *  listener hears its verdict), or a WALL the arm left through ({@code reported} false: the walk discards the root
     *  and surfaces the wall after the roots before it; the program road raises it at its turn the same way). */
    private record Resolved(RuntimeException failure, boolean reported) implements Step {
    }

    private static final class Root {
        final String listenerName;
        final List<Step> steps = new ArrayList<>();
        /** The caller's message where it READS ({@link #message}): its row, sent only where the root fails. */
        @com.legend.base.Nullable SqlQuery message;
        Sessions.@com.legend.base.Nullable Session messageOn;

        Root(String listenerName) {
            this.listenerName = listenerName;
        }
    }

    /** CENSUS (printed by the corpus lanes): fused statements sent, and the
     * batches that fell to the split rung. */

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
    private final SelfContained selfContained;
    private final Judge judge;
    private final Runner runner;
    private final List<Root> roots = new ArrayList<>();
    private @com.legend.base.Nullable Root current;
    /** The frames the body's asserts read by reference: name → the frame's
     * plan (leg 3.4 step 2), defined once per body. */
    private final Map<String, SqlQuery> frames = new LinkedHashMap<>();

    /** The body's execution trace: its fused statement publishes its stamp there. */
    private final ExecutionTrace trace;

    /** The lowering's one row over a generic verdict row and its ruled row. */
    private final Either either;

    public VerdictBatch(Fusion fusion, Either either, SelfContained selfContained, Judge judge, Runner runner,
            ExecutionTrace trace) {
        this.fusion = fusion;
        this.either = either;
        this.selfContained = selfContained;
        this.judge = judge;
        this.runner = runner;
        this.trace = trace;
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
    public void message(SqlQuery row, Sessions.Session on) {
        Root r = java.util.Objects.requireNonNull(current, "verdict batch: no open root");
        r.message = row;
        r.messageOn = on;
    }

    public void defer(String name, boolean wantEqual, SqlQuery query, Sessions.Session on) {
        defer(name, wantEqual, query, on, null);
    }

    public void defer(String name, boolean wantEqual, SqlQuery query, Sessions.Session on,
            @com.legend.base.Nullable Appeal appeal) {
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
        SqlQuery row = selfContained.wrap(query, name, wantEqual);
        Pending of = orElseOf.peek();
        if (of != null) {
            // the verdict of the root's last row's ruled comparison (orElse): attached to that row, never a step of its
            // own; plan() makes the two one row
            int at = r.steps.indexOf(of);
            if (at < 0 || of.orElse() != null || appeal != null) {
                throw new IllegalStateException("verdict batch: a fallback for no row of the open root: " + name);
            }
            Pending withFallback = new Pending(of.ix(), of.name(), of.query(), of.on(), of.appeal(),
                    new Pending(-1, name, row, on, null, null));
            r.steps.set(at, withFallback);
            orElseOf.pop();
            orElseOf.push(withFallback);
            return;
        }
        r.steps.add(new Pending(ix, name, row, on, appeal, null));
    }

    /** The batch's one root as ONE statement (the user's ruling S135, 2026-11-05: an assert inside assertError is a
     *  statement that raises its own failure): its one verdict row, self-contained, with the batch's frames at its head
     *  and the session it reads; or the failure it was decided with at planning; null where the batch holds no root,
     *  several, a root of several steps, a fallback, an appeal or a caller's message that reads (no one row decides
     *  it alone). */
    public @com.legend.base.Nullable Sole sole() {
        if (roots.size() != 1 || roots.get(0).steps.size() != 1 || roots.get(0).message != null) {
            return null;
        }
        Step step = roots.get(0).steps.get(0);
        if (step instanceof Pending p && p.appeal() == null && p.orElse() == null) {
            return new Sole.Row(com.legend.sql.FrameCtes.attach(p.query(), frames()), p.on());
        }
        if (step instanceof Resolved r && r.reported() && r.failure() instanceof com.legend.error.AssertFailed f
                && f.unjudgedReason() == null) {
            return new Sole.Decided(f);
        }
        return null;
    }

    /** {@link #sole}'s answer: a verdict row the database computes, or a failure decided at planning. */
    public sealed interface Sole permits Sole.Row, Sole.Decided {
        /** The verdict row ({@code VerdictSql.schema}'s columns) and the session it reads. */
        record Row(SqlQuery query, Sessions.Session on) implements Sole {
        }

        /** The assert's failure, decided at planning. */
        record Decided(com.legend.error.AssertFailed failure) implements Sole {
        }
    }

    /** The row whose ruled comparison {@link #orElse}'s arm is planning, while it does: pushed and popped by it. */
    private final java.util.ArrayDeque<Pending> orElseOf = new java.util.ArrayDeque<>(1);

    /** {@code arm} planned as the RULED comparison of the open root's last row (2026-11-04, the core program): the
     *  verdict it defers -- planned with that row as ONE row ({@link #plan}, 2026-11-06) -- or the failure it decides
     *  at planning decides the root where the generic row does not hold: a rendered text's two strings, then its
     *  rows (compatibility mode's S131 and S132). */
    public void orElse(Runnable arm) {
        Root r = java.util.Objects.requireNonNull(current, "verdict batch: no open root");
        if (r.steps.isEmpty() || !(r.steps.get(r.steps.size() - 1) instanceof Pending last)) {
            throw new IllegalStateException("verdict batch: a fallback with no row before it");
        }
        orElseOf.push(last);
        try {
            arm.run();
        } catch (com.legend.error.AssertFailed | com.legend.error.DataError e) {
            Pending p = java.util.Objects.requireNonNull(orElseOf.peek());
            r.steps.set(r.steps.indexOf(p), new Pending(p.ix(), p.name(), p.query(), p.on(), p.appeal(),
                    new Resolved(e, true)));
        } finally {
            orElseOf.pop();
        }
    }

    /** The verdict rows deferred so far (the next row's index). */
    public int pendingCount() {
        int ix = 0;
        for (Root x : roots) {
            for (Step s : x.steps) {
                if (s instanceof Pending) {
                    ix++;
                }
            }
        }
        return ix;
    }

    /** THE FRAGMENT MAP (block-compiler stage 2, 2026-09-21): every frame name and
     * verdict branch index → the let / assert it came from, so a database error
     * on the fused statement names the statement that owns it. */
    private final Map<String, String> fragments = new java.util.LinkedHashMap<>();

    public void fragments(Map<String, String> map) {
        fragments.putAll(map);
    }

    /** The fragments a database message names (a frame alias appears in DuckDB's
     * binder errors and H2's column errors), or an empty string. */
    public String attribute(String message) {
        StringBuilder sb = new StringBuilder();
        for (var e : fragments.entrySet()) {
            if (e.getKey().startsWith("frame_") && message.contains(e.getKey())) {
                sb.append(sb.length() == 0 ? " — near " : ", ").append(e.getValue());
            }
        }
        return sb.toString();
    }

    public void resolve(RuntimeException failure) {
        java.util.Objects.requireNonNull(current, "verdict batch: no open root")
                .steps.add(new Resolved(failure, true));
    }

    /** The open root left through a WALL (not a verdict, not a data error): raised at its turn after the roots before
     *  it are judged, told to no listener -- as the walk's {@code batched} discards it and surfaces it after the flush
     *  (the test-is-the-product plan's M7: the program road plans the raise at the root's place; its exit is counted
     *  where it was planned, as the walk counts it at the arm). */
    public void wall(RuntimeException failure) {
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
     *  (each verdict its keyed wrapper, no fused statement; a rejection leaves its rows absent). */
    public record Group(ExecutionPlan.Verdicts step, Sessions.Session on, boolean keyed) {
    }

    /**
     * THE BATCH PLANNED (the test-is-the-product plan's M7): the deferred verdicts as verdict groups -- one fused
     * statement per session over the batch's frames (the dialect's split rung at planning, member by member:
     * {@link #spelled}), the keyed members a group of their own -- and the judgment over the rows the groups answer
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
        // ONE verdict row for a generic row and its ruled row, never a statement sent after another (2026-11-06,
        // the user: "Remove fallback"; compatibility mode's rendered text, S131/S132): what is sent never depends on
        // whether the generic comparison happened to hold -- a union's row order decided that, run to run, while the
        // ruled row was a statement of its own sent after a failed text. Planned once, here, by what the dialect
        // spells:
        //  - both rows: ONE row, the generic row's columns where it holds, else the ruled row's (VerdictSql.either);
        //  - the ruled row alone (the dialect cannot spell the generic one): the ruled row decides -- measured
        //    2026-11-06 on the DuckDB and H2 corpora and PCT channel B on DuckDB, H2 and SQLite: H2's rendered text
        //    over STRING_AGG, specialUnion::testGroupBy and association::mixed::testAssociationMixed, two asserts;
        //  - the generic row alone (the dialect cannot spell the ruled one): the generic row decides, and where it fails
        //    the assert fails with the dialect's refusal of the ruled comparison, by name, nothing sent for it (measured:
        //    no such assert on those lanes);
        //  - neither: the generic row's own refusal at its place (UNJUDGED, by name), as any member the dialect refuses.
        for (Root r : batch) {
            for (int i = 0; i < r.steps.size(); i++) {
                if (!(r.steps.get(i) instanceof Pending p && p.orElse() instanceof Pending f)) {
                    continue;
                }
                if (p.appeal() != null || f.orElse() != null || f.appeal() != null) {
                    throw new IllegalStateException("verdict batch: a ruled row in a shape no rule plans as one row: "
                            + p.name());
                }
                if (f.on() != p.on()) {
                    // the texts on one session, the rows they render on another (a side routed to the graph's system
                    // database): no one statement holds both, and two would be the fallback ruled out -- this assert
                    // UNJUDGED by name, the batch's other asserts judged (the final audit, 2026-11-06)
                    r.steps.set(i, new Resolved(unjudged(p.name(), "a rendered text and its rows on two sessions"),
                            true));
                    continue;
                }
                com.legend.sql.dialect.DialectCapability genericRefused = planAhead(p.name(), p.query(), p.on()).refused();
                com.legend.sql.dialect.DialectCapability ruledRefused = planAhead(f.name(), f.query(), p.on()).refused();
                Pending one;
                if (genericRefused == null && ruledRefused == null) {
                    SqlQuery both = either.either(p.query(), f.query());
                    com.legend.sql.dialect.DialectCapability bothRefused = planAhead(p.name(), both, p.on()).refused();
                    if (bothRefused != null) {
                        // each row spelled alone, the two together not: a composition bug of lite's, never a choice
                        throw new IllegalStateException("verdict batch: " + p.name() + "'s generic and ruled rows are"
                                + " each spelled but not as one row: " + bothRefused.getMessage(), bothRefused);
                    }
                    one = new Pending(p.ix(), p.name(), both, p.on(), null, null);
                } else if (genericRefused != null && ruledRefused == null) {
                    one = new Pending(p.ix(), p.name(), f.query(), p.on(), null, null);
                } else if (genericRefused == null) {
                    one = new Pending(p.ix(), p.name(), p.query(), p.on(), null,
                            new Resolved(java.util.Objects.requireNonNull(ruledRefused), true));
                } else {
                    one = new Pending(p.ix(), p.name(), p.query(), p.on(), null, null);
                }
                r.steps.set(i, one);
            }
        }
        Map<Sessions.Session, List<Pending>> groups = new LinkedHashMap<>();
        for (Root r : batch) {
            for (Step s : r.steps) {
                if (s instanceof Pending p) {
                    groups.computeIfAbsent(p.on(), k -> new ArrayList<>()).add(p);
                }
            }
        }
        List<Group> out = new ArrayList<>();
        Map<ExecutionPlan.Verdict, Pending> members = new java.util.IdentityHashMap<>();
        // the members the dialect cannot spell (the split rung at planning): each refused at its place, nothing sent
        // for it (2026-11-04, the core program)
        java.util.Set<Pending> unspelled = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        for (var g : groups.entrySet()) {
            List<Pending> all = g.getValue();
            if (PrepTrace.on()) {
                for (Pending p : all) {
                    PrepTrace.branch(p.name(), runner.chars(p.query()));
                }
            }
            // the group as a plan value (S2e): the fused statement its wrapper over wrappers, each verdict a step with
            // no wrapper of its own here -- a single wrapper is planned and run lazily in the root loop below, at the
            // place the split rung reaches it, so nothing is sent past the first failure
            // a member whose statement reads a raw grid's report is keyed: its wrapper's keys are its grid's columns,
            // which no fused statement shares, so the keyed members are a group of their own, each its wrapper
            // (a member is keyed by its statement with the batch's frames, as it is planned: a frame may read a report)
            Group keyedGroup = keyedGroup(all.stream().filter(this::keyed).toList(), g.getKey(), members);
            if (keyedGroup != null) {
                out.add(keyedGroup);
            }
            List<Pending> ps = all.stream().filter(p -> !keyed(p)).toList();
            if (ps.isEmpty()) {
                continue;
            }
            ExecutionPlan.TdsResult fusedPlan;
            try {
                fusedPlan = typed(runner.plan(fusion.fuse(ps.stream().map(Pending::query).toList(), frames), true,
                        g.getKey()));
            } catch (com.legend.sql.dialect.DialectCapability e) {
                // the split rung at planning, member by member (the planner-driver seam program's S3b, 2026-10-30): a
                // verdict whose canon the dialect cannot spell leaves the group and is refused at its own place in the
                // root loop (executeOne: UNJUDGED, its own words, nothing sent); the members the dialect spells stay
                // fused. Until S3b the wire probe's prepare rendered a wire-decided side at its planning, which refused
                // such a side before it joined the group -- by accident, and only for those sides
                List<Pending> unfused = ps;
                ps = spelled(unfused);
                for (Pending u : unfused) {
                    if (!ps.contains(u)) {
                        unspelled.add(u);
                    }
                }
                fusedPlan = null;
                if (!ps.isEmpty() && ps.size() < unfused.size()) {
                    try {
                        fusedPlan = typed(runner.plan(fusion.fuse(ps.stream().map(Pending::query).toList(), frames),
                                true, g.getKey()));
                    } catch (com.legend.sql.dialect.DialectCapability again) {
                        fusedPlan = null;
                    }
                }
                if (fusedPlan == null) {
                    // the split rung at planning: a canon the dialect cannot spell; counted as the run-time one is
                    fallback(e);
                    continue;
                }
            }
            for (Map.Entry<List<Pending>, ExecutionPlan.TdsResult> part : withinBudget(ps, fusedPlan, g.getKey())) {
                List<ExecutionPlan.Verdict> verdicts = new ArrayList<>(part.getKey().size());
                for (Pending p : part.getKey()) {
                    ExecutionPlan.Verdict v = new ExecutionPlan.Verdict(p.name(), armOf(p.name()), List.of(), null);
                    verdicts.add(v);
                    members.put(v, p);
                }
                out.add(new Group(new ExecutionPlan.Verdicts(verdicts, part.getValue(), runner.savepoint(),
                        runner.rules()), g.getKey(), false));
            }
        }
        // THE RUNTIME WALL (the deep audit, 2026-11-05): every statement the root loop may send after the plan's
        // rows are read -- a member's own statement (the split rung), a caller's message -- is
        // planned HERE, with the fused statements; the root loop only runs what was planned, never plans
        Map<Object, Ahead> ahead = new java.util.IdentityHashMap<>();
        for (Root r : batch) {
            if (r.message != null) {
                ahead.put(r, planAhead(r.listenerName, r.message, java.util.Objects.requireNonNull(r.messageOn)));
            }
            for (Step st : r.steps) {
                for (Step at = st; at instanceof Pending p; at = p.orElse()) {
                    ahead.put(p, planAhead(p.name(), p.query(), p.on()));
                }
            }
        }
        return new Planned(batch, out, members, unspelled, ahead);
    }

    /** A statement planned ahead of the run (the runtime wall): its plan, or the dialect's refusal of it, raised where
     *  the root loop reaches it. */
    private record Ahead(ExecutionPlan.@com.legend.base.Nullable Node plan,
            com.legend.sql.dialect.@com.legend.base.Nullable DialectCapability refused) {
    }

    /** An assert the database judge cannot judge, named {@code why} at planning: counted UNJUDGED, the assert's own
     *  failure, which the root loop reports to the listener at its place. */
    private static com.legend.error.AssertFailed unjudged(String name, String why) {
        CanonicalDivergence.sqlUnjudged(name, why);
        return com.legend.error.AssertFailed.unjudged(why, name + ": UNJUDGED in database mode -- " + why);
    }

    private Ahead planAhead(String name, SqlQuery query, Sessions.Session on) {
        try {
            return new Ahead(runner.plan(com.legend.sql.FrameCtes.attach(query, frames), false, on), null);
        } catch (com.legend.sql.dialect.DialectCapability e) {
            return new Ahead(null, e);
        }
    }

    /** The planned batch: its groups, and the judgment over their rows. */
    public final class Planned {
        private final List<Root> batch;
        private final List<Group> groups;
        private final Map<ExecutionPlan.Verdict, Pending> members;
        private final java.util.Set<Pending> unspelled;
        private final Map<Integer, List<Object>> rows = new HashMap<>();
        private boolean counted;
        /** The first fused statement the database rejected, or null: the split rung's tripwire -- a rejection no
         *  assert reproduces alone is a composition bug, never a pass (2026-11-05: after a verdict the harness may
         *  only explain a failure). */
        private com.legend.error.@com.legend.base.Nullable DataError rejected;

        private final Map<Object, Ahead> ahead;

        private Planned(List<Root> batch, List<Group> groups, Map<ExecutionPlan.Verdict, Pending> members,
                java.util.Set<Pending> unspelled, Map<Object, Ahead> ahead) {
            this.batch = batch;
            this.groups = groups;
            this.members = members;
            this.unspelled = unspelled;
            this.ahead = ahead;
        }

        /** The statement planned ahead for {@code key} (a pending step, or a root's message), read now: through the
         *  running program's reader where the root loop runs inside its run -- the same run, no second one (THE WALL,
         *  2026-11-06: a run started inside a run is refused) -- else run on {@code on}, after the batch's runs. */
        private List<Object> runAhead(Object key, String name, Sessions.Session on,
                PlanRunner.@com.legend.base.Nullable SideReader reader) {
            Ahead a = java.util.Objects.requireNonNull(ahead.get(key), "a statement the batch did not plan ahead");
            com.legend.sql.dialect.DialectCapability refused = a.refused();
            if (refused != null) {
                // counted UNJUDGED, raised as itself (as orElseDecides)
                CanonicalDivergence.sqlUnjudged(name, "dialect-capability: "
                        + String.valueOf(refused.getMessage()).split("\n")[0]);
                throw refused;
            }
            ExecutionPlan.Node plan = java.util.Objects.requireNonNull(a.plan());
            return reader != null ? readPlanned(name, plan, reader) : runPlanned(name, plan, on, runner);
        }

        /** The flush counted once, as the batch's groups start to run (the walk's flush before its first group; the
         *  program road's first row or fused word): a batch planned and then left for the walk counts nothing. */
        void counted() {
            if (!counted) {
                counted = true;
                Census.inc(Census.Key.VERDICT_FLUSHES);
            }
        }

        /** The verdict groups, in the order they run: a session's keyed group, then its fused group, per session. */
        public List<Group> groups() {
            return java.util.Collections.unmodifiableList(groups);
        }

        /** The frames the fused statements define at their head (the readers' CTEs), by frame name. */
        public Map<String, SqlQuery> frames() {
            return VerdictBatch.this.frames();
        }

        /** The runner's word on a group's fused statement: sent, or rejected (the split rung; counted here, the
         *  wrappers run by the root loop where a row is absent). */
        public void fused(ExecutionPlan.Verdicts group, com.legend.error.@com.legend.base.Nullable DataError rejected) {
            counted();
            if (rejected == null) {
                Census.inc(Census.Key.VERDICT_FUSED);
            } else {
                if (this.rejected == null) {
                    this.rejected = rejected;
                }
                fallback(rejected);
            }
        }

        /** The row the runner read for {@code verdict} (null where none: the fused statement rejected, a keyed
         *  wrapper unplanned or rejected); the root loop reaches an absent row at its place. */
        public void received(ExecutionPlan.Verdict verdict, @com.legend.base.Nullable List<Object> row) {
            counted();
            Pending p = members.get(verdict);
            if (p == null) {
                throw new IllegalStateException("a verdict row for no member of this batch: " + verdict.name());
            }
            if (row != null) {
                rows.put(p.ix(), row);
            }
        }

        /** {@code p}'s ruled comparison as decided at planning, raised where the generic row fails: the dialect's
         *  refusal of it (counted UNJUDGED), or a failure its planning raised. Nothing is sent. */
        private void orElseDecides(Pending p) {
            if (!(p.orElse() instanceof Resolved decided)) {
                // a ruled row is planned as part of its generic row's own statement (plan(), 2026-11-06): never a
                // statement of its own sent after a failure
                throw new IllegalStateException("verdict batch: a ruled row left as a statement of its own: " + p.name());
            }
            if (decided.failure() instanceof com.legend.sql.dialect.DialectCapability refused) {
                // counted UNJUDGED (the census), raised as the dialect's own refusal: a dialect wall is raised as
                // itself on both roads (BodyRouterParityTest's quantified programs), never turned into a verdict
                CanonicalDivergence.sqlUnjudged(p.name(), "dialect-capability: "
                        + String.valueOf(refused.getMessage()).split("\n")[0]);
            }
            throw decided.failure();
        }

        /** Every root judged in body order: a pending step's row (read, or run now at its place where absent: the
         *  split rung, lazily, nothing sent past the first failure), a resolved step's failure; each root's verdict to
         *  the listener; the first failure raises. */
        public void judge(@com.legend.base.Nullable AssertListener l) {
            judge(l, null);
        }

        /** {@link #judge(AssertListener)} inside a program's run: a row planned ahead (the split rung's, a caller's
         *  message) read through the program's {@code reader}, never a run of its own. */
        public void judge(@com.legend.base.Nullable AssertListener l,
                PlanRunner.@com.legend.base.Nullable SideReader reader) {
            counted();
            for (Root r : batch) {
                RuntimeException failure = null;
                boolean reported = true;
                if (r.steps.stream().noneMatch(s -> s instanceof Pending)) {
                    Census.inc(Census.Key.VERDICT_HOST_DECIDED);
                }
                for (Step s : r.steps) {
                    try {
                        if (s instanceof Pending p) {
                            List<Object> row = rows.get(p.ix());
                            if (row == null && p.orElse() != null && unspelled.contains(p)) {
                                // a generic row the dialect cannot write, whose ruled row was decided at planning:
                                // that decision raises, nothing sent
                                orElseDecides(p);
                                continue;
                            }
                            if (row == null) {
                                com.legend.diagnostics.Diagnostics.judgeSend("split-rung", p.name());
                                row = runAhead(p, p.name(), p.on(), reader);
                            }
                            try {
                                judge.judge(p.name(), row);
                            } catch (com.legend.error.AssertFailed e) {
                                if (p.orElse() != null) {
                                    // the generic row failed, its ruled comparison decided at planning (the dialect's
                                    // refusal of it, or a failure its planning raised): that decision raises
                                    orElseDecides(p);
                                    continue;
                                }
                                if (p.appeal() == null) {
                                    throw e;
                                }
                                // rung 2a: the row failed — the appeal decides (returns
                                // on a pass by rows, raises its own failure otherwise)
                                com.legend.diagnostics.Diagnostics.judgeSend("appeal", p.name());
                                p.appeal().appeal(row);
                            }
                        } else if (s instanceof Resolved rv) {
                            reported = rv.reported();
                            throw rv.failure();
                        }
                    } catch (com.legend.error.AssertFailed | com.legend.error.DataError e) {
                        failure = e;
                        break;
                    }
                }
                if (failure == null) {
                    if (l != null) {
                        l.verdict(r.listenerName, true, null);
                    }
                    continue;
                }
                if (r.message != null && failure instanceof com.legend.error.AssertFailed af
                        && af.unjudgedReason() == null) {
                    failure = messaged(r, this, reader);
                }
                CanonicalDivergence.sqlRaised();
                if (l != null && reported) {
                    if (failure instanceof com.legend.error.AssertFailed af && af.unjudgedReason() != null) {
                        l.unjudged(r.listenerName, af.unjudgedReason());
                    }
                    l.verdict(r.listenerName, false, failure.getMessage());
                }
                throw failure;   // first-failure sequencing, as before
            }
            com.legend.error.DataError r = rejected;
            if (r != null) {
                // every assert held alone where the fused statement was rejected: the split rung explained nothing, so
                // the rejection is the fusion's own -- a lite error, never a pass
                throw new IllegalStateException("the fused verdict statement was rejected and no assert reproduced"
                        + " the rejection alone (a composition bug in the fused statement): "
                        + String.valueOf(r.getMessage()).split("\n")[0], r);
            }
        }
    }

    /** The failure of {@code r}, a root that did not hold, with its caller's message: the message's row sent now, at
     *  the root's place, its value the failure's text -- or the error reading it raised. */
    private RuntimeException messaged(Root r, Planned planned, PlanRunner.@com.legend.base.Nullable SideReader reader) {
        com.legend.diagnostics.Diagnostics.judgeSend("message", r.listenerName);
        try {
            List<Object> row = planned.runAhead(r, r.listenerName, java.util.Objects.requireNonNull(r.messageOn), reader);
            return new com.legend.error.AssertFailed(String.valueOf(row.get(6)));
        } catch (com.legend.error.AssertFailed | com.legend.error.DataError e) {
            return e;
        }
    }

    /** Whether {@code p}'s statement, with the batch's frames as it is planned, reads a raw grid's report. */
    private boolean keyed(Pending p) {
        return runner.keyed(com.legend.sql.FrameCtes.attach(p.query(), frames));
    }

    /** The keyed members of a session's group as a verdict group of their own, each verdict its keyed wrapper (the
     *  planner-driver seam program's S3c, 2026-10-31): the body's statements, run with the fused group's. A wrapper
     *  the dialect cannot spell leaves its member out: the root loop reaches it at its place (executeOne), where its
     *  refusal is its own. Null where no member is keyed (or none is spelled). */
    private @com.legend.base.Nullable Group keyedGroup(List<Pending> keyed, Sessions.Session on,
            Map<ExecutionPlan.Verdict, Pending> members) {
        List<ExecutionPlan.Verdict> verdicts = new ArrayList<>(keyed.size());
        for (Pending p : keyed) {
            try {
                ExecutionPlan.Verdict v = new ExecutionPlan.Verdict(p.name(), armOf(p.name()), List.of(),
                        runner.plan(com.legend.sql.FrameCtes.attach(p.query(), frames), false, on));
                verdicts.add(v);
                members.put(v, p);
            } catch (com.legend.sql.dialect.DialectCapability e) {
                // refused at its own place in the root loop
            }
        }
        return verdicts.isEmpty() ? null : new Group(new ExecutionPlan.Verdicts(verdicts, null, runner.rules()), on,
                true);
    }

    /** A fused statement's plan: one typed result (a member that reads a raw grid's report never joins one). */
    /** {@code ps}'s fused statement {@code fused}, or -- where its text passes the database's budget
     *  ({@link Runner#fusedBudget}) -- {@code ps} halved, each half fused and halved again until it fits or is one
     *  member: the same members in their order, each part a fused group of its own, all planned here (2026-11-05, PCT
     *  round 22: ClickHouse's analyzer over 50 asserts of nested instances, 1.3M characters). */
    private List<Map.Entry<List<Pending>, ExecutionPlan.TdsResult>> withinBudget(List<Pending> ps,
            ExecutionPlan.TdsResult fused, Sessions.Session on) {
        if (ps.size() == 1 || fused.sql().statement().length() <= runner.fusedBudget()) {
            return List.of(Map.entry(ps, fused));
        }
        List<Map.Entry<List<Pending>, ExecutionPlan.TdsResult>> out = new ArrayList<>();
        int half = ps.size() / 2;
        for (List<Pending> part : List.of(ps.subList(0, half), ps.subList(half, ps.size()))) {
            out.addAll(withinBudget(part, typed(runner.plan(fusion.fuse(part.stream().map(Pending::query).toList(),
                    frames), true, on)), on));
        }
        return out;
    }

    private static ExecutionPlan.TdsResult typed(ExecutionPlan.Node plan) {
        if (plan instanceof ExecutionPlan.TdsResult t) {
            return t;
        }
        throw new IllegalStateException("a fused verdict statement planned as " + plan.getClass().getSimpleName()
                + ": a keyed member is judged at its place");
    }

    /** The members of a group whose own canon the dialect spells, each fused alone over the batch's frames (planning
     *  only: a render, nothing sent). */
    private List<Pending> spelled(List<Pending> group) {
        List<Pending> out = new ArrayList<>(group.size());
        for (Pending p : group) {
            try {
                runner.plan(fusion.fuse(List.of(p.query()), frames), true, p.on());
                out.add(p);
            } catch (com.legend.sql.dialect.DialectCapability e) {
                // refused at its own place in the root loop
            }
        }
        return out;
    }

    /** The split rung's accounting: counted, with the reason's head and the frames the message names. */
    private void fallback(RuntimeException e) {
        Census.inc(Census.Key.VERDICT_FALLBACKS);
        String m = String.valueOf(e.getMessage());
        Census.FALLBACK_REASONS.add(e.getClass().getSimpleName() + ": "
                + m.substring(0, Math.min(m.length(), 160)).replace('\n', ' ')
                + attribute(m));
    }

    /** ONE verdict statement already planned ({@code wrapper}) read through a running program's {@code reader} (the
     *  root loop inside the program road's run: the same run reads it, THE WALL, 2026-11-06), its one row; a database
     *  error is the database's verdict, as {@link #runPlanned}'s. */
    private static List<Object> readPlanned(String name, ExecutionPlan.Node wrapper, PlanRunner.SideReader reader) {
        ExecutionPlan.Side side = switch (wrapper) {
            case ExecutionPlan.TdsResult t -> new ExecutionPlan.Side.Rows(t);
            case ExecutionPlan.Keyed k -> new ExecutionPlan.Side.KeyedRows(k);
            default -> throw new IllegalStateException("a verdict's wrapper is a typed or keyed result");
        };
        PlanRunner.Rows rows;
        try (var __o = com.legend.exec.StatementOrigin.enter(com.legend.exec.StatementOrigin.FALLBACK)) {
            rows = reader.read(side);
        } catch (com.legend.error.DataError e) {
            CanonicalDivergence.sqlJudgedInDatabase(name);
            throw e;
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        if (rows.rows().size() != 1) {
            throw new IllegalStateException(name + ": the verdict statement returned " + rows.rows().size() + " rows");
        }
        return rows.rows().get(0);
    }

    /** Run ONE verdict statement already planned ({@code wrapper}) on {@code on} and return its one row: the root
     *  loop's form, which plans nothing while the program runs (the runtime wall, 2026-11-05). */
    static List<Object> runPlanned(String name, ExecutionPlan.Node wrapper, Sessions.Session on, Runner runner) {
        ExecutionPlan.Verdicts group = new ExecutionPlan.Verdicts(List.of(
                new ExecutionPlan.Verdict(name, armOf(name), List.of(), wrapper)), null, runner.rules());
        List<List<Object>> rows = new ArrayList<>(1);
        try (var __o = com.legend.exec.StatementOrigin.enter(com.legend.exec.StatementOrigin.FALLBACK)) {
            PlanRunner.program(new ExecutionPlan(List.of(), group), Map.of(), PlanSessions.of(on, null),
                    PlanRunner.Judging.DATABASE, new VerdictSink() {
                        @Override
                        public void fused(ExecutionPlan.Verdicts g, com.legend.error.@com.legend.base.Nullable DataError rejected) {
                            throw new IllegalStateException("a group of one wrapper has no fused statement");
                        }

                        @Override
                        public void verdict(ExecutionPlan.Verdict verdict, @com.legend.base.Nullable List<Object> row) {
                            rows.add(row);
                        }
                    });
        } catch (com.legend.error.DataError e) {
            // the database raised evaluating the test's own values (a date past its range, a value it refuses): it
            // judged, and the verdict is that error -- the test's failure, as the host judge's side read raises it;
            // never UNJUDGED (2026-11-03: channel B's BigNumber date tests counted 4 "unjudged" under the database
            // judge alone). A statement lite composed wrongly would fail a test its lane's exact ledger does not list.
            CanonicalDivergence.sqlJudgedInDatabase(name);
            throw e;
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        if (rows.size() != 1 || rows.get(0) == null) {
            throw new IllegalStateException(name + ": the verdict statement returned " + rows.size() + " rows");
        }
        return rows.get(0);
    }
}
