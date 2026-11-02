// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.driver;

import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.StatementEffects;
import com.legend.compiler.spec.typed.Lets;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.executionplan.ExecutionPlan;
import com.legend.judge.AssertListener;
import com.legend.judge.VerdictBatch;
import com.legend.planner.LetPlanner;
import com.legend.planner.PlanAllocations;
import com.legend.planner.ProgramFrames;
import com.legend.planner.QueryDoor;
import com.legend.testplan.TestDoor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE DATABASE JUDGE'S ASSERTS PLANNED BEFORE THE PROGRAM (the test-is-the-product plan's M7, the twin of
 * {@link HostAsserts}): the body's statement-root asserts run through the database arms under a {@link VerdictBatch}
 * that COLLECTS -- each arm's verdict statement deferred into its root, a decided outcome or a raise a resolved step,
 * nothing sent -- exactly as the walk collects them ({@code AssertVerdicts.batched}); at each SEGMENT's close (an
 * effect statement, the body's end: where the walk flushes, {@code BodyCompiler.Segments.closeVerdicts}) the batch is
 * PLANNED ({@link VerdictBatch#plan}): its fused statement over the segment's asserts with the frames' CTEs at its
 * head, one verdict group per session, placed by the test door at the segment's last assert
 * ({@link TestDoor.Asserts#at}: the asserts before it absorbed). A frame let is built as the walk builds it under the
 * batch -- not run, its CTE defined when a reader splices it -- its activity comment the fused statement's stamp, a
 * parameter the fused step binds (the walk's {@code ExecutionTrace.reserve}). The router's sink hands each group's
 * rows back ({@link Segment}) and judges at the segment's last group with the batch's own root loop
 * ({@link VerdictBatch.Planned#judge}: the walk's census and listener protocol, the split rung's wrapper run lazily at
 * its place). Null where the walk keeps the body: an assert of a shape the database arms answer null for, a group on
 * another session than the body's (the system database's: M10a), a keyed member (a raw grid's report), a frame whose
 * activity comment a later segment reads.
 */
final class DatabaseAsserts implements TestDoor.Asserts {

    /** One segment of the body: its planned batch and its groups' steps, in run order. */
    record Segment(VerdictBatch.Planned planned, List<ExecutionPlan.Verdicts> steps) {
        Segment {
            steps = List.copyOf(steps);
        }

        /** Whether {@code verdict} is the last verdict of the segment's last step: the segment is judged after it. */
        boolean closes(ExecutionPlan.Verdict verdict) {
            ExecutionPlan.Verdicts last = steps.get(steps.size() - 1);
            return last.verdicts().get(last.verdicts().size() - 1) == verdict;
        }
    }

    /** The steps at each statement-root assert's place (empty: absorbed into a later assert's). */
    private final Map<Integer, List<ExecutionPlan.Verdicts>> byStatement;
    /** The segment each verdict group's verdicts belong to (a record compares by value: by identity). */
    private final Map<ExecutionPlan.Verdict, Segment> byVerdict;
    /** The stamp parameter minted for each frame let, by the let's place: the comment of its segment's fused statement. */
    private final Map<Integer, String> frameStamps;

    private DatabaseAsserts(Map<Integer, List<ExecutionPlan.Verdicts>> byStatement,
            Map<ExecutionPlan.Verdict, Segment> byVerdict, Map<Integer, String> frameStamps) {
        this.byStatement = Collections.unmodifiableMap(new LinkedHashMap<>(byStatement));
        this.byVerdict = byVerdict;
        this.frameStamps = Collections.unmodifiableMap(new LinkedHashMap<>(frameStamps));
    }

    /**
     * {@code typed}'s asserts planned on {@code env} (the body's environment, as the walk makes it) for the DATABASE
     * judge, segment by segment. Null where the walk keeps the body (above). Two passes: a STATIC pass first, over a
     * fresh environment with no batch, that asks of every assert whether its database arm plans without reading a
     * side ({@link AssertVerdicts#databasePlansWithoutReading}) and of every let and frame whether it builds -- so a
     * body this planner leaves for the walk has counted nothing (the arms count the assert entered and its family as
     * they plan; the M7 audit's F1); then the planning pass under the collecting batch.
     */
    static @Nullable DatabaseAsserts plan(TypedQuery typed, String runtime, BodyDriver.ExecEnv env) {
        return plan(typed, runtime, env, false);
    }

    /** {@link #plan(TypedQuery, String, BodyDriver.ExecEnv)}; {@code systemRouted}: {@code env}'s session is the
     *  graph's system database (M10a), where an engine-text assert's appeal would read the user store on the wrong
     *  database: such a body is the walk's (M10b). */
    static @Nullable DatabaseAsserts plan(TypedQuery typed, String runtime, BodyDriver.ExecEnv env,
            boolean systemRouted) {
        SpecCompiler specs = typed.specs();
        if (!plannable(typed, systemRouted) || !everyAssertPlansStatically(typed, runtime, env, systemRouted)) {
            return null;
        }
        QueryDoor.Scan scan = new QueryDoor.Scan(typed, runtime);
        List<TypedSpec> body = typed.body();
        List<TypedSpec> letPrefix = new ArrayList<>();
        Map<String, BodyDriver.ExecFrame> frames = new LinkedHashMap<>();
        Map<com.legend.model.FunctionId, Boolean> effectMemo = new java.util.HashMap<>();
        Map<Integer, List<ExecutionPlan.Verdicts>> byStatement = new LinkedHashMap<>();
        Map<ExecutionPlan.Verdict, Segment> byVerdict = new IdentityHashMap<>();
        Map<Integer, String> frameStamps = new LinkedHashMap<>();
        // the walk's fragment map: a frame's CTE name to the let it came from, so a split rung's reason names it
        Map<String, String> fragments = new LinkedHashMap<>();
        int segment = 0;
        VerdictBatch batch = BodyDriver.newVerdictBatch(env);
        BodyDriver.ExecEnv benv = env.withVerdictBatch(batch);
        int lastAssert = -1;
        // once an arm left through a WALL the walk's batched discards the root, flushes the roots before it and
        // surfaces: nothing after runs or counts. The segment closes at the wall's assert; every later assert is
        // absorbed unplanned (the test door needs a non-null answer), every later let untouched (the M7 audit's F5)
        boolean walled = false;
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            boolean last = i == body.size() - 1;
            EnumSet<QueryDoor.TestOnly> kinds = scan.kinds(statement);
            if (statement instanceof TypedLet let && !last) {
                if (walled) {
                    continue;
                }
                java.util.function.Predicate<String> relationFrame =
                        n -> frames.containsKey(n) && frames.get(n).relationRooted();
                LetPlanner.Kind kind = LetPlanner.kind(let, body, i, specs, effectMemo, relationFrame);
                if (kind == LetPlanner.Kind.FRAME) {
                    // the frame as the walk builds it under the batch: not run, its CTE defined when a reader splices
                    // it; its activity comment the segment's fused statement's stamp
                    String stamp = stampOfSegment(segment);
                    frameStamps.put(i, stamp);
                    frames.put(let.name(), BodyDriver.plannedFrame((TypedNativeCall) LetPlanner.rhs(let), letPrefix,
                            specs, benv, stamp));
                    fragments.put("frame_" + let.name(), "let " + let.name() + " (statement " + (i + 1) + ")");
                } else if (kind == LetPlanner.Kind.ALIAS) {
                    frames.put(let.name(), java.util.Objects.requireNonNull(frames.get(
                            java.util.Objects.requireNonNull(LetPlanner.aliasOf(let.value(), relationFrame)))));
                } else {
                    PlanAllocations.registerHandlesIn(let.name(), LetPlanner.rhs(let), letPrefix, specs, benv.plan());
                    letPrefix.add(let);
                }
                continue;
            }
            if (kinds.contains(QueryDoor.TestOnly.EFFECT)) {
                // the walk flushes before an effect runs: the segment closes here, the next opens with a fresh batch
                if (!walled) {
                    batch.fragments(fragments);
                    if (!close(batch, segment, lastAssert, env, byStatement, byVerdict)) {
                        return null;
                    }
                    segment++;
                    batch = BodyDriver.newVerdictBatch(env);
                    benv = env.withVerdictBatch(batch);
                }
                continue;
            }
            if (!kinds.equals(EnumSet.of(QueryDoor.TestOnly.ASSERT)) || statement instanceof TypedLet) {
                continue;
            }
            if (walled) {
                byStatement.put(i, List.of());
                continue;
            }
            TypedSpec bare = Lets.bare(statement);
            BodyDriver.ExecEnv at = BodyDriver.frameReplaceEnv(statement, frames, benv, letPrefix, specs);
            AssertVerdicts.SpliceHook hook = BodyDriver.spliceHook(frames, letPrefix, specs, benv)::apply;
            AssertVerdicts.DatabasePlan outcome;
            try {
                outcome = AssertVerdicts.planDatabase(batch, bare, letPrefix, specs, at, hook);
            } catch (RuntimeException e) {
                // the gate's own classification raised (a side's planning): the arm would raise the same at the
                // assert's turn, after the roots before it reported -- a wall at the root's place
                batch.open(AssertVerdicts.listenerName(bare));
                batch.wall(e);
                batch.close();
                outcome = AssertVerdicts.DatabasePlan.WALLED;
            }
            if (outcome == AssertVerdicts.DatabasePlan.UNPLANNED) {
                // not an assert the database arms plan without reading a side at its turn (the static pass asked
                // first; kept as the guard behind it): the walk runs the body
                return null;
            }
            byStatement.put(i, List.of());
            lastAssert = i;
            if (outcome == AssertVerdicts.DatabasePlan.WALLED) {
                batch.fragments(fragments);
                if (!close(batch, segment, lastAssert, env, byStatement, byVerdict)) {
                    return null;
                }
                walled = true;
            }
        }
        if (!walled) {
            batch.fragments(fragments);
            if (!close(batch, segment, lastAssert, env, byStatement, byVerdict)) {
                return null;
            }
        }
        if (!everyReadStampBound(byStatement.values())) {
            // a frame's activity comment read by a segment other than the one whose fused statement is its stamp (the
            // frame's own segment closed with no assert): the walk's reserved comment is the first stamped statement's
            // after the frame, which no step of this plan names -- the walk keeps the body (the static pass refuses
            // the shape by name; kept as the guard behind it)
            return null;
        }
        return new DatabaseAsserts(byStatement, byVerdict, frameStamps);
    }

    /**
     * THE STATIC PASS (the M7 audit's F1, F3, F4): over a fresh environment with no batch -- nothing registered on the
     * body's, nothing counted -- every let built as the walk builds it (a frame with no run, an alias, a handle
     * registered) and every statement-root assert asked whether its database arm plans without reading a side; a
     * let that raises, a frame environment that raises before an arm, or an assert the arms do not plan statically
     * leaves the body to the walk (which raises at the let's or the assert's turn, after the earlier segments ran).
     * Also refused: a frame whose activities a LATER segment's assert reads while the frame's own segment holds no
     * assert (its stamp would be bound by no fused statement: the walk's reserved comment is the next stamped
     * statement's). A gate that raises is left to the arm, which raises the same at its turn (a planned wall).
     */
    private static boolean everyAssertPlansStatically(TypedQuery typed, String runtime, BodyDriver.ExecEnv env,
            boolean systemRouted) {
        SpecCompiler specs = typed.specs();
        BodyDriver.ExecEnv pre = env.withPlan(com.legend.planner.PlanEnv.of(env.ctx(), env.runtimeFqn(),
                env.dialect(), env.plan().features(), env.plan().generatorPlanText()));
        QueryDoor.Scan scan = new QueryDoor.Scan(typed, runtime);
        List<TypedSpec> body = typed.body();
        List<TypedSpec> letPrefix = new ArrayList<>();
        Map<String, BodyDriver.ExecFrame> frames = new LinkedHashMap<>();
        Map<String, Integer> frameSegment = new LinkedHashMap<>();
        java.util.Set<Integer> segmentsWithAssert = new java.util.HashSet<>();
        // the segments where an assert's arm PLANS a verdict row (the gate's PLANS or ENGINE_TEXT): a raise at
        // planning after one has a row to carry its wall (M10b)
        java.util.Set<Integer> segmentsWithRows = new java.util.HashSet<>();
        Map<com.legend.model.FunctionId, Boolean> effectMemo = new java.util.HashMap<>();
        Map<com.legend.model.FunctionId, Boolean> readMemo = new java.util.HashMap<>();
        int segment = 0;
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            boolean last = i == body.size() - 1;
            EnumSet<QueryDoor.TestOnly> kinds = scan.kinds(statement);
            if (statement instanceof TypedLet let && !last) {
                try {
                    java.util.function.Predicate<String> relationFrame =
                            n -> frames.containsKey(n) && frames.get(n).relationRooted();
                    LetPlanner.Kind kind = LetPlanner.kind(let, body, i, specs, effectMemo, relationFrame);
                    if (kind == LetPlanner.Kind.FRAME) {
                        frames.put(let.name(), BodyDriver.buildFrame((TypedNativeCall) LetPlanner.rhs(let), letPrefix,
                                false, specs, pre));
                        frameSegment.put(let.name(), segment);
                    } else if (kind == LetPlanner.Kind.ALIAS) {
                        String of = java.util.Objects.requireNonNull(LetPlanner.aliasOf(let.value(), relationFrame));
                        frames.put(let.name(), java.util.Objects.requireNonNull(frames.get(of)));
                        frameSegment.put(let.name(), java.util.Objects.requireNonNull(frameSegment.get(of)));
                    } else {
                        PlanAllocations.registerHandlesIn(let.name(), LetPlanner.rhs(let), letPrefix, specs,
                                pre.plan());
                        letPrefix.add(let);
                    }
                } catch (RuntimeException e) {
                    // the let raises: the walk raises it at the let's turn, after the earlier segments ran and
                    // reported (the M7 audit's F4) -- the walk's body
                    return false;
                }
                continue;
            }
            if (kinds.contains(QueryDoor.TestOnly.EFFECT)) {
                segment++;
                continue;
            }
            if (!kinds.equals(EnumSet.of(QueryDoor.TestOnly.ASSERT)) || statement instanceof TypedLet) {
                continue;
            }
            TypedSpec bare = Lets.bare(statement);
            BodyDriver.ExecEnv at;
            AssertVerdicts.SpliceHook hook;
            try {
                at = BodyDriver.frameReplaceEnv(statement, frames, pre, letPrefix, specs);
                hook = BodyDriver.spliceHook(frames, letPrefix, specs, pre)::apply;
            } catch (RuntimeException e) {
                // the frame environment raises before the arm: the walk raises it there, before anything is flushed
                // (the M7 audit's F3) -- the walk's body
                return false;
            }
            try {
                AssertVerdicts.DatabaseGate gate = AssertVerdicts.databaseGate(bare, letPrefix, specs, at, hook);
                if (gate == AssertVerdicts.DatabaseGate.UNPLANNED) {
                    return false;
                }
                if (gate == AssertVerdicts.DatabaseGate.ENGINE_TEXT && systemRouted) {
                    // an engine-text assert in a system-routed body: its appeal reads the user store on the body's
                    // session where the walk runs it, not this program's (M10b)
                    return false;
                }
                if (gate == AssertVerdicts.DatabaseGate.RAISES && !segmentsWithRows.contains(segment)) {
                    // the arm raises at planning and no row before it in its segment: the planning pass would wall a
                    // batch with no row to carry the wall (close) and give the body back after counting, and the walk
                    // would count it again -- the walk's, decided here so it counts nothing (M7's rule; found by
                    // M10b's foreign-dialect witness). A row an earlier arm RESOLVED at planning carries none either:
                    // that case is the gap it was (M7), recorded.
                    return false;
                }
                if (gate != AssertVerdicts.DatabaseGate.RAISES) {
                    segmentsWithRows.add(segment);
                }
            } catch (RuntimeException e) {
                // the gate's classification raised: not a route -- the arm raises the same at the assert's turn and
                // the planning pass plans that raise at the root's place
            }
            for (var f : frameSegment.entrySet()) {
                if (f.getValue() < segment && !segmentsWithAssert.contains(f.getValue())
                        && StatementEffects.reaches(statement, n -> readsActivitiesOf(n, f.getKey()), specs,
                                readMemo)) {
                    return false;
                }
            }
            segmentsWithAssert.add(segment);
        }
        return true;
    }

    /** {@code $frame.activities}, the read of {@code frame}'s activity rows. */
    private static boolean readsActivitiesOf(TypedSpec n, String frame) {
        return n instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa && "activities".equals(pa.property())
                && pa.source() instanceof com.legend.compiler.spec.typed.TypedVariable v && frame.equals(v.name());
    }

    /** Whether every stamp parameter a verdict step's statement reads is one a fused statement among {@code steps}
     *  binds (a frame's activity comment: the fused statement of the frame's own segment). */
    private static boolean everyReadStampBound(java.util.Collection<List<ExecutionPlan.Verdicts>> steps) {
        java.util.Set<String> bound = new java.util.HashSet<>();
        List<ExecutionPlan.Sql> sqls = new ArrayList<>();
        for (List<ExecutionPlan.Verdicts> groups : steps) {
            for (ExecutionPlan.Verdicts group : groups) {
                if (group.fused() != null) {
                    sqls.add(group.fused().sql());
                    if (group.fused().sql().stamp() != null) {
                        bound.add(group.fused().sql().stamp());
                    }
                }
                for (ExecutionPlan.Verdict v : group.verdicts()) {
                    if (v.wrapper() instanceof ExecutionPlan.TdsResult t) {
                        sqls.add(t.sql());
                    }
                }
            }
        }
        for (ExecutionPlan.Sql sql : sqls) {
            for (ExecutionPlan.Slot slot : sql.slots()) {
                if (slot.parameter().startsWith(ProgramFrames.STAMP) && !bound.contains(slot.parameter())) {
                    return false;
                }
            }
        }
        return true;
    }

    /** The shapes the planner refuses BEFORE planning anything, so a body it leaves for the walk has nothing counted
     *  twice (the census the arms count as they plan: the assert entered, its family; the flush): a raw grid read (an
     *  {@code executeInDb} whose text is a SELECT reads as a value; its verdict wrapper is KEYED, which the program
     *  road does not place), a lineage scan (its rows join the metamodel tables: the system session) in a body that is
     *  not system-routed -- in one that is, the program's one session is the system one and the scan is planned like
     *  any other side (M10c item 3). The checks after planning (a group's session, a keyed member, an unspelled
     *  segment, an unbound frame stamp) stay as the guard behind this one. The scan reads what the program road would
     *  RUN of each statement ({@link #scanned}). */
    private static boolean plannable(TypedQuery typed, boolean systemRouted) {
        Map<com.legend.model.FunctionId, Boolean> memo = new java.util.HashMap<>();
        java.util.function.Predicate<TypedSpec> refused = n -> n instanceof
                com.legend.compiler.spec.typed.TypedRawSqlRelation || !systemRouted && lineageScan(n);
        for (TypedSpec statement : typed.body()) {
            for (TypedSpec part : scanned(statement)) {
                if (com.legend.compiler.spec.StatementEffects.reaches(part, refused, typed.specs(), memo)) {
                    return false;
                }
            }
        }
        return true;
    }

    /** What the program road would run of {@code statement}: the statement, but for a root ENGINE-TEXT assert (M10b,
     *  {@link SqlTextVerdicts#isRoot}) its ARGUMENTS alone -- on the program road, which this scan gates, the arm plans
     *  the two texts from them and the callee's body is never planned ({@code AssertVerdicts.planHost}: the arm under
     *  a collector, else a declined dual-golden rewritten to {@code assertEquals(upgraded, actual)}, else the walk's);
     *  on the walk the dual-golden arm is total and the other two roots' declines inline the callee as they always did.
     *  The engine's {@code assertEqualsH2Compatible} reads the H2 version through {@code executeInDb} in its helper
     *  (h2Extension.pure): a raw grid the program road never sends, which this scan read as the body's until the
     *  test-is-the-product plan's M10c (2026-11-02; 321 DuckDB bodies). The statement itself is matched, never a let's
     *  value: a let-wrapped root is no assert to the class rule, and this planner would plan such a let as a value --
     *  the call spliced whole -- so a let is scanned whole. */
    private static List<TypedSpec> scanned(TypedSpec statement) {
        if (statement instanceof com.legend.compiler.spec.typed.TypedUserCall u
                && SqlTextVerdicts.isRoot(u.callee().qualifiedName())) {
            return u.args();
        }
        return List.of(statement);
    }

    private static boolean lineageScan(TypedSpec n) {
        return n instanceof TypedNativeCall call
                && (com.legend.compiler.element.type.PlatformTypes.SCAN_RELATIONS.equals(call.callee().qualifiedName())
                        || com.legend.compiler.element.type.PlatformTypes.SCAN_COLUMNS
                                .equals(call.callee().qualifiedName()));
    }

    /** The segment's batch planned and its groups placed at the segment's last assert; false where the walk keeps
     *  the body: a group on another session than the body's, or a keyed member. */
    private static boolean close(VerdictBatch batch, int segment, int lastAssert, BodyDriver.ExecEnv env,
            Map<Integer, List<ExecutionPlan.Verdicts>> byStatement, Map<ExecutionPlan.Verdict, Segment> byVerdict) {
        VerdictBatch.Planned planned = batch.plan();
        if (planned == null) {
            return true;
        }
        if (lastAssert < 0) {
            throw new IllegalStateException("a verdict batch planned with no assert in its segment");
        }
        List<ExecutionPlan.Verdicts> steps = new ArrayList<>();
        for (VerdictBatch.Group g : planned.groups()) {
            if (g.on() != env.session() || g.keyed()) {
                return false;
            }
            steps.add(stamped(g.step(), stampOfSegment(segment)));
        }
        if (steps.isEmpty()) {
            // every member's canon the dialect cannot spell: the walk refuses each at its place in the root loop,
            // which the program road reaches only through a group's step -- the walk keeps the body (M8 carries the
            // refusal as a step)
            return false;
        }
        Segment seg = new Segment(planned, steps);
        for (ExecutionPlan.Verdicts step : steps) {
            for (ExecutionPlan.Verdict v : step.verdicts()) {
                byVerdict.put(v, seg);
            }
        }
        byStatement.put(lastAssert, steps);
        return true;
    }

    /** {@code step}'s fused statement stamped with {@code stamp}: the comment it is sent under, which the segment's
     *  frames' activity rows read as a parameter (the walk's reserved comment). */
    private static ExecutionPlan.Verdicts stamped(ExecutionPlan.Verdicts step, String stamp) {
        ExecutionPlan.TdsResult fused = step.fused();
        if (fused == null) {
            return step;
        }
        return new ExecutionPlan.Verdicts(step.verdicts(),
                new ExecutionPlan.TdsResult(fused.shape(), fused.columns(), fused.sql().stamped(stamp)),
                step.savepoint());
    }

    /** The stamp parameter of the fused statement of segment {@code segment}: the stamp prefix (the program's, so the
     *  test door adopts it and the runner binds it), then the segment's place. */
    private static String stampOfSegment(int segment) {
        return ProgramFrames.STAMP + ".fused" + segment;
    }

    @Override
    public @Nullable List<ExecutionPlan.Verdicts> at(int statement) {
        return byStatement.get(statement);
    }

    @Override
    public @Nullable String frameStamp(int statement) {
        return frameStamps.get(statement);
    }

    @Override
    public boolean runsFrames() {
        return false;
    }

    /** The segment the runner handed {@code verdict} for. */
    Segment of(ExecutionPlan.Verdict verdict) {
        Segment s = byVerdict.get(verdict);
        if (s == null) {
            throw new IllegalStateException("a verdict the database judge's planner did not plan: " + verdict.name());
        }
        return s;
    }

    /** The runner's rows for {@code verdict} handed to its segment; the segment judged once its last verdict's row is
     *  in (the batch's root loop: the walk's census and listener protocol, the first failure raising). */
    void received(ExecutionPlan.Verdict verdict, @Nullable List<Object> row, @Nullable AssertListener listener) {
        Segment s = of(verdict);
        s.planned().received(verdict, row);
        if (s.closes(verdict)) {
            s.planned().judge(listener);
        }
    }

    /** The runner's word on a group's fused statement, to its segment's batch. */
    void fused(ExecutionPlan.Verdicts group, com.legend.error.@Nullable DataError rejected) {
        of(group.verdicts().get(0)).planned().fused(group, rejected);
    }
}
