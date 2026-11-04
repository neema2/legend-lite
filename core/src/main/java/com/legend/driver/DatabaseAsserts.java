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
    record Segment(VerdictBatch.Planned planned, List<ExecutionPlan.Verdicts> steps, boolean decided) {
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

    /** The branches planned (the asserts-as-plan-nodes program, step 4), by the statement's place. */
    private final Map<Integer, ExecutionPlan.Branch> branches;

    private DatabaseAsserts(Map<Integer, List<ExecutionPlan.Verdicts>> byStatement,
            Map<ExecutionPlan.Verdict, Segment> byVerdict, Map<Integer, String> frameStamps,
            Map<ExecutionPlan.Verdict, HostAsserts.Planned> hostSteps, Map<Integer, ExecutionPlan.Branch> branches) {
        this.byStatement = Collections.unmodifiableMap(new LinkedHashMap<>(byStatement));
        this.byVerdict = byVerdict;
        this.frameStamps = Collections.unmodifiableMap(new LinkedHashMap<>(frameStamps));
        this.hostSteps = hostSteps;
        this.branches = Collections.unmodifiableMap(new LinkedHashMap<>(branches));
    }

    /** The assertErrors' steps by their verdict (ruling N3: the comparison is Java's in both judge modes, so the step
     *  is the host planner's, its sides read and judged by the host's rule at the assert's turn). */
    private final Map<ExecutionPlan.Verdict, HostAsserts.Planned> hostSteps;

    /** The assertError step {@code verdict} is, or null: a verdict of the database's segments. */
    HostAsserts.@Nullable Planned hostStep(ExecutionPlan.Verdict verdict) {
        return hostSteps.get(verdict);
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
        return plan(typed, runtime, env, systemRouted, List.of(), "");
    }

    /** {@link #plan(TypedQuery, String, BodyDriver.ExecEnv, boolean)} over a body whose statements follow {@code lets}
     *  (a branch's side: the value lets before the branch, the outer body's, already planned there), its segments'
     *  stamps named under {@code place} (the side's place in the outer body: no two fused statements share a stamp). */
    private static @Nullable DatabaseAsserts plan(TypedQuery typed, String runtime, BodyDriver.ExecEnv env,
            boolean systemRouted, List<TypedSpec> lets, String place) {
        SpecCompiler specs = typed.specs();
        if (!plannable(typed, systemRouted)) {
            com.legend.diagnostics.Diagnostics.judgeDecline("unplanned", "", "the body reaches a lineage scan");
            return null;
        }
        if (!everyAssertPlansStatically(typed, runtime, env, systemRouted, lets)) {
            com.legend.diagnostics.Diagnostics.judgeDecline("unplanned", "", "the static pass");
            return null;
        }
        QueryDoor.Scan scan = new QueryDoor.Scan(typed, runtime);
        Map<ExecutionPlan.Verdict, HostAsserts.Planned> hostSteps = new IdentityHashMap<>();
        List<TypedSpec> body = typed.body();
        List<TypedSpec> letPrefix = new ArrayList<>(lets);
        Map<String, BodyDriver.ExecFrame> frames = new LinkedHashMap<>();
        Map<com.legend.model.FunctionId, Boolean> effectMemo = new java.util.HashMap<>();
        Map<Integer, List<ExecutionPlan.Verdicts>> byStatement = new LinkedHashMap<>();
        Map<Integer, ExecutionPlan.Branch> branches = new LinkedHashMap<>();
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
                    String stamp = stampOfSegment(place, segment);
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
                    if (!close(batch, place, segment, lastAssert, env, byStatement, byVerdict)) {
                        return null;
                    }
                    segment++;
                    batch = BodyDriver.newVerdictBatch(env);
                    benv = env.withVerdictBatch(batch);
                }
                continue;
            }
            if (!last && kinds.isEmpty() && !(statement instanceof TypedLet) && !frames.isEmpty()
                    && !java.util.Collections.disjoint(com.legend.compiler.spec.typed.FreeVars.of(statement),
                            frames.keySet())) {
                // a CHECK statement over a frame (an unread let's value, the user's ruling of 2026-11-02): the database
                // judge runs no frame at its let, so the check has no frame to read -- the walk's, by name, until the
                // check carries the frame's chain as its readers' statements do
                com.legend.exec.Census.unplannedAssert("database: a check over a frame not run at its let");
                return null;
            }
            AssertBranch.Parts parts = AssertBranch.of(statement, last, scan, specs);
            if (parts != null) {
                // an assert under an if (step 4): the segment before it closes here -- its asserts run before the
                // condition is read -- each side's asserts are segments of their own, planned as this body's are over
                // the lets before the branch, and the next segment opens after it
                if (walled) {
                    byStatement.put(i, List.of());
                    continue;
                }
                if (!frames.isEmpty()) {
                    // a side reading a frame would compose the frame's CTE into its own fused statement, its activity
                    // comment that statement's stamp: not designed for a side yet -- refused by name
                    com.legend.diagnostics.Diagnostics.judgeDecline("unplanned", "", "a branch after a frame let");
                    return null;
                }
                batch.fragments(fragments);
                if (!close(batch, place, segment, lastAssert, env, byStatement, byVerdict)) {
                    return null;
                }
                segment++;
                batch = BodyDriver.newVerdictBatch(env);
                benv = env.withVerdictBatch(batch);
                ExecutionPlan.Side condition = JudgePlans.plannedSide(parts.condition(), letPrefix, specs, env, false,
                        false, null).side();
                DatabaseAsserts then = plan(AssertBranch.body(typed, parts.then()), runtime, env, systemRouted,
                        letPrefix, place + i + "t.");
                DatabaseAsserts otherwise = then == null ? null : plan(AssertBranch.body(typed, parts.otherwise()),
                        runtime, env, systemRouted, letPrefix, place + i + "e.");
                if (then == null || otherwise == null) {
                    return null;
                }
                branches.put(i, new ExecutionPlan.Branch(condition, then.steps(parts.then().size()),
                        otherwise.steps(parts.otherwise().size())));
                for (DatabaseAsserts side : List.of(then, otherwise)) {
                    byVerdict.putAll(side.byVerdict);
                    hostSteps.putAll(side.hostSteps);
                }
                continue;
            }
            if (QueryDoor.assertErrorRoot(statement, specs)) {
                // ruling N3 under the database judge: assertError's comparison is Java's in both judge modes, so its
                // step is the host planner's (HostAsserts.planAssertError), placed at its turn -- the walk runs it at
                // its segment's close, so the segment before it closes here and the next opens after it
                if (walled) {
                    byStatement.put(i, List.of());
                    continue;
                }
                batch.fragments(fragments);
                if (!close(batch, place, segment, lastAssert, env, byStatement, byVerdict)) {
                    return null;
                }
                segment++;
                batch = BodyDriver.newVerdictBatch(env);
                benv = env.withVerdictBatch(batch);
                HostAsserts.Planned step = HostAsserts.planAssertError(statement, Lets.bare(statement), letPrefix,
                        frames, specs, env);
                if (step == null) {
                    return null;
                }
                byStatement.put(i, List.of(step.step()));
                hostSteps.put(step.verdict(), step);
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
                if (!close(batch, place, segment, lastAssert, env, byStatement, byVerdict)) {
                    return null;
                }
                walled = true;
            }
        }
        if (!walled) {
            batch.fragments(fragments);
            if (!close(batch, place, segment, lastAssert, env, byStatement, byVerdict)) {
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
        return new DatabaseAsserts(byStatement, byVerdict, frameStamps, hostSteps, branches);
    }

    /** The steps of this body's {@code statements} statements, in order (an absorbed assert's none). */
    List<ExecutionPlan.Node> steps(int statements) {
        List<ExecutionPlan.Node> out = new ArrayList<>();
        for (int i = 0; i < statements; i++) {
            List<? extends ExecutionPlan.Node> at = at(i);
            if (at == null) {
                throw new IllegalStateException("a branch side's statement " + i + " planned no step");
            }
            out.addAll(at);
        }
        return out;
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
            boolean systemRouted, List<TypedSpec> lets) {
        SpecCompiler specs = typed.specs();
        BodyDriver.ExecEnv pre = env.withPlan(com.legend.planner.PlanEnv.of(env.ctx(), env.runtimeFqn(),
                env.dialect(), env.plan().features(), env.plan().generatorPlanText()));
        QueryDoor.Scan scan = new QueryDoor.Scan(typed, runtime);
        List<TypedSpec> body = typed.body();
        List<TypedSpec> letPrefix = new ArrayList<>(lets);
        Map<String, BodyDriver.ExecFrame> frames = new LinkedHashMap<>();
        Map<String, Integer> frameSegment = new LinkedHashMap<>();
        java.util.Set<Integer> segmentsWithAssert = new java.util.HashSet<>();
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
            if (QueryDoor.assertErrorRoot(statement, specs)
                    || AssertBranch.of(statement, last, scan, specs) != null) {
                // the main pass closes the segment at an assertError (its step is the host's, ruling N3) and at a
                // branch (each side's asserts planned as a body of their own, its own static pass first): numbered
                // alike here, so a frame's stamp is bound in the same segment on both passes
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
                if (AssertVerdicts.engineText(gate) && systemRouted) {
                    // an engine-text assert in a system-routed body: its appeal reads the user store on the body's
                    // session where the walk runs it, not this program's (M10b)
                    return false;
                }
                // an arm that raises at planning with no row before it in its segment: the segment's batch holds the
                // wall alone, decided at planning, and close carries it as a step with no statement whose root loop
                // raises it at its turn (2026-11-03; until then close gave such a segment back to the walk, so this
                // pass refused it -- M7's rule -- and the database judge alone refused every body whose assert's side
                // the dialect cannot write: H2's 33 list operations, SQLite's and MariaDB's regular expressions)
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

    /** One assert's database half under BOTH judges (the walker-deletion program, 2026-11-02): its root planned under
     *  the body's collecting batch and planned alone ({@link VerdictBatch#plan}, one root), and its one verdict group
     *  where the batch made one (null: the root's steps are resolved at planning, or every member's canon the dialect
     *  cannot spell -- the root loop reaches it at its place, as the walk's statement-by-statement arm does). */
    record Each(VerdictBatch.Planned planned, @Nullable ExecutionPlan.Verdicts group) {
    }

    /**
     * {@code typed}'s asserts planned for the DATABASE half of the AGREEMENT judge ({@code JudgeMode.BOTH}; the
     * walker-deletion program, 2026-11-02): as the walk's agreement arm asks the database arm at each assert's turn --
     * no batch held across asserts, every frame run at its let by the host's check step -- each statement-root assert's
     * root planned alone, its verdict statement over the frames' CTEs (the frames built on {@code env}'s batch under the
     * stamp {@code frameStamp} names for the let: the host half's, which its check step binds). An assert absent from
     * the answer is one the database judge DECLINES (the agreement rule: the host decides, the listener is told): a
     * shape its arms answer null for or hand to the host at its turn ({@link AssertVerdicts#databaseGate}), an
     * engine-text assert of a system-routed body, a group on another session or a keyed member (a raw grid's report),
     * or every assert after a let the database half could not build. {@code env} judges with the DATABASE judge.
     */
    static Map<Integer, Each> planEach(TypedQuery typed, String runtime, BodyDriver.ExecEnv env, boolean systemRouted,
            java.util.function.IntFunction<@Nullable String> frameStamp) {
        return planEach(typed, runtime, env, systemRouted, frameStamp, List.of());
    }

    /** {@link #planEach(TypedQuery, String, BodyDriver.ExecEnv, boolean, java.util.function.IntFunction)} over a body
     *  whose statements follow {@code lets} (a branch's side: the agreement judge assembles the branch from the host
     *  half's, {@code AgreementAsserts}, and asks this half of each side's asserts). A branch statement is none of this
     *  half's: its sides are asked as bodies of their own. */
    static Map<Integer, Each> planEach(TypedQuery typed, String runtime, BodyDriver.ExecEnv env, boolean systemRouted,
            java.util.function.IntFunction<@Nullable String> frameStamp, List<TypedSpec> lets) {
        Map<Integer, Each> out = new LinkedHashMap<>();
        if (!plannable(typed, systemRouted)) {
            return out;
        }
        SpecCompiler specs = typed.specs();
        QueryDoor.Scan scan = new QueryDoor.Scan(typed, runtime);
        List<TypedSpec> body = typed.body();
        List<TypedSpec> letPrefix = new ArrayList<>(lets);
        Map<String, BodyDriver.ExecFrame> frames = new LinkedHashMap<>();
        Map<com.legend.model.FunctionId, Boolean> effectMemo = new java.util.HashMap<>();
        VerdictBatch batch = BodyDriver.newVerdictBatch(env);
        BodyDriver.ExecEnv benv = env.withVerdictBatch(batch);
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            boolean last = i == body.size() - 1;
            EnumSet<QueryDoor.TestOnly> kinds = scan.kinds(statement);
            if (statement instanceof TypedLet let && !last) {
                boolean built = false;
                try {
                    java.util.function.Predicate<String> relationFrame =
                            n -> frames.containsKey(n) && frames.get(n).relationRooted();
                    LetPlanner.Kind kind = LetPlanner.kind(let, body, i, specs, effectMemo, relationFrame);
                    if (kind == LetPlanner.Kind.FRAME) {
                        frames.put(let.name(), BodyDriver.plannedFrame((TypedNativeCall) LetPlanner.rhs(let),
                                letPrefix, specs, benv, java.util.Objects.requireNonNull(frameStamp.apply(i),
                                        "the host half minted no stamp for the frame let")));
                    } else if (kind == LetPlanner.Kind.ALIAS) {
                        frames.put(let.name(), java.util.Objects.requireNonNull(frames.get(
                                java.util.Objects.requireNonNull(LetPlanner.aliasOf(let.value(), relationFrame)))));
                    } else {
                        PlanAllocations.registerHandlesIn(let.name(), LetPlanner.rhs(let), letPrefix, specs,
                                benv.plan());
                        letPrefix.add(let);
                    }
                    built = true;
                } catch (RuntimeException e) {
                    // the host half raises it at its place (a planned raise); the database half declines the rest
                }
                if (!built) {
                    break;
                }
                continue;
            }
            if (!kinds.equals(EnumSet.of(QueryDoor.TestOnly.ASSERT)) || statement instanceof TypedLet
                    || AssertBranch.of(statement, last, scan, specs) != null) {
                continue;
            }
            TypedSpec bare = Lets.bare(statement);
            AssertVerdicts.DatabasePlan outcome;
            try {
                BodyDriver.ExecEnv at = BodyDriver.frameReplaceEnv(statement, frames, benv, letPrefix, specs);
                AssertVerdicts.SpliceHook hook = BodyDriver.spliceHook(frames, letPrefix, specs, benv)::apply;
                AssertVerdicts.DatabaseGate gate = AssertVerdicts.databaseGate(bare, letPrefix, specs, at, hook);
                // a raise at planning (RAISES) is planned as the walk's arm raises it: the root resolved or walled,
                // judged at the step with no row
                outcome = gate == AssertVerdicts.DatabaseGate.UNPLANNED
                        || AssertVerdicts.engineText(gate) && systemRouted
                        ? AssertVerdicts.DatabasePlan.UNPLANNED
                        : AssertVerdicts.planDatabase(batch, bare, letPrefix, specs, at, hook);
            } catch (RuntimeException e) {
                // the frame environment, the gate's classification or the arm raised: the arm raises the same at its
                // turn, a wall at the root's place (a frame environment's raise is the host half's before its arm: the
                // agreement judge never asks this half there)
                batch.open(AssertVerdicts.listenerName(bare));
                batch.wall(e);
                batch.close();
                outcome = AssertVerdicts.DatabasePlan.WALLED;
            }
            if (outcome == AssertVerdicts.DatabasePlan.UNPLANNED) {
                continue;
            }
            VerdictBatch.Planned planned = batch.plan();
            if (planned == null) {
                continue;
            }
            List<VerdictBatch.Group> groups = planned.groups();
            if (groups.size() > 1 || groups.stream().anyMatch(g -> !placed(g, env))) {
                com.legend.diagnostics.Diagnostics.judgeDecline("unplanned", AssertVerdicts.listenerName(bare),
                        groups.size() > 1 ? "several groups" : "a group on another session");
                continue;
            }
            out.put(i, new Each(planned, groups.isEmpty() ? null : groups.get(0).step()));
        }
        return out;
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

    /** Whether the program places {@code g} as a step: a group of fused verdicts (a keyed member's wrapper is the
     *  walk's) on the body's own session, or on the graph's system database beside it -- a verdict over the system
     *  store alone on a server engine, its statement naming that target, which the runner routes to the system
     *  database's session (per-statement routing; 2026-11-03: until then such a group left the body to the walk, so
     *  the database judge alone refused {@code getAll::testBasic} on Postgres, MariaDB and ClickHouse). */
    private static boolean placed(VerdictBatch.Group g, BodyDriver.ExecEnv env) {
        return g.on() == env.session() || SystemDatabase.of(env.ctx()).engineOf(g.on()) != null;
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
        java.util.function.Predicate<TypedSpec> refused = n -> !systemRouted && lineageScan(n);
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
    private static boolean close(VerdictBatch batch, String place, int segment, int lastAssert,
            BodyDriver.ExecEnv env, Map<Integer, List<ExecutionPlan.Verdicts>> byStatement,
            Map<ExecutionPlan.Verdict, Segment> byVerdict) {
        VerdictBatch.Planned planned = batch.plan();
        if (planned == null) {
            return true;
        }
        if (lastAssert < 0) {
            throw new IllegalStateException("a verdict batch planned with no assert in its segment");
        }
        if (planned.groups().isEmpty()) {
            // every root of the segment decided at planning (a statically identified assertIs: the compiler compares
            // the spelled types, for both judges alike; an unjudged decline; a planned raise), or every member's canon
            // one the dialect cannot spell (the split rung at planning): nothing for the database to read -- one
            // verdict step with no statement carries the segment, and the sink runs the batch's root loop at it, as
            // after a segment's fused statement, where an unspelled member is refused at its own place in the
            // dialect's own words before anything is sent (VerdictBatch.executeOne) -- the host judge's raise for the
            // same side (2026-11-03: the walk had kept such a body, so the database judge alone planned 9 PCT bodies
            // fewer than the host on DuckDB, and refused H2's 33 list operations)
            ExecutionPlan.Verdict decided = new ExecutionPlan.Verdict("decided@" + place + segment, "decided",
                    List.of(), null);
            ExecutionPlan.Verdicts step = new ExecutionPlan.Verdicts(List.of(decided), null, null,
                    env.options().judgingRules());
            byVerdict.put(decided, new Segment(planned, List.of(step), true));
            byStatement.put(lastAssert, List.of(step));
            return true;
        }
        List<ExecutionPlan.Verdicts> steps = new ArrayList<>();
        for (VerdictBatch.Group g : planned.groups()) {
            if (!placed(g, env)) {
                return false;
            }
            steps.add(stamped(g.step(), stampOfSegment(place, segment)));
        }
        Segment seg = new Segment(planned, steps, false);
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
                step.savepoint(), step.rules());
    }

    /** The stamp parameter of the fused statement of segment {@code segment}: the stamp prefix (the program's, so the
     *  test door adopts it and the runner binds it), then the segment's place. */
    private static String stampOfSegment(String place, int segment) {
        return ProgramFrames.STAMP + ".fused" + place + segment;
    }

    @Override
    public @Nullable List<? extends ExecutionPlan.Node> at(int statement) {
        ExecutionPlan.Branch b = branches.get(statement);
        return b != null ? List.of(b) : byStatement.get(statement);
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
        if (!s.decided()) {
            // a decided segment's step carries no member of the batch: its root loop reads no row
            s.planned().received(verdict, row);
        }
        if (s.closes(verdict)) {
            s.planned().judge(listener);
        }
    }

    /** The runner's word on a group's fused statement, to its segment's batch. */
    void fused(ExecutionPlan.Verdicts group, com.legend.error.@Nullable DataError rejected) {
        of(group.verdicts().get(0)).planned().fused(group, rejected);
    }
}
