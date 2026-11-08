// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.driver;

import com.legend.testexecutionplan.HostJudgment;
import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.StatementEffects;
import com.legend.compiler.spec.typed.Lets;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testexecutionplan.TestPlan;
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
 * rows back and judges at the segment's last group with the segment's root loop, the plan's data ({@code RootLoops},
 * the plan-as-data design's step 5: the census and listener protocol; a member the fused statement could not spell
 * has its own statement in the plan, the split rung at planning, P1.2, 2026-11-07). Null where the body is not
 * planned here: an assert of a shape the database arms answer null for, a group on another session than the body's
 * (the system database's: M10a), a keyed member (a raw grid's report), a frame whose activity comment a later
 * segment reads.
 */
final class DatabaseAsserts implements TestDoor.Asserts {

    /** The steps at each statement-root assert's place (empty: absorbed into a later assert's). */
    private final Map<Integer, List<TestPlan.Verdicts>> byStatement;
    /** The stamp parameter minted for each frame let, by the let's place: the comment of its segment's fused statement. */
    private final Map<Integer, String> frameStamps;

    /** The branches planned (the asserts-as-plan-nodes program, step 4), by the statement's place. */
    private final Map<Integer, TestPlan.Branch> branches;

    /** (the plan-as-data design's step 5, 2026-11-08: the planner's run-side maps -- each verdict's segment, the
     *  assertErrors' steps by verdict -- deleted; the run reads each segment's root loop, a verdict's host facts and its
     *  statement from the plan: BodyRouter, RootLoops) */
    private DatabaseAsserts(Map<Integer, List<TestPlan.Verdicts>> byStatement, Map<Integer, String> frameStamps,
            Map<Integer, TestPlan.Branch> branches) {
        this.byStatement = Collections.unmodifiableMap(new LinkedHashMap<>(byStatement));
        this.frameStamps = Collections.unmodifiableMap(new LinkedHashMap<>(frameStamps));
        this.branches = Collections.unmodifiableMap(new LinkedHashMap<>(branches));
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
        return plan(typed, runtime, env, systemRouted, null, List.of(), "");
    }

    /** {@link #plan(TypedQuery, String, BodyDriver.ExecEnv, boolean)} for a SYSTEM-ROUTED body ({@code bodyEnv}
     *  non-null): an engine-text assert's compatibility appeal reads the user's tables on {@code bodyEnv}, the body's
     *  own environment ({@code ExecEnv.appealOn}), its verdict row on the program's -- the host judge's split (M10c
     *  item 10), the database judge's since 2026-11-06 (the walk deletion's step 3). */
    static @Nullable DatabaseAsserts plan(TypedQuery typed, String runtime, BodyDriver.ExecEnv env,
            BodyDriver.@Nullable ExecEnv bodyEnv) {
        return plan(typed, runtime, env, bodyEnv != null, bodyEnv, List.of(), "");
    }

    /** {@link #plan(TypedQuery, String, BodyDriver.ExecEnv, boolean)} over a body whose statements follow {@code lets}
     *  (a branch's side: the value lets before the branch, the outer body's, already planned there), its segments'
     *  stamps named under {@code place} (the side's place in the outer body: no two fused statements share a stamp). */
    private static @Nullable DatabaseAsserts plan(TypedQuery typed, String runtime, BodyDriver.ExecEnv env,
            boolean systemRouted, BodyDriver.@Nullable ExecEnv bodyEnv, List<TypedSpec> lets, String place) {
        SpecCompiler specs = typed.specs();
        if (!plannable(typed, systemRouted)) {
            com.legend.diagnostics.Diagnostics.judgeDecline("unplanned", "", "the body reaches a lineage scan");
            return null;
        }
        if (!everyAssertPlansStatically(typed, runtime, env, systemRouted && bodyEnv == null, lets)) {
            com.legend.diagnostics.Diagnostics.judgeDecline("unplanned", "", "the static pass");
            return null;
        }
        QueryDoor.Scan scan = new QueryDoor.Scan(typed, runtime);
        List<TypedSpec> body = typed.body();
        List<TypedSpec> letPrefix = new ArrayList<>(lets);
        Map<String, BodyDriver.ExecFrame> frames = new LinkedHashMap<>();
        Map<com.legend.model.FunctionId, Boolean> effectMemo = new java.util.HashMap<>();
        Map<Integer, List<TestPlan.Verdicts>> byStatement = new LinkedHashMap<>();
        Map<Integer, TestPlan.Branch> branches = new LinkedHashMap<>();
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
                } else if (kind == LetPlanner.Kind.STAGED_GRID) {
                    // a raw grid staged across a later effect: read from the table the test door stages it
                    // into at its turn (PlanMaker.stagedGridLet; the walk deletion, 2026-11-06)
                    letPrefix.add(com.legend.resolver.StagedRows.gridBoundTo(let.name(),
                            com.legend.planner.PlanMaker.gridRead(
                                    com.legend.resolver.StagedRows.gridTable(let.name(), i), benv.dialect()),
                            (com.legend.compiler.spec.typed.TypedRawSqlRelation) LetPlanner.rhs(let),
                            let.info()));
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
                    if (!close(batch, place, segment, lastAssert, env, byStatement)) {
                        return null;
                    }
                    segment++;
                    batch = BodyDriver.newVerdictBatch(env);
                    benv = env.withVerdictBatch(batch);
                }
                continue;
            }
            // a CHECK statement over a frame (a println; the user's ruling of 2026-11-02 for an unread let's value) is
            // the program's: the test door runs a frame a check reads at its let, as Pure runs it and the host judge
            // does (TestKinds.frame, 2026-11-06), and the check reads it as the host road's readers do; the asserts
            // here read it inside their fused statement as before
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
                if (!close(batch, place, segment, lastAssert, env, byStatement)) {
                    return null;
                }
                segment++;
                batch = BodyDriver.newVerdictBatch(env);
                benv = env.withVerdictBatch(batch);
                TestPlan.Side condition = JudgePlans.plannedSide(parts.condition(), letPrefix, specs, env, false,
                        false, null).side();
                DatabaseAsserts then = plan(AssertBranch.body(typed, parts.then()), runtime, env, systemRouted,
                        bodyEnv, letPrefix, place + i + "t.");
                DatabaseAsserts otherwise = then == null ? null : plan(AssertBranch.body(typed, parts.otherwise()),
                        runtime, env, systemRouted, bodyEnv, letPrefix, place + i + "e.");
                if (then == null || otherwise == null) {
                    return null;
                }
                branches.put(i, new TestPlan.Branch(condition, then.steps(parts.then().size()),
                        otherwise.steps(parts.otherwise().size())));
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
                if (!close(batch, place, segment, lastAssert, env, byStatement)) {
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
                TestPlan.Verdicts judged = errorVerdict(step, env,
                        () -> innerSole(statement, letPrefix, frames, specs, env));
                if (judged != null) {
                    // the user's ruling S135 (2026-11-05): the host catches, the database judges
                    byStatement.put(i, List.of(judged));
                    continue;
                }
                if (step.assertion().beforeTheArm() == null && HostJudgment.core(
                        step.assertion().check().judgment()) instanceof HostJudgment.AssertError) {
                    // strict (the user, 2026-11-05: the database judge has no fallback): an assertError it cannot
                    // plan as its verdict is refused by name, never judged by the host
                    com.legend.diagnostics.Diagnostics.judgeDecline("unplanned", step.assertion().listenerName(),
                            "the database judge cannot plan this assertError as its verdict");
                    return null;
                }
                byStatement.put(i, List.of(step.step()));
                com.legend.diagnostics.Diagnostics.judgeSend("host-step", AssertVerdicts.listenerName(
                        Lets.bare(statement)));
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
                if (bodyEnv != null) {
                    // a system-routed body (above): an engine-text assert's appeal planned on the body's environment
                    at = at.withAppealOn(bodyEnv);
                }
                outcome = AssertVerdicts.planDatabase(batch, bare, letPrefix, specs, at, hook);
            } catch (RuntimeException e) {
                // the gate's own classification raised (a side's planning): the arm would raise the same at the
                // assert's turn, after the roots before it reported -- a wall at the root's place
                batch.open(AssertVerdicts.listenerName(bare));
                batch.wall(com.legend.judge.Failures.of(e, "DatabaseAsserts.plan: the gate's classification"));
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
                if (!close(batch, place, segment, lastAssert, env, byStatement)) {
                    return null;
                }
                walled = true;
            }
        }
        if (!walled) {
            batch.fragments(fragments);
            if (!close(batch, place, segment, lastAssert, env, byStatement)) {
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
        return new DatabaseAsserts(byStatement, frameStamps, branches);
    }

    /**
     * An assertError's step as the DATABASE judges it (the user's ruling S135, 2026-11-05): the host planner's step
     * ({@code step}: the asserted function's sides, the group's savepoint) re-planned as a raising verdict -- the runner
     * reads the sides and binds what they raised to the statement's parameters, the statement compares (VerdictSql
     * .assertError) -- or, where the function raised while it was planned, a verdict over that error as constants.
     * Null where the step holds no comparison: an inner assert (its own verdict's failure is the error, planned by the
     * host planner until it is a raising statement), or a planning raise no arm takes (a wall raised at its turn).
     */
    private static TestPlan.@Nullable Verdicts errorVerdict(HostAsserts.Planned step, BodyDriver.ExecEnv env,
            java.util.function.Supplier<com.legend.judge.VerdictBatch.@Nullable Sole> innerSole) {
        AssertVerdicts.HostAssert a = step.assertion();
        if (a.beforeTheArm() != null
                || !(HostJudgment.core(a.check().judgment()) instanceof HostJudgment.AssertError ae)) {
            return null;
        }
        TestPlan.Verdicts host = step.step();
        com.legend.lowering.VerdictSql.Raised raised;
        List<TestPlan.Side> base;
        boolean raises;
        if (ae.inner() != null) {
            // f IS one assert: its own verdict row, planned by the database judge, as a statement that RAISES its failure
            // (VerdictSql.raisingRow) -- the base the runner reads and catches as any f's; a failure decided at planning
            // is the outcome as a constant
            com.legend.judge.VerdictBatch.Sole sole = innerSole.get();
            if (sole instanceof com.legend.judge.VerdictBatch.Sole.Decided d) {
                raised = com.legend.lowering.VerdictSql.Raised.raised(String.valueOf(d.failure().message()), null,
                        null);
                base = List.of();
                raises = false;
            } else if (sole instanceof com.legend.judge.VerdictBatch.Sole.Row r && r.on().equals(env.target())) {
                var one = new com.legend.compiler.element.type.Multiplicity.Bounded(1, 1);
                var schema = new com.legend.compiler.element.type.Type.RelationType(List.of(
                        new com.legend.compiler.element.type.Type.RelationType.Column("value",
                                com.legend.compiler.element.type.Type.Primitive.BOOLEAN, one)));
                base = List.of(JudgePlans.sideOf(JudgePlans.rowsPlan(
                        com.legend.lowering.VerdictSql.raisingRow(r.query()), schema, env)));
                raised = com.legend.lowering.VerdictSql.Raised.bound(TestPlan.Verdict.RAISED_MESSAGE,
                        TestPlan.Verdict.RAISED_LINE, TestPlan.Verdict.RAISED_COLUMN);
                raises = true;
            } else {
                return null;
            }
        } else if (ae.caught() instanceof com.legend.testexecutionplan.Failure caught) {
            // the error f raised while it was planned, as the plan's data: no position (planning sends no statement,
            // so no database's raise carries one)
            raised = com.legend.lowering.VerdictSql.Raised.raised(String.valueOf(caught.message()), null, null);
            base = List.of();
            raises = false;
        } else {
            raised = com.legend.lowering.VerdictSql.Raised.bound(TestPlan.Verdict.RAISED_MESSAGE,
                    TestPlan.Verdict.RAISED_LINE, TestPlan.Verdict.RAISED_COLUMN);
            base = step.verdict().base();
            raises = true;
        }
        ExecutionPlan.Node wrapper = JudgePlans.rowsPlan(com.legend.lowering.VerdictSql.assertError(ae.expected(),
                ae.line(), ae.column(), raised), com.legend.lowering.VerdictSql.schema(), env);
        // the host's verdict rebuilt as the raising one: the same verdict, so the same id
        TestPlan.Verdict v = new TestPlan.Verdict(step.verdict().id(), step.verdict().name(),
                step.verdict().arm(), base, wrapper,
                null, raises).withHost(step.verdict().host());
        return new TestPlan.Verdicts(List.of(v), null, host.savepoint(), host.rules());
    }

    /** {@link #errorVerdict} for the agreement judge (S135): the assertError {@code statement} (after {@code lets})
     *  the host planner planned as {@code step}, as the database judges it; null where it holds no comparison. */
    static TestPlan.@Nullable Verdicts errorVerdictFor(HostAsserts.Planned step, TypedSpec statement,
            List<TypedSpec> lets, SpecCompiler specs, BodyDriver.ExecEnv env) {
        TestPlan.Verdicts raising = errorVerdict(step, env, () -> innerSole(statement, lets, Map.of(), specs, env));
        if (raising == null) {
            return null;
        }
        // both halves are steps of one plan under the agreement judge (the host's own step where the function is an
        // assert, then this one): the raising verdict is a verdict of its own, its own id -- the database judge's
        // alone keeps the host's, the step it replaces (the slice 1 audit's id fix, 2026-11-08: identity had told
        // the two apart)
        TestPlan.Verdict r = raising.verdicts().get(0);
        return new TestPlan.Verdicts(List.of(new TestPlan.Verdict(env.verdictIds().next(), r.name(), r.arm(), r.base(),
                r.wrapper(), r.message(), r.raises()).withHost(step.verdict().host())), raising.fused(),
                raising.savepoint(), raising.rules());
    }

    /** The assert {@code statement}'s assertError holds as its function ({@code assertError(|assertEquals(...), m)}),
     *  planned by the database judge's arm alone in a batch of its own: its one verdict row, or its failure decided at
     *  planning; null where the arm plans it as no one row. */
    private static com.legend.judge.VerdictBatch.@Nullable Sole innerSole(TypedSpec statement, List<TypedSpec> letPrefix,
            Map<String, BodyDriver.ExecFrame> frames, SpecCompiler specs, BodyDriver.ExecEnv env) {
        if (!(Lets.bare(statement) instanceof com.legend.compiler.spec.typed.TypedNativeCall ae)) {
            return null;
        }
        TypedSpec inner = QueryDoor.innerAssert(ae, specs);
        if (inner == null) {
            return null;
        }
        com.legend.judge.VerdictBatch batch = BodyDriver.newVerdictBatch(env);
        BodyDriver.ExecEnv benv = env.withVerdictBatch(batch);
        BodyDriver.ExecEnv at = BodyDriver.frameReplaceEnv(inner, frames, benv, letPrefix, specs);
        AssertVerdicts.SpliceHook hook = BodyDriver.spliceHook(frames, letPrefix, specs, benv)::apply;
        if (AssertVerdicts.planDatabase(batch, inner, letPrefix, specs, at, hook) != AssertVerdicts.DatabasePlan.PLANNED) {
            return null;
        }
        return batch.sole();
    }

    /** The steps of this body's {@code statements} statements, in order (an absorbed assert's none). */
    List<TestPlan.TestStep> steps(int statements) {
        List<TestPlan.TestStep> out = new ArrayList<>();
        for (int i = 0; i < statements; i++) {
            List<? extends TestPlan.TestStep> at = at(i);
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
                    } else if (kind == LetPlanner.Kind.STAGED_GRID) {
                        // a raw grid staged across a later effect: read from the table the test door stages it
                        // into at its turn (PlanMaker.stagedGridLet; the walk deletion, 2026-11-06)
                        letPrefix.add(com.legend.resolver.StagedRows.gridBoundTo(let.name(),
                                com.legend.planner.PlanMaker.gridRead(
                                        com.legend.resolver.StagedRows.gridTable(let.name(), i), pre.dialect()),
                                (com.legend.compiler.spec.typed.TypedRawSqlRelation) LetPlanner.rhs(let),
                                let.info()));
                    } else {
                        PlanAllocations.registerHandlesIn(let.name(), LetPlanner.rhs(let), letPrefix, specs,
                                pre.plan());
                        letPrefix.add(let);
                    }
                } catch (RuntimeException e) {
                    // the let raises: the walk raises it at the let's turn, after the earlier segments ran and
                    // reported (the M7 audit's F4) -- the walk's body
                    com.legend.diagnostics.Diagnostics.judgeDecline("static", "let " + let.name(),
                            "the let raises: " + e);
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
                com.legend.diagnostics.Diagnostics.judgeDecline("static", "statement " + (i + 1),
                        "the frame environment raises: " + e);
                return false;
            }
            try {
                AssertVerdicts.DatabaseGate gate = AssertVerdicts.databaseGate(bare, letPrefix, specs, at, hook);
                if (gate == AssertVerdicts.DatabaseGate.UNPLANNED) {
                    com.legend.diagnostics.Diagnostics.judgeDecline("static", "statement " + (i + 1),
                            "the gate: " + AssertVerdicts.shapeOf(bare));
                    return false;
                }
                if (AssertVerdicts.engineText(gate) && systemRouted
                        && !AssertVerdicts.engineTextDecidedEqual(bare, letPrefix, specs, at, hook)) {
                    // an engine-text assert in a system-routed body: its appeal reads the user store on the body's
                    // session where the walk runs it, not this program's (M10b)
                    com.legend.diagnostics.Diagnostics.judgeDecline("static", "statement " + (i + 1),
                            "an engine-text assert in a system-routed body");
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
                    com.legend.diagnostics.Diagnostics.judgeDecline("static", "statement " + (i + 1),
                            "a frame's activity read in a later segment than its own: " + f.getKey());
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
    record Each(VerdictBatch.Planned planned, @Nullable TestPlan.Verdicts group) {
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
            com.legend.diagnostics.Diagnostics.judgeDecline("unplanned", "", "agreement: the body reaches a lineage"
                    + " scan");
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
                    } else if (kind == LetPlanner.Kind.STAGED_GRID) {
                        // a raw grid staged across a later effect: read from the table the test door stages it
                        // into at its turn (PlanMaker.stagedGridLet; the walk deletion, 2026-11-06)
                        letPrefix.add(com.legend.resolver.StagedRows.gridBoundTo(let.name(),
                                com.legend.planner.PlanMaker.gridRead(
                                        com.legend.resolver.StagedRows.gridTable(let.name(), i), benv.dialect()),
                                (com.legend.compiler.spec.typed.TypedRawSqlRelation) LetPlanner.rhs(let),
                                let.info()));
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
                    com.legend.diagnostics.Diagnostics.judgeDecline("unplanned", let.name(), "agreement: a let the"
                            + " database half could not build; every assert after it declined");
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
                                && !AssertVerdicts.engineTextDecidedEqual(bare, letPrefix, specs, at, hook)
                        ? AssertVerdicts.DatabasePlan.UNPLANNED
                        : AssertVerdicts.planDatabase(batch, bare, letPrefix, specs, at, hook);
            } catch (RuntimeException e) {
                // the frame environment, the gate's classification or the arm raised: the arm raises the same at its
                // turn, a wall at the root's place (a frame environment's raise is the host half's before its arm: the
                // agreement judge never asks this half there)
                batch.open(AssertVerdicts.listenerName(bare));
                batch.wall(com.legend.judge.Failures.of(e, "DatabaseAsserts.planEach: the frame environment, the gate or the arm"));
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
    private static boolean everyReadStampBound(java.util.Collection<List<TestPlan.Verdicts>> steps) {
        java.util.Set<String> bound = new java.util.HashSet<>();
        List<ExecutionPlan.Sql> sqls = new ArrayList<>();
        for (List<TestPlan.Verdicts> groups : steps) {
            for (TestPlan.Verdicts group : groups) {
                if (group.fused() != null) {
                    sqls.add(group.fused().sql());
                    if (group.fused().sql().stamp() != null) {
                        bound.add(group.fused().sql().stamp());
                    }
                }
                for (TestPlan.Verdict v : group.verdicts()) {
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
        return g.on().equals(env.target())
                || g.on().database() instanceof com.legend.executionplan.ExecutionPlan.Database.System;
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

    /** The segment's batch planned and its groups placed at the segment's last assert, its root loop on the last of
     *  them (the plan-as-data design's step 5, 2026-11-08: judged at run from the plan once that group's last verdict is
     *  in); false where the walk keeps the body: a group on another session than the body's, or a keyed member. */
    private static boolean close(VerdictBatch batch, String place, int segment, int lastAssert,
            BodyDriver.ExecEnv env, Map<Integer, List<TestPlan.Verdicts>> byStatement) {
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
            // verdict step with no statement carries the segment and its root loop, judged there, where an unspelled
            // member is refused at its own place in the dialect's own words before anything is sent -- the host
            // judge's raise for the same side (2026-11-03: the walk had kept such a body, so the database judge alone
            // planned 9 PCT bodies fewer than the host on DuckDB, and refused H2's 33 list operations)
            TestPlan.Verdict decided = new TestPlan.Verdict(env.verdictIds().next(), "decided@" + place + segment,
                    "decided",
                    List.of(), null);
            TestPlan.Verdicts step = new TestPlan.Verdicts(List.of(decided), null, null,
                    env.options().judgingRules()).withRoots(planned.loop());
            byStatement.put(lastAssert, List.of(step));
            return true;
        }
        List<TestPlan.Verdicts> steps = new ArrayList<>();
        for (VerdictBatch.Group g : planned.groups()) {
            if (!placed(g, env)) {
                return false;
            }
            steps.add(stamped(g.step(), stampOfSegment(place, segment)));
        }
        int last = steps.size() - 1;
        steps.set(last, steps.get(last).withRoots(planned.loop()));
        byStatement.put(lastAssert, steps);
        return true;
    }

    /** {@code step}'s fused statement stamped with {@code stamp}: the comment it is sent under, which the segment's
     *  frames' activity rows read as a parameter (the walk's reserved comment). */
    private static TestPlan.Verdicts stamped(TestPlan.Verdicts step, String stamp) {
        ExecutionPlan.TdsResult fused = step.fused();
        if (fused == null) {
            return step;
        }
        return new TestPlan.Verdicts(step.verdicts(),
                new ExecutionPlan.TdsResult(fused.shape(), fused.columns(), fused.sql().stamped(stamp)),
                step.savepoint(), step.rules(), step.roots());
    }

    /** The stamp parameter of the fused statement of segment {@code segment}: the stamp prefix (the program's, so the
     *  test door adopts it and the runner binds it), then the segment's place. */
    private static String stampOfSegment(String place, int segment) {
        return ProgramFrames.STAMP + ".fused" + place + segment;
    }

    @Override
    public @Nullable List<? extends TestPlan.TestStep> at(int statement) {
        TestPlan.Branch b = branches.get(statement);
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

    @Override
    public TestPlan.Judge judge() {
        return TestPlan.Judge.DATABASE;
    }

}
