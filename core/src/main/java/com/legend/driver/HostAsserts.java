// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.Lets;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.exec.Sessions;
import com.legend.executionplan.ExecutionPlan;
import com.legend.planner.LetPlanner;
import com.legend.planner.PlanAllocations;
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
 * THE HOST JUDGE'S ASSERTS, PLANNED BEFORE THE PROGRAM IS (the test-is-the-product plan's M6, brief B3): each
 * statement-root assert of an assert program ({@code BodyClass.ASSERT_PROGRAM}) planned as the walk plans it --
 * classified ({@link AssertVerdicts#planHost}), its sides planned on the body's environment with the lets before it
 * ({@link HostSides#plan}) -- as one verdict step the test door places at the assert's turn, its judgment beside it
 * here, never in the plan. Planning sends nothing and counts nothing; a body holding an assert of a shape the host
 * checks do not plan yet has none ({@link #plan} answers null) and stays on the walk.
 *
 * <p>Where the walk would raise at a statement's turn, the assert is planned as that raise at its turn, after the
 * asserts before it ran and reported (R14's planned raise, the walk's behaviour): a classification or a side's
 * planning that raises inside the arm, counted as the walk counts it ({@link AssertVerdicts.HostAssert#raising}); a
 * let's handle registration or the assert's frame environment that raises BEFORE the arm, counted by nothing
 * ({@link AssertVerdicts.HostAssert#raisingBeforeTheArm}; M6's audit, S1). A let whose registration raised and no
 * assert after it to raise at: the body fails at planning, as a moved body the test door refuses does (the plan's §9,
 * the M5 audit's S6).
 */
final class HostAsserts implements TestDoor.Asserts {

    /** One assert planned: its classification and judgment, its sides' plans (what shaping each needs), and its
     *  verdict step. */
    record Planned(AssertVerdicts.HostAssert assertion, HostSides.Planned sides, ExecutionPlan.Verdicts step) {

        ExecutionPlan.Verdict verdict() {
            return step.verdicts().get(0);
        }
    }

    /** A branch planned (the asserts-as-plan-nodes program, step 4): its plan node, its condition as the host's
     *  planner planned it, each side's asserts as this planner planned them over the side's body, the branch's parts
     *  and the lets before it (the prefix both sides were planned over). */
    record Branched(ExecutionPlan.Branch node, ExecutionPlan.Side condition, HostAsserts then, HostAsserts otherwise,
            AssertBranch.Parts parts, List<TypedSpec> lets) {
        Branched {
            lets = List.copyOf(lets);
        }
    }

    /** The planned asserts by the statement's place in the body. */
    private final Map<Integer, Planned> byStatement;
    /** The branches planned, by the statement's place. */
    private final Map<Integer, Branched> branches;
    /** The same, by the verdict the runner hands back (a record compares by value: two asserts alike are two
     *  verdicts). */
    private final Map<ExecutionPlan.Verdict, Planned> byVerdict;
    /** The stamp parameter minted for each frame let, by the let's place (M9c): the comment the let's eager run
     *  binds and the sides reading the frame's activities carry. */
    private final Map<Integer, String> frameStamps;

    private HostAsserts(Map<Integer, Planned> byStatement, Map<Integer, String> frameStamps,
            Map<Integer, Branched> branches) {
        this.byStatement = Collections.unmodifiableMap(new LinkedHashMap<>(byStatement));
        this.frameStamps = Collections.unmodifiableMap(new LinkedHashMap<>(frameStamps));
        this.branches = Collections.unmodifiableMap(new LinkedHashMap<>(branches));
        Map<ExecutionPlan.Verdict, Planned> verdicts = new IdentityHashMap<>();
        byStatement.values().forEach(p -> verdicts.put(p.verdict(), p));
        // a branch side's asserts are this body's: the runner hands their verdicts back as any other's
        branches.values().forEach(b -> {
            verdicts.putAll(b.then().byVerdict);
            verdicts.putAll(b.otherwise().byVerdict);
        });
        this.byVerdict = Collections.unmodifiableMap(verdicts);
    }

    /**
     * {@code typed}'s asserts planned on {@code env} (the body's environment, as the walk makes it), each with the
     * lets before it as the walk holds them -- every let of an assert program the program rides ({@code VALUE}): its
     * handles registered, then the let on the prefix. Null where an assert is of a shape the host checks do not plan
     * yet: the walk runs the body. A frame let ({@code let r = execute(...)}, M9) is built as the walk builds it
     * ({@code BodyDriver.buildFrame}) without its eager run -- the program's check step runs it at the let -- and the
     * asserts after it splice their reads of it as the walk does.
     */
    static @Nullable HostAsserts plan(TypedQuery typed, String runtime, BodyDriver.ExecEnv env) {
        return plan(typed, runtime, env, null);
    }

    /** {@link #plan(TypedQuery, String, BodyDriver.ExecEnv)}; {@code bodyEnv} non-null where the body is
     *  SYSTEM-ROUTED (M10a): {@code env}'s session is then the graph's system database, the program's one target, and
     *  {@code bodyEnv} the body's own environment, the walk's. An engine-text assert's ARM is planned on {@code bodyEnv}
     *  (M10c item 10): its appeal -- the rows leg over the user's tables -- closes over it and reads them on the body's
     *  session as the walk's does; its SIDES are planned on {@code env}, so each names the program's one target. The
     *  assert is planned first as any other and planned again so (the discarded pass sends and counts nothing; it
     *  writes the plan environment's run-scoped accumulators, as item 8's deferred side planned twice already does). */
    static @Nullable HostAsserts plan(TypedQuery typed, String runtime, BodyDriver.ExecEnv env,
            BodyDriver.@Nullable ExecEnv bodyEnv) {
        return plan(typed, runtime, env, bodyEnv, List.of(), Map.of());
    }

    /** {@link #plan(TypedQuery, String, BodyDriver.ExecEnv, BodyDriver.ExecEnv)} over a body whose statements follow
     *  {@code lets} and {@code knownFrames} (a branch's side: the lets and frames before the branch, the outer body's,
     *  already planned there). */
    private static @Nullable HostAsserts plan(TypedQuery typed, String runtime, BodyDriver.ExecEnv env,
            BodyDriver.@Nullable ExecEnv bodyEnv, List<TypedSpec> lets,
            Map<String, BodyDriver.ExecFrame> knownFrames) {
        SpecCompiler specs = typed.specs();
        QueryDoor.Scan scan = new QueryDoor.Scan(typed, runtime);
        List<TypedSpec> body = typed.body();
        List<TypedSpec> letPrefix = new ArrayList<>(lets);
        // the body's frames as the walk builds them (M9): a frame let's chain, not run here -- its eager run is the
        // program's check step at the let -- so an assert's side reading the frame is the walk's statement
        Map<String, BodyDriver.ExecFrame> frames = new LinkedHashMap<>(knownFrames);
        Map<Integer, Branched> branches = new LinkedHashMap<>();
        Map<com.legend.model.FunctionId, Boolean> effectMemo = new java.util.HashMap<>();
        Map<Integer, Planned> planned = new LinkedHashMap<>();
        Map<Integer, String> frameStamps = new LinkedHashMap<>();
        // a let whose handle registration raised: the walk raises it at the let's turn, after the asserts before it
        // ran and reported; the next assert raises it at its turn instead, the asserts after it never run (S1) --
        // where a frame or an effect stands between them the body stays on the walk (above)
        RuntimeException pendingLet = null;
        boolean raisedByAnAssert = false;
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            boolean last = i == body.size() - 1;
            EnumSet<QueryDoor.TestOnly> kinds = scan.kinds(statement);
            if (pendingLet != null && (kinds.contains(QueryDoor.TestOnly.EFFECT)
                    || kinds.contains(QueryDoor.TestOnly.LET_EXECUTE))) {
                // the raise is the next assert's, and nothing between may send: a frame's eager run or an effect
                // here would be a step the program runs before that assert, where the walk raised at the let and
                // sent nothing (the M9c audit's F3) -- the walk runs the body
                return null;
            }
            if (statement instanceof TypedLet let && !last) {
                if (pendingLet == null) {
                    try {
                        java.util.function.Predicate<String> relationFrame =
                                n -> frames.containsKey(n) && frames.get(n).relationRooted();
                        LetPlanner.Kind kind = LetPlanner.kind(let, body, i, specs, effectMemo, relationFrame);
                        if (kind == LetPlanner.Kind.FRAME) {
                            // the frame's activity comment is the stamp of its eager run, the program's check step at
                            // the let: a parameter named here, adopted by the test door for that step (M9c)
                            String stamp = stampOfFrameLet(i);
                            frameStamps.put(i, stamp);
                            frames.put(let.name(), BodyDriver.plannedFrame(
                                    (com.legend.compiler.spec.typed.TypedNativeCall) LetPlanner.rhs(let), letPrefix,
                                    specs, env, stamp));
                        } else if (kind == LetPlanner.Kind.ALIAS) {
                            // an alias of a relation frame: the same frame under the let's name (the walk's rule)
                            frames.put(let.name(), java.util.Objects.requireNonNull(frames.get(
                                    java.util.Objects.requireNonNull(LetPlanner.aliasOf(let.value(), relationFrame)))));
                        } else {
                            PlanAllocations.registerHandlesIn(let.name(), LetPlanner.rhs(let), letPrefix, specs,
                                    env.plan());
                            letPrefix.add(let);
                        }
                    } catch (RuntimeException e) {
                        pendingLet = e;
                    }
                }
                continue;
            }
            AssertBranch.Parts parts = AssertBranch.of(statement, last, scan, specs);
            if (parts != null) {
                // an assert under an if (step 4): a branch, each side's asserts planned as this body's are, over the
                // lets before it; a pending let's raise is the next assert's, which no side may be
                if (pendingLet != null || !frames.isEmpty()) {
                    // a side reading a frame: the database judge composes no frame into a side's statement yet, so
                    // neither judge plans the shape (refused by name, as the database judge refuses it)
                    return null;
                }
                Branched b = planBranch(typed, runtime, env, bodyEnv, parts, letPrefix, frames);
                if (b == null) {
                    return null;
                }
                branches.put(i, b);
                continue;
            }
            if (QueryDoor.assertErrorRoot(statement, specs)) {
                // assertError (the walker-deletion program, 2026-11-03): its function's body the step's side, the
                // error it raises the answer
                TypedSpec bare = Lets.bare(statement);
                Planned p = pendingLet != null
                        ? planned(AssertVerdicts.HostAssert.raisingBeforeTheArm(AssertVerdicts.listenerName(bare),
                                pendingLet), HostSides.Planned.NONE, env.options().judgingRules())
                        : planAssertError(statement, bare, letPrefix, frames, specs, env);
                if (p == null) {
                    return null;
                }
                raisedByAnAssert |= pendingLet != null;
                planned.put(i, p);
                continue;
            }
            if (!kinds.equals(EnumSet.of(QueryDoor.TestOnly.ASSERT)) || statement instanceof TypedLet) {
                continue;
            }
            TypedSpec bare = Lets.bare(statement);
            Planned p;
            if (pendingLet != null) {
                p = planned(AssertVerdicts.HostAssert.raisingBeforeTheArm(AssertVerdicts.listenerName(bare),
                        pendingLet), HostSides.Planned.NONE, env.options().judgingRules());
                raisedByAnAssert = true;
            } else {
                p = planAssert(statement, bare, letPrefix, frames, specs, env, env);
                if (p != null && bodyEnv != null && AssertVerdicts.holdsEngineText(p.assertion().check().judgment())) {
                    // an engine-text assert of a system-routed body: its arm planned again on the body's environment,
                    // the walk's -- its appeal reads the user's tables on the body's session -- its sides on the
                    // program's (they name the program's one target, the system database: M10c item 10). Sound because
                    // a system-routed body is an assert program: no frame, so no splice hook binds the two environments
                    if (!frames.isEmpty()) {
                        throw new IllegalStateException("a system-routed body with a frame let: its engine-text arm"
                                + " and its sides would be planned on two environments over one frame");
                    }
                    p = planAssert(statement, bare, letPrefix, frames, specs, bodyEnv, env);
                }
                if (p != null && !AssertVerdicts.oneTextOrigin(p.assertion().check().judgment())) {
                    // an unroll whose parts' texts carry different statement origins: one step cannot be sent under
                    // both (the M10c item 8 audit's finding 2)
                    com.legend.exec.Census.unplannedAssert("unrolled engine texts under two origins");
                    com.legend.diagnostics.Diagnostics.judgeDecline("host", AssertVerdicts.listenerName(bare),
                            "unrolled engine texts under two origins");
                    return null;
                }
                if (p == null) {
                    com.legend.diagnostics.Diagnostics.judgeDecline("host", AssertVerdicts.listenerName(bare),
                            "the host's arm planned none" + (bodyEnv != null ? " (system-routed)" : ""));
                    return null;
                }
            }
            planned.put(i, p);
        }
        if (pendingLet != null && !raisedByAnAssert) {
            throw pendingLet;
        }
        return new HostAsserts(planned, frameStamps, branches);
    }

    /** A branch statement planned: its condition a side (the lets before it), each side's statements a body of their
     *  own planned by this planner over the same lets and frames; null where a side holds an assert it does not plan.
     */
    private static @Nullable Branched planBranch(TypedQuery typed, String runtime, BodyDriver.ExecEnv env,
            BodyDriver.@Nullable ExecEnv bodyEnv, AssertBranch.Parts parts, List<TypedSpec> letPrefix,
            Map<String, BodyDriver.ExecFrame> frames) {
        ExecutionPlan.Side condition = JudgePlans.plannedSide(parts.condition(), letPrefix, typed.specs(), env, false,
                false, null).side();
        HostAsserts then = plan(AssertBranch.body(typed, parts.then()), runtime, env, bodyEnv, letPrefix, frames);
        HostAsserts otherwise = then == null ? null
                : plan(AssertBranch.body(typed, parts.otherwise()), runtime, env, bodyEnv, letPrefix, frames);
        if (then == null || otherwise == null) {
            return null;
        }
        ExecutionPlan.Branch node = new ExecutionPlan.Branch(condition, then.steps(parts.then().size()),
                otherwise.steps(parts.otherwise().size()));
        return new Branched(node, condition, then, otherwise, parts, letPrefix);
    }

    /** The steps of this body's {@code statements} statements, in order. */
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

    /** The stamp parameter of the frame let at {@code statement}: the stamp prefix (the program's, so the test door
     *  adopts it and the runner binds it), then the let's place -- a name no program-minted stamp spells. */
    private static String stampOfFrameLet(int statement) {
        return com.legend.planner.ProgramFrames.STAMP + ".let" + statement;
    }

    /** {@code statement}, a statement-root assert ({@code bare} its call), planned over the lets before it: its arm on
     *  {@code env} (the environment its judgment's appeal closes over), its sides on {@code program} (the program's
     *  environment: a planned side names its target, the program's one); null where the host checks do not plan its
     *  shape, or a side reads a store on another session than the program's. The two are one environment but for an
     *  engine-text assert of a system-routed body (M10c item 10). */
    private static @Nullable Planned planAssert(TypedSpec statement, TypedSpec bare, List<TypedSpec> letPrefix,
            Map<String, BodyDriver.ExecFrame> frames, SpecCompiler specs, BodyDriver.ExecEnv env,
            BodyDriver.ExecEnv program) {
        Sessions.Session target = program.session();
        // the lets before the assert, as they stood at its turn (its judgment reads them after the program ran)
        List<TypedSpec> before = new ArrayList<>(letPrefix);
        BodyDriver.ExecEnv at;
        AssertVerdicts.SpliceHook hook;
        try {
            at = BodyDriver.frameReplaceEnv(statement, frames, env, before, specs);
            // the hook on the body's environment, as the walk's (BodyCompiler hands it seg.env()): an inline frame it
            // builds takes no renames from the statement's frames (the M9c audit's F6)
            hook = BodyDriver.spliceHook(frames, before, specs, env)::apply;
        } catch (RuntimeException e) {
            // the frame environment raised: the walk raises it before the arm is entered (S1)
            return planned(AssertVerdicts.HostAssert.raisingBeforeTheArm(AssertVerdicts.listenerName(bare), e),
                    HostSides.Planned.NONE, env.options().judgingRules());
        }
        AssertVerdicts.HostAssert assertion;
        try {
            assertion = AssertVerdicts.planHost(bare, before, specs, at, hook);
        } catch (RuntimeException e) {
            // the classification raised before any family was named: raised at the assert's turn, counted as the
            // walk counts it (entered, raised)
            assertion = AssertVerdicts.HostAssert.raising(AssertVerdicts.listenerName(bare), null, e);
        }
        com.legend.builtin.NativeFn.Verdict declined = assertion != null ? null : com.legend.builtin.NativeFn.Verdict
                .of(com.legend.compiler.spec.typed.Calls.calleeIdOf(bare)).orElse(null);
        if (assertion == null && (declined == com.legend.builtin.NativeFn.Verdict.ASSERT_IS
                || declined == com.legend.builtin.NativeFn.Verdict.ASSERT_INSTANCE_OF)) {
            // assertIs / assertInstanceOf whose arm declines (no element identity, a computed type): the walk's generic
            // path lowers the assert as a statement, which raises (an assert native is no scalar: NoRule) -- the same
            // lowering here, its raise the assert's at its turn (2026-11-04, the core program). Any other assert no arm
            // plans (an engine text the walk's own arms read) stays the walk's
            try {
                HostSides.plan(AssertVerdicts.listenerName(bare), List.of(HostJudge.statementAsk(bare, hook)), before,
                        specs, at);
            } catch (IllegalStateException | com.legend.error.NotImplementedException e) {
                // the lowering's refusal of an assert native as a scalar (NoRule: "not as a scalar")
                return planned(AssertVerdicts.HostAssert.raising(AssertVerdicts.listenerName(bare), null, e),
                        HostSides.Planned.NONE, env.options().judgingRules());
            }
            return null;
        }
        if (assertion == null) {
            return null;
        }
        HostSides.Planned sides;
        try {
            BodyDriver.ExecEnv sidesAt = program == env ? at
                    : BodyDriver.frameReplaceEnv(statement, frames, program, before, specs);
            HostSides.PlannedCheck pc = HostSides.planChecked(assertion.name(), assertion.check(), before, specs,
                    sidesAt);
            sides = pc.planned();
            if (pc.check() != assertion.check()) {
                // a caller's message the planner cannot plan: raised where the assert fails (HostSides.planChecked)
                assertion = new AssertVerdicts.HostAssert(assertion.listenerName(), assertion.family(),
                        assertion.wantEqual(), pc.check(), assertion.beforeTheArm());
            }
        } catch (RuntimeException e) {
            // a side's planning raised: raised at the assert's turn, once its family is counted (the walk plans the
            // sides there, after the asserts before it ran)
            assertion = AssertVerdicts.HostAssert.raising(assertion.listenerName(), assertion.family(), e);
            sides = HostSides.Planned.NONE;
        }
        if (sides.on() != null && sides.on() != target
                && SystemDatabase.of(program.ctx()).engineOf(sides.on()) == null) {
            // a side the walk's planner routed to another session than the body's (the system database's: a lineage
            // scan's rows join the metamodel tables, an element's row) -- the walk runs the group there; a moved
            // body's program has ONE target (the body's session, or the system database's when the body is
            // system-routed, M10a), so a body mixing the two stays on the walk, decided by the planned route before
            // any run (M9d)
            return null;
        }
        return planned(assertion, sides, env.options().judgingRules());
    }

    /**
     * {@code statement}, an {@code assertError(f, expected[, line, column])} root ({@code bare} its call), planned: f's
     * lets after the body's lets before it, f's last statement its one side (never folded: the database evaluates it,
     * so its raise carries its position, as the walk's arm runs it), the judgment the arm's
     * ({@link HostJudgment.AssertError}). An error f raises while it is planned -- a deferred guard's, the arm's catch --
     * is the judgment's own; a shape the arm refuses, and an error no arm takes (a wall), raise at the assert's turn, as
     * the walk's arm raises them. The side is the one statement whose failure the program survives: its step carries
     * the dialect's savepoint, which the runner sends only inside a transaction (F11), so the expected error leaves the
     * transaction live for the steps after it. A database that aborts the transaction and has no savepoint (DuckDB,
     * as F11's rung) fails the steps after it loudly -- a named limit: a program that writes a store and then expects
     * an error. Null where f holds a statement before its last that is no let, or its side is on a session the body
     * cannot reach: the walk.
     */
    static @Nullable Planned planAssertError(TypedSpec statement, TypedSpec bare, List<TypedSpec> letPrefix,
            Map<String, BodyDriver.ExecFrame> frames, SpecCompiler specs, BodyDriver.ExecEnv env) {
        String name = AssertVerdicts.listenerName(bare);
        if (!(bare instanceof com.legend.compiler.spec.typed.TypedNativeCall ae)) {
            return null;
        }
        ExecutionPlan.Verdicts.@Nullable Savepoint savepoint = savepointOf(env);
        AssertErrorNative.Args args;
        try {
            args = AssertErrorNative.args(ae, letPrefix);
        } catch (com.legend.error.NotImplementedException e) {
            // a shape the arm refuses: raised at the assert's turn, as the walk's arm raises it
            return planned(AssertVerdicts.HostAssert.raising(name, null, e), HostSides.Planned.NONE, env.options().judgingRules());
        }
        com.legend.compiler.spec.typed.TypedSpec inner = QueryDoor.innerAssert(ae, specs);
        if (inner != null) {
            // f IS one assert (the asserts-as-plan-nodes program, 2026-11-04): planned as any assert, its sides the
            // step's; its failure is the error assertError matches (AssertErrorNative.judge)
            Planned p = planAssert(inner, inner, letPrefix, frames, specs, env, env);
            if (p == null) {
                return null;
            }
            AssertVerdicts.HostAssert assertion = new AssertVerdicts.HostAssert(name, null, true,
                    new HostSides.Check(p.assertion().check().asks(), new HostJudgment.AssertError(args.expected(),
                            args.line(), args.column(), null, p.assertion())));
            return planned(assertion, p.sides(), savepoint, env.options().judgingRules());
        }
        List<TypedSpec> body = args.f().body();
        if (body.isEmpty()) {
            return null;
        }
        // f's WHOLE body is the sub-plan (ruling N3): Pure runs each of f's lets at its turn, so a let that does work
        // is read as a side in f's order, before the last statement -- the first error any of them raises is the
        // answer; a let that does no work cannot raise (LetPlanner.doesNoWork). A read let is spliced where it is read
        // as well (S129).
        List<TypedSpec> before = new ArrayList<>(letPrefix);
        List<TypedSpec> working = new ArrayList<>();
        for (TypedSpec st : body.subList(0, body.size() - 1)) {
            if (!(st instanceof TypedLet let)) {
                return null;
            }
            TypedSpec rhs = LetPlanner.rhs(let);
            if (rhs instanceof com.legend.compiler.spec.typed.TypedNativeCall nc
                    && (com.legend.builtin.NativeFn.Handle.runsAFrame(nc.callee().id())
                            || com.legend.builtin.Pure.Lite.STAGE.equals(nc.callee().qualifiedName()))) {
                // a frame or a staged let inside f: not a side; the walk's (the door names it)
                return null;
            }
            if (!LetPlanner.doesNoWork(let.value(), before, env.ctx())) {
                working.add(let.value());
            }
            before.add(st);
        }
        working.add(Lets.bare(body.get(body.size() - 1)));
        BodyDriver.ExecEnv at;
        AssertVerdicts.SpliceHook hook;
        try {
            at = BodyDriver.frameReplaceEnv(statement, frames, env, before, specs);
            hook = BodyDriver.spliceHook(frames, before, specs, env)::apply;
        } catch (RuntimeException e) {
            return planned(AssertVerdicts.HostAssert.raisingBeforeTheArm(name, e), HostSides.Planned.NONE, env.options().judgingRules());
        }
        List<HostSides.Ask> asks = new ArrayList<>(working.size());
        for (TypedSpec side : working) {
            asks.add(new HostSides.Ask(side, false, false, hook));
        }
        HostSides.Planned sides;
        RuntimeException caught = null;
        try {
            sides = HostSides.plan(name, asks, before, specs, at);
        } catch (RuntimeException e) {
            caught = AssertErrorNative.caught(e);
            if (caught == null) {
                // no error the arm takes (a wall): raised at the assert's turn, loud, as the walk's arm raises it
                return planned(AssertVerdicts.HostAssert.raising(name, null, e), HostSides.Planned.NONE, env.options().judgingRules());
            }
            sides = HostSides.Planned.NONE;
        }
        if (sides.on() != null && sides.on() != env.session()
                && SystemDatabase.of(env.ctx()).engineOf(sides.on()) == null) {
            return null;
        }
        AssertVerdicts.HostAssert assertion = new AssertVerdicts.HostAssert(name, null, true,
                new HostSides.Check(caught == null ? asks : List.of(),
                        new HostJudgment.AssertError(args.expected(), args.line(), args.column(), caught)));
        return planned(assertion, sides, savepoint, env.options().judgingRules());
    }

    /** The dialect's savepoint texts for a statement whose failure the program survives, or null where it has none. */
    private static ExecutionPlan.Verdicts.@Nullable Savepoint savepointOf(BodyDriver.ExecEnv env) {
        com.legend.sql.dialect.SqlDialect.VerdictSavepoint s = env.dialect().verdictSavepoint();
        return s == null ? null : new ExecutionPlan.Verdicts.Savepoint(s.set(), s.rollbackTo(), s.release());
    }

    /** {@code assertion}'s verdict step over {@code sides}' base: one verdict, its wrapper none (the host judges). */
    private static Planned planned(AssertVerdicts.HostAssert assertion, HostSides.Planned sides, ExecutionPlan.JudgingRules rules) {
        return planned(assertion, sides, null, rules);
    }

    private static Planned planned(AssertVerdicts.HostAssert assertion, HostSides.Planned sides,
            ExecutionPlan.Verdicts.@Nullable Savepoint savepoint, ExecutionPlan.JudgingRules rules) {
        // a caller's message, the check's last side (HostJudge.messaged), is the verdict's MESSAGE: the runner never
        // reads it on its own, the judge asks for it only where the assert fails (upstream evaluates it only there)
        List<ExecutionPlan.Side> base = sides.base();
        ExecutionPlan.Side message = null;
        if (assertion.check().judgment() instanceof HostJudgment.Messaged m && m.unplanned() == null
                && base.size() == m.innerSides() + 1) {
            message = base.get(m.innerSides());
            base = base.subList(0, m.innerSides());
        }
        ExecutionPlan.Verdict verdict = new ExecutionPlan.Verdict(assertion.listenerName(), assertion.name(),
                base, null, message);
        return new Planned(assertion, sides, new ExecutionPlan.Verdicts(List.of(verdict), null, savepoint, rules));
    }

    @Override
    public @Nullable List<? extends ExecutionPlan.Node> at(int statement) {
        Branched b = branches.get(statement);
        if (b != null) {
            return List.of(b.node());
        }
        Planned p = byStatement.get(statement);
        return p == null ? null : List.of(p.step());
    }

    /** The branch planned at {@code statement}, or null. */
    @Nullable Branched branch(int statement) {
        return branches.get(statement);
    }

    /** The assert planned at {@code statement}, or null (no statement-root assert there). */
    @Nullable Planned planned(int statement) {
        return byStatement.get(statement);
    }

    @Override
    public @Nullable String frameStamp(int statement) {
        return frameStamps.get(statement);
    }

    /** The assert the runner handed {@code verdict} for. */
    Planned of(ExecutionPlan.Verdict verdict) {
        Planned p = find(verdict);
        if (p == null) {
            throw new IllegalStateException("a verdict step no assert of the body planned: " + verdict.name());
        }
        return p;
    }

    /** The assert planned for {@code verdict}, or null where this judge planned none for it (the database judge's). */
    @Nullable Planned find(ExecutionPlan.Verdict verdict) {
        return byVerdict.get(verdict);
    }
}
