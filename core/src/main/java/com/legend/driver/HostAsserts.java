// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.Lets;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedSpec;
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

    /** The planned asserts by the statement's place in the body. */
    private final Map<Integer, Planned> byStatement;
    /** The same, by the verdict the runner hands back (a record compares by value: two asserts alike are two
     *  verdicts). */
    private final Map<ExecutionPlan.Verdict, Planned> byVerdict;
    /** The stamp parameter minted for each frame let, by the let's place (M9c): the comment the let's eager run
     *  binds and the sides reading the frame's activities carry. */
    private final Map<Integer, String> frameStamps;

    private HostAsserts(Map<Integer, Planned> byStatement, Map<Integer, String> frameStamps) {
        this.byStatement = Collections.unmodifiableMap(new LinkedHashMap<>(byStatement));
        this.frameStamps = Collections.unmodifiableMap(new LinkedHashMap<>(frameStamps));
        Map<ExecutionPlan.Verdict, Planned> verdicts = new IdentityHashMap<>();
        byStatement.values().forEach(p -> verdicts.put(p.verdict(), p));
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
        return plan(typed, runtime, env, false);
    }

    /** {@link #plan(TypedQuery, String, BodyDriver.ExecEnv)}; {@code systemRouted}: {@code env}'s session is the
     *  graph's system database (M10a), where an engine-text assert's appeal -- the rows leg over the user store -- would
     *  read the wrong database: such a body is the walk's (M10b). */
    static @Nullable HostAsserts plan(TypedQuery typed, String runtime, BodyDriver.ExecEnv env, boolean systemRouted) {
        SpecCompiler specs = typed.specs();
        QueryDoor.Scan scan = new QueryDoor.Scan(typed, runtime);
        List<TypedSpec> body = typed.body();
        List<TypedSpec> letPrefix = new ArrayList<>();
        // the body's frames as the walk builds them (M9): a frame let's chain, not run here -- its eager run is the
        // program's check step at the let -- so an assert's side reading the frame is the walk's statement
        Map<String, BodyDriver.ExecFrame> frames = new LinkedHashMap<>();
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
            if (!kinds.equals(EnumSet.of(QueryDoor.TestOnly.ASSERT)) || statement instanceof TypedLet) {
                continue;
            }
            TypedSpec bare = Lets.bare(statement);
            Planned p;
            if (pendingLet != null) {
                p = planned(AssertVerdicts.HostAssert.raisingBeforeTheArm(AssertVerdicts.listenerName(bare),
                        pendingLet), HostSides.Planned.NONE);
                raisedByAnAssert = true;
            } else {
                p = planAssert(statement, bare, letPrefix, frames, specs, env);
                if (p == null || systemRouted && AssertVerdicts.holdsEngineText(p.assertion().check().judgment())) {
                    // an engine-text assert in a system-routed body: its appeal reads the user store on the body's
                    // session where the walk runs it, which is not this program's one session (M10b)
                    return null;
                }
            }
            planned.put(i, p);
        }
        if (pendingLet != null && !raisedByAnAssert) {
            throw pendingLet;
        }
        return new HostAsserts(planned, frameStamps);
    }

    /** The stamp parameter of the frame let at {@code statement}: the stamp prefix (the program's, so the test door
     *  adopts it and the runner binds it), then the let's place -- a name no program-minted stamp spells. */
    private static String stampOfFrameLet(int statement) {
        return com.legend.planner.ProgramFrames.STAMP + ".let" + statement;
    }

    /** {@code statement}, a statement-root assert ({@code bare} its call), planned over the lets before it; null where
     *  the host checks do not plan its shape. */
    private static @Nullable Planned planAssert(TypedSpec statement, TypedSpec bare, List<TypedSpec> letPrefix,
            Map<String, BodyDriver.ExecFrame> frames, SpecCompiler specs, BodyDriver.ExecEnv env) {
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
                    HostSides.Planned.NONE);
        }
        AssertVerdicts.HostAssert assertion;
        try {
            assertion = AssertVerdicts.planHost(bare, before, specs, at, hook);
        } catch (RuntimeException e) {
            // the classification raised before any family was named: raised at the assert's turn, counted as the
            // walk counts it (entered, raised)
            assertion = AssertVerdicts.HostAssert.raising(AssertVerdicts.listenerName(bare), null, e);
        }
        if (assertion == null) {
            return null;
        }
        HostSides.Planned sides;
        try {
            sides = HostSides.plan(assertion.name(), assertion.check().asks(), before, specs, at);
        } catch (RuntimeException e) {
            // a side's planning raised: raised at the assert's turn, once its family is counted (the walk plans the
            // sides there, after the asserts before it ran)
            assertion = AssertVerdicts.HostAssert.raising(assertion.listenerName(), assertion.family(), e);
            sides = HostSides.Planned.NONE;
        }
        if (sides.on() != null && sides.on() != env.session()) {
            // a side the walk's planner routed to another session than the body's (the system database's: a lineage
            // scan's rows join the metamodel tables, an element's row) -- the walk runs the group there; a moved
            // body's program has ONE target (the body's session, or the system database's when the body is
            // system-routed, M10a), so a body mixing the two stays on the walk, decided by the planned route before
            // any run (M9d)
            return null;
        }
        return planned(assertion, sides);
    }

    /** {@code assertion}'s verdict step over {@code sides}' base: one verdict, its wrapper none (the host judges). */
    private static Planned planned(AssertVerdicts.HostAssert assertion, HostSides.Planned sides) {
        ExecutionPlan.Verdict verdict = new ExecutionPlan.Verdict(assertion.listenerName(), assertion.name(),
                sides.base(), null);
        return new Planned(assertion, sides, new ExecutionPlan.Verdicts(List.of(verdict), null));
    }

    @Override
    public @Nullable List<ExecutionPlan.Verdicts> at(int statement) {
        Planned p = byStatement.get(statement);
        return p == null ? null : List.of(p.step());
    }

    @Override
    public @Nullable String frameStamp(int statement) {
        return frameStamps.get(statement);
    }

    /** The assert the runner handed {@code verdict} for. */
    Planned of(ExecutionPlan.Verdict verdict) {
        Planned p = byVerdict.get(verdict);
        if (p == null) {
            throw new IllegalStateException("a verdict step no assert of the body planned: " + verdict.name());
        }
        return p;
    }
}
