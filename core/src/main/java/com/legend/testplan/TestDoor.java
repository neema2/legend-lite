// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.executionplan.ExecutionPlan;
import com.legend.planner.PlanMaker;
import com.legend.platform.Feature;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * THE TEST DOOR (the test-is-the-product plan's M3, brief B12 items 1 and 3; B4 brought forward by R17 C7): a test
 * body planned whole as one program on the product's program road, whatever it holds -- the statements a product
 * query holds planned by the planner, the kinds only a test's program holds ({@code QueryDoor.TestOnly}) by this
 * library. The test-planning library is its own target ({@code //core:testplan}): nothing on the plan side
 * ({@code //core:plan_side}, which the WebAssembly and native planners carry) and no server class reaches it, so the
 * product's query door can plan none of a test's kinds (ArchitectureTest). Its one caller is the driver; until a body
 * class moves to it (M5, R16), its callers are its tests.
 */
public final class TestDoor {

    private TestDoor() {
    }

    /** {@code body}'s plan on {@code runtime}, under {@code features}, for a session whose server is
     *  {@code serverVersion} (null: the dialect's declared target). */
    public static ExecutionPlan plan(TypedQuery body, String runtime,
            Set<Feature> features, @Nullable String serverVersion) {
        return planned(body, runtime, features, serverVersion).plan();
    }

    /** A statement the raw-SQL ledger records once it ran: the text the walk records (the referee's spelling) and
     *  whether it answered rows (M5: the referee mirrors the ledger to replay the golden SQL of later bodies). */
    public record Recorded(String text, boolean query) {
    }

    /** An effect step beside the plan: the walk's name for its segment ({@link EffectSegment#where}), which a
     *  failure is named by, and its statements' ledger texts in order. */
    public record Effect(String where, List<Recorded> statements) {
        public Effect {
            statements = List.copyOf(statements);
        }
    }

    /** A body's plan and, beside it, its effect steps' segments and ledger texts and its seed steps' ledger texts
     *  (whether a seed answered rows is the run's to say), each by the step's place in the plan's sequence (the test
     *  door's own index: the product plan carries no referee text); and the places of its frame lets' eager runs
     *  (M9: a check the walk sends under its own origin, LET, which the plan does not say). */
    public record Planned(ExecutionPlan plan, Map<Integer, Effect> effects, Map<Integer, List<String>> seeds,
            Set<Integer> frameRuns) {
        public Planned {
            effects = Collections.unmodifiableMap(new LinkedHashMap<>(effects));
            seeds = Collections.unmodifiableMap(new LinkedHashMap<>(seeds));
            frameRuns = Collections.unmodifiableSet(new java.util.TreeSet<>(frameRuns));
        }
    }

    /**
     * A body's asserts, each planned by its judge before the program is (the test-is-the-product plan's M6, brief B3):
     * the verdict step for the statement at a body's place, or null where its judge planned none there. The judge's
     * comparison rides beside the plan with the judge (the driver's), never in it; the library places the step.
     */
    @FunctionalInterface
    public interface Asserts {
        /** No assert planned: an assert is refused by name. */
        Asserts NONE = statement -> null;

        /** The verdict step for the statement at {@code statement} (0-based), or null. */
        ExecutionPlan.@Nullable Verdicts at(int statement);

        /** The execution-trace stamp parameter the asserts' planner minted for the frame let at {@code statement}
         *  (its sides read the frame's activity comment through it: M9c), or null where it minted none; the frame's
         *  eager run then binds that name ({@code ProgramFrames.adoptStamp}) instead of one of the program's own. */
        default @Nullable String frameStamp(int statement) {
            return null;
        }
    }

    /** {@link #plan} with the effects' and seeds' ledger texts beside it; no assert planned. */
    public static Planned planned(TypedQuery body, String runtime, Set<Feature> features,
            @Nullable String serverVersion) {
        return planned(body, runtime, features, serverVersion, Asserts.NONE);
    }

    /** {@link #planned(TypedQuery, String, Set, String)} with the body's asserts placed as {@code asserts} planned
     *  them. */
    public static Planned planned(TypedQuery body, String runtime, Set<Feature> features,
            @Nullable String serverVersion, Asserts asserts) {
        TestKinds kinds = new TestKinds(body, runtime, asserts);
        ExecutionPlan plan = PlanMaker.testProgram(body, runtime, features, serverVersion, kinds);
        List<ExecutionPlan.Node> steps = plan.root() instanceof ExecutionPlan.Sequence s ? s.steps()
                : List.of(plan.root());
        Map<Integer, Effect> effects = byStep(steps, kinds.effects());
        Map<Integer, List<String>> seeds = byStep(steps, kinds.seeds());
        for (int place : kinds.frameRuns()) {
            if (place >= steps.size() || !(steps.get(place) instanceof ExecutionPlan.Check)) {
                throw new IllegalStateException("the frame run at place " + place + " of the test door's plan is not"
                        + " a check of its sequence");
            }
        }
        return new Planned(plan, effects, seeds, kinds.frameRuns());
    }

    /** {@code beside}, keyed by the step it rides, re-keyed by the step's place in {@code steps}; a step the library
     *  planned that is not one of them is refused by name (its texts would never reach the ledger). */
    private static <T> Map<Integer, T> byStep(List<ExecutionPlan.Node> steps, Map<ExecutionPlan.Node, T> beside) {
        Map<Integer, T> byIndex = new LinkedHashMap<>();
        for (int i = 0; i < steps.size(); i++) {
            T texts = beside.get(steps.get(i));
            if (texts != null) {
                byIndex.put(i, texts);
            }
        }
        if (byIndex.size() != beside.size()) {
            throw new IllegalStateException(beside.size() - byIndex.size() + " effect or seed step(s) of the test"
                    + " door's plan are not in its sequence: their ledger texts have no step to ride");
        }
        return byIndex;
    }
}
