// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.executionplan.ExecutionPlan;
import com.legend.planner.PlanMaker;
import com.legend.planner.PlannedStep;
import com.legend.platform.Feature;
import com.legend.testexecutionplan.TestPlan;

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

    /** {@code body}'s plan on {@code runtime}, under {@code features}. */
    public static TestPlan plan(TypedQuery body, String runtime, Set<Feature> features) {
        return planned(body, runtime, features).plan();
    }

    /** A statement the raw-SQL ledger records once it ran: the text the walk records (the referee's spelling) and
     *  whether it answered rows (M5: the referee mirrors the ledger to replay the golden SQL of later bodies). */
    public record Recorded(String text, boolean query) {
    }

    /** An effect step beside the plan: its statements' ledger texts in order, recorded once the step ran (a step
     *  that fails records nothing: its failure is the database's own error, P1, 2026-11-07). */
    public record Effect(List<Recorded> statements) {
        public Effect {
            statements = List.copyOf(statements);
        }
    }

    /** A body's plan and, beside it, its effect steps' segments and ledger texts and its seed steps' ledger texts
     *  (whether a seed answered rows is the run's to say), each by the step's place in the plan's steps (the test
     *  door's own index: the plan carries no referee text). A frame let's eager run is a step of the plan's own
     *  ({@link TestPlan.FrameRun}: the plan-as-data design's step 3, 2026-11-08). */
    public record Planned(TestPlan plan, Map<Integer, Effect> effects, Map<Integer, List<String>> seeds) {
        public Planned {
            effects = Collections.unmodifiableMap(new LinkedHashMap<>(effects));
            seeds = Collections.unmodifiableMap(new LinkedHashMap<>(seeds));
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

        /** The verdict steps for the statement-root assert at {@code statement} (0-based): one step (the host judge's
         *  verdict over its sides), several (the database judge's groups of the segment this assert closes: its keyed
         *  members' then its fused statement, per session), none (the assert is ABSORBED into a later statement's
         *  groups: the database judge defers every assert of a segment to the segment's last), one branch (an
         *  {@code if} whose sides hold asserts, each side's steps its judge's: the asserts-as-plan-nodes program, step
         *  4), or null (not planned: the body is refused by name). */
        @Nullable List<? extends TestPlan.TestStep> at(int statement);

        /** The judge the asserts were planned for: the body's plan names it (the host's where none is planned). */
        default TestPlan.Judge judge() {
            return TestPlan.Judge.HOST;
        }

        /** Whether a frame let's eager run is a step of the program (the host judge: Pure runs it at the let); the
         *  database judge runs no frame at its let -- its readers reference the frame's CTE inside the fused statement
         *  (the walk's one statement per body) -- so its planner answers false and the test door places no check. */
        default boolean runsFrames() {
            return true;
        }

        /** The execution-trace stamp parameter the asserts' planner minted for the frame let at {@code statement}
         *  (its sides read the frame's activity comment through it: M9c), or null where it minted none; the frame's
         *  eager run then binds that name ({@code ProgramFrames.adoptStamp}) instead of one of the program's own. */
        default @Nullable String frameStamp(int statement) {
            return null;
        }
    }

    /** {@link #plan} with the effects' and seeds' ledger texts beside it; no assert planned. */
    public static Planned planned(TypedQuery body, String runtime, Set<Feature> features) {
        return planned(body, runtime, features, Asserts.NONE);
    }

    /** {@link #planned(TypedQuery, String, Set)} with the body's asserts placed as {@code asserts} planned them. */
    public static Planned planned(TypedQuery body, String runtime, Set<Feature> features, Asserts asserts) {
        return planned(body, runtime, features, asserts, false);
    }

    /** {@link #planned(TypedQuery, String, Set, Asserts)}; {@code systemRouted}: the body reads the system
     *  metamodel store and nothing else ({@code BodyClass.systemRouted}), so its program's target is the graph's
     *  SYSTEM database on the runtime's engine ({@link ExecutionPlan.Database.System}; the test-is-the-product plan's
     *  M10a), which the caller's sessions source opens. */
    public static Planned planned(TypedQuery body, String runtime, Set<Feature> features, Asserts asserts,
            boolean systemRouted) {
        return planned(body, runtime, features, asserts, systemRouted, null);
    }

    /** {@link #planned(TypedQuery, String, Set, Asserts, boolean)} with the run's test-input resource resolver (a
     *  resource's path to its local file), which a CSV load's statement names the file through (2026-11-06). */
    public static Planned planned(TypedQuery body, String runtime, Set<Feature> features, Asserts asserts,
            boolean systemRouted,
            java.util.function.@Nullable Function<String, java.nio.file.Path> resources) {
        TestKinds kinds = new TestKinds(body, runtime, asserts, resources);
        PlanMaker.TestProgram program = PlanMaker.testProgram(body, runtime, features, kinds, systemRouted);
        List<PlannedStep> planned = program.steps();
        for (int place : kinds.frameRuns()) {
            if (place >= planned.size() || !(planned.get(place) instanceof PlannedStep.Product p
                    && p.node() instanceof ExecutionPlan.Check)) {
                throw new IllegalStateException("the frame run at place " + place + " of the test door's plan is not"
                        + " a check of its steps");
            }
        }
        // each step the planner holds as the test plan holds it: a product node, a frame let's eager run (M9: sent under
        // its own origin, LET -- a fact of the plan), or the verdict step the library placed under its handle
        List<TestPlan.TestStep> steps = new java.util.ArrayList<>(planned.size());
        for (int i = 0; i < planned.size(); i++) {
            boolean frameRun = kinds.frameRuns().contains(i);
            steps.add(switch (planned.get(i)) {
                case PlannedStep.Product p when frameRun ->
                        new TestPlan.FrameRun((ExecutionPlan.Check) p.node());
                case PlannedStep.Product p -> new TestPlan.Run(p.node());
                case PlannedStep.Extension e -> kinds.extension(e.handle());
            });
        }
        Map<Integer, Effect> effects = byStep(planned, kinds.effects());
        Map<Integer, List<String>> seeds = byStep(planned, kinds.seeds());
        return new Planned(new TestPlan(program.parameters(), steps, asserts.judge()), effects, seeds);
    }

    /** {@code beside}, keyed by the step it rides, re-keyed by the step's place in {@code steps}; a step the library
     *  planned that is not one of them is refused by name (its texts would never reach the ledger). */
    private static <T> Map<Integer, T> byStep(List<PlannedStep> steps, Map<ExecutionPlan.Node, T> beside) {
        Map<Integer, T> byIndex = new LinkedHashMap<>();
        for (int i = 0; i < steps.size(); i++) {
            T texts = steps.get(i) instanceof PlannedStep.Product p ? beside.get(p.node()) : null;
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
