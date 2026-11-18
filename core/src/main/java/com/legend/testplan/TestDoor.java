// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.testexecutionplan.Edge;

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
        return planned(body, runtime, features);
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

    /** {@link #plan}, no assert planned and nothing for the edge. */
    public static TestPlan planned(TypedQuery body, String runtime, Set<Feature> features) {
        return planned(body, runtime, features, Asserts.NONE);
    }

    /** {@link #planned(TypedQuery, String, Set)} with the body's asserts placed as {@code asserts} planned them. */
    public static TestPlan planned(TypedQuery body, String runtime, Set<Feature> features, Asserts asserts) {
        return planned(body, runtime, features, asserts, false);
    }

    /** {@link #planned(TypedQuery, String, Set, Asserts)}; {@code systemRouted}: the body reads the system
     *  metamodel store and nothing else ({@code BodyClass.systemRouted}), so its program's target is the graph's
     *  SYSTEM database on the runtime's engine ({@link ExecutionPlan.Database.System}; the test-is-the-product plan's
     *  M10a), which the caller's sessions source opens. */
    public static TestPlan planned(TypedQuery body, String runtime, Set<Feature> features, Asserts asserts,
            boolean systemRouted) {
        return planned(body, runtime, features, asserts, systemRouted, com.legend.executionplan.DataStores.none(),
                com.legend.testexecutionplan.NoEdge.NONE,
                com.legend.database.PlanStyle.PRODUCT, TestPlanner.facts(body));
    }

    /** {@link #planned(TypedQuery, String, Set, Asserts, boolean)} with the stores a CSV load's file is read from at
     *  planning, for its reference's digest (docs/DATA_IN_THE_PLAN_2026_11_09.md L1, 2026-11-09), the program's
     *  statements written in {@code dialects}' dialects (the run's options': the product's own, or the edge's, D1.0). */
    public static TestPlan planned(TypedQuery body, String runtime, Set<Feature> features, Asserts asserts,
            boolean systemRouted,
            com.legend.executionplan.DataStores stores,
            com.legend.testexecutionplan.Edge edge,
            com.legend.database.PlanStyle style,
            com.legend.testexecutionplan.BodyFacts facts) {
        TestKinds kinds = new TestKinds(body, runtime, asserts, stores);
        PlanMaker.TestProgram program = PlanMaker.testProgram(body, runtime, features, kinds, systemRouted, style);
        List<PlannedStep> planned = program.steps();
        // each step the planner holds as the test plan holds it: a product node, a frame let's eager run (M9: sent under
        // its own origin, LET -- a fact of the plan, marked where it was planned), or the verdict step the library
        // placed under its handle
        List<TestPlan.TestStep> steps = new java.util.ArrayList<>(planned.size());
        for (int i = 0; i < planned.size(); i++) {
            steps.add(switch (planned.get(i)) {
                case PlannedStep.FrameRun f -> new TestPlan.FrameRun(f.check());
                case PlannedStep.Product p -> new TestPlan.Run(p.node());
                case PlannedStep.Extension e -> kinds.extension(e.handle());
            });
        }
        Map<Integer, Effect> effects = byStep(planned, kinds.effects());
        Map<Integer, java.util.function.Supplier<com.legend.setup.LoadSteps>> seeds = byStep(planned, kinds.seeds());
        // the ledger texts are the compatibility edge's to plan, by the step's index (the plan-as-data design's step 8,
        // 2.7: a recorder implies the compatibility rules); a run whose hook records no ledger (the product's NoEdge)
        // plans none
        Map<Integer, List<com.legend.testexecutionplan.Extension>> byStep = new LinkedHashMap<>();
        if (edge.records()) {
            unseededLoads(planned, seeds);
            effects.forEach((i, e) -> byStep.put(i, edge.effect(e.statements().stream().map(Recorded::text).toList(),
                    e.statements().stream().map(Recorded::query).toList())));
            seeds.forEach((i, copy) -> {
                com.legend.setup.LoadSteps referee = copy.get();
                byStep.put(i, edge.seed(referee.steps(), referee.content()));
            });
            // legend-pure's words of the raise sites this planning call's style recorded (K10 of the edge audit, the
            // orchestrator's ruling (A) of 2026-11-17), read ONCE, after the planning above, planned by the edge and
            // held at the step that sends their statement; a run whose hook records nothing judges by lite's rules,
            // whose words are its own, and plans none
            siteWordsByStep(steps, EngineTextGates.siteWords(style), byStep, edge);
        }
        // whether the body's last statement is an assert, whose verdict is then the body's answer: a fact of the plan
        // (the slice 2 invariant audit's hole 3, 2026-11-08)
        boolean answersLastVerdict = asserts.at(body.body().size() - 1) != null;
        // the session the body runs on: its runtime's database, the test door's own target (slice 5, 2026-11-09: the
        // door planned it beside the plan until then)
        // the body's facts, stated over the body as its caller gave it, before any rewrite (C5-J item 2, 2026-11-15;
        // the C5-J audit's SHOULD 1: the record the test door's facts state, TestPlanner.plan)
        TestPlan plan = new TestPlan(program.parameters(), steps, asserts.judge(), byStep, Map.of(), answersLastVerdict,
                PlanMaker.testSessionTarget(body.context(), runtime, style), facts, null,
                style.twoForm(com.legend.lowering.VerdictWords.class,
                        com.legend.lowering.LiteVerdictWords.INSTANCE).name());
        return plan.withAnswerSizeWords(answerSizeWords(plan, style));
    }

    /** Each of {@code words}' records (a statement text's raise sites' words) held at the first of {@code steps} that
     *  sends that text, beside the step's other records. A text no step sends is never run, so never raises: its
     *  records are not the plan's. */
    private static void siteWordsByStep(List<TestPlan.TestStep> steps,
            Map<String, List<com.legend.testexecutionplan.Extension>> words,
            Map<Integer, List<com.legend.testexecutionplan.Extension>> byStep,
            com.legend.testexecutionplan.Edge edge) {
        if (words.isEmpty()) {
            return;
        }
        java.util.Set<String> filed = new java.util.HashSet<>();
        for (int i = 0; i < steps.size(); i++) {
            List<String> texts = new java.util.ArrayList<>();
            switch (steps.get(i)) {
                case TestPlan.Run r -> textsOf(r.node(), texts);
                case TestPlan.FrameRun f -> textsOf(f.check(), texts);
                case TestPlan.Verdicts v -> TestPlan.statements(v).forEach(n -> textsOf(n, texts));
                case TestPlan.Branch b -> TestPlan.statements(b).forEach(n -> textsOf(n, texts));
                case TestPlan.Isolated p -> throw new IllegalStateException("an isolated pair is assembled after its two"
                        + " plans, each filed at its own door");
            }
            for (String text : texts) {
                List<com.legend.testexecutionplan.Extension> records = words.get(text);
                if (records != null && filed.add(text)) {
                    List<com.legend.testexecutionplan.Extension> at = new java.util.ArrayList<>(
                            byStep.getOrDefault(i, List.of()));
                    at.addAll(edge.siteWords(records));
                    byStep.put(i, at);
                }
            }
        }
    }

    /** The texts of the statements {@code node} sends, in order. */
    private static void textsOf(ExecutionPlan.Node node, List<String> into) {
        switch (node) {
            case ExecutionPlan.Sequence s -> s.steps().forEach(step -> textsOf(step, into));
            case ExecutionPlan.TdsResult t -> into.add(t.sql().statement());
            case ExecutionPlan.Keyed k -> {
                into.add(k.keys().statement());
                into.add(k.body().statement());
            }
            case ExecutionPlan.Update u -> into.add(u.sql().statement());
            case ExecutionPlan.Check c -> into.add(c.sql().statement());
            case ExecutionPlan.Effect e -> into.add(e.sql().statement());
            case ExecutionPlan.Load l -> {
                for (ExecutionPlan.LoadStep step : l.steps()) {
                    if (step instanceof ExecutionPlan.LoadStep.File f
                            && f.delivery() instanceof ExecutionPlan.LoadStep.Delivery.Reads reads) {
                        into.addAll(reads.statements());
                    }
                }
            }
            case ExecutionPlan.Constant c -> {
                // rows, no statement
            }
        }
    }

    /** The body's answer's refusal of no rows, worded at planning as {@code style} words it (C5's step E, 2026-11-17):
     *  where the plan sends a value's answer whose column requires a row ({@code TestRunner}'s walk contract), the
     *  side's words ({@link JudgePlans#sizeWords}); null otherwise. */
    private static @Nullable String answerSizeWords(TestPlan plan, com.legend.database.PlanStyle style) {
        if (!plan.answers() || !plan.sendsTheAnswer()
                || plan.answerShape() != com.legend.executionplan.ExecutionPlan.TdsResult.Shape.VALUE) {
            return null;
        }
        List<com.legend.executionplan.ExecutionPlan.Column> columns = com.legend.executionplan.ExecutionPlan
                .reading(plan.parameters(), plan.answer()).answerColumns();
        if (columns == null || columns.isEmpty() || columns.get(0).multiplicity().lower() < 1) {
            return null;
        }
        return JudgePlans.sizeWords(style, columns.get(0).multiplicity().text());
    }

    /**
     * Under the compatibility edge, every load step of the body's program is a seed the test planner planned texts for
     * ({@code TestKinds.seeds}), which the edge's ledger records; one with none is refused by name, since it would fail
     * the run at it. The graph's system store's load is no body's, and the ledger records none of it, as at the run.
     * (A {@code data:} payload's session table, recorded as nothing, went with the JSON pass, 2026-11-11: a payload is
     * bound or inline.)
     */
    private static void unseededLoads(List<PlannedStep> steps,
            Map<Integer, java.util.function.Supplier<com.legend.setup.LoadSteps>> seeds) {
        for (int i = 0; i < steps.size(); i++) {
            if (steps.get(i) instanceof PlannedStep.Product p && p.node() instanceof ExecutionPlan.Load u
                    && !(u.target().database() instanceof ExecutionPlan.Database.System) && !seeds.containsKey(i)) {
                throw new IllegalStateException("a load step of the test door's plan (" + u.identity() + ") is not a"
                        + " seed the edge's ledger records");
            }
        }
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
