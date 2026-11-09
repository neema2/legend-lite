// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.exec.StatementOrigin;

import com.legend.Execution;
import com.legend.ExecuteOptions;
import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.diagnostics.Census;
import com.legend.testexecutionplan.TestPlan;
import com.legend.sql.dialect.SqlDialect;
import com.legend.testplan.BodyClass;
import com.legend.testplan.BodyKinds;
import com.legend.testplan.TestDoor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

/**
 * THE BODY ROUTER at the test door (the test-is-the-product plan's R16, brief B1, M5): a body of a class that moves
 * ({@link BodyClass}, a named rule over its typed shape) is planned WHOLE by the test door and run as one plan on its
 * session; every other body is walked by the body driver, until M10 moves the last class and deletes the walk and
 * this router. A moved body the test door refuses fails (R14), never falls back to the walk.
 */
public final class BodyRouter {

    private BodyRouter() {
    }

    /**
     * {@code given} PLANNED WHOLE by the test door, its asserts by its judge's planner: lifted, classed, its environment
     * made -- with no run started, no session opened, no statement sent, and nothing told to anyone (hole 4,
     * 2026-11-08: planning is handed no listener and no referee, so what it decides reaches the caller only as the
     * plan's data, at its turn in the run). A body the program road does not plan is refused by name.
     */
    public static TestPlan plan(TypedQuery given, @Nullable String runtimeFqn, SqlDialect dialect,
            ExecuteOptions options) {
        ExecuteOptions.JudgeMode mode = options.judgeMode();
        TypedQuery typed;
        BodyClass bodyClass;
        boolean systemRouted;
        HostAsserts host = null;
        DatabaseAsserts database = null;
        AgreementAsserts agreement = null;
        // the body's engine-text appeals as its arms planned them, by number: the plan carries those its verdicts name
        // (step 8b, 2026-11-08)
        EngineTextAppeals textAppeals = null;
        // THE WALL's rule 2 (2026-11-06): the body is PLANNED -- lifted, classed, its environment made, its asserts
        // planned -- with no run started, no session opened and no statement sent, each refused by name (until then
        // only the asserts' planning was scoped, and the system database's session was opened just before it)
        try (var planning = com.legend.exec.StatementOrigin.planning("a test body")) {
            typed = lifted(given, options);
            // every judge plans its asserts on the program road: the agreement judge too since the walker-deletion
            // program (2026-11-02; AgreementAsserts), so the classes are one judge's rules whatever the mode
            bodyClass = runtimeFqn == null ? null : BodyClass.of(typed, runtimeFqn, true);
            // an assert program's asserts, planned by its judge before the program is (M6 the host's, M7 the
            // database's): a body holding an assert of a shape the judge's planner does not plan yet has none, and the
            // walk runs it (a named rule over its shape: AssertVerdicts.planHost; the database arms answering null)
            boolean assertsPlanned = bodyClass == BodyClass.ASSERT_PROGRAM || bodyClass == BodyClass.FRAME_PROGRAM;
            // a SYSTEM-ROUTED assert program (M10a): every store it reads is the graph's system metamodel store, so
            // its asserts are planned on the system database's TARGET and its program's target is that database, whose
            // session the run's sessions source opens -- one per graph, which the plan's own seed step writes
            // (SystemRoutes.seeds; nothing is sent before the plan runs, and planning names no session: I3)
            systemRouted = bodyClass == BodyClass.ASSERT_PROGRAM
                    && BodyClass.systemRouted(typed, java.util.Objects.requireNonNull(runtimeFqn));
            BodyDriver.ExecEnv bodyEnv = runtimeFqn == null ? null
                    : BodyDriver.bodyEnv(typed, runtimeFqn, dialect, options);
            BodyDriver.ExecEnv env = bodyEnv;
            textAppeals = bodyEnv == null ? null : bodyEnv.appeals();
            if (systemRouted) {
                BodyDriver.ExecEnv on = java.util.Objects.requireNonNull(bodyEnv);
                com.legend.planner.SystemRoutes.Route route = BodyDriver.systemRouteOf(typed.context(),
                        java.util.Objects.requireNonNull(runtimeFqn));
                // named, never opened at planning: the run opens the system database's session, and the program's
                // seed step writes the store (a server engine is refused by the class rule before this, so the tables
                // keep their names)
                env = on.withTarget(BodyDriver.systemTarget(route, dialect));
            }
            if (assertsPlanned) {
                // the body's own environment, the walk's: an engine-text assert of a system-routed body has its ARM
                // planned on it -- its appeal reads the user's tables on the body's session -- and its sides on the
                // program's (M10c item 10)
                BodyDriver.ExecEnv on = java.util.Objects.requireNonNull(env);
                String runtime = java.util.Objects.requireNonNull(runtimeFqn);
                // a frame program with no assert judges nothing (2026-11-04, the core program): its frames run and its
                // last statement answers as the host's planner plans them, whatever the judge -- the database judge's
                // planner composes frames into verdict statements and splices none into an answer (ExecuteFrameTest
                // under the database judge alone)
                boolean judgesNothing = bodyClass == BodyClass.FRAME_PROGRAM && !BodyClass.anAssert(typed, runtime);
                switch (judgesNothing ? ExecuteOptions.JudgeMode.HOST : mode) {
                    case HOST -> host = HostAsserts.plan(typed, runtime, on, systemRouted ? bodyEnv : null);
                    case DATABASE -> database = DatabaseAsserts.plan(typed, runtime, on, systemRouted ? bodyEnv : null);
                    // both judges in one plan: each assert's step carries the host's sides and the database's
                    // statement
                    case BOTH -> agreement = AgreementAsserts.plan(typed, runtime, on, systemRouted ? bodyEnv : null,
                            systemRouted);
                }
            }
        }
        boolean assertsPlanned = bodyClass == BodyClass.ASSERT_PROGRAM || bodyClass == BodyClass.FRAME_PROGRAM;
        TestDoor.Asserts asserts = host != null ? host : database != null ? database : agreement;
        if (runtimeFqn == null || bodyClass == null || assertsPlanned && asserts == null) {
            String queryRule = runtimeFqn == null || bodyClass != null ? null : BodyClass.queryRuleRefusal(typed, runtimeFqn);
            String why = (runtimeFqn == null ? "no runtime" : bodyClass == null ? "no class admits the body"
                    : mode.name().toLowerCase(java.util.Locale.ROOT) + " judge's planner refused an assert")
                    + (runtimeFqn == null ? "" : " [" + BodyKinds.of(typed, runtimeFqn, null) + "]")
                    + (queryRule != null ? " {query rule: " + queryRule + "}" : "");
            // the walk is deleted (2026-11-06): a body the program road does not plan is refused by name
            throw new com.legend.error.NotImplementedException("the program road does not plan this body: " + why);
        }
        // the census of the bodies the test door plans whole (a measurement: counted where the class admits the body)
        Census.inc(Census.Key.BODIES_MOVED);
        TestPlan plan;
        try (var p = com.legend.exec.StatementOrigin.planning("a test body's program")) {
            plan = TestDoor.planned(typed, runtimeFqn, options.features(),
                    asserts == null ? TestDoor.Asserts.NONE : asserts, systemRouted, options.resources(),
                    options.edge());
        }
        // the caller's sink for an answer a statement sends is asked for before anything is sent, as the walk asks
        // for it before its last statement runs (M5f's audit S1: a caller may refuse the shape); a constant's when the
        // program has run, as the walk answers a folded value at the body's end
        // G11 (the goals of record, 2026-11-07): every test body's plan is portable data -- it survives its JSON
        if (textAppeals != null) {
            // the appeals the plan's verdicts name, the plan's own data (the plan-as-data design's step 8b, 2026-11-08):
            // heard at their verdicts' steps, decided after the run from these records. Under lite's own rules the
            // verdicts name none, so the plan carries none; whatever the rules, a plan naming an appeal it does not
            // carry is refused (PlannedAppeals.requireCarried, the slice 2 invariant audit's D2)
            plan = plan.withExtensionsByVerdict(AppealsByVerdict.of(plan, textAppeals));
        }
        com.legend.testexecutionplan.TestPlanJson.requireRoundTrip(plan);
        if (com.legend.diagnostics.Diagnostics.on("decoded-plan")) {
            // guard 3 in report mode (docs/PLAN_AS_DATA_2026_11_07.md 2.10, from the design's step 3, 2026-11-08; hard
            // from step 9): the run reads the plan READ BACK from its JSON, never the planner's objects -- a lookup by
            // a planned object's identity, not its data, shows as a verdict that changes on the lane run so
            plan = com.legend.testexecutionplan.TestPlanJson.read(com.legend.testexecutionplan.TestPlanJson.write(plan));
        }
        if (com.legend.diagnostics.Diagnostics.on("plan-json")) {
            // the plan as data, printed whole (the plan-as-data design, 2026-11-07: what the run reads)
            System.out.println("[plan-json] " + com.legend.testexecutionplan.TestPlanJson.write(plan));
        }
        return plan;
    }

    /** {@code given} as every road sees it: the lift (the asserts-as-plan-nodes program, step 4: an assert in a value's
     *  position is a statement of its own at its place, true where it stood), and register S137 (the user's ruling,
     *  2026-11-05: a relation passed to assertSize, assertEmpty or assertNotEmpty is ONE value upstream -- lite's own
     *  rules refuse it, the compatibility rules count it as upstream does). */
    private static TypedQuery lifted(TypedQuery given, ExecuteOptions options) {
        List<com.legend.compiler.spec.typed.TypedSpec> lifted = com.legend.compiler.spec.AssertLift.lift(given.body());
        if (options.judgingRules() == com.legend.testexecutionplan.TestPlan.JudgingRules.LITE) {
            com.legend.compiler.spec.RelationCountAsserts.refuse(lifted);
        } else {
            lifted = com.legend.compiler.spec.RelationCountAsserts.countedAsOne(lifted);
        }
        return lifted == given.body() ? given : given.withBody(lifted);
    }

    /**
     * {@code given} PLANNED NOW, to run later on a session of the same database (zero exemptions, the owner,
     * 2026-11-05): a runner's fixture program planned with the assert whose compatibility appeal provides it, and run
     * where the appeal provides it -- inside the body's run, which plans nothing. A body that judges nothing (an effect
     * or query program, no assert) planned whole by the test door as {@link #plan} plans it; null for any other
     * (the walk's, an assert's, a system-routed body's), which the caller cannot plan ahead.
     */
    public static @Nullable Execution.PlannedProgram planned(TypedQuery given, @Nullable String runtimeFqn,
            SqlDialect dialect, ExecuteOptions options) {
        TypedQuery typed = lifted(given, options);
        BodyClass bodyClass = runtimeFqn == null ? null : BodyClass.of(typed, runtimeFqn, true);
        if (runtimeFqn == null || bodyClass == null || bodyClass == BodyClass.ASSERT_PROGRAM
                || BodyClass.anAssert(typed, runtimeFqn)) {
            return null;
        }
        // counted where it is planned, as a body is (plan; until hole 4 a program planned ahead counted when it ran)
        Census.inc(Census.Key.BODIES_MOVED);
        TestPlan planned;
        try (var p = com.legend.exec.StatementOrigin.planning("a program planned ahead")) {
            planned = TestDoor.planned(typed, runtimeFqn, options.features(), TestDoor.Asserts.NONE, false,
                    options.resources(), options.edge());
        }
        com.legend.testexecutionplan.TestPlanJson.requireRoundTrip(planned);
        TestPlan ahead = com.legend.diagnostics.Diagnostics.on("decoded-plan")
                // guard 3 in report mode on a program planned ahead too (the slice 1 audit, 2026-11-08)
                ? com.legend.testexecutionplan.TestPlanJson.read(com.legend.testexecutionplan.TestPlanJson.write(planned))
                : planned;
        return new Execution.PlannedProgram(ahead);
    }
}
