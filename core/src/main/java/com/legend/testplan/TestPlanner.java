// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;


import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.diagnostics.Census;
import com.legend.testexecutionplan.TestPlan;
import com.legend.sql.dialect.SqlDialect;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

/**
 * THE TEST PLANNER (the plan-as-data design's slice 5, 2026-11-09: the planning half of the body router of the
 * test-is-the-product plan's R16, M5): a test body of a class the program road admits ({@link BodyClass}, a named rule
 * over its typed shape) PLANNED WHOLE into a test plan -- its asserts by its judge's planner, the whole by the test
 * door -- with no session opened and no statement sent (THE WALL's rule 2, {@code RunScope.plans}). A body no class
 * admits is refused by name (R14); there is no walk to fall back to (deleted 2026-11-06). The test door
 * ({@code TestExecution}) is its one caller; the plan's run is {@code testexec.TestRunner}'s.
 */
public final class TestPlanner {

    private TestPlanner() {
    }

    /** {@code typed} PLANNED whole for a session of its runtime's database (the door's {@code planBody}): the
     *  runtime's dialect chosen, an unread let made the statement it binds (the user's ruling, 2026-11-02), then
     *  {@link #plan(TypedQuery, String, SqlDialect, PlanningOptions)}. */
    public static TestPlan plan(TypedQuery typed, @Nullable String given, PlanningOptions options) {
        if (options.judgeMode() == PlanningOptions.JudgeMode.BOTH
                && options.judgingRules() == com.legend.testexecutionplan.TestPlan.JudgingRules.LITE) {
            // both judges ISOLATED under lite's own rules (the user's judge model, 2026-11-17): each plans the body
            // alone; the compatibility rules keep the fused agreement until PCT runs the host alone (step 4)
            return planIsolated(typed, given, options);
        }
        // the body's facts over the body as given, BEFORE any rewrite below (the generator's planning, the unread lets
        // made statements, the lift): the record the test door's facts state, carried by the plan (the C5-J audit's
        // SHOULD 1, 2026-11-15: until then stated over the rewritten body, so an unread let or a generator's effect
        // read differently at the door and in the plan)
        com.legend.testexecutionplan.BodyFacts facts = facts(typed);
        // none given: the one runtime the body's from()s name (A5, PlanMaker.runtimeOf)
        String runtimeFqn = com.legend.planner.PlanMaker.runtimeOf(typed, given);
        SqlDialect dialect = options.style().dialect(com.legend.Compiler.executesOn(typed.context(),
                runtimeFqn).type());
        BodyDriver.refusePlanHandlesOffTheEdge(typed, options);
        // a test-data generator the body binds planned first (B2d.2, 2026-11-11): its reads the values they are, so the
        // body is classed and planned as any other
        // its texts print a timestamp as the body is judged (L9, the user's ruling of 2026-11-12): lite's own rules the
        // clean form, the compatibility rules upstream's
        com.legend.testdatagen.TestDataPlanner.Printing printing = options.judgingRules()
                == com.legend.testexecutionplan.TestPlan.JudgingRules.COMPILED_PURE_COMPAT
                ? com.legend.testdatagen.TestDataPlanner.Printing.COMPAT
                : com.legend.testdatagen.TestDataPlanner.Printing.CLEAN;
        return plan(withUnreadLets(GeneratedTestData.planned(typed, dialect, runtimeFqn, printing,
                options.style())), runtimeFqn,
                dialect, options, facts);
    }

    /** {@code typed} with each unread let the statement it binds: it runs at its turn, on every road. */
    private static TypedQuery withUnreadLets(TypedQuery typed) {
        List<TypedSpec> body = typed.body();
        List<TypedSpec> program = com.legend.planner.LetPlanner.unreadAsStatements(body, typed.resolved(),
                typed.context());
        return program != body ? typed.withBody(program) : typed;
    }

    /**
     * BOTH JUDGES, ISOLATED (the user's judge model, 2026-11-17; (b')): the body planned by each judge alone, exactly as
     * that judge plans it by itself, the two plans held by one step at which the runner compares their verdicts and
     * messages. Neither plan sees the other: no host column in a database statement, no fused agreement. The planner's
     * answer under BOTH with lite's own rules.
     */
    static TestPlan planIsolated(TypedQuery typed, @Nullable String given, PlanningOptions options) {
        TestPlan host = plan(typed, given, options.withJudgeMode(PlanningOptions.JudgeMode.HOST));
        TestPlan database = plan(typed, given, options.withJudgeMode(PlanningOptions.JudgeMode.DATABASE));
        TestPlan both = new TestPlan(List.of(), List.of(new TestPlan.Isolated(host, database)),
                TestPlan.Judge.AGREEMENT, java.util.Map.of(), java.util.Map.of(), host.answersLastVerdict(),
                host.session(), host.facts());
        com.legend.testexecutionplan.TestPlanJson.requireRoundTrip(both);
        return both;
    }

    /**
     * {@code given} PLANNED WHOLE by the test door, its asserts by its judge's planner: lifted, classed, its environment
     * made -- with no run started, no session opened, no statement sent, and nothing told to anyone (hole 4,
     * 2026-11-08: planning is handed no listener and no referee, so what it decides reaches the caller only as the
     * plan's data, at its turn in the run). A body the program road does not plan is refused by name.
     */
    static TestPlan plan(TypedQuery given, @Nullable String runtimeFqn, SqlDialect dialect,
            PlanningOptions options, com.legend.testexecutionplan.BodyFacts facts) {
        PlanningOptions.JudgeMode mode = options.judgeMode();
        TypedQuery typed;
        BodyClass bodyClass;
        boolean systemRouted;
        HostAsserts host = null;
        DatabaseAsserts database = null;
        TestDoor.Asserts agreement = null;
        // the body's engine-text appeals as its arms planned them, by number: the plan carries those its verdicts name
        // (step 8b, 2026-11-08)
        EngineTextAppeals textAppeals = null;
        // THE WALL's rule 2 (2026-11-06): the body is PLANNED -- lifted, classed, its environment made, its asserts
        // planned -- with no run started, no session opened and no statement sent, each refused by name (until then
        // only the asserts' planning was scoped, and the system database's session was opened just before it)
        try (var planning = com.legend.base.RunScope.plans("a test body")) {
            typed = lifted(given, options);
            // every judge plans its asserts on the program road: the agreement judge too since the walker-deletion
            // program (2026-11-02; AgreementAsserts), so the classes are one judge's rules whatever the mode
            bodyClass = runtimeFqn == null ? null : BodyClass.of(typed, runtimeFqn, true);
            // an assert program's asserts, planned by its judge before the program is (M6 the host's, M7 the
            // database's): a body holding an assert of a shape the judge's planner does not plan yet has none, and is
            // refused by name below (the walk that ran it is deleted; a named rule over its shape:
            // AssertVerdicts.planHost; the database arms answering null)
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
                // named, never opened at planning: the run opens the catalog's session, and the program's seed step
                // registers the model's rows; the body written in the catalog's dialect (B4, 2026-11-11: DuckDB's for a
                // body on a server, whose engine-text appeals still read the user's tables on the body's own
                // environment, bodyEnv)
                env = on.onCatalogBody(route);
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
                switch (judgesNothing ? PlanningOptions.JudgeMode.HOST : mode) {
                    case HOST -> host = HostAsserts.plan(typed, runtime, on, systemRouted ? bodyEnv : null);
                    case DATABASE -> database = DatabaseAsserts.plan(typed, runtime, on, systemRouted ? bodyEnv : null);
                    // both judges over the DATABASE judge's real program, ONE road (E-5c, the user's ruling O5 of
                    // 2026-11-13; DE4, the orchestrator's ruling under the user's delegation of 2026-11-15): its fused
                    // statements, the host reading each single-value side as a typed column of them, and a root no
                    // fused row carries (decided at planning, keyed, the split rung) judged by the host on its own
                    // sides. A body the database judge plans no program for is refused by name below, as under the
                    // database judge: there is no second road.
                    case BOTH -> agreement = AgreementAsserts.planFused(typed, runtime, on,
                            systemRouted ? bodyEnv : null, systemRouted);
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
        try (var p = com.legend.base.RunScope.plans("a test body's program")) {
            plan = TestDoor.planned(typed, runtimeFqn, options.features(),
                    asserts == null ? TestDoor.Asserts.NONE : asserts, systemRouted, options.stores(),
                    options.edge(), options.style(), facts);
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

    /** The body's facts as data (C5-J item 2, 2026-11-15): the planner's {@link com.legend.planner.ProgramFacts}
     *  over the body as its caller gave it, before any rewrite -- a test-data generator's planning, an unread let made
     *  the statement it binds, an assert lifted -- what a test runner reads of a body. {@link #plan(TypedQuery,
     *  String, PlanningOptions)} states them first and the test plan carries that record
     *  ({@link com.legend.testexecutionplan.TestPlan#facts}), so a runner choosing the body's session before it is
     *  planned reads the same record here (the C5-J audit's SHOULD 1, 2026-11-15). */
    public static com.legend.testexecutionplan.BodyFacts facts(TypedQuery given) {
        // planning, refused inside a run on its own -- never by the typing it happens to start (the C5-J audit's nit)
        com.legend.base.RunScope.planning("state a test body's facts");
        com.legend.planner.ProgramFacts f = com.legend.planner.ProgramFacts.of(given.body(), given.specs());
        return new com.legend.testexecutionplan.BodyFacts(f.effects(), f.seedsInlineCsv(), f.verdicts(), f.shape());
    }

    /** {@code given} as every road sees it: the lift (the asserts-as-plan-nodes program, step 4: an assert in a value's
     *  position is a statement of its own at its place, true where it stood), and register S137 (the user's ruling,
     *  2026-11-05: a relation passed to assertSize, assertEmpty or assertNotEmpty is ONE value upstream -- lite's own
     *  rules refuse it, the compatibility rules count it as upstream does). */
    private static TypedQuery lifted(TypedQuery given, PlanningOptions options) {
        List<com.legend.compiler.spec.typed.TypedSpec> lifted = com.legend.compiler.spec.AssertLift.lift(given.body());
        if (options.judgingRules() == com.legend.testexecutionplan.TestPlan.JudgingRules.LITE) {
            com.legend.compiler.spec.RelationCountAsserts.refuse(lifted);
        } else {
            lifted = com.legend.compiler.spec.RelationCountAsserts.countedAsOne(lifted);
        }
        TypedQuery out = lifted == given.body() ? given : given.withBody(lifted);
        // the run's planning options reach the body's own statements (its lets, checks and answer, which
        // ProductStatements plans by the query's options) as they reach its asserts (the planner's env): the final
        // review of PR #81, 2026-11-09 -- DETERMINISTIC_AS_OF had reached only the asserts
        if (out.options().containsAll(options.options())) {
            return out;
        }
        java.util.Set<com.legend.platform.PlanOption> all = java.util.EnumSet.noneOf(com.legend.platform.PlanOption.class);
        all.addAll(out.options());
        all.addAll(options.options());
        return out.withOptions(all);
    }
}
