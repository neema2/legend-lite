// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.testexecutionplan.TypedSide;

import java.util.List;

/**
 * THE TEST PLANNER, AS THE ENGINE-TEXT ARM READS IT (W9's judge half, S2c, the orchestrator's ruling of 2026-11-16:
 * one public facade, one architecture row): the engine-text assert's arm is the compatibility edge's
 * ({@code edge.SqlTextVerdicts}), and every read it makes of the test planner, the planner or the lowering goes
 * through here -- the environment's parts, a side planned for either judge, the replay facts, the verdict batch, the
 * plan's appeals. Each method forwards one call; nothing here decides or evaluates.
 * {@code ArchitectureTest.PLANNER_ENTRIES} lists each member the edge's arm reaches, by its full signature and for its
 * callers alone (one "#*" row until 2026-11-17, the GUARD stream's C.4); {@code OpenClassMembersTest} pins the whole
 * member set.
 */
@com.legend.base.OpenLine("C4") public final class EngineTextPlanning {

    private EngineTextPlanning() {
    }

    // ---- the environment ---------------------------------------------------------------------------------------

    public static ModelContext ctx(BodyDriver.ExecEnv env) {
        return env.ctx();
    }

    public static com.legend.executionplan.ExecutionPlan.Target target(BodyDriver.ExecEnv env) {
        return env.target();
    }

    public static com.legend.compiler.spec.typed.@com.legend.base.Nullable ExecutionContext frame(BodyDriver.ExecEnv env) {
        return env.frame();
    }

    public static BodyDriver.@com.legend.base.Nullable ExecEnv appealOn(BodyDriver.ExecEnv env) {
        return env.appealOn();
    }

    public static @com.legend.base.Nullable TextCollector collector(BodyDriver.ExecEnv env) {
        return env.engineText();
    }

    public static com.legend.database.PlanStyle style(BodyDriver.ExecEnv env) {
        return env.options().style();
    }

    public static java.util.Map<String, String> tableReplace(BodyDriver.ExecEnv env) {
        return env.postProcessors().tableReplace();
    }

    public static BodyDriver.ExecEnv withCollector(BodyDriver.ExecEnv env,
            @com.legend.base.Nullable TextCollector c) {
        return env.withCollector(c);
    }

    public static BodyDriver.ExecEnv withFrame(BodyDriver.ExecEnv env, com.legend.compiler.spec.typed.ExecutionContext f) {
        return env.withFrame(f);
    }

    /** {@code env} with its own copy of the body's consumed lets: what it plans reads them and leaves none behind. */
    public static BodyDriver.ExecEnv withOwnLets(BodyDriver.ExecEnv env) {
        return env.withPlan(env.plan().withOwnQueryLets());
    }

    public static BodyDriver.ExecEnv withTableReplace(BodyDriver.ExecEnv env, java.util.Map<String, String> tr) {
        return env.withTableReplace(tr);
    }

    /** The edge's record numbered in the body: {@code numbered} makes it, given its number (census C2, 2026-11-17:
     *  the product holds the edge's records by number, no appeal type). */
    public static int appeal(BodyDriver.ExecEnv env,
            java.util.function.IntFunction<com.legend.testexecutionplan.Extension> numbered) {
        return env.appeals().add(numbered);
    }

    public static @com.legend.base.Nullable String marking(BodyDriver.ExecEnv env, String origin, boolean ifUnmarked) {
        return EngineTextGates.marking(env, origin, ifUnmarked);
    }

    public static void restoreMarking(BodyDriver.ExecEnv env, @com.legend.base.Nullable String prior) {
        EngineTextGates.restoreMarking(env, prior);
    }

    // (the verdict batch's reads -- active, defer -- and the text equalities below: the edge's engine-text rows'
    // under the database judge, deleted with its appeal path, A4 of the compatibility program, stage A ratified by the user 2026-11-19)

    // ---- an assert's ask and check --------------------------------------------------------------------------------

    public static TypedSpec bare(TextAsk ask) {
        return ask.bare;
    }

    public static List<TypedSpec> letPrefix(TextAsk ask) {
        return ask.letPrefix;
    }

    public static SpecCompiler specs(TextAsk ask) {
        return ask.specs;
    }

    public static BodyDriver.ExecEnv env(TextAsk ask) {
        return ask.env;
    }

    public static AssertVerdicts.@com.legend.base.Nullable SpliceHook hook(TextAsk ask) {
        return ask.hook;
    }

    public static com.legend.testexecutionplan.HostJudgment judgment(HostSides.Check check) {
        return check.judgment();
    }

    public static TextPlanned planned(HostSides.Check check) {
        return new TextPlanned(check);
    }

    public static HostSides.Ask ask(TypedSpec arg, boolean identity, boolean fold,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook, BodyDriver.@com.legend.base.Nullable ExecEnv at) {
        return new HostSides.Ask(arg, identity, fold, hook, at);
    }

    /** An ask planned under the step's environment. */
    public static HostSides.Ask ask(TypedSpec arg, boolean identity, boolean fold,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        return new HostSides.Ask(arg, identity, fold, hook);
    }

    public static HostSides.Check check(List<HostSides.Ask> asks, com.legend.testexecutionplan.HostJudgment judgment) {
        return new HostSides.Check(asks, judgment);
    }

    // ---- a side planned for a judge ---------------------------------------------------------------------------------

    public static TypedSide evalSide(TypedSpec value, List<TypedSpec> letPrefix, SpecCompiler specs,
            BodyDriver.ExecEnv env, boolean identity, boolean fold,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        return JudgePlans.evalSide(value, letPrefix, specs, env, identity, fold, hook);
    }

    public static JudgePlans.PlannedSide plannedSide(TypedSpec value, List<TypedSpec> letPrefix, SpecCompiler specs,
            BodyDriver.ExecEnv env, boolean identity, boolean fold,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        return JudgePlans.plannedSide(value, letPrefix, specs, env, identity, fold, hook);
    }

    public static com.legend.testexecutionplan.TestPlan.Side side(JudgePlans.PlannedSide p) {
        return p.side();
    }

    /** Whether {@code p} is a side of values ({@code JudgePlans.Kind.VALUES}). */
    public static boolean values(JudgePlans.PlannedSide p) {
        return p.kind() == JudgePlans.Kind.VALUES;
    }

    public static com.legend.testexecutionplan.SideShape shapeOf(JudgePlans.PlannedSide p, com.legend.database.PlanStyle style) {
        return JudgePlans.shapeOf(p, style);
    }

    /** {@code side} planned as the database judge's rows (C4, 2026-11-17: forwarding alone; the edge reads them). */
    public static DatabaseJudge.SideRows planSide(TypedSpec side, List<TypedSpec> letPrefix, SpecCompiler specs,
            BodyDriver.ExecEnv at, AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        return DatabaseJudge.planSide(side, false, letPrefix, specs, at, hook);
    }

    public static @com.legend.base.Nullable String why(DatabaseJudge.SideRows rows) {
        return rows.why();
    }

    public static com.legend.sql.@com.legend.base.Nullable SqlQuery scalarRow(DatabaseJudge.SideRows rows) {
        return rows.scalarRow(false);
    }

    // ---- the replay facts ------------------------------------------------------------------------------------------

    // (replayFacts, noReplayGates and the gates' three readers: gone 2026-11-18, the edge audit's J40 -- the referee's
    // gates are the edge's own ReplayGates, computed from the typed chain there)

    /** {@code spec} through the body's splice hook. */
    public static TypedSpec splice(AssertVerdicts.SpliceHook hook, TypedSpec spec, java.util.Set<String> names) {
        return hook.apply(spec, names);
    }

    public static @com.legend.base.Nullable TypedSpec unpagedRead(TypedSpec read) {
        return AssertVerdicts.unpagedRead(read);
    }

    public static @com.legend.base.Nullable String calleeFqn(TypedSpec bare) {
        return AssertVerdicts.calleeFqn(bare);
    }

    public static com.legend.testexecutionplan.EnumFacts enumsOf(ModelContext ctx,
            @com.legend.base.Nullable String mappingFqn, @com.legend.base.Nullable String classFqn,
            com.legend.testexecutionplan.@com.legend.base.Nullable SideShape shape) {
        return JudgmentFacts.enumsOf(ctx, mappingFqn, classFqn, shape);
    }

    // ---- the planner's and the lowering's reads ---------------------------------------------------------------------

    public static @com.legend.base.Nullable String isoTimestamp(com.legend.values.PureDateLiteral value) {
        return com.legend.lowering.LiteralSpelling.isoTimestamp(value);
    }

    public static String inZone(String iso, String zone) {
        return com.legend.lowering.LiteralSpelling.inZone(iso, zone);
    }

}
