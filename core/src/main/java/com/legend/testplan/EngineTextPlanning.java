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
 * {@code ArchitectureTest.PLANNER_ENTRIES} lists this class once, for the edge's arm alone.
 */
public final class EngineTextPlanning {

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

    public static EngineTestText.@com.legend.base.Nullable Collector collector(BodyDriver.ExecEnv env) {
        return env.engineText();
    }

    public static @com.legend.base.Nullable VerdictBatch verdictBatch(BodyDriver.ExecEnv env) {
        return env.verdictBatch();
    }

    public static com.legend.testexecutionplan.TestPlan.JudgingRules judgingRules(BodyDriver.ExecEnv env) {
        return env.options().judgingRules();
    }

    public static java.util.Map<String, String> tableReplace(BodyDriver.ExecEnv env) {
        return env.postProcessors().tableReplace();
    }

    public static BodyDriver.ExecEnv withCollector(BodyDriver.ExecEnv env,
            EngineTestText.@com.legend.base.Nullable Collector c) {
        return env.withCollector(c);
    }

    public static BodyDriver.ExecEnv withFrame(BodyDriver.ExecEnv env, com.legend.compiler.spec.typed.ExecutionContext f) {
        return env.withFrame(f);
    }

    /** {@code env} with its own copy of the body's consumed lets: what it plans reads them and leaves none behind. */
    public static BodyDriver.ExecEnv withOwnLets(BodyDriver.ExecEnv env) {
        return env.withPlan(env.plan().withQueryLets(new java.util.LinkedHashMap<>(env.plan().queryLets())));
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
        return EngineTestText.marking(env, origin, ifUnmarked);
    }

    public static void restoreMarking(BodyDriver.ExecEnv env, @com.legend.base.Nullable String prior) {
        EngineTestText.restoreMarking(env, prior);
    }

    // ---- the verdict batch --------------------------------------------------------------------------------------

    public static boolean active(VerdictBatch batch) {
        return batch.active();
    }

    public static void defer(VerdictBatch batch, String name, boolean wantEqual, com.legend.sql.SqlQuery query,
            com.legend.executionplan.ExecutionPlan.Target on, int appeal, @com.legend.base.Nullable String decline) {
        batch.defer(name, wantEqual, query, on, appeal, decline);
    }

    public static void defer(VerdictBatch batch, String name, boolean wantEqual, com.legend.sql.SqlQuery query,
            com.legend.executionplan.ExecutionPlan.Target on) {
        batch.defer(name, wantEqual, query, on);
    }

    // ---- an assert's ask and check --------------------------------------------------------------------------------

    public static TypedSpec bare(EngineTestText.Ask ask) {
        return ask.bare;
    }

    public static List<TypedSpec> letPrefix(EngineTestText.Ask ask) {
        return ask.letPrefix;
    }

    public static SpecCompiler specs(EngineTestText.Ask ask) {
        return ask.specs;
    }

    public static BodyDriver.ExecEnv env(EngineTestText.Ask ask) {
        return ask.env;
    }

    public static AssertVerdicts.@com.legend.base.Nullable SpliceHook hook(EngineTestText.Ask ask) {
        return ask.hook;
    }

    public static boolean notAConstant(HostSides.@com.legend.base.Nullable Check check) {
        return EngineTestText.notAConstant(check);
    }

    public static String why(HostSides.@com.legend.base.Nullable Check check) {
        return EngineTestText.why(check);
    }

    /** The arm's refusal of a text side that is not a constant of the plan, {@code what} after its label. */
    public static EngineTestText.NotAConstant notAConstantError(String what) {
        return new EngineTestText.NotAConstant(EngineTestText.NotAConstant.LABEL + what);
    }

    public static EngineTestText.Planned planned(HostSides.Check check) {
        return new EngineTestText.Planned(check);
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

    public static com.legend.testexecutionplan.SideShape shapeOf(JudgePlans.PlannedSide p,
            com.legend.database.PlanStyle style) {
        return JudgePlans.shapeOf(p, style);
    }

    /** {@code side} planned as the database judge's rows: its one-row relation of its value, or null where the side
     *  has none ({@code DatabaseJudge.SideRows#why}). */
    public static com.legend.sql.@com.legend.base.Nullable SqlQuery scalarRow(TypedSpec side, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv at, AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        DatabaseJudge.SideRows rows = DatabaseJudge.planSide(side, false, letPrefix, specs, at, hook);
        return rows.why() != null ? null : rows.scalarRow(false);
    }

    // ---- the replay facts ------------------------------------------------------------------------------------------

    public static AssertVerdicts.ReplayGates replayFacts(TypedSpec query, List<TypedSpec> letPrefix) {
        return AssertVerdicts.replayFacts(query, letPrefix);
    }

    public static AssertVerdicts.ReplayGates noReplayGates() {
        return AssertVerdicts.ReplayGates.NONE;
    }

    public static boolean extentSubset(AssertVerdicts.ReplayGates g) {
        return g.extentSubset();
    }

    public static boolean ordered(AssertVerdicts.ReplayGates g) {
        return g.ordered();
    }

    public static @com.legend.base.Nullable List<String> sortKeys(AssertVerdicts.ReplayGates g) {
        return g.sortKeys();
    }

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

    public static com.legend.sql.SqlQuery textEquals(String golden, String ours) {
        return com.legend.lowering.VerdictSql.textEquals(golden, ours);
    }

    public static com.legend.sql.SqlQuery textEquals(String golden, com.legend.sql.SqlQuery oursRow) {
        return com.legend.lowering.VerdictSql.textEquals(golden, oursRow);
    }

    public static @com.legend.base.Nullable String isoTimestamp(com.legend.values.PureDateLiteral value) {
        return com.legend.lowering.LiteralSpelling.isoTimestamp(value);
    }

    public static String inZone(String iso, String zone) {
        return com.legend.lowering.LiteralSpelling.inZone(iso, zone);
    }

    public static com.legend.compiler.spec.typed.ExecutionContext boundContext(TypedSpec context,
            List<TypedSpec> letPrefix, SpecCompiler specs) {
        return com.legend.planner.BodyPlanner.boundContext(context, letPrefix, specs);
    }

    /** {@code producer}'s structured inputs ({@code planner.SqlTextInputs}), or null where its shape has none. */
    public static com.legend.planner.@com.legend.base.Nullable SqlTextInputs sqlTextInputs(
            com.legend.compiler.spec.NativeDispatch.RoutineCall producer, List<TypedSpec> letPrefix) {
        return com.legend.planner.SqlTextInputs.of(producer, letPrefix);
    }

    public static TypedSpec query(com.legend.planner.SqlTextInputs in) {
        return in.query();
    }

    public static TypedSpec mapping(com.legend.planner.SqlTextInputs in) {
        return in.mapping();
    }

    public static TypedSpec dialect(com.legend.planner.SqlTextInputs in) {
        return in.dialect();
    }

    public static TypedSpec runtime(com.legend.planner.SqlTextInputs in) {
        return in.runtime();
    }
}
