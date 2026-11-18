// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.List;

/**
 * THE ENGINE-TEXT ASSERT'S ARM, a test-planning facet of the plan style (W9's judge half, the orchestrator's ruling of
 * 2026-11-16; docs/records/e-java-w9-judge-half-2026-11-16.md): an assert that compares legend-engine's text (a
 * golden {@code toSQLString} or plan text) with ours is upstream's form, so its planning is the compatibility edge's.
 * The test planner asks the style whether it judges engine text ({@code PlanStyle.judgesEngineText}: the product's
 * does not, and such an assert takes the ordinary path, its upstream text native refused by name at lowering) and,
 * where it does, asks this facet ({@code edge.EdgeEngineTestText}). A bare hook (C4, 2026-11-17, the orchestrator's
 * ruling): signatures only; its routing gates are the edge's, its records top-level product types.
 */
public interface EngineTestText {

    /** The arm planned for {@code ask}: null where the arm does not claim the assert's shape, or declines it (the
     *  test planner's generic path continues). */
    @com.legend.base.Nullable TextPlanned plan(TextAsk ask);

    /** The equality arm over an assert's sides, as the walk's adjudication runs it: its verdict, or null where it
     *  declines (the generic path continues). Asked only where {@link #reachesUpstreamText} holds. */
    com.legend.testexecutionplan.@com.legend.base.Nullable TypedSide tryArm(String name, boolean wantEqual,
            List<TypedSpec> args, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook);

    /** The {@code assertSameSQL} root arm: its verdict, or null where it declines. */
    com.legend.testexecutionplan.@com.legend.base.Nullable TypedSide tryArmSameSql(
            com.legend.compiler.spec.typed.TypedUserCall root, List<TypedSpec> letPrefix, SpecCompiler specs,
            BodyDriver.ExecEnv env, AssertVerdicts.@com.legend.base.Nullable SpliceHook hook);

    /** The dual-golden root arm: its verdict, or null where it declines. */
    com.legend.testexecutionplan.@com.legend.base.Nullable TypedSide tryArmDualGolden(
            com.legend.compiler.spec.typed.TypedUserCall root, List<TypedSpec> letPrefix, SpecCompiler specs,
            BodyDriver.ExecEnv env, AssertVerdicts.@com.legend.base.Nullable SpliceHook hook);

    /** The database judge's text collector for an engine-text assert whose OUR text a side computes: planned as a
     *  side of the verdict row, nothing read at planning. */
    BatchTexts batchTexts();


    /** Whether {@code fqn} is a ROOT arm's helper: {@code assertSameSQL} or the dual golden. */
    boolean isRoot(String fqn);

    /** Whether an equality's two sides REACH a producer of upstream's text (through the lets, or for
     *  {@code assertEquals} through a helper's body): the shape's own facts, nothing of upstream's text computed. */
    boolean reachesUpstreamText(String name, boolean wantEqual, List<TypedSpec> args, List<TypedSpec> letPrefix,
            SpecCompiler specs);

    /** Whether {@code check} is the arm's answer for a text side that is not a constant of the plan. */
    boolean notAConstant(HostSides.@com.legend.base.Nullable Check check);

    /** The side a {@link #notAConstant} check names: its kind and planned root. */
    String why(HostSides.@com.legend.base.Nullable Check check);

    /** The database judge's gate for the edge's comparison {@code judgment} (J41, 2026-11-18; census C3 until then
     *  answered how the plan holds its two texts, and the product mapped that to two constants of its own):
     *  {@code shape} names the assert where the gate declines it. */
    DatabaseGate gate(com.legend.testexecutionplan.Extension judgment, java.util.function.Supplier<String> shape);

    /** A FRESH RECORDER of one test body's planning call over {@code template} (K10b of the edge audit, the
     *  orchestrator's ruling (A) of 2026-11-17): the style that body is planned in, recording legend-pure's words of
     *  each statement's raise sites ({@code PlanStyle#siteWords}); read once by the test door ({@link #siteWords}). */
    com.legend.database.PlanStyle recording(com.legend.database.PlanStyle template);

    /** The test door's ONE READ of {@code recording} ({@link #recording}'s), after its planning: each statement text's
     *  records of its sites' words (the hearing's data), the recorder sealed. */
    java.util.Map<String, List<com.legend.testexecutionplan.Extension>> siteWords(
            com.legend.database.PlanStyle recording);

    /** A required side's refusal of no rows as legend-pure words it ({@code multiplicity} the bound; C5's step E,
     *  2026-11-17; moved behind this facet with K10b: a site's words are no member of the product's style). */
    String sizeWords(String multiplicity);
}
