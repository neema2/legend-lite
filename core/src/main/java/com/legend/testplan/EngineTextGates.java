// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.List;

/**
 * THE TEST PLANNER'S QUESTIONS TO THE ENGINE-TEXT HOOK (census C4, the orchestrator's ruling of 2026-11-17): each asks
 * the plan style whether it judges legend-engine's text ({@code PlanStyle.judgesEngineText}) and only then its
 * {@link EngineTestText} facet. The product's style does not: every answer is "no", so an engine-text assert takes the
 * ordinary path and its upstream text native is refused by name at lowering. No knowledge of upstream's text here --
 * the gates themselves (which helpers are roots, what reaches upstream's text) are the edge's answers.
 */
final class EngineTextGates {

    private EngineTextGates() {
    }

    /** The style's engine-text facet, where it judges engine text; null where it does not (the product's). */
    static @com.legend.base.Nullable EngineTestText facet(BodyDriver.ExecEnv env) {
        com.legend.database.PlanStyle style = env.plan().style();
        return style.judgesEngineText() ? style.facet(EngineTestText.class) : null;
    }

    /** Whether {@code fqn} is an engine-text ROOT arm's helper, on this style. */
    static boolean isRoot(BodyDriver.ExecEnv env, @com.legend.base.Nullable String fqn) {
        EngineTestText f = facet(env);
        return fqn != null && f != null && f.isRoot(fqn);
    }

    /** Whether an equality's sides reach upstream's text, on this style. */
    static boolean reachesUpstreamText(BodyDriver.ExecEnv env, String name, boolean wantEqual, List<TypedSpec> args,
            List<TypedSpec> letPrefix, SpecCompiler specs) {
        EngineTestText f = facet(env);
        return f != null && f.reachesUpstreamText(name, wantEqual, args, letPrefix, specs);
    }

    /** The engine-text arm's check for {@code bare}, on this style: null where it judges none or does not claim it. */
    static HostSides.@com.legend.base.Nullable Check armFor(TypedSpec bare, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv env, AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        EngineTestText f = facet(env);
        TextPlanned p = f == null ? null : f.plan(new TextAsk(bare, letPrefix, specs, env, hook));
        return p == null ? null : p.check;
    }

    /** Whether {@code check} is the arm's not-a-constant answer, on this style. */
    static boolean notAConstant(BodyDriver.ExecEnv env, HostSides.@com.legend.base.Nullable Check check) {
        EngineTestText f = facet(env);
        return f != null && f.notAConstant(check);
    }

    /** The side a not-a-constant check names (asked only where {@link #notAConstant} holds). */
    static String why(BodyDriver.ExecEnv env, HostSides.@com.legend.base.Nullable Check check) {
        return java.util.Objects.requireNonNull(facet(env), "a not-a-constant answer with no engine-text facet")
                .why(check);
    }

    /** The statement origin put in force on {@code env}'s text collector: what was in force before, to restore; none
     *  where the environment carries no collector (no engine-text arm is planning). */
    static @com.legend.base.Nullable String marking(BodyDriver.ExecEnv env, String origin, boolean ifUnmarked) {
        TextCollector c = env.engineText();
        return c == null ? null : c.marking(origin, ifUnmarked);
    }

    /** The origin in force on {@code env}'s text collector restored to {@code prior}. */
    static void restoreMarking(BodyDriver.ExecEnv env, @com.legend.base.Nullable String prior) {
        TextCollector c = env.engineText();
        if (c != null) {
            c.restoreMarking(prior);
        }
    }

    /** THE STYLE OF ONE TEST BODY'S PLANNING CALL (K10b of the edge audit, 2026-11-17): where {@code style} judges
     *  engine text (the compatibility edge's), a fresh recorder of its raise sites' words, minted by its facet; the
     *  product's style itself, which records none (its answer, lite's form). */
    static com.legend.database.PlanStyle planningCall(com.legend.database.PlanStyle style) {
        return style.judgesEngineText() ? style.facet(EngineTestText.class).recording(style) : style;
    }

    /** The test door's one read of the raise sites' words {@code style} recorded ({@link #planningCall}'s): none where
     *  the style judges no engine text (the product's records none). */
    static java.util.Map<String, java.util.Map<Integer, String>> siteWords(
            com.legend.database.PlanStyle style) {
        return style.judgesEngineText() ? style.facet(EngineTestText.class).siteWords(style) : java.util.Map.of();
    }

    /** A required side's refusal of no rows as {@code style} words it: legend-pure's where it judges engine text (the
     *  edge's facet), else the product's own -- its kind's sentence and the bound. */
    static String sizeWords(com.legend.database.PlanStyle style, String multiplicity) {
        return style.judgesEngineText() ? style.facet(EngineTestText.class).sizeWords(multiplicity)
                : com.legend.error.ErrorKind.SIZE_NOT_MULTIPLICITY.sentence() + ": " + multiplicity;
    }
}
