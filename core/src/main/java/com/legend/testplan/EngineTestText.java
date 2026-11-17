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
 * The test planner recognizes the shape ({@link #claims}) and asks the style for this facet; the product's style
 * refuses it by name, and the edge answers it ({@code edge.EdgeEngineTestText}). The arm's code moves to the edge in
 * small steps (S2b on); until it has, the edge's answer runs it through {@link #arm}.
 */
public interface EngineTestText {

    /** The arm planned for {@code ask}: the assert's check, or null where the arm declines the shape (the test
     *  planner's generic path continues). */
    @com.legend.base.Nullable Planned plan(Ask ask);

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

    /** The database judge's text collector ({@link #batchTexts}). */
    interface BatchTexts {
        /** {@code at} with this collector its engine-text collector: the environment the arm is planned under. */
        BodyDriver.ExecEnv under(BodyDriver.ExecEnv at);

        /** The arm has answered: the appeal it built reads what it needs from here on. */
        void endPlanning();
    }

    /** The ENGINE-TEXT ARM for {@code bare}: null where the arm does not claim the shape ({@link #claims}); else the
     *  plan style's facet's answer -- the compatibility edge's arm, the product's style refusing by name. */
    static HostSides.@com.legend.base.Nullable Check armFor(TypedSpec bare, List<TypedSpec> letPrefix, SpecCompiler specs,
            BodyDriver.ExecEnv env, AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        if (!claims(bare, letPrefix, specs)) {
            return null;
        }
        Planned p = env.plan().style().facet(EngineTestText.class).plan(new Ask(bare, letPrefix, specs, env, hook));
        return p == null ? null : p.check;
    }

    /** Whether {@code fqn} is a ROOT arm's helper: {@code assertSameSQL} or the dual golden. */
    static boolean isRoot(String fqn) {
        return com.legend.compiler.element.type.PlatformTypes.ASSERT_SAME_SQL.equals(fqn)
                || com.legend.compiler.element.type.PlatformTypes.ASSERT_EQUALS_DUAL_GOLDEN.equals(fqn);
    }

    /** Whether an equality's two sides REACH a producer of upstream's text -- {@code toSQLString}'s natives, a plan
     *  handle ({@code executionPlan}), the corpus's SQL reads ({@code sql}, {@code sqlRemoveFormatting}) -- through the
     *  lets, or for {@code assertEquals} through a helper's body: the shape's own facts, read from the typed tree,
     *  nothing of upstream's text computed. A static superset of the arm's gates: the edge's arm narrows it. */
    static boolean reachesUpstreamText(String name, boolean wantEqual, List<TypedSpec> args, List<TypedSpec> letPrefix,
            SpecCompiler specs) {
        if (!wantEqual || args.size() < 2) {
            return false;
        }
        for (TypedSpec side : args.subList(0, 2)) {
            if (reaches(side, letPrefix) || "assertEquals".equals(name) && reachesThroughHelpers(side, letPrefix, specs)) {
                return true;
            }
        }
        return false;
    }

    private static boolean reaches(TypedSpec t, List<TypedSpec> letPrefix) {
        java.util.ArrayDeque<TypedSpec> work = new java.util.ArrayDeque<>();
        work.add(t);
        java.util.Set<String> seen = new java.util.HashSet<>();
        while (!work.isEmpty()) {
            TypedSpec cur = work.poll();
            String fqn = cur instanceof com.legend.compiler.spec.typed.TypedNativeCall n ? n.callee().qualifiedName()
                    : cur instanceof com.legend.compiler.spec.typed.TypedUserCall u ? u.callee().qualifiedName() : null;
            if (fqn != null && UPSTREAM_TEXT_PRODUCERS.contains(fqn)) {
                return true;
            }
            if (cur instanceof com.legend.compiler.spec.typed.TypedVariable v && seen.add(v.name())) {
                com.legend.compiler.spec.typed.TypedLet let = com.legend.compiler.spec.typed.Lets.binding(letPrefix,
                        v.name());
                if (let != null) {
                    work.add(let.value());
                }
            }
            work.addAll(cur.children());
        }
        return false;
    }

    private static boolean reachesThroughHelpers(TypedSpec side, List<TypedSpec> letPrefix, SpecCompiler specs) {
        try {
            List<TypedSpec> seq = new java.util.ArrayList<>(letPrefix);
            seq.add(side);
            List<TypedSpec> reduced = new com.legend.compiler.spec.UserCallInliner(specs).inlineBody(seq);
            return reaches(reduced.get(reduced.size() - 1), reduced.subList(0, reduced.size() - 1));
        } catch (RuntimeException e) {
            return false;   // an un-inlinable helper: no producer, as the arm reads it
        }
    }

    /** The producers of upstream's text an engine-text assert reads. */
    java.util.Set<String> UPSTREAM_TEXT_PRODUCERS = java.util.Set.of(
            com.legend.compiler.element.type.PlatformTypes.TO_SQL_STRING,
            com.legend.compiler.element.type.PlatformTypes.TO_SQL_STRING_PRETTY,
            com.legend.compiler.element.type.PlatformTypes.TO_NON_EXECUTABLE_SQL_STRING,
            com.legend.compiler.element.type.PlatformTypes.EXECUTION_PLAN,
            com.legend.compiler.spec.ResultEnvelopeSplice.SQL_FQN,
            com.legend.compiler.spec.ResultEnvelopeSplice.SQL_REMOVE_FORMATTING_FQN);

    /** Whether {@code check} is the arm's answer for a text side that is not a constant of the plan. */
    static boolean notAConstant(HostSides.@com.legend.base.Nullable Check check) {
        return check != null && com.legend.testexecutionplan.HostJudgment.core(check.judgment())
                instanceof com.legend.testexecutionplan.HostJudgment.Raise r
                && r.failure().kind() == com.legend.testexecutionplan.Failure.Kind.NOT_A_CONSTANT;
    }

    /** The side a {@link #notAConstant} check names: its kind and planned root (the message after its label). */
    static String why(HostSides.@com.legend.base.Nullable Check check) {
        if (check == null) {
            throw new IllegalStateException("the side of a check that is not a not-a-constant answer");
        }
        String what = String.valueOf(((com.legend.testexecutionplan.HostJudgment.Raise)
                com.legend.testexecutionplan.HostJudgment.core(check.judgment())).failure().message());
        return what.startsWith(NotAConstant.LABEL) ? what.substring(NotAConstant.LABEL.length()) : what;
    }

    /** The origin {@code origin} put in force on {@code env}'s text collector: what was in force before, to restore;
     *  none where the environment carries no collector. */
    static @com.legend.base.Nullable String marking(BodyDriver.ExecEnv env, String origin, boolean ifUnmarked) {
        Collector c = env.engineText();
        return c == null ? null : c.marking(origin, ifUnmarked);
    }

    /** The origin in force on {@code env}'s text collector restored to {@code prior}. */
    static void restoreMarking(BodyDriver.ExecEnv env, @com.legend.base.Nullable String prior) {
        Collector c = env.engineText();
        if (c != null) {
            c.restoreMarking(prior);
        }
    }

    /** Where the arms' verdict goes while an assert is PLANNED for the program road (the test-is-the-product plan's
     *  M10b, {@code SqlTextVerdicts.plan}): the comparison and its appeal, handed over instead of judged; {@code planning} while the
     *  arm runs under it (a text side is then a constant of the plan, or OUR text deferred to the verdict step:
     *  {@code JudgePlans.evalSide}, {@code SqlTextVerdicts.defer}), false once the arm has answered (the appeal it built runs later
     *  in the same environment and reads what it needs). */
    interface Collector {
        /** The comparison the arm reached {@code SqlTextVerdicts.decide} with: its texts (ours the token of a deferred text), the
         *  equality the arm computed, the decline, the appeal, the environment. */
        void collect(String name, String golden, String ours, boolean textEqual,
                @com.legend.base.Nullable String decline, com.legend.testexecutionplan.Extension appeal, BodyDriver.ExecEnv env);

        boolean planning();

        /** OUR text planned as a side the plan runs (M10c item 8): where {@code planned} is a {@code VALUES} side the
         *  arm is reading as our text, declared a String to one, planned under the environment the arm was given
         *  ({@code at}), the collector records it as the verdict step's ASK with the statement origin the arm marked
         *  and answers a text token the arm compares as any text; null where the side is not deferred (the caller
         *  refuses the body by name). */
        /** The statement origin {@code origin} put in force on this collector, by the statement origin's own rule
         *  ({@code StatementOrigin.under} where {@code ifUnmarked}, else set): what was in force before, to restore. */
        @com.legend.base.Nullable String marking(String origin, boolean ifUnmarked);

        /** The origin in force restored to {@code prior}. */
        void restoreMarking(@com.legend.base.Nullable String prior);

        @com.legend.base.Nullable com.legend.testexecutionplan.TypedSide defer(TypedSpec side, JudgePlans.PlannedSide planned, boolean identity,
                boolean fold, AssertVerdicts.@com.legend.base.Nullable SpliceHook hook, BodyDriver.ExecEnv at,
                List<TypedSpec> letPrefix, SpecCompiler specs);
    }

    /** Raised while an arm is PLANNED for the program road where a text side is not a constant of the plan (it would
     *  send a statement at planning): the body stays on the walk, by name ({@code SqlTextVerdicts.plan} answers it as
     *  {@code SqlTextVerdicts.notAConstant}'s check, its message the side's kind and planned root for the unplanned-assert census). */
    final class NotAConstant extends RuntimeException {
        /** The message's label; the side's kind and planned root follow it. */
        public static final String LABEL = "a text side that is not a constant of the plan: ";

        public NotAConstant(String what) {
            super(what);
        }
    }

    /** One assert the test planner hands the arm: opaque to everyone but the test planner. */
    final class Ask {
        final TypedSpec bare;
        final List<TypedSpec> letPrefix;
        final SpecCompiler specs;
        final BodyDriver.ExecEnv env;
        final AssertVerdicts.@com.legend.base.Nullable SpliceHook hook;

        Ask(TypedSpec bare, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
                AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
            this.bare = bare;
            this.letPrefix = letPrefix;
            this.specs = specs;
            this.env = env;
            this.hook = hook;
        }
    }

    /** The arm's answer for one assert: opaque to everyone but the test planner. */
    final class Planned {
        final HostSides.Check check;

        Planned(HostSides.Check check) {
            this.check = check;
        }
    }

    /** Whether the arm claims {@code bare}: a root arm's helper ({@code assertSameSQL}, the dual golden) or an
     *  equality whose sides a producer of upstream's text feeds -- the shape's own facts, read from the typed tree;
     *  nothing of upstream's text is computed. */
    static boolean claims(TypedSpec bare, List<TypedSpec> letPrefix, SpecCompiler specs) {
        String fqn = AssertVerdicts.calleeFqn(bare);
        if (fqn != null && isRoot(fqn) && bare instanceof com.legend.compiler.spec.typed.TypedUserCall) {
            return true;
        }
        com.legend.builtin.NativeFn.Verdict fn = com.legend.builtin.NativeFn.Verdict.of(
                com.legend.compiler.spec.typed.Calls.calleeIdOf(bare)).orElse(null);
        if (fn != com.legend.builtin.NativeFn.Verdict.ASSERT_EQUALS && fn != com.legend.builtin.NativeFn.Verdict.ASSERT_NOT_EQUALS) {
            return false;
        }
        List<TypedSpec> raw = bare instanceof com.legend.compiler.spec.typed.TypedUserCall u ? u.args()
                : ((com.legend.compiler.spec.typed.TypedNativeCall) bare).args();
        return reachesUpstreamText(fn.bareName(), fn == com.legend.builtin.NativeFn.Verdict.ASSERT_EQUALS,
                com.legend.compiler.spec.ExecuteChainAssembly.narrowSideStamps(raw, letPrefix, specs), letPrefix, specs);
    }

}
