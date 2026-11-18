// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.testexecutionplan.TypedSide;
import com.legend.testplan.AssertVerdicts;
import com.legend.testplan.BodyDriver;
import com.legend.testplan.EngineTestText;
import com.legend.testplan.EngineTextPlanning;

import java.util.List;

/**
 * The edge's engine-text assert arm (W9's judge half, 2026-11-16): the facet {@link EngineTestText} the edge's style
 * answers, over the arm itself ({@link SqlTextVerdicts}, moved here from the test planner in S2c).
 */
final class EdgeEngineTestText implements EngineTestText {

    static final EdgeEngineTestText INSTANCE = new EdgeEngineTestText();

    private EdgeEngineTestText() {
    }

    /** The arm for {@code ask}: null where it does not claim the shape ({@link #claims}); else the arm's plan. */
    @Override
    public com.legend.testplan.@com.legend.base.Nullable TextPlanned plan(com.legend.testplan.TextAsk ask) {
        return !claims(EngineTextPlanning.bare(ask), EngineTextPlanning.letPrefix(ask), EngineTextPlanning.specs(ask))
                ? null : SqlTextVerdicts.plan(ask);
    }

    @Override
    public @com.legend.base.Nullable TypedSide tryArm(String name, boolean wantEqual, List<TypedSpec> args,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        return SqlTextVerdicts.tryArm(name, wantEqual, args, letPrefix, specs, env, hook);
    }

    @Override
    public @com.legend.base.Nullable TypedSide tryArmSameSql(com.legend.compiler.spec.typed.TypedUserCall root,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        return SqlTextVerdicts.tryArmSameSql(root, letPrefix, specs, env, hook);
    }

    @Override
    public @com.legend.base.Nullable TypedSide tryArmDualGolden(com.legend.compiler.spec.typed.TypedUserCall root,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        return SqlTextVerdicts.tryArmDualGolden(root, letPrefix, specs, env, hook);
    }

    @Override
    public com.legend.testplan.BatchTexts batchTexts() {
        return new SqlTextVerdicts.BatchTexts();
    }

    // ---- the routing gates (census C4, 2026-11-17: moved from EngineTestText's statics -- the hook bare) ----

    /** Whether {@code fqn} is a ROOT arm's helper: {@code assertSameSQL} or the dual golden. */
    @Override
    public boolean isRoot(String fqn) {
        return com.legend.compiler.element.type.PlatformTypes.ASSERT_SAME_SQL.equals(fqn)
                || com.legend.compiler.element.type.PlatformTypes.ASSERT_EQUALS_DUAL_GOLDEN.equals(fqn);
    }

    /** Whether an equality's two sides REACH a producer of upstream's text -- {@code toSQLString}'s natives, a plan
     *  handle ({@code executionPlan}), the corpus's SQL reads ({@code sql}, {@code sqlRemoveFormatting}) -- through the
     *  lets, or for {@code assertEquals} through a helper's body: the shape's own facts, read from the typed tree,
     *  nothing of upstream's text computed. A static superset of the arm's gates: the edge's arm narrows it. */
    @Override
    public boolean reachesUpstreamText(String name, boolean wantEqual, List<TypedSpec> args, List<TypedSpec> letPrefix,
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
    private static final java.util.Set<String> UPSTREAM_TEXT_PRODUCERS = java.util.Set.of(
            com.legend.compiler.element.type.PlatformTypes.TO_SQL_STRING,
            com.legend.compiler.element.type.PlatformTypes.TO_SQL_STRING_PRETTY,
            com.legend.compiler.element.type.PlatformTypes.TO_NON_EXECUTABLE_SQL_STRING,
            com.legend.compiler.element.type.PlatformTypes.EXECUTION_PLAN,
            com.legend.compiler.spec.ResultEnvelopeSplice.SQL_FQN,
            com.legend.compiler.spec.ResultEnvelopeSplice.SQL_REMOVE_FORMATTING_FQN);

    /** Whether {@code check} is the arm's answer for a text side that is not a constant of the plan. */
    @Override
    public boolean notAConstant(com.legend.testplan.HostSides.@com.legend.base.Nullable Check check) {
        return check != null && com.legend.testexecutionplan.HostJudgment.core(EngineTextPlanning.judgment(check))
                instanceof com.legend.testexecutionplan.HostJudgment.Raise r
                && r.failure().kind() == com.legend.testexecutionplan.Failure.Kind.NOT_A_CONSTANT;
    }

    /** The side a {@link #notAConstant} check names: its kind and planned root (the message after its label). */
    @Override
    public String why(com.legend.testplan.HostSides.@com.legend.base.Nullable Check check) {
        if (check == null) {
            throw new IllegalStateException("the side of a check that is not a not-a-constant answer");
        }
        String what = String.valueOf(((com.legend.testexecutionplan.HostJudgment.Raise)
                com.legend.testexecutionplan.HostJudgment.core(EngineTextPlanning.judgment(check))).failure().message());
        return what.startsWith(com.legend.testplan.NotAConstant.LABEL) ? what.substring(com.legend.testplan.NotAConstant.LABEL.length()) : what;
    }

    /** The database judge's gate for the comparison {@code judgment}, from how the plan holds its two texts, the
     *  edge's own record (census C3; J41, 2026-11-18: the gate itself the edge's, its two engine-text constants gone
     *  from the product):
     *  <ul>
     *  <li>a golden a side the step reads: not planned -- the arm processes its golden at planning;</li>
     *  <li>our text a side the step reads: planned under the style's text collector, the verdict row computing our
     *  text and the equality, its actual column the appeal's text;</li>
     *  <li>both texts constants of the plan: planned; appealed only where they differ (an appeal is heard on a failed
     *  text alone), so two equal texts read no store.</li>
     *  </ul> */
    @Override
    public com.legend.testplan.DatabaseGate gate(com.legend.testexecutionplan.Extension judgment,
            java.util.function.Supplier<String> shape) {
        EngineTextJudgment e = (EngineTextJudgment) judgment;
        if (e.golden() == null) {
            com.legend.diagnostics.Census.unplannedAssert("database: a golden read at the step: " + shape.get());
            return com.legend.testplan.DatabaseGate.UNPLANNED;
        }
        if (e.ours() == null) {
            return com.legend.testplan.DatabaseGate.appealed(true);
        }
        return e.textEqual() ? com.legend.testplan.DatabaseGate.PLANS : com.legend.testplan.DatabaseGate.appealed(false);
    }

    /** Whether the arm claims {@code bare}: a root arm's helper ({@code assertSameSQL}, the dual golden) or an
     *  equality whose sides a producer of upstream's text feeds -- the shape's own facts, read from the typed tree;
     *  nothing of upstream's text is computed. */
    private boolean claims(TypedSpec bare, List<TypedSpec> letPrefix, SpecCompiler specs) {
        String fqn = EngineTextPlanning.calleeFqn(bare);
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
