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

    /** Where the arms' verdict goes while an assert is PLANNED for the program road (the test-is-the-product plan's
     *  M10b, {@code SqlTextVerdicts.plan}): the comparison and its appeal, handed over instead of judged; {@code planning} while the
     *  arm runs under it (a text side is then a constant of the plan, or OUR text deferred to the verdict step:
     *  {@code JudgePlans.evalSide}, {@code SqlTextVerdicts.defer}), false once the arm has answered (the appeal it built runs later
     *  in the same environment and reads what it needs). */
    interface Collector {
        /** The comparison the arm reached {@code SqlTextVerdicts.decide} with: its texts (ours the token of a deferred text), the
         *  equality the arm computed, the decline, the appeal, the environment. */
        void collect(String name, String golden, String ours, boolean textEqual,
                @com.legend.base.Nullable String decline, com.legend.testexecutionplan.Appeal appeal, BodyDriver.ExecEnv env);

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
        return SqlTextVerdicts.claims(bare, letPrefix, specs);
    }

    /** The arm as the test planner's package still holds it (S2b): run for the edge's facet alone, until the arm's
     *  code moves to the edge. */
    static @com.legend.base.Nullable Planned arm(Ask ask) {
        HostSides.Check c = SqlTextVerdicts.plan(ask.bare, ask.letPrefix, ask.specs, ask.env, ask.hook);
        return c == null ? null : new Planned(c);
    }
}
