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
