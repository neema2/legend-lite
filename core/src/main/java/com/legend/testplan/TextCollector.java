// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.List;

/** Where the arms' verdict goes while an assert is PLANNED for the program road (the test-is-the-product plan's
 *  M10b, {@code SqlTextVerdicts.plan}): the comparison and its appeal, handed over instead of judged; {@code planning} while the
 *  arm runs under it (a text side is then a constant of the plan, or OUR text deferred to the verdict step:
 *  {@code JudgePlans.evalSide}, {@code SqlTextVerdicts.defer}), false once the arm has answered (the appeal it built runs later
 *  in the same environment and reads what it needs). */
public interface TextCollector {
    /** The comparison the arm reached {@code SqlTextVerdicts.decide} with: its texts (ours the token of a deferred text), the
     *  equality the arm computed, the decline, the appeal, the environment. */
    void collect(String name, String golden, String ours, boolean textEqual,
            @com.legend.base.Nullable String decline, com.legend.testexecutionplan.Extension appeal, BodyDriver.ExecEnv env);

    boolean planning();

    /** OUR text planned as a side the plan runs (M10c item 8): where {@code planned} is a {@code VALUES} side the
     *  arm is reading as our text, declared a String to one, planned under the environment the arm was given
     *  ({@code at}), the collector records it as the verdict step's ASK with the statement origin the arm marked
     *  and answers a text token the arm compares as any text; where the side is not deferred, the collector refuses
     *  the body by name ({@code what}: the side's kind and planned root), its own refusal (train 17, 2026-11-18). */
    /** The statement origin {@code origin} put in force on this collector, by the statement origin's own rule
     *  ({@code StatementOrigin.under} where {@code ifUnmarked}, else set): what was in force before, to restore. */
    @com.legend.base.Nullable String marking(String origin, boolean ifUnmarked);

    /** The origin in force restored to {@code prior}. */
    void restoreMarking(@com.legend.base.Nullable String prior);

    com.legend.testexecutionplan.TypedSide defer(TypedSpec side, JudgePlans.PlannedSide planned, boolean identity,
            boolean fold, AssertVerdicts.@com.legend.base.Nullable SpliceHook hook, BodyDriver.ExecEnv at,
            List<TypedSpec> letPrefix, SpecCompiler specs, String what);
}
