// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import com.legend.base.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * THE DATABASE JUDGE'S ROOT LOOP AS DATA (docs/PLAN_AS_DATA_2026_11_07.md, the design's step 5, 2026-11-08): one
 * segment's statement-root asserts in body order, each with its steps -- a verdict row the plan reads, by its verdict's
 * id, or an outcome decided at planning -- and its caller's message. The run judges them in this order once the
 * segment's last verdict group has run, the first failure raising (the walk's first-failure sequencing). Carried on the
 * segment's last group ({@link TestPlan.Verdicts#roots}). Until this step the planner's batch held the same facts in
 * maps keyed by object identity, and judged them itself.
 *
 * @param roots     the segment's asserts, in body order
 * @param fragments each frame name of the body and the let or assert it came from: a database error on the segment's
 *                  fused statement names the statement that owns it (the census's reason)
 */
public record RootLoop(List<Root> roots, Map<String, String> fragments) {

    /** No verdict: a row the dialect cannot spell, planned with no statement. */
    public static final int NO_VERDICT = -1;

    /** No appeal: a row whose failure is the assert's. */
    public static final int NO_APPEAL = 0;

    public RootLoop {
        roots = List.copyOf(roots);
        fragments = Collections.unmodifiableMap(new LinkedHashMap<>(fragments));
    }

    /**
     * One statement-root assert.
     *
     * @param listenerName the name the test listener hears its verdict under
     * @param steps        its rows and decided outcomes, in the order they decide (an unrolled assert is several rows)
     * @param message      its caller's message where it READS (a side read only where the assert fails), or null
     */
    public record Root(String listenerName, List<Step> steps, @Nullable Message message) {
        public Root {
            Objects.requireNonNull(listenerName, "listenerName");
            steps = List.copyOf(steps);
        }
    }

    /**
     * A root's caller's message that reads: the side the plan reads for it, or the dialect's refusal of it (raised where
     * the root fails, nothing sent).
     *
     * @param side    the message's statement, a side of the plan; null where refused
     * @param refusal the dialect's refusal; null where the side is planned
     * @param carried whether a verdict of the plan carries the side (a root every member of which was refused or
     *                decided at planning has no statement to carry it: its message is refused by name)
     */
    public record Message(TestPlan.@Nullable Side side, @Nullable Failure refusal, boolean carried) {
        public Message {
            if ((side == null) == (refusal == null)) {
                throw new IllegalArgumentException("a caller's message is a side or a refusal, one of the two");
            }
        }
    }

    /** A root's step. */
    public sealed interface Step permits Row, Decided {
    }

    /**
     * A verdict row the database computes.
     *
     * @param name    the row's assert name (the row's own message names it)
     * @param verdict the id of the plan's verdict whose row this is; {@link #NO_VERDICT} where the dialect cannot spell
     *                the row's statement, planned with none
     * @param refusal the dialect's refusal of the row's statement, raised at its place (nothing sent); null where spelled
     * @param orElse  the row's RULED comparison as decided at planning (the dialect's refusal of it, or a failure its
     *                planning raised), raised where the generic row fails; null where none
     * @param appeal  the number of the appeal a failed row is heard by (compatibility mode's rows), {@link #NO_APPEAL}
     *                where the row's failure is the assert's
     * @param decline the reason the arm decided before its texts were compared (a foreign dialect, unbindable plan
     *                parameters), reported to the listener at the row's turn, before the row is judged; null where
     *                none (hole 5, 2026-11-08: until then it was reported while the assert was planned)
     */
    public record Row(String name, int verdict, @Nullable Failure refusal, @Nullable Failure orElse, int appeal,
            @Nullable String decline) implements Step {
        public Row {
            Objects.requireNonNull(name, "name");
            if ((verdict == NO_VERDICT) != (refusal != null)) {
                throw new IllegalArgumentException(name + ": a row has a verdict or the dialect's refusal, one of the"
                        + " two");
            }
        }
    }

    /**
     * An outcome decided at planning: a static decision, an unjudged decline, a planned raise ({@code reported}: the
     * listener hears its verdict), or a WALL the arm left through ({@code reported} false: raised at its turn after the
     * roots before it, told to no listener).
     */
    public record Decided(Failure failure, boolean reported) implements Step {
        public Decided {
            Objects.requireNonNull(failure, "failure");
        }
    }
}
