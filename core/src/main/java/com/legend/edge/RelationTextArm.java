// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.Lets;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.testexecutionplan.HostJudgment;
import com.legend.testexecutionplan.RootLoop;
import com.legend.testexecutionplan.TypedSide;
import com.legend.testplan.EngineTextPlanning;
import com.legend.testplan.HostSides;
import com.legend.testplan.TextAsk;

import java.util.ArrayList;
import java.util.List;

/**
 * SPIKE (H11, docs/records/compat-homework/H11-relation-text-spike.md; never proposed for merge as is): AN ASSERT
 * WHOSE SIDE IS A RELATION'S TEXT AT THE SIDE'S ROOT ({@code assertEquals('#TDS...', $rel->toString())}), planned as
 * the relation's ROWS and presented in Java before the compare. The arm is the edge's, reached through the hooks that
 * exist: {@code EngineTestText.reachesUpstreamText} and {@code plan} at planning, {@code HostJudgment.EdgeJudged} in
 * the plan, {@code EdgeHearing.judged} at the run. On only under {@code -Dlegend.spike.relationText=java}, which also
 * switches the SQL builder's {@code toString} off ({@link RelationTextLowering}), so a print anywhere else fails by
 * name and is counted.
 */
final class RelationTextArm {

    private RelationTextArm() {
    }

    /** The spike's switch. */
    static final boolean ON = "java".equals(System.getProperty("legend.spike.relationText"));

    private static final String RELATION_TO_STRING = "meta::pure::functions::relation::toString";

    /** The relation {@code side} prints at its ROOT (through the lets), or null: {@code toString(rel)}, or
     *  {@code toString(rel, false)}. */
    static @com.legend.base.Nullable TypedSpec printedRelation(TypedSpec side, List<TypedSpec> letPrefix) {
        TypedSpec s = Lets.bound(side, letPrefix);
        if (!(s instanceof TypedNativeCall c) || c.args().isEmpty() || c.args().size() > 2) {
            return null;
        }
        String fqn = c.callee().qualifiedName();
        if (!RELATION_TO_STRING.equals(fqn) && !PlatformTypes.TO_STRING.equals(fqn)) {
            return null;
        }
        if (c.args().size() == 2 && !(c.args().get(1) instanceof TypedCBoolean b && !b.value())) {
            return null;
        }
        return Type.isRelation(c.args().get(0).info().type()) ? c.args().get(0) : null;
    }

    /** Whether the arm claims an equality over {@code args}: on, and a side prints a relation at its root. */
    static boolean claims(boolean wantEqual, List<TypedSpec> args, List<TypedSpec> letPrefix) {
        return ON && wantEqual && args.size() >= 2
                && (printedRelation(args.get(0), letPrefix) != null || printedRelation(args.get(1), letPrefix) != null);
    }

    /** {@code ask}'s assert planned: a printed side asked as its relation (rows), the other as the value it is; the
     *  judgment the edge's record, no appeal. Null where the arm does not claim it. */
    static HostSides.@com.legend.base.Nullable Check plan(TextAsk ask) {
        TypedSpec bare = EngineTextPlanning.bare(ask);
        List<TypedSpec> letPrefix = EngineTextPlanning.letPrefix(ask);
        com.legend.builtin.NativeFn.Verdict fn = com.legend.builtin.NativeFn.Verdict.of(
                com.legend.compiler.spec.typed.Calls.calleeIdOf(bare)).orElse(null);
        if (fn != com.legend.builtin.NativeFn.Verdict.ASSERT_EQUALS) {
            return null;
        }
        List<TypedSpec> raw = bare instanceof TypedUserCall u ? u.args() : ((TypedNativeCall) bare).args();
        List<TypedSpec> args = com.legend.compiler.spec.ExecuteChainAssembly.narrowSideStamps(raw, letPrefix,
                EngineTextPlanning.specs(ask));
        if (!claims(true, args, letPrefix)) {
            return null;
        }
        List<HostSides.Ask> asks = new ArrayList<>(2);
        int printed = 0;
        for (int i = 0; i < 2; i++) {
            TypedSpec rel = printedRelation(args.get(i), letPrefix);
            printed |= rel == null ? 0 : 1 << i;
            asks.add(EngineTextPlanning.ask(rel == null ? args.get(i) : rel, false, true, EngineTextPlanning.hook(ask)));
        }
        return EngineTextPlanning.check(asks, new HostJudgment.EdgeJudged(
                new RelationTextJudgment(fn.bareName(), true, printed, "TDS"), RootLoop.NO_APPEAL, null, false));
    }

    // ---- the run -------------------------------------------------------------------------------------------------

    /** {@code j} judged over its two sides as the run read them: each printed side presented, then the two texts
     *  compared; a failure in legend-pure's words of a failed assertEquals. */
    static TypedSide judge(RelationTextJudgment j, List<TypedSide> sides) {
        String expected = text(sides.get(0), (j.printed() & 1) != 0, j);
        String actual = text(sides.get(1), (j.printed() & 2) != 0, j);
        COUNT.incrementAndGet();
        boolean held = expected.equals(actual) == j.wantEqual();
        // the spike's measurement line: one per comparison the arm judged, under the running test's name
        System.out.println("[H11] " + (held ? "HELD" : "DIFFERS") + " " + System.getProperty("legend.test.current")
                + " printed=" + j.printed() + " chars=" + expected.length());
        if (held) {
            return com.legend.testexecutionplan.Verdicts.ok();
        }
        // legend-pure's words of a failed assertEquals ('\nexpected: %r\nactual:   %r'), each text its %r
        return com.legend.testexecutionplan.Verdicts.fail("\nexpected: " + RelationText.representation(expected)
                + "\nactual:   " + RelationText.representation(actual));
    }

    /** How many comparisons the arm judged in this JVM (the spike's count). */
    static final java.util.concurrent.atomic.AtomicInteger COUNT = new java.util.concurrent.atomic.AtomicInteger();

    private static String text(TypedSide side, boolean printed, RelationTextJudgment j) {
        if (printed) {
            if (!(side instanceof TypedSide.Tabular t)) {
                throw new IllegalStateException(j.name() + " (relation text): the printed side is no rows but a "
                        + side.getClass().getSimpleName());
            }
            List<RelationText.Col> cols = new ArrayList<>(t.columns().size());
            for (com.legend.testexecutionplan.Column c : t.columns()) {
                cols.add(new RelationText.Col(c.name(), c.pureType()));
            }
            List<List<Object>> rows = new ArrayList<>(t.rows().size());
            for (com.legend.testexecutionplan.Row r : t.rows()) {
                rows.add(r.values());
            }
            return RelationText.tds(cols, rows);
        }
        Object v = switch (side) {
            case TypedSide.Scalar s -> s.value();
            case TypedSide.Collection c when c.values().size() == 1 -> c.values().get(0);
            default -> null;
        };
        if (!(v instanceof String s)) {
            throw new IllegalStateException(j.name() + " (relation text): the other side is no String but " + side);
        }
        return s;
    }
}
