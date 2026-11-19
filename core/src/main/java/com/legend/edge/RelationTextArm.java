// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.Lets;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
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
 * WHOSE SIDE IS A RELATION'S TEXT AT THE SIDE'S ROOT, planned as the relation's ROWS and presented in Java before the
 * compare. The prints claimed (each through the lets):
 * <ul>
 * <li>{@code toString(rel)}, {@code toString(rel, false)}: legend-pure's {@code #TDS} text;</li>
 * <li>{@code toCSV(tds)}, {@code toCSV(tds, renderTdsNull)}, {@code toCSV(tds, '%t{yyyy-MM-dd HH:mm:ss}',
 *     '%t{yyyy-MM-dd}', renderTdsNull)}: legend-engine's test CSV;</li>
 * <li>{@code toCSV(...)->replace('\n', sep)}: the CSV with its rows joined by {@code sep} -- a presenter FORM (the
 *     calendar family's one-line spelling; the product's host judge names it {@code CSVJOIN:<sep>});</li>
 * <li>a TDS's {@code csv} property.</li>
 * </ul>
 * The arm is the edge's, reached through the hooks that exist: {@code EngineTestText.reachesUpstreamText} and
 * {@code plan} at planning, {@code HostJudgment.EdgeJudged} in the plan, {@code EdgeHearing.judged} at the run. Each
 * comparison is judged as MAIN's host judge judges the same pair today, the text now Java's: where main names the pair
 * a render form (its {@code AssertVerdicts.renderForm}: {@code CSVTEXT}, {@code TDSTEXT} over
 * {@code string::toString}, {@code CSVJOIN:<sep>}), the two strings, then the compatibility rules' ruled comparison
 * (S131, S132) where they differ, and main's failure words; elsewhere (main's generic equality) the two strings. On
 * only under {@code -Dlegend.spike.relationText=java}, which also switches the SQL builder OFF
 * ({@link RelationTextLowering}): a print anywhere else fails by name and is counted.
 */
final class RelationTextArm {

    private RelationTextArm() {
    }

    /** The spike's switch. */
    static final boolean ON = "java".equals(System.getProperty("legend.spike.relationText"));

    private static final String RELATION_TO_STRING = "meta::pure::functions::relation::toString";

    /** A side's print: the relation it prints, the presenter's form, and the render form main's host judge names the
     *  side by (null where it names none: main's generic equality). */
    record Printed(TypedSpec relation, String kind, @com.legend.base.Nullable String mainForm, boolean renderTdsNull,
            @com.legend.base.Nullable String sep) {
    }

    /** The print {@code side} is at its ROOT (through the lets), or null. */
    static @com.legend.base.Nullable Printed printed(TypedSpec side, List<TypedSpec> letPrefix) {
        TypedSpec s = Lets.bound(side, letPrefix);
        if (s instanceof TypedNativeCall rep && PlatformTypes.STRING_REPLACE.equals(rep.callee().qualifiedName())
                && rep.args().size() == 3
                && Lets.bound(rep.args().get(0), letPrefix) instanceof TypedNativeCall csv
                && PlatformTypes.TO_CSV.equals(csv.callee().qualifiedName())
                && rep.args().get(1) instanceof TypedCString from && "\n".equals(from.value())
                && rep.args().get(2) instanceof TypedCString to) {
            Printed inner = csvOf(csv);
            return inner == null ? null
                    : new Printed(inner.relation(), "CSVJOIN", "CSVJOIN:" + to.value(), inner.renderTdsNull(), to.value());
        }
        if (s instanceof TypedNativeCall csv && PlatformTypes.TO_CSV.equals(csv.callee().qualifiedName())) {
            return csvOf(csv);
        }
        // a TDS's csv: its receiver TDS-shaped at planning (a cast(@TDS<Any>) has its schema only after the store's
        // resolution, which the lowering's Render.csvOverRelation reads)
        if (s instanceof TypedPropertyAccess read && read.property().equals(PlatformTypes.TDS_CSV_PROPERTY)
                && (PlatformTypes.isTdsShaped(read.source().info().type())
                        || Type.relationSchema(read.source().info().type()) != null)) {
            // a cast to a TDS type changes no row: the rows are asked of the relation under it, whose schema the
            // plan knows (asked through the cast, the side plans as a collection: testEnumInRelation)
            TypedSpec rel = read.source();
            while (Lets.bound(rel, letPrefix) instanceof com.legend.compiler.spec.typed.TypedCast cast
                    && PlatformTypes.isTdsShaped(cast.target())) {
                rel = cast.source();
            }
            return new Printed(rel, "TDSCSV", null, true, null);
        }
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
        if (!Type.isRelation(c.args().get(0).info().type())) {
            return null;
        }
        // main's renderForm names TDSTEXT only for string::toString over a relation, with one argument
        String mainForm = PlatformTypes.TO_STRING.equals(fqn) && c.args().size() == 1 ? "TDSTEXT" : null;
        return new Printed(c.args().get(0), "TDS", mainForm, false, null);
    }

    /** {@code toCSV}'s print, or null: its three overloads (the lowering's {@code lowerToCsv}: arity 1, 2, 4 with a
     *  literal renderTdsNull and the default date formats). */
    private static @com.legend.base.Nullable Printed csvOf(TypedNativeCall csv) {
        boolean renderTdsNull;
        switch (csv.args().size()) {
            case 1 -> renderTdsNull = false;
            case 2, 4 -> {
                if (!(csv.args().get(csv.args().size() - 1) instanceof TypedCBoolean b)) {
                    return null;
                }
                if (csv.args().size() == 4 && !(csv.args().get(1) instanceof TypedCString f1
                        && f1.value().equals("%t{yyyy-MM-dd HH:mm:ss}") && csv.args().get(2) instanceof TypedCString f2
                        && f2.value().equals("%t{yyyy-MM-dd}"))) {
                    return null;
                }
                renderTdsNull = b.value();
            }
            default -> {
                return null;
            }
        }
        // main's renderForm names CSVTEXT for the one-argument toCSV alone
        return new Printed(csv.args().get(0), "CSV", csv.args().size() == 1 ? "CSVTEXT" : null, renderTdsNull, null);
    }

    /** Whether the arm claims an equality over {@code args}: on, and a side prints at its root. */
    static boolean claims(List<TypedSpec> args, List<TypedSpec> letPrefix) {
        return ON && args.size() >= 2
                && (printed(args.get(0), letPrefix) != null || printed(args.get(1), letPrefix) != null);
    }

    /** {@code ask}'s assert planned: a printed side asked as its relation (rows), the other as the value it is; the
     *  judgment the edge's record, no appeal. Null where the arm does not claim it. */
    static HostSides.@com.legend.base.Nullable Check plan(TextAsk ask) {
        TypedSpec bare = EngineTextPlanning.bare(ask);
        List<TypedSpec> letPrefix = EngineTextPlanning.letPrefix(ask);
        com.legend.builtin.NativeFn.Verdict fn = com.legend.builtin.NativeFn.Verdict.of(
                com.legend.compiler.spec.typed.Calls.calleeIdOf(bare)).orElse(null);
        if (fn != com.legend.builtin.NativeFn.Verdict.ASSERT_EQUALS
                && fn != com.legend.builtin.NativeFn.Verdict.ASSERT_NOT_EQUALS) {
            return null;
        }
        List<TypedSpec> raw = bare instanceof TypedUserCall u ? u.args() : ((TypedNativeCall) bare).args();
        List<TypedSpec> args = com.legend.compiler.spec.ExecuteChainAssembly.narrowSideStamps(raw, letPrefix,
                EngineTextPlanning.specs(ask));
        if (!claims(args, letPrefix)) {
            return null;
        }
        Printed[] p = {printed(args.get(0), letPrefix), printed(args.get(1), letPrefix)};
        List<HostSides.Ask> asks = new ArrayList<>(2);
        int printedBits = 0;
        for (int i = 0; i < 2; i++) {
            printedBits |= p[i] == null ? 0 : 1 << i;
            asks.add(EngineTextPlanning.ask(p[i] == null ? args.get(i) : p[i].relation(), false, true,
                    EngineTextPlanning.hook(ask)));
        }
        // main's route for the pair: renderedPlan's (both forms null, or two different forms: no render pair)
        String eForm = p[0] == null ? null : p[0].mainForm();
        String aForm = p[1] == null ? null : p[1].mainForm();
        String mainForm = (eForm == null && aForm == null) || (eForm != null && aForm != null && !eForm.equals(aForm))
                ? null : aForm != null ? aForm : eForm;
        TypedSpec rendered = aForm != null ? args.get(1) : args.get(0);
        // HostJudge.planRendered's order fact: one side rendered, its chain sorted
        boolean sorted = mainForm != null && (eForm == null ^ aForm == null)
                && com.legend.compiler.spec.OrderView.of(rendered, letPrefix) == com.legend.compiler.spec.OrderView.SORTED;
        boolean wantEqual = fn == com.legend.builtin.NativeFn.Verdict.ASSERT_EQUALS;
        return EngineTextPlanning.check(asks, new HostJudgment.EdgeJudged(new RelationTextJudgment(fn.bareName(),
                wantEqual, printedBits, form(p[0]), form(p[1]), mainForm, aForm != null, sorted), RootLoop.NO_APPEAL,
                null, false));
    }

    /** The presenter's form of a side's print, as the record carries it: TDS, CSV, CSV+null, CSVJOIN:sep,
     *  CSVJOIN+null:sep, TDSCSV; empty for a side not printed. */
    private static String form(@com.legend.base.Nullable Printed p) {
        if (p == null) {
            return "";
        }
        return switch (p.kind()) {
            case "CSVJOIN" -> "CSVJOIN" + (p.renderTdsNull() ? "+null" : "") + ":" + p.sep();
            case "CSV" -> p.renderTdsNull() ? "CSV+null" : "CSV";
            default -> p.kind();
        };
    }

    // ---- the run -------------------------------------------------------------------------------------------------

    /** {@code j} judged over its two sides as the run read them: each printed side presented, then the two texts
     *  compared as main compares the pair (see the class's note). */
    static TypedSide judge(RelationTextJudgment j, List<TypedSide> sides) {
        String test = System.getProperty("legend.test.current");
        String expected;
        String actual;
        try {
            expected = text(sides.get(0), j.eForm(), j);
            actual = text(sides.get(1), j.aForm(), j);
        } catch (RuntimeException e) {
            System.out.println("[H11] REFUSED " + test + " forms=" + j.eForm() + "|" + j.aForm() + " :: "
                    + String.valueOf(e.getMessage()).replace('\n', ' '));
            throw e;
        }
        COUNT.incrementAndGet();
        boolean equal = expected.equals(actual);
        boolean held;
        String how;
        if (j.mainForm() != null) {
            // main's renderedOf: the strings; the compatibility rules' ruled comparison where they differ
            String golden = j.renderedIsActual() ? expected : actual;
            String ours = j.renderedIsActual() ? actual : expected;
            boolean ruled = !equal && UpstreamRenderedTexts.INSTANCE.held(golden, ours, j.mainForm(), j.sorted());
            held = (equal || ruled) == j.wantEqual();
            how = equal ? "BYTE" : ruled ? "RULED" : "DIFF";
        } else {
            held = equal == j.wantEqual();
            how = equal ? "BYTE" : "DIFF";
        }
        // the spike's measurement line: one per comparison the arm judged, under the running test's name
        System.out.println("[H11] " + (held ? "HELD" : "DIFFERS") + " " + test + " how=" + how + " forms="
                + j.eForm() + "|" + j.aForm() + " main=" + j.mainForm() + " sorted=" + j.sorted() + " chars="
                + expected.length());
        if (!equal && Boolean.getBoolean("legend.spike.relationText.dump")) {
            System.out.println("[H11-text] " + test + " expected=" + RelationText.representation(expected));
            System.out.println("[H11-text] " + test + " actual  =" + RelationText.representation(actual));
        }
        if (held) {
            if (j.mainForm() != null) {
                com.legend.diagnostics.VerdictCensus.sqlRoute(j.name(), "rendered-text");
            }
            return com.legend.testexecutionplan.Verdicts.ok();
        }
        if (j.mainForm() != null) {
            // main's words for a rendered pair (HostJudgments.renderedOf)
            return com.legend.testexecutionplan.Verdicts.fail(j.name() + " (rendered " + j.mainForm() + "): "
                    + com.legend.testexecutionplan.Verdicts.firstTextDiff(expected, actual));
        }
        // legend-pure's words of a failed assertEquals ('\nexpected: %r\nactual:   %r'), each text its %r
        return com.legend.testexecutionplan.Verdicts.fail("\nexpected: " + RelationText.representation(expected)
                + "\nactual:   " + RelationText.representation(actual));
    }

    private static final java.util.Set<String> VALUE_PRINTS = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /** SPIKE (H11): the value-print census -- a print whose text is the edge's (legend-pure's, not the product's), met
     *  at LOWERING, once per (test, facet). In the corpus nearly every value print is on an assert's side (H2: 2
     *  query-side sites in all), so this counts the judge-side value prints that need the edge's text. */
    static void valuePrint(String facet) {
        if (ON && VALUE_PRINTS.add(System.getProperty("legend.test.current") + " " + facet)) {
            System.out.println("[H11-vp] " + facet + " " + System.getProperty("legend.test.current"));
        }
    }

    /** How many comparisons the arm judged in this JVM (the spike's count). */
    static final java.util.concurrent.atomic.AtomicInteger COUNT = new java.util.concurrent.atomic.AtomicInteger();

    private static String text(TypedSide side, String form, RelationTextJudgment j) {
        if (!form.isEmpty()) {
            if (!(side instanceof TypedSide.Tabular t)) {
                throw new IllegalStateException(j.name() + " (relation text " + form + "): the printed side is no"
                        + " rows but a " + side.getClass().getSimpleName());
            }
            List<RelationText.Col> cols = new ArrayList<>(t.columns().size());
            for (com.legend.testexecutionplan.Column c : t.columns()) {
                cols.add(new RelationText.Col(c.name(), c.pureType()));
            }
            List<List<Object>> rows = new ArrayList<>(t.rows().size());
            for (com.legend.testexecutionplan.Row r : t.rows()) {
                rows.add(r.values());
            }
            if (form.equals("TDS")) {
                return RelationText.tds(cols, rows);
            }
            if (form.equals("TDSCSV")) {
                return RelationText.tdsCsvProperty(cols, rows);
            }
            if (form.startsWith("CSVJOIN")) {
                String sep = form.substring(form.indexOf(':') + 1);
                // Pure's replace: every '\n' of the CSV text, the cells' own included
                return RelationText.csv(cols, rows, form.startsWith("CSVJOIN+null")).replace("\n", sep);
            }
            return RelationText.csv(cols, rows, form.equals("CSV+null"));
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
