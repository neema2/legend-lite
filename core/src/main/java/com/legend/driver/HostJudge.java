// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.driver;
import com.legend.builtin.NativeFn;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.judge.TypedSide;
import com.legend.judge.PureAsserts;
import java.util.ArrayList;
import java.util.List;

/**
 * THE HOST JUDGE (task #27, 2026-09-21 — split out of AssertVerdicts): both sides of an
 * assert EXECUTE in the database and Java compares the rows ({@link com.legend.judge.Equality},
 * {@link com.legend.judge.PureAsserts}, {@link com.legend.judge.TdsCompare}) — the verdict of
 * record in host mode; in agreement mode the database judge's peer. The router
 * ({@link AssertVerdicts}) classifies the assert and hands the sides over; the database
 * judge ({@link DatabaseJudge}) is the other arm of the same router.
 */

final class HostJudge {
    private HostJudge() {
    }

    static TypedSide finish(String family, boolean wantEqual, boolean hostHeld,
            java.util.function.Supplier<@com.legend.base.Nullable String> hostMessage) {
        return finish(family, wantEqual, hostHeld, hostMessage, () -> {
            throw new IllegalStateException(family + ": an assertNotEquals with no sides to name");
        });
    }

    /** {@link #finish} where the assert may be {@code assertNotEquals}: {@code notEqualMessage} its upstream
     *  message over the two sides. */
    static TypedSide finish(String family, boolean wantEqual, boolean hostHeld,
            java.util.function.Supplier<@com.legend.base.Nullable String> hostMessage,
            java.util.function.Supplier<String> notEqualMessage) {
        // the host judge's verdict (P1, 2026-11-07: the byte channel beside it -- a canon rider built and declined at
        // run since the plan path's step 6, 2026-10-18 -- is deleted; the database judge is the referee)
        if (hostHeld == wantEqual) {
            return AssertVerdicts.ok();
        }
        if (!wantEqual) {
            return AssertVerdicts.fail(notEqualMessage.get());
        }
        String d = hostMessage.get();
        if (d == null) {
            throw new IllegalStateException(family
                    + ": verdict/message divergence — the host lattice"
                    + " failed but its message lattice held");
        }
        return AssertVerdicts.fail(d);
    }

    // ── §8 LEG 1 (grid canon, user-ratified 2026-08-28): a TABULAR side is judged by the host cell lattice, and the
    // failure message derives from the SAME judgment that failed (the reverted attempt's 28-row phantom: message and
    // judgment from different lattices -- structurally impossible here). The byte channel that rode beside it is
    // deleted (P1, 2026-11-07).

    /** A grid side's declared column kinds, in order; none for a side that is no grid (its cells keep their own). */
    private static List<String> columnKinds(TypedSide.@com.legend.base.Nullable Tabular grid) {
        List<String> kinds = new ArrayList<>();
        if (grid != null) {
            grid.columns().forEach(c -> kinds.add(c.pureType()));
        }
        return kinds;
    }

    /** The FLAT-CELLS verdict for a pair with at least one TABULAR
     * AssertVerdicts.side (both-wrapped pairs took the grid-pair arm earlier). */
    static TypedSide tdsRowValuesVerdict(String name,
            boolean wantEqual, HostJudgment.FlatFacts flat,
            SideFetch ef, SideFetch af,
            boolean incidental, com.legend.executionplan.ExecutionPlan.JudgingRules rules) {
        List<Object> e = ef.values();
        List<Object> a = af.values();
        // audit 22b F2: raw cells (a bare .rows view) never equal a
        // WHOLE-TDS value — flattening the TDS side would fabricate a
        // match its column-name pin refuses. Static stamps decide (read
        // when the arm was planned: the flat facts).
        boolean mixedFlatVsTds =
                (flat.bareRowStampE() && af.grid() != null && flat.wrappedRelationStampA())
                || (flat.bareRowStampA() && ef.grid() != null && flat.wrappedRelationStampE());
        // each side's cells typed by ITS OWN declaration (step 2b): a grid's declared column types decide its cells
        // (the engine's boundary rule on the wire), a literal's cells keep their own kinds -- never the peer's
        // declaration stamped on them (the user's ruling, 2026-11-03: Integer 1 against a Float column's 1.0 differs)
        List<String> eKinds = columnKinds(ef.grid());
        List<String> aKinds = columnKinds(af.grid());
        boolean hostHeld;
        if (mixedFlatVsTds) {
            hostHeld = false;
        } else {
            List<com.legend.judge.Equality.Typed> te = com.legend.judge.Equality.grid(e, eKinds);
            List<com.legend.judge.Equality.Typed> ta = com.legend.judge.Equality.grid(a, aKinds);
            hostHeld = com.legend.judge.Equality.ordered(te, ta, rules) == null;
            if (!hostHeld && incidental && e.size() == a.size()) {
                // ROW COHESION (audit 9): the incidental-order fallback
                // matches ROW TUPLES of the grid's width — cross-row
                // cell shuffles must FAIL; width 1 = the pool multiset
                TypedSide.Tabular g = af.grid() != null ? af.grid() : ef.grid();
                int w = g != null ? g.columns().size() : 1;
                hostHeld = com.legend.judge.Equality.rowMultiset(
                        te, ta, w > 1 && e.size() % w == 0 ? w : 1, rules);
            }
        }
        return finish(name, wantEqual, hostHeld,
                () -> mixedFlatVsTds
                        ? name + " (TDSRow.values) raw cells do not"
                                + " equal a whole TDS value"
                        : tdsHostMessage(name, PureAsserts.assertEqualsTyped(
                                com.legend.judge.Equality.grid(e, eKinds),
                                com.legend.judge.Equality.grid(a, aKinds), rules)),
                () -> PureAsserts.notEqualsMessage(e, a));
    }

    /** The TDSRow.values failure narrative — the host lattice's text, upstream's own ('\nexpected: %r\nactual:   %r'),
     * with no prefix of lite's (2026-11-03, the core parity program: the database judge writes upstream's words, and the
     * "assertEquals (TDSRow.values)" prefix was lite's); null iff the lattice held. */
    static @com.legend.base.Nullable String tdsHostMessage(String name,
            @com.legend.base.Nullable String d) {
        return d;
    }

    /** The MULTISET flat-cells verdict: loose CELL pool host lattice
     * (direction-aware sentinel — pool matching, never a sorted zip:
     * sorting separates an expected 'TDSNull' from its NULL cell). */
    static TypedSide tdsRowValuesSameElements(String family,
            SideFetch ef, SideFetch af, com.legend.executionplan.ExecutionPlan.JudgingRules rules) {
        List<Object> e = ef.values();
        List<Object> a = af.values();
        boolean hostHeld = e.size() == a.size()
                && com.legend.judge.Equality.rowMultiset(
                        com.legend.judge.Equality.Typed.all(e, null),
                        com.legend.judge.Equality.Typed.all(a, null), 1, rules);
        return finish(family, true, hostHeld,
                () -> {
                    String d = PureAsserts.assertSameElements(e, a, rules);
                    return d != null
                            ? tdsHostMessage(family, d)
                            : family + " (TDSRow.values): cell"
                                    + " multiset differs";
                });
    }

    /** Every wire value is an instance stamped {@code __type} = {@code cls}. */
    static boolean allWireType(List<Object> vals, String cls) {
        if (vals.isEmpty()) {
            return false;
        }
        for (Object v : vals) {
            if (!(v instanceof java.util.Map<?, ?> m)
                    || !cls.equals(m.get(com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE))) {
                return false;
            }
        }
        return true;
    }

    /** X5 — the HOST lattice's keyed-instance rule, applied as
     * MODEL-DRIVEN evidence projection at the K-arm: EqualityUtilities
     * compares a keyed class BY ITS KEY PROPERTIES ONLY, so both
     * sides' wire maps restrict to the key tree before the ONE lattice
     * judges (non-key fields are outside the equality relation — this
     * is the engine's rule, not a leniency). Keyless classes are
     * untouched (their sides never produce a non-null key tree). */
    static List<Object> restrictToKeys(List<Object> vals,
            com.legend.compiler.element.EqualityKeys keys, HostJudgment.Keys known) {
        List<Object> out = new ArrayList<>(vals.size());
        for (Object v : vals) {
            out.add(restrictOne(v, keys, known));
        }
        return out;
    }

    static Object restrictOne(Object v,
            com.legend.compiler.element.EqualityKeys keys, HostJudgment.Keys known) {
        if (!(v instanceof java.util.Map<?, ?> m)) {
            return v;
        }
        var out = new java.util.LinkedHashMap<String, Object>();
        // the CLASSIFIER first, then the keys -- upstream's equal on objects (EqualityUtilities: the classifiers
        // compared before the equality keys): a C and its subclass D with equal keys differ (the user's ruling,
        // 2026-11-03: never more lenient than upstream)
        if (m.containsKey("__type")) {
            out.put("__type", m.get("__type"));
        }
        for (var k : keys.keys()) {
            Object val = m.get(k.name());
            if (val != null) {
                val = val instanceof List<?> l
                        ? l.stream().map(x -> restrictNested(x, k.nested(), known))
                                .toList()
                        : restrictNested(val, k.nested(), known);
            }
            out.put(k.name(), val);
        }
        return out;
    }

    /** A value inside a key slot: restricted to the keys of the slot's
     * DECLARED class when that class is keyed, else to the keys of the
     * value's OWN class — the wire carries it as {@code __type}
     * (WORLD_MAP §4: a polymorphic slot's elements are judged by their
     * own classifier, the engine's rule), read from the keyed classes the
     * planning collected ({@code known}). A keyless class stays the
     * whole map (identity). */
    static Object restrictNested(Object v,
            com.legend.compiler.element.@com.legend.base.Nullable EqualityKeys declared,
            HostJudgment.Keys known) {
        if (declared != null) {
            return restrictOne(v, declared, known);
        }
        if (v instanceof java.util.Map<?, ?> m
                && m.get(com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE)
                        instanceof String own) {
            var keys = known.of(own);
            if (keys != null) {
                return restrictOne(v, keys, known);
            }
        }
        return v;
    }

    /** One assert side: its values (the host judge's, the gates', the declared policies'). §8 leg 1: a TABULAR side
     *  keeps its grid ({@code grid} non-null; {@code values} are the row-major CELLS, NULL slots kept -- engine
     *  TDSRow semantics, column names OUT). */
    record SideFetch(List<Object> values, TypedSide.@com.legend.base.Nullable Tabular grid) {
    }

    /** NUMERIC CHARTER Rule 3: both sides DECLARED Float — their kind is
     * Float whatever carrier the wire chose (PureAsserts judges by value). */
    /** A side's values paired with the kind its DECLARATION gives them — the
     * argument's type (a collection's element type); the judge lets a fine
     * primitive kind decide and takes the carrier's for Number / classes. */
    static List<com.legend.judge.Equality.Typed> typedSide(List<Object> values,
            HostJudgment.SideFacts arg) {
        return com.legend.judge.Equality.Typed.all(values, arg.declared());
    }

    /** A class-kind side's values with JSON object text decoded to structures ({@link AssertVerdicts#structured}). */
    static List<Object> structuredSide(HostJudgment.SideFacts side, List<Object> vals) {
        if (side.classFqn() == null) {
            return vals;
        }
        List<Object> out = new ArrayList<>(vals.size());
        for (Object v : vals) {
            out.add(AssertVerdicts.structured(v));
        }
        return out;
    }

    // ---- the arm's sides as one verdict step's base (the seam program's S2f, 2026-10-30) ----------------------

    /** A side as {@code AssertVerdicts.side} fetched it: folded where literal, no identity lane. */
    /** A statement read as a value side: what the walk's generic path plans for an assert no arm plans. */
    static HostSides.Ask statementAsk(TypedSpec statement, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return valueAsk(statement, hook);
    }

    private static HostSides.Ask valueAsk(TypedSpec arg, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return new HostSides.Ask(arg, false, true, hook);
    }

    /** A side on the IDENTITY LANE without a canon rider (F13c: the assert-condition/predicate evaluator; in-SQL
     *  eq/equal needs instance identity, and the boolean egress keeps every other lane blind to the field): folded
     *  where literal. */
    private static HostSides.Ask identityAsk(TypedSpec arg,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return new HostSides.Ask(arg, true, true, hook);
    }

    /** A side the canon fetch reads ({@link #sideCanonOf}): never folded, on the identity lane. */
    private static HostSides.Ask canonAsk(TypedSpec arg, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return new HostSides.Ask(arg, true, false, hook);
    }

    /** {@code r}, a side read on the identity lane (never folded: a literal evaluates in the database here too), as
     *  the arms judge it: a plan's result, decoded by its declared types (P1, 2026-11-07: the canon rider built and
     *  declined here since the plan path's step 6 is deleted, with the byte channel it fed). */
    static SideFetch sideCanonOf(TypedSide r) {
        if (r instanceof TypedSide.Tabular t) {
            List<Object> cells = AssertVerdicts.cells(t);
            com.legend.judge.CanonicalDivergence.v7SideRows(cells.size());
            return new SideFetch(cells, t);
        }
        // NUMERIC CHARTER Rule 2 (docs/NUMERIC_CHARTER_2026_09_17.md): a
        // side's cells are converted ONCE by the side's DECLARED kind —
        // IN SQL, on the side's own root select (the value-root envelope
        // the Lowerer applies to every Float-declared root), never here:
        // the referee judges values, it never evaluates them (tenet #1,
        // JavaEvalLedgerTest). Both sides of every verdict ride that same
        // path, so a DECIMAL carrier under a Float declaration and a Float
        // literal meet as the SAME kind (Rule 3 then judges same-kind,
        // same-value).
        return new SideFetch(AssertVerdicts.decodeSide(r), null);
    }

    /** Failure-message sketch of a frame (columns + row count — the
     * spec's toString(true) grid rendering is message-position only;
     * no witness pins its spelling). */
    static String summarize(TypedSide.Tabular t) {
        return t.columns().stream().map(com.legend.judge.Column::name)
                .toList() + " (" + t.rows().size() + " rows)";
    }

    // ---- the judgment as data (the test-is-the-product plan's M8a, 2026-11-01) ----------------------------------

    /**
     * THE ONE DISPATCH: {@code j}, the comparison a planned arm named with the facts it read at planning, judged over
     * {@code sides} (the arm's rows, shaped, in its order). Every arm's function takes the record and the rows and
     * nothing else -- no typed tree, no let prefix, no model, no environment (but for the engine-text asserts' appeal,
     * {@link HostJudgment.EngineText}, the one named exception: M10b): the sink holds data. Null where the
     * judgment declines the sides it read (the walk's generic path continues).
     */
    static @com.legend.base.Nullable TypedSide judge(HostJudgment j, List<TypedSide> sides, com.legend.executionplan.ExecutionPlan.JudgingRules rules) {
        return judge(j, sides, rules, null);
    }

    /** {@code message}: the reader of the assert's caller's message where the verdict carries it apart from its sides
     *  (ExecutionPlan.Verdict.message), asked only where the assert fails; null where the message, if any, is one of
     *  {@code sides} (a judgment nested in another's, whose sides the step read together). */
    static @com.legend.base.Nullable TypedSide judge(HostJudgment j, List<TypedSide> sides,
            com.legend.executionplan.ExecutionPlan.JudgingRules rules,
            java.util.function.@com.legend.base.Nullable Supplier<TypedSide> message) {
        java.util.Objects.requireNonNull(rules, "a judgment names the rules it judges by");
        return switch (j) {
            case HostJudgment.TdsEquivalent t -> tdsEquivalentOf(t, sides);
            case HostJudgment.GridPair g -> gridPairOf(g, sides, rules);
            case HostJudgment.Size s -> sizeOf(s, sides);
            case HostJudgment.Contains c -> containsOf(c, sides, rules);
            case HostJudgment.Tolerance t -> toleranceOf(sides);
            case HostJudgment.Condition c -> conditionOf(c, sides);
            case HostJudgment.AllRows a -> allRowsOf(a, sides);
            case HostJudgment.Rows r -> rowsOf(r, sides, rules);
            case HostJudgment.Unrolled u -> unrolledOf(u, sides, rules);
            case HostJudgment.Subset s -> subsetOf(s, sides, rules);
            case HostJudgment.Empty e -> emptyOf(e, sides.get(0));
            case HostJudgment.Eq e -> eqOf(e, sides, rules);
            case HostJudgment.InstanceOf i -> instanceOfOf(i, sides.get(0));
            case HostJudgment.Is i -> isOf(i, sides);
            case HostJudgment.Decided d -> decided(d);
            case HostJudgment.SameElements s -> sameElementsOf(s, sides, rules);
            case HostJudgment.Equals e -> equalsOf(e, sides, rules);
            case HostJudgment.Json js -> jsonOf(js, sides, rules);
            case HostJudgment.Rendered r -> renderedOf(r, sides, rules);
            case HostJudgment.Raise r -> throw com.legend.judge.Failures.raised(r.failure());
            case HostJudgment.EngineText e -> SqlTextVerdicts.judge(e, sides);
            case HostJudgment.AssertError a -> AssertErrorNative.judge(a, () -> sides, rules);
            case HostJudgment.Messaged m -> messagedOf(m, sides, rules, message);
        };
    }

    /** A verdict decided at planning: it compares nothing, so no rules are asked of it. */
    static TypedSide decided(HostJudgment.Decided d) {
        return d.held() ? AssertVerdicts.ok()
                : AssertVerdicts.fail(java.util.Objects.requireNonNull(d.message(), "a failed verdict's message"));
    }

    /**
     * {@code j} judged over its sides AS THE RUNNER READ THEM (the walker-deletion program, 2026-11-03): {@code sides}
     * hands their rows, or raises the error reading them raised -- every judgment's one entry at its verdict step. The
     * judgment decides what an error is: the answer of the one that asks for an error ({@link HostJudgment.AssertError}:
     * the error its function raises), every other's failure, raised as it was.
     */
    static @com.legend.base.Nullable TypedSide judge(HostJudgment j,
            java.util.function.Supplier<List<TypedSide>> sides, com.legend.executionplan.ExecutionPlan.JudgingRules rules,
            java.util.function.@com.legend.base.Nullable Supplier<TypedSide> message) {
        return j instanceof HostJudgment.AssertError a ? AssertErrorNative.judge(a, sides, rules)
                : judge(j, sides.get(), rules, message);
    }

    /** The static facts of one side, read once at planning. */
    private static HostJudgment.SideFacts facts(TypedSpec s) {
        return HostJudgment.SideFacts.of(s);
    }

    /** {@link #tdsEquivalent} planned: the two grids and the tolerances, one verdict's base (M6: the walk read the
     *  tolerances after the grids, a group of their own; literal tolerances send nothing either way). */
    static HostSides.Check planTdsEquivalent(List<TypedSpec> targs,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        List<HostSides.Ask> asks = new java.util.ArrayList<>(List.of(valueAsk(targs.get(0), hook),
                valueAsk(targs.get(1), hook), valueAsk(targs.get(2), hook)));
        if (targs.size() == 4) {
            asks.add(valueAsk(targs.get(3), hook));
        }
        return new HostSides.Check(asks, new HostJudgment.TdsEquivalent(targs.size() == 4));
    }

    private static @com.legend.base.Nullable TypedSide tdsEquivalentOf(HostJudgment.TdsEquivalent j,
            List<TypedSide> sides) {
        TypedSide.Tabular one = sides.get(0) instanceof TypedSide.Tabular t0 ? t0 : null;
        TypedSide.Tabular two = sides.get(1) instanceof TypedSide.Tabular t1 ? t1 : null;
        if (one == null || two == null) {
            return null;   // non-tabular shape — fall through, loud later
        }
        double delta = ((Number) AssertVerdicts.one(AssertVerdicts.decodeSide(sides.get(2)),
                "assertTdsEquivalent delta")).doubleValue();
        double timeDelta = j.hasTimeDelta()
                ? ((Number) AssertVerdicts.one(AssertVerdicts.decodeSide(sides.get(3)),
                        "assertTdsEquivalent timeDelta")).doubleValue()
                : 0.0;
        List<String> c1 = one.columns().stream()
                .map(com.legend.judge.Column::name).toList();
        List<String> c2 = two.columns().stream()
                .map(com.legend.judge.Column::name).toList();
        if (!c1.equals(c2) || one.rows().size() != two.rows().size()) {
            return AssertVerdicts.fail("\n" + summarize(one) + "\n is not"
                    + " equivalent to:\n" + summarize(two));
        }
        String d = com.legend.judge.TdsCompare.tdsEquivalent(
                AssertVerdicts.cells(one), AssertVerdicts.cells(two), delta, timeDelta);
        return d == null ? AssertVerdicts.ok() : AssertVerdicts.fail(d);
    }

    /** The GRID PAIR ({@code assertEquals} / {@code assertNotEquals} over two statically relation-stamped sides,
     *  D3): both grids executed in definition order, the grid owner ({@link com.legend.judge.TdsCompare#grids}:
     *  columns ordered, rows under the order view of the actual side) judges -- never a byte decline. */
    static HostSides.Check planGridPair(String name, boolean wantEqual, List<TypedSpec> args,
            List<TypedSpec> letPrefix, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        boolean sorted = com.legend.compiler.spec.OrderView.of(args.get(1), letPrefix)
                == com.legend.compiler.spec.OrderView.SORTED;
        return new HostSides.Check(List.of(valueAsk(args.get(0), hook), valueAsk(args.get(1), hook)),
                new HostJudgment.GridPair(name, wantEqual, sorted));
    }

    private static TypedSide gridPairOf(HostJudgment.GridPair j, List<TypedSide> grids, com.legend.executionplan.ExecutionPlan.JudgingRules rules) {
        TypedSide.Tabular te = grids.get(0) instanceof TypedSide.Tabular t0 ? t0 : null;
        TypedSide.Tabular ta = grids.get(1) instanceof TypedSide.Tabular t1 ? t1 : null;
        if (te == null || ta == null) {
            throw new com.legend.error.NotImplementedException(
                    "relation-stamped assert side executed"
                            + " to a non-tabular result");
        }
        boolean held = com.legend.judge.TdsCompare.grids(te, ta, j.sortedOrder(), rules);
        com.legend.judge.CanonicalDivergence.sqlRoute(j.name(), "grid-pair");
        if (held != j.wantEqual()) {
            // upstream's words ('\nexpected: %r\nactual:   %r'; assertNotEquals.pure's '%r should not equal %r'), each
            // grid's rows as the database judge frames them (VerdictSql: no row '[]', one row its cells, several in
            // brackets) -- the two judges one text (2026-11-03, the core parity program: the summary
            // '[age] (2 rows) does not match' was lite's)
            String e = gridText(te);
            String a = gridText(ta);
            return AssertVerdicts.fail(j.wantEqual() ? "\nexpected: " + e + "\nactual:   " + a
                    : e + " should not equal " + a);
        }
        return AssertVerdicts.ok();
    }

    /** A grid's rows framed as the database judge frames a side's canons: none {@code []}, one its cells, several in
     *  brackets; a row's cells by {@code %r}, comma-separated. */
    private static String gridText(TypedSide.Tabular t) {
        List<String> rows = new ArrayList<>(t.rows().size());
        for (com.legend.judge.Row r : t.rows()) {
            rows.add(r.values().stream().map(com.legend.judge.PureAsserts::repr)
                    .collect(java.util.stream.Collectors.joining(", ")));
        }
        return rows.isEmpty() ? "[]" : rows.size() == 1 ? rows.get(0) : "[" + String.join(", ", rows) + "]";
    }

    static HostSides.Check planSize(List<TypedSpec> args, List<TypedSpec> letPrefix,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return new HostSides.Check(List.of(valueAsk(args.get(1), hook), valueAsk(args.get(0), hook)),
                new HostJudgment.Size(AssertVerdicts.envelopeValuesRead(args.get(0), letPrefix)));
    }

    private static TypedSide sizeOf(HostJudgment.Size j, List<TypedSide> both) {
        Object n = AssertVerdicts.one(AssertVerdicts.decodeSide(both.get(0)), "assertSize size");
        // D3: the size rule is per-result-kind — grid ROWS,
        // graph array length, values otherwise; the ONE-CARRIER
        // envelope rule ({@code $r.values} of a relation-rooted
        // execute holds one TDS) is the MODEL's
        // (TypedSide.envelopeCarriers), keyed by the READ
        // SHAPE exactly as the harness's cluster-34 arm.
        TypedSide r0 = both.get(1);
        long actual = switch (r0) {
            case null -> 0L;
            case TypedSide.Tabular t ->
                    j.envelopeValuesRead()
                            ? t.envelopeCarriers(t.rows().size())
                            : t.rows().size();
            case TypedSide.Graph g -> {
                Object p = com.legend.sql.Json.parse(g.json());
                yield p instanceof List<?> l ? l.size() : 1L;
            }
            default -> AssertVerdicts.decodeSide(r0).size();
        };
        boolean heldSize = n instanceof Number num
                && num.longValue() == actual;
        return heldSize ? AssertVerdicts.ok()
                : AssertVerdicts.fail("expected size: " + n + ", actual size: " + actual);
    }

    static HostSides.Check planContains(List<TypedSpec> args,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return new HostSides.Check(List.of(valueAsk(args.get(0), hook), valueAsk(args.get(1), hook)),
                new HostJudgment.Contains(facts(args.get(0)), facts(args.get(1))));
    }

    private static @com.legend.base.Nullable TypedSide containsOf(HostJudgment.Contains j, List<TypedSide> both,
            com.legend.executionplan.ExecutionPlan.JudgingRules rules) {
        List<Object> coll = AssertVerdicts.decodeSide(both.get(0));
        List<Object> val = AssertVerdicts.decodeSide(both.get(1));
        if (val.size() != 1) {
            return null;   // non-[1] value arg — generic path
        }
        boolean member = coll.stream().anyMatch(x ->
                com.legend.judge.Equality.same(com.legend.judge.Equality.Typed.of(x),
                        com.legend.judge.Equality.Typed.of(val.get(0)), rules));
        return member ? AssertVerdicts.ok()
                : AssertVerdicts.fail(com.legend.judge.PureAsserts.containsMessage(coll, val.get(0),
                        j.collection().classFqn(), j.value().classFqn()));
    }

    static HostSides.Check planTolerance(List<TypedSpec> args,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return new HostSides.Check(List.of(valueAsk(args.get(0), hook), valueAsk(args.get(1), hook),
                valueAsk(args.get(2), hook)), new HostJudgment.Tolerance());
    }

    private static TypedSide toleranceOf(List<TypedSide> three) {
        String d = com.legend.judge.PureAsserts.assertEqWithinTolerance(
                (Number) AssertVerdicts.one(AssertVerdicts.decodeSide(three.get(0)), "tolerance expected"),
                (Number) AssertVerdicts.one(AssertVerdicts.decodeSide(three.get(1)), "tolerance actual"),
                (Number) AssertVerdicts.one(AssertVerdicts.decodeSide(three.get(2)), "tolerance delta"));
        return d == null ? AssertVerdicts.ok() : AssertVerdicts.fail(d);
    }

    static HostSides.Check planCondition(String name, TypedSpec cond, boolean wantTrue,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        // forAll-contains SUBSET (the functionvariables idiom
        // — the harness's audited fc arm, moved to the owner):
        // both sides evaluate IN THE DATABASE; the membership
        // fold is assert-level logic judged host-side (a
        // subquery inside a SQL lambda cannot lower, and
        // pure's own evaluation of this shape is in-memory).
        TypedSpec[] fc = AssertVerdicts.forAllContains(cond);
        if (fc != null) {
            return new HostSides.Check(List.of(valueAsk(fc[0], hook), valueAsk(fc[1], hook)),
                    new HostJudgment.Subset(name, wantTrue));
        }
        // F13c: the CONDITION rides the identity lane — eq/
        // equal over instances compile the engine relation
        // (identity/key canon); the egress is one boolean, so
        // no other lane ever sees the identity field
        return new HostSides.Check(List.of(identityAsk(cond, hook)), new HostJudgment.Condition(name, wantTrue));
    }

    /** {@code check} with the caller's failure message (the asserts-as-plan-nodes program, step 2): the message one
     *  more side, folded where it is a constant (a literal costs no statement). */
    static HostSides.Check messaged(HostSides.Check check, TypedSpec message,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
        List<HostSides.Ask> asks = new java.util.ArrayList<>(check.asks());
        asks.add(new HostSides.Ask(message, false, true, hook));
        return new HostSides.Check(asks, new HostJudgment.Messaged(check.judgment(), check.asks().size()));
    }

    private static @com.legend.base.Nullable TypedSide messagedOf(HostJudgment.Messaged m, List<TypedSide> sides,
            com.legend.executionplan.ExecutionPlan.JudgingRules rules,
            java.util.function.@com.legend.base.Nullable Supplier<TypedSide> message) {
        try (var messaged = com.legend.judge.CallerMessage.judging()) {
            return judge(m.inner(), sides.subList(0, m.innerSides()), rules);
        } catch (com.legend.error.AssertFailed e) {
            if (e.unjudgedReason() != null) {
                throw e;
            }
            com.legend.testexecutionplan.Failure unplanned = m.unplanned();
            if (unplanned != null) {
                throw new com.legend.error.AssertFailed(
                        com.legend.compiler.spec.AssertMessages.unevaluated(unplanned.message()));
            }
            // read now, the assert having failed: through the verdict's reader, or the side after the inner ones
            TypedSide read = message != null ? message.get() : sides.get(m.innerSides());
            Object text = AssertVerdicts.one(AssertVerdicts.decodeSide(read), "the message");
            throw new com.legend.error.AssertFailed(String.valueOf(text));
        }
    }

    private static TypedSide conditionOf(HostJudgment.Condition j, List<TypedSide> one) {
        Object c = AssertVerdicts.one(AssertVerdicts.decodeSide(one.get(0)), j.name() + " condition");
        boolean held = Boolean.TRUE.equals(c) == j.wantTrue();
        return held ? AssertVerdicts.ok() : AssertVerdicts.fail("Assert failed");
    }

    private static TypedSide subsetOf(HostJudgment.Subset j, List<TypedSide> pair, com.legend.executionplan.ExecutionPlan.JudgingRules rules) {
        String name = j.name();
        boolean wantTrue = j.wantTrue();
        List<Object> need = AssertVerdicts.decodeSide(pair.get(0));
        List<Object> have = AssertVerdicts.decodeSide(pair.get(1));
        List<Object> missing = need.stream()
                .filter(n2 -> have.stream().noneMatch(h ->
                        com.legend.judge.Equality.same(com.legend.judge.Equality.Typed.of(n2),
                                com.legend.judge.Equality.Typed.of(h), rules)))
                .toList();
        boolean subsetHolds = missing.isEmpty();
        if (subsetHolds == wantTrue) {
            return AssertVerdicts.ok();
        }
        return AssertVerdicts.fail(name + " (forAll-contains subset):"
                + " missing " + missing);
    }

    static HostSides.Check planEmpty(TypedSpec arg, boolean wantEmpty,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        // §8 leg 1: a TABULAR side's emptiness is its ROW count
        // (engine relation semantics) — no canon involved
        return new HostSides.Check(List.of(valueAsk(arg, hook)), new HostJudgment.Empty(wantEmpty));
    }

    private static TypedSide emptyOf(HostJudgment.Empty j, TypedSide er) {
        boolean wantEmpty = j.wantEmpty();
        boolean empty = er instanceof TypedSide.Tabular te3
                ? te3.rows().isEmpty()
                : AssertVerdicts.decodeSide(er).isEmpty();
        boolean held = empty == wantEmpty;
        return held ? AssertVerdicts.ok()
                : AssertVerdicts.fail(wantEmpty
                        ? com.legend.judge.PureAsserts.notEmptyMessage(AssertVerdicts.decodeSide(er))
                        : "Expected non-empty collection");
    }

    static HostSides.Check planEq(List<TypedSpec> args, List<TypedSpec> letPrefix, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        HostJudgment.SideFacts e = facts(args.get(0));
        HostJudgment.SideFacts a = facts(args.get(1));
        HostJudgment.Keys keys = HostJudgment.Keys.of(args.get(0), args.get(1), env.ctx());
        return new HostSides.Check(List.of(canonAsk(args.get(0), hook), canonAsk(args.get(1), hook)),
                new HostJudgment.Eq(e, a, keys));
    }

    private static TypedSide eqOf(HostJudgment.Eq j, List<TypedSide> pair, com.legend.executionplan.ExecutionPlan.JudgingRules rules) {
        SideFetch ef = sideCanonOf(pair.get(0));
        SideFetch af = sideCanonOf(pair.get(1));
        Object ee = AssertVerdicts.one(ef.values(), "assertEq expected");
        Object aa = AssertVerdicts.one(af.values(), "assertEq actual");
        // eq's non-primitive identity rule throws LOUD here (P2-5)
        String d = com.legend.judge.PureAsserts.assertEq(ee, aa, rules);
        com.legend.judge.CanonicalDivergence.probeEqual("assertEq",
                java.util.Collections.singletonList(ee),
                java.util.Collections.singletonList(aa), d == null);
        return finish("assertEq", true, d == null, () -> d);
    }

    static HostSides.Check planInstanceOf(List<TypedSpec> args, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        // the classes the type accepts: itself and its subtree, the model's subtype relation read at planning (the
        // judgment reads the wire's classifier against the set); a type argument that is no reference names none, and
        // the judgment declines the side it read (the walk's generic path, as before)
        String type = AssertVerdicts.typeRefName(args.get(1));
        java.util.Set<String> accepted = new java.util.LinkedHashSet<>();
        if (type != null) {
            accepted.add(type);
            accepted.addAll(env.ctx().subtree(type));
        }
        return new HostSides.Check(List.of(valueAsk(args.get(0), hook)), new HostJudgment.InstanceOf(type, accepted));
    }

    private static @com.legend.base.Nullable TypedSide instanceOfOf(HostJudgment.InstanceOf j, TypedSide side) {
        Object v = AssertVerdicts.one(AssertVerdicts.decodeSide(side), "assertInstanceOf instance");
        String type = j.typeName();
        if (type == null) {
            return null;   // non-literal type arg — fall through
        }
        // a CLASS value's wire carries its classifier (__type,
        // batch 53): instanceOf is the model's subtype relation
        // (a property-less class such as NullLiteral has nothing
        // else on the wire)
        if (AssertVerdicts.structured(v) instanceof java.util.Map<?, ?> m
                && m.get(com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE)
                        instanceof String wireType) {
            boolean ok = j.acceptedTypes().contains(wireType);
            return ok ? AssertVerdicts.ok() : AssertVerdicts.fail("expected " + com.legend.judge.PureAsserts.repr(v,
                    wireType) + " to be an instance of " + type + ", actual: " + wireType);
        }
        String d = com.legend.judge.PureAsserts.assertInstanceOf(v, type);
        return d == null ? AssertVerdicts.ok() : AssertVerdicts.fail(d);
    }

    /** {@link #is} planned; null (no side asked) where the pair is not two elements. */
    static HostSides.@com.legend.base.Nullable Check planIs(String name, List<TypedSpec> args, SpecCompiler specs,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        // ELEMENT IDENTITY (metamodel-as-relations D2/D3): a tracked
        // element's identity is its row's primary key — `is` over an
        // element reference and a metamodel row is the same equality
        // the chain normalizer rewrites to a key compare
        // (ChainNormalizer.identityEquality); adjudicated as the
        // condition assert on the identity lane
        if (AssertVerdicts.elementTyped(args.get(0), specs) && AssertVerdicts.elementTyped(args.get(1), specs)) {
            TypedSpec cond = com.legend.resolver.ChainNormalizer.identityCondition(
                    specs.ctx(), args.get(0), args.get(1));
            return new HostSides.Check(List.of(identityAsk(cond, hook)), new HostJudgment.Is(name));
        }
        return null;
    }

    private static TypedSide isOf(HostJudgment.Is j, List<TypedSide> one) {
        Object c2 = AssertVerdicts.one(AssertVerdicts.decodeSide(one.get(0)), j.name() + " condition");
        return Boolean.TRUE.equals(c2) ? AssertVerdicts.ok()
                : AssertVerdicts.fail(j.name() + ": the element references differ");
    }

    static HostSides.Check planSameElements(List<TypedSpec> args, boolean gridPair, List<TypedSpec> letPrefix,
            BodyDriver.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        HostJudgment.SideFacts e = facts(args.get(0));
        HostJudgment.SideFacts a = facts(args.get(1));
        HostJudgment.Keys keys = HostJudgment.Keys.of(args.get(0), args.get(1), env.ctx());
        return new HostSides.Check(List.of(canonAsk(args.get(0), hook), canonAsk(args.get(1), hook)),
                new HostJudgment.SameElements(e, a, gridPair, keys));
    }

    private static TypedSide sameElementsOf(HostJudgment.SameElements j, List<TypedSide> pair, com.legend.executionplan.ExecutionPlan.JudgingRules rules) {
        SideFetch ef = sideCanonOf(pair.get(0));
        SideFetch af = sideCanonOf(pair.get(1));
        // §8 leg 1 — TABULAR sides under the MULTISET form:
        // loose CELL pool (the corpus writes flat expected sets
        // column-grouped — loose multiset IS this assert's
        // reference semantics, audit 9)
        if (ef.grid() != null || af.grid() != null) {
            return tdsRowValuesSameElements("assertSameElements", ef, af, rules);
        }
        // a CLASS-kind side that rode a JSON carrier (a polymorphic
        // Node[1] program value) arrives as object text: decode it
        // to the structure the key restriction reads (the executor's
        // own rule for JSON slots — WORLD_MAP §4, __type rides along)
        List<Object> eVals = structuredSide(j.e(), ef.values());
        List<Object> aVals = structuredSide(j.a(), af.values());
        var ik = j.keys().resolve(j.e(), j.a(), eVals, aVals);
        List<Object> e = ik != null
                ? restrictToKeys(eVals, ik, j.keys()) : eVals;
        List<Object> a = ik != null
                ? restrictToKeys(aVals, ik, j.keys()) : aVals;
        String d = com.legend.judge.Equality.sameElements(
                typedSide(e, j.e()), typedSide(a, j.a()), rules) == null
                ? null : com.legend.judge.PureAsserts.assertSameElements(e, a, j.e().classFqn(), j.a().classFqn(), rules);
        com.legend.judge.CanonicalDivergence.probeSameElements(
                e, a, d == null);
        // V4/V11 — the multiset BYTE VERDICT OF RECORD: rows
        // arrive ORDER BY canon text (the wrap's sort key) in
        // the SAME execution; the host multiset judgment above
        // is the parallel referee.
        return finish("assertSameElements", true, d == null,
                () -> d);
    }

    /** The VECTOR judgment planned (M10c item 5): one identity-lane side, the predicate vector; judged by
     *  {@link #allRowsOf} as {@link #quantified} judges its read. */
    static HostSides.Check planAllRows(String fqn, TypedSpec predMap, boolean wantTrue, String message,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return new HostSides.Check(List.of(identityAsk(predMap, hook)),
                new HostJudgment.AllRows(fqn, wantTrue, message));
    }

    /** Each element's planned assert judged over its slice of the sides, in order, counted as the walk counts an
     *  element it adjudicates (entered, its family; an engine-text route once answered): the last verdict, the first
     *  failure raising; an element whose judgment declines its sides is the walk's wall. */
    private static TypedSide unrolledOf(HostJudgment.Unrolled u, List<TypedSide> sides, com.legend.executionplan.ExecutionPlan.JudgingRules rules) {
        int at = 0;
        TypedSide last = null;
        for (int i = 0; i < u.parts().size(); i++) {
            AssertVerdicts.HostAssert part = u.parts().get(i);
            int n = u.askCounts().get(i);
            List<TypedSide> slice = sides.subList(at, at + n);
            at += n;
            com.legend.testexecutionplan.Failure before = part.beforeTheArm();
            if (before != null) {
                throw com.legend.judge.Failures.raised(before);
            }
            part.enter();
            TypedSide v = part.judge(slice, rules);
            if (v == null) {
                throw new com.legend.error.NotImplementedException(
                        "unrolled quantified assert: element verdict not adjudicable for " + part.listenerName());
            }
            last = v;
        }
        return last == null ? AssertVerdicts.ok() : last;
    }

    private static TypedSide allRowsOf(HostJudgment.AllRows j, List<TypedSide> sides) {
        for (Object v : AssertVerdicts.decodeSide(sides.get(0))) {
            if (Boolean.TRUE.equals(v) != j.wantTrue()) {
                return AssertVerdicts.fail(j.message());
            }
        }
        return AssertVerdicts.ok();
    }

    /** A quantified assert over rows planned for the program road (HostJudgment.Rows): its asks the zip's arms or the
     *  one relation source, its arguments read at planning -- a row's field, the row, or a literal; any other argument
     *  refused by name, never evaluated here (the walk's rule, {@link #rowValue}). Null: another family or source. */
    static @com.legend.base.Nullable HostSides.Check planRows(String fqn, TypedSpec source,
            com.legend.compiler.spec.typed.TypedLambda lam, TypedSpec root, List<TypedSpec> letPrefix,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        com.legend.builtin.NativeFn.Verdict family = com.legend.builtin.NativeFn.Verdict
                .of(com.legend.compiler.spec.typed.Calls.calleeIdOf(root)).orElse(null);
        boolean isEquals = family == com.legend.builtin.NativeFn.Verdict.ASSERT_EQUALS;
        boolean isTolerance = family == com.legend.builtin.NativeFn.Verdict.ASSERT_EQ_WITHIN_TOLERANCE;
        List<TypedSpec> args = com.legend.compiler.spec.typed.Calls.argsOf(root);
        if (!isEquals && !isTolerance || args.size() < (isTolerance ? 3 : 2)) {
            return null;
        }
        String binder = lam.parameters().get(0);
        List<HostJudgment.RowArg> read = new java.util.ArrayList<>();
        for (TypedSpec arg : args.subList(0, isTolerance ? 3 : 2)) {
            HostJudgment.RowArg a = rowArg(arg, binder, fqn);
            if (a == null) {
                return null;   // an argument computed per element: not this read's
            }
            read.add(a);
        }
        List<TypedSpec> arms = new java.util.ArrayList<>();
        TypedSpec bound = com.legend.compiler.spec.typed.Lets.bound(source, letPrefix);
        zipArms(bound, letPrefix, arms);
        List<HostSides.Ask> asks = new java.util.ArrayList<>(arms.size());
        for (TypedSpec arm : arms) {
            asks.add(valueAsk(arms.size() == 1 ? source : arm, hook));
        }
        HostJudgment.Pairing pairing = arms.size() == 1 ? null : pairing(bound, letPrefix, new int[] {0});
        return new HostSides.Check(asks, new HostJudgment.Rows(fqn, isTolerance, read, args.get(0).info(),
                args.get(1).info(), pairing));
    }

    /** A row assert's argument as data: {@link #rowValue}'s shapes, read at planning. */
    private static HostJudgment.@com.legend.base.Nullable RowArg rowArg(TypedSpec arg, String binder, String fqn) {
        return switch (arg) {
            case com.legend.compiler.spec.typed.TypedCast c -> rowArg(c.source(), binder, fqn);
            case com.legend.compiler.spec.typed.TypedVariable v when v.name().equals(binder) ->
                    new HostJudgment.RowArg.Whole();
            case com.legend.compiler.spec.typed.TypedPropertyAccess pa
                    when pa.source() instanceof com.legend.compiler.spec.typed.TypedVariable v
                            && v.name().equals(binder) -> new HostJudgment.RowArg.Field(pa.property());
            case com.legend.compiler.spec.typed.TypedCString l -> new HostJudgment.RowArg.Literal(l.value());
            case com.legend.compiler.spec.typed.TypedCInteger l -> new HostJudgment.RowArg.Literal(l.value());
            case com.legend.compiler.spec.typed.TypedCFloat l -> new HostJudgment.RowArg.Literal(l.value());
            case com.legend.compiler.spec.typed.TypedCBoolean l -> new HostJudgment.RowArg.Literal(l.value());
            // another argument is computed per element: the database computes it (the per-element form's operand
            // pairs, or the unroll's per-element sides), never this read of the row (2026-11-04)
            default -> null;
        };
    }

    /** A quantified equality element by element on the host (the asserts-as-plan-nodes program, step 5): its one
     *  side the operand pairs the database computes, each element judged as the element arm judges a pair
     *  ({@link com.legend.judge.PureAsserts}: the same decision, the same message) -- nothing computed here. */
    static HostSides.Check planPerElement(String fqn, TypedSpec pairs, com.legend.compiler.element.type.ExprType expected,
            com.legend.compiler.element.type.ExprType actual, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return new HostSides.Check(List.of(valueAsk(pairs, hook)), new HostJudgment.Rows(fqn, false,
                List.of(new HostJudgment.RowArg.Field("first"), new HostJudgment.RowArg.Field("second")), expected,
                actual, null));
    }

    /** The zip's pairing over its arms, in {@link #zipArms}' order. */
    private static HostJudgment.Pairing pairing(TypedSpec arm, List<TypedSpec> letPrefix, int[] next) {
        TypedSpec bound = com.legend.compiler.spec.typed.Lets.bound(arm, letPrefix);
        if (bound instanceof com.legend.compiler.spec.typed.TypedNativeCall z
                && com.legend.compiler.element.type.PlatformTypes.COLLECTION_ZIP.equals(z.callee().qualifiedName())
                && z.args().size() == 2) {
            HostJudgment.Pairing left = pairing(z.args().get(0), letPrefix, next);
            HostJudgment.Pairing right = pairing(z.args().get(1), letPrefix, next);
            return new HostJudgment.Pairing.Zip(left, right);
        }
        return new HostJudgment.Pairing.Arm(next[0]++);
    }

    /** {@link HostJudgment.Rows} judged over its sides: each row as the element arm judges it, the first failure. */
    private static @com.legend.base.Nullable TypedSide rowsOf(HostJudgment.Rows j, List<TypedSide> sides,
            com.legend.executionplan.ExecutionPlan.JudgingRules rules) {
        List<Object> rows;
        if (j.pairing() == null && !(sides.get(0) instanceof TypedSide.Tabular)) {
            // the per-element operand pairs (planPerElement): one value per element, each a {first, second} the
            // database computed, in the source's order
            rows = AssertVerdicts.decodeSide(sides.get(0));
        } else if (j.pairing() == null) {
            TypedSide.Tabular t = (TypedSide.Tabular) sides.get(0);
            rows = new java.util.ArrayList<>(t.rows().size());
            for (com.legend.judge.Row r : t.rows()) {
                java.util.Map<String, Object> m = new java.util.LinkedHashMap<>();
                for (int i = 0; i < t.columns().size(); i++) {
                    m.put(t.columns().get(i).name(), r.values().get(i));
                }
                rows.add(m);
            }
        } else {
            List<List<Object>> values = new java.util.ArrayList<>(sides.size());
            for (TypedSide r : sides) {
                values.add(r instanceof TypedSide.Tabular t ? AssertVerdicts.cells(t) : AssertVerdicts.decodeSide(r));
            }
            rows = pairedOf(j.pairing(), values);
        }
        for (Object row : rows) {
            Object e = rowArgValue(j.args().get(0), row, j.name());
            Object a = rowArgValue(j.args().get(1), row, j.name());
            String d = j.tolerance()
                    ? com.legend.judge.PureAsserts.assertEqWithinTolerance(number(e, j.name()), number(a, j.name()),
                            number(rowArgValue(j.args().get(2), row, j.name()), j.name()))
                    : com.legend.judge.PureAsserts.assertEquals(
                            java.util.Collections.singletonList(e), java.util.Collections.singletonList(a),
                            com.legend.compiler.element.EqualityKeys.fqnOf(j.expectedInfo().type()),
                            com.legend.compiler.element.EqualityKeys.fqnOf(j.actualInfo().type()), rules);
            if (d != null) {
                return AssertVerdicts.fail(d);
            }
        }
        return AssertVerdicts.ok();
    }

    private static List<Object> pairedOf(HostJudgment.Pairing p, List<List<Object>> values) {
        return switch (p) {
            case HostJudgment.Pairing.Arm arm -> values.get(arm.index());
            case HostJudgment.Pairing.Zip z -> {
                List<Object> left = pairedOf(z.left(), values);
                List<Object> right = pairedOf(z.right(), values);
                int n = Math.min(left.size(), right.size());
                List<Object> out = new java.util.ArrayList<>(n);
                for (int i = 0; i < n; i++) {
                    java.util.Map<String, Object> pair = new java.util.LinkedHashMap<>();
                    pair.put("first", left.get(i));
                    pair.put("second", right.get(i));
                    out.add(pair);
                }
                yield out;
            }
        };
    }

    private static @com.legend.base.Nullable Object rowArgValue(HostJudgment.RowArg arg, Object row, String fqn) {
        return switch (arg) {
            case HostJudgment.RowArg.Whole w -> row;
            case HostJudgment.RowArg.Literal l -> l.value();
            case HostJudgment.RowArg.Field f -> {
                if (!(row instanceof java.util.Map<?, ?> m) || !m.containsKey(f.name())) {
                    throw new com.legend.error.NotImplementedException(fqn + ": the quantified assert's row holds no"
                            + " field '" + f.name() + "': " + row);
                }
                yield m.get(f.name());
            }
        };
    }

    /** The arms of a (nested) zip, left to right; a source that is no zip is its own one arm. */
    private static void zipArms(TypedSpec source, List<TypedSpec> letPrefix, List<TypedSpec> out) {
        if (source instanceof com.legend.compiler.spec.typed.TypedNativeCall z
                && com.legend.compiler.element.type.PlatformTypes.COLLECTION_ZIP.equals(z.callee().qualifiedName())
                && z.args().size() == 2) {
            for (TypedSpec arm : z.args()) {
                TypedSpec bound = com.legend.compiler.spec.typed.Lets.bound(arm, letPrefix);
                if (bound instanceof com.legend.compiler.spec.typed.TypedNativeCall inner
                        && com.legend.compiler.element.type.PlatformTypes.COLLECTION_ZIP.equals(
                                inner.callee().qualifiedName())) {
                    zipArms(bound, letPrefix, out);
                } else {
                    out.add(arm);
                }
            }
            return;
        }
        out.add(source);
    }

    private static Number number(@com.legend.base.Nullable Object v, String fqn) {
        if (v instanceof Number n) {
            return n;
        }
        throw new com.legend.error.NotImplementedException(fqn + ": assertEqWithinTolerance over a row whose value is"
                + " not a number: " + v);
    }

    static HostSides.Check planEquals(String name, boolean wantEqual, List<TypedSpec> args, boolean incidental,
            boolean gridPair, List<TypedSpec> letPrefix, BodyDriver.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        HostJudgment.SideFacts e = facts(args.get(0));
        HostJudgment.SideFacts a = facts(args.get(1));
        HostJudgment.Keys keys = HostJudgment.Keys.of(args.get(0), args.get(1), env.ctx());
        HostJudgment.FlatFacts flat = new HostJudgment.FlatFacts(
                AssertVerdicts.bareRowStamp(args.get(0), letPrefix), AssertVerdicts.bareRowStamp(args.get(1), letPrefix),
                AssertVerdicts.wrappedRelationStamp(args.get(0), letPrefix),
                AssertVerdicts.wrappedRelationStamp(args.get(1), letPrefix));
        return new HostSides.Check(List.of(canonAsk(args.get(0), hook), canonAsk(args.get(1), hook)),
                new HostJudgment.Equals(name, wantEqual, incidental, gridPair, e, a, keys, flat));
    }

    private static TypedSide equalsOf(HostJudgment.Equals j, List<TypedSide> pair, com.legend.executionplan.ExecutionPlan.JudgingRules rules) {
        String name = j.name();
        boolean wantEqual = j.wantEqual();
        boolean incidental = j.incidental();
        SideFetch ef = sideCanonOf(pair.get(0));
        SideFetch af = sideCanonOf(pair.get(1));
        // the FLAT-CELLS verdict (the host cell lattice)
        if (ef.grid() != null || af.grid() != null) {
            return tdsRowValuesVerdict(name, wantEqual, j.flat(), ef, af, incidental, rules);
        }
        // X5: a same-class KEYED pair restricts both sides to
        // the key tree — the engine's own equality relation for
        // keyed classes, applied before the lattice judges
        // a CLASS-kind side that rode a JSON carrier (a polymorphic
        // Node[1] program value) arrives as object text: decode it
        // to the structure the key restriction reads (the executor's
        // own rule for JSON slots — WORLD_MAP §4, __type rides along)
        List<Object> eVals = structuredSide(j.e(), ef.values());
        List<Object> aVals = structuredSide(j.a(), af.values());
        var ik = j.keys().resolve(j.e(), j.a(), eVals, aVals);
        List<Object> e = ik != null
                ? restrictToKeys(eVals, ik, j.keys()) : eVals;
        List<Object> a = ik != null
                ? restrictToKeys(aVals, ik, j.keys()) : aVals;
        List<com.legend.judge.Equality.Typed> te = typedSide(e, j.e());
        List<com.legend.judge.Equality.Typed> ta = typedSide(a, j.a());
        boolean equal = incidental
                ? com.legend.judge.Equality.sameElements(te, ta, rules) == null
                : com.legend.judge.Equality.ordered(te, ta, rules) == null;
        // R1a divergence instrument (CANONICAL_FORM_SPEC §0): the host lattice vs a canonical render of the same
        // values, measurement only (the byte verdict it once shadowed is deleted, P1, 2026-11-07)
        com.legend.judge.CanonicalDivergence.probeEqual(
                name, e, a, equal, incidental);
        return finish(name, wantEqual, equal,
                () -> incidental
                        ? com.legend.judge.PureAsserts.assertSameElements(e, a, j.e().classFqn(), j.a().classFqn(), rules)
                        : com.legend.judge.PureAsserts.assertEquals(e, a, j.e().classFqn(), j.a().classFqn(), rules),
                () -> com.legend.judge.PureAsserts.notEqualsMessage(e, a));
    }

    /** Upstream's {@code assertEquals} failure text for two values ({@code '\nexpected: %r\nactual:   %r'}, the host
     *  judge's spelling): an engine-text arm declining the texts its verdict step read, a plain String equality that
     *  failed (2026-11-04, the core program). */
    static String equalsMessage(Object expected, Object actual) {
        return "\nexpected: " + com.legend.judge.PureAsserts.repr(expected) + "\nactual:   "
                + com.legend.judge.PureAsserts.repr(actual);
    }

    static HostSides.Check planJsonStringsEqual(List<TypedSpec> args, List<TypedSpec> letPrefix,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return new HostSides.Check(List.of(valueAsk(args.get(0), hook), valueAsk(args.get(1), hook)),
                new HostJudgment.Json(com.legend.compiler.spec.OrderView.of(args.get(1), letPrefix)
                        == com.legend.compiler.spec.OrderView.INCIDENTAL));
    }

    private static @com.legend.base.Nullable TypedSide jsonOf(HostJudgment.Json j, List<TypedSide> pair,
            com.legend.executionplan.ExecutionPlan.JudgingRules rules) {
        String ejson = AssertVerdicts.jsonSideTextOf(pair.get(0));
        String ajson = AssertVerdicts.jsonSideTextOf(pair.get(1));
        if (ejson == null || ajson == null) {
            return null;   // non-[1]-string shape: generic path
        }
        // the GOLDEN side parses first and names itself when it does not: both texts read as the rules read JSON
        // (JudgeJson, register S144: strict in lite's own runs, upstream's json-simple grammar under the compatibility
        // rules -- the milestoned graph-fetch goldens end in a stray `"` json-simple reads past)
        Object expected;
        try {
            expected = JudgeJson.read(ejson, rules);
        } catch (IllegalStateException e) {
            throw new IllegalStateException(
                    "golden JSON does not parse: " + e.getMessage(), e);
        }
        Object actual = JudgeJson.read(ajson, rules);
        // no [x] for x (the user's ruling, 2026-11-03): a result serialized otherwise than the engine serializes it
        // is the serializer's to fix.
        // D3 for graph results: an INCIDENTAL-order chain (a root
        // read with no sort) has SQL arrival order on BOTH sides —
        // the golden's is H2's, ours DuckDB's — so the ROOT array
        // compares as a multiset, exactly the row verdict's policy
        // under the same compile-time fact; nested arrays stay
        // ordered (a property's order is the mapping's)
        boolean incidentalRoot = j.incidentalRoot();
        String diff = incidentalRoot
                ? com.legend.judge.Equality.pureJsonUnorderedRoot(expected, actual)
                : com.legend.judge.Equality.pureJson(expected, actual);
        return diff == null ? AssertVerdicts.ok()
                                : AssertVerdicts.fail("assertJsonStringsEqual: FIRST DIFF at "
                        + diff);
    
    }

    /** {@link #rendered} planned; {@code counted} whether the route is counted where the judgment holds (the host
     *  judge's own plan path counts it, as {@code AssertVerdicts.adjudicate} counts the walk's once the arm answered). */
    static HostSides.Check planRendered(String name, boolean wantEqual, List<TypedSpec> args, String form,
            TypedSpec rendered, @com.legend.base.Nullable String eForm, @com.legend.base.Nullable String aForm,
            boolean orderedForm, List<TypedSpec> letPrefix, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook,
            boolean counted) {
        // a BOTH-rendered pair always judges as a multiset (even
        // sorted queries legally tie-flip between two executions)
        boolean sorted = orderedForm
                && (eForm == null ^ aForm == null)
                && com.legend.compiler.spec.OrderView.of(rendered, letPrefix)
                        == com.legend.compiler.spec.OrderView.SORTED;
        return new HostSides.Check(List.of(valueAsk(args.get(0), hook), valueAsk(args.get(1), hook)),
                new HostJudgment.Rendered(name, wantEqual, form, eForm, aForm, sorted, counted));
    }

    private static TypedSide renderedOf(HostJudgment.Rendered j, List<TypedSide> pair,
            com.legend.executionplan.ExecutionPlan.JudgingRules rules) {
        String name = j.name();
        String form = j.form();
        List<Object> ev = AssertVerdicts.decodeSide(pair.get(0));
        List<Object> av = AssertVerdicts.decodeSide(pair.get(1));
        if (ev.size() == 1 && ev.get(0) instanceof String et
                && av.size() == 1 && av.get(0) instanceof String at) {
            // GENERIC FIRST (2026-11-04, the core program): the two strings compared as strings; the ruled policy (an
            // unsorted query's lines as a multiset, S131; float tokens within 2 ULP, S132) only where they differ
            // (measured on the corpus's DuckDB host pass: 240 of 269 texts byte-equal, 25 held by the policy)
            // clean mode (the user, 2026-11-05: after a verdict the harness may only explain): the two strings ARE the
            // verdict; the ruled row comparison after a failed text is compatibility mode's alone
            boolean held = et.equals(at)
                    || rules == com.legend.executionplan.ExecutionPlan.JudgingRules.COMPILED_PURE_COMPAT
                            && com.legend.judge.TdsCompare.renderedText(
                                    j.aForm() != null ? et : at, j.aForm() != null ? at : et,
                                    form, j.sortedOrder());
            if (held != j.wantEqual()) {
                return AssertVerdicts.fail(name + " (rendered " + form + "): "
                        + AssertVerdicts.firstTextDiff(et, at));
            }
            if (j.counted()) {
                com.legend.judge.CanonicalDivergence.sqlRoute(name, "rendered-text");
            }
            return AssertVerdicts.ok();
        }
        // a render form whose sides are not two strings — loud, never
        // a silent fall-through re-execution
        throw new com.legend.error.NotImplementedException(
                "rendered-text assert side is not a string pair ("
                        + form + ")");
    }
}
