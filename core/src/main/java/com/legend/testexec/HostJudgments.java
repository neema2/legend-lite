// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.testexecutionplan.JudgeJson;
import com.legend.testexecutionplan.KeyTree;

import static com.legend.testexecutionplan.Verdicts.decided;
import com.legend.testexecutionplan.HostJudgment;
import com.legend.testexecutionplan.TestPlan;
import com.legend.testexecutionplan.TypedSide;
import java.util.ArrayList;
import java.util.List;

final class HostJudgments {

    private HostJudgments() {
    }

    /** The platform's primitive types, named by their short names. */
    private static final java.util.Set<String> PRIMITIVES = java.util.Set.of("Boolean", "Integer", "Float", "Decimal",
            "Number", "String", "Date", "StrictDate", "DateTime", "StrictTime", "LatestDate");

    /** A type as lite's sentence names it, as the database judge does (upstream's elementToPath): a primitive by its
     *  short name, any other type by its full path. */
    static String typeName(String type) {
        String prefix = "meta::pure::metamodel::type::";
        return type.startsWith(prefix) && PRIMITIVES.contains(type.substring(prefix.length()))
                ? type.substring(prefix.length()) : type;
    }

    /** {@code row}'s record over its slots' texts, each slot's name then its text. */
    static com.legend.error.ErrorRecord record(com.legend.error.AssertKind row, String... slotsAndTexts) {
        java.util.Map<String, String> slots = new java.util.LinkedHashMap<>();
        for (int i = 0; i < slotsAndTexts.length; i += 2) {
            slots.put(slotsAndTexts[i], slotsAndTexts[i + 1]);
        }
        return row.record(slots);
    }


    static TypedSide finish(String family, boolean wantEqual, boolean hostHeld,
            java.util.function.Supplier<com.legend.error.@com.legend.base.Nullable ErrorRecord> hostMessage) {
        return finish(family, wantEqual, hostHeld, hostMessage, () -> {
            throw new IllegalStateException(family + ": an assertNotEquals with no sides to name");
        });
    }

    /** {@link #finish} where the assert may be {@code assertNotEquals}: {@code notEqualMessage} its upstream
     *  message over the two sides. */
    static TypedSide finish(String family, boolean wantEqual, boolean hostHeld,
            java.util.function.Supplier<com.legend.error.@com.legend.base.Nullable ErrorRecord> hostMessage,
            java.util.function.Supplier<com.legend.error.ErrorRecord> notEqualMessage) {
        // the host judge's verdict (P1, 2026-11-07: the byte channel beside it -- a canon rider built and declined at
        // run since the plan path's step 6, 2026-10-18 -- is deleted; the database judge is the referee)
        if (hostHeld == wantEqual) {
            return com.legend.testexecutionplan.Verdicts.ok();
        }
        if (!wantEqual) {
            return com.legend.testexecutionplan.Verdicts.fail(notEqualMessage.get());
        }
        com.legend.error.ErrorRecord d = hostMessage.get();
        if (d == null) {
            throw new IllegalStateException(family
                    + ": verdict/message divergence — the host lattice"
                    + " failed but its message lattice held");
        }
        return com.legend.testexecutionplan.Verdicts.fail(d);
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
            boolean incidental, HostRules rules, BodyRun run) {
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
            List<com.legend.testexec.Equality.Typed> te = com.legend.testexec.Equality.grid(e, eKinds);
            List<com.legend.testexec.Equality.Typed> ta = com.legend.testexec.Equality.grid(a, aKinds);
            hostHeld = com.legend.testexec.Equality.ordered(te, ta, rules);
            if (!hostHeld && incidental && e.size() == a.size()) {
                // ROW COHESION (audit 9): the incidental-order fallback
                // matches ROW TUPLES of the grid's width — cross-row
                // cell shuffles must FAIL; width 1 = the pool multiset
                TypedSide.Tabular g = af.grid() != null ? af.grid() : ef.grid();
                int w = g != null ? g.columns().size() : 1;
                hostHeld = com.legend.testexec.Equality.rowMultiset(
                        te, ta, w > 1 && e.size() % w == 0 ? w : 1, rules);
            }
        }
        return finish(name, wantEqual, hostHeld,
                // raw cells against a whole TDS value never equal: the two sides as written (item 3, 2026-11-21: until
                // then a sentence of its own outside the catalog)
                () -> AssertRecords.rowValuesDiffer(e, a, !mixedFlatVsTds && incidental),
                () -> AssertRecords.notEqual(e, a, null));
    }

    /** The MULTISET flat-cells verdict: loose CELL pool host lattice
     * (direction-aware sentinel — pool matching, never a sorted zip:
     * sorting separates an expected 'TDSNull' from its NULL cell). */
    static TypedSide tdsRowValuesSameElements(String family,
            SideFetch ef, SideFetch af, HostRules rules, BodyRun run) {
        List<Object> e = ef.values();
        List<Object> a = af.values();
        boolean hostHeld = e.size() == a.size()
                && com.legend.testexec.Equality.rowMultiset(
                        com.legend.testexec.Equality.Typed.all(e, null),
                        com.legend.testexec.Equality.Typed.all(a, null), 1, rules);
        return finish(family, true, hostHeld,
                () -> AssertRecords.sameElements(e, a, null, null));
    }

    /** Every wire value is an instance stamped {@code __type} = {@code cls}. */
    static boolean allWireType(List<Object> vals, String cls) {
        if (vals.isEmpty()) {
            return false;
        }
        for (Object v : vals) {
            if (!(v instanceof java.util.Map<?, ?> m)
                    || !cls.equals(m.get(com.legend.sql.SqlExpr.SYNTHETIC_TYPE))) {
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
            com.legend.testexecutionplan.KeyTree keys, HostJudgment.Keys known) {
        List<Object> out = new ArrayList<>(vals.size());
        for (Object v : vals) {
            out.add(restrictOne(v, keys, known));
        }
        return out;
    }

    static Object restrictOne(Object v,
            com.legend.testexecutionplan.KeyTree keys, HostJudgment.Keys known) {
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
            com.legend.testexecutionplan.@com.legend.base.Nullable KeyTree declared,
            HostJudgment.Keys known) {
        if (declared != null) {
            return restrictOne(v, declared, known);
        }
        if (v instanceof java.util.Map<?, ?> m
                && m.get(com.legend.sql.SqlExpr.SYNTHETIC_TYPE)
                        instanceof String own) {
            var keys = known.byClass().get(own);
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
    static List<com.legend.testexec.Equality.Typed> typedSide(List<Object> values,
            HostJudgment.SideFacts arg) {
        return com.legend.testexec.Equality.Typed.all(values, arg.declared());
    }

    /** A class-kind side's values with JSON object text decoded to structures ({@link AssertVerdicts#structured}); a
     *  Variant side's, each carried as its JSON text, as {@code LiteWords.VariantText}: a value of its own type, never
     *  equal to a value of another (register S401: two Variants are refused at planning, ruled by the user
     *  2026-11-23). */
    static List<Object> structuredSide(HostJudgment.SideFacts side, List<Object> vals) {
        if (side.classFqn() == null || AssertRecords.variant(side)) {
            return side.classFqn() == null ? vals : LiteWords.variantTexts(vals);
        }
        List<Object> out = new ArrayList<>(vals.size());
        for (Object v : vals) {
            out.add(com.legend.testexec.AssertRuns.structured(v));
        }
        return out;
    }

    /** {@code r}, a side read on the identity lane (never folded: a literal evaluates in the database here too), as
     *  the arms judge it: a plan's result, decoded by its declared types (P1, 2026-11-07: the canon rider built and
     *  declined here since the plan path's step 6 is deleted, with the byte channel it fed). */
    static SideFetch sideCanonOf(TypedSide r) {
        if (r instanceof TypedSide.Tabular t) {
            List<Object> cells = com.legend.testexec.AssertRuns.cells(t);
            com.legend.testexec.CanonicalDivergence.v7SideRows(cells.size());
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
        return new SideFetch(com.legend.testexec.AssertRuns.decodeSide(r), null);
    }


    // ---- the judgment as data (the test-is-the-product plan's M8a, 2026-11-01) ----------------------------------

    /**
     * THE ONE DISPATCH: {@code j}, the comparison a planned arm named with the facts it read at planning, judged over
     * {@code sides} (the arm's rows, shaped, in its order). Every arm's function takes the record and the rows and
     * nothing else -- no typed tree, no let prefix, no model, no environment (but for the engine-text asserts' appeal,
     * {@link HostJudgment.EdgeJudged}, the one named exception: M10b): the sink holds data. Null where the
     * judgment declines the sides it read (the walk's generic path continues).
     */
    static @com.legend.base.Nullable TypedSide judge(HostJudgment j, List<TypedSide> sides,
            BodyRun run) {
        return judge(j, sides, null, run);
    }

    /** {@code message}: the reader of the assert's caller's message where the verdict carries it apart from its sides
     *  (TestPlan.Verdict.message), asked only where the assert fails; null where the message, if any, is one of
     *  {@code sides} (a judgment nested in another's, whose sides the step read together). */
    static @com.legend.base.Nullable TypedSide judge(HostJudgment j, List<TypedSide> sides,
            java.util.function.@com.legend.base.Nullable Supplier<TypedSide> message, BodyRun run) {
        HostRules rules = new HostRules(run.hearing().hostForms());   // the run hook's forms (train 17: the plan carries no rules)
        return switch (j) {
            case HostJudgment.TdsEquivalent t -> tdsEquivalentOf(t, sides, rules);
            case HostJudgment.GridPair g -> gridPairOf(g, sides, rules, run);
            case HostJudgment.Size s -> sizeOf(s, sides, rules, run);
            case HostJudgment.Contains c -> containsOf(c, sides, rules, run);
            case HostJudgment.Tolerance t -> toleranceOf(sides, rules, run);
            case HostJudgment.Condition c -> conditionOf(c, sides, rules);
            case HostJudgment.AllRows a -> allRowsOf(a, sides);
            case HostJudgment.Rows r -> rowsOf(r, sides, rules, run);
            case HostJudgment.Unrolled u -> unrolledOf(u, sides, rules, run);
            case HostJudgment.Subset s -> subsetOf(s, sides, rules);
            case HostJudgment.Empty e -> emptyOf(e, sides.get(0), rules, run);
            case HostJudgment.Eq e -> eqOf(e, sides, rules, run);
            case HostJudgment.InstanceOf i -> instanceOfOf(i, sides.get(0), rules, run);
            case HostJudgment.Is i -> isOf(i, sides, rules);
            case HostJudgment.Decided d -> decided(d);
            case HostJudgment.SameElements s -> sameElementsOf(s, sides, rules, run);
            case HostJudgment.Equals e -> equalsOf(e, sides, rules, run);
            case HostJudgment.Json js -> jsonOf(js, sides, rules, run.hearing().json());
            case HostJudgment.Rendered r -> renderedOf(r, sides, run.hearing().renderedTexts());
            case HostJudgment.Raise r -> throw com.legend.testexecutionplan.Failures.raised(r.failure());
            case HostJudgment.EdgeJudged e -> run.judged(e, sides);
            case HostJudgment.AssertError a -> com.legend.testexec.AssertErrorRuns.judge(a, () -> sides, run);
            case HostJudgment.Messaged m -> messagedOf(m, sides, rules, message, run);
            case HostJudgment.Negated n -> negatedOf(n, sides, rules, run);
        };
    }

    /**
     * {@code j} judged over its sides AS THE RUNNER READ THEM (the walker-deletion program, 2026-11-03): {@code sides}
     * hands their rows, or raises the error reading them raised -- every judgment's one entry at its verdict step. The
     * judgment decides what an error is: the answer of the one that asks for an error ({@link HostJudgment.AssertError}:
     * the error its function raises), every other's failure, raised as it was.
     */
    static @com.legend.base.Nullable TypedSide judge(HostJudgment j,
            java.util.function.Supplier<List<TypedSide>> sides,
            java.util.function.@com.legend.base.Nullable Supplier<TypedSide> message, BodyRun run) {
        return j instanceof HostJudgment.AssertError a ? com.legend.testexec.AssertErrorRuns.judge(a, sides, run)
                : judge(j, sides.get(), message, run);
    }

    /** Two sides as upstream's {@code equal} sees them (X5; {@code EqualityUtilities}): a class-kind side's JSON text
     *  decoded to its structure, then a keyed class's instances restricted to their classifier and keys, any other's
     *  whole (its identity read with it) -- the one rule assertEquals, assertSameElements and assertContains judge by. */
    static List<List<Object>> asEqualSees(HostJudgment.Keys keys, HostJudgment.SideFacts ef, HostJudgment.SideFacts af,
            List<Object> eRaw, List<Object> aRaw) {
        // (a Variant cast to Any is a Variant in its facts, read at planning: JudgmentFacts.sideType)
        List<Object> e = structuredSide(ef, eRaw);
        List<Object> a = structuredSide(af, aRaw);
        var ik = restrictKeys(keys, ef, af, e, a);
        return ik == null ? List.of(e, a) : List.of(restrictToKeys(e, ik, keys), restrictToKeys(a, ik, keys));
    }

    /** The keys the pair restricts to, or null: both sides declare the SAME keyed class (a side declared a supertype
     *  counts as the other's class when every value on its wire is of it), as planning read the facts. */
    static com.legend.testexecutionplan.@com.legend.base.Nullable KeyTree restrictKeys(HostJudgment.Keys k,
            HostJudgment.SideFacts ef, HostJudgment.SideFacts af, List<Object> eVals, List<Object> aVals) {
        String ec = ef.classFqn();
        String ac = af.classFqn();
        com.legend.testexecutionplan.KeyTree ek = k.e();
        com.legend.testexecutionplan.KeyTree ak = k.a();
        if (ec != null && ac != null && !ec.equals(ac)) {
            if (k.eSubA() && allWireType(aVals, ec)) {
                ac = ec;
                ak = ek;
            } else if (k.aSubE() && allWireType(eVals, ac)) {
                ec = ac;
                ek = ak;
            }
        }
        return ec != null && ec.equals(ac) ? ek : null;
    }

    private static @com.legend.base.Nullable TypedSide tdsEquivalentOf(HostJudgment.TdsEquivalent j,
            List<TypedSide> sides, HostRules rules) {
        TypedSide.Tabular one = sides.get(0) instanceof TypedSide.Tabular t0 ? t0 : null;
        TypedSide.Tabular two = sides.get(1) instanceof TypedSide.Tabular t1 ? t1 : null;
        if (one == null || two == null) {
            return null;   // non-tabular shape — fall through, loud later
        }
        double delta = ((Number) com.legend.testexec.AssertRuns.one(com.legend.testexec.AssertRuns.decodeSide(sides.get(2)),
                "assertTdsEquivalent delta")).doubleValue();
        double timeDelta = j.hasTimeDelta()
                ? ((Number) com.legend.testexec.AssertRuns.one(com.legend.testexec.AssertRuns.decodeSide(sides.get(3)),
                        "assertTdsEquivalent timeDelta")).doubleValue()
                : 0.0;
        List<String> c1 = one.columns().stream()
                .map(com.legend.testexecutionplan.Column::name).toList();
        List<String> c2 = two.columns().stream()
                .map(com.legend.testexecutionplan.Column::name).toList();
        if (!c1.equals(c2) || one.rows().size() != two.rows().size()) {
            return com.legend.testexecutionplan.Verdicts.fail(AssertRecords.notEquivalent(one, two, delta));
        }
        String d = com.legend.testexec.TdsCompare.tdsEquivalent(
                com.legend.testexec.AssertRuns.cells(one), com.legend.testexec.AssertRuns.cells(two), delta, timeDelta);
        return d == null ? com.legend.testexecutionplan.Verdicts.ok()
                : com.legend.testexecutionplan.Verdicts.fail(AssertRecords.notEquivalent(one, two, delta));
    }

    private static TypedSide gridPairOf(HostJudgment.GridPair j, List<TypedSide> grids, HostRules rules, BodyRun run) {
        TypedSide.Tabular te = grids.get(0) instanceof TypedSide.Tabular t0 ? t0 : null;
        TypedSide.Tabular ta = grids.get(1) instanceof TypedSide.Tabular t1 ? t1 : null;
        if (te == null || ta == null) {
            throw new com.legend.error.NotImplementedException(
                    "relation-stamped assert side executed"
                            + " to a non-tabular result");
        }
        boolean held = com.legend.testexec.TdsCompare.grids(te, ta, j.sortedOrder(), rules);
        com.legend.diagnostics.VerdictCensus.sqlRoute(j.name(), "grid-pair");
        if (held != j.wantEqual()) {
            // upstream's words ('\nexpected: %r\nactual:   %r'; assertNotEquals.pure's '%r should not equal %r'), each
            // grid's rows as the database judge frames them (VerdictSql: no row '[]', one row its cells, several in
            // brackets) -- the two judges one text (2026-11-03, the core parity program: the summary
            // '[age] (2 rows) does not match' was lite's)
            return com.legend.testexecutionplan.Verdicts.fail(AssertRecords.grids(te, ta, j.wantEqual()));
        }
        return com.legend.testexecutionplan.Verdicts.ok();
    }

    private static TypedSide sizeOf(HostJudgment.Size j, List<TypedSide> both,
            HostRules rules, BodyRun run) {
        Object n = com.legend.testexec.AssertRuns.one(com.legend.testexec.AssertRuns.decodeSide(both.get(0)), "assertSize size");
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
            default -> com.legend.testexec.AssertRuns.decodeSide(r0).size();
        };
        boolean heldSize = n instanceof Number num
                && num.longValue() == actual;
        return heldSize ? com.legend.testexecutionplan.Verdicts.ok()
                : com.legend.testexecutionplan.Verdicts.fail(AssertRecords.size(n, actual));
    }

    private static @com.legend.base.Nullable TypedSide containsOf(HostJudgment.Contains j, List<TypedSide> both,
            HostRules rules, BodyRun run) {
        List<Object> coll = com.legend.testexec.AssertRuns.decodeSide(both.get(0));
        List<Object> val = com.legend.testexec.AssertRuns.decodeSide(both.get(1));
        if (val.size() != 1) {
            return null;   // non-[1] value arg — generic path
        }
        // upstream's contains is its equal (the audit of 946d16f2)
        List<List<Object>> seen = asEqualSees(j.keys(), j.collection(), j.value(), coll, val);
        List<Object> c = seen.get(0);
        Object v = seen.get(1).get(0);
        boolean member = c.stream().anyMatch(x ->
                com.legend.testexec.Equality.same(com.legend.testexec.Equality.Typed.of(x),
                        com.legend.testexec.Equality.Typed.of(v), rules));
        return member ? com.legend.testexecutionplan.Verdicts.ok()
                : com.legend.testexecutionplan.Verdicts.fail(AssertRecords.notContained(coll, val.get(0),
                        j.collection().classFqn(), j.value().classFqn()));
    }

    private static TypedSide toleranceOf(List<TypedSide> three,
            HostRules rules, BodyRun run) {
        Number e = (Number) com.legend.testexec.AssertRuns.one(com.legend.testexec.AssertRuns.decodeSide(three.get(0)), "tolerance expected");
        Number a = (Number) com.legend.testexec.AssertRuns.one(com.legend.testexec.AssertRuns.decodeSide(three.get(1)), "tolerance actual");
        Number delta = (Number) com.legend.testexec.AssertRuns.one(com.legend.testexec.AssertRuns.decodeSide(three.get(2)), "tolerance delta");
        com.legend.error.ErrorRecord d = toleranceMessage(e, a, delta);
        return d == null ? com.legend.testexecutionplan.Verdicts.ok() : com.legend.testexecutionplan.Verdicts.fail(d);
    }

    /** {@code assertEqWithinTolerance}'s failure, or null where it holds. */
    private static com.legend.error.@com.legend.base.Nullable ErrorRecord toleranceMessage(Number e, Number a,
            Number delta) {
        if (com.legend.testexec.PureAsserts.withinTolerance(e, a, delta)) {
            return null;
        }
        return AssertRecords.notWithin(e, a, delta);
    }

    private static @com.legend.base.Nullable TypedSide messagedOf(HostJudgment.Messaged m, List<TypedSide> sides,
            HostRules rules,
            java.util.function.@com.legend.base.Nullable Supplier<TypedSide> message, BodyRun run) {
        try (var messaged = run.messaged()) {
            return judge(m.inner(), sides.subList(0, m.innerSides()), run);
        } catch (com.legend.error.AssertFailed e) {
            if (e.unjudgedReason() != null) {
                throw e;
            }
            com.legend.testexecutionplan.@com.legend.base.Nullable Failure unplanned = m.unplanned();
            if (unplanned != null) {
                // computing the message raises: that error is the outcome, as upstream's (HostSides.withoutTheMessage)
                throw com.legend.testexecutionplan.Failures.raised(unplanned);
            }
            // read now, the assert having failed: through the verdict's reader, or the side after the inner ones
            TypedSide read = message != null ? message.get() : sides.get(m.innerSides());
            Object text = com.legend.testexec.AssertRuns.one(com.legend.testexec.AssertRuns.decodeSide(read), "the message");
            // the caller's own message, a value the program computed: the catalog's MESSAGE record (item 3, 2026-11-21)
            throw new com.legend.error.AssertFailed(com.legend.error.AssertKind.MESSAGE.record(
                    java.util.Map.of("message", String.valueOf(text))));
        }
    }

    /** A default-message assert: its positive judgment over the same sides, negated, worded by the rules (#28). */
    private static TypedSide negatedOf(HostJudgment.Negated n, List<TypedSide> sides,
            HostRules rules, BodyRun run) {
        try {
            judge(n.inner(), sides, run);
        } catch (com.legend.error.AssertFailed failed) {
            if (failed.unjudgedReason() != null) {
                throw failed;
            }
            return com.legend.testexecutionplan.Verdicts.ok();
        }
        return com.legend.testexecutionplan.Verdicts.fail(negatedWords(n.kind(), sides));
    }

    /** A default-message negated assert's failure (assertNotEq, assertIsNot, assertNotContains, assertNotSize). */
    private static com.legend.error.ErrorRecord negatedWords(String kind, List<TypedSide> sides) {
        List<Object> first = com.legend.testexec.AssertRuns.decodeSide(sides.get(0));
        List<Object> second = sides.size() > 1 ? com.legend.testexec.AssertRuns.decodeSide(sides.get(1)) : List.of();
        return AssertRecords.negated(kind, first, second);
    }

    private static TypedSide conditionOf(HostJudgment.Condition j, List<TypedSide> one,
            HostRules rules) {
        Object c = com.legend.testexec.AssertRuns.one(com.legend.testexec.AssertRuns.decodeSide(one.get(0)), j.name() + " condition");
        boolean held = Boolean.TRUE.equals(c) == j.wantTrue();
        return held ? com.legend.testexecutionplan.Verdicts.ok()
                : com.legend.testexecutionplan.Verdicts.fail(AssertRecords.condition(j.wantTrue()));
    }

    private static TypedSide subsetOf(HostJudgment.Subset j, List<TypedSide> pair, HostRules rules) {
        boolean wantTrue = j.wantTrue();
        List<Object> need = com.legend.testexec.AssertRuns.decodeSide(pair.get(0));
        List<Object> have = com.legend.testexec.AssertRuns.decodeSide(pair.get(1));
        List<Object> missing = need.stream()
                .filter(n2 -> have.stream().noneMatch(h ->
                        com.legend.testexec.Equality.same(com.legend.testexec.Equality.Typed.of(n2),
                                com.legend.testexec.Equality.Typed.of(h), rules)))
                .toList();
        boolean subsetHolds = missing.isEmpty();
        if (subsetHolds == wantTrue) {
            return com.legend.testexecutionplan.Verdicts.ok();
        }
        // the subset is the fold of an assert's (or assertFalse's) condition, so its failure is the condition's record,
        // as the database judge's subset row writes it (the S2+S3 audit's MEDIUM, 2026-11-21: until then a sentence of
        // its own, "name (forAll-contains subset): missing [..]"; the fixes' audit: not a contains record)
        return com.legend.testexecutionplan.Verdicts.fail(AssertRecords.condition(wantTrue));
    }

    private static TypedSide emptyOf(HostJudgment.Empty j, TypedSide er,
            HostRules rules, BodyRun run) {
        boolean wantEmpty = j.wantEmpty();
        boolean empty = er instanceof TypedSide.Tabular te3
                ? te3.rows().isEmpty()
                : com.legend.testexec.AssertRuns.decodeSide(er).isEmpty();
        boolean held = empty == wantEmpty;
        if (held) {
            return com.legend.testexecutionplan.Verdicts.ok();
        }
        return com.legend.testexecutionplan.Verdicts.fail(AssertRecords.emptiness(wantEmpty, er,
                com.legend.testexec.AssertRuns.decodeSide(er)));
    }

    private static TypedSide eqOf(HostJudgment.Eq j, List<TypedSide> pair, HostRules rules, BodyRun run) {
        SideFetch ef = sideCanonOf(pair.get(0));
        SideFetch af = sideCanonOf(pair.get(1));
        Object ee = com.legend.testexec.AssertRuns.one(ef.values(), "assertEq expected");
        Object aa = com.legend.testexec.AssertRuns.one(af.values(), "assertEq actual");
        // eq's non-primitive identity rule throws LOUD here (P2-5); a primitive under its side's declared kind where that
        // is numeric, as assertEquals' typedSide reads it (PR #118 fix round, 2026-11-18, G2)
        com.legend.error.ErrorRecord d = com.legend.testexec.PureAsserts.eqHolds(ee, j.e().declared(), aa, j.a().declared(), rules) ? null
                : AssertRecords.notEq(ee, aa, j.e().classFqn(), j.a().classFqn(), j.e().declared(), j.a().declared());
        com.legend.testexec.CanonicalDivergence.probeEqual("assertEq",
                java.util.Collections.singletonList(ee),
                java.util.Collections.singletonList(aa), d == null);
        return finish("assertEq", true, d == null, () -> d);
    }

    private static @com.legend.base.Nullable TypedSide instanceOfOf(HostJudgment.InstanceOf j, TypedSide side,
            HostRules rules, BodyRun run) {
        Object v = com.legend.testexec.AssertRuns.one(com.legend.testexec.AssertRuns.decodeSide(side), "assertInstanceOf instance");
        String type = j.typeName();
        if (type == null) {
            return null;   // non-literal type arg — fall through
        }
        // a CLASS value's wire carries its classifier (__type,
        // batch 53): instanceOf is the model's subtype relation
        // (a property-less class such as NullLiteral has nothing
        // else on the wire)
        // an instance whose data names no class is of its static class (a layout of one class: the audit of 946d16f2)
        Object s = com.legend.testexec.AssertRuns.structured(v);
        String wireType = s instanceof java.util.Map<?, ?> m && m.get(com.legend.sql.SqlExpr.SYNTHETIC_TYPE)
                instanceof String w ? w : s instanceof java.util.Map<?, ?> ? j.valueClass() : null;
        if (wireType != null) {
            return j.acceptedTypes().contains(wireType) ? com.legend.testexecutionplan.Verdicts.ok()
                    : com.legend.testexecutionplan.Verdicts.fail(AssertRecords.notInstanceOfClass(v, wireType, type));
        }
        String carrier = com.legend.testexec.PureAsserts.notAnInstanceOf(v, type);
        return carrier == null ? com.legend.testexecutionplan.Verdicts.ok()
                : com.legend.testexecutionplan.Verdicts.fail(AssertRecords.notInstanceOf(v, type));
    }

    private static TypedSide isOf(HostJudgment.Is j, List<TypedSide> one,
            HostRules rules) {
        Object c2 = com.legend.testexec.AssertRuns.one(com.legend.testexec.AssertRuns.decodeSide(one.get(0)), j.name() + " condition");
        return Boolean.TRUE.equals(c2) ? com.legend.testexecutionplan.Verdicts.ok()
                : com.legend.testexecutionplan.Verdicts.fail(AssertRecords.notSameInstance());
    }

    private static TypedSide sameElementsOf(HostJudgment.SameElements j, List<TypedSide> pair,
            HostRules rules, BodyRun run) {
        SideFetch ef = sideCanonOf(pair.get(0));
        SideFetch af = sideCanonOf(pair.get(1));
        // §8 leg 1 — TABULAR sides under the MULTISET form:
        // loose CELL pool (the corpus writes flat expected sets
        // column-grouped — loose multiset IS this assert's
        // reference semantics, audit 9)
        if (ef.grid() != null || af.grid() != null) {
            return tdsRowValuesSameElements("assertSameElements", ef, af, rules, run);
        }
        // a CLASS-kind side that rode a JSON carrier (a polymorphic
        // Node[1] program value) arrives as object text: decode it
        // to the structure the key restriction reads (the executor's
        // own rule for JSON slots — WORLD_MAP §4, __type rides along)
        List<List<Object>> both = asEqualSees(j.keys(), j.e(), j.a(), ef.values(), af.values());
        List<Object> e = both.get(0);
        List<Object> a = both.get(1);
        com.legend.error.@com.legend.base.Nullable ErrorRecord d = com.legend.testexec.Equality.sameElements(
                typedSide(e, j.e()), typedSide(a, j.a()), rules)
                // the failed TYPED comparison's message, never a second, untyped one (the ledger audit's D4, 2026-11-16:
                // an untyped re-compare that held here returned null, and the failed assert passed)
                ? null : AssertRecords.equality(true, true, e, a, ef.values(), af.values(), j.e(), j.a());
        com.legend.testexec.CanonicalDivergence.probeSameElements(
                e, a, d == null);
        // V4/V11 — the multiset BYTE VERDICT OF RECORD: rows
        // arrive ORDER BY canon text (the wrap's sort key) in
        // the SAME execution; the host multiset judgment above
        // is the parallel referee.
        return finish("assertSameElements", true, d == null,
                () -> d);
    }

    /** Each element's planned assert judged over its slice of the sides, in order, counted as the walk counts an
     *  element it adjudicates (entered, its family; an engine-text route once answered): the last verdict, the first
     *  failure raising; an element whose judgment declines its sides is the walk's wall. */
    private static TypedSide unrolledOf(HostJudgment.Unrolled u, List<TypedSide> sides,
            HostRules rules, BodyRun run) {
        int at = 0;
        TypedSide last = null;
        for (int i = 0; i < u.parts().size(); i++) {
            HostJudgment.Part part = u.parts().get(i);
            int n = part.asks();
            List<TypedSide> slice = sides.subList(at, at + n);
            at += n;
            com.legend.testexecutionplan.Failure before = part.assertion().beforeTheArm();
            if (before != null) {
                throw com.legend.testexecutionplan.Failures.raised(before);
            }
            com.legend.testexec.AssertRuns.enter(part.assertion());
            TypedSide v;
            try {
                // its caller's message, where it reads, read only if the element fails (decision 4)
                v = com.legend.testexec.AssertRuns.judge(part.assertion(), part.judgment(), () -> slice,
                        run.message(part.message(), part.messageShape()), run);
            } catch (com.legend.error.AssertFailed failed) {
                // the element named by Pure's zero-based index (the user's direction, 2026-11-17): its own record
                // inside the AT_INDEX frame, on every route (item 3, 2026-11-21). A decline (refused at planning since
                // D) or the edge's appeal words (B3's, not yet a record) is raised as it is.
                throw AssertRecords.atIndex(failed, i);
            }
            if (v == null) {
                throw new com.legend.error.NotImplementedException(
                        "unrolled quantified assert: element verdict not adjudicable for "
                                + part.assertion().listenerName());
            }
            last = v;
        }
        return last == null ? com.legend.testexecutionplan.Verdicts.ok() : last;
    }

    private static TypedSide allRowsOf(HostJudgment.AllRows j, List<TypedSide> sides) {
        for (Object v : com.legend.testexec.AssertRuns.decodeSide(sides.get(0))) {
            if (Boolean.TRUE.equals(v) != j.wantTrue()) {
                return com.legend.testexecutionplan.Verdicts.fail(j.failure());
            }
        }
        return com.legend.testexecutionplan.Verdicts.ok();
    }

    /** {@link HostJudgment.Rows} judged over its sides: each row as the element arm judges it, the first failure. */
    private static @com.legend.base.Nullable TypedSide rowsOf(HostJudgment.Rows j, List<TypedSide> sides,
            HostRules rules, BodyRun run) {
        List<Object> rows;
        if (j.pairing() == null && !(sides.get(0) instanceof TypedSide.Tabular)) {
            // the per-element operand pairs (planPerElement): one value per element, each a {first, second} the
            // database computed, in the source's order
            rows = com.legend.testexec.AssertRuns.decodeSide(sides.get(0));
        } else if (j.pairing() == null) {
            TypedSide.Tabular t = (TypedSide.Tabular) sides.get(0);
            rows = new java.util.ArrayList<>(t.rows().size());
            for (com.legend.testexecutionplan.Row r : t.rows()) {
                java.util.Map<String, Object> m = new java.util.LinkedHashMap<>();
                for (int i = 0; i < t.columns().size(); i++) {
                    m.put(t.columns().get(i).name(), r.values().get(i));
                }
                rows.add(m);
            }
        } else {
            List<List<Object>> values = new java.util.ArrayList<>(sides.size());
            for (TypedSide r : sides) {
                values.add(r instanceof TypedSide.Tabular t ? com.legend.testexec.AssertRuns.cells(t) : com.legend.testexec.AssertRuns.decodeSide(r));
            }
            rows = pairedOf(j.pairing(), values);
        }
        for (int index = 0; index < rows.size(); index++) {
            Object row = rows.get(index);
            Object e = rowArgValue(j.args().get(0), row, j.name());
            Object a = rowArgValue(j.args().get(1), row, j.name());
            List<Object> es = java.util.Collections.singletonList(e);
            List<Object> as = java.util.Collections.singletonList(a);
            com.legend.error.ErrorRecord d = j.tolerance()
                    ? toleranceMessage(number(e, j.name()), number(a, j.name()),
                            number(rowArgValue(j.args().get(2), row, j.name()), j.name()))
                    : com.legend.testexec.PureAsserts.holds(es, as, rules) ? null
                    : AssertRecords.rowNotEqual(e, a, j.expectedClass(), j.actualClass());
            if (d != null) {
                return com.legend.testexecutionplan.Verdicts.fail(AssertRecords.atIndex(d, index));
            }
        }
        return com.legend.testexecutionplan.Verdicts.ok();
    }

    /** A literal argument's value, read back from its carrier and text exactly as the compiler typed it. */
    private static Object literal(HostJudgment.RowArg.Literal l) {
        return switch (l.carrier()) {
            case "String" -> l.text();
            case "Boolean" -> Boolean.valueOf(l.text());
            case "Double" -> Double.valueOf(l.text());
            case "Long" -> Long.valueOf(l.text());
            case "Integer" -> Integer.valueOf(l.text());
            case "BigInteger" -> new java.math.BigInteger(l.text());
            default -> throw new IllegalStateException("a row assert's literal of carrier '" + l.carrier()
                    + "': String, Boolean, Double, Long, Integer or BigInteger");
        };
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
            case HostJudgment.RowArg.Literal l -> literal(l);
            case HostJudgment.RowArg.Field f -> {
                if (!(row instanceof java.util.Map<?, ?> m) || !m.containsKey(f.name())) {
                    throw new com.legend.error.NotImplementedException(fqn + ": the quantified assert's row holds no"
                            + " field '" + f.name() + "': " + row);
                }
                yield m.get(f.name());
            }
        };
    }

    private static Number number(@com.legend.base.Nullable Object v, String fqn) {
        if (v instanceof Number n) {
            return n;
        }
        throw new com.legend.error.NotImplementedException(fqn + ": assertEqWithinTolerance over a row whose value is"
                + " not a number: " + v);
    }

    private static TypedSide equalsOf(HostJudgment.Equals j, List<TypedSide> pair,
            HostRules rules, BodyRun run) {
        String name = j.name();
        boolean wantEqual = j.wantEqual();
        boolean incidental = j.incidental();
        SideFetch ef = sideCanonOf(pair.get(0));
        SideFetch af = sideCanonOf(pair.get(1));
        // the FLAT-CELLS verdict (the host cell lattice)
        if (ef.grid() != null || af.grid() != null) {
            return tdsRowValuesVerdict(name, wantEqual, j.flat(), ef, af, incidental, rules, run);
        }
        // X5: a same-class KEYED pair restricts both sides to
        // the key tree — the engine's own equality relation for
        // keyed classes, applied before the lattice judges
        // a CLASS-kind side that rode a JSON carrier (a polymorphic
        // Node[1] program value) arrives as object text: decode it
        // to the structure the key restriction reads (the executor's
        // own rule for JSON slots — WORLD_MAP §4, __type rides along)
        List<List<Object>> both = asEqualSees(j.keys(), j.e(), j.a(), ef.values(), af.values());
        List<Object> e = both.get(0);
        List<Object> a = both.get(1);
        List<com.legend.testexec.Equality.Typed> te = typedSide(e, j.e());
        List<com.legend.testexec.Equality.Typed> ta = typedSide(a, j.a());
        boolean equal = incidental
                ? com.legend.testexec.Equality.sameElements(te, ta, rules)
                : com.legend.testexec.Equality.ordered(te, ta, rules);
        // R1a divergence instrument (CANONICAL_FORM_SPEC §0): the host lattice vs a canonical render of the same
        // values, measurement only (the byte verdict it once shadowed is deleted, P1, 2026-11-07)
        com.legend.testexec.CanonicalDivergence.probeEqual(
                name, e, a, equal, incidental);
        // the message is the failed verdict's own sides, spelled -- no second, untyped comparison that could hold
        // where the typed one failed (the ledger audit's D4, 2026-11-16)
        // the record the one choice makes (AssertRecords.equality), as the database judge's run makes it
        return finish(name, wantEqual, equal,
                () -> AssertRecords.equality(incidental, true, e, a, ef.values(), af.values(), j.e(),
                        j.a()),
                () -> AssertRecords.equality(incidental, false, e, a, ef.values(), af.values(), j.e(),
                        j.a()));
    }

    private static @com.legend.base.Nullable TypedSide jsonOf(HostJudgment.Json j, List<TypedSide> pair,
            HostRules rules, com.legend.testexecutionplan.JsonGrammar grammar) {
        String ejson = com.legend.testexec.AssertRuns.jsonSideTextOf(pair.get(0));
        String ajson = com.legend.testexec.AssertRuns.jsonSideTextOf(pair.get(1));
        if (ejson == null || ajson == null) {
            return null;   // non-[1]-string shape: generic path
        }
        // the GOLDEN side parses first and names itself when it does not: both texts read by the run's JSON grammar
        // (register S144; census C1, 2026-11-17: the product's strict form, upstream's json-simple the edge's, through
        // the run's hearing -- the milestoned graph-fetch goldens end in a stray `"` json-simple reads past)
        Object expected;
        try {
            expected = grammar.read(ejson);
        } catch (IllegalStateException e) {
            throw new IllegalStateException(
                    "golden JSON does not parse: " + e.getMessage(), e);
        }
        Object actual = grammar.read(ajson);
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
                ? com.legend.testexec.Equality.pureJsonUnorderedRoot(expected, actual)
                : com.legend.testexec.Equality.pureJson(expected, actual);
        return diff == null ? com.legend.testexecutionplan.Verdicts.ok()
                : com.legend.testexecutionplan.Verdicts.fail(AssertRecords.jsonDiffers(ejson, ajson, incidentalRoot));
    
    }

    private static TypedSide renderedOf(HostJudgment.Rendered j, List<TypedSide> pair, RenderedTexts texts) {
        String name = j.name();
        String form = j.form();
        List<Object> ev = com.legend.testexec.AssertRuns.decodeSide(pair.get(0));
        List<Object> av = com.legend.testexec.AssertRuns.decodeSide(pair.get(1));
        if (ev.size() == 1 && ev.get(0) instanceof String et
                && av.size() == 1 && av.get(0) instanceof String at) {
            // GENERIC FIRST (2026-11-04, the core program): the two strings compared as strings; the ruled policy (an
            // unsorted query's lines as a multiset, S131; float tokens within 2 ULP, S132) only where they differ
            // (measured on the corpus's DuckDB host pass: 240 of 269 texts byte-equal, 25 held by the policy)
            // clean mode (the user, 2026-11-05: after a verdict the harness may only explain): the two strings ARE the
            // verdict; the ruled row comparison after a failed text is compatibility mode's alone
            boolean held = et.equals(at)
                    || texts.held(j.aForm() != null ? et : at, j.aForm() != null ? at : et, form, j.sortedOrder());
            if (held != j.wantEqual()) {
                // the two texts, each a String as lite writes it (item 3, 2026-11-21: until then a sentence of its own
                // outside the catalog, the first differing line)
                return com.legend.testexecutionplan.Verdicts.fail(j.wantEqual()
                        ? record(com.legend.error.AssertKind.EQUALS, "expected", LiteWords.quoted(et), "actual",
                                LiteWords.quoted(at))
                        : record(com.legend.error.AssertKind.NOT_EQUALS, "actual", LiteWords.quoted(at)));
            }
            if (j.counted()) {
                com.legend.diagnostics.VerdictCensus.sqlRoute(name, "rendered-text");
            }
            return com.legend.testexecutionplan.Verdicts.ok();
        }
        // a render form whose sides are not two strings — loud, never
        // a silent fall-through re-execution
        throw new com.legend.error.NotImplementedException(
                "rendered-text assert side is not a string pair ("
                        + form + ")");
    }
}
