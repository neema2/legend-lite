// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.element.ModelContext;

import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.exec.ExecutionResult;
import com.legend.exec.PureAsserts;

import java.util.ArrayList;
import java.util.List;

/**
 * THE ASSERT-FAMILY VERDICT ARM (Charter Clause 2c, the Phase-4
 * redesign): a STATEMENT-ROOT call to the assert family is a VERDICT —
 * its result terminates in the runner, never in a data flow. Each
 * ARGUMENT executes through the full pipeline IN THE DATABASE (tenet #1
 * — the expressions are the data computation under test); the JUDGMENT
 * over the two produced sides is World 1's:
 * {@link PureAsserts} — the spec-exact Phase-2 adjudication layer. The
 * assert library's pure bodies are never β-inlined into SQL to produce
 * a verdict (the named Clause-2c violation; the Phase-4 seam arms were
 * its witnessed cost).
 *
 * <p>ASSERTS ARE VERDICTS, ALWAYS (homework 2026-08-19): legend-engine
 * has NO SQL translation for any assert — its relational adapters
 * execute only the inner expression in the store. The corpus's
 * map-wrapped asserts ({@code values->map(f|assert(...))}) are
 * QUANTIFIED verdicts over already-executed results — served by the
 * quantified arm here (predicates vectorize IN THE DATABASE, the
 * boolean vector is judged host-side, first failure raises with the
 * spec message) — the interpreter's per-element behavior, minus the
 * interpreter. Family members WITHOUT a verdict arm decline LOUDLY
 * with their shape — never a silent skip, never SQL-lowered verdicts.
 */
final class AssertVerdicts {

    private AssertVerdicts() {
    }

    private static final String PKG = "meta::pure::functions::asserts::";

    /** V7 batch 2: the statement loop's result-envelope splice hook,
     * threaded into every side evaluation so an assert argument reading
     * an execute() frame compiles the SPLICED chain — identical to the
     * ordinary-statement path (audit 19d B2; splice pin:
     * AssertVerdictSpliceTest). Null = no frames in scope. */
    interface SpliceHook extends java.util.function.BiFunction<TypedSpec,
            java.util.Set<String>, TypedSpec> {
    }

    /** Null = not a statement-root assert this arm owns (generic path
     * continues); otherwise the verdict (TRUE, or the spec's failure
     * raised as the runner's failure). */
    static @com.legend.Nullable ExecutionResult tryAdjudicate(TypedSpec bare,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            java.util.function.@com.legend.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> rawHook) {
        com.legend.exec.AssertListener l = env.assertListener();
        if (l == null) {
            return adjudicate(bare, letPrefix, specs, env, rawHook);
        }
        // the listener observes the arm's OWN outcomes only: a non-null
        // verdict = pass; a raise out of an owned adjudication = fail
        // (side-evaluation errors fail the same test either way — the
        // detail says which). Judgment itself is untouched.
        try {
            ExecutionResult v = adjudicate(bare, letPrefix, specs, env,
                    rawHook);
            if (v != null) {
                l.verdict(listenerName(bare), true, null);
            }
            return v;
        } catch (com.legend.error.AssertFailed
                | com.legend.error.DataError e) {
            // the seam: a FALSE verdict or a side-evaluation data error
            // fails the same test either way — the detail says which
            l.verdict(listenerName(bare), false, e.getMessage());
            throw e;
        }
    }

    private static String listenerName(TypedSpec bare) {
        String fqn = calleeFqn(bare);
        return fqn != null ? fqn
                : bare instanceof com.legend.compiler.spec.typed.TypedMap
                        ? "quantified-assert" : bare.getClass().getSimpleName();
    }

    /** An if branch's statement: the lambda's last statement, or the
     * expression itself. */
    private static TypedSpec branchStatement(TypedSpec branch) {
        return branch instanceof com.legend.compiler.spec.typed.TypedLambda l
                && !l.body().isEmpty()
                ? l.body().get(l.body().size() - 1) : branch;
    }

    private static @com.legend.Nullable ExecutionResult adjudicate(TypedSpec bare,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            java.util.function.@com.legend.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> rawHook) {
        SpliceHook hook = rawHook == null ? null : rawHook::apply;
        LineageTreeVerdicts.Verdict lineage = LineageTreeVerdicts.tryArm(bare, letPrefix, specs, env);
        if (lineage != null) {
            return lineage.held() ? ok() : fail(lineage.message());
        }
        TypedSpec unwrapped = com.legend.compiler.spec.VerdictQueries.distinctTrueWrapper(bare);
        if (unwrapped instanceof com.legend.compiler.spec.typed.TypedMap qm2) {
            ExecutionResult u = unrolled(qm2, letPrefix, specs, env, rawHook);
            if (u != null) {
                return u;
            }
        }
        if (bare instanceof com.legend.compiler.spec.typed.TypedMap qm) {
            ExecutionResult u = unrolled(qm, letPrefix, specs, env, rawHook);
            if (u != null) {
                return u;
            }
            return quantified(qm, letPrefix, specs, env, hook);
        }
        // an if whose BRANCHES are asserts (assertEqualsH2Compatible's body
        // once the H2 version probe answers): the condition is a value
        // query the database evaluates; the taken branch IS the verdict
        if (bare instanceof com.legend.compiler.spec.typed.TypedIf ti
                && ti.elseBranch().isPresent()
                && calleeFqn(branchStatement(ti.thenBranch())) != null
                && calleeFqn(branchStatement(ti.elseBranch().get())) != null) {
            ExecutionResult c = StatementExecutor.evalValue(
                    ti.condition(), letPrefix, specs, env);
            if (c instanceof ExecutionResult.Scalar sc
                    && sc.value() instanceof Boolean taken) {
                return adjudicate(branchStatement(taken
                        ? ti.thenBranch() : ti.elseBranch().get()),
                        letPrefix, specs, env, rawHook);
            }
            return null;
        }
        String fqn = calleeFqn(bare);
        if (fqn == null) {
            return null;
        }
        // SQLTEXT charter §8.3b — the assertSameSQL ROOT arm: the
        // statement root arrives PRE-inline, so SqlTextVerdicts owns
        // the whole golden-vs-executed-frame shape (rows judge, text
        // is the emission census). Null = not the simple shape — the
        // generic path (inline + fold) keeps it exactly as today.
        if (fqn.equals(
                "meta::relational::functions::asserts::assertSameSQL")
                && bare instanceof TypedUserCall sroot) {
            ExecutionResult sv = SqlTextVerdicts.tryArmSameSql(sroot,
                    letPrefix, specs, env, hook);
            if (sv != null) {
                return sv;
            }
        }
        // TDG scoring flip — the assertSqlEquals root (same discipline)
        if (fqn.equals("meta::relational::testDataGeneration::tests"
                + "::assertSqlEquals")
                && bare instanceof TypedUserCall troot) {
            ExecutionResult tv = SqlTextVerdicts.tryArmTdgRoot(troot,
                    letPrefix, specs, env, hook);
            if (tv != null) {
                return tv;
            }
        }
        // §8.3d — the dual-golden sibling (same root-arm discipline)
        if (fqn.equals("meta::relational::functions::sqlQueryToString"
                + "::h2::assertEqualsH2Compatible")
                && bare instanceof TypedUserCall hroot) {
            ExecutionResult hv = SqlTextVerdicts.tryArmH2Compat(hroot,
                    letPrefix, specs, env, hook);
            if (hv != null) {
                return hv;
            }
        }
        // THE GRID VERDICT (Clause 2c — TdsCompare's chartered route;
        // witness: the relation suite's 79 assertTdsEquivalent rows):
        // both relations execute IN THE DATABASE, the cell-zip
        // adjudicates host-side (tdsEquivalent.pure's numeric delta +
        // temporal seconds policies, already the one owner).
        if (fqn.equals(
                "meta::pure::functions::relation::assertTdsEquivalent")) {
            List<TypedSpec> targs = ((bare instanceof TypedUserCall u2)
                    ? u2.args() : ((TypedNativeCall) bare).args());
            if (targs.size() < 3 || targs.size() > 4) {
                return null;
            }
            ExecutionResult.Tabular one =
                    tabular(targs.get(0), letPrefix, specs, env, hook);
            ExecutionResult.Tabular two =
                    tabular(targs.get(1), letPrefix, specs, env, hook);
            if (one == null || two == null) {
                return null;   // non-tabular shape — fall through, loud later
            }
            double delta = ((Number) one(side(targs.get(2), letPrefix,
                    specs, env, hook), "assertTdsEquivalent delta")).doubleValue();
            double timeDelta = targs.size() == 4
                    ? ((Number) one(side(targs.get(3), letPrefix, specs,
                            env, hook), "assertTdsEquivalent timeDelta"))
                            .doubleValue()
                    : 0.0;
            List<String> c1 = one.columns().stream()
                    .map(com.legend.exec.Column::name).toList();
            List<String> c2 = two.columns().stream()
                    .map(com.legend.exec.Column::name).toList();
            if (!c1.equals(c2) || one.rows().size() != two.rows().size()) {
                return fail("\n" + summarize(one) + "\n is not"
                        + " equivalent to:\n" + summarize(two));
            }
            String d = com.legend.exec.TdsCompare.tdsEquivalent(
                    cells(one), cells(two), delta, timeDelta);
            return d == null ? ok() : fail(d);
        }
        if (!fqn.startsWith(PKG)) {
            return null;
        }
        String name = fqn.substring(PKG.length());
        List<TypedSpec> args = com.legend.compiler.spec.ExecuteChainAssembly.narrowSideStamps(
                (bare instanceof TypedUserCall u) ? u.args() : ((TypedNativeCall) bare).args(),
                letPrefix, specs);
        switch (name) {
            case "assertEquals", "assertNotEquals" -> {
                if (args.size() < 2) {
                    return null;
                }
                boolean wantEqual = name.equals("assertEquals");
                // SQLTEXT charter slice 3a — the SQL-TEXT arm: a
                // toSQLString producer in an argument tree judges on
                // ROWS (SqlTextVerdicts; text is a census number).
                // Shapes outside the exact cohort return null here.
                ExecutionResult sv = SqlTextVerdicts.tryArm(name,
                        wantEqual, args, letPrefix, specs, env, hook);
                if (sv != null) {
                    return sv;
                }
                // D3 — the RENDERED-TEXT arm: exactly one side is a
                // DB-rendered grid text (toCSV/toString/replace/join
                // spellings), the peer a string value. The DATABASE
                // computed the render; TdsCompare.renderedText (the
                // one policy owner, R1b-probed) judges the texts.
                ExecutionResult ra = renderedArm(name, wantEqual, args,
                        letPrefix, specs, env, hook, true);
                if (ra != null) {
                    return ra;
                }
                // D3 — the GRID-PAIR arm: both sides statically
                // relation-stamped execute as grids; the grid owner
                // (TdsCompare.grids: columns ordered, rows under the
                // order policy) judges — never a byte decline.
                if (com.legend.compiler.element.type.Type.isRelation(
                        args.get(0).info().type())
                        && com.legend.compiler.element.type.Type.isRelation(
                                args.get(1).info().type())) {
                    ExecutionResult.Tabular te = tabular(args.get(0),
                            letPrefix, specs, env, hook);
                    ExecutionResult.Tabular ta = tabular(args.get(1),
                            letPrefix, specs, env, hook);
                    if (te == null || ta == null) {
                        throw new com.legend.error.NotImplementedException(
                                "relation-stamped assert side executed"
                                        + " to a non-tabular result");
                    }
                    boolean held = com.legend.exec.TdsCompare.grids(te,
                            ta, orderView(args.get(1), letPrefix)
                                    == OrderView.SORTED);
                    if (held != wantEqual) {
                        return fail(name + ":\n" + summarize(te)
                                + "\n does not match:\n" + summarize(ta));
                    }
                    return ok();
                }
                // D3 — the ORDER VIEW: an INCIDENTAL-order actual side
                // (unsorted store read / frame read) has SQL arrival
                // order — engine goldens encode H2's, ours is DuckDB's
                // — so both sides fetch with the CANONICAL-order riders
                // and the judgment is order-insensitive (exactly the
                // assertSameElements shape). SORTED/DEFINED sides stay
                // strictly ordered.
                boolean incidental = orderView(args.get(1), letPrefix)
                        == OrderView.INCIDENTAL;
                // §8 leg 1 — grid-ness is STATIC (the declared result
                // shape, decided before execution — the ratified
                // no-runtime-sniffing rule): a grid pair fetches in
                // DEFINITION order (the peer's row chunking depends on
                // it; the canonical-order rider would destroy it) and
                // any multiset view sorts DB-computed canon texts
                // host-side instead — semantics-free string sorting.
                boolean gridPair = tabularShaped(args.get(0))
                        || tabularShaped(args.get(1));
                SideFetch ef = sideCanon(args.get(0), letPrefix, specs,
                        env, incidental && !gridPair, hook);
                SideFetch af = sideCanon(args.get(1), letPrefix, specs,
                        env, incidental && !gridPair, hook);
                // the FLAT-CELLS verdict (grid canon byte channel +
                // host cell lattice referee)
                if (ef.grid() != null || af.grid() != null) {
                    return tdsRowValuesVerdict(name, wantEqual, args,
                            letPrefix, ef, af, incidental);
                }
                // X5: a same-class KEYED pair restricts both sides to
                // the key tree — the engine's own equality relation for
                // keyed classes, applied before EITHER channel judges
                // a CLASS-kind side that rode a JSON carrier (a polymorphic
                // Node[1] program value) arrives as object text: decode it
                // to the structure the key restriction reads (the executor's
                // own rule for JSON slots — WORLD_MAP §4, __type rides along)
                List<Object> eVals = structuredSide(args.get(0), ef.values());
                List<Object> aVals = structuredSide(args.get(1), af.values());
                var ik = instanceKeys(args.get(0), args.get(1), env, eVals, aVals);
                List<Object> e = ik != null
                        ? restrictToKeys(eVals, ik, env.ctx()) : eVals;
                List<Object> a = ik != null
                        ? restrictToKeys(aVals, ik, env.ctx()) : aVals;
                boolean equal = incidental
                        ? PureAsserts.assertSameElements(e, a) == null
                        : PureAsserts.equal(e, a);
                // R1a divergence instrument (CANONICAL_FORM_SPEC §0):
                // host lattice vs host byte channel, measurement only
                com.legend.exec.CanonicalDivergence.probeEqual(
                        name, e, a, equal, incidental);
                // R2a/V11 — THE BYTE VERDICT OF RECORD for scalar-kind
                // sides: the canon rode the SIDE QUERY ITSELF (one
                // execution, wrapWithCanon); Java compares two
                // DB-computed byte strings. The host lattice above is
                // the PERMANENT PARALLEL REFEREE (ratified dual-verdict
                // design): disagreement is a pinned census row, never a
                // rescue. A decline (unclaimed kind, non-SQL arm,
                // non-scalar shape) is counted and the host judges.
                SqlVerdict byteVerdict = sqlByteVerdict(args.get(0),
                        args.get(1), ef, af, letPrefix, env, equal);
                return finish(name, wantEqual, equal,
                        byteVerdict == null ? null : byteVerdict.held(),
                        byteVerdict == null ? "" : byteVerdict.detail(),
                        () -> incidental
                                ? PureAsserts.assertSameElements(e, a)
                                : PureAsserts.assertEquals(e, a, args.get(0).info().type(), args.get(1).info().type()),
                        "byte-verdict: canonical renders differ (host"
                                + " lattice agreed — dual-verdict"
                                + " divergence, see [canon] census)");
            }
            case "assertSameElements" -> {
                if (args.size() < 2) {
                    return null;
                }
                // D3 — rendered-text sides (a sep-joined grid string
                // vs its golden): the token/line multiset judges
                ExecutionResult rse = renderedArm(name, true, args,
                        letPrefix, specs, env, hook, false);
                if (rse != null) {
                    return rse;
                }
                boolean seGridPair = tabularShaped(args.get(0))
                        || tabularShaped(args.get(1));
                SideFetch ef = sideCanon(args.get(0), letPrefix, specs,
                        env, !seGridPair, hook);
                SideFetch af = sideCanon(args.get(1), letPrefix, specs,
                        env, !seGridPair, hook);
                // §8 leg 1 — TABULAR sides under the MULTISET form:
                // loose CELL pool (the corpus writes flat expected sets
                // column-grouped — loose multiset IS this assert's
                // reference semantics, audit 9), cell-level byte canon
                if (ef.grid() != null || af.grid() != null) {
                    return tdsRowValuesSameElements(ef, af);
                }
                // a CLASS-kind side that rode a JSON carrier (a polymorphic
                // Node[1] program value) arrives as object text: decode it
                // to the structure the key restriction reads (the executor's
                // own rule for JSON slots — WORLD_MAP §4, __type rides along)
                List<Object> eVals = structuredSide(args.get(0), ef.values());
                List<Object> aVals = structuredSide(args.get(1), af.values());
                var ik = instanceKeys(args.get(0), args.get(1), env, eVals, aVals);
                List<Object> e = ik != null
                        ? restrictToKeys(eVals, ik, env.ctx()) : eVals;
                List<Object> a = ik != null
                        ? restrictToKeys(aVals, ik, env.ctx()) : aVals;
                String d = PureAsserts.assertSameElements(e, a);
                com.legend.exec.CanonicalDivergence.probeSameElements(
                        e, a, d == null);
                // V4/V11 — the multiset BYTE VERDICT OF RECORD: rows
                // arrive ORDER BY canon text (the wrap's sort key) in
                // the SAME execution; the host multiset judgment above
                // is the parallel referee.
                SqlVerdict byteVerdict = sqlByteVerdict(args.get(0),
                        args.get(1), ef, af, letPrefix, env, d == null);
                return finish("assertSameElements", true, d == null,
                        byteVerdict == null ? null : byteVerdict.held(),
                        byteVerdict == null ? "" : byteVerdict.detail(),
                        () -> d,
                        "byte-verdict: canonical sorted renders differ"
                                + " (host multiset agreed — dual-verdict"
                                + " divergence, see [canon] census)");
            }
            case "assertSize" -> {
                if (args.size() < 2) {
                    return null;
                }
                Object n = one(side(args.get(1), letPrefix, specs, env, hook),
                        "assertSize size");
                // D3: the size rule is per-result-kind — grid ROWS,
                // graph array length, values otherwise; the ONE-CARRIER
                // envelope rule ({@code $r.values} of a relation-rooted
                // execute holds one TDS) is the MODEL's
                // (ExecutionResult.envelopeCarriers), keyed by the READ
                // SHAPE exactly as the harness's cluster-34 arm.
                ExecutionResult r0 = StatementExecutor.evalValue(
                        args.get(0), letPrefix, specs, env, null, false,
                        hook);
                long actual = switch (r0) {
                    case null -> 0L;
                    case ExecutionResult.Tabular t ->
                            envelopeValuesRead(args.get(0), letPrefix)
                                    ? t.envelopeCarriers(t.rows().size())
                                    : t.rows().size();
                    case ExecutionResult.Graph g -> {
                        Object p = com.legend.sql.Json.parse(g.json());
                        yield p instanceof List<?> l ? l.size() : 1L;
                    }
                    default -> decodeSide(r0).size();
                };
                boolean heldSize = n instanceof Number num
                        && num.longValue() == actual;
                return heldSize ? ok()
                        : fail("assertSize: expected " + n + ", got "
                                + actual);
            }
            case "assertJsonStringsEqual" -> {
                // D4 — the JSON verdict: engine semantics (object keys
                // order-INSENSITIVE, arrays order-SENSITIVE) over
                // PARSED structures; JsonCompare is the one tree owner
                // (V3 register). Sides are DB-computed strings.
                if (args.size() != 2) {
                    return null;
                }
                String ejson = jsonSideText(args.get(0), letPrefix,
                        specs, env, hook);
                String ajson = jsonSideText(args.get(1), letPrefix,
                        specs, env, hook);
                if (ejson == null || ajson == null) {
                    return null;   // non-[1]-string shape: generic path
                }
                // the GOLDEN side parses first and names itself when it
                // does not: a golden the engine only accepts because its
                // json-simple parser stops after the first complete value
                // (a stray `]"` tail) is an engine-golden defect, never a
                // divergence of ours (AssertLedger's register keys on this
                // wording by exact test FQN)
                Object expected;
                try {
                    expected = com.legend.sql.Json.parse(ejson);
                } catch (IllegalStateException e) {
                    throw new IllegalStateException(
                            "golden JSON does not parse: " + e.getMessage(), e);
                }
                Object actual = com.legend.sql.Json.parse(ajson);
                // pure's [x] ≡ x at the ROOT: the engine serializes a
                // one-element result as the bare object; an enveloping
                // array bridges exactly that case (harness parity)
                if (!(expected instanceof List)
                        && actual instanceof List<?> al && al.size() == 1) {
                    actual = al.get(0);
                }
                String diff = com.legend.exec.JsonCompare.document(
                        expected, actual);
                return diff == null ? ok()
                        : fail("assertJsonStringsEqual: FIRST DIFF at "
                                + diff);
            }
            case "assertContains" -> {
                // real pure membership (assertContains.pure): both
                // sides DB-computed, the lattice judges element
                // equality; message args are failure-position only
                if (args.size() < 2) {
                    return null;
                }
                List<Object> coll = side(args.get(0), letPrefix, specs,
                        env, hook);
                List<Object> val = side(args.get(1), letPrefix, specs,
                        env, hook);
                if (val.size() != 1) {
                    return null;   // non-[1] value arg — generic path
                }
                boolean member = coll.stream().anyMatch(x ->
                        PureAsserts.equalScalar(x, val.get(0)));
                return member ? ok()
                        : fail("assertContains: " + coll
                                + " does not contain " + val.get(0));
            }
            case "assertEq" -> {
                if (args.size() < 2) {
                    return null;
                }
                SideFetch ef = sideCanon(args.get(0), letPrefix, specs,
                        env, false, hook);
                SideFetch af = sideCanon(args.get(1), letPrefix, specs,
                        env, false, hook);
                Object ee = one(ef.values(), "assertEq expected");
                Object aa = one(af.values(), "assertEq actual");
                // host judgment FIRST: eq's non-primitive identity rule
                // throws LOUD here (P2-5) before any byte verdict
                String d = PureAsserts.assertEq(ee, aa);
                com.legend.exec.CanonicalDivergence.probeEqual("assertEq",
                        java.util.Collections.singletonList(ee),
                        java.util.Collections.singletonList(aa), d == null);
                // V5/V11 — byte verdict of record (primitive eq
                // coincides with equal; the identity rule walled above)
                SqlVerdict byteVerdict = sqlByteVerdict(args.get(0),
                        args.get(1), ef, af, letPrefix, env, d == null);
                return finish("assertEq", true, d == null,
                        byteVerdict == null ? null : byteVerdict.held(),
                        byteVerdict == null ? "" : byteVerdict.detail(),
                        () -> d,
                        "byte-verdict: canonical renders differ (host"
                                + " lattice agreed — dual-verdict"
                                + " divergence, see [canon] census)");
            }
            case "assertEqWithinTolerance" -> {
                if (args.size() < 3) {
                    return null;
                }
                String d = PureAsserts.assertEqWithinTolerance(
                        (Number) one(side(args.get(0), letPrefix, specs, env, hook),
                                "tolerance expected"),
                        (Number) one(side(args.get(1), letPrefix, specs, env, hook),
                                "tolerance actual"),
                        (Number) one(side(args.get(2), letPrefix, specs, env, hook),
                                "tolerance delta"));
                return d == null ? ok() : fail(d);
            }
            case "assert", "assertFalse" -> {
                if (args.isEmpty()) {
                    return null;
                }
                // forAll-contains SUBSET (the functionvariables idiom
                // — the harness's audited fc arm, moved to the owner):
                // both sides evaluate IN THE DATABASE; the membership
                // fold is assert-level logic judged host-side (a
                // subquery inside a SQL lambda cannot lower, and
                // pure's own evaluation of this shape is in-memory).
                TypedSpec[] fc = forAllContains(args.get(0));
                if (fc != null) {
                    List<Object> need = side(fc[0], letPrefix, specs,
                            env, hook);
                    List<Object> have = side(fc[1], letPrefix, specs,
                            env, hook);
                    List<Object> missing = need.stream()
                            .filter(n2 -> have.stream().noneMatch(h ->
                                    PureAsserts.equalScalar(n2, h)))
                            .toList();
                    boolean subsetHolds = missing.isEmpty();
                    if (subsetHolds == name.equals("assert")) {
                        return ok();
                    }
                    return fail(name + " (forAll-contains subset):"
                            + " missing " + missing);
                }
                // F13c: the CONDITION rides the identity lane — eq/
                // equal over instances compile the engine relation
                // (identity/key canon); the egress is one boolean, so
                // no other lane ever sees the identity field
                Object c = one(identitySide(args.get(0), letPrefix,
                        specs, env, hook), name + " condition");
                boolean held = Boolean.TRUE.equals(c) == name.equals("assert");
                return held ? ok() : fail("Assert failed");
            }
            case "assertInstanceOf" -> {
                if (args.size() < 2) {
                    return null;
                }
                // the /3 message overload has no witness — fall through
                if (args.size() != 2) {
                    return null;
                }
                Object v = one(side(args.get(0), letPrefix, specs, env, hook),
                        "assertInstanceOf instance");
                String type = typeRefName(args.get(1));
                if (type == null) {
                    return null;   // non-literal type arg — fall through
                }
                // a CLASS value's wire carries its classifier (__type,
                // batch 53): instanceOf is the model's subtype relation
                // (a property-less class such as NullLiteral has nothing
                // else on the wire)
                if (com.legend.exec.Executor.structured(v) instanceof java.util.Map<?, ?> m
                        && m.get(com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE)
                                instanceof String wireType) {
                    boolean ok = wireType.equals(type)
                            || env.ctx().isSubtype(wireType, type);
                    return ok ? ok() : fail("expected an instance of " + type
                            + ", actual: " + wireType);
                }
                String d = PureAsserts.assertInstanceOf(v, type);
                return d == null ? ok() : fail(d);
            }
            case "assertIs" -> {
                // is() = IDENTITY (real pure is.pure:23, PCT.platformOnly).
                // World-1 adjudication for statically-identified operands
                // only; message overloads have no witness — fall through.
                if (args.size() != 2) {
                    return null;
                }
                return isVerdict(args.get(0), args.get(1));
            }
            case "assertEmpty", "assertNotEmpty" -> {
                if (args.isEmpty()) {
                    return null;
                }
                // §8 leg 1: a TABULAR side's emptiness is its ROW count
                // (engine relation semantics) — no canon involved
                ExecutionResult er = StatementExecutor.evalValue(
                        args.get(0), letPrefix, specs, env, null, false,
                        hook);
                boolean empty = er instanceof ExecutionResult.Tabular te3
                        ? te3.rows().isEmpty()
                        : decodeSide(er).isEmpty();
                boolean held = empty == name.equals("assertEmpty");
                return held ? ok()
                        : fail(name.equals("assertEmpty")
                                ? "collection is not empty"
                                : "collection is empty");
            }
            // assertError has its OWN K-arm (AssertErrorNative); every
            // other member rides the legacy inline path — the recorded
            // residual, never intercepted-and-broken
            default -> {
                return null;
            }
        }
    }

    /** The IDENTITY verdict ({@code assertIs} → {@code is()}, real pure
     * is.pure:23 "pointer equality"): adjudicable in World 1 ONLY when
     * both operands are STATICALLY identified — a type reference (bare
     * element, {@code type(x)->toOne()}, {@code genericType(x).rawType})
     * or the same let-bound instance by construction provenance. Any
     * other shape returns null: the legacy path then walls loudly on
     * {@code is}'s missing SQL rule — a wire carries values, never
     * reference identity (the eq/equalNonPrimitive irreducible ruling). */
    private static @com.legend.Nullable ExecutionResult isVerdict(
            TypedSpec left, TypedSpec right) {
        String lt = typeIdentityOf(left);
        String rt = typeIdentityOf(right);
        if (lt != null && rt != null) {
            return lt.equals(rt) ? ok()
                    : fail("\nexpected: " + lt + "\nactual:   " + rt);
        }
        TypedSpec l = instanceOrigin(left);
        TypedSpec r = instanceOrigin(right);
        if (l instanceof com.legend.compiler.spec.typed.TypedVariable lv
                && r instanceof com.legend.compiler.spec.typed.TypedVariable rv
                && lv.name().equals(rv.name())) {
            // the same let-bound variable in one frame IS the same object
            return ok();
        }
        return null;
    }

    /** The statically-known TYPE a value expression identifies, or null.
     * {@code type()}/{@code genericType().rawType} resolve to the STATIC
     * type of their argument — sound exactly when that type is concrete
     * (a literal or constructed instance), which is what the witnesses
     * pass ({@code type(+1)}, {@code genericType(^LA_Person(...))}). */
    private static @com.legend.Nullable String typeIdentityOf(TypedSpec t) {
        TypedSpec s = peel(t);
        if (s instanceof com.legend.compiler.spec.typed.TypedPackageableRef pr) {
            return canonicalTypeFqn(pr.fullPath());
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedTypeRef tr) {
            return canonicalTypeFqn(tr.target().typeName());
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedNativeCall c
                && c.callee().qualifiedName().equals(
                        "meta::pure::functions::meta::type")
                && !c.args().isEmpty()) {
            return staticTypeName(c.args().get(0));
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                && pa.property().equals("rawType")
                && peel(pa.source())
                        instanceof com.legend.compiler.spec.typed
                                .TypedNativeCall gt
                && gt.callee().qualifiedName().equals(
                        "meta::pure::functions::meta::genericType")
                && !gt.args().isEmpty()) {
            return staticTypeName(gt.args().get(0));
        }
        return null;
    }

    private static @com.legend.Nullable String staticTypeName(TypedSpec arg) {
        // concrete static identification only: a literal's primitive or a
        // constructed/class-typed value — never an Any/generic stamp
        var ty = peel(arg).info().type();
        if (ty instanceof com.legend.compiler.element.type.Type.ClassType ct) {
            return ct.fqn();
        }
        String n = ty.typeName();
        return switch (n) {
            case "Integer", "Float", "Decimal", "String", "Boolean", "Date",
                    "StrictDate", "DateTime", "StrictTime" ->
                    canonicalTypeFqn(n);
            default -> null;
        };
    }

    /** ONE spelling for a type identity: PRIMITIVES canonicalize to their
     * M3 FQN so all three resolution arms agree (bare {@code Integer},
     * {@code @Integer}, and {@code type(1)} name the same element).
     * Anything else — including packageless user test classes — keeps
     * its resolved spelling untouched. */
    private static String canonicalTypeFqn(String name) {
        return switch (name) {
            case "Integer", "Float", "Decimal", "String", "Boolean", "Date",
                    "StrictDate", "DateTime", "StrictTime", "Number" ->
                    "meta::pure::metamodel::type::" + name;
            default -> name;
        };
    }

    /** Peel value-preserving wrappers ({@code toOne}) and fold a property
     * read over a constructed instance to its constructor argument — the
     * provenance chain the OneToOne witness rides. */
    private static TypedSpec peel(TypedSpec t) {
        TypedSpec s = t;
        while (true) {
            if (s instanceof com.legend.compiler.spec.typed.TypedNativeCall c
                    && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())
                    && !c.args().isEmpty()) {
                s = c.args().get(0);
                continue;
            }
            return s;
        }
    }

    private static TypedSpec instanceOrigin(TypedSpec t) {
        TypedSpec s = peel(t);
        if (s instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                && peel(pa.source())
                        instanceof com.legend.compiler.spec.typed
                                .TypedNewInstance ni
                && ni.properties().get(pa.property()) != null) {
            return instanceOrigin(ni.properties().get(pa.property()));
        }
        return s;
    }

    /** The QUANTIFIED verdict: {@code xs->map(f|assert(pred[, 'msg']))}
     * at a statement root. The predicate VECTORIZES in the database
     * ({@code xs->map(f|pred)} — pure data computation); the boolean
     * vector is judged here, first failure raising the assert's message
     * — the interpreter's per-element semantics without an interpreter.
     * Null = not a quantified assert (generic path continues); shapes
     * beyond assert/assertFalse with a literal-or-absent message decline
     * LOUDLY. */
    /** A quantified assert over a LITERAL collection whose lambda carries
     * lets or a non-boolean assert ({@code [pair(H2, sql), ...]->map(p|
     * let driver = $p.first; ...; assertEquals($expectedSql, $result,
     * fmt, args))}): UNROLLED — each element binds the parameter as a let
     * ahead of the lambda's own lets, the inliner reduces the lets (the
     * one substitution engine), and the final assert statement
     * adjudicates as a statement-root verdict. All elements must hold.
     * Null when not this shape (a runtime collection, a one-statement
     * predicate lambda — the vector form). */
    private static @com.legend.Nullable ExecutionResult unrolled(
            com.legend.compiler.spec.typed.TypedMap qm,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            java.util.function.@com.legend.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> rawHook) {
        var lam = qm.mapper();
        // the collection through the caller's lets (let expected = [...])
        TypedSpec source = com.legend.compiler.spec.ExecuteChainAssembly
                .letBound(qm.source(), letPrefix);
        if (!(source instanceof com.legend.compiler.spec.typed.TypedCollection coll)
                || lam.parameters().size() != 1
                || lam.body().isEmpty()) {
            return null;
        }
        TypedSpec root = lam.body().get(lam.body().size() - 1);
        String fqn = calleeFqn(root);
        if (fqn == null || !fqn.startsWith(PKG)) {
            return null;
        }
        boolean simplePredicate = lam.body().size() == 1
                && (fqn.endsWith("::assert") || fqn.endsWith("::assertFalse"));
        if (simplePredicate) {
            return null;
        }
        ExecutionResult last = null;
        for (TypedSpec element : coll.elements()) {
            List<TypedSpec> reduced = com.legend.compiler.spec.VerdictQueries
                    .unrolledElement(specs, letPrefix, lam, element, rawHook);
            List<TypedSpec> lets = new java.util.ArrayList<>(
                    reduced.subList(0, reduced.size() - 1));
            TypedSpec bareStmt = reduced.get(reduced.size() - 1);
            ExecutionResult v = adjudicate(bareStmt, lets, specs, env, rawHook);
            if (v == null) {
                throw new com.legend.error.NotImplementedException(
                        "unrolled quantified assert: element verdict not"
                        + " adjudicable for " + calleeFqn(bareStmt));
            }
            last = v;
        }
        return last == null ? ok() : last;
    }

    private static @com.legend.Nullable ExecutionResult quantified(
            com.legend.compiler.spec.typed.TypedMap qm,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            @com.legend.Nullable SpliceHook hook) {
        var lam = qm.mapper();
        if (lam.body().size() != 1) {
            return null;
        }
        TypedSpec root = lam.body().get(0);
        String fqn = calleeFqn(root);
        if (fqn == null || !fqn.startsWith(PKG)) {
            return null;
        }
        String name = fqn.substring(PKG.length());
        List<TypedSpec> aargs = root instanceof TypedUserCall u ? u.args()
                : ((TypedNativeCall) root).args();
        if (!(name.equals("assert") || name.equals("assertFalse"))
                || aargs.isEmpty()) {
            throw new com.legend.error.NotImplementedException(
                    "quantified assert verdict: only map(f|assert/"
                    + "assertFalse(pred[, message])) is modeled — got '"
                    + name + "'/" + aargs.size());
        }
        String msg = aargs.size() >= 2
                && aargs.get(1) instanceof
                        com.legend.compiler.spec.typed.TypedCString cs
                ? cs.value() : "Assert failed";
        if (aargs.size() >= 2 && !(aargs.get(1) instanceof
                com.legend.compiler.spec.typed.TypedCString)) {
            throw new com.legend.error.NotImplementedException(
                    "quantified assert verdict: non-literal message"
                    + " expressions are not modeled");
        }
        // the predicate vector, computed in the database — SYNTHESIS is
        // compiler-owned (VerdictQueries, Invariant 7); the judgment
        // below stays host-side (Clause 2c)
        TypedSpec predMap = com.legend.compiler.spec.VerdictQueries
                .predicateVector(qm, lam, aargs.get(0));
        List<Object> verdicts = identitySide(predMap, letPrefix, specs, env, hook);
        boolean wantTrue = name.equals("assert");
        for (Object v : verdicts) {
            if (Boolean.TRUE.equals(v) != wantTrue) {
                return fail(msg);
            }
        }
        return ok();
    }

    /** D1 (V7_ARCH_AUDIT 2026-08-28) — THE ONE dual-verdict finisher:
     * the census probe, the verdict of record, and the failure
     * narrative all derive from a single judgment. {@code byteHeld}
     * null = the byte channel declined (already counted); the host
     * lattice judges. {@code hostMessage} is consulted ONLY when the
     * host lattice failed, and must speak then — a silent host failure
     * is a verdict/message divergence and THROWS (the reverted
     * flat-cells attempt's 28-row phantom, made structurally
     * impossible: no arm can print the byte-divergence text for a
     * judgment the byte channel never made, because the probe and the
     * message read the same two booleans). */
    private static ExecutionResult finish(String family, boolean wantEqual,
            boolean hostHeld, @com.legend.Nullable Boolean byteHeld,
            String detail,
            java.util.function.Supplier<@com.legend.Nullable String> hostMessage,
            String byteMessage) {
        if (byteHeld != null) {
            com.legend.exec.CanonicalDivergence.probeSqlVerdict(family,
                    hostHeld, byteHeld, detail);
        }
        boolean held = byteHeld != null ? byteHeld : hostHeld;
        if (held == wantEqual) {
            return ok();
        }
        if (!wantEqual) {
            return fail("assertNotEquals: both sides are equal");
        }
        String d = hostHeld ? null : hostMessage.get();
        if (!hostHeld && d == null) {
            throw new IllegalStateException(family
                    + ": verdict/message divergence — the host lattice"
                    + " failed but its message lattice held");
        }
        return fail(d != null ? d : byteMessage);
    }

    // ── §8 LEG 1 (grid canon, fusion-spike F2, user-ratified
    // 2026-08-28): a TABULAR side's byte channel is its per-ROW canon
    // (per-cell pure-literal spellings, TDS_CELL_SEP-joined, NULL
    // cells spelling bare TDSNull — disjoint from a quoted string);
    // the value peer's row canons FRAME from its literal-channel
    // element canons (chunked by the grid's width — framing writes
    // only separators, never renders). The host cell lattice stays
    // the PARALLEL REFEREE, and the failure message derives from the
    // SAME judgment that failed (the reverted attempt's 28-row
    // phantom: message and judgment from different lattices with the
    // probe unfired — structurally impossible here).

    /** The FLAT-CELLS verdict for a pair with at least one TABULAR
     * side (both-wrapped pairs took the grid-pair arm earlier). */
    private static ExecutionResult tdsRowValuesVerdict(String name,
            boolean wantEqual, List<TypedSpec> args,
            List<TypedSpec> letPrefix, SideFetch ef, SideFetch af,
            boolean incidental) {
        List<Object> e = ef.values();
        List<Object> a = af.values();
        // audit 22b F2: raw cells (a bare .rows view) never equal a
        // WHOLE-TDS value — flattening the TDS side would fabricate a
        // match its column-name pin refuses. Static stamps decide.
        boolean mixedFlatVsTds =
                (bareRowStamp(args.get(0), letPrefix) && af.grid() != null
                        && wrappedRelationStamp(args.get(1), letPrefix))
                || (bareRowStamp(args.get(1), letPrefix) && ef.grid() != null
                        && wrappedRelationStamp(args.get(0), letPrefix));
        boolean hostHeld;
        if (mixedFlatVsTds) {
            hostHeld = false;
        } else {
            hostHeld = PureAsserts.equal(e, a);
            if (!hostHeld && incidental && e.size() == a.size()) {
                // ROW COHESION (audit 9): the incidental-order fallback
                // matches ROW TUPLES of the grid's width — cross-row
                // cell shuffles must FAIL; width 1 = the pool multiset
                ExecutionResult.Tabular g = af.grid() != null ? af.grid()
                        : ef.grid();
                int w = g != null ? g.columns().size() : 1;
                hostHeld = com.legend.exec.TdsCompare.rowTupleMultiset(
                        e, a, w > 1 && e.size() % w == 0 ? w : 1);
            }
        }
        Boolean byteHeld = null;
        String detail = "";
        if (!mixedFlatVsTds) {
            ExecutionResult.Tabular wg = af.grid() != null ? af.grid()
                    : java.util.Objects.requireNonNull(ef.grid(),
                            "grid verdict without a grid side");
            int w = wg.columns().size();
            List<String> ec = sideRowCanons(ef, w, true);
            List<String> ac = sideRowCanons(af, w, false);
            if (ec != null && ac != null) {
                List<String> es = new ArrayList<>(ec);
                List<String> as2 = new ArrayList<>(ac);
                if (incidental) {
                    es.sort(String::compareTo);
                    as2.sort(String::compareTo);
                }
                byteHeld = es.equals(as2);
                // the DECLARED 2-ULP dialect-arithmetic policy, grid
                // form (the scalar channel's withinDeclaredUlp arm):
                // byte-differing rows whose every POSITIONAL cell pair
                // holds in the lattice with only finite-Double drift
                // hold BY POLICY — counted in the policy's own census
                // row, never a disagreement rescue.
                if (!byteHeld && hostHeld
                        && com.legend.exec.TdsCompare
                                .ulpOnlyCellDrift(e, a)) {
                    com.legend.exec.CanonicalDivergence.sqlUlpPolicy(
                            "grid " + com.legend.exec.TdsCompare
                                    .firstCanonDiff(es, as2));
                    byteHeld = true;
                }
                detail = "tds rows=" + ec.size() + "/" + ac.size()
                        + (byteHeld ? "" : com.legend.exec.TdsCompare
                                .firstCanonDiff(es, as2));
            }
        }
        return finish(name, wantEqual, hostHeld, byteHeld, detail,
                () -> mixedFlatVsTds
                        ? name + " (TDSRow.values) raw cells do not"
                                + " equal a whole TDS value"
                        : tdsHostMessage(name,
                                PureAsserts.assertEquals(e, a)),
                "byte-verdict: grid canonical renders differ (host"
                        + " lattice agreed — dual-verdict divergence,"
                        + " see [canon] census)");
    }

    /** The TDSRow.values failure narrative — the host lattice's text
     * with the pure-API prefix; null iff the lattice held. */
    private static @com.legend.Nullable String tdsHostMessage(String name,
            @com.legend.Nullable String d) {
        return d == null ? null
                : name + " (TDSRow.values) " + d.replaceFirst("^\\n", "");
    }

    /** The MULTISET flat-cells verdict: loose CELL pool host lattice
     * (direction-aware sentinel — pool matching, never a sorted zip:
     * sorting separates an expected 'TDSNull' from its NULL cell),
     * cell-level canon multiset as the byte channel. */
    private static ExecutionResult tdsRowValuesSameElements(SideFetch ef,
            SideFetch af) {
        List<Object> e = ef.values();
        List<Object> a = af.values();
        boolean hostHeld = e.size() == a.size()
                && com.legend.exec.TdsCompare.rowTupleMultiset(e, a, 1);
        List<String> ec = sideCellCanons(ef, true);
        List<String> ac = sideCellCanons(af, false);
        Boolean byteHeld = null;
        String detail = "";
        if (ec != null && ac != null) {
            List<String> es = new ArrayList<>(ec);
            List<String> as2 = new ArrayList<>(ac);
            es.sort(String::compareTo);
            as2.sort(String::compareTo);
            byteHeld = es.equals(as2);
            detail = "tds cells=" + ec.size() + "/" + ac.size()
                    + (byteHeld ? "" : com.legend.exec.TdsCompare
                            .firstCanonDiff(es, as2));
        }
        return finish("assertSameElements", true, hostHeld, byteHeld,
                detail,
                () -> {
                    String d = PureAsserts.assertSameElements(e, a);
                    return d != null
                            ? tdsHostMessage("assertSameElements", d)
                            : "assertSameElements (TDSRow.values): cell"
                                    + " multiset differs";
                },
                "byte-verdict: grid canonical renders differ (host"
                        + " lattice agreed — dual-verdict divergence,"
                        + " see [canon] census)");
    }

    /** A side's per-ROW canon texts via the grid policy owner: a
     * grid side reads its harvested row canons; a value peer frames
     * rows from its literal-channel element canons ({@link
     * com.legend.exec.TdsCompare} owns every rule and decline). */
    private static @com.legend.Nullable List<String> sideRowCanons(
            SideFetch side, int width, boolean isExpected) {
        return side.grid() != null
                ? com.legend.exec.TdsCompare.tdsRowCanons(side.rider())
                : com.legend.exec.TdsCompare.peerRowCanons(side.rider(),
                        side.values().size(), width, isExpected);
    }

    /** A side's per-CELL canon texts (the sameElements view), via the
     * grid policy owner. */
    private static @com.legend.Nullable List<String> sideCellCanons(
            SideFetch side, boolean isExpected) {
        return side.grid() != null
                ? com.legend.exec.TdsCompare.tdsCellCanons(side.rider())
                : com.legend.exec.TdsCompare.peerElementCanons(
                        side.rider(), side.values().size(), isExpected);
    }

    /** Whether a side is STATICALLY table-shaped (its declared result
     * shape — the same fact the executor's canon routing reads). */
    private static boolean tabularShaped(TypedSpec s) {
        return com.legend.exec.ResultShape.of(s)
                == com.legend.exec.ResultShape.TABULAR;
    }

    /** A bare {@code .rows} view stamp (row collection — bare struct,
     * many multiplicity), through let bindings. */
    private static boolean bareRowStamp(TypedSpec s0,
            List<TypedSpec> lets) {
        TypedSpec s = chaseLets(s0, lets);
        return s.info().type()
                instanceof com.legend.compiler.element.type.Type.RelationType
                && s.info().multiplicity().isMany();
    }

    /** A wrapped table stamp ({@code Relation<schema>}), through let
     * bindings. */
    private static boolean wrappedRelationStamp(TypedSpec s0,
            List<TypedSpec> lets) {
        return com.legend.compiler.element.type.Type.isRelation(
                chaseLets(s0, lets).info().type());
    }

    // ── D3 (batch-2 slice 2): the GOLDEN GRID/ORDER conventions move
    // into verdict construction — the ORDER VIEW of a side, the
    // rendered-text forms, and the grid-pair route. The comparison
    // POLICIES stay with their one production owner (TdsCompare).

    /** A side's order semantics: SORTED (ends in a sort through
     * order-preserving tails — the engine contract pins the order),
     * INCIDENTAL (bottoms at a store source or an execution-frame
     * read with no sort — SQL arrival order, engine goldens encode
     * H2's), DEFINED (pure values — the language's own order). */
    enum OrderView { SORTED, INCIDENTAL, DEFINED }

    private static final java.util.Set<String> SORT_FQNS = java.util.Set.of(
            "meta::pure::functions::collection::sort",
            "meta::pure::functions::collection::sortBy",
            "meta::pure::functions::collection::sortByReversed",
            "meta::pure::functions::relation::sort");

    /** Order-preserving native tails, BY SIMPLE NAME — the harness's
     * audited list (audit 23 D1), moved verbatim. */
    private static final java.util.Set<String> ORDER_PRESERVING =
            java.util.Set.of("map", "limit", "take", "drop", "slice",
                    "rows", "toOne", "at", "makeString", "toCSV",
                    "toString", "from", "filter", "select", "rename",
                    "renameColumns", "restrict", "project", "distinct");

    static OrderView orderView(TypedSpec s0, List<TypedSpec> letPrefix) {
        return orderView(s0, letPrefix, new java.util.HashSet<>());
    }

    private static OrderView orderView(TypedSpec s, List<TypedSpec> lets,
            java.util.Set<String> seen) {
        if (s instanceof com.legend.compiler.spec.typed.TypedSort
                || s instanceof com.legend.compiler.spec.typed.TypedSortBy) {
            return OrderView.SORTED;
        }
        if (s instanceof TypedNativeCall c) {
            String fqn = c.callee().qualifiedName();
            if (SORT_FQNS.contains(fqn)) {
                return OrderView.SORTED;
            }
            String simple = fqn.substring(fqn.lastIndexOf(':') + 1);
            if (ORDER_PRESERVING.contains(simple) && !c.args().isEmpty()) {
                return orderView(c.args().get(0), lets, seen);
            }
            return OrderView.DEFINED;
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedGetAll
                || s instanceof com.legend.compiler.spec.typed
                        .TypedTableReference
                || s instanceof com.legend.compiler.spec.typed
                        .TypedRawSqlRelation) {
            return OrderView.INCIDENTAL;
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedVariable v) {
            if (!seen.add(v.name())) {
                return OrderView.DEFINED;
            }
            for (int i = lets.size() - 1; i >= 0; i--) {
                if (lets.get(i) instanceof
                        com.legend.compiler.spec.typed.TypedLet l
                        && l.name().equals(v.name())) {
                    return orderView(l.value(), lets, seen);
                }
            }
            // unresolvable binding = an execution frame ($result) —
            // its chain is a store query by construction
            return OrderView.INCIDENTAL;
        }
        // order-preserving wrappers descend to their SOURCE (first
        // child); anything else keeps the language's defined order
        if (s instanceof com.legend.compiler.spec.typed.TypedFilter
                || s instanceof com.legend.compiler.spec.typed.TypedProject
                || s instanceof com.legend.compiler.spec.typed.TypedSelect
                || s instanceof com.legend.compiler.spec.typed.TypedRename
                || s instanceof com.legend.compiler.spec.typed.TypedDistinct
                || s instanceof com.legend.compiler.spec.typed.TypedLimit
                || s instanceof com.legend.compiler.spec.typed.TypedDrop
                || s instanceof com.legend.compiler.spec.typed.TypedSlice
                || s instanceof com.legend.compiler.spec.typed.TypedMap
                || s instanceof com.legend.compiler.spec.typed
                        .TypedPropertyAccess
                || s instanceof com.legend.compiler.spec.typed.TypedCast
                || s instanceof com.legend.compiler.spec.typed.TypedFrom
                || s instanceof com.legend.compiler.spec.typed.TypedNavigate
                || s instanceof com.legend.compiler.spec.typed
                        .TypedMilestonedAccess) {
            List<TypedSpec> ch = s.children();
            return ch.isEmpty() ? OrderView.DEFINED
                    : orderView(ch.get(0), lets, seen);
        }
        return OrderView.DEFINED;
    }

    private static final String FQ_TO_STRING =
            "meta::pure::functions::string::toString";
    private static final String FQ_REPLACE =
            "meta::pure::functions::string::replace";
    private static final String FQ_MAKE_STRING =
            "meta::pure::functions::string::makeString";
    private static final String FQ_JOIN_STRINGS =
            "meta::pure::functions::string::joinStrings";

    private static TypedSpec chaseLets(TypedSpec s0, List<TypedSpec> lets) {
        TypedSpec s = s0;
        java.util.Set<String> seen = new java.util.HashSet<>();
        while (s instanceof com.legend.compiler.spec.typed.TypedVariable v
                && seen.add(v.name())) {
            TypedSpec bound = null;
            for (int i = lets.size() - 1; i >= 0; i--) {
                if (lets.get(i) instanceof
                        com.legend.compiler.spec.typed.TypedLet l
                        && l.name().equals(v.name())) {
                    bound = l.value();
                    break;
                }
            }
            if (bound == null) {
                return s;
            }
            s = bound;
        }
        return s;
    }

    /** The D3 RENDERED-TEXT verdict, or null when the pair is not the
     * shape (exactly one side a render form, both sides one string).
     * {@code orderedForm} false = the sameElements view (token/line
     * multiset regardless of the chain's sort). */
    private static @com.legend.Nullable ExecutionResult renderedArm(
            String name, boolean wantEqual, List<TypedSpec> args,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            @com.legend.Nullable SpliceHook hook, boolean orderedForm) {
        String eForm = renderForm(args.get(0), letPrefix);
        String aForm = renderForm(args.get(1), letPrefix);
        // BOTH-RENDERED same-form pairs qualify too (two renders of one
        // unsorted query — pure guarantees the row MULTISET; each
        // render freezes its own execution's incident order, so a byte
        // compare was a coin flip: union testProjectThroughAsso's
        // toCSV-vs-toCSV flicker). Mismatched forms fall through.
        if ((eForm == null && aForm == null)
                || (eForm != null && aForm != null
                        && !eForm.equals(aForm))) {
            return null;
        }
        String form = aForm != null ? aForm
                : java.util.Objects.requireNonNull(eForm);
        TypedSpec rendered = aForm != null ? args.get(1) : args.get(0);
        List<Object> ev = side(args.get(0), letPrefix, specs, env, hook);
        List<Object> av = side(args.get(1), letPrefix, specs, env, hook);
        if (ev.size() == 1 && ev.get(0) instanceof String et
                && av.size() == 1 && av.get(0) instanceof String at) {
            // a BOTH-rendered pair always judges as a multiset (even
            // sorted queries legally tie-flip between two executions)
            boolean sorted = orderedForm
                    && (eForm == null ^ aForm == null)
                    && orderView(rendered, letPrefix) == OrderView.SORTED;
            boolean held = com.legend.exec.TdsCompare.renderedText(
                    aForm != null ? et : at, aForm != null ? at : et,
                    form, sorted);
            if (held != wantEqual) {
                return fail(name + " (rendered " + form + "): "
                        + firstTextDiff(et, at));
            }
            return ok();
        }
        // a render form whose sides are not two strings — loud, never
        // a silent fall-through re-execution
        throw new com.legend.error.NotImplementedException(
                "rendered-text assert side is not a string pair ("
                        + form + ")");
    }

    /** One-line first-difference sketch of two rendered texts (failure
     * message position — the full texts drown the diagnosis). */
    private static String firstTextDiff(String e, String a) {
        String[] el = e.split("\n", -1);
        String[] al = a.split("\n", -1);
        if (el.length != al.length) {
            return "line-count " + el.length + " != " + al.length
                    + " (expected first line: " + el[0] + ")";
        }
        for (int i = 0; i < el.length; i++) {
            if (!el[i].equals(al[i])) {
                return "line " + i + ": expected <" + el[i] + "> got <"
                        + al[i] + ">";
            }
        }
        return "texts differ only in leniency-adjudicated cells";
    }

    /** The RENDERED-TEXT form of a side, or null: toCSV → CSVTEXT,
     * toString over a relation → TDSTEXT, toCSV->replace('\n', sep) →
     * CSVJOIN:sep (the calendar family's one-line spelling), and a
     * makeString/joinStrings join over an INCIDENTAL-order chain →
     * CSVJOIN:sep (token multiset — sep-joined DB arrival order).
     * The comparison policy is {@link com.legend.exec.TdsCompare
     * #renderedText} — the one owner, probed by its own R1b census. */
    private static @com.legend.Nullable String renderForm(TypedSpec s0,
            List<TypedSpec> lets) {
        TypedSpec s = chaseLets(s0, lets);
        if (s instanceof TypedNativeCall rep
                && FQ_REPLACE.equals(rep.callee().qualifiedName())
                && rep.args().size() == 3
                && chaseLets(rep.args().get(0), lets)
                        instanceof TypedNativeCall csv
                && com.legend.compiler.element.type.PlatformTypes.TO_CSV
                        .equals(csv.callee().qualifiedName())
                && rep.args().get(1) instanceof
                        com.legend.compiler.spec.typed.TypedCString from
                && "\n".equals(from.value())
                && rep.args().get(2) instanceof
                        com.legend.compiler.spec.typed.TypedCString to) {
            return "CSVJOIN:" + to.value();
        }
        if (s instanceof TypedNativeCall csv2
                && com.legend.compiler.element.type.PlatformTypes.TO_CSV
                        .equals(csv2.callee().qualifiedName())
                && csv2.args().size() == 1) {
            return "CSVTEXT";
        }
        if (s instanceof TypedNativeCall ts
                && FQ_TO_STRING.equals(ts.callee().qualifiedName())
                && ts.args().size() == 1
                && com.legend.compiler.element.type.Type.isRelation(
                        ts.args().get(0).info().type())) {
            return "TDSTEXT";
        }
        if (s instanceof TypedNativeCall j
                && (FQ_MAKE_STRING.equals(j.callee().qualifiedName())
                        || FQ_JOIN_STRINGS.equals(j.callee().qualifiedName()))
                && j.args().size() == 2
                && j.args().get(1) instanceof
                        com.legend.compiler.spec.typed.TypedCString sep
                && orderView(j.args().get(0), lets) == OrderView.INCIDENTAL) {
            return "CSVJOIN:" + sep.value();
        }
        return null;
    }

    /** The cluster-34 envelope READ SHAPE: {@code $r.values} (through
     * optional toOne/first/at(0) peels) over a binding OUTSIDE the let
     * prefix — an execution frame; the TDS envelope is ONE carrier. */
    private static boolean envelopeValuesRead(TypedSpec arg,
            List<TypedSpec> letPrefix) {
        TypedSpec s = arg;
        while (s instanceof TypedNativeCall c && !c.args().isEmpty()) {
            String fqn = c.callee().qualifiedName();
            String simple = fqn.substring(fqn.lastIndexOf(':') + 1);
            if (simple.equals("toOne") || simple.equals("first")
                    || (simple.equals("at") && c.args().size() == 2
                            && c.args().get(1) instanceof
                                    com.legend.compiler.spec.typed
                                            .TypedCInteger ci
                            && ci.value().longValue() == 0)) {
                s = c.args().get(0);
                continue;
            }
            break;
        }
        if (!(s instanceof com.legend.compiler.spec.typed
                .TypedPropertyAccess pa
                && pa.property().equals("values")
                && pa.source() instanceof
                        com.legend.compiler.spec.typed.TypedVariable v)) {
            return false;
        }
        for (TypedSpec l : letPrefix) {
            if (l instanceof com.legend.compiler.spec.typed.TypedLet tl
                    && tl.name().equals(v.name())) {
                return false;   // an ordinary let, not a frame
            }
        }
        return true;
    }

    /** The {@code $exp->forAll(e|$act->contains($e))} SUBSET shape:
     * {expected, actual} sources, or null when not this idiom (the
     * predicate must be a contains of the forAll binder itself). */
    private static TypedSpec @com.legend.Nullable [] forAllContains(
            TypedSpec a0) {
        if (a0 instanceof TypedNativeCall fa
                && "meta::pure::functions::collection::forAll"
                        .equals(fa.callee().qualifiedName())
                && fa.args().size() == 2
                && fa.args().get(1) instanceof
                        com.legend.compiler.spec.typed.TypedLambda lam
                && lam.parameters().size() == 1
                && lam.body().size() == 1
                && lam.body().get(0) instanceof TypedNativeCall cont
                && "meta::pure::functions::collection::contains"
                        .equals(cont.callee().qualifiedName())
                && cont.args().size() == 2
                && cont.args().get(1) instanceof
                        com.legend.compiler.spec.typed.TypedVariable ev
                && ev.name().equals(lam.parameters().get(0))) {
            return new TypedSpec[] {fa.args().get(0), cont.args().get(0)};
        }
        return null;
    }

    private static @com.legend.Nullable String calleeFqn(TypedSpec bare) {
        if (bare instanceof TypedUserCall u) {
            return u.callee().qualifiedName();
        }
        if (bare instanceof TypedNativeCall n
                && !com.legend.compiler.element.type.PlatformTypes.ASSERT_ERROR
                        .equals(n.callee().qualifiedName())) {
            return n.callee().qualifiedName();
        }
        return null;
    }

    /** One assert SIDE: the argument expression executed in the
     * database through the ordinary pipeline, flattened to wire values
     * (a null scalar is the EMPTY collection — pure [0..1] emptiness). */
    /** R2a kind gate + DB renders: both sides must have the SAME
     * statically-stamped kind class (cross-kind incl. empty-vs-empty
     * stays the host lattice's — DECLINE, not a guess); the byte
     * verdict is equality of the two DB-computed canonical texts
     * (null text = EMPTY side; empty==empty holds, empty==value
     * fails — pure's own rule). */
    record SqlVerdict(boolean held, String detail) {
    }

    private static @com.legend.Nullable SqlVerdict sqlByteVerdict(
            TypedSpec eSpec, TypedSpec aSpec, SideFetch ef, SideFetch af,
            List<TypedSpec> letPrefix, StatementExecutor.ExecEnv env,
            boolean hostHeld) {
        List<Object> eVals = ef.values();
        List<Object> aVals = af.values();
        String ke = kindClassOf(eSpec.info().type());
        String ka = kindClassOf(aSpec.info().type());
        boolean eAny = isAnyStamped(eSpec);
        boolean aAny = isAnyStamped(aSpec);
        // MIXED-KIND numeric collections are unsound under SQL column
        // promotion (pure refuses 1 == 1.0 element-wise; one DOUBLE
        // column erases the distinction) — the HOST-fetched element
        // kinds gate the route (a routing fact, not a verdict). An
        // ANY side is EXEMPT (F10 v1): its JSON carrier never promotes
        // — each cell keeps its own kind and the literal channel spells
        // 1 and 1.0 apart.
        // ... and a LITERAL-ONLY side (the F10 kind-faithful carrier —
        // Number-stamped mixed LITERAL collections and mixed-sort
        // results ride it, label LITERAL) is equally exempt: its cells
        // never promote. RESIDUAL GUARD (F10 slice 2): only a COMPUTED
        // mixed collection (concatenated/derived, not a literal — no
        // carrier claim yet) can reach this decline; zero witnesses
        // today and the ceiling is pinned 0, so a firing is a NAMED
        // work item, never a silent count.
        if ((!eAny && !ef.rider().literalOnly() && mixedNumericKinds(eVals))
                || (!aAny && !af.rider().literalOnly()
                        && mixedNumericKinds(aVals))) {
            com.legend.exec.CanonicalDivergence.sqlDeclined(
                    "mixed-kind-collection");
            return null;
        }
        // X5: a Nil-stamped side is the []-born EMPTY value — pure
        // equality against ANY kind is decided by emptiness alone
        // (equal([], x) is element-wise vacuous), so the kind classes
        // need not match; both canons frame '[]' when empty and any
        // non-empty side byte-differs from '[]' — the engine's answer.
        boolean anyNil = com.legend.compiler.element.type.PlatformTypes
                .isNil(eSpec.info().type())
                || com.legend.compiler.element.type.PlatformTypes
                        .isNil(aSpec.info().type());
        // F10 v1: an ANY-stamped side has no static kind — the pair
        // compares in the pure-LITERAL channel (six disjoint spellings
        // carry kind in the bytes), so the static gate defers
        boolean anyAny = eAny || aAny;
        if (ke == null || ka == null
                || (!anyNil && !anyAny && !ke.equals(ka))) {
            com.legend.exec.CanonicalDivergence.sqlDeclined("kind-gate: "
                    + typeName(eSpec) + " / " + typeName(aSpec));
            return null;
        }
        // V11: the canon rode each side's OWN query (wrapWithCanon) —
        // a decline recorded by the wrap (non-SQL arm, unclaimed kind,
        // non-scalar shape) routes the pair to the host lattice.
        if (ef.rider().declined() != null) {
            com.legend.exec.CanonicalDivergence.sqlDeclined(
                    "side-e: " + ef.rider().declined());
            return null;
        }
        if (af.rider().declined() != null) {
            com.legend.exec.CanonicalDivergence.sqlDeclined(
                    "side-a: " + af.rider().declined());
            return null;
        }
        // F13 — IDENTITY-pair guards (keyless class: the canon claimed
        // via the synthetic __id identity field). Map carriers are NOT
        // identity pairs — mapEquals (F12) is their own claimed rule.
        if (!anyNil && !anyAny && ke.startsWith("instance:")
                && !com.legend.compiler.element.type.PlatformTypes
                        .isMapCarrier(eSpec.info().type())
                && instanceKeys(eSpec, aSpec, env) == null) {
            // v1 exclusion: a constructor under a LAMBDA evaluates per
            // element but mints ONE site id (no row index reaches
            // list_transform) — identity would conflate distinct
            // instances; decline, counted (OPEN_REGISTER F13).
            List<TypedSpec> scope = new ArrayList<>(letPrefix);
            scope.add(eSpec);
            scope.add(aSpec);
            if (keylessCtorUnderLambda(scope, env)) {
                com.legend.exec.CanonicalDivergence.sqlDeclined(
                        "keyless-ctor-in-lambda: " + ke);
                return null;
            }
            // an instance wire that carries NO id (a producer outside
            // the minting sites) must never byte-judge — identity
            // unknown is a decline, never a fabricated equality
            for (Object v : concat(eVals, aVals)) {
                if (v instanceof java.util.Map<?, ?> m
                        && m.get(com.legend.compiler.element.ClassLayouts
                                .SYNTHETIC_ID) == null) {
                    com.legend.exec.CanonicalDivergence.sqlDeclined(
                            "identityless-instance-wire: " + ke);
                    return null;
                }
            }
        }
        // X4 (VERDICT_RULE_AUDIT): the engine has NO cross-primitive-
        // kind equality — numeric pairs must be the SAME fine kind.
        // Abstract Number stamps projected one candidate column per
        // kind; the RUNTIME value kinds (pure's own Number dispatch)
        // SELECT the column — selection, never evaluation. Cross-kind
        // pairs decline to the host lattice's engine-FALSE.
        // V7 batch 2 (corpus alarm witness GeographicEntityType): an
        // ENUM kind cannot ride the literal channel — the Any/literal
        // wire spells the enum VALUE as a string ('CITY') while the
        // enum canon spells the bare name (CITY), so a byte compare
        // fabricates inequality where pure's own enum equality holds.
        // Decline, counted; the host lattice judges.
        if (!anyNil && (anyAny || ef.rider().literalOnly()
                || af.rider().literalOnly())
                && (ke.startsWith("enum:") || ka.startsWith("enum:"))) {
            com.legend.exec.CanonicalDivergence.sqlDeclined(
                    "any-pair: enum kind has no literal channel: "
                            + (ke.startsWith("enum:") ? ke : ka));
            return null;
        }
        int ei = 0;
        int ai = 0;
        if ((anyAny || ef.rider().literalOnly()
                || af.rider().literalOnly()) && !anyNil) {
            // both sides compare in the literal channel; a side without
            // one (unrefined Number, non-literal kind) declines
            ei = ef.rider().literalIndex();
            ai = af.rider().literalIndex();
            if (ei < 0 || ai < 0) {
                com.legend.exec.CanonicalDivergence.sqlDeclined(
                        "any-pair: no literal channel: " + typeName(eSpec)
                                + " / " + typeName(aSpec));
                return null;
            }
        } else if (!anyNil && ke.equals("numeric")) {
            String fe = selectedFineKind(ef, eVals);
            String fa = selectedFineKind(af, aVals);
            if (fe == null || fa == null) {
                com.legend.exec.CanonicalDivergence.sqlDeclined(
                        "unrefined-number: " + typeName(eSpec) + " / "
                                + typeName(aSpec));
                return null;
            }
            if (!fe.equals(fa)) {
                com.legend.exec.CanonicalDivergence.sqlDeclined(
                        "cross-kind-numeric: " + fe + "/" + fa);
                return null;
            }
            ei = candidateIndex(ef, fe);
            ai = candidateIndex(af, fa);
            if (ei < 0 || ai < 0) {
                com.legend.exec.CanonicalDivergence.sqlDeclined(
                        "unrefined-number: no candidate for " + fe);
                return null;
            }
        }
        Framed fe2 = frame(ef, ei);
        Framed fa2 = frame(af, ai);
        if (fe2.decline() != null || fa2.decline() != null) {
            com.legend.exec.CanonicalDivergence.sqlDeclined(
                    fe2.decline() != null ? "render-e: " + fe2.decline()
                            : "render-a: " + fa2.decline());
            return null;
        }
        if (containsTreeMarker(fe2.text())
                || containsTreeMarker(fa2.text())) {
            // an Any cell held a JSON tree — the literal channel cannot
            // spell it (F10 proper's kind-tagged carrier will); decline,
            // never compare markers (equal trees would fabricate)
            com.legend.exec.CanonicalDivergence.sqlDeclined(
                    "any-wire-tree: " + typeName(eSpec) + " / "
                            + typeName(aSpec));
            return null;
        }
        boolean byteEqual = java.util.Objects.equals(fe2.text(), fa2.text());
        String detail = "kinds=" + ef.rider().kinds().get(ei) + "/"
                + af.rider().kinds().get(ai)
                + " e<" + fe2.text() + "> a<" + fa2.text() + ">";
        // DECLARED 2-ULP dialect-arithmetic policy (OPEN_REGISTER §5,
        // X6/R3 owns its retirement): cross-dialect libm computes
        // transcendentals a last ULP apart (H2-derived corpus goldens
        // vs DuckDB acos/log/tan). The policy rides ON TOP of the byte
        // channel — byte-differing all-finite-Double pairs within
        // 2 ULP hold BY POLICY, counted in their own census row (the
        // host lattice carries the same policy, so this is never a
        // disagreement rescue). Before runtime-kind refinement these
        // pairs declined as unrefined NUMBER and the host policy
        // decided; the refinement must not silently retire the policy.
        if (!byteEqual && withinDeclaredUlp(eVals, aVals)) {
            com.legend.exec.CanonicalDivergence.sqlUlpPolicy(detail);
            return new SqlVerdict(true, "2ulp-policy " + detail);
        }
        // DECLARED TDSNull-sentinel policy (PureAsserts equalScalar:
        // an EXPECTED literal 'TDSNull' equals an actual NULL cell —
        // the engine golden's null spelling; audit 16 F5 keeps it
        // direction-aware). The canon spells the two differently by
        // construction, so a byte-differing pair that the host lattice
        // HOLDS and whose expected side carries the sentinel holds BY
        // POLICY — counted in its own census row (the 2-ULP shape),
        // never a silent rescue.
        if (!byteEqual && hostHeld && containsTdsNullSentinel(eVals)) {
            com.legend.exec.CanonicalDivergence.sqlTdsNullPolicy(detail);
            return new SqlVerdict(true, "tdsnull-policy " + detail);
        }
        return new SqlVerdict(byteEqual, detail);
    }

    /** The engine golden's null spelling anywhere in the EXPECTED wire
     * values (scalar cells, instance properties, nested lists). */
    private static boolean containsTdsNullSentinel(
            @com.legend.Nullable Object v) {
        return switch (v) {
            case null -> false;
            case String s -> "TDSNull".equals(s);
            case List<?> l -> l.stream()
                    .anyMatch(AssertVerdicts::containsTdsNullSentinel);
            case java.util.Map<?, ?> m -> m.values().stream()
                    .anyMatch(AssertVerdicts::containsTdsNullSentinel);
            default -> false;
        };
    }

    private static boolean isAnyStamped(TypedSpec s) {
        return s.info().type() instanceof
                com.legend.compiler.element.type.Type.ClassType ct
                && com.legend.compiler.element.type.PlatformTypes.isAny(ct);
    }

    private static boolean containsTreeMarker(
            @com.legend.Nullable String text) {
        return text != null && text.contains(
                com.legend.lowering.CanonicalRenderSql.TREE_MARKER);
    }

    private static List<Object> concat(List<Object> a, List<Object> b) {
        List<Object> out = new ArrayList<>(a.size() + b.size());
        out.addAll(a);
        out.addAll(b);
        return out;
    }

    /** F13 v1 exclusion scan: any KEYLESS model-class constructor (or
     * copy) under a lambda anywhere in the verdict's scope — the site
     * id cannot distinguish per-element evaluations. A plain
     * containment walk over {@code children()} (no shadow concerns —
     * this detects presence, it never resolves variables). */
    private static boolean keylessCtorUnderLambda(List<TypedSpec> roots,
            StatementExecutor.ExecEnv env) {
        for (TypedSpec r : roots) {
            if (scanKeylessCtor(r, false, env)) {
                return true;
            }
        }
        return false;
    }

    private static boolean scanKeylessCtor(TypedSpec n, boolean inLambda,
            StatementExecutor.ExecEnv env) {
        if (inLambda) {
            String fqn = n instanceof
                    com.legend.compiler.spec.typed.TypedNewInstance ni
                    ? ni.classFqn()
                    : n instanceof
                            com.legend.compiler.spec.typed.TypedCopyInstance cp
                            ? cp.classFqn() : null;
            if (fqn != null && env.ctx().findClass(fqn).isPresent()
                    && com.legend.compiler.element.EqualityKeys
                            .resolve(env.ctx(), fqn) == null) {
                return true;
            }
        }
        boolean in = inLambda
                || n instanceof com.legend.compiler.spec.typed.TypedLambda;
        for (TypedSpec k : n.children()) {
            if (scanKeylessCtor(k, in, env)) {
                return true;
            }
        }
        return false;
    }

    /** The fine numeric kind whose candidate column judges this side:
     * a refined stamp names it directly; an unrefined Number resolves
     * from the RUNTIME value kinds; null = undeterminable (decline). */
    private static @com.legend.Nullable String selectedFineKind(
            SideFetch f, List<Object> vals) {
        List<com.legend.compiler.element.type.Type> kinds = f.rider().kinds();
        if (kinds.size() == 1) {
            return fineNumericKind(kinds.get(0));
        }
        String k = runtimeNumericKind(vals);
        // B8: a runtime BigDecimal is evidence of the CARRIER, not the
        // kind — precision-exact Float literals are decimal-carried BY
        // DESIGN (the reference's own interpreted Float is
        // BigDecimal-backed; receipt in CFloat). When the compiler's
        // candidate set rules pure-Decimal OUT (no decimal candidate)
        // and a float candidate exists, the value IS a decimal-carried
        // Float and judges through the float canon. Static truth gates
        // the resolution; the value alone never decides a kind.
        if ("decimal".equals(k) && candidateIndex(f, "decimal") < 0
                && candidateIndex(f, "float") >= 0) {
            return "float";
        }
        return k;
    }

    /** Index of the fine kind's candidate column in the rider's
     * projection order, or -1. */
    private static int candidateIndex(SideFetch f, String fine) {
        List<com.legend.compiler.element.type.Type> kinds = f.rider().kinds();
        for (int i = 0; i < kinds.size(); i++) {
            if (fine.equals(fineNumericKind(kinds.get(i)))) {
                return i;
            }
        }
        return -1;
    }

    /** A framed side canon: {@code text} null = EMPTY (two empties are
     * byte-equal, as before); {@code decline} = an unframeable side. */
    private record Framed(@com.legend.Nullable String text,
            @com.legend.Nullable String decline) {
    }

    /** CanonicalForm.renderSide framing over the DB-computed element
     * texts (V11): 0 elements → '[]', 1 → the bare text, N → '[a, b]'.
     * The DATABASE computed every element's canonical text and (for
     * assertSameElements) the canonical order; this join writes only
     * the spec's separators — framing, never rendering. */
    private static Framed frame(SideFetch f, int idx) {
        List<String[]> rows = f.rider().rows();
        if (!f.rider().many()) {
            // EVERY empty form canons '[]' (X5 unification): pure has
            // no null value — a [0..1] with no row and a NULL cell are
            // both the EMPTY collection, and equal([],[]) holds across
            // multiplicities, so scalar-empty must byte-match
            // collection-empty ('[]' == '[]'), never null-vs-'[]'.
            if (rows.isEmpty() || rows.get(0)[idx] == null) {
                return new Framed("[]", null);
            }
            return new Framed(rows.get(0)[idx], null);
        }
        if (rows.isEmpty()) {
            return new Framed("[]", null);
        }
        if (rows.size() == 1) {
            String t = rows.get(0)[idx];
            return t == null ? new Framed(null, "null-canon-cell")
                    : new Framed(t, null);
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < rows.size(); i++) {
            String t = rows.get(i)[idx];
            if (t == null) {
                return new Framed(null, "null-canon-cell");
            }
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(t);
        }
        return new Framed(sb.append(']').toString(), null);
    }

    /** True iff both sides are same-length all-finite-Double vectors
     * whose pairs each hold under the lattice's declared 2-ULP arm —
     * PureAsserts OWNS the tolerance, this only vectorizes it. */
    private static boolean withinDeclaredUlp(List<Object> eVals,
            List<Object> aVals) {
        if (eVals.isEmpty() || eVals.size() != aVals.size()) {
            return false;
        }
        for (int i = 0; i < eVals.size(); i++) {
            if (!(eVals.get(i) instanceof Double de
                    && aVals.get(i) instanceof Double da
                    && Double.isFinite(de) && Double.isFinite(da)
                    && PureAsserts.equalScalar(de, da))) {
                return false;
            }
        }
        return true;
    }

    /** The RUNTIME numeric kind of a side's fetched values (uniform, or
     * null when empty/unknowable — the mixed case gated earlier). */
    private static @com.legend.Nullable String runtimeNumericKind(
            List<Object> vals) {
        String kind = null;
        for (Object v : vals) {
            String k = v instanceof java.math.BigDecimal ? "decimal"
                    : (v instanceof Double || v instanceof Float) ? "float"
                    : (v instanceof Long || v instanceof Integer
                            || v instanceof Short || v instanceof Byte
                            || v instanceof java.math.BigInteger) ? "integer"
                    : null;
            if (k == null) {
                return null;
            }
            kind = k;
        }
        return kind;
    }

    private static boolean mixedNumericKinds(List<Object> vals) {
        boolean integral = false;
        boolean floating = false;
        for (Object v : vals) {
            if (v instanceof Long || v instanceof Integer
                    || v instanceof Short || v instanceof Byte
                    || v instanceof java.math.BigInteger) {
                integral = true;
            } else if (v instanceof Double || v instanceof Float) {
                floating = true;
            }
        }
        return integral && floating;
    }

    private static @com.legend.Nullable String fineNumericKind(
            com.legend.compiler.element.type.Type t) {
        if (t == com.legend.compiler.element.type.Type.Primitive.INTEGER) {
            return "integer";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.FLOAT) {
            return "float";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.DECIMAL
                || t instanceof com.legend.compiler.element.type.Type.PrecisionDecimal) {
            return "decimal";
        }
        return null;   // an unrefined Number — decline, never guess
    }

    private static String typeName(TypedSpec spec) {
        var t = spec.info().type();
        return t.getClass().getSimpleName() + ":" + t;
    }

    /** Pure's equality kind classes over STAMPS (spec §3: the numeric
     * tower is ONE class; everything else compares within its kind). */
    private static @com.legend.Nullable String kindClassOf(
            com.legend.compiler.element.type.Type t) {
        if (t == com.legend.compiler.element.type.Type.Primitive.INTEGER
                || t == com.legend.compiler.element.type.Type.Primitive.FLOAT
                || t == com.legend.compiler.element.type.Type.Primitive.DECIMAL
                // NUMBER is the numeric tower's supertype — the concrete
                // render refines from the PLAN's SQL type (V6 burn)
                || t == com.legend.compiler.element.type.Type.Primitive.NUMBER
                || t instanceof com.legend.compiler.element.type.Type.PrecisionDecimal) {
            return "numeric";
        }
        if (t instanceof com.legend.compiler.element.type.Type.EnumType et) {
            // per-ENUMERATION kind class: values of different enums are
            // never equal in pure, and an enum never equals its name
            // string — the fqn IS the kind
            return "enum:" + et.fqn();
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.STRING) {
            return "string";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.BOOLEAN) {
            return "boolean";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.STRICT_DATE
                || t == com.legend.compiler.element.type.Type.Primitive.DATE_TIME
                || t == com.legend.compiler.element.type.Type.Primitive.DATE) {
            return "temporal";
        }
        String fqn = com.legend.compiler.element.EqualityKeys.fqnOf(t);
        if (fqn != null) {
            // X5: instance equality is per-CLASS (EqualityUtilities —
            // the classifiers must match exactly, so the fqn IS the
            // kind; a parameterized GenericType names the same
            // classifier); keyed-ness adjudicates at the wrap (a
            // keyless class declines with its own reason).
            return "instance:" + fqn;
        }
        return null;
    }

    /** X5 — the pair's shared key tree: non-null iff BOTH stamps are
     * the SAME keyed class (the engine's classifier-match precondition
     * plus resolvable {@code <<equality.Key>>} identity). */
    private static com.legend.compiler.element.@com.legend.Nullable EqualityKeys
            instanceKeys(TypedSpec eSpec, TypedSpec aSpec,
                    StatementExecutor.ExecEnv env, List<Object> eVals, List<Object> aVals) {
        String ef = com.legend.compiler.element.EqualityKeys.fqnOf(
                eSpec.info().type());
        String af = com.legend.compiler.element.EqualityKeys.fqnOf(
                aSpec.info().type());
        // a side DECLARED wider (a program's Node[1] return) whose wire
        // values all carry the other side's class as their __type IS that
        // class at runtime — the engine's classifier match holds on the
        // evidence, not on the declaration (batch 54: assertConversion's
        // `let actual = convertElement(…)` judged against ^StringLiteral(…))
        if (ef != null && af != null && !ef.equals(af)) {
            if (env.ctx().isSubtype(ef, af) && allWireType(aVals, ef)) {
                af = ef;
                aSpec = eSpec;
            } else if (env.ctx().isSubtype(af, ef) && allWireType(eVals, af)) {
                ef = af;
                eSpec = aSpec;
            }
        }
        if (ef != null && ef.equals(af)) {
            // substitution-aware: the E-side stamp's instantiation
            // (key NAMES are instantiation-independent; nesting follows
            // the arguments — Pair-of-Pairs)
            return com.legend.compiler.element.EqualityKeys.resolve(
                    env.ctx(), eSpec.info().type());
        }
        return null;
    }

    /** A class-kind side's values with JSON object text decoded to structures. */
    private static List<Object> structuredSide(TypedSpec spec, List<Object> vals) {
        if (com.legend.compiler.element.EqualityKeys.fqnOf(spec.info().type()) == null) {
            return vals;
        }
        List<Object> out = new ArrayList<>(vals.size());
        for (Object v : vals) {
            out.add(com.legend.exec.Executor.structured(v));
        }
        return out;
    }

    /** Declared classes only (no wire evidence in hand). */
    private static com.legend.compiler.element.@com.legend.Nullable EqualityKeys
            instanceKeys(TypedSpec eSpec, TypedSpec aSpec, StatementExecutor.ExecEnv env) {
        return instanceKeys(eSpec, aSpec, env, List.of(), List.of());
    }

    /** Every wire value is an instance stamped {@code __type} = {@code cls}. */
    private static boolean allWireType(List<Object> vals, String cls) {
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
    private static List<Object> restrictToKeys(List<Object> vals,
            com.legend.compiler.element.EqualityKeys keys, ModelContext ctx) {
        List<Object> out = new ArrayList<>(vals.size());
        for (Object v : vals) {
            out.add(restrictOne(v, keys, ctx));
        }
        return out;
    }

    private static Object restrictOne(Object v,
            com.legend.compiler.element.EqualityKeys keys, ModelContext ctx) {
        if (!(v instanceof java.util.Map<?, ?> m)) {
            return v;
        }
        var out = new java.util.LinkedHashMap<String, Object>();
        for (var k : keys.keys()) {
            Object val = m.get(k.name());
            if (val != null) {
                val = val instanceof List<?> l
                        ? l.stream().map(x -> restrictNested(x, k.nested(), ctx))
                                .toList()
                        : restrictNested(val, k.nested(), ctx);
            }
            out.put(k.name(), val);
        }
        return out;
    }

    /** A value inside a key slot: restricted to the keys of the slot's
     * DECLARED class when that class is keyed, else to the keys of the
     * value's OWN class — the wire carries it as {@code __type}
     * (WORLD_MAP §4: a polymorphic slot's elements are judged by their
     * own classifier, the engine's rule). A keyless class stays the
     * whole map (identity). */
    private static Object restrictNested(Object v,
            com.legend.compiler.element.@com.legend.Nullable EqualityKeys declared,
            ModelContext ctx) {
        if (declared != null) {
            return restrictOne(v, declared, ctx);
        }
        if (v instanceof java.util.Map<?, ?> m
                && m.get(com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE)
                        instanceof String own) {
            var keys = com.legend.compiler.element.EqualityKeys.resolve(ctx, own);
            if (keys != null) {
                return restrictOne(v, keys, ctx);
            }
        }
        return v;
    }

    /** One assert side under V11: the values (host referee, gates,
     * declared policies) and the canon rider (byte verdict texts) —
     * both produced by the SAME single execution. §8 leg 1: a TABULAR
     * side keeps its grid ({@code grid} non-null; {@code values} are
     * the row-major CELLS, NULL slots kept — engine TDSRow semantics,
     * column names OUT) and its canon is the rider's per-ROW texts. */
    private record SideFetch(List<Object> values,
            com.legend.exec.CanonRider rider,
            ExecutionResult.@com.legend.Nullable Tabular grid) {
    }

    private static SideFetch sideCanon(TypedSpec arg,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env, boolean canonicalOrder,
            @com.legend.Nullable SpliceHook hook) {
        var rider = new com.legend.exec.CanonRider(canonicalOrder);
        ExecutionResult r = StatementExecutor.evalValue(arg, letPrefix,
                specs, env, rider, false, hook);
        if (r instanceof ExecutionResult.Tabular t) {
            List<Object> cells = cells(t);
            com.legend.exec.CanonicalDivergence.v7SideRows(cells.size());
            return new SideFetch(cells, rider, t);
        }
        return new SideFetch(decodeSide(r), rider, null);
    }

    private static List<Object> side(TypedSpec arg, List<TypedSpec> letPrefix,
            SpecCompiler specs, StatementExecutor.ExecEnv env,
            @com.legend.Nullable SpliceHook hook) {
        return decodeSide(StatementExecutor.evalValue(arg, letPrefix,
                specs, env, null, false, hook));
    }

    /** A JSON assert side's ONE document text: a GRAPH result's JSON
     * IS the {@code String[1]} (a serialize execute's {@code .values}
     * — leg 2 made its stamp String[1], and the DB-built envelope is
     * the value); any other result must decode to one string. Null =
     * not this shape (generic path, loud downstream). */
    private static @com.legend.Nullable String jsonSideText(TypedSpec arg,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env,
            @com.legend.Nullable SpliceHook hook) {
        ExecutionResult r = StatementExecutor.evalValue(arg, letPrefix,
                specs, env, null, false, hook);
        if (r instanceof ExecutionResult.Graph g) {
            return g.json();
        }
        List<Object> v = decodeSide(r);
        return v.size() == 1 && v.get(0) instanceof String s ? s : null;
    }

    /** F13c — a side on the IDENTITY LANE without a canon rider: the
     * assert-condition/predicate evaluator (in-SQL eq/equal needs
     * instance identity; the boolean egress keeps every other lane
     * blind to the field). */
    private static List<Object> identitySide(TypedSpec arg,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env, @com.legend.Nullable SpliceHook hook) {
        return decodeSide(StatementExecutor.evalValue(arg, letPrefix,
                specs, env, null, true, hook));
    }

    private static List<Object> decodeSide(
            @com.legend.Nullable ExecutionResult r) {
        List<Object> side = decodeSideValues(r);
        // V7 §5-1 instrument: the side-size histogram (VALUES-literal
        // cost bracket for V12's fused-verdict design) — measurement
        // only, fed from the one place every side already flows through
        com.legend.exec.CanonicalDivergence.v7SideRows(side.size());
        return side;
    }

    private static List<Object> decodeSideValues(
            @com.legend.Nullable ExecutionResult r) {
        return switch (r) {
            case null -> new ArrayList<>();
            case ExecutionResult.Scalar s -> {
                List<Object> out = new ArrayList<>(1);
                if (s.value() instanceof java.sql.Array arr) {
                    // the LIST WIRE arriving as one JDBC array cell —
                    // the collection IS the side, flattened
                    try {
                        for (Object el : (Object[]) arr.getArray()) {
                            // ONE-CARRIER normalization at this raw JDBC
                            // read: the wire temporal is PureDateLiteral
                            // (D-arc) — driver temporals convert in one
                            // hop, same as the Executor seam
                            out.add(switch (el) {
                                case java.sql.Timestamp ts ->
                                        com.legend.values.PureDateLiteral
                                                .fromLocalDateTime(ts.toLocalDateTime());
                                case java.sql.Date sd ->
                                        com.legend.values.PureDateLiteral
                                                .fromLocalDate(sd.toLocalDate());
                                case null, default -> el;
                            });
                        }
                    } catch (java.sql.SQLException ex) {
                        throw new IllegalStateException(
                                "array side unwrap failed", ex);
                    }
                } else if (s.value() != null) {
                    out.add(s.value());
                }
                yield out;
            }
            case ExecutionResult.Collection c -> c.values();
            // D4: a GRAPH side's values are the DATABASE-built JSON
            // array's elements (the harness Eval convention moved to
            // the owner) — parsed structures; the lattice/JsonCompare
            // judge them, never raw json text
            case ExecutionResult.Graph g -> {
                Object p = com.legend.sql.Json.parse(g.json());
                yield p instanceof List<?> l ? new ArrayList<Object>(l)
                        : new ArrayList<>(List.of(p));
            }
            default -> throw new com.legend.error.NotImplementedException(
                    "assert verdict over a " + r.getClass().getSimpleName()
                    + " side — grid asserts stay with their own"
                    + " compare owners");
        };
    }

    private static Object one(List<Object> side, String what) {
        if (side.size() != 1) {
            throw new IllegalStateException(what + " must be one value,"
                    + " got " + side.size());
        }
        return side.get(0);
    }

    /** The literal type argument's name: @Type annotation
     * ({@code TypedTypeRef}) or a bare reference in value position
     * ({@code TypedPackageableRef}); null = not literal (fall through,
     * the body inlines and walls on its own terms). */
    private static @com.legend.Nullable String typeRefName(TypedSpec t) {
        return switch (t) {
            case com.legend.compiler.spec.typed.TypedTypeRef tr ->
                    tr.target().typeName();
            case com.legend.compiler.spec.typed.TypedPackageableRef pr ->
                    pr.fullPath();
            default -> null;
        };
    }

    /** The relation arg executed in the database, as its TABULAR frame;
     * null = the value did not execute to a relation (fall through). */
    private static ExecutionResult.@com.legend.Nullable Tabular tabular(
            TypedSpec arg, List<TypedSpec> letPrefix, SpecCompiler specs,
            StatementExecutor.ExecEnv env, @com.legend.Nullable SpliceHook hook) {
        ExecutionResult r = StatementExecutor.evalValue(arg,
                letPrefix, specs, env, null, false, hook);
        return r instanceof ExecutionResult.Tabular t ? t : null;
    }

    /** Row-major cell stream of a tabular frame (the cell-zip input). */
    /** Raw cell flatten — cells arrive ALREADY DECODED (the engine's
     * value-read conventions ride the FETCH: wrapTdsCanon conforms the
     * plan in SQL and the executor's label-driven unwrap delivers; the
     * former host-twin decode, valueRead, is deleted — Java-eviction
     * close of the disagree-9 burn). */
    private static List<Object> cells(ExecutionResult.Tabular t) {
        List<Object> out = new java.util.ArrayList<>();
        for (com.legend.exec.Row r : t.rows()) {
            out.addAll(r.values());
        }
        return out;
    }

    /** Failure-message sketch of a frame (columns + row count — the
     * spec's toString(true) grid rendering is message-position only;
     * no witness pins its spelling). */
    private static String summarize(ExecutionResult.Tabular t) {
        return t.columns().stream().map(com.legend.exec.Column::name)
                .toList() + " (" + t.rows().size() + " rows)";
    }

    private static ExecutionResult ok() {
        return new ExecutionResult.Scalar(Boolean.TRUE,
                com.legend.compiler.element.type.Type.Primitive.BOOLEAN);
    }

    private static ExecutionResult fail(String message) {
        // the seam: verdicts speak the platform vocabulary
        throw new com.legend.error.AssertFailed(message);
    }
}
