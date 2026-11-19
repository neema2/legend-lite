// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.NotImplementedException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE ENGINE REWRITE's mechanism ({@link PureMeaningRewrite}; the reviewer's D-M1): one walk decides what is flagged
 * and what is rewritten, by identity, within the store-bound roots alone. A recording substitute stands in for the
 * core's native (a fresh copy of each flagged call, so the test sees exactly which nodes were replaced); the native's
 * own construction and its planted wrong callees are {@code SubstringAsGivenCall}'s tests. The models are
 * {@link PureMeaningCheckTest}'s.
 */
class PureMeaningRewriteTest {

    private static final String TABLE = PureMeaningCheckTest.TABLE;

    /** Records each substitute it builds, and the code it was asked for. */
    private static final class Recording implements PureMeaningRewrite.Substitute {
        final Set<TypedSpec> built = Collections.newSetFromMap(new IdentityHashMap<>());
        final List<PureMeaningCheck.Code> codes = new ArrayList<>();

        @Override
        public TypedNativeCall of(PureMeaningCheck.Code code, TypedNativeCall flagged) {
            TypedNativeCall copy = new TypedNativeCall(flagged.callee(), flagged.args(), flagged.info(), flagged.pos());
            built.add(copy);
            codes.add(code);
            return copy;
        }

        /** How many of the substitutes it built are in {@code n}. */
        int in(TypedSpec n) {
            int here = built.contains(n) ? 1 : 0;
            for (TypedSpec c : n.children()) {
                here += in(c);
            }
            return here;
        }
    }

    private static PureMeaningRewrite.Rewritten rewrite(String model, String query, Recording r) {
        ModelContext c = Compiler.compileModel(model);
        TypedQuery q = Compiler.query(c, query);
        return PureMeaningRewrite.rewrite(q, c, "t::RT", null, r);
    }

    private static PureMeaningRewrite.Rewritten rewrite(String query, Recording r) {
        return rewrite(PureMeaningCheckTest.MODEL, query, r);
    }

    @Test
    void eachFlaggedCallIsRewritten_andTheRewrittenBodyPlansAsBefore() {
        Recording r = new Recording();
        PureMeaningRewrite.Rewritten w = rewrite(PureMeaningCheckTest.PLANTED, r);
        assertEquals(List.of(PureMeaningCheck.Code.SUBSTRING_3, PureMeaningCheck.Code.INDEX_OF_2), r.codes);
        assertEquals(2, r.in(w.query().body().get(0)));
        assertEquals("substring/3=1, indexOf/2=1", w.rewrote().header());
        // the substitutes here are copies of the calls, so the plan is the query's own: inlining a root on its own and
        // planning withBody changes nothing else
        ModelContext c = Compiler.compileModel(PureMeaningCheckTest.MODEL);
        assertEquals(Compiler.query(c, PureMeaningCheckTest.PLANTED).plan("t::RT", PureText.PURE_V1).sql(),
                w.query().plan("t::RT", PureText.PURE_V1).sql());
    }

    @Test
    void aCleanQueryIsReturnedAsItIs() {
        Recording r = new Recording();
        ModelContext c = Compiler.compileModel(PureMeaningCheckTest.MODEL);
        TypedQuery q = Compiler.query(c, "|" + TABLE + "->select(~[NAME])->from(t::RT)");
        assertSame(q, PureMeaningRewrite.rewrite(q, c, "t::RT", null, r).query());
        assertEquals(0, r.built.size());
    }

    @Test
    void aHostSideLetIsNotRewritten() {
        Recording r = new Recording();
        PureMeaningRewrite.Rewritten w = rewrite("{|let x = 'abcdef'->substring(1, 3); " + TABLE
                + "->filter(r|$r.NAME == $x)->from(t::RT);}", r);
        assertEquals(0, r.built.size());
        assertTrue(w.rewrote().clean());
    }

    /** A host-side let's own call stays as written beside a store-side call that is rewritten: the rewrite replaces
     *  the nodes the walk flagged, by identity, within the roots it walked. */
    @Test
    void aHostSideLetsCallStaysAsWritten_besideAStoreSideCallRewritten() {
        Recording r = new Recording();
        ModelContext c = Compiler.compileModel(PureMeaningCheckTest.MODEL);
        TypedQuery q = Compiler.query(c, "{|let x = 'abcdef'->substring(1, 3); " + TABLE
                + "->filter(r|$r.NAME == $x)->extend(~a: r|$r.NAME->toOne()->indexOf('o'))->from(t::RT);}");
        PureMeaningRewrite.Rewritten w = PureMeaningRewrite.rewrite(q, c, "t::RT", null, r);
        assertEquals(List.of(PureMeaningCheck.Code.INDEX_OF_2), r.codes);
        assertSame(q.body().get(0), w.query().body().get(0));
        assertEquals(1, r.in(w.query().body().get(1)));
    }

    /** The reviewer's D-M1 plant: a user function called both host-side and store-side is rewritten at the
     *  store-side use alone. The host-side let is kept as written (its call is the planner's to inline later, with
     *  Pure's meaning, as legend-engine computes it on the host). */
    @Test
    void aFunctionCalledHostSideAndStoreSideIsRewrittenAtTheStoreSideUseAlone() {
        Recording r = new Recording();
        String query = "{|let x = t::firstThree('abcdef'); " + TABLE
                + "->filter(r|$r.NAME == $x)->extend(~a: r|t::firstThree($r.NAME->toOne()))->from(t::RT);}";
        ModelContext c = Compiler.compileModel(PureMeaningCheckTest.MODEL);
        TypedQuery q = Compiler.query(c, query);
        PureMeaningRewrite.Rewritten w = PureMeaningRewrite.rewrite(q, c, "t::RT", null, r);
        assertEquals(List.of(PureMeaningCheck.Code.SUBSTRING_3), r.codes);
        assertSame(q.body().get(0), w.query().body().get(0), "the host-side let is kept as written");
        assertEquals(1, r.in(w.query().body().get(1)));
    }

    @Test
    void aQualifiedPropertyIsRewritten() {
        Recording r = new Recording();
        PureMeaningRewrite.Rewritten w = rewrite("|t::Person.all()->project(~[i: p|$p.initials()])"
                + "->from(t::M, t::RT)", r);
        assertEquals(List.of(PureMeaningCheck.Code.SUBSTRING_3), r.codes);
        assertEquals(1, r.in(w.query().body().get(0)));
    }

    /** A let-bound lambda the query evaluates is inlined into the statement that evaluates it (the planner's own
     *  inliner beta-reduces the call), so its call is rewritten there, where legend-engine sends it to the database
     *  too. */
    @Test
    void aLetBoundLambdaTheQueryEvaluatesIsRewrittenWhereItIsEvaluated() {
        Recording r = new Recording();
        PureMeaningRewrite.Rewritten w = rewrite("{|let f = {s: String[1]|$s->indexOf('x')}; " + TABLE
                + "->extend(~i: r|$f->eval($r.NAME->toOne()))->from(t::RT);}", r);
        assertEquals(List.of(PureMeaningCheck.Code.INDEX_OF_2), r.codes);
        assertEquals(1, r.in(w.query().body().get(1)));
    }

    /** A let that reads a store is a root of its own: its value is inlined and rewritten inside the let, which is kept
     *  for the planner to reduce. A rewrite that skipped it would replace one of the two calls flagged, and the count
     *  check refuses that rather than answer half rewritten. */
    @Test
    void aStoreBoundLetIsRewrittenInsideTheLet_andEveryRootIsCounted() {
        Recording r = new Recording();
        PureMeaningRewrite.Rewritten w = rewrite("{|let t = " + TABLE + "->extend(~a: r|$r.NAME->toOne()->substring(1, 3));"
                + " $t->extend(~b: r|$r.NAME->toOne()->indexOf('o'))->from(t::RT);}", r);
        assertEquals(List.of(PureMeaningCheck.Code.SUBSTRING_3, PureMeaningCheck.Code.INDEX_OF_2), r.codes);
        assertTrue(w.query().body().get(0) instanceof com.legend.compiler.spec.typed.TypedLet);
        assertEquals(1, r.in(w.query().body().get(0)));
        assertEquals(1, r.in(w.query().body().get(1)));
    }

    /** A let-bound lambda store-bound code uses, read again by a statement computed on the host: the planner's inliner
     *  reduces the store-bound use into its statement, so the rewrite replaces the call there, and both lets stay as
     *  written. The host-side read keeps Pure's meaning, as legend-engine computes it (the reviewer's E-S1). */
    private static final String SHARED_LET = "{|let f = {s: String[1]|$s->indexOf('x')}; let n = $f->eval('abcx'); "
            + TABLE + "->extend(~i: r|$f->eval($r.NAME->toOne()))->from(t::RT);}";

    @Test
    void aLetReadByTheStoreAndByTheHostIsRewrittenInTheStoresUseAlone() {
        Recording r = new Recording();
        ModelContext c = Compiler.compileModel(PureMeaningCheckTest.MODEL);
        TypedQuery q = Compiler.query(c, SHARED_LET);
        PureMeaningRewrite.Rewritten w = PureMeaningRewrite.rewrite(q, c, "t::RT", null, r);
        assertEquals(List.of(PureMeaningCheck.Code.INDEX_OF_2), r.codes);
        assertEquals(0, r.in(w.query().body().get(0)), "the let is not rewritten in place");
        assertSame(q.body().get(1), w.query().body().get(1), "the host-side read is kept as written");
        assertEquals(1, r.in(w.query().body().get(2)));
    }

    /** A value let whose own call is rewritten, read by a statement of its own: computed once, its call in its own SQL,
     *  as legend-engine writes it (asked of legend-engine 4.145.0: `let d = ...->toOne().desk->substring(1, 3);
     *  $d->toUpper()` plans `select substring(desk, 1, 3)` and answers "CRE"), so the later read is no reason to refuse
     *  (the reviewer's R2-S2). */
    @Test
    void aValueLetRewrittenAndReadByAStatementOfItsOwnIsNotRefused() {
        Recording r = new Recording();
        ModelContext c = Compiler.compileModel(PureMeaningCheckTest.MODEL);
        TypedQuery q = Compiler.query(c, "{|let d = t::Person.all()->filter(p|$p.name == 'abcdef')->from(t::M, t::RT)"
                + "->toOne().name->substring(1, 3); $d->toUpper();}");
        PureMeaningRewrite.Rewritten w = PureMeaningRewrite.rewrite(q, c, "t::RT", null, r);
        assertEquals(List.of(PureMeaningCheck.Code.SUBSTRING_3), r.codes);
        assertEquals(1, r.in(w.query().body().get(0)));
        assertSame(q.body().get(1), w.query().body().get(1));
    }

    /** Where a let that holds a flagged call is a root and a statement the rewrite does not touch reads it, the
     *  rewrite refuses by name rather than give the host-side read the database's meaning (the reviewer's E-S1). The
     *  typer substitutes a let-bound lambda into each use, and the detector walks a let only when its value is a lambda
     *  store-bound code names, so no query is known to reach this today: the walk's answer is built here as it would
     *  be, the let a root holding its call, and the host-side read beside it. */
    @Test
    void aRewrittenLetAlsoReadOnTheHostIsRefused() {
        ModelContext c = Compiler.compileModel(PureMeaningCheckTest.MODEL);
        TypedQuery q = Compiler.query(c, SHARED_LET);
        TypedSpec let = q.body().get(0);
        TypedNativeCall call = firstNative(let);
        assertTrue(call != null, let.toString());
        PureMeaningCheck.Result walked = new PureMeaningCheck.Result(List.of(new PureMeaningCheck.Site(
                PureMeaningCheck.Code.INDEX_OF_2, "the query", null, call)), List.of(let, q.body().get(2)));
        // a host-side statement that reads f by name (the typer substitutes a lambda let into its uses, so the reader
        // is taken from a query whose f stays a variable)
        TypedSpec reader = Compiler.query(c, "{|let f = 'abcx'; let n = $f->length(); " + TABLE
                + "->filter(r|$r.ID == $n)->from(t::RT);}").body().get(1);
        NotImplementedException e = assertThrows(NotImplementedException.class,
                () -> PureMeaningRewrite.refuseSharedLetRoots(List.of(let, reader, q.body().get(2)), walked));
        assertTrue(e.getMessage().contains("the let 'f'"), e.getMessage());
        // its partner: the same let, read by no statement left on the host, is not refused
        PureMeaningRewrite.refuseSharedLetRoots(q.body(), walked);
    }

    private static TypedNativeCall firstNative(TypedSpec n) {
        if (n instanceof TypedNativeCall nc) {
            return nc;
        }
        for (TypedSpec c : n.children()) {
            TypedNativeCall found = firstNative(c);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    @Test
    void theCorrectedOptionKeepsSubstringPures_andIndexOfIsRewritten() {
        Recording r = new Recording();
        rewrite("|" + TABLE + "->extend(~[a: r|$r.NAME->toOne()->substring(1, 3), b: r|$r.NAME->toOne()->indexOf('o')])"
                + "->from(t::RT)->meta::pure::executionPlan::featureFlag::withFeatureFlags("
                + "meta::pure::executionPlan::features::Feature.CORRECT_SQL_SUBSTRING_INDEXING)", r);
        assertEquals(List.of(PureMeaningCheck.Code.INDEX_OF_2), r.codes);
    }

    // ---- refused by name -------------------------------------------------------------------------------------------

    @Test
    void anM2MChainsTransformsAreRefused_theStoreResolverReachesThem() {
        NotImplementedException e = assertThrows(NotImplementedException.class, () -> rewrite("|t::Out.all()"
                + "->project(~[s: o|$o.short])->meta::pure::mapping::withChainedMappings([t::M])->from(t::M2, t::RT)",
                new Recording()));
        assertTrue(e.getMessage().contains("cannot be rewritten at this door"), e.getMessage());
    }

    @Test
    void anUnverifiedCallIsRefused() {
        NotImplementedException e = assertThrows(NotImplementedException.class,
                () -> rewrite(PureMeaningCheckTest.UNFOLLOWABLE, "|" + TABLE
                        + "->extend(~i: r|t::untyped($r.NAME->toOne()))->from(t::RT)", new Recording()));
        assertTrue(e.getMessage().contains("unverified"), e.getMessage());
    }

    @Test
    void aDatabaseNotMeasuredUnderEngineIsRefused() {
        String unmeasured = PureMeaningCheckTest.MODEL.replace("type: H2; specification: LocalH2 { }; auth: DefaultH2;",
                "type: DuckDB; specification: DuckDB { }; auth: Test;");
        Recording r = new Recording();
        NotImplementedException e = assertThrows(NotImplementedException.class,
                () -> rewrite(unmeasured, PureMeaningCheckTest.PLANTED, r));
        assertTrue(e.getMessage().contains("not measured on DuckDB"), e.getMessage());
        assertEquals(0, r.built.size());
    }
}
