// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedAggCol;
import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedDrop;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedGroupBy;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedMilestonedAccess;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSlice;
import com.legend.compiler.spec.typed.TypedSortBy;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
/**
 * SYNTHETIC HEAD identities — filtered navigations lift to
 * {@code head#fN} chains (predicate parked for the join target),
 * two-dates-per-head splits mint {@code head#dN} (a separate join
 * identity per distinct date-set), and {@link #realHead} keeps every
 * model lookup transparent. Append-only across nested resolutions —
 * names are counter-unique; the registry is the ONE owner of the
 * '#'-suffix convention — {@link JoinIdentity} is the value type, the
 * string form exists only because heads travel as property names.
 */
final class SyntheticHeads {

    /**
     * A join identity parsed from a head name. IDENTIFIER property names
     * cannot contain {@code '#'}; QUOTED property names (M3
     * {@code propertyName: (identifier | STRING)}) can — minting over one
     * throws loudly (the constructor guard), and a malformed suffix is a
     * loud resolver bug. RESIDUAL (documented, corpus-free): a quoted
     * property spelled exactly like a minted name ({@code 'emp#f0'})
     * decodes as synthetic — full closure needs registry-membership
     * decode. ALL encode/decode knowledge of the {@code #fN}/{@code #dN}/
     * {@code #cN} convention lives in this record.
     */
    record JoinIdentity(String prop, Kind kind, int seq) {
        enum Kind { PLAIN, FILTERED, DATED, CONCAT }

        JoinIdentity {
            if (prop.indexOf('#') >= 0) {
                // QUOTED pure property names are arbitrary strings (M3:
                // propertyName: (identifier | STRING)) — a real property
                // containing '#' must never silently masquerade as one of
                // our synthetic identities, and composed synthetics
                // (prop#cN#dM) are a resolver bug either way. LOUD.
                throw new IllegalStateException(
                        "synthetic-head identity over a property containing"
                                + " '#' (quoted-name property or composed"
                                + " synthetic — resolver bug): " + prop);
            }
        }

        static JoinIdentity of(String head) {
            int i = head.indexOf('#');
            if (i < 0) {
                return new JoinIdentity(head, Kind.PLAIN, -1);
            }
            char k = head.charAt(i + 1);
            Kind kind = switch (k) {
                case 'f' -> Kind.FILTERED;
                case 'd' -> Kind.DATED;
                case 'c' -> Kind.CONCAT;
                default -> throw new IllegalStateException(
                        "malformed synthetic head (resolver bug): " + head);
            };
            int seq;
            try {
                seq = Integer.parseInt(head.substring(i + 2));
            } catch (NumberFormatException e) {
                throw new IllegalStateException(
                        "malformed synthetic head (resolver bug): " + head);
            }
            return new JoinIdentity(head.substring(0, i), kind, seq);
        }

        String encoded() {
            return switch (kind) {
                case PLAIN -> prop;
                case FILTERED -> prop + "#f" + seq;
                case DATED -> prop + "#d" + seq;
                case CONCAT -> prop + "#c" + seq;
            };
        }
    }

    /** The head names a filter-lifted chain ({@code #fN}). */
    static boolean isFiltered(String head) {
        return JoinIdentity.of(head).kind() == JoinIdentity.Kind.FILTERED;
    }

    TypedLambda pred(String head) {
        return preds.get(head);
    }

    boolean hasPred(String head) {
        return preds.containsKey(head) || branchPreds.containsKey(head)
                || corrPreds.containsKey(head);
    }

    /** The CORRELATED predicate parked on {@code head}, or null. */
    TypedLambda correlatedPred(String head) {
        return corrPreds.get(head);
    }

    /** ALL parked correlated predicates — the demand scan reads their
     * OUTER-variable paths as PARENT demand (#69: the lift moved the
     * only occurrence of the read out of the projection column). */
    java.util.Collection<TypedLambda> allCorrelatedPreds() {
        return corrPreds.values();
    }

    /** ALL predicates parked on a head: singleton for a {@code #fN} head,
     * the non-null branch predicates for a {@code #cN} head, empty
     * otherwise. Demand/tail scans iterate this — every branch's reads
     * pull the target's slots exactly like a single lifted predicate. */
    List<TypedLambda> allPreds(String head) {
        TypedLambda single = preds.get(head);
        if (single == null) {
            single = corrPreds.get(head);
        }
        if (single != null) {
            return List.of(single);
        }
        List<TypedLambda> branches = branchPreds.get(head);
        if (branches != null) {
            return branches.stream().filter(java.util.Objects::nonNull).toList();
        }
        return List.of();
    }

    /**
     * Apply a head's parked filter material to its finished target
     * pipeline: a {@code #fN} head filters once; a {@code #cN} head maps
     * each branch (a null branch predicate = the unfiltered stream) and
     * UNION-ALLs the branch pipes (engine: concatenated navigation
     * streams join as one union subselect). PLAIN/DATED heads pass
     * through.
     */
    TypedSpec applyToPipe(String head, TypedSpec pipe,
            java.util.function.BiFunction<TypedSpec, TypedLambda, TypedSpec> filter) {
        TypedLambda single = preds.get(head);
        if (single != null) {
            return filter.apply(pipe, single);
        }
        List<TypedLambda> branches = branchPreds.get(head);
        if (branches == null) {
            return pipe;
        }
        TypedSpec out = null;
        for (TypedLambda b : branches) {
            TypedSpec member = b == null ? pipe : filter.apply(pipe, b);
            out = out == null ? member
                    : new com.legend.compiler.spec.typed.TypedConcatenate(
                            out, member, member.info());
        }
        return out;
    }

    /** A fresh date-fingerprinted identity for {@code prop}. */
    String mintDateName(String prop) {
        return new JoinIdentity(prop, JoinIdentity.Kind.DATED, count++).encoded();
    }

    /** A fresh filter-lifted identity for {@code prop}. */

    /** The synthetic identity for a (property, predicate) pair — REUSED
     * when an EQUAL predicate is already parked on the same real head
     * (engine merge-by-identity: employeesByCityOrManager('Hoboken','Bla')
     * twice plus its inline spelling share ONE subselect; per-call-site
     * minting over-fragments and cross-multiplies projection rows — the
     * Fork golden's 3 joins for 5 columns). Structural record equality;
     * alpha-variant spellings stay separate (safe over-fragmentation). */
    private String parkFiltered(String prop, TypedLambda pred) {
        boolean closed = predClosedOverParam(pred);
        java.util.Map<String, TypedLambda> pool = closed ? preds : corrPreds;
        TypedSpec canon = alphaCanonicalBody(pred);
        for (var e : pool.entrySet()) {
            if (realHead(e.getKey()).equals(prop)
                    && alphaCanonicalBody(e.getValue()).equals(canon)) {
                return e.getKey();
            }
        }
        String synth = mintFilteredName(prop);
        pool.put(synth, pred);
        return synth;
    }

    /** The predicate body with its binder renamed to a canonical name —
     * the inliner alpha-freshens per call site (e, e_1, e_2 under an
     * outer shadowing scope), which defeated plain record equality (the
     * OffsetExplosion probe: 5 subselects for 2 distinct preds). */
    private static TypedSpec alphaCanonicalBody(TypedLambda pred) {
        if (pred.parameters().size() != 1 || pred.body().size() != 1) {
            return pred;
        }
        String param = pred.parameters().get(0);
        var pInfo = pred.info().type() instanceof Type.FunctionType ft
                && ft.params().size() == 1
                ? new ExprType(ft.params().get(0).type(),
                        ft.params().get(0).multiplicity())
                : null;
        if (pInfo == null) {
            return pred;
        }
        return Substitution.inlineParam(pred.body().get(0), param,
                new com.legend.compiler.spec.typed.TypedVariable(
                        "_canon", pInfo));
    }

    private String mintFilteredName(String prop) {
        return new JoinIdentity(prop, JoinIdentity.Kind.FILTERED, count++).encoded();
    }

    /** A fresh concatenated-stream identity for {@code prop}. */
    private String mintConcatName(String prop) {
        return new JoinIdentity(prop, JoinIdentity.Kind.CONCAT, count++).encoded();
    }

    /** Scan entry: the lambda's BODY under its own parameter (never the lambda node). */
    /**
     * PRE-REWRITE: a filtered navigation consumed as a BARE COLLECTION —
     * {@code $o.head(%d)->filter(f).leaf} with non-scalar multiplicity —
     * lifts into a SYNTHETIC head {@code head#fN}: a plain 2-hop chain
     * whose association-join TARGET pipeline carries the substituted
     * predicate (engine parity: the chain filter parks INSIDE the
     * navigation's join-tree node; the LEFT join row-explodes and
     * delivers NULL — TDSNull — on no surviving match). Scalar
     * ({@code [0..1]}) bare reads stay with the correlated-scalar arm
     * ({@code filteredNavLeafRead}) — the split is exactly complementary.
     * The walk is BEST-EFFORT: unknown node kinds pass through unchanged,
     * so an unlifted shape keeps today's loud not-substitutable error —
     * never silent SQL.
     */
    TypedSpec liftFilteredHeads(TypedSpec n) {
        return liftFilteredHeads(n, true);
    }

    private TypedSpec liftFilteredHeads(TypedSpec n, boolean enabled) {
        // ->map(e|$e.leaf) over a (filtered) class navigation IS the
        // property-path spelling — normalize and take the lift arm (the
        // qualifier-inlined aggregate shape:
        // joinStrings(map(filter(head, pred), .leaf)); #69).
        if (enabled && n instanceof TypedMap tm
                && tm.mapper().parameters().size() == 1
                && tm.mapper().body().size() == 1
                && tm.mapper().body().get(0) instanceof TypedPropertyAccess mb
                && mb.source() instanceof com.legend.compiler.spec.typed
                        .TypedVariable mv
                && mv.name().equals(tm.mapper().parameters().get(0))
                && tm.source() instanceof TypedFilter
                && tm.source().info().type() instanceof Type.ClassType) {
            return liftFilteredHeads(new TypedPropertyAccess(
                    tm.source(), mb.property(), tm.info()), enabled);
        }
        if (enabled
                && n instanceof TypedPropertyAccess pa
                && filterBehindToOne(pa.source()) instanceof TypedFilter f
                && f.predicate().parameters().size() == 1
                && f.info().type()
                        instanceof Type.ClassType
                && isLiftableNav(f.source())
                // scalar ([0..1]) reads: DEPTH-1 heads stay with the
                // correlated-scalar arm (filteredNavLeafRead — the
                // complementary split); a DEEP head has no scalar arm and
                // lifts into the chained assoc-join route, whose LEFT
                // joins deliver NULL on no match (engine golden
                // testQualifierInLambdaDeep: the closed pred wraps the
                // chained hop's target subselect).
                && !(pa.info().multiplicity()
                        instanceof com.legend.compiler.element.type
                                .Multiplicity.Bounded b
                        && Integer.valueOf(1).equals(b.upper())
                        && directlyOnVar(f.source()))) {
            TypedSpec head = liftFilteredHeads(f.source(), true);
            TypedSpec renamed;
            String synth;
            // CLOSED predicates park on the target pipeline; a predicate
            // reading the OUTER row parks CORRELATED — applied at the join
            // CONDITION where both rows are in scope. EQUAL preds on the
            // same head REUSE one identity (parkFiltered).
            if (head instanceof com.legend.compiler.spec.typed
                    .TypedMilestonedAccess ma) {
                synth = parkFiltered(ma.property(), f.predicate());
                renamed = new TypedMilestonedAccess(
                        ma.source(), synth, ma.dates(), ma.sweep(), ma.info());
            } else {
                var hp = (TypedPropertyAccess) head;
                synth = parkFiltered(hp.property(), f.predicate());
                renamed = new TypedPropertyAccess(
                        hp.source(), synth, hp.info());
            }
            return new TypedPropertyAccess(
                    renamed, pa.property(), pa.info());
        }
        // COMPUTED-mapper aggregation source (#69) —
        // map(filter(nav), λe.<computed>) where the mapper body is NOT a
        // plain property read (derived-property β-inlines: concat(...)).
        // The filtered SOURCE lifts into a synthetic head exactly like the
        // leaf-read spelling; the mapper rides along and substitutes
        // through the target's bindings at the aggregation fold.
        if (enabled && n instanceof TypedMap tm2
                && tm2.mapper().parameters().size() == 1
                && tm2.source() instanceof TypedFilter f0
                && f0.predicate().parameters().size() == 1
                && f0.info().type() instanceof Type.ClassType
                && isLiftableNav(f0.source())
                && !(tm2.info().multiplicity()
                        instanceof com.legend.compiler.element.type
                                .Multiplicity.Bounded mb2
                        && Integer.valueOf(1).equals(mb2.upper()))) {
            TypedSpec head = liftFilteredHeads(f0.source(), true);
            TypedSpec renamed;
            String synth;
            if (head instanceof com.legend.compiler.spec.typed
                    .TypedMilestonedAccess ma) {
                synth = parkFiltered(ma.property(), f0.predicate());
                renamed = new TypedMilestonedAccess(
                        ma.source(), synth, ma.dates(), ma.sweep(), ma.info());
            } else {
                var hp = (TypedPropertyAccess) head;
                synth = parkFiltered(hp.property(), f0.predicate());
                renamed = new TypedPropertyAccess(
                        hp.source(), synth, hp.info());
            }
            return new TypedMap(renamed,
                    (TypedLambda) liftFilteredHeads(tm2.mapper(), enabled),
                    tm2.info());
        }
        // CONCATENATED navigation streams read as a bare collection —
        // $p.head->filter(f1).leaf spelled over concatenate(...): every
        // branch is a (possibly filtered) navigation of the SAME head
        // property; the union lifts into ONE synthetic head #cN whose
        // join target is the UNION ALL of the branch pipelines (engine:
        // one unionalias subselect, LEFT-joined, row-exploding).
        if (enabled && n instanceof TypedPropertyAccess pa2
                && pa2.source() instanceof TypedNativeCall cc
                && cc.callee().qualifiedName()
                        .equals("meta::pure::functions::collection::concatenate")
                && cc.info().type() instanceof Type.ClassType
                && !(pa2.info().multiplicity()
                        instanceof Multiplicity.Bounded b2
                        && Integer.valueOf(1).equals(b2.upper()))) {
            TypedSpec lifted = liftConcatStreams(cc, pa2);
            if (lifted != null) {
                return lifted;
            }
        }
        return switch (n) {
            case TypedProject p ->
                    new TypedProject(
                            liftFilteredHeads(p.source(), enabled),
                            p.columns().stream().map(c ->
                                    new TypedFuncCol(
                                            c.name(),
                                            (TypedLambda) liftFilteredHeads(c.fn(),
                                                    enabled && !valuesLambdas
                                                            .contains(c.fn()))))
                                    .toList(),
                            p.info());
            case TypedFilter f -> new TypedFilter(
                    liftFilteredHeads(f.source(), enabled),
                    (TypedLambda) liftFilteredHeads(f.predicate(), enabled),
                    f.info());
            case TypedSortBy sb -> new TypedSortBy(
                    liftFilteredHeads(sb.source(), enabled),
                    (TypedLambda) liftFilteredHeads(sb.key(), enabled),
                    sb.ascending(), sb.info());
            case TypedLimit l -> new TypedLimit(
                    liftFilteredHeads(l.source(), enabled), l.count(), l.info());
            case TypedDrop d -> new TypedDrop(
                    liftFilteredHeads(d.source(), enabled), d.count(), d.info());
            case TypedSlice sl -> new TypedSlice(
                    liftFilteredHeads(sl.source(), enabled),
                    sl.start(), sl.stop(), sl.info());
            case TypedFrom fr -> new TypedFrom(
                    liftFilteredHeads(fr.source(), enabled),
                    fr.mapping(), fr.runtime(), fr.info());
            case TypedLambda l -> new TypedLambda(l.parameters(),
                    l.body().stream().map(b -> liftFilteredHeads(b, enabled))
                            .toList(), l.info());
            case TypedNativeCall c ->
                    new TypedNativeCall(c.callee(),
                            c.args().stream().map(a -> liftFilteredHeads(a, enabled))
                                    .toList(), c.info());
            case TypedPropertyAccess pa ->
                    new TypedPropertyAccess(
                            liftFilteredHeads(pa.source(), enabled),
                            pa.property(), pa.info());
            case TypedMilestonedAccess ma ->
                    new TypedMilestonedAccess(
                            liftFilteredHeads(ma.source(), enabled), ma.property(),
                            ma.dates(), ma.sweep(), ma.info());
            // auto-map mapper bodies are VALUE flattenings (empties drop) —
            // the TDS lift stays off inside them; unlifted shapes keep
            // their loud error
            case TypedMap m ->
                    new TypedMap(
                            liftFilteredHeads(m.source(), enabled),
                            (TypedLambda) liftFilteredHeads(m.mapper(), false),
                            m.info());
            case TypedIf i ->
                    new TypedIf(
                            liftFilteredHeads(i.condition(), enabled),
                            liftFilteredHeads(i.thenBranch(), enabled),
                            i.elseBranch().map(e -> liftFilteredHeads(e, enabled)),
                            i.info());
            case TypedCollection c ->
                    new TypedCollection(
                            c.elements().stream().map(e ->
                                    liftFilteredHeads(e, enabled)).toList(),
                            c.info());
            case TypedCast c ->
                    new TypedCast(
                            liftFilteredHeads(c.source(), enabled),
                            c.target(), c.info());
            default -> n;
        };
    }

    /**
     * VALUES-position filtered navigation (map terminal): pure flattening
     * DROPS empties here, so the predicate parks in the OUTER where
     * (engine golden: plain LEFT JOIN + WHERE — non-matching parents
     * contribute nothing, never a NULL value). The head still lifts to a
     * synthetic chain for join identity, but WITHOUT the in-target
     * predicate; the predicate joins the chain as an injected
     * object-space filter whose reads inline through the synthetic head.
     */
    TypedMap liftValueMapFilter(
            TypedMap m) {
        TypedLambda mapper = m.mapper();
        if (mapper.body().size() != 1
                || !(mapper.body().get(0)
                        instanceof TypedPropertyAccess pa)
                || !(pa.source() instanceof TypedFilter f)
                || f.predicate().parameters().size() != 1
                || f.predicate().body().size() != 1
                || !(f.info().type()
                        instanceof Type.ClassType)
                || !isLiftableNav(f.source())
                || (pa.info().multiplicity()
                        instanceof com.legend.compiler.element.type
                                .Multiplicity.Bounded b
                        && Integer.valueOf(1).equals(b.upper()))) {
            return m;
        }
        TypedSpec renamed;
        String synth;
        if (f.source() instanceof com.legend.compiler.spec.typed
                .TypedMilestonedAccess ma) {
            synth = mintFilteredName(ma.property());
            renamed = new TypedMilestonedAccess(
                    ma.source(), synth, ma.dates(), ma.sweep(), ma.info());
        } else {
            var hp = (TypedPropertyAccess) f.source();
            synth = mintFilteredName(hp.property());
            renamed = new TypedPropertyAccess(
                    hp.source(), synth, hp.info());
        }
        TypedLambda mapper2 = new TypedLambda(mapper.parameters(),
                List.of(new com.legend.compiler.spec.typed
                        .TypedPropertyAccess(renamed, pa.property(), pa.info())),
                mapper.info());
        TypedSpec inlined = Substitution.inlineParam(f.predicate().body().get(0),
                f.predicate().parameters().get(0), renamed);
        var srcParam = ((Type.FunctionType)
                mapper.info().type()).params().get(0);
        TypedLambda filterLam = new TypedLambda(mapper.parameters(),
                List.of(inlined),
                new ExprType(
                        new Type.FunctionType(
                                List.of(srcParam),
                                new Type.Param(
                                        com.legend.compiler.element.type
                                                .Type.Primitive.BOOLEAN,
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ONE)),
                        Multiplicity.Bounded.ONE));
        valuesLambdas.add(mapper2);
        return new TypedMap(
                new TypedFilter(m.source(), filterLam, m.source().info()),
                mapper2, m.info());
    }

    /**
     * The concat-stream lift body: flatten nested binary concatenates,
     * require every branch to be a (filtered) navigation of ONE shared
     * head property bottoming at the same receiver shape, mint the
     * {@code #cN} identity and park the branch predicates in order
     * (null = unfiltered branch). Null when any branch refuses — the
     * caller falls through to the loud wall.
     */
    private TypedSpec liftConcatStreams(TypedNativeCall cc,
            TypedPropertyAccess leafRead) {
        List<TypedSpec> streams = new java.util.ArrayList<>();
        flattenConcat(cc, streams);
        String prop = null;
        TypedSpec headNode = null;
        List<TypedLambda> branches = new java.util.ArrayList<>(streams.size());
        for (TypedSpec s : streams) {
            TypedSpec nav;
            TypedLambda pred;
            // conform-by-emission wrappers are SQL-erased (Scalars toOne
            // policy): a derived property declared [1] over a filtered
            // stream arrives as toOne(filter(...)) — look through
            while (s instanceof TypedNativeCall w
                    && w.args().size() == 1
                    && w.callee().qualifiedName()
                            .equals("meta::pure::functions::multiplicity::toOne")) {
                s = w.args().get(0);
            }
            if (s instanceof TypedFilter f
                    && f.predicate().parameters().size() == 1
                    && f.info().type() instanceof Type.ClassType
                    && isLiftableNav(f.source())
                    && predClosedOverParam(f.predicate())) {
                nav = f.source();
                pred = f.predicate();
            } else if ((s instanceof TypedPropertyAccess
                    || s instanceof TypedMilestonedAccess)
                    && s.info().type() instanceof Type.ClassType
                    && isLiftableNav(s)) {
                nav = s;
                pred = null;
            } else {
                return null;
            }
            String p = nav instanceof TypedMilestonedAccess ma
                    ? ma.property() : ((TypedPropertyAccess) nav).property();
            if (prop == null) {
                prop = p;
                headNode = nav;
            } else if (!prop.equals(p) || !nav.equals(headNode)) {
                // ONE head means one WHOLE navigation node: the property
                // AND its receiver chain AND its milestoning dates (audit
                // 16: branch 2's $p.parent hop or a different business
                // date silently vanished into branch 1's head — wrong
                // rows). Cross-head/cross-date unions are their own rung;
                // the refusal keeps the loud not-substitutable wall.
                return null;
            }
            branches.add(pred);
        }
        if (prop == null || branches.size() < 2) {
            return null;
        }
        // ONE identity per distinct stream expression: the same
        // concatenated stream in two projection columns rides ONE join
        // (engine merge-by-identity — two-column Merge golden expects 7
        // rows, two joins gave 13). The HEAD NODE is part of the identity
        // (same property over different receivers/dates is a different
        // stream); its BOTTOM VARIABLE alpha-normalizes so per-column
        // lambda param names (p| vs t|) don't split one stream into two
        // joins.
        Map<String, String> rootEnv = new LinkedHashMap<>();
        rootEnv.put(bottomVarOf(headNode), "#root");
        List<Object> memoKey = List.of(prop,
                alphaNormalize(headNode, rootEnv, new int[]{0}),
                branches.stream().map(b -> b == null ? ""
                        : (Object) canonicalPred(b)).toList());
        String synth = concatMemo.get(memoKey);
        if (synth == null) {
            synth = mintConcatName(prop);
            concatMemo.put(memoKey, synth);
            branchPreds.put(synth, branches);
        }
        TypedSpec renamed;
        if (headNode instanceof TypedMilestonedAccess ma) {
            renamed = new TypedMilestonedAccess(
                    ma.source(), synth, ma.dates(), ma.sweep(), ma.info());
        } else {
            var hp = (TypedPropertyAccess) headNode;
            renamed = new TypedPropertyAccess(
                    hp.source(), synth, hp.info());
        }
        return new TypedPropertyAccess(
                renamed, leafRead.property(), leafRead.info());
    }

    private static void flattenConcat(TypedSpec n, List<TypedSpec> out) {
        if (n instanceof TypedNativeCall c
                && c.callee().qualifiedName()
                        .equals("meta::pure::functions::collection::concatenate")
                && c.args().size() == 2) {
            flattenConcat(c.args().get(0), out);
            flattenConcat(c.args().get(1), out);
            return;
        }
        out.add(n);
    }

    /** The predicate reads no variables beyond its own parameter and the
     * parameters of lambdas that lexically ENCLOSE the read — SHADOW-AWARE
     * (audit 21b F4): a nested lambda's param binds only within that
     * lambda's subtree. An outer variable that merely shares a param's
     * name stays FREE, so a correlated pred can never look closed by name
     * collision and get applied inside the target pipeline where the
     * outer row does not exist. (Conservative the other way stays fine:
     * over-refusing the lift is loud.) */
    private static boolean predClosedOverParam(TypedLambda pred) {
        Set<String> bound = new LinkedHashSet<>(pred.parameters());
        return pred.body().stream().allMatch(b -> readsOnly(b, bound));
    }

    private static boolean readsOnly(TypedSpec n, Set<String> allowed) {
        if (n instanceof TypedVariable v
                && !allowed.contains(v.name())) {
            return false;
        }
        if (n instanceof TypedLambda l) {
            Set<String> inner = new LinkedHashSet<>(allowed);
            inner.addAll(l.parameters());
            return l.body().stream().allMatch(b -> readsOnly(b, inner));
        }
        for (TypedSpec c : n.children()) {
            if (!readsOnly(c, allowed)) {
                return false;
            }
        }
        return true;
    }

    /** The filter's source is a navigation hop whose receiver chain bottoms
     * at a lambda variable — the shape the lift can rename. */
    private static boolean isLiftableNav(TypedSpec n) {
        if (n instanceof TypedPropertyAccess pa) {
            return navBottomsAtVar(pa.source());
        }
        if (n instanceof TypedMilestonedAccess ma) {
            return navBottomsAtVar(ma.source());
        }
        return false;
    }

    /** The filter node, looking through a conform-by-emission
     * {@code ->toOne()} wrapper (a qualifier body's own coercion —
     * multiplicity-only, SQL-erased; the read semantics are the LEFT
     * join's NULL-on-no-match either way). */
    private static TypedSpec filterBehindToOne(TypedSpec n) {
        if (n instanceof com.legend.compiler.spec.typed.TypedNativeCall c
                && c.args().size() == 1
                && c.callee().qualifiedName().equals(
                        "meta::pure::functions::multiplicity::toOne")) {
            return c.args().get(0);
        }
        return n;
    }

    /** The navigation's receiver IS the lambda variable (depth-1 head). */
    private static boolean directlyOnVar(TypedSpec n) {
        return switch (n) {
            case TypedPropertyAccess pa ->
                    pa.source() instanceof com.legend.compiler.spec.typed
                            .TypedVariable;
            case TypedMilestonedAccess ma ->
                    ma.source() instanceof com.legend.compiler.spec.typed
                            .TypedVariable;
            default -> false;
        };
    }

    private static boolean navBottomsAtVar(TypedSpec n) {
        return switch (n) {
            case TypedVariable ignored -> true;
            case TypedPropertyAccess pa ->
                    navBottomsAtVar(pa.source());
            case TypedMilestonedAccess ma ->
                    navBottomsAtVar(ma.source());
            case TypedFilter f -> navBottomsAtVar(f.source());
            case TypedNativeCall c
                    when c.args().size() == 1 && c.callee().qualifiedName()
                            .equals("meta::pure::functions::multiplicity::toOne") ->
                    navBottomsAtVar(c.args().get(0));
            default -> false;
        };
    }

    /** A synthetic head's underlying property name ({@code product#f0} /
     * {@code product#d1} → {@code product}); identity for ordinary heads. */
    static String realHead(String head) {
        return JoinIdentity.of(head).prop();
    }

    /** Apply {@code renames} (identity-keyed milestoned-access nodes →
     * date-fingerprinted synthetic names) throughout the tree. */
    TypedSpec replaceDatedNodes(TypedSpec n,
            IdentityHashMap<TypedSpec, String> renames) {
        String newName = renames.get(n);
        if (newName != null) {
            var ma = (TypedMilestonedAccess) n;
            return new TypedMilestonedAccess(
                    replaceDatedNodes(ma.source(), renames), newName,
                    ma.dates(), ma.sweep(), ma.info());
        }
        return rebuildChildren(n, c -> replaceDatedNodes(c, renames));
    }

    /**
     * ONE-LEVEL generic rebuild: {@code f} applies to every child
     * expression (lambda bodies included; lambda/column structure is
     * preserved). Unknown node kinds pass through UNCHANGED — walkers
     * built on this are best-effort by design (an unvisited shape keeps
     * its loud downstream error, never silent SQL).
     */
    private static TypedSpec rebuildChildren(TypedSpec n,
            UnaryOperator<TypedSpec> f) {
        return switch (n) {
            case TypedProject p ->
                    new TypedProject(
                            f.apply(p.source()),
                            p.columns().stream().map(c ->
                                    new TypedFuncCol(
                                            c.name(), (TypedLambda) f.apply(c.fn())))
                                    .toList(),
                            p.info());
            case TypedFilter fl -> new TypedFilter(f.apply(fl.source()),
                    (TypedLambda) f.apply(fl.predicate()), fl.info());
            case TypedGroupBy gb -> new TypedGroupBy(f.apply(gb.source()),
                    gb.keys().stream().map(k -> new TypedGroupBy.GroupKey(
                            k.column(), k.fn().map(fn -> (TypedLambda) f.apply(fn))))
                            .toList(),
                    gb.aggs().stream().map(a -> new TypedAggCol(a.name(),
                            (TypedLambda) f.apply(a.map()), a.reduce()))
                            .toList(),
                    gb.info());
            case TypedSortBy sb -> new TypedSortBy(f.apply(sb.source()),
                    (TypedLambda) f.apply(sb.key()), sb.ascending(), sb.info());
            case TypedLimit l -> new TypedLimit(f.apply(l.source()),
                    l.count(), l.info());
            case TypedDrop d -> new TypedDrop(f.apply(d.source()),
                    d.count(), d.info());
            case TypedSlice sl -> new TypedSlice(f.apply(sl.source()),
                    sl.start(), sl.stop(), sl.info());
            case TypedFrom fr -> new TypedFrom(f.apply(fr.source()),
                    fr.mapping(), fr.runtime(), fr.info());
            case TypedLambda l -> new TypedLambda(l.parameters(),
                    l.body().stream().map(f).toList(), l.info());
            case TypedNativeCall c ->
                    new TypedNativeCall(c.callee(),
                            c.args().stream().map(f).toList(), c.info());
            case TypedPropertyAccess pa ->
                    new TypedPropertyAccess(
                            f.apply(pa.source()), pa.property(), pa.info());
            case TypedMilestonedAccess ma ->
                    new TypedMilestonedAccess(
                            f.apply(ma.source()), ma.property(),
                            ma.dates(), ma.sweep(), ma.info());
            case TypedMap m ->
                    new TypedMap(
                            f.apply(m.source()),
                            (TypedLambda) f.apply(m.mapper()), m.info());
            case TypedIf i ->
                    new TypedIf(
                            f.apply(i.condition()), f.apply(i.thenBranch()),
                            i.elseBranch().map(f), i.info());
            case TypedCollection c ->
                    new TypedCollection(
                            c.elements().stream().map(f).toList(), c.info());
            case TypedCast c ->
                    new TypedCast(
                            f.apply(c.source()), c.target(), c.info());
            default -> n;
        };
    }

    /** Lifted filtered-navigation heads: synthetic name → the user
     * predicate parked on the head ({@link #liftFilteredHeads}).
     * Append-only across nested resolutions — names are counter-unique. */
    /** CORRELATED lifted predicates (read the OUTER lambda's row too):
     * applied at the association JOIN CONDITION, where both rows are in
     * scope — never at the target pipeline (audit 14 B-F1's correlation
     * pass, finally built). */
    private final Map<String, TypedLambda> corrPreds =
            new java.util.LinkedHashMap<>();

    private final Map<String, TypedLambda> preds =
            new LinkedHashMap<>();

    /** {@code #cN} heads: synthetic name → the ORDERED branch predicates
     * (null members = unfiltered branches). */
    private final Map<String, List<TypedLambda>> branchPreds =
            new LinkedHashMap<>();

    /** (prop, branch predicates) → minted {@code #cN} name: the same
     * stream expression appearing twice shares ONE join identity. */
    private final Map<List<Object>, String> concatMemo =
            new LinkedHashMap<>();

    /** Alpha-normalized predicate for identity comparison: separate
     * β-inlines of the same derived property differ only in the fresh
     * parameter name — rename to a fixed one so record equality sees
     * through it. */
    private static TypedLambda canonicalPred(TypedLambda pred) {
        // FULL alpha-normalization (audit 16): the top-level rename alone
        // let nested-lambda fresh names (_iN from separate β-inlines of one
        // derived property) defeat the memo — two identities, two joins,
        // row multiplication. Canonical names contain '#', unspellable as
        // pure variables, so user code can never capture them.
        return (TypedLambda) alphaNormalize(pred,
                new LinkedHashMap<>(), new int[]{0});
    }

    private static TypedSpec alphaNormalize(TypedSpec n,
            Map<String, String> env, int[] counter) {
        if (n instanceof TypedVariable v) {
            String canonical = env.get(v.name());
            return canonical == null ? v
                    : new TypedVariable(canonical, v.info());
        }
        if (n instanceof TypedLambda l) {
            Map<String, String> inner = new LinkedHashMap<>(env);
            List<String> ps = new java.util.ArrayList<>(l.parameters().size());
            for (String p : l.parameters()) {
                String c = "#a" + counter[0]++;
                inner.put(p, c);
                ps.add(c);
            }
            return new TypedLambda(ps,
                    l.body().stream()
                            .map(b -> alphaNormalize(b, inner, counter))
                            .toList(),
                    l.info());
        }
        return rebuildChildren(n, c -> alphaNormalize(c, env, counter));
    }

    /** The variable a liftable navigation chain bottoms at. */
    private static String bottomVarOf(TypedSpec n) {
        return switch (n) {
            case TypedVariable v -> v.name();
            case TypedPropertyAccess pa -> bottomVarOf(pa.source());
            case TypedMilestonedAccess ma -> bottomVarOf(ma.source());
            case TypedFilter f -> bottomVarOf(f.source());
            case TypedNativeCall c when c.args().size() == 1 ->
                    bottomVarOf(c.args().get(0));
            default -> throw new IllegalStateException(
                    "resolver bug: liftable nav does not bottom at a variable");
        };
    }

    private int count = 0;

    /** Column lambdas born from VALUES-position map terminals: pure
     * flattening drops empties there, so the TDS lift (whose LEFT-join
     * NULL row is the point) must NOT fire inside them. */
    private final Set<TypedLambda> valuesLambdas =
            Collections.newSetFromMap(new IdentityHashMap<>());
    /** A lifted head's (and a drilled synthetic MID component's) predicate
     * reads are TAILS too: they pull the target's own slots exactly like
     * demanded leaves. */
    List<List<String>> predTailsFor(List<String> path, int mid) {
        List<List<String>> predTails = new java.util.ArrayList<>();
        Set<String> predComponents = new java.util.LinkedHashSet<>();
        predComponents.add(path.get(0));
        if (mid > 1) {
            predComponents.add(path.get(mid - 1));
        }
        for (String pcpt : predComponents) {
            for (TypedLambda liftedPred : allPreds(pcpt)) {
                Set<List<String>> predPaths = new java.util.LinkedHashSet<>();
                for (TypedSpec b : liftedPred.body()) {
                    StoreResolver.consumedPaths(b, liftedPred.parameters().get(0),
                            predPaths);
                }
                predTails.addAll(predPaths);
            }
        }
        return predTails;
    }


    /** #69 (audit-22 follow-on): a CORRELATED pred's OUTER-variable
     * reads are PARENT demand — the lift moved the only occurrence of
     * {@code $f.<head>...} out of the projection column, so the ordinary
     * scans no longer see it and the head's navigate material never
     * registered (the Substitution 'class-typed slot' wall family). */
    void corrPredOuterDemand(TypedLambda fn, Set<List<String>> out) {
        if (fn.parameters().isEmpty()) {
            return;
        }
        String uv = fn.parameters().get(0);
        for (TypedLambda corr : allCorrelatedPreds()) {
            for (TypedSpec b : corr.body()) {
                StoreResolver.consumedPaths(b, uv, out);
            }
        }
    }


}
