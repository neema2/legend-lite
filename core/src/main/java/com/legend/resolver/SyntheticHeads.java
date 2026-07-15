// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.spec.typed.TypedDrop;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedGroupBy;
import com.legend.compiler.spec.typed.TypedAggCol;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedSlice;
import com.legend.compiler.spec.typed.TypedSortBy;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SYNTHETIC HEAD identities — filtered navigations lift to
 * {@code head#fN} chains (predicate parked for the join target),
 * two-dates-per-head splits mint {@code head#dN} (a separate join
 * identity per distinct date-set), and {@link #realHead} keeps every
 * model lookup transparent. Append-only across nested resolutions —
 * names are counter-unique; the registry is the ONE owner of the
 * '#'-suffix convention (the join-identity value type replaces the
 * string carrier next — ledger).
 */
final class SyntheticHeads {

    TypedLambda pred(String head) {
        return preds.get(head);
    }

    boolean hasPred(String head) {
        return preds.containsKey(head);
    }

    /** A fresh date-fingerprinted identity for {@code prop}. */
    String mintDateName(String prop) {
        return prop + "#d" + count++;
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
        if (enabled
                && n instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                && pa.source() instanceof TypedFilter f
                && f.predicate().parameters().size() == 1
                && f.info().type()
                        instanceof com.legend.compiler.element.type.Type.ClassType
                && isLiftableNav(f.source())
                // the predicate must be CLOSED over its own parameter: an
                // outer-variable read has no correlation pass on this route
                // and a column-name collision would silently self-correlate
                // (audit 14 B-F1) — unlifted shapes keep their loud wall
                && predClosedOverParam(f.predicate())
                && !(pa.info().multiplicity()
                        instanceof com.legend.compiler.element.type
                                .Multiplicity.Bounded b
                        && Integer.valueOf(1).equals(b.upper()))) {
            TypedSpec head = liftFilteredHeads(f.source(), true);
            TypedSpec renamed;
            String synth;
            if (head instanceof com.legend.compiler.spec.typed
                    .TypedMilestonedAccess ma) {
                synth = ma.property() + "#f" + count++;
                renamed = new com.legend.compiler.spec.typed.TypedMilestonedAccess(
                        ma.source(), synth, ma.dates(), ma.sweep(), ma.info());
            } else {
                var hp = (com.legend.compiler.spec.typed.TypedPropertyAccess) head;
                synth = hp.property() + "#f" + count++;
                renamed = new com.legend.compiler.spec.typed.TypedPropertyAccess(
                        hp.source(), synth, hp.info());
            }
            preds.put(synth, f.predicate());
            return new com.legend.compiler.spec.typed.TypedPropertyAccess(
                    renamed, pa.property(), pa.info());
        }
        return switch (n) {
            case com.legend.compiler.spec.typed.TypedProject p ->
                    new com.legend.compiler.spec.typed.TypedProject(
                            liftFilteredHeads(p.source(), enabled),
                            p.columns().stream().map(c ->
                                    new com.legend.compiler.spec.typed.TypedFuncCol(
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
            case com.legend.compiler.spec.typed.TypedNativeCall c ->
                    new com.legend.compiler.spec.typed.TypedNativeCall(c.callee(),
                            c.args().stream().map(a -> liftFilteredHeads(a, enabled))
                                    .toList(), c.info());
            case com.legend.compiler.spec.typed.TypedPropertyAccess pa ->
                    new com.legend.compiler.spec.typed.TypedPropertyAccess(
                            liftFilteredHeads(pa.source(), enabled),
                            pa.property(), pa.info());
            case com.legend.compiler.spec.typed.TypedMilestonedAccess ma ->
                    new com.legend.compiler.spec.typed.TypedMilestonedAccess(
                            liftFilteredHeads(ma.source(), enabled), ma.property(),
                            ma.dates(), ma.sweep(), ma.info());
            // auto-map mapper bodies are VALUE flattenings (empties drop) —
            // the TDS lift stays off inside them; unlifted shapes keep
            // their loud error
            case com.legend.compiler.spec.typed.TypedMap m ->
                    new com.legend.compiler.spec.typed.TypedMap(
                            liftFilteredHeads(m.source(), enabled),
                            (TypedLambda) liftFilteredHeads(m.mapper(), false),
                            m.info());
            case com.legend.compiler.spec.typed.TypedIf i ->
                    new com.legend.compiler.spec.typed.TypedIf(
                            liftFilteredHeads(i.condition(), enabled),
                            liftFilteredHeads(i.thenBranch(), enabled),
                            i.elseBranch().map(e -> liftFilteredHeads(e, enabled)),
                            i.info());
            case com.legend.compiler.spec.typed.TypedCollection c ->
                    new com.legend.compiler.spec.typed.TypedCollection(
                            c.elements().stream().map(e ->
                                    liftFilteredHeads(e, enabled)).toList(),
                            c.info());
            case com.legend.compiler.spec.typed.TypedCast c ->
                    new com.legend.compiler.spec.typed.TypedCast(
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
    com.legend.compiler.spec.typed.TypedMap liftValueMapFilter(
            com.legend.compiler.spec.typed.TypedMap m) {
        TypedLambda mapper = m.mapper();
        if (mapper.body().size() != 1
                || !(mapper.body().get(0)
                        instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa)
                || !(pa.source() instanceof TypedFilter f)
                || f.predicate().parameters().size() != 1
                || f.predicate().body().size() != 1
                || !(f.info().type()
                        instanceof com.legend.compiler.element.type.Type.ClassType)
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
            synth = ma.property() + "#f" + count++;
            renamed = new com.legend.compiler.spec.typed.TypedMilestonedAccess(
                    ma.source(), synth, ma.dates(), ma.sweep(), ma.info());
        } else {
            var hp = (com.legend.compiler.spec.typed.TypedPropertyAccess) f.source();
            synth = hp.property() + "#f" + count++;
            renamed = new com.legend.compiler.spec.typed.TypedPropertyAccess(
                    hp.source(), synth, hp.info());
        }
        TypedLambda mapper2 = new TypedLambda(mapper.parameters(),
                java.util.List.of(new com.legend.compiler.spec.typed
                        .TypedPropertyAccess(renamed, pa.property(), pa.info())),
                mapper.info());
        TypedSpec inlined = Substitution.inlineParam(f.predicate().body().get(0),
                f.predicate().parameters().get(0), renamed);
        var srcParam = ((com.legend.compiler.element.type.Type.FunctionType)
                mapper.info().type()).params().get(0);
        TypedLambda filterLam = new TypedLambda(mapper.parameters(),
                java.util.List.of(inlined),
                new com.legend.compiler.element.type.ExprType(
                        new com.legend.compiler.element.type.Type.FunctionType(
                                java.util.List.of(srcParam),
                                new com.legend.compiler.element.type.Type.Param(
                                        com.legend.compiler.element.type
                                                .Type.Primitive.BOOLEAN,
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ONE)),
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
        valuesLambdas.add(mapper2);
        return new com.legend.compiler.spec.typed.TypedMap(
                new TypedFilter(m.source(), filterLam, m.source().info()),
                mapper2, m.info());
    }

    /** The predicate reads no variables beyond its own parameter and the
     * parameters of lambdas nested WITHIN it (conservative: any other
     * variable name refuses the lift — over-refusing stays loud). */
    private static boolean predClosedOverParam(TypedLambda pred) {
        Set<String> bound = new java.util.LinkedHashSet<>(pred.parameters());
        collectLambdaParamNames(pred.body(), bound);
        return pred.body().stream().allMatch(b -> readsOnly(b, bound));
    }

    private static void collectLambdaParamNames(java.util.List<TypedSpec> body,
            Set<String> out) {
        for (TypedSpec b : body) {
            collectLambdaParamNames(b, out);
        }
    }

    private static void collectLambdaParamNames(TypedSpec n, Set<String> out) {
        if (n instanceof TypedLambda l) {
            out.addAll(l.parameters());
        }
        for (TypedSpec c : n.children()) {
            collectLambdaParamNames(c, out);
        }
    }

    private static boolean readsOnly(TypedSpec n, Set<String> allowed) {
        if (n instanceof com.legend.compiler.spec.typed.TypedVariable v
                && !allowed.contains(v.name())) {
            return false;
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
        if (n instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa) {
            return navBottomsAtVar(pa.source());
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedMilestonedAccess ma) {
            return navBottomsAtVar(ma.source());
        }
        return false;
    }

    private static boolean navBottomsAtVar(TypedSpec n) {
        return switch (n) {
            case com.legend.compiler.spec.typed.TypedVariable ignored -> true;
            case com.legend.compiler.spec.typed.TypedPropertyAccess pa ->
                    navBottomsAtVar(pa.source());
            case com.legend.compiler.spec.typed.TypedMilestonedAccess ma ->
                    navBottomsAtVar(ma.source());
            case TypedFilter f -> navBottomsAtVar(f.source());
            case com.legend.compiler.spec.typed.TypedNativeCall c
                    when c.args().size() == 1 && c.callee().qualifiedName()
                            .equals("meta::pure::functions::multiplicity::toOne") ->
                    navBottomsAtVar(c.args().get(0));
            default -> false;
        };
    }

    /** A synthetic head's underlying property name ({@code product#f0} /
     * {@code product#d1} → {@code product}); identity for ordinary heads. */
    static String realHead(String head) {
        int i = head.indexOf('#');
        return i < 0 ? head : head.substring(0, i);
    }

    /** Apply {@code renames} (identity-keyed milestoned-access nodes →
     * date-fingerprinted synthetic names) throughout the tree. */
    TypedSpec replaceDatedNodes(TypedSpec n,
            java.util.IdentityHashMap<TypedSpec, String> renames) {
        String newName = renames.get(n);
        if (newName != null) {
            var ma = (com.legend.compiler.spec.typed.TypedMilestonedAccess) n;
            return new com.legend.compiler.spec.typed.TypedMilestonedAccess(
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
            java.util.function.UnaryOperator<TypedSpec> f) {
        return switch (n) {
            case com.legend.compiler.spec.typed.TypedProject p ->
                    new com.legend.compiler.spec.typed.TypedProject(
                            f.apply(p.source()),
                            p.columns().stream().map(c ->
                                    new com.legend.compiler.spec.typed.TypedFuncCol(
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
            case com.legend.compiler.spec.typed.TypedNativeCall c ->
                    new com.legend.compiler.spec.typed.TypedNativeCall(c.callee(),
                            c.args().stream().map(f).toList(), c.info());
            case com.legend.compiler.spec.typed.TypedPropertyAccess pa ->
                    new com.legend.compiler.spec.typed.TypedPropertyAccess(
                            f.apply(pa.source()), pa.property(), pa.info());
            case com.legend.compiler.spec.typed.TypedMilestonedAccess ma ->
                    new com.legend.compiler.spec.typed.TypedMilestonedAccess(
                            f.apply(ma.source()), ma.property(),
                            ma.dates(), ma.sweep(), ma.info());
            case com.legend.compiler.spec.typed.TypedMap m ->
                    new com.legend.compiler.spec.typed.TypedMap(
                            f.apply(m.source()),
                            (TypedLambda) f.apply(m.mapper()), m.info());
            case com.legend.compiler.spec.typed.TypedIf i ->
                    new com.legend.compiler.spec.typed.TypedIf(
                            f.apply(i.condition()), f.apply(i.thenBranch()),
                            i.elseBranch().map(f), i.info());
            case com.legend.compiler.spec.typed.TypedCollection c ->
                    new com.legend.compiler.spec.typed.TypedCollection(
                            c.elements().stream().map(f).toList(), c.info());
            case com.legend.compiler.spec.typed.TypedCast c ->
                    new com.legend.compiler.spec.typed.TypedCast(
                            f.apply(c.source()), c.target(), c.info());
            default -> n;
        };
    }

    /** Lifted filtered-navigation heads: synthetic name → the user
     * predicate parked on the head ({@link #liftFilteredHeads}).
     * Append-only across nested resolutions — names are counter-unique. */
    private final java.util.Map<String, TypedLambda> preds =
            new java.util.LinkedHashMap<>();

    private int count = 0;

    /** Column lambdas born from VALUES-position map terminals: pure
     * flattening drops empties there, so the TDS lift (whose LEFT-join
     * NULL row is the point) must NOT fire inside them. */
    private final java.util.Set<TypedLambda> valuesLambdas =
            java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
}
