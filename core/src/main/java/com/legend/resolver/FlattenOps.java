// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.spec.typed.TypedDrop;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import java.util.ArrayList;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedSlice;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.NotImplementedException;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Ops BELOW a class-flatten hop ({@code Firm.all()->filter(...)->at(0)
 * .employees}) apply to the flatten's LEFT (source-set) pipeline before
 * the association join — the filtered/limited source set is what the
 * join fans out (engine: the chain's own subselect joins the target).
 * Substitution is the caller's bindings-only rewriter — a pred that
 * navigates onward from the source class stays loud.
 */
final class FlattenOps {

    /** BELOW-OP SPLIT for a nav-slot flatten: ops whose reads pass
     * through the HOP head hoist ABOVE the materialization
     * (row-equivalent under the INNER hop) with the hop's AssocSub for
     * dispatch; the rest splice below with factory materials. Collects
     * the non-colliding paths ({@code spliceFull}) and the colliding
     * heads/tail-heads that extend the hop's own demand. */
    record BelowSplit(List<TypedSpec> hoisted, List<TypedSpec> spliceOps,
            java.util.Set<List<String>> spliceFull,
            java.util.Set<String> hopHeads,
            java.util.Set<String> hopTailHeads) {}

    static BelowSplit splitBelowOps(List<TypedSpec> belowOps,
            ClassSource src, @com.legend.base.Nullable String alias,
            java.util.Set<String> navStepKeys) {
        List<TypedSpec> hoisted = new java.util.ArrayList<>();
        List<TypedSpec> spliceOps = new java.util.ArrayList<>();
        java.util.Set<List<String>> spliceFull = new java.util.LinkedHashSet<>();
        java.util.Set<String> hopHeads = new java.util.LinkedHashSet<>();
        java.util.Set<String> hopTailHeads = new java.util.LinkedHashSet<>();
        for (TypedSpec op : belowOps) {
            List<TypedLambda> bls = switch (op) {
                case TypedFilter f -> List.of(f.predicate());
                case com.legend.compiler.spec.typed.TypedSortBy sb ->
                        List.of(sb.key());
                default -> List.of();
            };
            java.util.Set<List<String>> opPaths =
                    new java.util.LinkedHashSet<>();
            for (TypedLambda bl : bls) {
                if (bl.parameters().isEmpty()) {
                    continue;
                }
                for (TypedSpec b : bl.body()) {
                    consumedPaths(b,
                            bl.parameters().get(0), opPaths);
                }
            }
            boolean collides = false;
            for (List<String> pp : opPaths) {
                TypedSpec hb = src.bindings().get(
                        SyntheticHeads.realHead(pp.get(0)));
                // null alias = no hop to collide with (the assoc-route /
                // re-root splices reuse this splitter for path collection)
                if (alias != null && hb != null
                        && alias.equals(InnerDemand.navSlotAlias(
                                hb, src.rowVar(), navStepKeys))) {
                    collides = true;
                    hopHeads.add(pp.get(0));
                    if (pp.size() >= 2) {
                        hopTailHeads.add(pp.get(1));
                    }
                }
            }
            if (collides) {
                hoisted.add(op);
            } else {
                spliceOps.add(op);
                spliceFull.addAll(opPaths);
            }
        }
        return new BelowSplit(hoisted, spliceOps, spliceFull,
                hopHeads, hopTailHeads);
    }

    private FlattenOps() {
    }

    /** SPLICE below-ops into a SLOT-ROUTE pipeline spine: the class
     * extent the ops act on is {@code base scan + mapping ~filter}, and
     * the flatten's navigate/joinslot steps must fan out from the
     * REDUCED set — so the spine rebuilds as
     * {@code steps( belowOps( mapFilters( base ) ) )}. The mapping
     * filter reads base columns only, so pulling it beneath the ops is
     * a syntactic move, not a semantic one; a LIMIT above it would be
     * wrong (limit-then-filter). */
    static TypedSpec spliceBelow(TypedSpec pipe, List<TypedSpec> belowOps,
            Function<TypedLambda, TypedLambda> sub) {
        if (pipe instanceof com.legend.compiler.spec.typed.TypedNavigate nv) {
            return new com.legend.compiler.spec.typed.TypedNavigate(
                    spliceBelow(nv.source(), belowOps, sub), nv.alias(),
                    nv.target(), nv.predicate(), nv.form(), nv.info());
        }
        if (pipe instanceof com.legend.compiler.spec.typed.TypedJoinSlot js) {
            return new com.legend.compiler.spec.typed.TypedJoinSlot(
                    spliceBelow(js.source(), belowOps, sub), js.alias(),
                    js.target(), js.condition(), js.frameName(), js.info());
        }
        if (pipe instanceof TypedFilter mf) {
            // mapping ~filter above the steps: keep it BELOW the ops
            return applyBelow(mf, belowOps, sub);
        }
        return applyBelow(pipe, belowOps, sub);
    }

    /** {@code belowOps} arrive in chain-walk order (topmost first);
     * application is bottom-up. Row-set-shaping ops beyond filter and
     * the limit family keep a loud wall — a sort/distinct below the hop
     * needs its own emission decision, not a silent guess. */
    static TypedSpec applyBelow(TypedSpec left, List<TypedSpec> belowOps,
            Function<TypedLambda, TypedLambda> sub) {
        TypedSpec p = left;
        for (int i = belowOps.size() - 1; i >= 0; i--) {
            TypedSpec op = belowOps.get(i);
            p = switch (op) {
                case TypedFilter f ->
                        new TypedFilter(p, sub.apply(f.predicate()), p.info());
                // the limit family over an ORDERED metamodel collection
                // counts rows in DECLARATION order (byDeclarationOrder)
                case TypedLimit l -> new TypedLimit(byDeclarationOrder(p), l.count(), p.info());
                case TypedDrop d -> new TypedDrop(byDeclarationOrder(p), d.count(), p.info());
                case TypedSlice sl ->
                        new TypedSlice(byDeclarationOrder(p), sl.start(), sl.stop(), p.info());
                // a sort below the hop orders the source rows the limit
                // family counts (sortBy->first() before a to-many hop):
                // its key reads the row like a filter predicate
                case com.legend.compiler.spec.typed.TypedSortBy sb ->
                        new com.legend.compiler.spec.typed.TypedSortBy(p,
                                sub.apply(sb.key()), sb.ascending(),
                                sb.keyAlias(), p.info());
                default -> throw new NotImplementedException(
                        "object-space " + op.getClass().getSimpleName()
                        + " below a class-flatten hop is not supported yet");
            };
        }
        return p;
    }

    /** APPLY hop-colliding hoisted ops above the materialization: the
     * rewriter dispatches their reads through the hop's AssocSub —
     * row-equivalent to below-application under the INNER hop. Only
     * filters hoist; other op kinds keep a loud wall. */
    static TypedSpec applyHoisted(TypedSpec pipe, List<TypedSpec> hoisted,
            Function<TypedLambda, TypedLambda> sub) {
        TypedSpec p = pipe;
        for (TypedSpec op : hoisted) {
            if (!(op instanceof TypedFilter f)) {
                throw new NotImplementedException("hop-colliding below-op"
                        + " kind " + op.getClass().getSimpleName()
                        + " is not supported yet");
            }
            p = new TypedFilter(p, sub.apply(f.predicate()), p.info());
        }
        return p;
    }

    /** Heads read off the RE-ROOTED target class in the chain's lambdas —
     * the flatten's downstream demand (task #63: the hop target must
     * materialize WITH the nav/slot steps those heads dispatch through). */
    static java.util.Set<String> downstreamHeads(List<TypedSpec> ops,
            @com.legend.base.Nullable TypedSpec top) {
        java.util.Set<String> heads = new java.util.LinkedHashSet<>();
        collectLambdaHeads(ops == null ? List.of() : ops, heads);
        if (top != null) {
            collectLambdaHeads(List.of(top), heads);
        }
        return heads;
    }

    /** The FULL consumed paths of the ops/top above a hop (heads are
     * {@link #downstreamHeads}): a hop materializing its target's
     * navigate slots as tails needs the whole path so a slot-of-slot
     * read composes (the depth leg, 2026-09-02). */
    static java.util.Set<List<String>> downstreamPaths(List<TypedSpec> ops,
            @com.legend.base.Nullable TypedSpec top) {
        java.util.Set<List<String>> paths = new java.util.LinkedHashSet<>();
        collectLambdaPaths(ops == null ? List.of() : ops, paths);
        if (top != null) {
            collectLambdaPaths(List.of(top), paths);
        }
        return paths;
    }

    private static void collectLambdaPaths(List<TypedSpec> nodes,
            java.util.Set<List<String>> out) {
        for (TypedSpec n : nodes) {
            if (n instanceof TypedLambda lam && !lam.parameters().isEmpty()) {
                for (TypedSpec b : lam.body()) {
                    consumedPaths(b, lam.parameters().get(0), out);
                }
            }
            collectLambdaPaths(n.children(), out);
        }
    }

    private static void collectLambdaHeads(List<TypedSpec> nodes,
            java.util.Set<String> out) {
        for (TypedSpec n : nodes) {
            if (n instanceof TypedLambda lam && !lam.parameters().isEmpty()) {
                InnerDemand.collectParamPathHeads(lam,
                        lam.parameters().get(0), out);
            }
            collectLambdaHeads(n.children(), out);
        }
    }

    /** Re-stamp the join carrying {@code prefix} INNER (audit 21b F3 —
     * the flatten's row-set contract). Walks the materialized spine
     * (joins + filters); not finding the join is a loud resolver bug,
     * never a silent LEFT. */
    static TypedSpec innerizeFlattenJoin(TypedSpec pipe, String prefix) {
        TypedSpec out = innerizeOrNull(pipe, prefix, "");
        if (out == null) {
            throw new IllegalStateException("resolver bug: flatten inner-stamp"
                    + " did not find the navigate join '" + prefix
                    + "' in the materialized pipeline");
        }
        return out;
    }

    /** {@code pipe} with the join whose COMPOSED prefix (outer join
     * prefixes concatenated down the right spine) matches {@code prefix}
     * stamped INNER, or null when absent. The composed walk serves the
     * multi-hop flatten: an inner hop's navigate join nests inside the
     * previous hop's join-right with only its local prefix. */
    private static @com.legend.base.Nullable TypedSpec innerizeOrNull(TypedSpec pipe, String prefix,
            String acc) {
        if (pipe instanceof com.legend.compiler.spec.typed.TypedJoin j) {
            String composed = j.prefix().map(p -> acc + p).orElse(null);
            if (composed != null && composed.equals(prefix)) {
                return new com.legend.compiler.spec.typed.TypedJoin(
                        j.left(), j.right(), AssociationJoins.innerKind(),
                        j.condition(), j.prefix(), j.frameName(), j.info(),
                false /* resolver-synth */);
            }
            TypedSpec left = innerizeOrNull(j.left(), prefix, acc);
            if (left != null) {
                return new com.legend.compiler.spec.typed.TypedJoin(
                        left, j.right(), j.kind(), j.condition(),
                        j.prefix(), j.frameName(), j.info(),
                false /* resolver-synth */);
            }
            if (composed != null && prefix.startsWith(composed)) {
                TypedSpec right = innerizeOrNull(j.right(), prefix, composed);
                if (right != null) {
                    return new com.legend.compiler.spec.typed.TypedJoin(
                            j.left(), right, j.kind(), j.condition(),
                            j.prefix(), j.frameName(), j.info(),
                false /* resolver-synth */);
                }
            }
            return null;
        }
        if (pipe instanceof TypedFilter f) {
            TypedSpec src = innerizeOrNull(f.source(), prefix, acc);
            return src == null ? null
                    : new TypedFilter(src, f.predicate(), f.info());
        }
        // SOURCE-preserving wrappers a materialized nav target may sit
        // under (the projection subselect a nested slot materializes as,
        // a below-op limit/distinct): the join lives beneath them
        if (pipe instanceof com.legend.compiler.spec.typed.TypedProject p) {
            TypedSpec src = innerizeOrNull(p.source(), prefix, acc);
            return src == null ? null : new com.legend.compiler.spec.typed
                    .TypedProject(src, p.columns(), p.info(), p.wireForm());
        }
        if (pipe instanceof com.legend.compiler.spec.typed.TypedLimit l) {
            TypedSpec src = innerizeOrNull(l.source(), prefix, acc);
            return src == null ? null : new com.legend.compiler.spec.typed
                    .TypedLimit(src, l.count(), l.info());
        }
        if (pipe instanceof com.legend.compiler.spec.typed.TypedDistinct d) {
            TypedSpec src = innerizeOrNull(d.source(), prefix, acc);
            return src == null ? null : new com.legend.compiler.spec.typed
                    .TypedDistinct(src, d.columns(), d.info());
        }
        // a sort spliced below the hop (sortBy(...).visible — group F burn)
        if (pipe instanceof com.legend.compiler.spec.typed.TypedSortBy sb) {
            TypedSpec src = innerizeOrNull(sb.source(), prefix, acc);
            return src == null ? null : new com.legend.compiler.spec.typed
                    .TypedSortBy(src, sb.key(), sb.ascending(), sb.keyAlias(), sb.info());
        }
        return null;
    }

    /** Whether a row-count op (limit / drop / slice / first-like / static
     * at) sits directly under a chain position, through the row-
     * preserving wrappers (filter, sort, cast, from). */
    static boolean rowCountOpBelow(TypedSpec n) {
        TypedSpec cur = n;
        while (true) {
            switch (cur) {
                case com.legend.compiler.spec.typed.TypedLimit ignored -> { return true; }
                case com.legend.compiler.spec.typed.TypedDrop ignored -> { return true; }
                case com.legend.compiler.spec.typed.TypedSlice ignored -> { return true; }
                case com.legend.compiler.spec.typed.TypedNativeCall nc when ClassSorts.isFirstLike(nc) || Anchors.isStaticAt(nc) -> {
                    return true;
                }
                case com.legend.compiler.spec.typed.TypedFilter f -> cur = f.source();
                case com.legend.compiler.spec.typed.TypedSortBy sb -> cur = sb.source();
                case com.legend.compiler.spec.typed.TypedCast c -> cur = c.source();
                case com.legend.compiler.spec.typed.TypedFrom fr -> cur = fr.source();
                default -> { return false; }
            }
        }
    }


    /** The tails a flatten hop pre-joins inside its target: the chain
     * of OUTER hops (nearest first) with each hop's downstream paths.
     * A ROW-SET op between this hop's JOIN and a TO-MANY tail (first()/
     * limit/drop/slice/distinct) must see the un-fanned rows: that tail
     * is NOT pre-joined — the later hop joins afresh above the op. A
     * to-one tail is row-preserving and rides. A to-one hop with ops
     * below it joins BEFORE them (the join-first route), so its own
     * segment counts too. */
    static java.util.Set<List<String>> nextTails(int i, List<String> hops,
            List<Boolean> many, List<List<TypedSpec>> segs,
            List<TypedSpec> ops, @com.legend.base.Nullable TypedSpec top) {
        java.util.Set<List<String>> out = new java.util.LinkedHashSet<>();
        List<String> chain = new java.util.ArrayList<>();
        boolean rowSetSeen = !many.get(i)
                && segs.get(i).stream().anyMatch(FlattenOps::isRowSetOp);
        for (int k = i - 1; k >= 0; k--) {
            if (hops.get(k).startsWith(ChainDispatch.CAST_HOP)) {
                // the chain above a cast pseudo-hop reads off the subtype's
                // re-rooted extent, never off this hop's target
                break;
            }
            rowSetSeen |= segs.get(k).stream().anyMatch(FlattenOps::isRowSetOp);
            if (many.get(k) && rowSetSeen) {
                break;
            }
            chain.add(hops.get(k));
            out.add(List.copyOf(chain));
            for (List<String> pth : downstreamPaths(
                    k == 0 ? ops : segs.get(k - 1), k == 0 ? top : null)) {
                List<String> t2 = new java.util.ArrayList<>(chain);
                t2.addAll(pth);
                out.add(t2);
            }
        }
        return out;
    }

    /** The downstream paths that continue THROUGH the nested head whose
     * navigate alias is {@code alias} (head → alias per
     * {@code headNavAlias}), as tails relative to that head's target
     * (the navigate-slot route's depth leg). */
    static List<List<String>> tailsThrough(String alias,
            java.util.Map<String, String> headNavAlias,
            java.util.Set<List<String>> downstreamPaths) {
        List<List<String>> out = new java.util.ArrayList<>();
        for (List<String> p : downstreamPaths) {
            if (p.size() >= 2 && alias.equals(headNavAlias.get(p.get(0)))) {
                out.add(List.copyOf(p.subList(1, p.size())));
            }
        }
        return out;
    }

    /** Whether the NEXT outer hop of hop {@code i} is to-many with a
     * row-count op between (then it is not this hop's extra head: it
     * must join ABOVE the op, not inside this hop's target). */
    static boolean nextHopFans(int i, List<Boolean> many,
            List<List<TypedSpec>> segs) {
        return i > 0 && many.get(i - 1)
                && ((!many.get(i)
                        && segs.get(i).stream().anyMatch(FlattenOps::isRowSetOp))
                    || segs.get(i - 1).stream().anyMatch(FlattenOps::isRowSetOp));
    }

    /** A row-COUNT-sensitive op: limit / drop / slice / distinct. */
    static boolean isRowSetOp(TypedSpec op) {
        return op instanceof com.legend.compiler.spec.typed.TypedLimit
                || op instanceof com.legend.compiler.spec.typed.TypedDrop
                || op instanceof com.legend.compiler.spec.typed.TypedSlice
                || op instanceof com.legend.compiler.spec.typed.TypedDistinct;
    }

    /** One re-pointed binding for the flatten's composed source: scalar
     * bindings ride {@link Pipelines#prefixColumns}; an EMBEDDED binding
     * (TypedNewInstance ctor over parent-alias columns) re-points each
     * inner property expression, keeping the ctor. */
    static TypedSpec prefixBinding(TypedSpec b, String targetRowVar,
            String prefix, String newRowVar,
            com.legend.compiler.element.type.ExprType rowInfo) {
        TypedSpec inner = b;
        if (inner instanceof com.legend.compiler.spec.typed.TypedNativeCall c
                && c.args().size() == 1
                && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())
                && c.args().get(0) instanceof
                        com.legend.compiler.spec.typed.TypedNewInstance) {
            inner = c.args().get(0);
        }
        if (inner instanceof com.legend.compiler.spec.typed.TypedNewInstance ctor) {
            java.util.Map<String, TypedSpec> props =
                    new java.util.LinkedHashMap<>();
            for (var pe : ctor.properties().entrySet()) {
                props.put(pe.getKey(), prefixBinding(pe.getValue(),
                        targetRowVar, prefix, newRowVar, rowInfo));
            }
            return new com.legend.compiler.spec.typed.TypedNewInstance(
                    ctor.classFqn(), props, ctor.info());
        }
        return Pipelines.prefixColumns(b, targetRowVar, prefix,
                v -> new com.legend.compiler.spec.typed.TypedVariable(
                        newRowVar, rowInfo));
    }

    /**
     * A lifted head's user predicate, substituted over the TARGET's
     * bindings (the {@code rewriteExists} cfSub pattern applied at the
     * materialization site) and wrapped around the finished target
     * pipeline — the join's composite right side carries the filter
     * INSIDE (engine JTN parity), so unmatched parents keep their NULL
     * row and the outer join-stamping never double-stamps.
     */
        /** As above with the target's MATERIALIZED nav-step SubNavs: a pred's
     * depth-2 reads through class-typed slot heads ($e.address.name — the
     * Fork family) dispatch through AssocSub registries built from them
     * (the corrPredOnJoinedRow pass-1 rule, shared). */
            /** The demand half of the shared funnel: every $p.<path> read in a lambda. */
    static void consumedPaths(TypedSpec n, String userVar,
                                      Set<List<String>> out) {
        List<String> path = Substitution.pathOf(n, userVar);
        if (path != null) {
            out.add(path);
        }
        InnerDemand.composeAutoMapPaths(n, userVar, out, FlattenOps::consumedPaths);
        InnerDemand.scanTdsContainsFns(n, userVar, (b, pv) -> consumedPaths(b, pv, out));
        if (n instanceof TypedLambda l && l.parameters().contains(userVar)) {
            return;   // shadowing: the substitution stops here too (one funnel)
        }
        for (TypedSpec c : n.children()) {
            consumedPaths(c, userVar, out);
        }
    }


    /**
     * Reads of a union-THREADED column off a COMPOSED row. A flattened hop
     * into a member-union class (a single-table hierarchy) leaves each
     * key column on the composed row as member threads
     * {@code <prefix><col>_<ordinal>} — at most one non-null per row (the
     * member the row IS). A condition re-pointed at the composed row
     * names the bare {@code <prefix><col>}; when that column is absent
     * and the threads ride the row, the read is the nested coalesce over
     * the threads (the normalizer's coalesceReads rule, at the resolver's
     * row). Other reads pass through untouched.
     */
    static TypedSpec coalesceThreadedReads(TypedSpec n, String param,
            Type.RelationType row,
            com.legend.compiler.element.TypedFunction coalesce) {
        if (n instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable v
                && v.name().equals(param)) {
            String col = pa.property();
            if (row.columns().stream().anyMatch(c -> c.name().equals(col))) {
                return n;
            }
            List<Type.Column> threads = new ArrayList<>();
            for (int i = 0; ; i++) {
                String name = col + "_" + i;
                var c = row.columns().stream()
                        .filter(x -> x.name().equals(name)).findFirst();
                if (c.isEmpty()) {
                    break;
                }
                threads.add(c.get());
            }
            if (threads.size() < 2) {
                return n;
            }
            TypedSpec acc = null;
            for (int i = threads.size() - 1; i >= 0; i--) {
                Type.Column c = threads.get(i);
                TypedSpec read = new TypedPropertyAccess(v, c.name(),
                        new ExprType(c.type(),
                                com.legend.compiler.element.type.Multiplicity
                                        .Bounded.ZERO_ONE));
                acc = acc == null ? read
                        : new TypedNativeCall(coalesce, List.of(read, acc),
                                read.info());
            }
            return java.util.Objects.requireNonNull(acc);
        }
        if (n instanceof TypedLambda l && l.parameters().contains(param)) {
            return n;
        }
        return n.mapChildren(k -> coalesceThreadedReads(k, param, row, coalesce));
    }

    /** {@code pipeline} sorted by the store's declaration ORDINAL when its
     * row carries one — an ordered metamodel collection's position IS data
     * (SyntheticHeads.positionalRows), and the limit family (first/take/
     * drop/slice) counts rows in that order, never in the scan's (H2 scans
     * a keyed table in key order; corpus testEnumTheSame: enumeration-
     * Mappings->first() is Foo by declaration, Active by key). A hop's
     * target columns arrive renamed {@code prefix + name} by its join, so
     * the ordinal is read through the join's prefix. A sort already in
     * force wins; an unordered row passes through untouched. */
    static TypedSpec byDeclarationOrder(TypedSpec pipeline) {
        if (pipeline instanceof com.legend.compiler.spec.typed.TypedSortBy
                || pipeline instanceof com.legend.compiler.spec.typed.TypedSort) {
            return pipeline;
        }
        com.legend.compiler.element.type.Type.RelationType row =
                com.legend.compiler.element.type.Type.schemaView(pipeline.info().type());
        if (row == null) {
            return pipeline;
        }
        String ord = com.legend.builtin.SystemMetamodel.ORDINAL_COLUMN;
        String col = pipeline instanceof com.legend.compiler.spec.typed.TypedJoin j
                && j.prefix().isPresent() ? j.prefix().get() + ord : ord;
        if (row.columns().stream().noneMatch(c -> c.name().equals(col))) {
            return pipeline;
        }
        return new com.legend.compiler.spec.typed.TypedSort(pipeline,
                List.of(new com.legend.compiler.spec.typed.TypedSort.TypedSortKey(col, true, null)),
                false, pipeline.info());
    }
}
