// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedDistinct;
import com.legend.compiler.spec.typed.TypedDrop;
import com.legend.compiler.spec.typed.TypedJoin;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedSlice;
import com.legend.compiler.spec.typed.TypedSortBy;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A class query CUT by position ({@code take}, {@code drop}, {@code slice},
 * {@code first}) and then projected through a to-many navigation: the cut
 * counts the class's OBJECTS, and each object's projection then gives one row
 * per element it navigates to. {@code Equity.all()->sort(..)->take(5)->project(
 * ~[t: e|$e.ticker, s: e|$e.instrument.news.storyId])} is the news of five
 * equities, never five stories.
 *
 * <p>The resolver materializes the root's rows with every navigation joined
 * and folds the chain's ops on top (StoreResolver.resolveObject): a cut over
 * a to-many join counted the joined rows (2026-10-27: a LIMIT 5 after the
 * LEFT JOIN to the news). Here the joins NO op reads -- the projection's own
 * -- move ABOVE the ops: the cut rows are the class's, and the projection's
 * joins fan them out after. A join an op reads stays below it (a filter over
 * a to-many crossing keeps its engine row semantics, one row per matching
 * element). The rows keep the order asked for: the outermost sort is
 * re-applied above the moved joins, since a join does not keep its input's
 * order.
 *
 * <p>Only when it changes an answer: a positional cut in the chain, a
 * projected column of many values, and a to-many hop among the joins it
 * moves. Every other shape folds exactly as before.
 */
final class CutBeforeFanOut {

    private CutBeforeFanOut() {
    }

    /**
     * The association joins to fold above the chain's ops, in their fold
     * order; empty when the cut cannot miscount (see the class comment).
     *
     * @param opPaths      the navigation paths the chain's ops read
     * @param joinsByChain the registered joins, by dotted chain key
     * @param assocJoins   every registered join, in fold order
     */
    static List<AssociationJoins.AssocJoin> joinsAbove(ModelContext ctx,
            ClassSource cs, List<TypedSpec> ops, TypedSpec top,
            Set<List<String>> opPaths,
            Map<String, AssociationJoins.AssocJoin> joinsByChain,
            List<AssociationJoins.AssocJoin> assocJoins) {
        if (!(top instanceof TypedProject p) || !fansOut(p)
                || ops.stream().noneMatch(CutBeforeFanOut::isCut)
                || ops.stream().anyMatch(op -> op instanceof TypedDistinct)) {
            return List.of();
        }
        Set<String> opChains = new HashSet<>();
        for (List<String> path : opPaths) {
            for (int i = 1; i < path.size(); i++) {
                opChains.add(String.join(".", path.subList(0, i)));
            }
        }
        Set<AssociationJoins.AssocJoin> read = identitySet();
        Set<AssociationJoins.AssocJoin> unread = identitySet();
        boolean toMany = false;
        for (var e : joinsByChain.entrySet()) {
            if (opChains.contains(e.getKey())) {
                read.add(e.getValue());
            } else {
                unread.add(e.getValue());
                toMany |= isToManyHop(ctx, cs, List.of(e.getKey().split("\\.")))
                        || fansOut(ctx, e.getValue().target().classFqn(),
                                e.getValue().targetSubNavs());
            }
        }
        if (!toMany) {
            return List.of();
        }
        List<AssociationJoins.AssocJoin> out = new ArrayList<>();
        for (AssociationJoins.AssocJoin aj : assocJoins) {
            if (unread.contains(aj) && !read.contains(aj)) {
                out.add(aj);
            }
        }
        return out;
    }

    /** The joins that fold below the cut: {@code joins} less those {@code above} it, in order, compared by
     *  identity. */
    static List<AssociationJoins.AssocJoin> below(List<AssociationJoins.AssocJoin> joins,
            List<AssociationJoins.AssocJoin> above) {
        Set<AssociationJoins.AssocJoin> moved = identitySet();
        moved.addAll(above);
        return joins.stream().filter(aj -> !moved.contains(aj)).toList();
    }

    /** The joins folded above {@code cut} -- the {@code moved} outermost
     * joins of {@code joined} -- rebuilt over {@code cutRows}: the same
     * joins, their left the cut rows (the ops keep the row's columns). */
    static TypedSpec rebase(TypedSpec joined, int moved, TypedSpec cut,
            TypedSpec cutRows) {
        List<TypedJoin> joins = new ArrayList<>();
        TypedSpec cur = joined;
        for (int i = 0; i < moved; i++) {
            if (!(cur instanceof TypedJoin j)) {
                throw new IllegalStateException("resolver bug: " + moved
                        + " joins above the cut, found "
                        + cur.getClass().getSimpleName());
            }
            joins.add(j);
            cur = j.left();
        }
        if (cur != cut) {
            throw new IllegalStateException(
                    "resolver bug: the moved joins do not sit on the cut rows");
        }
        Collections.reverse(joins);
        TypedSpec out = cutRows;
        for (TypedJoin j : joins) {
            out = new TypedJoin(out, j.right(), j.kind(), j.condition(),
                    j.prefix(), j.frameName(), j.info(), j.userCondition());
        }
        return out;
    }

    /** The ops' result with the joins above the cut put back over it ({@link #rebase}), then ordered again by the
     *  chain's outermost sort, {@code order} (its key {@code key}, substituted), since a join does not keep its input's
     *  order; none when the chain has no sort. */
    static TypedSpec overTheCut(TypedSpec joined, int moved, TypedSpec cut, TypedSpec cutRows,
            @com.legend.base.Nullable TypedSortBy order,
            com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedLambda key) {
        TypedSpec out = rebase(joined, moved, cut, cutRows);
        return order == null || key == null ? out
                : new TypedSortBy(out, key, order.ascending(), order.keyAlias(), out.info());
    }

    /** The sort that orders the chain's rows: its outermost. */
    static @com.legend.base.Nullable TypedSortBy outermostSort(
            List<TypedSpec> ops) {
        for (TypedSpec op : ops) {   // ops run outermost first
            if (op instanceof TypedSortBy sb) {
                return sb;
            }
        }
        return null;
    }

    private static boolean isCut(TypedSpec op) {
        return op instanceof TypedLimit || op instanceof TypedDrop
                || op instanceof TypedSlice;
    }

    /** A projected column of many values: a to-many navigation's. */
    private static boolean fansOut(TypedProject p) {
        return p.columns().stream().anyMatch(c -> {
            var body = c.fn().body();
            return body.get(body.size() - 1).info().multiplicity().isMany();
        });
    }

    /** The chain's last hop is a property of many values. */
    private static boolean isToManyHop(ModelContext ctx, ClassSource cs,
            List<String> chain) {
        String owner = CastNav.classAtHop(ctx, cs, chain, chain.size() - 1);
        return owner != null && ctx.findProperty(owner,
                SyntheticHeads.realHead(chain.get(chain.size() - 1)))
                .map(pr -> pr.multiplicity().isMany()).orElse(false);
    }

    /** A join's target carries a to-many navigation INSIDE it (a hop's own
     * navigate slots ride its join: {@code $e.instrument.news}, the news
     * joined within the instrument's). */
    private static boolean fansOut(ModelContext ctx, String classFqn,
            Map<String, Substitution.SubNav> subNavs) {
        for (var e : subNavs.entrySet()) {
            var pr = ctx.findProperty(classFqn,
                    SyntheticHeads.realHead(e.getKey())).orElse(null);
            if (pr == null) {
                continue;
            }
            if (pr.multiplicity().isMany()
                    || (pr.type() instanceof com.legend.compiler.element.type
                            .Type.ClassType ct
                            && fansOut(ctx, ct.fqn(), e.getValue().children()))) {
                return true;
            }
        }
        return false;
    }

    private static <T> Set<T> identitySet() {
        return Collections.newSetFromMap(new IdentityHashMap<>());
    }
}
