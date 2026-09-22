// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNewInstanceCast;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.NotImplementedException;

/**
 * H5c cast navigation: a class-typed MODEL-TO-MODEL binding is the
 * {@code ^Target($src.slot)} cast idiom — the head demands the
 * UPSTREAM navigate slot, and its leaves resolve through the cast
 * TARGET's own composed (M2M) binding table, never the raw source
 * columns (a renaming M2M binding would otherwise silently read the
 * wrong column). The frame identity guard mirrors the graph channel's
 * m2mAssocChild rule: the composed target shares the slot target's
 * row (composition preserves the upstream pipeline).
 */
final class CastNav {

    private CastNav() {
    }

    /** The cast TARGET class FQN when {@code drill} is a class-typed M2M
     * cast on a composed source — null otherwise. */
    static @com.legend.base.Nullable String castTarget(ClassSource cs,
            @com.legend.base.Nullable TypedSpec drill) {
        return unwrapToOne(drill) instanceof TypedNewInstanceCast nic
                && cs.sourceClass() != null ? nic.classFqn() : null;
    }

    /** The cast's SOURCE expression (the upstream slot read). */
    static TypedSpec castSource(@com.legend.base.Nullable TypedSpec drill) {
        return java.util.Objects.requireNonNull(unwrapToOne(
                ((TypedNewInstanceCast) java.util.Objects.requireNonNull(unwrapToOne(drill)))
                        .source()));
    }

    /** The source whose bindings serve the head's LEAVES: the cast
     * target's composed source when the head is a cast (frame identity
     * guarded), else the slot target itself. */
    static ClassSource leafSource(ClassSources sources, ClassSource cs,
            @com.legend.base.Nullable String castFqn, ClassSource target, String headKey) {
        if (castFqn == null) {
            return target;
        }
        ClassSource child = sources.get(cs.mappingFqn(), castFqn, cs.scope());
        if (!child.rowVar().equals(target.rowVar())) {
            throw new NotImplementedException("M2M cast navigation '"
                    + headKey + "': the target class '" + castFqn
                    + "' does not compose the slot target '"
                    + target.classFqn() + "' — cross-source M2M navigation"
                    + " is not supported yet");
        }
        return child;
    }

    /** The CLASS at hop {@code upto} of a navigation path (declared
     * property walk) — null when a hop is not class-typed. (Relocated
     * from StoreResolver, shared path utility.) */
    static @com.legend.base.Nullable String classAtHop(com.legend.compiler.element.ModelContext ctx,
            ClassSource cs, java.util.List<String> path, int upto) {
        String cur = cs.classFqn();
        for (int i = 0; i < upto; i++) {
            var pr = ctx.findProperty(cur,
                    SyntheticHeads.realHead(path.get(i))).orElse(null);
            if (pr == null || !(pr.type()
                    instanceof com.legend.compiler.element.type.Type
                            .ClassType ct)) {
                return null;
            }
            cur = ct.fqn();
        }
        return cur;
    }

    private static @com.legend.base.Nullable TypedSpec unwrapToOne(
            @com.legend.base.Nullable TypedSpec v) {
        return v instanceof TypedNativeCall c && c.args().size() == 1
                && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())
                ? c.args().get(0) : v;
    }

    /** WHOLE-SOURCE M2M cast heads (details : $src — H5c): the cast
     * target COMPOSES the same physical row, so its leaves dispatch as a
     * SAME-ROW AssocSub — empty prefix, shared row var (the frame-
     * identity rule); slot-read casts keep their own registration route.
     * {@code childOf}: the CONTEXT-aware (chain-dispatching) class-source
     * lookup. Returns {@code assocs} unchanged when nothing registers. */
    static java.util.Map<String, Substitution.AssocSub> withWholeSourceCasts(
            ClassSources sources, ClassSource cs,
            java.util.Map<String, Substitution.AssocSub> assocs,
            java.util.function.Function<String, ClassSource> childOf) {
        java.util.Map<String, Substitution.AssocSub> out = assocs;
        for (var e : cs.bindings().entrySet()) {
            if (out.containsKey(e.getKey())) {
                continue;
            }
            if (Pipelines.unwrapToOne(e.getValue())
                    instanceof TypedNewInstanceCast nic
                    && Pipelines.unwrapToOne(nic.source())
                            instanceof com.legend.compiler.spec.typed
                                    .TypedVariable sv
                    && sv.name().equals(cs.rowVar())) {
                ClassSource child = childOf.apply(nic.classFqn());
                if (!child.rowVar().equals(cs.rowVar())) {
                    continue; // cross-source cast: the loud wall stands
                }
                if (out == assocs) {
                    out = new java.util.LinkedHashMap<>(assocs);
                }
                out.put(e.getKey(), new Substitution.AssocSub(
                        "", child.rowVar(), child.bindings(),
                        nic.classFqn(),
                        Pipelines.slotAliases(child.pipeline())));
            }
        }
        return out;
    }
}
