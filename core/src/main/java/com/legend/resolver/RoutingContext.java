// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.spec.typed.TypedDistinct;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedGraphFetch;
import com.legend.compiler.spec.typed.TypedGroupBy;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedSerialize;
import com.legend.compiler.spec.typed.TypedSlice;
import com.legend.compiler.spec.typed.TypedSortBy;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;

/**
 * Call-site routing context (slice-1 job 1): the helpers that thread a
 * query's OWN mapping/chain context to every consumption point — the
 * spine fold for pre-walk consumers and the execute()/executionPlan()
 * argument context for the generic walk. Extracted from StoreResolver
 * (file-size guardrail); the semantics live with the resolver.
 */
final class RoutingContext {

    private RoutingContext() {
    }

    /** The mapping REF of an execute()/executionPlan() call, or null
     * when the node is not one (or its mapping argument is not a plain
     * reference — those keep the outer context). */
    static com.legend.compiler.spec.typed.@com.legend.base.Nullable
            TypedPackageableRef routedEntryMapping(TypedNativeCall nc0,
            java.util.function.UnaryOperator<TypedSpec> bind) {
        TypedNativeCall nc = entryCall(nc0, bind);
        boolean routed = com.legend.builtin.NativeFn.Handle.isExecute(nc.callee().id());
        return routed && nc.args().size() >= 2
                && nc.args().get(1) instanceof com.legend.compiler.spec
                        .typed.TypedPackageableRef mr ? mr : null;
    }

    /** The call whose mapping and runtime arguments NAME the routing: an
     * execute / executionPlan call itself, or the executionPlan BUILD a
     * plan-execute peels to ({@code $plan->execute(values, ext)} where
     * {@code $plan} is let-bound to {@code executionPlan(f, m, rt, ext)}) —
     * a plan execution re-evaluated as a value routes exactly as the plan
     * statement did. */
    static TypedNativeCall entryCall(TypedNativeCall nc,
            java.util.function.UnaryOperator<TypedSpec> bind) {
        if (com.legend.compiler.element.type.PlatformTypes.EXECUTION_PLAN_EXECUTE
                        .equals(nc.callee().qualifiedName())
                && !nc.args().isEmpty()
                && bind.apply(nc.args().get(0)) instanceof TypedNativeCall pb
                && com.legend.compiler.element.type.PlatformTypes.EXECUTION_PLAN
                        .equals(pb.callee().qualifiedName())) {
            return pb;
        }
        return nc;
    }

    static StoreResolver.Context routedContext(TypedNativeCall nc0,
            StoreResolver.Context outer,
            com.legend.compiler.spec.SpecCompiler specs,
            java.util.function.UnaryOperator<TypedSpec> bind) {
        TypedNativeCall nc = entryCall(nc0, bind);
        var mr = java.util.Objects.requireNonNull(routedEntryMapping(nc, bind));
        TypedSpec rt = nc.args().size() >= 3 ? nc.args().get(2) : null;
        if (rt instanceof TypedUserCall) {
            // a HELPER-built runtime (m2m2r::runtime()) is brought to its
            // VALUE before the context is read
            rt = new com.legend.compiler.spec.UserCallInliner(specs)
                    .inlineBody(java.util.List.of(rt)).get(0);
        }
        var bound = com.legend.compiler.spec.typed.ExecutionContext.reader()
                .bind(bind)
                .read(java.util.Optional.of(mr), rt)
                .withOptions(com.legend.compiler.spec.ExecuteChainAssembly
                        .executionContextArg(nc), bind);
        // a call that declares no chain of its own INHERITS the enclosing one
        return new StoreResolver.Context(mr.fullPath(), outer.runtimeFqn(),
                bound.inheritingChain(outer.chainMappings()).chainMappings());
    }

    /** The context in effect at the chain's getAll: fold every in-chain
     * from() met on the source spine (deepest wins by composition) —
     * the SAME folding the collect walk applies at 'in-chain from()
     * re-scopes BOTH locals'. Pre-walk consumers (the synthetic-head
     * canonicalizer) must dispatch under THIS context, not the entry
     * context: an entry-captured context re-derives at consumption what
     * the query's own from() already decided (slice-1 job 1 — the
     * runtime-fallback firings were all such stale captures). */
    static StoreResolver.Context spineContext(TypedSpec top,
            StoreResolver.Context outer,
            java.util.function.BiFunction<TypedFrom, StoreResolver.Context,
                    StoreResolver.Context> fromContext) {
        StoreResolver.Context c = outer;
        TypedSpec cur = top;
        while (true) {
            if (cur instanceof TypedFrom fr) {
                c = fromContext.apply(fr, c);
                cur = fr.source();
            } else if (cur instanceof TypedSerialize sz) {
                cur = sz.source() instanceof TypedGraphFetch gf
                        ? gf.source() : sz.source();
            } else if (cur instanceof TypedProject n) {
                cur = n.source();
            } else if (cur instanceof TypedGroupBy n) {
                cur = n.source();
            } else if (cur instanceof TypedFilter n) {
                cur = n.source();
            } else if (cur instanceof TypedDistinct n) {
                cur = n.source();
            } else if (cur instanceof TypedLimit n) {
                cur = n.source();
            } else if (cur instanceof TypedSlice n) {
                cur = n.source();
            } else if (cur instanceof TypedSortBy n) {
                cur = n.source();
            } else if (cur instanceof TypedMap n) {
                cur = n.source();
            } else if (cur instanceof com.legend.compiler.spec.typed
                    .TypedPropertyAccess n) {
                cur = n.source();
            } else if (cur instanceof TypedNativeCall n
                    && !n.args().isEmpty()) {
                cur = n.args().get(0);
            } else {
                break;
            }
        }
        return c;
    }

    /** Per-class dispatch: the runtime candidate that BINDS the class wins
     * (chain-aware — ClassSources owns the binding logic). */
    /** The memo key of a context-dependent resolution (audit 23: runtime
     * + chain mappings participate — a mixed read poisoned the cache
     * across an in-chain from()). */
    static String contextKey(StoreResolver.Context c) {
        return (c.explicitMapping() == null ? "" : c.explicitMapping())
                + '\u0000'
                + (c.runtimeFqn() == null ? "" : c.runtimeFqn())
                + (c.chainMappings().isEmpty() ? ""
                        : '\u0000' + String.join(",", c.chainMappings()));
    }

}
