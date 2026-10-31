// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;

/**
 * A LET's kind, planned (the planner-driver seam program's S2b, 2026-10-31; the body driver's loop decided it until
 * then): what a non-last {@code let} of a test body is, read off its right-hand side and the statements after it, with
 * no session. D75: a let is a staged table or a compile-time splice, never a value Java holds -- so every kind but
 * the value let says where its rows live: an ALIAS reads a frame already planned, STAGED rows and a STAGED GRID are a
 * temporary table of the session made at the let's turn, a FRAME is an execute call whose chain its readers splice
 * ({@link FramePlanner}), and a VALUE let (a handle's included) is spliced into its readers at planning, a handle's
 * lineage rows registered ({@link PlanAllocations#registerHandlesIn}). The driver dispatches on the kind and runs
 * what runs.
 */
public final class LetPlanner {

    private LetPlanner() {
    }

    /** The kinds of a non-last let, in the order they are decided. */
    public enum Kind {
        /** {@code $r.values(->at(0)/->toOne()/->first())} over a relation-rooted frame: the same frame. */
        ALIAS,
        /** {@code rows->stage()}: the rows computed at the let's turn into a temporary table read by name after. */
        STAGED,
        /** {@code execute(...)} or {@code executeLegendQuery(...)}: a frame. */
        FRAME,
        /** A raw grid ({@code executeInDb('select ...', $c)}) a statement reads after a later effect: the engine runs
         *  it AT the let, so its rows are held in a temporary table at the let's turn (D75). Read before any effect,
         *  it stays a splice: the state it read is the state its readers see. */
        STAGED_GRID,
        /** Any other binding, a handle's included: spliced into its readers. */
        VALUE
    }

    /** The kind of {@code let}, statement {@code at} of {@code stmts}; {@code relationFrame} says whether a name is a
     *  relation-rooted frame planned before it. */
    public static Kind kind(TypedLet let, java.util.List<TypedSpec> stmts, int at, SpecCompiler specs,
            java.util.Map<com.legend.model.FunctionId, Boolean> effectMemo,
            java.util.function.Predicate<String> relationFrame) {
        if (aliasOf(let.value(), relationFrame) != null) {
            return Kind.ALIAS;
        }
        TypedSpec rhs = rhs(let);
        if (rhs instanceof TypedNativeCall sc
                && com.legend.builtin.Pure.Lite.STAGE.equals(sc.callee().qualifiedName())) {
            return Kind.STAGED;
        }
        if (rhs instanceof TypedNativeCall ec
                && (com.legend.builtin.NativeFn.Handle.isExecute(ec.callee().id())
                    || com.legend.builtin.NativeFn.Handle.of(ec.callee().id()).orElse(null)
                            == com.legend.builtin.NativeFn.Handle.EXECUTE_LEGEND_QUERY)) {
            return Kind.FRAME;
        }
        if (rhs instanceof com.legend.compiler.spec.typed.TypedRawSqlRelation
                && readAfterEffect(stmts, at, let.name(), specs, effectMemo)) {
            return Kind.STAGED_GRID;
        }
        return Kind.VALUE;
    }

    /** {@code let}'s right-hand side under its execution contexts (a {@code from} only binds where it runs). */
    public static TypedSpec rhs(TypedLet let) {
        TypedSpec rhs = let.value();
        while (rhs instanceof com.legend.compiler.spec.typed.TypedFrom rf) {
            rhs = rf.source();
        }
        return rhs;
    }

    /**
     * {@code let tds = $r.values(->at(0)/->toOne()/->first())} over a RELATION-rooted
     * frame: the wrappers are the Result envelope and the alias IS the same
     * frame ({@code $tds->size()} keeps ONE-TDS semantics). {@code at(k>0)}
     * is loud — the envelope holds one TDS. Class/scalar roots return null:
     * their at/toOne are REAL selections and the binding is an ordinary let.
     * The name of the frame aliased, or null.
     */
    public static @com.legend.base.Nullable String aliasOf(TypedSpec rhs,
            java.util.function.Predicate<String> relationFrame) {
        TypedSpec cur = rhs;
        boolean badIndex = false;
        while (true) {
            if (cur instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                    && pa.property().equals("values")) {
                cur = pa.source();
                continue;
            }
            if (cur instanceof com.legend.compiler.spec.typed.TypedNativeCall nc
                    && (com.legend.compiler.spec.ResultEnvelopeSplice.AT_FQN.equals(nc.callee().qualifiedName())
                            || com.legend.compiler.spec.ResultEnvelopeSplice.TO_ONE_FQN.equals(nc.callee().qualifiedName())
                            || com.legend.compiler.spec.ResultEnvelopeSplice.FIRST_FQN.equals(nc.callee().qualifiedName()))
                    && !nc.args().isEmpty()) {
                if (com.legend.compiler.spec.ResultEnvelopeSplice.AT_FQN.equals(nc.callee().qualifiedName())
                        && !(nc.args().size() == 2 && nc.args().get(1)
                                instanceof com.legend.compiler.spec.typed.TypedCInteger k
                                && k.value().longValue() == 0)) {
                    badIndex = true;
                }
                cur = nc.args().get(0);
                continue;
            }
            break;
        }
        if (cur instanceof com.legend.compiler.spec.typed.TypedVariable v && relationFrame.test(v.name())) {
            if (badIndex) {
                throw new IllegalStateException("Result.values->at(k>0) on a"
                        + " relation-rooted query — the values envelope holds"
                        + " one TDS");
            }
            return v.name();
        }
        return null;
    }

    /** Whether a statement after {@code at} reads the let {@code name} -- itself or through a let that reads it --
     *  once an effect has run after it (an effect's own statement included: it may read the let, its change after). */
    public static boolean readAfterEffect(java.util.List<TypedSpec> stmts, int at, String name, SpecCompiler specs,
            java.util.Map<com.legend.model.FunctionId, Boolean> effectMemo) {
        java.util.Set<String> names = new java.util.HashSet<>(java.util.List.of(name));
        boolean effectSeen = false;
        for (int j = at + 1; j < stmts.size(); j++) {
            TypedSpec s = stmts.get(j);
            boolean reads = reads(s, names);
            if (effectSeen && reads) {
                return true;
            }
            if (s instanceof TypedLet l && reads) {
                names.add(l.name());
            }
            effectSeen |= com.legend.compiler.spec.StatementEffects.containsEffect(s, specs, effectMemo);
        }
        return false;
    }

    /** Whether {@code n} reads a variable named in {@code names}. */
    private static boolean reads(TypedSpec n, java.util.Set<String> names) {
        if (n instanceof com.legend.compiler.spec.typed.TypedVariable v && names.contains(v.name())) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (reads(c, names)) {
                return true;
            }
        }
        return false;
    }
}
