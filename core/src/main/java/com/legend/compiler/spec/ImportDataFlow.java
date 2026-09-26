// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.NotImplementedException;
import com.legend.model.KeyThread;
import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.NewInstance;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.List;

/**
 * The engine's {@code importDataFlow} execution-context option
 * ({@code RelationalExecutionContext(importDataFlow = true)},
 * {@code pureToSQLQuery_union.pure:140–150}): a class query over an
 * Operation union surfaces the union's primary-key THREADS — every member
 * set's key as {@code <column>_<memberOrdinal>} — as result columns, a
 * non-member row carrying the kind's DEFAULT literal in place of NULL. This
 * is the COLUMN DERIVATION at chain assembly: the threads are the facts the
 * union synthesis recorded ({@link ModelContext#unionKeyThreads}); the
 * resolver appends them where the frame executes
 * ({@code ImportDataFlowAppend}).
 */
public final class ImportDataFlow {

    private ImportDataFlow() {
    }

    /** The result columns the option adds for a query over {@code chain}
     * under {@code mappingFqn}: the root class's union key threads, typed
     * by their recorded Pure kind, each {@code [1]} (the default literal
     * fills the non-member rows). */
    public static List<Type.Column> columns(String mappingFqn, TypedSpec chain,
            ModelContext ctx) {
        TypedGetAll root = rootClass(chain);
        if (root == null) {
            throw new NotImplementedException("importDataFlow: the query has no class"
                    + " root (getAll) — the option surfaces a union's member primary"
                    + " keys; mapping=" + mappingFqn);
        }
        List<KeyThread> threads = ctx.unionKeyThreads(mappingFqn, root.classFqn());
        if (threads == null) {
            throw new NotImplementedException("importDataFlow: '" + root.classFqn()
                    + "' is not an Operation union under mapping '" + mappingFqn
                    + "' — the option over a single set is not supported yet");
        }
        List<Type.Column> out = new ArrayList<>(threads.size());
        for (KeyThread t : threads) {
            if (t.shared()) {
                continue;   // the shared table key is row identity, not a member thread
            }
            if (t.pureKind() == null) {
                throw new NotImplementedException("importDataFlow: the store declares"
                        + " no kind for union key thread '" + t.name() + "'; mapping="
                        + mappingFqn);
            }
            // the kind is the store column's Pure primitive NAME (RelationalKinds)
            Type prim = Type.Primitive.findByFqn(
                    "meta::pure::metamodel::type::" + t.pureKind()).orElseThrow(
                            () -> new IllegalStateException("importDataFlow: recorded"
                                    + " kind '" + t.pureKind() + "' is not a primitive"));
            out.add(new Type.Column(t.name(), prim, Multiplicity.Bounded.ONE));
        }
        return out;
    }

    /** Whether an execute call's ExecutionContext argument (raw, let aliases
     * resolved; typed for its class) asks for {@code importDataFlow}: a
     * RelationalExecutionContext instance whose flag is a literal true. */
    public static boolean requested(ValueSpecification rawArg, TypedSpec typedArg) {
        NewInstance ni = SourceSubst.instanceOf(rawArg);
        if (ni == null || !(typedArg.info().type() instanceof Type.ClassType ct)
                || !PlatformTypes.RELATIONAL_EXECUTION_CONTEXT.equals(ct.fqn())) {
            return false;
        }
        for (NewInstance.KeyBinding kb : ni.properties()) {
            if (!"importDataFlow".equals(kb.key())) {
                continue;
            }
            if (kb.expression().value() instanceof CBoolean b) {
                return b.value();
            }
            // the option is a compile-time fact of the call: computed = loud
            throw new NotImplementedException("importDataFlow must be a literal; got "
                    + kb.expression().value().getClass().getSimpleName());
        }
        return false;
    }

    /** {@code out} with every relation type inside it widened by
     * {@code cols} (the execute call's {@code Result<TDS>} gains the key
     * threads the executed projection will carry). */
    public static ExprType widen(ExprType out, List<Type.Column> cols) {
        return new ExprType(widenType(out.type(), cols), out.multiplicity());
    }

    private static Type widenType(Type t, List<Type.Column> cols) {
        if (t instanceof Type.RelationType r) {
            List<Type.Column> all = new ArrayList<>(r.columns());
            for (Type.Column c : cols) {
                if (all.stream().noneMatch(x -> x.name().equals(c.name()))) {
                    all.add(c);
                }
            }
            return new Type.RelationType(all, r.dynamicColumns());
        }
        if (t instanceof Type.GenericType g) {
            return new Type.GenericType(g.rawFqn(),
                    g.arguments().stream().map(a -> widenType(a, cols)).toList(),
                    g.multArguments());
        }
        return t;
    }

    /** The first class root ({@code getAll}) under {@code n} in tree order. */
    static @com.legend.base.Nullable TypedGetAll rootClass(TypedSpec n) {
        if (n instanceof TypedGetAll g) {
            return g;
        }
        for (TypedSpec c : n.children()) {
            TypedGetAll g = rootClass(c);
            if (g != null) {
                return g;
            }
        }
        return null;
    }
}
