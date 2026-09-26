// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.resolver;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedAggCol;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedGroupBy;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import java.util.List;

/**
 * A REDUCER over a to-many navigation inside a derived graph leaf — a
 * checked constraint or a qualified property: {@code
 * $this.employees.name->isDistinct()}, {@code $this.employees->map(e |
 * …)->isDistinct()}, {@code $this.orders.amount->sum()}. The value is ONE
 * scalar per object, so it is a correlated scalar aggregate subquery (the
 * engine's own description of qualifier expressions with navigations; the
 * store-row LEFT-JOIN tenet governs READS, not a single aggregate value):
 * the corr-filtered target relation, the mapped value projected through
 * the target's bindings, a keyless group-by over it with the reducer —
 * one row, one column, rendered as a scalar subquery by the lowering's
 * relation-in-scalar-position rule.
 */
final class NavReducer {

    private NavReducer() {
    }

    /** {@code head} the navigation off {@code $this}; {@code var} the
     *  element variable the mapped {@code body} reads. */
    record Shape(TypedSpec head, String var, TypedSpec body) {
    }

    /** The reducer's argument as head + element mapper: a {@code map} over
     *  the head, or a leaf read {@code head.leaf}; null for other shapes. */
    static @com.legend.base.Nullable Shape shapeOf(TypedNativeCall rc, String thisVar) {
        if (rc.args().size() != 1) {
            return null;
        }
        TypedSpec arg = rc.args().get(0);
        if (arg instanceof TypedMap m && m.mapper().parameters().size() == 1
                && !m.mapper().body().isEmpty()) {
            return new Shape(m.source(), m.mapper().parameters().get(0),
                    m.mapper().body().get(m.mapper().body().size() - 1));
        }
        if (arg instanceof TypedPropertyAccess pa
                && (pa.source() instanceof TypedPropertyAccess
                        || pa.source() instanceof com.legend.compiler.spec.typed.TypedFilter)
                && navigationOffThis(pa.source(), thisVar)) {
            // a leaf read off the head ($this.employees.name), or off a
            // FILTERED head ($this.employees->filter(…).name): the head
            // (filter included) is what the correlated relation serves
            TypedSpec head = pa.source();
            String var = "_e" + System.identityHashCode(pa);
            return new Shape(head, var, new TypedPropertyAccess(
                    new TypedVariable(var, new ExprType(
                            java.util.Objects.requireNonNull(Type.asClassType(head.info().type())),
                            Multiplicity.Bounded.ONE)),
                    pa.property(), pa.info()));
        }
        if (arg instanceof com.legend.compiler.spec.typed.TypedFilter f
                && navigationOffThis(f, thisVar)) {
            // a COUNTED filtered head ($this.employees->filter(…)->count()):
            // the element itself is the value — one row per element,
            // projected as the literal 1
            String var = "_e" + System.identityHashCode(f);
            return new Shape(f, var, new com.legend.compiler.spec.typed.TypedCInteger(1L,
                    ExprType.one(Type.Primitive.INTEGER)));
        }
        return null;
    }

    /** Whether {@code n} is a navigation off {@code $this} — a property
     * access on the variable, possibly beneath a filter. */
    private static boolean navigationOffThis(TypedSpec n, String thisVar) {
        TypedSpec cur = n;
        if (cur instanceof com.legend.compiler.spec.typed.TypedFilter f) {
            cur = f.source();
        }
        return cur instanceof TypedPropertyAccess head
                && head.source() instanceof TypedVariable v && v.name().equals(thisVar);
    }

    /** {@code rel}: the corr-filtered target relation over {@code rowVar}
     *  (rows of {@code targetRow}); {@code value}: the mapped element value
     *  already inlined through the target's bindings on {@code rowVar}. */
    static TypedSpec subquery(TypedNativeCall rc, TypedSpec rel, String rowVar,
            Type.RelationType targetRow, TypedSpec value) {
        Type valueType = value.info().type();
        Multiplicity valueMult = value.info().multiplicity();
        Type.RelationType oneCol = new Type.RelationType(List.of(
                new Type.Column("v", valueType, valueMult)));
        var projFn = new Type.FunctionType(
                List.of(new Type.Param(targetRow, Multiplicity.Bounded.ONE)),
                new Type.Param(valueType, valueMult));
        TypedSpec proj = new TypedProject(rel,
                List.of(new TypedFuncCol("v", new TypedLambda(List.of(rowVar), List.of(value),
                        new ExprType(projFn, Multiplicity.Bounded.ONE)))),
                new ExprType(Type.relation(oneCol), Multiplicity.Bounded.ZERO_MANY));
        String x = rowVar + "_v";
        var mapFn = new Type.FunctionType(
                List.of(new Type.Param(oneCol, Multiplicity.Bounded.ONE)),
                new Type.Param(valueType, valueMult));
        TypedLambda map = new TypedLambda(List.of(x), List.of(new TypedPropertyAccess(
                new TypedVariable(x, new ExprType(oneCol, Multiplicity.Bounded.ONE)),
                "v", new ExprType(valueType, valueMult))),
                new ExprType(mapFn, Multiplicity.Bounded.ONE));
        String y = rowVar + "_ys";
        var reduceFn = new Type.FunctionType(
                List.of(new Type.Param(valueType, Multiplicity.Bounded.ZERO_MANY)),
                new Type.Param(rc.info().type(), rc.info().multiplicity()));
        TypedLambda reduce = new TypedLambda(List.of(y), List.of(rc.withChildren(List.of(
                new TypedVariable(y, new ExprType(valueType, Multiplicity.Bounded.ZERO_MANY))))),
                new ExprType(reduceFn, Multiplicity.Bounded.ONE));
        Type.RelationType aggCol = new Type.RelationType(List.of(
                new Type.Column("agg", rc.info().type(), Multiplicity.Bounded.ONE)));
        return new TypedGroupBy(proj, List.of(),
                List.of(new TypedAggCol("agg", map, reduce, List.of())),
                new ExprType(Type.relation(aggCol), Multiplicity.Bounded.ONE));
    }
}
