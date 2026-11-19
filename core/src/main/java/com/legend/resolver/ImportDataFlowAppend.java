// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCDate;
import com.legend.compiler.spec.typed.TypedCDecimal;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedConcatenate;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.NotImplementedException;
import com.legend.values.PureDateLiteral;

import java.util.ArrayList;
import java.util.List;

/**
 * The engine's {@code importDataFlow} option at the frame that executes
 * ({@code pureToSQLQuery_union.pure:140–150}): the projection over the
 * UNION ROW gains one column per key thread — {@code coalesce(row.<thread>,
 * <default>)}, the kind's default literal standing in for the NULL a
 * non-member row carries ({@code getDefaultLiteralValue}: Integer 0, Float
 * 0.0, String '', Boolean false, dates %9999-01-01). {@code DriverPkAppend}'s
 * sibling; the columns were derived at chain assembly
 * ({@code ImportDataFlow.columns}) and ride the frame's bound context.
 *
 * <p>The pass DESCENDS to the projection whose source row carries the
 * threads: an assert side re-plans the executed chain under its own map, so
 * the union-row projection is rarely the statement root; a projection over
 * the frame's own result is left alone; a row carrying SOME threads is a
 * half-built union — loud.
 */
public final class ImportDataFlowAppend {

    private ImportDataFlowAppend() {
    }

    public static List<TypedSpec> apply(List<TypedSpec> body,
            List<Type.Column> threads, ModelContext ctx) {
        TypedFunction coalesce = new Callees(ctx).coalesce();
        List<TypedSpec> out = new ArrayList<>(body);
        int last = out.size() - 1;
        out.set(last, appendTo(out.get(last), threads, coalesce));
        return out;
    }

    private static TypedSpec appendTo(TypedSpec n, List<Type.Column> threads,
            TypedFunction coalesce) {
        if (n instanceof TypedFrom f) {
            TypedSpec src = appendTo(f.source(), threads, coalesce);
            return f.withSource(src, src.info());
        }
        if (n instanceof TypedLet l) {
            TypedSpec v = appendTo(l.value(), threads, coalesce);
            return new TypedLet(l.name(), v, v.info());
        }
        if (n instanceof TypedConcatenate c) {
            TypedSpec l = appendTo(c.left(), threads, coalesce);
            TypedSpec r = appendTo(c.right(), threads, coalesce);
            // the rebuilt union's column stamps are both sides' (TypedConcatenate.unionInfo)
            return new TypedConcatenate(l, r, TypedConcatenate.unionInfo(l, r, l.info().multiplicity()));
        }
        if (n instanceof TypedProject p) {
            Type.RelationType srcRow = Type.requireRelationSchema(p.source().info().type());
            long carried = threads.stream()
                    .filter(t -> srcRow.columns().stream()
                            .anyMatch(c -> c.name().equals(t.name())))
                    .count();
            if (carried == threads.size()) {
                return appendThreads(p, srcRow, threads, coalesce);
            }
            if (carried != 0) {
                throw new IllegalStateException("importDataFlow: the projection source"
                        + " row carries " + carried + " of " + threads.size()
                        + " union key threads — a half-built union row");
            }
        }
        // any other node (a map over the frame's own result, a render tail):
        // the union-row projection is somewhere beneath
        return n.mapChildren(c -> appendTo(c, threads, coalesce));
    }

    private static TypedSpec appendThreads(TypedProject p, Type.RelationType srcRow,
            List<Type.Column> threads, TypedFunction coalesce) {
        String rowVar = p.columns().isEmpty() ? "row"
                : p.columns().get(0).fn().parameters().get(0);
        ExprType rowInfo = new ExprType(srcRow, Multiplicity.Bounded.ONE);
        List<TypedFuncCol> cols = new ArrayList<>(p.columns());
        Type.RelationType outRow = Type.requireRelationSchema(p.info().type());
        List<Type.Column> outCols = new ArrayList<>(outRow.columns());
        for (Type.Column t : threads) {
            Type.Column src = srcRow.columns().stream()
                    .filter(c -> c.name().equals(t.name())).findFirst().orElseThrow();
            TypedSpec read = new TypedPropertyAccess(new TypedVariable(rowVar, rowInfo),
                    src.name(), new ExprType(src.type(), src.multiplicity()));
            ExprType one = new ExprType(t.type(), Multiplicity.Bounded.ONE);
            TypedSpec value = new TypedNativeCall(coalesce,
                    List.of(read, defaultLiteral(t.type(), one)), one);
            cols.add(new TypedFuncCol(t.name(), new TypedLambda(List.of(rowVar),
                    List.of(value), ExprType.one(new Type.FunctionType(
                            List.of(new Type.Param(rowInfo.type(), rowInfo.multiplicity())),
                            new Type.Param(t.type(), Multiplicity.Bounded.ONE)))), null));
            outCols.add(new Type.Column(t.name(), t.type(), Multiplicity.Bounded.ONE));
        }
        return new TypedProject(p.source(), cols,
                new ExprType(Type.relation(new Type.RelationType(outCols,
                        outRow.dynamicColumns())), p.info().multiplicity()));
    }

    /** The engine's {@code getDefaultLiteralValue} for a thread kind. */
    private static TypedSpec defaultLiteral(Type kind, ExprType info) {
        if (!(kind instanceof Type.Primitive prim)) {
            throw new NotImplementedException("importDataFlow: no default literal for"
                    + " key thread kind " + kind);
        }
        return switch (prim) {
            case INTEGER -> new TypedCInteger(0L, info);
            case FLOAT, NUMBER -> new TypedCFloat(0.0, null, info);
            case DECIMAL -> new TypedCDecimal(java.math.BigDecimal.ZERO, info);
            case STRING -> new TypedCString("", info);
            case BOOLEAN -> new TypedCBoolean(false, info);
            case STRICT_DATE -> new TypedCDate(
                    new PureDateLiteral.StrictDate(9999, 1, 1), info);
            case DATE, DATE_TIME -> new TypedCDate(
                    new PureDateLiteral.DateWithSecond(9999, 1, 1, 0, 0, 0), info);
            case BYTE, LATEST_DATE, STRICT_TIME -> throw new NotImplementedException(
                    "importDataFlow: no default literal for key thread kind " + prim);
        };
    }
}
