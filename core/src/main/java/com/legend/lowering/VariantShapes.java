// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

import java.util.ArrayList;
import java.util.List;

/**
 * Variant/instance-literal SHAPE recognition for the {@link Lowerer} —
 * split from it purely by size (the file guardrail; the
 * Comparators/manyPropertyMap precedent). Pure static predicates and
 * rebuilds over the typed tree; no lowering state.
 */
final class VariantShapes {

    /** A comparison where one side rides the Any/JSON channel and the
     * other is a primitive LITERAL: the literal enters the channel
     * (to_json) — a bare VARCHAR would be parsed AS JSON by the engine
     * (getH2Versions' version cell against '1.4.200'). */
    static List<SqlExpr> alignLiteralToJson(List<TypedSpec> typed, List<SqlExpr> lowered) {
        for (int i = 0; i < 2; i++) {
            int j = 1 - i;
            if (isJson(lowered.get(i)) && !isJson(lowered.get(j))
                    && CastPolicy.literalish(typed.get(j))) {
                SqlExpr wrapped = SqlExpr.Call.of(SqlFn.TO_VARIANT, lowered.get(j));
                return i == 0 ? List.of(lowered.get(0), wrapped) : List.of(wrapped, lowered.get(1));
            }
        }
        return lowered;
    }

    private static boolean isJson(SqlExpr e) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t
                && t.type() == com.legend.sql.SqlType.Scalar.JSON;
    }

    private VariantShapes() {
    }

    /** An instance literal in relation position: {@code ^X(…)} or a collection of them. */
    static boolean isInstanceLiteral(TypedSpec source) {
        return source instanceof TypedNewInstance
                || (source instanceof TypedCollection c
                        && !c.elements().isEmpty()
                        && c.elements().stream().allMatch(e ->
                                e instanceof TypedNewInstance));
    }

    /**
     * The variant→class cast at the base of a property-access chain
     * ({@code to(@C).a.b} — every {@code source()} hop a property access),
     * or null when the chain roots elsewhere.
     */
    /** {@code $x.payload->get('person')->to(@Person)->isEmpty()}: EMPTINESS of a
     *  class conversion of a VARIANT. The conversion never materializes — the
     *  value is empty exactly when the JSON node is NULL (a missing key, a JSON
     *  null); the engine's DuckDB adapter passes these (PCT composition tests).
     *  The materialized to(@Class) VALUE stays its verbatim refusal (CastPolicy). */
    static boolean emptinessOverClassCast(com.legend.compiler.spec.typed.TypedNativeCall n) {
        return (Lowerer.isFamily(n, "isEmpty") || Lowerer.isFamily(n, "isNotEmpty"))
                && n.args().size() == 1
                && n.args().get(0) instanceof TypedCast vc
                && variantCastBase(vc) == vc;
    }

    static SqlExpr emptiness(com.legend.compiler.spec.typed.TypedNativeCall n,
            java.util.function.Function<TypedSpec, SqlExpr> scalar) {
        TypedCast vc = (TypedCast) n.args().get(0);
        return SqlExpr.Call.of(Lowerer.isFamily(n, "isEmpty") ? SqlFn.IS_NULL : SqlFn.IS_NOT_NULL,
                scalar.apply(vc.source()));
    }

    static @com.legend.Nullable TypedCast variantCastBase(TypedSpec spec) {
        TypedSpec cur = spec;
        while (cur instanceof TypedPropertyAccess pa) {
            cur = pa.source();
        }
        if (cur instanceof TypedCast vc
                && vc.source().info().type() instanceof Type.ClassType vsrc
                && PlatformTypes.isVariant(vsrc)
                && vc.target() instanceof Type.ClassType vtgt
                && !PlatformTypes.isVariant(vtgt)) {
            return vc;
        }
        return null;
    }

    /** Rebuild a pair struct with fields COERCED to the LUB's slots (Any -> variant). */
    static SqlExpr pairToLub(SqlExpr pair, Type own, Type.GenericType lub) {
        String[] names = {"first", "second"};
        List<SqlExpr.StructLit.Field> fields = new ArrayList<>(2);
        for (int i = 0; i < 2; i++) {
            SqlExpr f = SqlExpr.StructGet.of(pair, names[i]);
            Type lubArg = lub.arguments().get(i);
            Type ownArg = own instanceof Type.GenericType og && og.arguments().size() == 2
                    ? og.arguments().get(i) : null;
            if (lubArg instanceof Type.ClassType lc
                    && PlatformTypes.isAny(lc)
                    && (ownArg == null || !(ownArg instanceof Type.ClassType oc
                            && PlatformTypes.isAny(oc)))) {
                f = SqlExpr.Call.of(SqlFn.TO_VARIANT, f);
            }
            fields.add(new SqlExpr.StructLit.Field(names[i], f));
        }
        return new SqlExpr.StructLit(fields);
    }
}
