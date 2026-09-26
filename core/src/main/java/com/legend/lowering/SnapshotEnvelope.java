// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;

import java.util.List;

/**
 * The SNAPSHOT (scalar-position) graph envelope: the array-aggregated
 * serialize select folded to ONE string with the engine JsonBuilder
 * cardinality rule — a SINGLETON result serializes as the bare object,
 * anything else as the array (the corpus goldens pin the dynamic
 * unwrap: {@code "values":{...}} for one row, {@code "values":[...]}
 * otherwise).
 */
final class SnapshotEnvelope {

    private SnapshotEnvelope() {
    }

    /** Simple type name; the FQN when fullyQualifiedTypePath is set. */
    static String typeName(String classFqn, boolean fq) {
        int cut = classFqn.lastIndexOf("::");
        return fq || cut < 0 ? classFqn : classFqn.substring(cut + 2);
    }

    static SqlSelect fold(SqlSelect env) {
        SqlSelect.Projection p = env.projections().get(0);
        if (!(p.expr() instanceof SqlExpr.JsonArrayAgg ja)) {
            return env;
        }
        SqlExpr count = new SqlAgg.Reducer(SqlAgg.Fn.COUNT,
                List.of(), false, List.of());
        SqlExpr sole = new SqlAgg.Reducer(SqlAgg.Fn.MIN,
                List.of(ja.value()), false, List.of());
        SqlExpr chosen = new SqlExpr.Case(List.of(
                new SqlExpr.Case.When(new SqlExpr.Call(SqlFn.EQUAL,
                        List.of(count, new SqlExpr.IntLit(1))), sole)),
                ja);
        return env.withProjections(
                List.of(new SqlSelect.Projection(chosen, "result",
                        new OutputCol("result",
                                PureSql.type(Type.Primitive.STRING),
                                false))));
    }

    /** The null-stripping envelope object (Lowerer.serializeGraph):
     * one json_object per pair, json_merge_patch-folded — RFC 7386
     * merge REMOVES null-valued keys (removePropertiesWithNullValues);
     * removeEmptySets maps an ARRAY child's '[]' aggregate to NULL
     * first so the merge drops it too. */
    static SqlExpr mergePatchObject(java.util.List<SqlExpr> pairs,
            java.util.Set<String> arrayProps, boolean removeEmpty) {
        java.util.List<SqlExpr> pieces =
                new java.util.ArrayList<>(pairs.size() / 2);
        for (int i = 0; i < pairs.size(); i += 2) {
            SqlExpr k = pairs.get(i);
            SqlExpr v = pairs.get(i + 1);
            if (removeEmpty && k instanceof SqlExpr.StringLit sl
                    && arrayProps.contains(sl.value())) {
                v = new SqlExpr.Case(java.util.List.of(
                        new SqlExpr.Case.When(
                                SqlExpr.Call.of(com.legend.sql.SqlFn.EQUAL,
                                        new SqlExpr.Cast(v, com.legend.sql
                                                .SqlType.Scalar.VARCHAR),
                                        new SqlExpr.StringLit("[]")),
                                new SqlExpr.NullLit())), v);
            }
            pieces.add(new SqlExpr.JsonObject(java.util.List.of(k, v)));
        }
        return pieces.size() == 1 ? pieces.get(0)
                : new SqlExpr.Call(com.legend.sql.SqlFn.JSON_MERGE_PATCH,
                        pieces);
    }

    /** The ASOR objectReference wrap (Lowerer.serializeGraph): the
     * per-row pk json appends to the resolver-built static prefix and
     * base64-encodes IN SQL (padding stripped). PK values come from the
     * PK_ORDER_PREFIX order keys — LOAD-BEARING here (loud when absent,
     * never a silent partial reference). */
    static SqlExpr asorWrap(
            com.legend.compiler.spec.typed.TypedSerializeGraph g,
            SqlExpr obj,
            java.util.function.Function<com.legend.compiler.spec.typed
                    .TypedFuncCol, SqlExpr> scalar) {
        java.util.List<SqlExpr> parts = new java.util.ArrayList<>();
        parts.add(new SqlExpr.StringLit("{"));
        int i = 0;
        for (var k : g.orderKeys()) {
            if (!k.name().startsWith(com.legend.compiler.spec.typed
                    .TypedSerializeGraph.PK_ORDER_PREFIX)) {
                continue;
            }
            // STRING pks json-quote; numerics stay bare ({"pk$_0":"A"})
            boolean strPk = k.fn() instanceof
                    com.legend.compiler.spec.typed.TypedLambda kl
                    && !kl.body().isEmpty()
                    && kl.body().get(kl.body().size() - 1).info().type()
                            == com.legend.compiler.element.type
                                    .Type.Primitive.STRING;
            parts.add(new SqlExpr.StringLit(
                    (i > 0 ? "," : "") + "\"pk$_" + i + "\":"
                            + (strPk ? "\"" : "")));
            parts.add(new SqlExpr.Cast(scalar.apply(k),
                    com.legend.sql.SqlType.Scalar.VARCHAR));
            if (strPk) {
                parts.add(new SqlExpr.StringLit("\""));
            }
            i++;
        }
        if (i == 0) {
            throw new IllegalStateException("objectReference needs pk order"
                    + " keys — none projected on the root envelope");
        }
        parts.add(new SqlExpr.StringLit("}"));
        SqlExpr pkJson = new SqlExpr.Call(com.legend.sql.SqlFn.CONCAT, parts);
        SqlExpr full = new SqlExpr.Call(com.legend.sql.SqlFn.CONCAT,
                java.util.List.of(
                        new SqlExpr.StringLit(java.util.Objects
                                .requireNonNull(g.objectRefPrefix())),
                        SqlExpr.Call.of(com.legend.sql.SqlFn.LPAD,
                                new SqlExpr.Cast(
                                        SqlExpr.Call.of(com.legend.sql
                                                .SqlFn.LENGTH, pkJson),
                                        com.legend.sql.SqlType.Scalar.VARCHAR),
                                new SqlExpr.IntLit(
                                com.legend.lowering.AsorRef.SEG_LEN_WIDTH),
                                new SqlExpr.StringLit("0")),
                        new SqlExpr.StringLit(":"), pkJson));
        SqlExpr asor = new SqlExpr.Call(com.legend.sql.SqlFn.CONCAT,
                java.util.List.of(
                        new SqlExpr.StringLit(
                                com.legend.lowering.AsorRef.MARKER),
                        SqlExpr.Call.of(com.legend.sql.SqlFn.RTRIM,
                                SqlExpr.Call.of(com.legend.sql.SqlFn
                                        .ENCODE_BASE64, full),
                                new SqlExpr.StringLit("="))));
        return new SqlExpr.JsonObject(java.util.List.of(
                new SqlExpr.StringLit("objectReference"), asor,
                new SqlExpr.StringLit("value"), obj));
    }
}
