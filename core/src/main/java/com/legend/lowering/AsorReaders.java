// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.lowering;

import com.legend.builtin.Pure;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ASOR store-object-reference READERS (batch 72b; extracted from
 * {@link Scalars} at the file guardrail): the reference decodes IN SQL —
 * base64 (unpadded on the wire), then the framing regex picks the
 * segment. {@code asorPkValue} = the {@code pk$_i} value cast to the pk
 * column's type the resolver stamped; {@code asorDecodePkMap} = the
 * engine's {@code {pathToMapping, pkMap, setId}} JSON, {@code pk$_i} keys
 * renamed by the spelled setId → pk-column-names table
 * (resolver ObjectReferenceDecode).
 */
final class AsorReaders {

    private AsorReaders() {
    }

    static void register(Map<String, Scalars.Rule> RULES) {
        // decodes IN SQL — base64 (unpadded on the wire), then the framing
        // regex picks the segment. asorPkValue = the pk$_i value cast to the
        // pk column's type the resolver stamped; asorDecodePkMap = the
        // engine's {pathToMapping, pkMap, setId} JSON, pk$_i keys renamed by
        // the spelled setId -> pk-column-names table (ObjectReferenceDecode).
        for (String f : Pure.nativeKeysAt(Pure.Lite.ASOR_PK_VALUE)) {
            RULES.put(f, (n, args) -> {
                if (!(args.get(1) instanceof SqlExpr.IntLit idx)) {
                    throw new IllegalStateException("asorPkValue index must be literal");
                }
                SqlExpr pkJson = new SqlExpr.Cast(asorSegment(args.get(0), 6),
                        SqlType.Scalar.JSON);
                SqlExpr text = SqlExpr.Call.of(SqlFn.REGEXP_REPLACE,
                        new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.VARIANT_GET, pkJson,
                                new SqlExpr.StringLit("pk$_" + idx.value())),
                                SqlType.Scalar.VARCHAR),
                        new SqlExpr.StringLit("^\"|\"$"), new SqlExpr.StringLit(""),
                        new SqlExpr.StringLit("g"));
                return new SqlExpr.Cast(text, PureSql.type(n.info().type()));
            });
        }
        for (String f : Pure.nativeKeysAt(Pure.Lite.ASOR_DECODE_PK_MAP)) {
            RULES.put(f, (n, args) -> {
                if (!(args.get(1) instanceof SqlExpr.StringLit table)
                        || !(com.legend.sql.Json.parseOne(table.value())
                                instanceof java.util.Map<?, ?> sets)) {
                    throw new IllegalStateException("asorDecodePkMap needs a spelled"
                            + " setId -> pk-columns table");
                }
                SqlExpr mapping = asorSegment(args.get(0), 2);
                SqlExpr setId = asorSegment(args.get(0), 4);
                SqlExpr pkJson = new SqlExpr.Cast(asorSegment(args.get(0), 6),
                        SqlType.Scalar.JSON);
                List<SqlExpr.Case.When> whens = new ArrayList<>();
                for (var e : sets.entrySet()) {
                    List<SqlExpr> kv = new ArrayList<>();
                    int i = 0;
                    for (Object name : (List<?>) e.getValue()) {
                        kv.add(new SqlExpr.StringLit(String.valueOf(name)));
                        kv.add(SqlExpr.Call.of(SqlFn.VARIANT_GET, pkJson,
                                new SqlExpr.StringLit("pk$_" + i++)));
                    }
                    whens.add(new SqlExpr.Case.When(
                            SqlExpr.Call.of(SqlFn.EQUAL, setId,
                                    new SqlExpr.StringLit(String.valueOf(e.getKey()))),
                            new SqlExpr.JsonObject(kv)));
                }
                return new SqlExpr.Cast(new SqlExpr.JsonObject(List.of(
                        new SqlExpr.StringLit("pathToMapping"), mapping,
                        new SqlExpr.StringLit("pkMap"), new SqlExpr.Case(whens, null),
                        new SqlExpr.StringLit("setId"), setId)), SqlType.Scalar.VARCHAR);
            });
        }
    }

    /** Segment {@code k} (1-based) of an {@code ASOR:} reference: the
     * base64 payload after the marker, re-padded (the wire strips '='),
     * decoded, and cut by the framing regex — kind, defining mapping,
     * root set id, set id, connection JSON, pk JSON (AsorRef's layout). */
    private static SqlExpr asorSegment(SqlExpr ref, int k) {
        SqlExpr b64 = SqlExpr.Call.of(SqlFn.SUBSTRING, ref, new SqlExpr.IntLit(6));
        SqlExpr pad = SqlExpr.Call.of(SqlFn.MOD,
                SqlExpr.Call.of(SqlFn.MINUS, new SqlExpr.IntLit(4),
                        SqlExpr.Call.of(SqlFn.MOD,
                                SqlExpr.Call.of(SqlFn.LENGTH, b64), new SqlExpr.IntLit(4))),
                new SqlExpr.IntLit(4));
        SqlExpr decoded = SqlExpr.Call.of(SqlFn.DECODE_BASE64,
                SqlExpr.Call.of(SqlFn.CONCAT, b64,
                        SqlExpr.Call.of(SqlFn.REPEAT_STR, new SqlExpr.StringLit("="), pad)));
        String framing = "^001:010:\\d{10}:([^:]*):\\d{10}:(.*?):\\d{10}:([^:]*):\\d{10}:([^:]*)"
                + ":\\d{10}:(\\{.*\\}):\\d{10}:(\\{[^{}]*\\})$";
        return SqlExpr.Call.of(SqlFn.REGEXP_EXTRACT, decoded,
                new SqlExpr.StringLit(framing), new SqlExpr.IntLit(k));
    }
}
