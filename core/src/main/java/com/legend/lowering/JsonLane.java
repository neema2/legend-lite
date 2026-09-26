// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.builtin.Pure;
import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;

/**
 * The {@code meta::json} natives on the VARIANT lane (real json.pure /
 * jsonExtension.pure signatures, registered in {@link Pure}): parseJSON is
 * the JSON cast, getValue(obj, key) the member access, the compact text is
 * the JSON value's text, pretty is the dialect's printer. The tree READS
 * ({@code keyValuePairs}, {@code values}, {@code value}) are typed nodes
 * ({@code TypedJsonAccess}) lowered in {@code Lowerer.jsonAccess}.
 */
final class JsonLane {

    private JsonLane() {
    }

    static void register(java.util.Map<com.legend.model.FunctionId, Scalars.Rule> rules) {
        // fromJson(String): the string IS the variant — a JSON cast.
        for (com.legend.model.FunctionId f : Pure.AT_VARIANT_CONVERT_FROM_JSON) {
            rules.put(f, (n, args) -> new SqlExpr.Cast(args.get(0),
                    SqlType.Scalar.JSON));
        }
        // parseJSON carries the engine parser's ONE tolerance the assert
        // channel already mirrors (Json.parseOne lenient: corpus goldens
        // with a MISSING COMMA between array object elements —
        // testSubTypeAtRootLevelWithInheritanceMapping's expected text):
        // '}{' reads as '},{' before the strict JSON cast.
        for (com.legend.model.FunctionId f : Pure.AT_JSON_PARSE_JSON) {
            rules.put(f, (n, args) -> new SqlExpr.Cast(
                    SqlExpr.Call.of(SqlFn.REGEXP_REPLACE, args.get(0),
                            new SqlExpr.StringLit("\\}\\s*\\{"),
                            new SqlExpr.StringLit("},{"),
                            new SqlExpr.StringLit("g")),
                    SqlType.Scalar.JSON));
        }
        for (com.legend.model.FunctionId f : Pure.AT_JSON_GET_VALUE) {
            rules.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.VARIANT_GET,
                    args.get(0), args.get(1)));
        }
        for (com.legend.model.FunctionId f : Pure.AT_JSON_TO_COMPACT_JSONSTRING) {
            rules.put(f, (n, args) -> new SqlExpr.Cast(args.get(0),
                    PureSql.type(Type.Primitive.STRING)));
        }
        for (com.legend.model.FunctionId f : Pure.AT_JSON_TO_PRETTY_JSONSTRING) {
            rules.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.JSON_PRETTY,
                    args.get(0)));
        }
    }
}
