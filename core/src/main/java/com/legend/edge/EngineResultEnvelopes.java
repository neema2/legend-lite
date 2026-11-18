// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;

import java.util.List;

/**
 * legend-engine's RESULT ENVELOPES (Phase 6's B5, 2026-11-17: moved here from the product's lowering,
 * {@code lowering.JsonEmission}), answering the plan style's {@link com.legend.lowering.ResultEnvelopes} facet. A
 * relation's is legend-engine's bytes as its {@code RelationalResultToJsonDefaultSerializer} streams them: {@code
 * {"builder": } + the tdsBuilder (compact) + {@code , "activities": } + the activities + {@code , "result" :
 * {"columns" : } + the names + {@code , "rows" : [} + each row {@code {"values": [...]}}, comma-joined, + {@code ]}}};
 * the compact pieces are the database's compact JSON, the skeleton's separators the engine's own. A class root's is
 * the classBuilder envelope with its {@code objects}; a serialized root's the json builder around its {@code values}.
 */
final class EngineResultEnvelopes implements com.legend.lowering.ResultEnvelopes {

    static final EngineResultEnvelopes INSTANCE = new EngineResultEnvelopes();

    private EngineResultEnvelopes() {
    }

    /** A JSON value as its compact text (the database's own spelling). */
    private static SqlExpr text(SqlExpr json) {
        return new SqlExpr.Cast(json, SqlType.Scalar.VARCHAR, SqlExpr.Reason.CARRIER);
    }

    /** The activities array: the one relational activity's SQL, or empty where none was recorded. */
    private static SqlExpr activities(@com.legend.base.Nullable String sql) {
        return sql == null
                ? new SqlExpr.JsonArray(List.of())
                : new SqlExpr.JsonArray(List.of(new SqlExpr.JsonObject(List.of(
                        new SqlExpr.StringLit("_type"), new SqlExpr.StringLit("relational"),
                        new SqlExpr.StringLit("sql"), new SqlExpr.StringLit(sql)))));
    }

    @Override
    public SqlExpr tds(List<String> names, List<String> typeNames, List<SqlExpr> cells,
            @com.legend.base.Nullable String activitySql) {
        List<SqlExpr> colMeta = new java.util.ArrayList<>();
        List<SqlExpr> colNames = new java.util.ArrayList<>();
        for (int i = 0; i < names.size(); i++) {
            colMeta.add(new SqlExpr.JsonObject(List.of(
                    new SqlExpr.StringLit("name"), new SqlExpr.StringLit(names.get(i)),
                    new SqlExpr.StringLit("type"), new SqlExpr.StringLit(typeNames.get(i)))));
            colNames.add(new SqlExpr.StringLit(names.get(i)));
        }
        SqlExpr builder = new SqlExpr.JsonObject(List.of(
                new SqlExpr.StringLit("_type"), new SqlExpr.StringLit("tdsBuilder"),
                new SqlExpr.StringLit("columns"), new SqlExpr.JsonArray(colMeta)));
        SqlExpr rowText = new SqlExpr.Call(SqlFn.CONCAT, List.of(
                new SqlExpr.StringLit("{\"values\": "),
                text(new SqlExpr.JsonArray(cells)),
                new SqlExpr.StringLit("}")));
        SqlExpr rowsText = new SqlExpr.Call(SqlFn.COALESCE, List.of(
                new com.legend.sql.SqlAgg.Reducer(com.legend.sql.SqlAgg.Fn.STRING_AGG,
                        List.of(rowText, new SqlExpr.StringLit(",")), false, List.of()),
                new SqlExpr.StringLit("")));
        return new SqlExpr.Call(SqlFn.CONCAT, List.of(
                new SqlExpr.StringLit("{\"builder\": "), text(builder),
                new SqlExpr.StringLit(", \"activities\": "), text(activities(activitySql)),
                new SqlExpr.StringLit(", \"result\" : {\"columns\" : "),
                text(new SqlExpr.JsonArray(colNames)),
                new SqlExpr.StringLit(", \"rows\" : ["), rowsText,
                new SqlExpr.StringLit("]}}")));
    }

    @Override
    public SqlExpr classes(SqlExpr objects, @com.legend.base.Nullable String activitySql) {
        return new SqlExpr.JsonObject(List.of(
                new SqlExpr.StringLit("builder"),
                new SqlExpr.JsonObject(List.of(new SqlExpr.StringLit("_type"), new SqlExpr.StringLit("classBuilder"))),
                new SqlExpr.StringLit("activities"), activities(activitySql),
                new SqlExpr.StringLit("objects"), objects));
    }
}
