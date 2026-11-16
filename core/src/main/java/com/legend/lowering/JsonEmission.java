// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedJsonAccess;
import com.legend.compiler.spec.typed.TypedJsonResult;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.lowering.Resolvers.ColumnResolver;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The JSON emissions of the string entry and the {@code meta::json}
 * tree reads — {@link TypedJsonAccess} navigation on the variant lane
 * and the {@link TypedJsonResult} result envelope (the seam split from
 * {@link Lowerer}: the owner of these two node kinds).
 */
final class JsonEmission {

    private JsonEmission() {
    }

    /** The two node kinds this owner lowers. */
    static boolean owns(TypedSpec n) {
        return n instanceof TypedJsonAccess || n instanceof TypedJsonResult;
    }

    static SqlExpr lower(Lowerer lw, TypedSpec n, ColumnResolver columns) {
        return n instanceof TypedJsonAccess ja ? access(lw, ja, columns)
                : result(lw, (TypedJsonResult) n);
    }

    /** meta::json navigation on the variant lane (JsonChecker's
     * emissions): a member is (json -> 'key'), all members (json -> '$.*')
     * as a list, an array's elements as a list, a scalar element's text /
     * number / boolean through the '$' extraction (DuckDB renders the
     * text form ->>). */
    static SqlExpr access(Lowerer lw, TypedJsonAccess ja, ColumnResolver columns) {
        SqlExpr src = lw.scalar(ja.source(), columns);
        return switch (ja.op()) {
            case MEMBER -> SqlExpr.Call.of(SqlFn.VARIANT_GET, src,
                    lw.scalar(Objects.requireNonNull(ja.key(), "member key"), columns));
            case MEMBERS -> SqlExpr.Call.of(SqlFn.VARIANT_ELEMENTS,
                    SqlExpr.Call.of(SqlFn.VARIANT_GET, src,
                            new SqlExpr.StringLit("$.*")));
            case ELEMENTS -> SqlExpr.Call.of(SqlFn.VARIANT_ELEMENTS, src);
            case TEXT -> new SqlExpr.Cast(
                    SqlExpr.Call.of(SqlFn.VARIANT_GET, src, new SqlExpr.StringLit("$")),
                    PureSql.type(Type.Primitive.STRING), SqlExpr.Reason.CARRIER);
            case NUMBER -> new SqlExpr.Cast(
                    SqlExpr.Call.of(SqlFn.VARIANT_GET, src, new SqlExpr.StringLit("$")),
                    PureSql.type(Type.Primitive.NUMBER), SqlExpr.Reason.CARRIER);
            case BOOLEAN -> new SqlExpr.Cast(
                    SqlExpr.Call.of(SqlFn.VARIANT_GET, src, new SqlExpr.StringLit("$")),
                    PureSql.type(Type.Primitive.BOOLEAN), SqlExpr.Reason.CARRIER);
            case IDENTITY -> src;
        };
    }

    /**
     * The engine's RESULT JSON of a TDS/class-rooted query (the string
     * entry's value — {@link com.legend.compiler.spec.typed.TypedJsonResult}):
     * ONE scalar subquery over the chain's rows. TDS: {@code {"builder":
     * {"_type":"tdsBuilder","columns":[{"name","type"}…]},"activities":
     * [{"_type":"relational","sql":…}],"result":{"columns":[…],"rows":
     * [{"values":[…]}…]}}} — row cells wrap as variants so each keeps its
     * JSON kind (strings quoted, numbers bare); rows aggregate in the
     * chain's own order. Class: the classBuilder envelope with the rows
     * as {@code objects}. An absent activity text is the empty array.
     */
    /** A JSON value as its compact text (the database's own spelling). */
    private static SqlExpr text(SqlExpr json) {
        return new SqlExpr.Cast(json, PureSql.type(Type.Primitive.STRING), SqlExpr.Reason.CARRIER);
    }

    static SqlExpr result(Lowerer lw, TypedJsonResult jr) {
        String alias = "_lq";   // the wrapper select's own scope — no collision possible
        SqlExpr activities = jr.sql() == null
                ? new SqlExpr.JsonArray(List.of())
                : new SqlExpr.JsonArray(List.of(
                        new SqlExpr.JsonObject(List.of(
                                new SqlExpr.StringLit("_type"),
                                new SqlExpr.StringLit("relational"),
                                new SqlExpr.StringLit("sql"),
                                new SqlExpr.StringLit(jr.sql())))));
        SqlQuery inner;
        SqlExpr envelope;
        if (jr.kind() == TypedJsonResult.Kind.CLASS) {
            // a class root resolves to the GRAPH emission (one row, the
            // objects array as JSON text): the objects ARE that array
            inner = lw.lower(jr.chain());
            String col = inner.outputs().get(0).name();
            SqlExpr builder = new SqlExpr.JsonObject(List.of(
                    new SqlExpr.StringLit("_type"), new SqlExpr.StringLit("classBuilder")));
            envelope = new SqlExpr.JsonObject(List.of(
                    new SqlExpr.StringLit("builder"), builder,
                    new SqlExpr.StringLit("activities"), activities,
                    new SqlExpr.StringLit("objects"),
                    new SqlExpr.Cast(SqlExpr.Column.derived(alias, col),
                            SqlType.Scalar.JSON, SqlExpr.Reason.CARRIER)));
        } else {
            SqlSelect rel = lw.relation(jr.chain());
            inner = rel;
            List<String> names = rel.outputs().stream().map(o -> o.name()).toList();
            Type.RelationType schema = Type.relationSchema(jr.chain().info().type());
            List<SqlExpr> colMeta = new ArrayList<>();
            List<SqlExpr> colNames = new ArrayList<>();
            List<SqlExpr> cells = new ArrayList<>();
            boolean bareTdsJson = jr.kind() == TypedJsonResult.Kind.TDS_JSON;
            boolean keyValue = jr.kind() == TypedJsonResult.Kind.TDS_JSON_KV;
            List<SqlExpr> rowObject = new ArrayList<>();
            for (String name : names) {
                String typeName = "";
                String metaType = "";
                Type.Column col = schema == null ? null
                        : schema.columns().stream()
                                .filter(c -> c.name().equals(name)).findFirst()
                                .orElse(null);
                if (col != null) {
                    String tn = col.type().typeName();
                    int cut = tn.lastIndexOf("::");
                    typeName = cut < 0 ? tn : tn.substring(cut + 2);
                    // toJSON.pure: PrimitiveType / Enumeration / InvalidType
                    metaType = col.type() instanceof Type.Primitive ? "PrimitiveType"
                            : col.type() instanceof Type.EnumType ? "Enumeration"
                            : "InvalidType";
                }
                List<SqlExpr> meta = new ArrayList<>(List.of(
                        new SqlExpr.StringLit("name"), new SqlExpr.StringLit(name),
                        new SqlExpr.StringLit("type"), new SqlExpr.StringLit(typeName)));
                if (bareTdsJson) {
                    meta.add(new SqlExpr.StringLit("metaType"));
                    meta.add(new SqlExpr.StringLit(metaType));
                }
                colMeta.add(new SqlExpr.JsonObject(meta));
                colNames.add(new SqlExpr.StringLit(name));
                cells.add(SqlExpr.Column.derived(alias, name));
                rowObject.add(new SqlExpr.StringLit(name));
                rowObject.add(SqlExpr.Column.derived(alias, name));
            }

            SqlExpr rows = new SqlExpr.JsonArrayAgg(new SqlExpr.JsonObject(List.of(
                    new SqlExpr.StringLit("values"), new SqlExpr.JsonArray(cells))));
            if (keyValue) {
                // tdsRowToJSONKeyValueObject: one object per row keyed by
                // column name — numbers/booleans bare, strings/dates quoted,
                // a NULL cell null (the JSON carrier's own rendering); an
                // empty relation is '[]'
                envelope = new SqlExpr.Call(SqlFn.COALESCE, List.of(
                        new SqlExpr.JsonArrayAgg(new SqlExpr.JsonObject(rowObject)),
                        new SqlExpr.Cast(new SqlExpr.StringLit("[]"), SqlType.Scalar.JSON, SqlExpr.Reason.CARRIER)));
            } else if (bareTdsJson) {
                // the bare toJSON(tds) document: column metadata + rows
                envelope = new SqlExpr.JsonObject(List.of(
                        new SqlExpr.StringLit("columns"), new SqlExpr.JsonArray(colMeta),
                        new SqlExpr.StringLit("rows"), rows));
            } else {
                // THE ENGINE'S BYTES (RelationalResultToJsonDefaultSerializer, a
                // hand-written stream): `{"builder": ` + builder (Jackson compact)
                // + `, "activities": [` + activities (compact, comma-joined) +
                // `], "result" : {"columns" : [` + names (compact) + `], "rows" : [`
                // + rows, each `{"values": [` + cells (comma-joined) + `]}`,
                // comma-joined + `]}}`. The compact pieces ARE the database's
                // compact JSON; the skeleton's separators are the engine's own —
                // the result string a test reads (`$json->contains('"result" :
                // {…')`) is the engine's, byte for byte, as ordinary compiled Pure.
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
                envelope = new SqlExpr.Call(SqlFn.CONCAT, List.of(
                        new SqlExpr.StringLit("{\"builder\": "), text(builder),
                        new SqlExpr.StringLit(", \"activities\": "), text(activities),
                        new SqlExpr.StringLit(", \"result\" : {\"columns\" : "),
                        text(new SqlExpr.JsonArray(colNames)),
                        new SqlExpr.StringLit(", \"rows\" : ["), rowsText,
                        new SqlExpr.StringLit("]}}")));
            }
        }
        SqlSelect agg = new SqlSelect(
                List.of(new SqlSelect.Projection(
                        new SqlExpr.Cast(envelope, PureSql.type(Type.Primitive.STRING), SqlExpr.Reason.CARRIER),
                        "value", null)),
                false, new SqlSource.Subselect(inner, alias, null), null, List.of(),
                null, null, List.of(), null, null, List.of());
        return new SqlExpr.ScalarSubquery(agg);
    }

}
