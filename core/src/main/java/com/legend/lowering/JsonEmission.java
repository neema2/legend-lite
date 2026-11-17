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
                : result(lw, (TypedJsonResult) n, columns);
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
     * The RESULT JSON of a TDS/class/serialized-rooted query (the string entry's value,
     * {@link com.legend.compiler.spec.typed.TypedJsonResult}): ONE scalar subquery over the chain's rows. Its TDS, CLASS
     * and VALUES kinds are legend-engine's envelopes, composed by the plan style's {@link ResultEnvelopes} facet (the
     * edge's) from the product's parts and refused by the product's at lowering ({@link #refused}); TDS_JSON and
     * TDS_JSON_KV are legend-pure's toJSON documents, the product's.
     */
    /** THE PRODUCT'S REFUSAL of legend-engine's result envelope (Phase 6's B5, 2026-11-17, the orchestrator's ruling):
     *  at lowering, by name, pointing to the product's result writers. */
    static com.legend.error.ModelException refused() {
        return new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.LOWER,
                "executeLegendQuery's result envelope is legend-engine's JSON, the compatibility edge's: the product"
                        + " writes a query's rows by its result writers (JSON, CSV, Arrow)");
    }

    static SqlExpr result(Lowerer lw, TypedJsonResult jr, ColumnResolver columns) {
        String alias = "_lq";   // the wrapper select's own scope — no collision possible
        boolean upstream = jr.kind() == TypedJsonResult.Kind.TDS || jr.kind() == TypedJsonResult.Kind.CLASS
                || jr.kind() == TypedJsonResult.Kind.VALUES;
        ResultEnvelopes envelopes = upstream ? lw.resultEnvelopes(JsonEmission::refused) : null;
        if (jr.kind() == TypedJsonResult.Kind.VALUES) {
            return Objects.requireNonNull(envelopes, "the envelope facet").values(lw.scalar(jr.chain(), columns));
        }
        SqlQuery inner;
        SqlExpr envelope;
        if (jr.kind() == TypedJsonResult.Kind.CLASS) {
            // a class root resolves to the GRAPH emission (one row, the
            // objects array as JSON text): the objects ARE that array
            inner = lw.lower(jr.chain());
            String col = inner.outputs().get(0).name();
            envelope = Objects.requireNonNull(envelopes, "the envelope facet").classes(
                    new SqlExpr.Cast(SqlExpr.Column.derived(alias, col), SqlType.Scalar.JSON, SqlExpr.Reason.CARRIER),
                    jr.sql());
        } else {
            SqlSelect rel = lw.relation(jr.chain());
            inner = rel;
            List<String> names = rel.outputs().stream().map(o -> o.name()).toList();
            Type.RelationType schema = Type.relationSchema(jr.chain().info().type());
            List<SqlExpr> colMeta = new ArrayList<>();
            List<String> typeNames = new ArrayList<>();
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
                typeNames.add(typeName);
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
                // legend-engine's TDS result: the edge's envelope (ResultEnvelopes.tds)
                envelope = Objects.requireNonNull(envelopes, "the envelope facet").tds(names, typeNames, cells, jr.sql());
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
