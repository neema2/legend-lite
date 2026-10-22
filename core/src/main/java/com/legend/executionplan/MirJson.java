// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.executionplan;

import com.legend.json.Json;
import com.legend.sql.DateFmt;
import com.legend.sql.DatePart;
import com.legend.sql.JsonKind;
import com.legend.sql.NameQuoting;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlDdl;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.SqlUnion;
import com.legend.sql.SqlWith;
import com.legend.sql.TypeFact;
import com.legend.sql.ValueKind;
import com.legend.sql.ValueTyping;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE TYPED SQL TREE AS JSON (docs/PLAN_PATH_PROGRAM_2026_10_17.md §3, step 1b): the MIR -- every {@link SqlQuery},
 * {@link SqlSource}, {@link SqlExpr}, {@link SqlAgg}, {@link SqlType}, {@link TypeFact}, {@link DateFmt} and the
 * declared column types a stored read carries -- written as JSON and read back, so a plan read from its JSON is as
 * typed as the one the planner made. Every variant is its own {@code _type}, an enum constant the writer spells and
 * the reader switches over exhaustively; the writer's switches run over the sealed interfaces, so a new MIR variant
 * fails to compile here until it has its tag. Numbers travel losslessly: a long as a JSON integer, a double and a
 * BigDecimal as their own text. An unknown tag is refused by name.
 */
public final class MirJson {
    private MirJson() {
    }

    enum QueryTag { select, union, with }

    enum SourceTag { table, subselect, cte, join, pivot, values, dual, sourceUrl, rawSql, catalog, varSet }

    enum ExprTag {
        rowOrder, storedRead, reduceCollection, membership, column, starExcept, star, stringLit, intLit, floatLit,
        decimalLit, boolLit, nullLit, dateLit, group, planParam, formatLit, datePartLit, jsonKindLit, typeNameLit,
        timestampLit, timeLit, orderedListAgg, arrayLit, structLit, structGet, call, caseExpr, exists, inSubquery,
        checkedDefects, checkedChildValue, quantified, scalarSubquery, checkedOne, compactList, deferredTdsString,
        tempTableInSplice, jsonObject, jsonArray, jsonArrayAgg, windowCall, lambda, cast, foldCall, reducer, keyCell
    }

    enum AggTag { reducer, rankingFn, valueFn }

    enum TypeTag { scalar, decimal, array, map, struct }

    enum FactTag { typed, bottom, raises, unknown }

    enum BoundTag { unboundedPreceding, preceding, currentRow, following, unboundedFollowing, intervalPreceding,
        intervalFollowing }

    enum FmtTag { part, text }

    enum ColumnTypeTag { plain, sized, scaled }

    // ---------------------------------------------------------------- writing

    public static Map<String, Object> query(SqlQuery q) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (q) {
            case SqlSelect s -> {
                o.put("_type", QueryTag.select.name());
                List<Object> projections = new ArrayList<>();
                for (SqlSelect.Projection p : s.projections()) {
                    projections.add(projection(p));
                }
                o.put("projections", projections);
                o.put("distinct", s.distinct());
                o.put("from", source(s.from()));
                o.put("where", s.where() == null ? null : expr(s.where()));
                o.put("groupBy", exprs(s.groupBy()));
                o.put("having", s.having() == null ? null : expr(s.having()));
                o.put("qualify", s.qualify() == null ? null : expr(s.qualify()));
                o.put("orderBy", sortKeys(s.orderBy()));
                o.put("limit", s.limit());
                o.put("offset", s.offset());
                o.put("outputs", outputCols(s.outputs()));
            }
            case SqlUnion u -> {
                o.put("_type", QueryTag.union.name());
                List<Object> branches = new ArrayList<>();
                for (SqlQuery b : u.branches()) {
                    branches.add(query(b));
                }
                o.put("branches", branches);
                o.put("all", u.all());
                o.put("outputs", outputCols(u.outputs()));
            }
            case SqlWith w -> {
                o.put("_type", QueryTag.with.name());
                List<Object> ctes = new ArrayList<>();
                for (SqlWith.Cte c : w.ctes()) {
                    Map<String, Object> co = new LinkedHashMap<>();
                    co.put("name", c.name());
                    co.put("query", query(c.query()));
                    co.put("materialized", c.materialized());
                    ctes.add(co);
                }
                o.put("ctes", ctes);
                o.put("body", query(w.body()));
                o.put("recursive", w.recursive());
            }
        }
        return o;
    }

    private static Map<String, Object> projection(SqlSelect.Projection p) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("expr", expr(p.expr()));
        o.put("alias", p.alias());
        o.put("out", p.out() == null ? null : outputCol(p.out()));
        return o;
    }

    private static List<Object> sortKeys(List<SqlSelect.SortKey> keys) {
        List<Object> out = new ArrayList<>();
        for (SqlSelect.SortKey k : keys) {
            Map<String, Object> o = new LinkedHashMap<>();
            o.put("expr", expr(k.expr()));
            o.put("ascending", k.ascending());
            o.put("nullOrder", k.nullOrder() == null ? null : k.nullOrder().name());
            o.put("outputName", k.outputName());
            out.add(o);
        }
        return out;
    }

    private static List<Object> outputCols(List<OutputCol> cols) {
        List<Object> out = new ArrayList<>();
        for (OutputCol c : cols) {
            out.add(outputCol(c));
        }
        return out;
    }

    private static Map<String, Object> outputCol(OutputCol c) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("name", c.name());
        o.put("type", type(c.type()));
        o.put("nullable", c.nullable());
        o.put("origin", c.origin() == null ? null : c.origin().name());
        return o;
    }

    public static Map<String, Object> source(SqlSource src) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (src) {
            case SqlSource.Table t -> {
                o.put("_type", SourceTag.table.name());
                o.put("name", t.name());
                o.put("alias", t.alias());
                o.put("outputs", outputCols(t.outputs()));
                o.put("call", t.call());
                Map<String, Object> stored = new LinkedHashMap<>();
                for (Map.Entry<String, SqlDdl.ColumnType> e : t.storedTypes().entrySet()) {
                    stored.put(e.getKey(), columnType(e.getValue()));
                }
                o.put("storedTypes", stored);
                o.put("quoting", quoting(t.quoting()));
            }
            case SqlSource.Subselect s -> {
                o.put("_type", SourceTag.subselect.name());
                o.put("inner", query(s.inner()));
                o.put("alias", s.alias());
                o.put("frameName", s.frameName());
            }
            case SqlSource.Cte c -> {
                o.put("_type", SourceTag.cte.name());
                o.put("name", c.name());
                o.put("alias", c.alias());
                o.put("outputs", outputCols(c.outputs()));
            }
            case SqlSource.Join j -> {
                o.put("_type", SourceTag.join.name());
                o.put("left", source(j.left()));
                o.put("right", source(j.right()));
                o.put("kind", j.kind().name());
                o.put("on", j.on() == null ? null : expr(j.on()));
                o.put("outputs", outputCols(j.outputs()));
            }
            case SqlSource.Pivot p -> {
                o.put("_type", SourceTag.pivot.name());
                o.put("source", source(p.source()));
                o.put("on", exprs(p.on()));
                o.put("in", exprs(p.in()));
                o.put("named", exprs(p.named()));
                List<Object> usings = new ArrayList<>();
                for (SqlSource.Pivot.Using u : p.usings()) {
                    Map<String, Object> uo = new LinkedHashMap<>();
                    uo.put("agg", agg(u.agg()));
                    uo.put("alias", u.alias());
                    uo.put("type", u.type() == null ? null : type(u.type()));
                    usings.add(uo);
                }
                o.put("usings", usings);
                o.put("alias", p.alias());
                o.put("outputs", outputCols(p.outputs()));
            }
            case SqlSource.Values v -> {
                o.put("_type", SourceTag.values.name());
                List<Object> rows = new ArrayList<>();
                for (List<SqlExpr> row : v.rows()) {
                    rows.add(exprs(row));
                }
                o.put("rows", rows);
                o.put("columns", new ArrayList<Object>(v.columns()));
                o.put("alias", v.alias());
                o.put("outputs", outputCols(v.outputs()));
            }
            case SqlSource.Dual d -> o.put("_type", SourceTag.dual.name());
            case SqlSource.SourceUrl u -> {
                o.put("_type", SourceTag.sourceUrl.name());
                o.put("url", u.url());
                o.put("alias", u.alias());
                o.put("outputs", outputCols(u.outputs()));
            }
            case SqlSource.RawSql r -> {
                o.put("_type", SourceTag.rawSql.name());
                o.put("sql", r.sql());
                o.put("alias", r.alias());
                o.put("outputs", outputCols(r.outputs()));
                o.put("writtenFor", r.writtenFor());
            }
            case SqlSource.Catalog c -> {
                o.put("_type", SourceTag.catalog.name());
                o.put("grid", c.grid().name());
                o.put("schemaPattern", c.schemaPattern());
                o.put("tablePattern", c.tablePattern());
                o.put("columnPattern", c.columnPattern());
                o.put("alias", c.alias());
                o.put("outputs", outputCols(c.outputs()));
            }
            case SqlSource.VarSetPlaceholder v -> {
                o.put("_type", SourceTag.varSet.name());
                o.put("varName", v.varName());
                o.put("alias", v.alias());
                o.put("outputs", outputCols(v.outputs()));
            }
        }
        return o;
    }

    private static Map<String, Object> quoting(NameQuoting q) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("schema", q.schema());
        o.put("table", q.table());
        return o;
    }

    private static List<Object> exprs(List<SqlExpr> es) {
        List<Object> out = new ArrayList<>();
        for (SqlExpr e : es) {
            out.add(expr(e));
        }
        return out;
    }

    public static Map<String, Object> expr(SqlExpr e) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (e) {
            case SqlExpr.RowOrder r -> {
                tag(o, ExprTag.rowOrder, r.type());
                o.put("table", r.table());
            }
            case SqlExpr.StoredRead r -> {
                tag(o, ExprTag.storedRead, r.type());
                o.put("column", expr(r.column()));
                o.put("stored", columnType(r.stored()));
            }
            case SqlExpr.ReduceCollection r -> {
                tag(o, ExprTag.reduceCollection, r.type());
                o.put("reducer", r.reducer().name());
                o.put("collection", expr(r.collection()));
                o.put("extras", exprs(r.extras()));
            }
            case SqlExpr.Membership m -> {
                tag(o, ExprTag.membership, m.type());
                o.put("needle", expr(m.needle()));
                o.put("collection", expr(m.collection()));
            }
            case SqlExpr.Column c -> {
                tag(o, ExprTag.column, c.type());
                o.put("table", c.table());
                o.put("name", c.name());
                o.put("origin", c.origin() == null ? null : c.origin().name());
            }
            case SqlExpr.StarExcept s -> {
                tag(o, ExprTag.starExcept, s.type());
                o.put("table", s.table());
                o.put("except", new ArrayList<Object>(s.except()));
            }
            case SqlExpr.Star s -> {
                tag(o, ExprTag.star, s.type());
                o.put("table", s.table());
            }
            case SqlExpr.StringLit l -> {
                tag(o, ExprTag.stringLit, l.type());
                o.put("value", l.value());
            }
            case SqlExpr.IntLit l -> {
                tag(o, ExprTag.intLit, l.type());
                o.put("value", l.value());
            }
            case SqlExpr.FloatLit l -> {
                tag(o, ExprTag.floatLit, l.type());
                o.put("value", Double.toString(l.value()));
            }
            case SqlExpr.DecimalLit l -> {
                tag(o, ExprTag.decimalLit, l.type());
                o.put("value", l.value().toString());
            }
            case SqlExpr.BoolLit l -> {
                tag(o, ExprTag.boolLit, l.type());
                o.put("value", l.value());
            }
            case SqlExpr.NullLit l -> tag(o, ExprTag.nullLit, l.type());
            case SqlExpr.DateLit l -> {
                tag(o, ExprTag.dateLit, l.type());
                o.put("iso", l.iso());
            }
            case SqlExpr.Group g -> {
                tag(o, ExprTag.group, g.type());
                o.put("inner", expr(g.inner()));
            }
            case SqlExpr.PlanParam p -> planParam(o, p);
            case SqlExpr.KeyCell k -> {
                tag(o, ExprTag.keyCell, k.type());
                o.put("cell", k.cell());
            }
            case SqlExpr.FormatLit f -> formatLit(o, f);
            case SqlExpr.DatePartLit d -> {
                tag(o, ExprTag.datePartLit, d.type());
                o.put("part", d.part().name());
            }
            case SqlExpr.JsonKindLit j -> {
                tag(o, ExprTag.jsonKindLit, j.type());
                o.put("kind", j.kind().name());
            }
            case SqlExpr.TypeNameLit t -> {
                tag(o, ExprTag.typeNameLit, t.type());
                o.put("scalar", t.scalar().name());
            }
            case SqlExpr.TimestampLit l -> {
                tag(o, ExprTag.timestampLit, l.type());
                o.put("iso", l.iso());
            }
            case SqlExpr.TimeLit l -> {
                tag(o, ExprTag.timeLit, l.type());
                o.put("iso", l.iso());
            }
            case SqlExpr.OrderedListAgg a -> {
                tag(o, ExprTag.orderedListAgg, a.type());
                o.put("value", expr(a.value()));
                o.put("orderBy", expr(a.orderBy()));
            }
            case SqlExpr.ArrayLit a -> {
                tag(o, ExprTag.arrayLit, a.type());
                o.put("elements", exprs(a.elements()));
            }
            case SqlExpr.StructLit s -> structLit(o, s);
            case SqlExpr.StructGet g -> {
                tag(o, ExprTag.structGet, g.type());
                o.put("source", expr(g.source()));
                o.put("field", g.field());
            }
            case SqlExpr.Call c -> {
                tag(o, ExprTag.call, c.type());
                o.put("fn", c.fn().name());
                o.put("args", exprs(c.args()));
            }
            case SqlExpr.Case c -> caseExpr(o, c);
            case SqlExpr.Exists x -> {
                tag(o, ExprTag.exists, x.type());
                o.put("subquery", query(x.subquery()));
            }
            case SqlExpr.InSubquery i -> {
                tag(o, ExprTag.inSubquery, i.type());
                o.put("value", expr(i.value()));
                o.put("subquery", query(i.subquery()));
            }
            case SqlExpr.CheckedDefects d -> checkedDefects(o, d);
            case SqlExpr.CheckedChildValue v -> {
                tag(o, ExprTag.checkedChildValue, v.type());
                o.put("envelope", expr(v.envelope()));
                o.put("toMany", v.toMany());
            }
            case SqlExpr.Quantified q -> {
                tag(o, ExprTag.quantified, q.type());
                o.put("value", expr(q.value()));
                o.put("comparison", q.comparison().name());
                o.put("quantifier", q.quantifier().name());
                o.put("subquery", query(q.subquery()));
            }
            case SqlExpr.ScalarSubquery s -> {
                tag(o, ExprTag.scalarSubquery, s.type());
                o.put("subquery", query(s.subquery()));
            }
            case SqlExpr.CheckedOne c -> {
                tag(o, ExprTag.checkedOne, c.type());
                o.put("list", expr(c.list()));
                o.put("scalarCarrier", c.scalarCarrier());
                o.put("atLeastOnly", c.atLeastOnly());
            }
            case SqlExpr.CompactList c -> {
                tag(o, ExprTag.compactList, c.type());
                o.put("list", expr(c.list()));
            }
            case SqlExpr.DeferredTdsString d -> {
                tag(o, ExprTag.deferredTdsString, d.type());
                o.put("inner", query(d.inner()));
                o.put("alias", d.alias());
                o.put("id", d.id());
                o.put("form", d.form().name());
                o.put("renderTdsNull", d.renderTdsNull());
            }
            case SqlExpr.TempTableInSplice t -> {
                tag(o, ExprTag.tempTableInSplice, t.type());
                o.put("tempTableName", t.tempTableName());
            }
            case SqlExpr.JsonObject j -> {
                tag(o, ExprTag.jsonObject, j.type());
                o.put("kv", exprs(j.kv()));
            }
            case SqlExpr.JsonArray j -> {
                tag(o, ExprTag.jsonArray, j.type());
                o.put("elements", exprs(j.elements()));
            }
            case SqlExpr.JsonArrayAgg j -> jsonArrayAgg(o, j);
            case SqlExpr.WindowCall w -> windowCall(o, w);
            case SqlExpr.Lambda l -> {
                tag(o, ExprTag.lambda, l.type());
                o.put("params", new ArrayList<Object>(l.params()));
                o.put("body", expr(l.body()));
            }
            case SqlExpr.Cast c -> {
                tag(o, ExprTag.cast, c.type());
                o.put("value", expr(c.value()));
                o.put("target", type(c.target()));
                o.put("conform", c.conform());
            }
            case SqlExpr.FoldCall f -> {
                tag(o, ExprTag.foldCall, f.type());
                o.put("source", expr(f.source()));
                o.put("lambda", expr(f.lambda()));
                o.put("init", expr(f.init()));
                o.put("accIsList", f.accIsList());
                o.put("homogeneous", f.homogeneous());
            }
            case SqlAgg.Reducer r -> {
                tag(o, ExprTag.reducer, r.type());
                reducerFields(o, r);
            }
        }
        return o;
    }

    private static void planParam(Map<String, Object> o, SqlExpr.PlanParam p) {
        tag(o, ExprTag.planParam, p.type());
        o.put("name", p.name());
        o.put("kind", p.kind().name());
        o.put("optional", p.optional());
        o.put("enumMapFn", p.enumMapFn());
        if (p.byValue() == null) {
            o.put("byValue", null);
        } else {
            Map<String, Object> v = new LinkedHashMap<>();
            List<Object> kinds = new ArrayList<>();
            for (ValueKind k : p.byValue().kinds()) {
                kinds.add(k.name());
            }
            v.put("kinds", kinds);
            v.put("absent", p.byValue().absent().name());
            o.put("byValue", v);
        }
    }

    private static void formatLit(Map<String, Object> o, SqlExpr.FormatLit f) {
        tag(o, ExprTag.formatLit, f.type());
        List<Object> parts = new ArrayList<>();
        for (DateFmt part : f.parts()) {
            parts.add(fmt(part));
        }
        o.put("parts", parts);
    }

    private static void structLit(Map<String, Object> o, SqlExpr.StructLit s) {
        tag(o, ExprTag.structLit, s.type());
        List<Object> fields = new ArrayList<>();
        for (SqlExpr.StructLit.Field f : s.fields()) {
            Map<String, Object> fo = new LinkedHashMap<>();
            fo.put("name", f.name());
            fo.put("value", expr(f.value()));
            fo.put("declared", f.declared() == null ? null : type(f.declared()));
            fields.add(fo);
        }
        o.put("fields", fields);
    }

    private static void caseExpr(Map<String, Object> o, SqlExpr.Case c) {
        tag(o, ExprTag.caseExpr, c.type());
        List<Object> whens = new ArrayList<>();
        for (SqlExpr.Case.When w : c.whens()) {
            Map<String, Object> wo = new LinkedHashMap<>();
            wo.put("condition", expr(w.condition()));
            wo.put("then", expr(w.then()));
            whens.add(wo);
        }
        o.put("whens", whens);
        o.put("otherwise", c.otherwise() == null ? null : expr(c.otherwise()));
    }

    private static void checkedDefects(Map<String, Object> o, SqlExpr.CheckedDefects d) {
        tag(o, ExprTag.checkedDefects, d.type());
        o.put("own", expr(d.own()));
        List<Object> hoists = new ArrayList<>();
        for (SqlExpr.CheckedDefects.Hoist h : d.hoists()) {
            Map<String, Object> ho = new LinkedHashMap<>();
            ho.put("path", new ArrayList<Object>(h.path()));
            ho.put("envelope", expr(h.envelope()));
            ho.put("toMany", h.toMany());
            hoists.add(ho);
        }
        o.put("hoists", hoists);
    }

    private static void jsonArrayAgg(Map<String, Object> o, SqlExpr.JsonArrayAgg j) {
        tag(o, ExprTag.jsonArrayAgg, j.type());
        o.put("value", expr(j.value()));
        List<Object> keys = new ArrayList<>();
        for (SqlExpr.JsonArrayAgg.Key k : j.orderKeys()) {
            Map<String, Object> ko = new LinkedHashMap<>();
            ko.put("expr", expr(k.expr()));
            ko.put("desc", k.desc());
            keys.add(ko);
        }
        o.put("orderKeys", keys);
    }

    private static void windowCall(Map<String, Object> o, SqlExpr.WindowCall w) {
        tag(o, ExprTag.windowCall, w.type());
        o.put("fn", agg(w.fn()));
        o.put("partitionBy", exprs(w.partitionBy()));
        o.put("orderBy", sortKeys(w.orderBy()));
        if (w.frame() == null) {
            o.put("frame", null);
        } else {
            Map<String, Object> fo = new LinkedHashMap<>();
            fo.put("kind", w.frame().kind().name());
            fo.put("from", bound(w.frame().from()));
            fo.put("to", bound(w.frame().to()));
            o.put("frame", fo);
        }
    }

    private static void tag(Map<String, Object> o, ExprTag t, TypeFact type) {
        o.put("_type", t.name());
        o.put("type", fact(type));
    }

    private static void reducerFields(Map<String, Object> o, SqlAgg.Reducer r) {
        o.put("fn", r.fn().name());
        o.put("args", exprs(r.args()));
        o.put("distinct", r.distinct());
        o.put("orderBy", sortKeys(r.orderBy()));
    }

    public static Map<String, Object> agg(SqlAgg a) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (a) {
            case SqlAgg.Reducer r -> {
                o.put("_type", AggTag.reducer.name());
                o.put("type", fact(r.type()));
                reducerFields(o, r);
            }
            case SqlAgg.RankingFn r -> {
                o.put("_type", AggTag.rankingFn.name());
                o.put("fn", r.fn().name());
                o.put("args", exprs(r.args()));
            }
            case SqlAgg.ValueFn v -> {
                o.put("_type", AggTag.valueFn.name());
                o.put("fn", v.fn().name());
                o.put("args", exprs(v.args()));
            }
        }
        return o;
    }

    private static Map<String, Object> bound(SqlExpr.WindowCall.Frame.Bound b) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (b) {
            case SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding u -> o.put("_type", BoundTag.unboundedPreceding.name());
            case SqlExpr.WindowCall.Frame.Bound.Preceding p -> {
                o.put("_type", BoundTag.preceding.name());
                number(o, p.n());
            }
            case SqlExpr.WindowCall.Frame.Bound.CurrentRow c -> o.put("_type", BoundTag.currentRow.name());
            case SqlExpr.WindowCall.Frame.Bound.Following f -> {
                o.put("_type", BoundTag.following.name());
                number(o, f.n());
            }
            case SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing u -> o.put("_type", BoundTag.unboundedFollowing.name());
            case SqlExpr.WindowCall.Frame.Bound.IntervalPreceding p -> {
                o.put("_type", BoundTag.intervalPreceding.name());
                o.put("n", p.n());
                o.put("unit", p.unit());
            }
            case SqlExpr.WindowCall.Frame.Bound.IntervalFollowing f -> {
                o.put("_type", BoundTag.intervalFollowing.name());
                o.put("n", f.n());
                o.put("unit", f.unit());
            }
        }
        return o;
    }

    /** A bound's {@link Number}, kept as the Java number it is: its text and its class, so the round trip is exact. */
    private static void number(Map<String, Object> o, Number n) {
        String kind = switch (n) {
            case Long l -> "Long";
            case Integer i -> "Integer";
            case Double d -> "Double";
            case BigDecimal b -> "BigDecimal";
            default -> throw new IllegalArgumentException("a window bound's number of class "
                    + n.getClass().getName() + " has no JSON form");
        };
        o.put("n", n.toString());
        o.put("numberClass", kind);
    }

    private static Number readNumber(Json.Obj o) {
        String n = o.getString("n");
        return switch (o.getString("numberClass")) {
            case "Long" -> Long.valueOf(n);
            case "Integer" -> Integer.valueOf(n);
            case "Double" -> Double.valueOf(n);
            case "BigDecimal" -> new BigDecimal(n);
            default -> throw new IllegalArgumentException("unknown window bound number class '"
                    + o.getString("numberClass") + "'");
        };
    }

    public static Map<String, Object> type(SqlType t) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (t) {
            case SqlType.Scalar s -> {
                o.put("_type", TypeTag.scalar.name());
                o.put("name", s.name());
            }
            case SqlType.Decimal d -> {
                o.put("_type", TypeTag.decimal.name());
                o.put("precision", d.precision());
                o.put("scale", d.scale());
            }
            case SqlType.Array a -> {
                o.put("_type", TypeTag.array.name());
                o.put("element", type(a.element()));
            }
            case SqlType.Map m -> {
                o.put("_type", TypeTag.map.name());
                o.put("key", type(m.key()));
                o.put("value", type(m.value()));
            }
            case SqlType.Struct s -> {
                o.put("_type", TypeTag.struct.name());
                List<Object> fields = new ArrayList<>();
                for (SqlType.Struct.Field f : s.fields()) {
                    Map<String, Object> fo = new LinkedHashMap<>();
                    fo.put("name", f.name());
                    fo.put("type", type(f.type()));
                    fields.add(fo);
                }
                o.put("fields", fields);
            }
        }
        return o;
    }

    public static Map<String, Object> fact(TypeFact f) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (f) {
            case TypeFact.Typed t -> {
                o.put("_type", FactTag.typed.name());
                o.put("sqlType", type(t.type()));
                o.put("nullable", t.nullable());
            }
            case TypeFact.Bottom b -> o.put("_type", FactTag.bottom.name());
            case TypeFact.Raises r -> o.put("_type", FactTag.raises.name());
            case TypeFact.Unknown u -> o.put("_type", FactTag.unknown.name());
        }
        return o;
    }

    private static Map<String, Object> fmt(DateFmt f) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (f) {
            case DateFmt.Part p -> {
                o.put("_type", FmtTag.part.name());
                o.put("part", p.name());
            }
            case DateFmt.Text t -> {
                o.put("_type", FmtTag.text.name());
                o.put("s", t.s());
            }
        }
        return o;
    }

    public static Map<String, Object> columnType(SqlDdl.ColumnType t) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (t) {
            case SqlDdl.ColumnType.Plain p -> {
                o.put("_type", ColumnTypeTag.plain.name());
                o.put("kind", p.kind().name());
            }
            case SqlDdl.ColumnType.Sized s -> {
                o.put("_type", ColumnTypeTag.sized.name());
                o.put("kind", s.kind());
                o.put("size", s.size());
            }
            case SqlDdl.ColumnType.Scaled s -> {
                o.put("_type", ColumnTypeTag.scaled.name());
                o.put("kind", s.kind());
                o.put("precision", s.precision());
                o.put("scale", s.scale());
            }
        }
        return o;
    }

    // ---------------------------------------------------------------- reading

    private static <E extends Enum<E>> E named(Class<E> family, String name, String what) {
        for (E e : family.getEnumConstants()) {
            if (e.name().equals(name)) {
                return e;
            }
        }
        throw new IllegalArgumentException("unknown " + what + " '" + name + "'");
    }

    private static <E extends Enum<E>> E tagOf(Class<E> family, Json.Obj o, String what) {
        return named(family, o.getString("_type"), what + " tag");
    }

    private static @com.legend.base.Nullable Json.Obj objOrNull(Json.Obj o, String key) {
        Json.Node n = o.getOr(key, null);
        return n == null ? null : (Json.Obj) n;
    }

    private static List<String> strings(Json.Arr a) {
        List<String> out = new ArrayList<>();
        for (Json.Node n : a.items()) {
            out.add(((Json.Str) n).value());
        }
        return out;
    }

    public static SqlQuery readQuery(Json.Obj o) {
        return switch (tagOf(QueryTag.class, o, "query")) {
            case select -> {
                List<SqlSelect.Projection> projections = new ArrayList<>();
                for (Json.Node n : o.getArr("projections").items()) {
                    projections.add(readProjection((Json.Obj) n));
                }
                Json.Obj where = objOrNull(o, "where");
                Json.Obj having = objOrNull(o, "having");
                Json.Obj qualify = objOrNull(o, "qualify");
                Json.Node limit = o.getOr("limit", null);
                Json.Node offset = o.getOr("offset", null);
                yield new SqlSelect(projections, o.getBool("distinct"), readSource(o.getObj("from")),
                        where == null ? null : readExpr(where), readExprs(o.getArr("groupBy")),
                        having == null ? null : readExpr(having), qualify == null ? null : readExpr(qualify),
                        readSortKeys(o.getArr("orderBy")), limit == null ? null : ((Json.Num) limit).longValue(),
                        offset == null ? null : ((Json.Num) offset).longValue(), readOutputCols(o.getArr("outputs")));
            }
            case union -> {
                List<SqlQuery> branches = new ArrayList<>();
                for (Json.Node n : o.getArr("branches").items()) {
                    branches.add(readQuery((Json.Obj) n));
                }
                yield new SqlUnion(branches, o.getBool("all"), readOutputCols(o.getArr("outputs")));
            }
            case with -> {
                List<SqlWith.Cte> ctes = new ArrayList<>();
                for (Json.Node n : o.getArr("ctes").items()) {
                    Json.Obj c = (Json.Obj) n;
                    ctes.add(new SqlWith.Cte(c.getString("name"), readQuery(c.getObj("query")),
                            c.getBool("materialized")));
                }
                yield new SqlWith(ctes, readQuery(o.getObj("body")), o.getBool("recursive"));
            }
        };
    }

    private static SqlSelect.Projection readProjection(Json.Obj o) {
        Json.Obj out = objOrNull(o, "out");
        return new SqlSelect.Projection(readExpr(o.getObj("expr")), o.getStringOr("alias", null),
                out == null ? null : readOutputCol(out));
    }

    private static List<SqlSelect.SortKey> readSortKeys(Json.Arr a) {
        List<SqlSelect.SortKey> out = new ArrayList<>();
        for (Json.Node n : a.items()) {
            Json.Obj k = (Json.Obj) n;
            String nullOrder = k.getStringOr("nullOrder", null);
            out.add(new SqlSelect.SortKey(readExpr(k.getObj("expr")), k.getBool("ascending"),
                    nullOrder == null ? null : named(SqlSelect.SortKey.NullOrder.class, nullOrder, "null order"),
                    k.getStringOr("outputName", null)));
        }
        return out;
    }

    private static List<OutputCol> readOutputCols(Json.Arr a) {
        List<OutputCol> out = new ArrayList<>();
        for (Json.Node n : a.items()) {
            out.add(readOutputCol((Json.Obj) n));
        }
        return out;
    }

    private static OutputCol readOutputCol(Json.Obj o) {
        return new OutputCol(o.getString("name"), readType(o.getObj("type")), o.getBool("nullable"),
                named(OutputCol.Origin.class, o.getString("origin"), "output origin"));
    }

    public static SqlSource readSource(Json.Obj o) {
        return switch (tagOf(SourceTag.class, o, "source")) {
            case table -> {
                Map<String, SqlDdl.ColumnType> stored = new LinkedHashMap<>();
                for (Map.Entry<String, Json.Node> e : o.getObj("storedTypes").fields().entrySet()) {
                    stored.put(e.getKey(), readColumnType((Json.Obj) e.getValue()));
                }
                Json.Obj q = o.getObj("quoting");
                yield new SqlSource.Table(o.getString("name"), o.getString("alias"),
                        readOutputCols(o.getArr("outputs")), o.getBool("call"), stored,
                        new NameQuoting(q.getBool("schema"), q.getBool("table")));
            }
            case subselect -> new SqlSource.Subselect(readQuery(o.getObj("inner")), o.getString("alias"),
                    o.getStringOr("frameName", null));
            case cte -> new SqlSource.Cte(o.getString("name"), o.getString("alias"),
                    readOutputCols(o.getArr("outputs")));
            case join -> {
                Json.Obj on = objOrNull(o, "on");
                yield new SqlSource.Join(readSource(o.getObj("left")), readSource(o.getObj("right")),
                        named(SqlSource.Join.Kind.class, o.getString("kind"), "join kind"),
                        on == null ? null : readExpr(on), readOutputCols(o.getArr("outputs")));
            }
            case pivot -> {
                List<SqlSource.Pivot.Using> usings = new ArrayList<>();
                for (Json.Node n : o.getArr("usings").items()) {
                    Json.Obj u = (Json.Obj) n;
                    Json.Obj t = objOrNull(u, "type");
                    usings.add(new SqlSource.Pivot.Using((SqlAgg.Reducer) readAgg(u.getObj("agg")),
                            u.getString("alias"), t == null ? null : readType(t)));
                }
                yield new SqlSource.Pivot(readSource(o.getObj("source")), readExprs(o.getArr("on")),
                        readExprs(o.getArr("in")), readExprs(o.getArr("named")), usings, o.getString("alias"),
                        readOutputCols(o.getArr("outputs")));
            }
            case values -> {
                List<List<SqlExpr>> rows = new ArrayList<>();
                for (Json.Node n : o.getArr("rows").items()) {
                    rows.add(readExprs((Json.Arr) n));
                }
                yield new SqlSource.Values(rows, strings(o.getArr("columns")), o.getString("alias"),
                        readOutputCols(o.getArr("outputs")));
            }
            case dual -> new SqlSource.Dual();
            case sourceUrl -> new SqlSource.SourceUrl(o.getString("url"), o.getString("alias"),
                    readOutputCols(o.getArr("outputs")));
            case rawSql -> new SqlSource.RawSql(o.getString("sql"), o.getString("alias"),
                    readOutputCols(o.getArr("outputs")), o.getString("writtenFor"));
            case catalog -> new SqlSource.Catalog(named(SqlSource.Catalog.Grid.class, o.getString("grid"), "grid"),
                    o.getStringOr("schemaPattern", null), o.getStringOr("tablePattern", null),
                    o.getStringOr("columnPattern", null), o.getString("alias"), readOutputCols(o.getArr("outputs")));
            case varSet -> new SqlSource.VarSetPlaceholder(o.getString("varName"), o.getString("alias"),
                    readOutputCols(o.getArr("outputs")));
        };
    }

    private static List<SqlExpr> readExprs(Json.Arr a) {
        List<SqlExpr> out = new ArrayList<>();
        for (Json.Node n : a.items()) {
            out.add(readExpr((Json.Obj) n));
        }
        return out;
    }

    public static SqlExpr readExpr(Json.Obj o) {
        ExprTag t = tagOf(ExprTag.class, o, "expression");
        TypeFact type = readFact(o.getObj("type"));
        return switch (t) {
            case rowOrder -> new SqlExpr.RowOrder(o.getStringOr("table", null), type);
            case storedRead -> new SqlExpr.StoredRead((SqlExpr.Column) readExpr(o.getObj("column")),
                    readColumnType(o.getObj("stored")), type);
            case reduceCollection -> new SqlExpr.ReduceCollection(
                    named(SqlAgg.Fn.class, o.getString("reducer"), "aggregate"), readExpr(o.getObj("collection")),
                    readExprs(o.getArr("extras")), type);
            case membership -> new SqlExpr.Membership(readExpr(o.getObj("needle")), readExpr(o.getObj("collection")),
                    type);
            case column -> {
                String origin = o.getStringOr("origin", null);
                yield new SqlExpr.Column(o.getStringOr("table", null), o.getString("name"), type,
                        origin == null ? null : named(OutputCol.Origin.class, origin, "output origin"));
            }
            case starExcept -> new SqlExpr.StarExcept(o.getStringOr("table", null), strings(o.getArr("except")),
                    type);
            case star -> new SqlExpr.Star(o.getStringOr("table", null), type);
            case stringLit -> new SqlExpr.StringLit(o.getString("value"), type);
            case intLit -> new SqlExpr.IntLit(o.getLong("value"), type);
            case floatLit -> new SqlExpr.FloatLit(Double.parseDouble(o.getString("value")), type);
            case decimalLit -> new SqlExpr.DecimalLit(new BigDecimal(o.getString("value")), type);
            case boolLit -> new SqlExpr.BoolLit(o.getBool("value"), type);
            case nullLit -> new SqlExpr.NullLit(type);
            case dateLit -> new SqlExpr.DateLit(o.getString("iso"), type);
            case group -> new SqlExpr.Group(readExpr(o.getObj("inner")), type);
            case keyCell -> new SqlExpr.KeyCell(o.getInt("cell"), type);
            case planParam -> {
                Json.Obj v = objOrNull(o, "byValue");
                ValueTyping byValue = null;
                if (v != null) {
                    List<ValueKind> kinds = new ArrayList<>();
                    for (String k : strings(v.getArr("kinds"))) {
                        kinds.add(named(ValueKind.class, k, "value kind"));
                    }
                    byValue = new ValueTyping(kinds, named(ValueKind.class, v.getString("absent"), "value kind"));
                }
                yield new SqlExpr.PlanParam(o.getString("name"),
                        named(SqlExpr.PlanParam.Kind.class, o.getString("kind"), "parameter kind"),
                        o.getBool("optional"), o.getStringOr("enumMapFn", null), type, byValue);
            }
            case formatLit -> {
                List<DateFmt> parts = new ArrayList<>();
                for (Json.Node n : o.getArr("parts").items()) {
                    parts.add(readFmt((Json.Obj) n));
                }
                yield new SqlExpr.FormatLit(parts, type);
            }
            case datePartLit -> new SqlExpr.DatePartLit(named(DatePart.class, o.getString("part"), "date part"), type);
            case jsonKindLit -> new SqlExpr.JsonKindLit(named(JsonKind.class, o.getString("kind"), "JSON kind"), type);
            case typeNameLit -> new SqlExpr.TypeNameLit(named(SqlType.Scalar.class, o.getString("scalar"),
                    "scalar type"), type);
            case timestampLit -> new SqlExpr.TimestampLit(o.getString("iso"), type);
            case timeLit -> new SqlExpr.TimeLit(o.getString("iso"), type);
            case orderedListAgg -> new SqlExpr.OrderedListAgg(readExpr(o.getObj("value")),
                    readExpr(o.getObj("orderBy")), type);
            case arrayLit -> new SqlExpr.ArrayLit(readExprs(o.getArr("elements")), type);
            case structLit -> {
                List<SqlExpr.StructLit.Field> fields = new ArrayList<>();
                for (Json.Node n : o.getArr("fields").items()) {
                    Json.Obj f = (Json.Obj) n;
                    Json.Obj declared = objOrNull(f, "declared");
                    fields.add(new SqlExpr.StructLit.Field(f.getString("name"), readExpr(f.getObj("value")),
                            declared == null ? null : readType(declared)));
                }
                yield new SqlExpr.StructLit(fields, type);
            }
            case structGet -> new SqlExpr.StructGet(readExpr(o.getObj("source")), o.getString("field"), type);
            case call -> new SqlExpr.Call(named(SqlFn.class, o.getString("fn"), "function"),
                    readExprs(o.getArr("args")), type);
            case caseExpr -> {
                List<SqlExpr.Case.When> whens = new ArrayList<>();
                for (Json.Node n : o.getArr("whens").items()) {
                    Json.Obj w = (Json.Obj) n;
                    whens.add(new SqlExpr.Case.When(readExpr(w.getObj("condition")), readExpr(w.getObj("then"))));
                }
                Json.Obj otherwise = objOrNull(o, "otherwise");
                yield new SqlExpr.Case(whens, otherwise == null ? null : readExpr(otherwise), type);
            }
            case exists -> new SqlExpr.Exists(readQuery(o.getObj("subquery")), type);
            case inSubquery -> new SqlExpr.InSubquery(readExpr(o.getObj("value")), readQuery(o.getObj("subquery")),
                    type);
            case checkedDefects -> {
                List<SqlExpr.CheckedDefects.Hoist> hoists = new ArrayList<>();
                for (Json.Node n : o.getArr("hoists").items()) {
                    Json.Obj h = (Json.Obj) n;
                    hoists.add(new SqlExpr.CheckedDefects.Hoist(strings(h.getArr("path")),
                            readExpr(h.getObj("envelope")), h.getBool("toMany")));
                }
                yield new SqlExpr.CheckedDefects(readExpr(o.getObj("own")), hoists, type);
            }
            case checkedChildValue -> new SqlExpr.CheckedChildValue(readExpr(o.getObj("envelope")),
                    o.getBool("toMany"), type);
            case quantified -> new SqlExpr.Quantified(readExpr(o.getObj("value")),
                    named(SqlFn.class, o.getString("comparison"), "function"),
                    named(SqlExpr.Quantifier.class, o.getString("quantifier"), "quantifier"),
                    readQuery(o.getObj("subquery")), type);
            case scalarSubquery -> new SqlExpr.ScalarSubquery(readQuery(o.getObj("subquery")), type);
            case checkedOne -> new SqlExpr.CheckedOne(readExpr(o.getObj("list")), o.getBool("scalarCarrier"),
                    o.getBool("atLeastOnly"), type);
            case compactList -> new SqlExpr.CompactList(readExpr(o.getObj("list")), type);
            case deferredTdsString -> new SqlExpr.DeferredTdsString((SqlSelect) readQuery(o.getObj("inner")),
                    o.getString("alias"), o.getInt("id"),
                    named(SqlExpr.DeferredTdsString.Form.class, o.getString("form"), "TDS form"),
                    o.getBool("renderTdsNull"), type);
            case tempTableInSplice -> new SqlExpr.TempTableInSplice(o.getString("tempTableName"), type);
            case jsonObject -> new SqlExpr.JsonObject(readExprs(o.getArr("kv")), type);
            case jsonArray -> new SqlExpr.JsonArray(readExprs(o.getArr("elements")), type);
            case jsonArrayAgg -> {
                List<SqlExpr.JsonArrayAgg.Key> keys = new ArrayList<>();
                for (Json.Node n : o.getArr("orderKeys").items()) {
                    Json.Obj k = (Json.Obj) n;
                    keys.add(new SqlExpr.JsonArrayAgg.Key(readExpr(k.getObj("expr")), k.getBool("desc")));
                }
                yield new SqlExpr.JsonArrayAgg(readExpr(o.getObj("value")), keys, type);
            }
            case windowCall -> {
                Json.Obj f = objOrNull(o, "frame");
                SqlExpr.WindowCall.Frame frame = f == null ? null : new SqlExpr.WindowCall.Frame(
                        named(SqlExpr.WindowCall.Frame.Kind.class, f.getString("kind"), "frame kind"),
                        readBound(f.getObj("from")), readBound(f.getObj("to")));
                yield new SqlExpr.WindowCall(readAgg(o.getObj("fn")), readExprs(o.getArr("partitionBy")),
                        readSortKeys(o.getArr("orderBy")), frame, type);
            }
            case lambda -> new SqlExpr.Lambda(strings(o.getArr("params")), readExpr(o.getObj("body")), type);
            case cast -> new SqlExpr.Cast(readExpr(o.getObj("value")), readType(o.getObj("target")),
                    o.getBool("conform"), type);
            case foldCall -> new SqlExpr.FoldCall(readExpr(o.getObj("source")),
                    (SqlExpr.Lambda) readExpr(o.getObj("lambda")), readExpr(o.getObj("init")),
                    o.getBool("accIsList"), o.getBool("homogeneous"), type);
            case reducer -> readReducer(o, type);
        };
    }

    private static SqlAgg.Reducer readReducer(Json.Obj o, TypeFact type) {
        return new SqlAgg.Reducer(named(SqlAgg.Fn.class, o.getString("fn"), "aggregate"),
                readExprs(o.getArr("args")), o.getBool("distinct"), readSortKeys(o.getArr("orderBy")), type);
    }

    public static SqlAgg readAgg(Json.Obj o) {
        return switch (tagOf(AggTag.class, o, "aggregate")) {
            case reducer -> readReducer(o, readFact(o.getObj("type")));
            case rankingFn -> new SqlAgg.RankingFn(named(SqlAgg.Fn.class, o.getString("fn"), "aggregate"),
                    readExprs(o.getArr("args")));
            case valueFn -> new SqlAgg.ValueFn(named(SqlAgg.Fn.class, o.getString("fn"), "aggregate"),
                    readExprs(o.getArr("args")));
        };
    }

    private static SqlExpr.WindowCall.Frame.Bound readBound(Json.Obj o) {
        return switch (tagOf(BoundTag.class, o, "window bound")) {
            case unboundedPreceding -> new SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding();
            case preceding -> new SqlExpr.WindowCall.Frame.Bound.Preceding(readNumber(o));
            case currentRow -> new SqlExpr.WindowCall.Frame.Bound.CurrentRow();
            case following -> new SqlExpr.WindowCall.Frame.Bound.Following(readNumber(o));
            case unboundedFollowing -> new SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing();
            case intervalPreceding -> new SqlExpr.WindowCall.Frame.Bound.IntervalPreceding(o.getLong("n"),
                    o.getString("unit"));
            case intervalFollowing -> new SqlExpr.WindowCall.Frame.Bound.IntervalFollowing(o.getLong("n"),
                    o.getString("unit"));
        };
    }

    public static SqlType readType(Json.Obj o) {
        return switch (tagOf(TypeTag.class, o, "SQL type")) {
            case scalar -> named(SqlType.Scalar.class, o.getString("name"), "scalar type");
            case decimal -> new SqlType.Decimal(o.getInt("precision"), o.getInt("scale"));
            case array -> new SqlType.Array(readType(o.getObj("element")));
            case map -> new SqlType.Map(readType(o.getObj("key")), readType(o.getObj("value")));
            case struct -> {
                List<SqlType.Struct.Field> fields = new ArrayList<>();
                for (Json.Node n : o.getArr("fields").items()) {
                    Json.Obj f = (Json.Obj) n;
                    fields.add(new SqlType.Struct.Field(f.getString("name"), readType(f.getObj("type"))));
                }
                yield new SqlType.Struct(fields);
            }
        };
    }

    public static TypeFact readFact(Json.Obj o) {
        return switch (tagOf(FactTag.class, o, "type fact")) {
            case typed -> new TypeFact.Typed(readType(o.getObj("sqlType")), o.getBool("nullable"));
            case bottom -> new TypeFact.Bottom();
            case raises -> new TypeFact.Raises();
            case unknown -> new TypeFact.Unknown();
        };
    }

    private static DateFmt readFmt(Json.Obj o) {
        return switch (tagOf(FmtTag.class, o, "date format")) {
            case part -> named(DateFmt.Part.class, o.getString("part"), "date format part");
            case text -> new DateFmt.Text(o.getString("s"));
        };
    }

    public static SqlDdl.ColumnType readColumnType(Json.Obj o) {
        return switch (tagOf(ColumnTypeTag.class, o, "column type")) {
            case plain -> new SqlDdl.ColumnType.Plain(named(SqlDdl.ColumnType.Kind.class, o.getString("kind"),
                    "column kind"));
            case sized -> new SqlDdl.ColumnType.Sized(o.getString("kind"), o.getInt("size"));
            case scaled -> new SqlDdl.ColumnType.Scaled(o.getString("kind"), o.getInt("precision"), o.getInt("scale"));
        };
    }
}
