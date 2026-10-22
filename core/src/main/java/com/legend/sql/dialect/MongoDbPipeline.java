// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.base.Nullable;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlUnion;
import com.legend.sql.SqlWith;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A query as MongoDB's aggregation pipeline (docs/MONGODB_PLAN_2026_10_16.md §5.1, 2026-10-16): the stages
 * {@link MongoDb} writes for a select, a union and their sources. The row conventions every stage keeps:
 * <ul>
 *   <li>a select's RESULT rows are flat documents, one field per output column, named as the select names them;</li>
 *   <li>inside a select, the current row holds each source under its alias ({@code {t0: {...}, t1: {...}}}), so a
 *       column reference {@code t0.c} is the path {@code $t0.c}; a scan projects every declared column with
 *       {@code $ifNull}, so an absent field reads as null (D7) and no later stage meets a missing field;</li>
 *   <li>a join is a {@code $lookup} whose pipeline sees the left row's aliases as {@code let} variables and the right
 *       source's rows FLAT (the right alias is the lookup's {@code as}), then {@code $unwind}; a left join's missed
 *       row is the null row of the right source's columns;</li>
 *   <li>a group is one {@code $group} whose accumulators are read back through markers the projection carries; an
 *       aggregate over no rows answers SQL's one row through a union with the empty row;</li>
 *   <li>a sort key is computed into a carried field with its null flag, so NULLS FIRST and LAST are the SQL
 *       engine's (BSON's own order puts nulls first ascending);</li>
 *   <li>a literal relation is {@code $documents}: PCT's storeless expressions load nothing.</li>
 * </ul>
 * What the pipeline has no shape for yet is refused by name: a right, full or lateral join, a pivot, a CTE, raw text,
 * a catalog grid, a window.
 */
final class MongoDbPipeline {

    private final MongoDb dialect;

    MongoDbPipeline(MongoDb dialect) {
        this.dialect = dialect;
    }

    /** The stages written so far at one level: a comma before each but the first. */
    static final class Stages {
        private int written;

        SqlWriter begin(SqlWriter w) {
            if (written++ > 0) {
                w.append(", ");
            }
            return w;
        }
    }

    /** How a source's rows are shaped: under the source's alias (the current row of a select), or flat (the right
     *  side of a lookup, whose alias is the lookup's {@code as}). */
    private enum Shape { WRAPPED, FLAT }

    // ==================================================================
    // The command
    // ==================================================================

    /** The whole {@code aggregate} command for {@code q}: over its base collection, or the database when the query
     *  reads only literal relations. */
    SqlWriter command(SqlWriter w, SqlQuery q) {
        String base = baseCollection(q);
        w.append("{\"aggregate\": ").append(base == null ? "1" : MongoJson.string(base)).append(", \"pipeline\": [");
        query(w, q, new Stages());
        return w.append("], \"cursor\": {}}");
    }

    /** The collection a query's pipeline starts from: its FROM's base table, read through its subselects and down the
     *  left side of its joins; null for a query over literal rows alone. */
    private static @com.legend.base.Nullable String baseCollection(SqlQuery q) {
        return switch (q) {
            case SqlSelect s -> baseOf(s.from());
            case SqlUnion u -> baseCollection(u.branches().get(0));
            case SqlWith with -> throw new DialectCapability("a WITH (common table expressions) reached MongoDb,"
                    + " which has no shape for one yet");
        };
    }

    private static @com.legend.base.Nullable String baseOf(SqlSource src) {
        return switch (src) {
            case SqlSource.Table t -> collectionName(t);
            case SqlSource.Subselect sub -> baseCollection(sub.inner());
            case SqlSource.Join j -> baseOf(j.left());
            case SqlSource.Values v -> null;
            case SqlSource.Dual d -> null;
            case SqlSource.Cte c -> throw new DialectCapability("a CTE reference reached MongoDb: no shape yet");
            case SqlSource.VarSetPlaceholder v -> throw new IllegalStateException("a plan variable (" + v.varName()
                    + ") reached an executable dialect: plan-text vocabulary only");
            case SqlSource.RawSql r -> throw new DialectCapability("a raw statement as a relation reached MongoDb:"
                    + " a user's own pipeline as a source is not run yet");
            case SqlSource.Catalog c -> throw new DialectCapability("a catalog grid reached MongoDb: its catalog"
                    + " (listCollections) is not read as a grid yet");
            case SqlSource.SourceUrl u -> throw new DialectCapability("sourceUrl reached MongoDb: no external"
                    + " source encoding");
            case SqlSource.Pivot p -> baseOf(p.source());
        };
    }

    /** A collection's name: a table's bare name; a schema-qualified or tabular-function table is refused. */
    private static String collectionName(SqlSource.Table t) {
        if (t.call()) {
            throw new DialectCapability("a tabular function (" + t.name() + ") reached MongoDb, which has none");
        }
        if (t.name().indexOf('.') >= 0) {
            throw new DialectCapability("a schema-qualified table (" + t.name() + ") reached MongoDb: a collection"
                    + " has no schema (its database is the connection's)");
        }
        return t.name();
    }

    // ==================================================================
    // Queries
    // ==================================================================

    /** The query's stages; the fields its last projection names, or null when a star carries more than it names. */
    private @Nullable List<String> query(SqlWriter w, SqlQuery q, Stages st) {
        return switch (q) {
            case SqlSelect s -> select(w, s, st);
            case SqlUnion u -> union(w, u, st);
            case SqlWith with -> throw new DialectCapability("a WITH (common table expressions) reached MongoDb,"
                    + " which has no shape for one yet");
        };
    }

    /** A union: the first branch's stages, then each other branch as a {@code $unionWith} over its own base; a
     *  deduplicating union groups the rows afterwards. */
    private @Nullable List<String> union(SqlWriter w, SqlUnion u, Stages st) {
        List<String> projected = query(w, u.branches().get(0), st);
        for (int i = 1; i < u.branches().size(); i++) {
            SqlQuery branch = u.branches().get(i);
            String base = baseCollection(branch);
            st.begin(w).append("{\"$unionWith\": {");
            if (base != null) {
                w.append("\"coll\": ").append(MongoJson.string(base)).append(", ");
            }
            w.append("\"pipeline\": [");
            query(w, branch, new Stages());
            w.append("]}}");
        }
        if (!u.all()) {
            distinct(w, st);
        }
        return projected;
    }

    /** The rows made distinct: grouped by the whole row, then each group's row. */
    private static void distinct(SqlWriter w, Stages st) {
        st.begin(w).append("{\"$group\": {\"_id\": \"$$ROOT\"}}");
        st.begin(w).append("{\"$replaceWith\": \"$_id\"}");
    }

    /** One select, clause by clause, in SQL's order of evaluation. */
    private @Nullable List<String> select(SqlWriter w, SqlSelect s, Stages st) {
        source(w, s.from(), st, Shape.WRAPPED);
        // the row wrapped under one alias: an unqualified column reads under it until the projection flattens it
        List<String> aliases = aliases(s.from());
        dialect.soleAlias(aliases.size() == 1 && !(s.from() instanceof SqlSource.Join) ? aliases.get(0) : null);
        Clauses clauses = subqueries(w, s, st);
        if (clauses.where != null) {
            match(w, st, clauses.where);
        }
        Grouping g = group(w, s.groupBy(), clauses.projections, clauses.having, clauses.orderBy, st);
        if (g.having != null) {
            match(w, st, g.having);
        }
        g = windows(w, st, g, s.qualify());
        List<SortCarry> sorts = sortCarries(w, st, g);
        List<String> projected = projection(w, s, st, g, sorts);
        dialect.soleAlias(null);
        if (s.distinct()) {
            // the sort carries are functions of the row's columns, so equal rows carry equal keys: the distinct
            // groups them with the row and the sort reads them after
            distinct(w, st);
        }
        if (!sorts.isEmpty()) {
            st.begin(w).append("{\"$sort\": {");
            for (int i = 0; i < sorts.size(); i++) {
                SortCarry c = sorts.get(i);
                if (i > 0) {
                    w.append(", ");
                }
                w.append(MongoJson.string(c.nullField)).append(": ").append(c.nullsFirst ? -1 : 1).append(", ")
                        .append(MongoJson.string(c.field)).append(": ").append(c.ascending ? 1 : -1);
            }
            w.append("}}");
            st.begin(w).append("{\"$unset\": [");
            for (int i = 0; i < sorts.size(); i++) {
                if (i > 0) {
                    w.append(", ");
                }
                w.append(MongoJson.string(sorts.get(i).field)).append(", ")
                        .append(MongoJson.string(sorts.get(i).nullField));
            }
            w.append("]}");
        }
        if (s.offset() != null) {
            st.begin(w).append("{\"$skip\": ").append(s.offset()).append("}");
        }
        if (s.limit() != null) {
            st.begin(w).append("{\"$limit\": ").append(s.limit()).append("}");
        }
        return projected;
    }

    // ==================================================================
    // Subqueries
    // ==================================================================

    /** A select's clauses with each subquery read through its lookup's marker. */
    private record Clauses(@com.legend.base.Nullable SqlExpr where, List<SqlSelect.Projection> projections,
            @com.legend.base.Nullable SqlExpr having, List<SqlSelect.SortKey> orderBy) {
    }

    /** The subqueries of a select's expressions, each replaced by its marker: a scalar subquery and an EXISTS by a
     *  marker column; an IN by the needle's membership in the marker's rows; a quantified comparison by the list
     *  predicate over the marker's rows -- {@code exists} for ANY, {@code forAll} for ALL -- whose lambda compares
     *  the value with each row (the Lowerer drops the rows' nulls and guards the value, so the two-valued
     *  predicates are SQL's: RelationPredicates.quantifier). */
    private static final class Subqueries {
        final List<SqlExpr> found = new ArrayList<>();

        SqlExpr apply(SqlExpr e) {
            return switch (e) {
                case SqlExpr.ScalarSubquery sq -> marker(sq);
                case SqlExpr.Exists ex -> marker(ex);
                case SqlExpr.InSubquery in -> new SqlExpr.Membership(apply(in.value()), marker(in));
                case SqlExpr.Quantified q -> {
                    SqlExpr value = apply(q.value());
                    SqlExpr rows = marker(q);
                    String row = "__q" + (found.size() - 1);
                    SqlExpr compared = SqlExpr.Call.of(q.comparison(), value, SqlExpr.Column.derived(null, row));
                    yield SqlExpr.Call.of(q.quantifier() == SqlExpr.Quantifier.ANY ? SqlFn.LIST_EXISTS
                            : SqlFn.LIST_FOR_ALL, rows, new SqlExpr.Lambda(List.of(row), compared));
                }
                default -> e.mapChildren(this::apply);
            };
        }

        private SqlExpr marker(SqlExpr subquery) {
            found.add(subquery);
            return SqlExpr.Column.derived(null, MongoDb.SUBQUERY_MARKER + (found.size() - 1));
        }
    }

    /**
     * Each subquery of the select as a {@code $lookup} into a marker field, after the sources and before the clauses
     * that read it: the row's aliases are the lookup's {@code let} variables, so a correlated subquery reads them as
     * variables (a reference the lookup does not bind fails on the server by name); the subquery's own pipeline
     * runs over its base collection, or over its literal rows. A scalar subquery reads its first row's one column
     * (null over no row, SQL's answer); EXISTS reads whether a row came; IN and a quantified comparison read the
     * column of every row.
     */
    private Clauses subqueries(SqlWriter w, SqlSelect s, Stages st) {
        Subqueries sub = new Subqueries();
        SqlExpr where = s.where() == null ? null : sub.apply(s.where());
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (SqlSelect.Projection p : s.projections()) {
            SqlExpr e = sub.apply(p.expr());
            ps.add(e == p.expr() ? p : new SqlSelect.Projection(e, p.alias() != null ? p.alias() : p.outputName(),
                    p.out()));
        }
        SqlExpr having = s.having() == null ? null : sub.apply(s.having());
        List<SqlSelect.SortKey> keys = new ArrayList<>();
        for (SqlSelect.SortKey k : s.orderBy()) {
            keys.add(new SqlSelect.SortKey(sub.apply(k.expr()), k.ascending(), k.nullOrder(), k.outputName()));
        }
        if (!sub.found.isEmpty()) {
            List<String> outer = aliases(s.from());
            Map<String, SqlWriter.Piece> reads = new LinkedHashMap<>();
            for (int i = 0; i < sub.found.size(); i++) {
                String field = MongoDb.SUBQUERY_MARKER + i;
                SqlQuery inner = switch (sub.found.get(i)) {
                    case SqlExpr.ScalarSubquery sq -> sq.subquery();
                    case SqlExpr.Exists ex -> ex.subquery();
                    case SqlExpr.InSubquery in -> in.subquery();
                    case SqlExpr.Quantified q -> q.subquery();
                    default -> throw new IllegalStateException("not a subquery: " + sub.found.get(i));
                };
                String base = baseCollection(inner);
                st.begin(w).append("{\"$lookup\": {");
                if (base != null) {
                    w.append("\"from\": ").append(MongoJson.string(base)).append(", ");
                }
                w.append("\"let\": {");
                for (int a = 0; a < outer.size(); a++) {
                    w.append(a > 0 ? ", " : "").append(MongoJson.string(MongoJson.variable(outer.get(a))))
                            .append(": ").append(MongoJson.path(outer.get(a)));
                }
                w.append("}, \"pipeline\": [");
                dialect.pushLookupScope(outer, null);
                List<String> projected;
                try {
                    projected = query(w, inner, new Stages());
                } finally {
                    dialect.popScope();
                }
                w.append("], \"as\": ").append(MongoJson.key(field)).append("}}");
                reads.put(field, subqueryRead(sub.found.get(i), field, inner, projected));
            }
            dialect.subqueryReads(reads);
        }
        return new Clauses(where, ps, having, keys);
    }

    private static SqlWriter.Piece subqueryRead(SqlExpr subquery, String field, SqlQuery inner,
            @Nullable List<String> projected) {
        String path = MongoJson.string("$" + field);
        return switch (subquery) {
            case SqlExpr.ScalarSubquery sq -> MongoJson.text("{\"$let\": {\"vars\": {\"r\": {\"$arrayElemAt\": ["
                    + path + ", 0]}}, \"in\": " + MongoJson.string("$$r." + readField(inner, projected)) + "}}");
            case SqlExpr.Exists ex -> MongoJson.text("{\"$gt\": [{\"$size\": " + path + "}, 0]}");
            case SqlExpr.InSubquery in -> MongoJson.text("{\"$map\": {\"input\": " + path + ", \"in\": "
                    + MongoJson.string("$$this." + readField(inner, projected)) + "}}");
            case SqlExpr.Quantified q -> MongoJson.text("{\"$map\": {\"input\": " + path + ", \"in\": "
                    + MongoJson.string("$$this." + readField(inner, projected)) + "}}");
            default -> throw new IllegalStateException("not a subquery: " + subquery);
        };
    }

    /** The one field a subquery in expression position reads: the field its pipeline's projection stage named (a
     *  projection without an output column, a bare {@code COUNT(*)}, still has its field); else its one output
     *  column. */
    private static String readField(SqlQuery inner, @Nullable List<String> projected) {
        if (projected != null && projected.size() == 1) {
            return projected.get(0);
        }
        List<OutputCol> outs = inner.outputs();
        if (outs.size() != 1) {
            throw new DialectCapability("a subquery of " + outs.size() + " columns reached MongoDb in expression"
                    + " position: one column is read");
        }
        return outs.get(0).name();
    }

    /** The field the {@code i}th projection becomes: its alias, else its output column's name, else positional. */
    private static String fieldName(SqlSelect.Projection p, int i) {
        return p.alias() != null ? p.alias() : p.outputName() != null ? p.outputName() : "_c" + i;
    }

    private static void match(SqlWriter w, Stages st, SqlExpr condition) {
        st.begin(w).append("{\"$match\": {\"$expr\": ").expr(condition, 0).append("}}");
    }

    // ==================================================================
    // Sources
    // ==================================================================

    private void source(SqlWriter w, SqlSource src, Stages st, Shape shape) {
        switch (src) {
            case SqlSource.Table t -> scan(w, t, st, shape);
            case SqlSource.Values v -> values(w, v, st, shape);
            case SqlSource.Dual d -> st.begin(w).append("{\"$documents\": [{}]}");
            case SqlSource.Subselect sub -> {
                query(w, sub.inner(), st);
                if (shape == Shape.WRAPPED) {
                    st.begin(w).append("{\"$replaceWith\": {").append(MongoJson.key(sub.alias()))
                            .append(": \"$$ROOT\"}}");
                }
            }
            case SqlSource.Join j -> {
                if (shape == Shape.FLAT) {
                    throw new DialectCapability("a join on the right side of a join reached MongoDb: a lookup"
                            + " over a joined source has no shape yet");
                }
                join(w, j, st);
            }
            case SqlSource.Cte c -> throw new DialectCapability("a CTE reference reached MongoDb: no shape yet");
            case SqlSource.VarSetPlaceholder v -> throw new IllegalStateException("a plan variable (" + v.varName()
                    + ") reached an executable dialect: plan-text vocabulary only");
            case SqlSource.RawSql r -> throw new DialectCapability("a raw statement as a relation reached MongoDb:"
                    + " a user's own pipeline as a source is not run yet");
            case SqlSource.Catalog c -> throw new DialectCapability("a catalog grid reached MongoDb: its catalog"
                    + " (listCollections) is not read as a grid yet");
            case SqlSource.SourceUrl u -> throw new DialectCapability("sourceUrl reached MongoDb: no external"
                    + " source encoding");
            case SqlSource.Pivot p -> pivot(w, p, st, shape);
        }
    }

    /**
     * A static pivot (its values given, so its columns are known) as two groups and a projection: the source's rows
     * grouped by the pivot's keys and its ON column, each USING aggregate accumulated as a group stage does; the
     * groups regrouped by the keys alone, the ON value and the aggregates pushed; and each pivot column projected
     * as the pushed aggregate of its value (null where no row had the value). The keys are the pivot's outputs that
     * are no pivot column (a name without the separator); a pivot column's name spells its value and its USING
     * alias (Type.RelationType.presentPivotName). A dynamic pivot (no values given) is refused by name: its columns
     * exist only once the database is asked. One ON column: a static pivot's values are one column's.
     */
    private void pivot(SqlWriter w, SqlSource.Pivot p, Stages st, Shape shape) {
        // a dynamic pivot a cast names pivots on the named values (SqlSource.Pivot.values, step 9, 2026-10-19)
        if (p.values().isEmpty()) {
            throw new DialectCapability("a dynamic pivot reached MongoDb: its columns are known only once the"
                    + " database is asked (a static pivot, its values given, has its grouped form)");
        }
        if (p.on().size() != 1) {
            throw new DialectCapability("a static pivot over " + p.on().size() + " columns reached MongoDb: its"
                    + " values name one column's");
        }
        // the source's row wrapped under its alias: the ON column and the aggregates' arguments are qualified by it
        source(w, p.source(), st, Shape.WRAPPED);
        String separator = PIVOT_SEPARATOR;
        List<OutputCol> keys = p.outputs().stream().filter(c -> !c.name().contains(separator)).toList();
        List<OutputCol> pivoted = p.outputs().stream().filter(c -> c.name().contains(separator)).toList();
        if (pivoted.isEmpty() && p.in().isEmpty()) {
            // a dynamic pivot a cast names: its columns are the named values' by each USING, the cast reads them by
            // their bare names (step 9, 2026-10-19)
            List<OutputCol> named = new ArrayList<>();
            for (SqlExpr v : p.named()) {
                for (SqlSource.Pivot.Using u : p.usings()) {
                    named.add(CarrierStrategies.generatedSlot(p, u, pivotValueText(v) + separator + u.alias()));
                }
            }
            pivoted = named;
        }
        // the first group: by the keys and the ON value, each USING aggregate accumulated
        st.begin(w).append("{\"$group\": {\"_id\": {");
        for (int i = 0; i < keys.size(); i++) {
            w.append("\"k").append(i).append("\": ").append(MongoJson.path(p.source().alias(), keys.get(i).name()))
                    .append(", ");
        }
        w.append("\"on\": ").expr(p.on().get(0), 0).append("}");
        List<SqlWriter.Piece> reads = new ArrayList<>();
        Map<String, SqlWriter.Piece> byField = new LinkedHashMap<>();
        for (int i = 0; i < p.usings().size(); i++) {
            String field = MongoDb.AGGREGATE_MARKER + i;
            w.append(", ").append(MongoJson.string(field)).append(": ");
            SqlWriter.Piece read = dialect.accumulator(w, p.usings().get(i).agg(),
                    new MongoDb.GroupField(field, false));
            reads.add(read);
            byField.put(field, read);
        }
        w.append("}}");
        // the second group: by the keys alone, the ON value and the aggregates' reads pushed
        dialect.aggregateReads(byField);
        st.begin(w).append("{\"$group\": {\"_id\": {");
        for (int i = 0; i < keys.size(); i++) {
            w.append(i > 0 ? ", " : "").append("\"k").append(i).append("\": \"$_id.k").append(i).append("\"");
        }
        w.append("}, \"__p\": {\"$push\": {\"on\": \"$_id.on\"");
        for (int i = 0; i < reads.size(); i++) {
            w.append(", \"a").append(i).append("\": ").piece(reads.get(i));
        }
        w.append("}}}}");
        // the projection: the keys, and each pivot column as its value's pushed aggregate
        st.begin(w).append("{\"$project\": {\"_id\": 0, ");
        if (shape == Shape.WRAPPED) {
            w.append(MongoJson.key(p.alias())).append(": {");
        }
        for (int i = 0; i < keys.size(); i++) {
            w.append(i > 0 ? ", " : "").append(MongoJson.key(keys.get(i).name())).append(": \"$_id.k").append(i)
                    .append("\"");
        }
        for (OutputCol c : pivoted) {
            String bare = c.name().startsWith("'") && c.name().endsWith("'")
                    ? c.name().substring(1, c.name().length() - 1) : c.name();
            int sep = bare.lastIndexOf(separator);
            String valueText = bare.substring(0, sep);
            String alias = bare.substring(sep + separator.length());
            int using = -1;
            for (int i = 0; i < p.usings().size(); i++) {
                if (p.usings().get(i).alias().equals(alias)) {
                    using = i;
                }
            }
            SqlExpr value = null;
            for (SqlExpr v : p.values()) {
                if (valueText.equals(pivotValueText(v))) {
                    value = v;
                }
            }
            if (using < 0 || value == null) {
                throw new IllegalStateException("the pivot column " + c.name() + " names no USING alias and value"
                        + " of the pivot (" + p.values() + ", " + p.usings().stream().map(SqlSource.Pivot.Using::alias)
                        .toList() + ")");
            }
            w.append(keys.isEmpty() && pivoted.indexOf(c) == 0 ? "" : ", ").append(MongoJson.key(c.name()))
                    .append(": {\"$let\": {\"vars\": {\"m\": {\"$arrayElemAt\": [{\"$filter\": {\"input\": \"$__p\","
                            + " \"cond\": {\"$eq\": [\"$$this.on\", ").expr(value, 0).append("]}}}, 0]}}, \"in\":"
                            + " {\"$ifNull\": [\"$$m.a").append(using).append("\", null]}}}");
        }
        if (shape == Shape.WRAPPED) {
            w.append("}");
        }
        w.append("}}");
    }

    /** The separator of a pivot column's name (its value, then its USING alias): the Lowerer's
     *  (Type.RelationType.PIVOT_SEPARATOR, which the MIR layer does not see). */
    private static final String PIVOT_SEPARATOR = "__|__";

    /** A pivot value's text, as the pivot column's name spells it: a number's digits, a string itself. */
    private static String pivotValueText(SqlExpr v) {
        return switch (v) {
            case SqlExpr.IntLit i -> String.valueOf(i.value());
            case SqlExpr.StringLit sl -> sl.value();
            case SqlExpr.FloatLit f -> String.valueOf(f.value());
            case SqlExpr.BoolLit b -> String.valueOf(b.value());
            default -> throw new DialectCapability("a pivot value that is no literal (" + v + ") reached MongoDb");
        };
    }

    /** A collection's scan: each declared column read with {@code $ifNull}, so an absent field is null (D7), under
     *  the alias or flat; the document's own {@code _id} dropped unless the store declares it. */
    private static void scan(SqlWriter w, SqlSource.Table t, Stages st, Shape shape) {
        collectionName(t);
        boolean declaresId = t.outputs().stream().anyMatch(c -> c.name().equals("_id"));
        st.begin(w).append("{\"$project\": {");
        if (!declaresId) {
            w.append("\"_id\": 0").append(t.outputs().isEmpty() ? "" : ", ");
        }
        if (shape == Shape.WRAPPED) {
            w.append(MongoJson.key(t.alias())).append(": {");
        }
        for (int i = 0; i < t.outputs().size(); i++) {
            OutputCol c = t.outputs().get(i);
            if (i > 0) {
                w.append(", ");
            }
            w.append(MongoJson.key(c.name())).append(": {\"$ifNull\": [").append(MongoJson.path(c.name()))
                    .append(", null]}");
        }
        if (shape == Shape.WRAPPED) {
            w.append("}");
        }
        w.append("}}");
    }

    /** Literal rows as {@code $documents}, each row's cells under the relation's column names. */
    private static void values(SqlWriter w, SqlSource.Values v, Stages st, Shape shape) {
        st.begin(w).append("{\"$documents\": [");
        for (int r = 0; r < v.rows().size(); r++) {
            if (r > 0) {
                w.append(", ");
            }
            if (shape == Shape.WRAPPED) {
                w.append("{").append(MongoJson.key(v.alias())).append(": ");
            }
            w.append("{");
            List<SqlExpr> row = v.rows().get(r);
            for (int c = 0; c < row.size(); c++) {
                if (c > 0) {
                    w.append(", ");
                }
                w.append(MongoJson.key(v.columns().get(c))).append(": ").expr(row.get(c), 0);
            }
            w.append("}");
            if (shape == Shape.WRAPPED) {
                w.append("}");
            }
        }
        w.append("]}");
    }

    /** The aliases a source brings to the row, in order. */
    private static List<String> aliases(SqlSource src) {
        List<String> out = new ArrayList<>();
        switch (src) {
            case SqlSource.Join j -> {
                out.addAll(aliases(j.left()));
                out.addAll(aliases(j.right()));
            }
            case SqlSource.Dual d -> {
            }
            default -> out.add(src.alias());
        }
        return out;
    }

    /**
     * A join as a {@code $lookup} and an {@code $unwind}: the outer side's row wrapped under its alias, the inner side
     * looked up into a field named by its alias -- the lookup's {@code let} binds the outer row's aliases, so its
     * pipeline (the inner source, then the ON match) reads them as variables, which is what a lateral join's
     * correlated right side does ({@code CROSS_LATERAL} as a cross join, {@code LEFT_LATERAL} as a left join) --
     * then unwound, a preserved outer row's missing inner side padded with nulls. An as-of join keeps, of the right
     * rows the ON admits, the one nearest the left row's key ({@code $sort} by the match inequality's right-side
     * column, the first taken). A right join looks the left side up from the right; a full join appends, by
     * {@code $unionWith}, the right rows no left row matched (the left side padded) to the left join. Either over a
     * joined left side is refused by name (one alias looks up one source).
     */
    private void join(SqlWriter w, SqlSource.Join j, Stages st) {
        if (j.right() instanceof SqlSource.Join) {
            throw new DialectCapability("a join on the right side of a join reached MongoDb: a lookup over a"
                    + " joined source has no shape yet");
        }
        switch (j.kind()) {
            case INNER, CROSS, CROSS_LATERAL -> lookupJoin(w, st, j.left(), j.right(), j.on(), false, null);
            case LEFT, LEFT_LATERAL -> lookupJoin(w, st, j.left(), j.right(), j.on(), true, null);
            case ASOF_LEFT -> lookupJoin(w, st, j.left(), j.right(), j.on(), true, asOfKey(j.on(), j.right().alias()));
            case RIGHT -> {
                oneSource(j);
                lookupJoin(w, st, j.right(), j.left(), j.on(), true, null);
            }
            case FULL -> {
                oneSource(j);
                lookupJoin(w, st, j.left(), j.right(), j.on(), true, null);
                st.begin(w).append("{\"$unionWith\": {");
                String base = baseOf(j.right());
                if (base != null) {
                    w.append("\"coll\": ").append(MongoJson.string(base)).append(", ");
                }
                w.append("\"pipeline\": [");
                Stages inner = new Stages();
                source(w, j.right(), inner, Shape.WRAPPED);
                lookup(w, inner, aliases(j.right()), j.left(), j.on(), null);
                inner.begin(w).append("{\"$match\": {").append(MongoJson.key(j.left().alias())).append(": {\"$size\": 0}}}");
                // the left side (an empty array here) replaced by its null columns -- under $literal, since
                // $addFields merges a document written plainly into the field's own documents
                inner.begin(w).append("{\"$addFields\": {").append(MongoJson.key(j.left().alias()))
                        .append(": {\"$literal\": ");
                nulls(w, j.left());
                w.append("}}}");
                w.append("]}}");
            }
        }
    }

    private static void oneSource(SqlSource.Join j) {
        if (j.left() instanceof SqlSource.Join) {
            throw new DialectCapability("a " + j.kind() + " join over a joined left side reached MongoDb: one alias"
                    + " looks up one source");
        }
    }

    /** The outer source wrapped, the inner looked up and unwound; the outer rows kept ({@code preserve}) with the
     *  inner side padded, or dropped where the inner is empty. */
    private void lookupJoin(SqlWriter w, Stages st, SqlSource outer, SqlSource inner, @Nullable SqlExpr on,
            boolean preserve, @Nullable SqlSelect.SortKey nearest) {
        source(w, outer, st, Shape.WRAPPED);
        lookup(w, st, aliases(outer), inner, on, nearest);
        st.begin(w).append("{\"$unwind\": {\"path\": ").append(MongoJson.path(inner.alias()))
                .append(", \"preserveNullAndEmptyArrays\": ").append(preserve ? "true" : "false").append("}}");
        if (preserve) {
            pad(w, st, inner);
        }
    }

    /** The {@code $lookup} of {@code inner} into the field its alias names, the outer aliases its variables. */
    private void lookup(SqlWriter w, Stages st, List<String> outer, SqlSource inner, @Nullable SqlExpr on,
            @Nullable SqlSelect.SortKey nearest) {
        String as = inner.alias();
        String base = baseOf(inner);
        st.begin(w).append("{\"$lookup\": {");
        if (base != null) {
            w.append("\"from\": ").append(MongoJson.string(base)).append(", ");
        }
        w.append("\"let\": {");
        for (int i = 0; i < outer.size(); i++) {
            if (i > 0) {
                w.append(", ");
            }
            w.append(MongoJson.string(MongoJson.variable(outer.get(i)))).append(": ")
                    .append(MongoJson.path(outer.get(i)));
        }
        w.append("}, \"pipeline\": [");
        dialect.pushLookupScope(outer, as);
        try {
            Stages in = new Stages();
            source(w, inner, in, Shape.FLAT);
            if (on != null) {
                match(w, in, on);
            }
            if (nearest != null) {
                in.begin(w).append("{\"$sort\": {").append(MongoJson.key(((SqlExpr.Column) nearest.expr()).name()))
                        .append(": ").append(nearest.ascending() ? 1 : -1).append("}}");
                in.begin(w).append("{\"$limit\": 1}");
            }
        } finally {
            dialect.popScope();
        }
        w.append("], \"as\": ").append(MongoJson.key(as)).append("}}");
    }

    /** A missed row: the side's columns, each null, so every later read finds the field. */
    private static void pad(SqlWriter w, Stages st, SqlSource side) {
        st.begin(w).append("{\"$addFields\": {").append(MongoJson.key(side.alias())).append(": {\"$ifNull\": [")
                .append(MongoJson.path(side.alias())).append(", ");
        nulls(w, side);
        w.append("]}}}");
    }

    /** {@code {col: null, ...}}: the side's columns, each null. */
    private static void nulls(SqlWriter w, SqlSource side) {
        w.append("{");
        List<OutputCol> cols = side.outputs();
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) {
                w.append(", ");
            }
            w.append(MongoJson.key(cols.get(i).name())).append(": null");
        }
        w.append("}");
    }

    /**
     * The right-side column an as-of join's match inequality compares, and the direction that puts the nearest
     * right row first: the ON is the match, or the keys' condition AND the match (Lowerer.asOfJoin); the match
     * compares a left column with a right one, either side first, each perhaps under a checked narrowing or
     * parentheses. A left key greater than the right one wants the greatest right key: descending.
     */
    private static SqlSelect.SortKey asOfKey(@Nullable SqlExpr on, String right) {
        SqlExpr match = on;
        while (match instanceof SqlExpr.Call c && c.fn() == SqlFn.AND) {
            match = c.args().get(c.args().size() - 1);
        }
        if (!(match instanceof SqlExpr.Call cmp) || cmp.args().size() != 2) {
            throw new IllegalStateException("an as-of join's match is not a comparison: " + match);
        }
        SqlExpr.Column first = columnOf(cmp.args().get(0));
        SqlExpr.Column second = columnOf(cmp.args().get(1));
        boolean rightFirst = first != null && right.equals(first.table());
        SqlExpr.Column key = rightFirst ? first : second;
        if (key == null || !right.equals(key.table())) {
            throw new DialectCapability("an as-of join matching on " + match + " reached MongoDb: its lookup sorts"
                    + " by the right side's column");
        }
        boolean leftGreater = switch (cmp.fn()) {
            case GREATER, GREATER_EQUAL -> !rightFirst;
            case LESS, LESS_EQUAL -> rightFirst;
            default -> throw new IllegalStateException("an as-of join's match is not an inequality: " + cmp.fn());
        };
        return new SqlSelect.SortKey(key, !leftGreater, null, null);
    }

    /** The column under a comparison's operand -- itself, or wrapped in a checked narrowing or parentheses. */
    private static SqlExpr.@Nullable Column columnOf(SqlExpr e) {
        return switch (e) {
            case SqlExpr.Column c -> c;
            case SqlExpr.CheckedOne co -> columnOf(co.list());
            case SqlExpr.Group g -> columnOf(g.inner());
            default -> null;
        };
    }

    // ==================================================================
    // Grouping
    // ==================================================================

    /** A grouped select's rewritten clauses: each aggregate node a marker the group stage computed, each group key a
     *  read of the group's {@code _id}. An ungrouped select keeps its clauses as they are. */
    private record Grouping(List<SqlSelect.Projection> projections, @com.legend.base.Nullable SqlExpr having,
            List<SqlSelect.SortKey> orderBy) {
    }

    /** The rewrite: aggregates to markers, keys to {@code _id} reads. */
    private static final class Rewrite {
        final List<SqlExpr> keys;
        final List<SqlExpr> aggregates = new ArrayList<>();

        Rewrite(List<SqlExpr> keys) {
            this.keys = keys;
        }

        SqlExpr apply(SqlExpr e) {
            if (e instanceof SqlAgg.Reducer || e instanceof SqlExpr.OrderedListAgg
                    || e instanceof SqlExpr.JsonArrayAgg) {
                int at = aggregates.indexOf(e);
                if (at < 0) {
                    at = aggregates.size();
                    aggregates.add(e);
                }
                return SqlExpr.Column.derived(null, MongoDb.AGGREGATE_MARKER + at);
            }
            int k = keys.indexOf(e);
            if (k >= 0) {
                return SqlExpr.Column.derived(null, "_id.k" + k);
            }
            return e.mapChildren(this::apply);
        }
    }

    /** Whether {@code e} reads a dynamic pivot's key ({@link SqlExpr.KeyCell}): its value differs per copy of a per-key
     *  region. */
    private static boolean readsKey(SqlExpr e) {
        return e instanceof SqlExpr.KeyCell || e.children().stream().anyMatch(MongoDbPipeline::readsKey);
    }

    /** The {@code $group} stage when the select groups or aggregates; the clauses rewritten over its outputs. */
    private Grouping group(SqlWriter w, List<SqlExpr> groupBy, List<SqlSelect.Projection> projections,
            @com.legend.base.Nullable SqlExpr having0, List<SqlSelect.SortKey> orderBy, Stages st) {
        boolean aggregates = projections.stream().anyMatch(p -> SqlAgg.aggregatesSelect(p.expr()))
                || having0 != null && SqlAgg.aggregatesSelect(having0)
                || orderBy.stream().anyMatch(k -> SqlAgg.aggregatesSelect(k.expr()));
        if (groupBy.isEmpty() && !aggregates) {
            return new Grouping(projections, having0, orderBy);
        }
        Rewrite rewrite = new Rewrite(groupBy);
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (SqlSelect.Projection p : projections) {
            // the output keeps its name: a key projected bare reads the group's _id, under the column's own name
            ps.add(new SqlSelect.Projection(rewrite.apply(p.expr()), p.alias() != null ? p.alias() : p.outputName(),
                    p.out()));
        }
        SqlExpr having = having0 == null ? null : rewrite.apply(having0);
        List<SqlSelect.SortKey> keys = new ArrayList<>();
        for (SqlSelect.SortKey k : orderBy) {
            keys.add(new SqlSelect.SortKey(rewrite.apply(k.expr()), k.ascending(), k.nullOrder(), k.outputName()));
        }
        st.begin(w).append("{\"$group\": {\"_id\": ");
        if (groupBy.isEmpty()) {
            w.append("null");
        } else {
            w.append("{");
            for (int i = 0; i < groupBy.size(); i++) {
                if (i > 0) {
                    w.append(", ");
                }
                w.append("\"k").append(i).append("\": ").expr(groupBy.get(i), 0);
            }
            w.append("}");
        }
        Map<String, SqlWriter.Piece> reads = new LinkedHashMap<>();
        // a dynamic pivot's per-key aggregates (each over its key's rows, a key's cell in its argument: KeyedPivot)
        // are written last, a per-key region the runner writes once per key, each copy's fields named by the key's
        // position
        List<Integer> perKey = new ArrayList<>();
        for (int i = 0; i < rewrite.aggregates.size(); i++) {
            if (readsKey(rewrite.aggregates.get(i))) {
                perKey.add(i);
                continue;
            }
            String field = MongoDb.AGGREGATE_MARKER + i;
            w.append(", ").append(MongoJson.string(field)).append(": ");
            reads.put(field, dialect.accumulator(w, rewrite.aggregates.get(i), new MongoDb.GroupField(field, false)));
        }
        if (!perKey.isEmpty()) {
            w.region(", ", ", ", "", x -> {
                for (int n = 0; n < perKey.size(); n++) {
                    int i = perKey.get(n);
                    MongoDb.GroupField field = new MongoDb.GroupField(MongoDb.AGGREGATE_MARKER + i, true);
                    field.name(n > 0 ? x.append(", ") : x, "").append(": ");
                    reads.put(MongoDb.AGGREGATE_MARKER + i, dialect.accumulator(x, rewrite.aggregates.get(i), field));
                }
            });
        }
        w.append("}}");
        if (groupBy.isEmpty()) {
            // SQL's one row over no input: the group answers none, so the empty row is unioned in and the first of
            // the two (the real one when there is one) kept
            st.begin(w).append("{\"$unionWith\": {\"pipeline\": [{\"$documents\": [{\"_id\": null");
            for (int i = 0; i < rewrite.aggregates.size(); i++) {
                if (!perKey.contains(i)) {
                    w.append(", ").append(MongoJson.string(MongoDb.AGGREGATE_MARKER + i)).append(": ")
                            .append(MongoDb.emptyInput(rewrite.aggregates.get(i)));
                }
            }
            if (!perKey.isEmpty()) {
                w.region(", ", ", ", "", x -> {
                    for (int n = 0; n < perKey.size(); n++) {
                        int i = perKey.get(n);
                        new MongoDb.GroupField(MongoDb.AGGREGATE_MARKER + i, true).name(n > 0 ? x.append(", ") : x, "")
                                .append(": ").append(MongoDb.emptyInput(rewrite.aggregates.get(i)));
                    }
                });
            }
            w.append(", \"__none\": 1}]}]}}");
            st.begin(w).append("{\"$sort\": {\"__none\": 1}}");
            st.begin(w).append("{\"$limit\": 1}");
            st.begin(w).append("{\"$unset\": \"__none\"}");
        }
        dialect.aggregateReads(reads);
        return new Grouping(ps, having, keys);
    }

    // ==================================================================
    // Windows
    // ==================================================================

    /** The window functions of a select's expressions, each replaced by its marker column. */
    private static final class Windows {
        final List<SqlExpr.WindowCall> found = new ArrayList<>();

        SqlExpr apply(SqlExpr e) {
            if (e instanceof SqlExpr.WindowCall wc) {
                int at = found.indexOf(wc);
                if (at < 0) {
                    at = found.size();
                    found.add(wc);
                }
                return SqlExpr.Column.derived(null, MongoDb.WINDOW_MARKER + at);
            }
            return e.mapChildren(this::apply);
        }
    }

    /**
     * What a window function asks of the stage that computes it: nothing ({@code ANY}: the partition entire); a
     * sort of any shape ({@code SORTED}: a lag, a lead, a ROWS frame -- a {@code documents} window); the sort as ONE
     * field ({@code SINGLE}: MongoDB's ranking operators take a {@code sortBy} of exactly one element); the peers of
     * the current row ({@code TO_CURRENT}: SQL's RANGE between unbounded and the current row -- an unframed ordered
     * aggregate, a cumulative distribution -- which MongoDB ranges over a numeric field: the order's dense rank,
     * computed first); a range by offsets over the one sort key itself ({@code RANGE_KEY}).
     */
    private enum Need { ANY, SORTED, SINGLE, TO_CURRENT, RANGE_KEY }

    private static Need need(SqlExpr.WindowCall c) {
        return switch (c.fn()) {
            case SqlAgg.RankingFn rf -> rf.fn() == SqlAgg.Fn.CUME_DIST ? Need.TO_CURRENT : Need.SINGLE;
            case SqlAgg.ValueFn vf -> vf.fn() == SqlAgg.Fn.LAG || vf.fn() == SqlAgg.Fn.LEAD ? Need.SORTED : framed(c);
            case SqlAgg.Reducer r -> framed(c);
        };
    }

    private static Need framed(SqlExpr.WindowCall c) {
        SqlExpr.WindowCall.Frame f = c.frame();
        if (f == null) {
            return c.orderBy().isEmpty() ? Need.ANY : Need.TO_CURRENT;
        }
        if (f.kind() == SqlExpr.WindowCall.Frame.Kind.ROWS) {
            return Need.SORTED;
        }
        return offset(f.from()) || offset(f.to()) ? Need.RANGE_KEY : Need.TO_CURRENT;
    }

    private static boolean offset(SqlExpr.WindowCall.Frame.Bound b) {
        return !(b instanceof SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding
                || b instanceof SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing
                || b instanceof SqlExpr.WindowCall.Frame.Bound.CurrentRow);
    }

    /**
     * The window functions of the select, after its group and before its sort and projection, each a field of a
     * {@code $setWindowFields} stage. The calls are grouped by their window specification (partition, order); a
     * group's partition is the keys' document, and its order is computed into fields first: ONE field holding the
     * keys' document, each key beside its null flag so the SQL engine's null placement holds, when a call needs the
     * sort as one field or its dense rank ({@link Need}); else a field and a flag per key. That one field sorts in
     * the first key's direction, so keys sorted both ways are refused by name. A group writes up to three stages:
     * the sorted stage (the ranking operators, lags and leads, ROWS frames, the unframed aggregates of an unordered
     * window, and the order's dense rank where a call ranges to the current row); the stage sorted by that dense
     * rank (RANGE windows bounded by unbounded or the current row: the peers included, as SQL reads them); and the
     * stage sorted by the one key itself, ascending (RANGE windows by offsets, a descending order's bounds negated
     * and swapped). A QUALIFY is the match after the stages. The dialect spells each output and its read
     * ({@link MongoDb#windowOutput}).
     */
    private Grouping windows(SqlWriter w, Stages st, Grouping g, @Nullable SqlExpr qualify) {
        Windows rewrite = new Windows();
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (SqlSelect.Projection p : g.projections) {
            SqlExpr e = rewrite.apply(p.expr());
            ps.add(e == p.expr() ? p : new SqlSelect.Projection(e, p.alias() != null ? p.alias() : p.outputName(),
                    p.out()));
        }
        SqlExpr qualified = qualify == null ? null : rewrite.apply(qualify);
        List<SqlSelect.SortKey> keys = new ArrayList<>();
        for (SqlSelect.SortKey k : g.orderBy) {
            keys.add(new SqlSelect.SortKey(rewrite.apply(k.expr()), k.ascending(), k.nullOrder(), k.outputName()));
        }
        if (rewrite.found.isEmpty()) {
            return g;
        }
        Map<String, SqlWriter.Piece> reads = new LinkedHashMap<>();
        // the calls grouped by their window specification, in first-seen order
        List<List<Integer>> groups = new ArrayList<>();
        for (int i = 0; i < rewrite.found.size(); i++) {
            SqlExpr.WindowCall c = rewrite.found.get(i);
            boolean placed = false;
            for (List<Integer> group : groups) {
                SqlExpr.WindowCall first = rewrite.found.get(group.get(0));
                if (first.partitionBy().equals(c.partitionBy()) && first.orderBy().equals(c.orderBy())) {
                    group.add(i);
                    placed = true;
                    break;
                }
            }
            if (!placed) {
                groups.add(new ArrayList<>(List.of(i)));
            }
        }
        for (int serial = 0; serial < groups.size(); serial++) {
            List<Integer> group = groups.get(serial);
            SqlExpr.WindowCall spec = rewrite.found.get(group.get(0));
            Map<Need, List<Integer>> byNeed = new EnumMap<>(Need.class);
            for (int i : group) {
                byNeed.computeIfAbsent(need(rewrite.found.get(i)), n -> new ArrayList<>()).add(i);
            }
            boolean ordered = !spec.orderBy().isEmpty();
            boolean one = byNeed.containsKey(Need.SINGLE) || byNeed.containsKey(Need.TO_CURRENT);
            boolean sorted = ordered || byNeed.size() > 1 || !byNeed.containsKey(Need.ANY);
            String sortField = "__ws" + serial;
            String rankField = "__wd" + serial;
            int direction = ordered && !spec.orderBy().get(0).ascending() ? -1 : 1;
            List<String> keyFields = new ArrayList<>();
            List<Integer> keyDirections = new ArrayList<>();
            if (sorted) {
                st.begin(w).append("{\"$addFields\": {");
                if (!ordered) {
                    // a sort every row ties: the stage's operators ask for one
                    w.append(MongoJson.string(sortField)).append(": 0");
                } else if (one) {
                    w.append(MongoJson.string(sortField)).append(": {");
                    for (int i = 0; i < spec.orderBy().size(); i++) {
                        SqlSelect.SortKey k = spec.orderBy().get(i);
                        if ((k.ascending() ? 1 : -1) != direction) {
                            throw new DialectCapability("a window ranking or ranging over keys sorted both ways"
                                    + " reached MongoDb, whose ranking operators sort by one field");
                        }
                        // the flag that puts the nulls where the key asks, in the one field's direction
                        int nullFlag = nullsFirst(k) == k.ascending() ? 0 : 1;
                        w.append(i > 0 ? ", " : "").append("\"k").append(i).append("\": {\"n\": {\"$cond\": [{\"$eq\": [")
                                .expr(k.expr(), 0).append(", null]}, ").append(nullFlag).append(", ")
                                .append(1 - nullFlag).append("]}, \"v\": {\"$ifNull\": [").expr(k.expr(), 0)
                                .append(", null]}}");
                    }
                    w.append("}");
                } else {
                    for (int i = 0; i < spec.orderBy().size(); i++) {
                        SqlSelect.SortKey k = spec.orderBy().get(i);
                        String field = "__wk" + serial + "_" + i;
                        w.append(i > 0 ? ", " : "").append(MongoJson.string(field)).append(": ").expr(k.expr(), 0)
                                .append(", ").append(MongoJson.string(field + "n")).append(": {\"$eq\": [")
                                .expr(k.expr(), 0).append(", null]}");
                        keyFields.add(field);
                        keyDirections.add(k.ascending() ? 1 : -1);
                    }
                }
                w.append("}}");
            }
            // the sorted stage: the ranking operators, the lags and leads, the ROWS frames, the unordered
            // aggregates, and the dense rank the ranges to the current row sort by
            List<Integer> first = new ArrayList<>();
            for (Need n : List.of(Need.ANY, Need.SORTED, Need.SINGLE)) {
                first.addAll(byNeed.getOrDefault(n, List.of()));
            }
            boolean ranksForRange = byNeed.containsKey(Need.TO_CURRENT);
            if (!first.isEmpty() || ranksForRange) {
                st.begin(w).append("{\"$setWindowFields\": {");
                partitionBy(w, spec);
                if (sorted) {
                    w.append("\"sortBy\": {");
                    if (!ordered || one) {
                        w.append(MongoJson.string(sortField)).append(": ").append(direction);
                    } else {
                        for (int i = 0; i < keyFields.size(); i++) {
                            SqlSelect.SortKey k = spec.orderBy().get(i);
                            w.append(i > 0 ? ", " : "").append(MongoJson.string(keyFields.get(i) + "n")).append(": ")
                                    .append(nullsFirst(k) ? -1 : 1).append(", ").append(MongoJson.string(keyFields.get(i)))
                                    .append(": ").append(keyDirections.get(i));
                        }
                    }
                    w.append("}, ");
                }
                w.append("\"output\": {");
                boolean comma = false;
                if (ranksForRange) {
                    w.append(MongoJson.string(rankField)).append(": {\"$denseRank\": {}}");
                    comma = true;
                }
                for (int i : first) {
                    SqlExpr.WindowCall c = rewrite.found.get(i);
                    w.append(comma ? ", " : "");
                    comma = true;
                    reads.put(MongoDb.WINDOW_MARKER + i, dialect.windowOutput(w, c, MongoDb.WINDOW_MARKER + i,
                            documentsWindow(c)));
                }
                w.append("}}}");
            }
            // the stage sorted by the dense rank: RANGE windows between unbounded and the current row
            if (ranksForRange) {
                st.begin(w).append("{\"$setWindowFields\": {");
                partitionBy(w, spec);
                w.append("\"sortBy\": {").append(MongoJson.string(rankField)).append(": 1}, \"output\": {");
                boolean comma = false;
                for (int i : byNeed.getOrDefault(Need.TO_CURRENT, List.of())) {
                    SqlExpr.WindowCall c = rewrite.found.get(i);
                    w.append(comma ? ", " : "");
                    comma = true;
                    reads.put(MongoDb.WINDOW_MARKER + i, dialect.windowOutput(w, c, MongoDb.WINDOW_MARKER + i,
                            toCurrentWindow(c)));
                }
                w.append("}}}");
            }
            // the stage sorted by the one key, ascending: RANGE windows by offsets
            if (byNeed.containsKey(Need.RANGE_KEY)) {
                if (spec.orderBy().size() != 1) {
                    throw new DialectCapability("a range window by offsets over " + spec.orderBy().size()
                            + " sort keys reached MongoDb, whose range windows order by one field");
                }
                SqlSelect.SortKey k = spec.orderBy().get(0);
                String keyField = "__wr" + serial;
                // a null key sorts to its end as the infinity of the key's kind (a number, or a date beyond any),
                // so an offset from a null row reaches its null peers alone and an unbounded side reaches the
                // partition's edge, nulls included: the SQL engines' RANGE over nulls
                boolean nullsLast = nullsFirst(k) != k.ascending();
                boolean dates = byNeed.get(Need.RANGE_KEY).stream().map(rewrite.found::get)
                        .anyMatch(c -> c.frame() != null && (unitOf(c.frame().from()) != null
                                || unitOf(c.frame().to()) != null));
                String infinity = dates ? "{\"$date\": \"" + (nullsLast ? "9000" : "1000") + "-01-01T00:00:00Z\"}"
                        : "{\"$numberDouble\": \"" + (nullsLast ? "" : "-") + "Infinity\"}";
                st.begin(w).append("{\"$addFields\": {").append(MongoJson.string(keyField)).append(": {\"$ifNull\": [")
                        .expr(k.expr(), 0).append(", ").append(infinity).append("]}}}");
                st.begin(w).append("{\"$setWindowFields\": {");
                partitionBy(w, spec);
                w.append("\"sortBy\": {").append(MongoJson.string(keyField)).append(": 1}, \"output\": {");
                boolean comma = false;
                for (int i : byNeed.getOrDefault(Need.RANGE_KEY, List.of())) {
                    SqlExpr.WindowCall c = rewrite.found.get(i);
                    SqlExpr.WindowCall.Frame f = c.frame();
                    if (f == null) {
                        throw new IllegalStateException("a range window by offsets without a frame (MongoDbPipeline.need)");
                    }
                    w.append(comma ? ", " : "");
                    comma = true;
                    reads.put(MongoDb.WINDOW_MARKER + i, dialect.windowOutput(w, c, MongoDb.WINDOW_MARKER + i,
                            offsetWindow(f, k.ascending())));
                }
                w.append("}}}");
            }
        }
        dialect.windowReads(reads);
        if (qualified != null) {
            match(w, st, qualified);
        }
        return new Grouping(ps, g.having, keys);
    }

    private static boolean nullsFirst(SqlSelect.SortKey k) {
        return k.nullOrder() == null ? !k.ascending() : k.nullOrder() == SqlSelect.SortKey.NullOrder.NULLS_FIRST;
    }

    /** {@code "partitionBy": ...,} -- the one key's expression or the keys' document -- or nothing. */
    private void partitionBy(SqlWriter w, SqlExpr.WindowCall spec) {
        if (spec.partitionBy().isEmpty()) {
            return;
        }
        w.append("\"partitionBy\": ");
        if (spec.partitionBy().size() == 1) {
            w.expr(spec.partitionBy().get(0), 0);
        } else {
            w.append("{");
            for (int i = 0; i < spec.partitionBy().size(); i++) {
                w.append(i > 0 ? ", " : "").append("\"p").append(i).append("\": ").expr(spec.partitionBy().get(i), 0);
            }
            w.append("}");
        }
        w.append(", ");
    }

    /** A sorted-stage call's {@code , "window": {"documents": [...]}} for a ROWS frame; nothing otherwise (the
     *  partition entire -- $setWindowFields' own default -- or an operator without a window). */
    private static SqlWriter.Piece documentsWindow(SqlExpr.WindowCall c) {
        SqlExpr.WindowCall.Frame f = c.frame();
        if (f == null || f.kind() != SqlExpr.WindowCall.Frame.Kind.ROWS) {
            return MongoJson.text("");
        }
        return MongoJson.text(", \"window\": {\"documents\": [" + bound(f.from(), false) + ", " + bound(f.to(), false)
                + "]}");
    }

    /** A dense-rank-sorted call's {@code , "window": {"range": [...]}}: the frame's unbounded and current bounds, or
     *  SQL's default over an order, unbounded preceding to the current row. */
    private static SqlWriter.Piece toCurrentWindow(SqlExpr.WindowCall c) {
        SqlExpr.WindowCall.Frame f = c.frame();
        String from = f == null ? "\"unbounded\"" : bound(f.from(), true);
        String to = f == null ? "0" : bound(f.to(), true);
        return MongoJson.text(", \"window\": {\"range\": [" + from + ", " + to + "]}");
    }

    /** A key-sorted call's {@code , "window": {"range": [...], "unit": ...}}: the frame's bounds as offsets of the
     *  key, which the stage sorts ascending -- a descending order's bounds negated and swapped. One unit: bounds
     *  counting in two units of fixed length (a week to a millisecond) are both counted in the finer; a month or
     *  a year beside another unit is refused by name. */
    private static SqlWriter.Piece offsetWindow(SqlExpr.WindowCall.Frame f, boolean ascending) {
        String from = bound(f.from(), true);
        String to = bound(f.to(), true);
        String fromUnit = unitOf(f.from());
        String toUnit = unitOf(f.to());
        String unit = fromUnit != null ? fromUnit : toUnit;
        if (fromUnit != null && toUnit != null && !fromUnit.equals(toUnit)) {
            Long fromMillis = MILLIS.get(fromUnit);
            Long toMillis = MILLIS.get(toUnit);
            if (fromMillis == null || toMillis == null) {
                throw new DialectCapability("a range window bounded in " + fromUnit + "s and " + toUnit + "s reached"
                        + " MongoDb, whose range windows count in one unit");
            }
            unit = fromMillis < toMillis ? fromUnit : toUnit;
            long finer = Math.min(fromMillis, toMillis);
            from = String.valueOf(Long.parseLong(from) * (fromMillis / finer));
            to = String.valueOf(Long.parseLong(to) * (toMillis / finer));
        }
        if (!ascending) {
            String lo = negated(to);
            to = negated(from);
            from = lo;
        }
        return MongoJson.text(", \"window\": {\"range\": [" + from + ", " + to + "]"
                + (unit == null ? "" : ", \"unit\": " + MongoJson.string(unit)) + "}");
    }

    /** The milliseconds of each unit of fixed length. */
    private static final Map<String, Long> MILLIS = Map.of("week", 604_800_000L, "day", 86_400_000L,
            "hour", 3_600_000L, "minute", 60_000L, "second", 1_000L, "millisecond", 1L);

    private static String negated(String bound) {
        if (bound.startsWith("\"") || bound.equals("0")) {
            return bound;
        }
        return bound.startsWith("-") ? bound.substring(1) : "-" + bound;
    }

    /** A frame bound as $setWindowFields spells it. The current row of a range window is the offset 0: its
     *  {@code "current"} is the current document alone, where SQL's RANGE reads the row's peers. */
    private static String bound(SqlExpr.WindowCall.Frame.Bound b, boolean range) {
        return switch (b) {
            case SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding u -> "\"unbounded\"";
            case SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing u -> "\"unbounded\"";
            case SqlExpr.WindowCall.Frame.Bound.CurrentRow cr -> range ? "0" : "\"current\"";
            case SqlExpr.WindowCall.Frame.Bound.Preceding p -> "-" + p.n();
            case SqlExpr.WindowCall.Frame.Bound.Following fo -> String.valueOf(fo.n());
            case SqlExpr.WindowCall.Frame.Bound.IntervalPreceding p -> "-" + p.n();
            case SqlExpr.WindowCall.Frame.Bound.IntervalFollowing fo -> String.valueOf(fo.n());
        };
    }

    /** The unit of an interval bound, in $setWindowFields' names; null for a plain bound. */
    private static @Nullable String unitOf(SqlExpr.WindowCall.Frame.Bound b) {
        String unit = switch (b) {
            case SqlExpr.WindowCall.Frame.Bound.IntervalPreceding p -> p.unit();
            case SqlExpr.WindowCall.Frame.Bound.IntervalFollowing fo -> fo.unit();
            default -> null;
        };
        if (unit == null) {
            return null;
        }
        return switch (unit.toUpperCase(java.util.Locale.ROOT)) {
            case "YEARS", "YEAR" -> "year";
            case "MONTHS", "MONTH" -> "month";
            case "WEEKS", "WEEK" -> "week";
            case "DAYS", "DAY" -> "day";
            case "HOURS", "HOUR" -> "hour";
            case "MINUTES", "MINUTE" -> "minute";
            case "SECONDS", "SECOND" -> "second";
            case "MILLISECONDS", "MILLISECOND" -> "millisecond";
            default -> throw new DialectCapability("a range window by " + unit + " reached MongoDb, whose windows"
                    + " range by years to milliseconds");
        };
    }

    // ==================================================================
    // Sorting and projecting
    // ==================================================================

    /** A sort key computed into a carried field, with its null flag beside it. */
    private record SortCarry(String field, String nullField, boolean ascending, boolean nullsFirst) {
    }

    /** Each sort key computed before the projection ({@code $addFields}), with its null flag, so the sort reads
     *  fields and the SQL engine's null placement holds (ascending puts nulls last, descending first, unless the key
     *  says otherwise). */
    private static List<SortCarry> sortCarries(SqlWriter w, Stages st, Grouping g) {
        if (g.orderBy.isEmpty()) {
            return List.of();
        }
        List<SortCarry> out = new ArrayList<>();
        st.begin(w).append("{\"$addFields\": {");
        for (int i = 0; i < g.orderBy.size(); i++) {
            SqlSelect.SortKey k = g.orderBy.get(i);
            String field = "__sort" + i;
            String nullField = "__null" + i;
            boolean nullsFirst = k.nullOrder() == null ? !k.ascending()
                    : k.nullOrder() == SqlSelect.SortKey.NullOrder.NULLS_FIRST;
            if (i > 0) {
                w.append(", ");
            }
            w.append(MongoJson.string(field)).append(": ").expr(k.expr(), 0).append(", ")
                    .append(MongoJson.string(nullField)).append(": {\"$eq\": [").expr(k.expr(), 0)
                    .append(", null]}");
            out.add(new SortCarry(field, nullField, k.ascending(), nullsFirst));
        }
        w.append("}}");
        return out;
    }

    /** The projection as one {@code $replaceWith}: the outputs by name, a star as the merged source documents, and
     *  the sort carries beside them. */
    private static @Nullable List<String> projection(SqlWriter w, SqlSelect s, Stages st, Grouping g,
            List<SortCarry> sorts) {
        st.begin(w).append("{\"$replaceWith\": ");
        List<SqlWriter.Piece> merged = new ArrayList<>();
        List<String> names = new ArrayList<>();
        List<SqlExpr> exprs = new ArrayList<>();
        List<String> unnested = new ArrayList<>();
        List<String> keyStems = new ArrayList<>();
        List<SqlExpr> keyExprs = new ArrayList<>();
        if (g.projections.isEmpty()) {
            for (String alias : aliases(s.from())) {
                merged.add(x -> x.append(MongoJson.path(alias)));
            }
        }
        for (int i = 0; i < g.projections.size(); i++) {
            SqlSelect.Projection p = g.projections.get(i);
            OutputCol out = p.out();
            if (p.expr() instanceof SqlExpr.Star star) {
                if (star.table() != null) {
                    String starred = java.util.Objects.requireNonNull(star.table());
                    merged.add(x -> x.append(MongoJson.path(starred)));
                } else {
                    for (String alias : aliases(s.from())) {
                        merged.add(x -> x.append(MongoJson.path(alias)));
                    }
                }
            } else if (p.expr() instanceof SqlExpr.StarExcept se) {
                // the starred row without the excluded fields (an absent one unset is no change)
                for (String alias : se.table() != null ? List.of(java.util.Objects.requireNonNull(se.table()))
                        : aliases(s.from())) {
                    merged.add(x -> {
                        String input = MongoJson.path(alias);
                        for (String name : se.except()) {
                            MongoJson.key(name);
                            input = "{\"$unsetField\": {\"field\": {\"$literal\": " + MongoJson.string(name)
                                    + "}, \"input\": " + input + "}}";
                        }
                        x.append(input);
                    });
                }
            } else if (out != null && out.origin() == OutputCol.Origin.PER_KEY) {
                // a dynamic pivot's per-key column: written in the per-key run below
                MongoJson.key(out.name());
                keyStems.add(out.name());
                keyExprs.add(p.expr());
            } else {
                String name = fieldName(p, i);
                names.add(name);
                if (p.expr() instanceof SqlExpr.Call call && call.fn() == SqlFn.UNNEST) {
                    // the list is projected whole and exploded after ($unwind): a collection result's rows
                    unnested.add(name);
                    exprs.add(call.args().get(0));
                } else {
                    exprs.add(p.expr());
                }
            }
        }
        SqlWriter.Piece fields = x -> {
            x.append("{");
            for (int i = 0; i < names.size(); i++) {
                if (i > 0) {
                    x.append(", ");
                }
                x.append(MongoJson.key(names.get(i))).append(": ").expr(exprs.get(i), 0);
            }
            for (SortCarry c : sorts) {
                if (!names.isEmpty() || sorts.indexOf(c) > 0) {
                    x.append(", ");
                }
                x.append(MongoJson.string(c.field)).append(": ").append(MongoJson.path(c.field)).append(", ")
                        .append(MongoJson.string(c.nullField)).append(": ").append(MongoJson.path(c.nullField));
            }
            if (!keyStems.isEmpty()) {
                // the per-key run: written once per key by the runner, each field named by the key's position
                // ("_pk3__u0" for _u0), as the SQL dialects name a per-key column (AnsiSqlRenderer.perKeyName)
                boolean before = !names.isEmpty() || !sorts.isEmpty();
                x.region(before ? ", " : "", ", ", before || !merged.isEmpty() ? "" : null, r -> {
                    for (int i = 0; i < keyStems.size(); i++) {
                        r.append(i > 0 ? ", " : "").append("\"_pk").counter().append("_" + keyStems.get(i) + "\": ")
                                .expr(keyExprs.get(i), 0);
                    }
                });
            }
            x.append("}");
        };
        if (merged.isEmpty()) {
            w.piece(fields);
        } else {
            merged.add(fields);
            MongoJson.opOf(w, "$mergeObjects", merged);
        }
        w.append("}");
        unwind(w, st, unnested);
        return merged.isEmpty() && keyStems.isEmpty() ? names : null;
    }

    /** The exploded lists of a projection: one list unwound (a null or empty list gives no row, as UNNEST does);
     *  several zipped by position to the longest, null-padded (the select-list UNNEST's positional rule), unwound,
     *  and read back into their columns. */
    private static void unwind(SqlWriter w, Stages st, List<String> unnested) {
        if (unnested.isEmpty()) {
            return;
        }
        if (unnested.size() == 1) {
            st.begin(w).append("{\"$unwind\": ").append(MongoJson.path(unnested.get(0))).append("}");
            return;
        }
        st.begin(w).append("{\"$addFields\": {\"__zip\": {\"$zip\": {\"inputs\": [");
        for (int i = 0; i < unnested.size(); i++) {
            w.append(i > 0 ? ", " : "").append("{\"$ifNull\": [").append(MongoJson.path(unnested.get(i)))
                    .append(", []]}");
        }
        w.append("], \"useLongestLength\": true}}}}");
        st.begin(w).append("{\"$unwind\": \"$__zip\"}");
        st.begin(w).append("{\"$addFields\": {");
        for (int i = 0; i < unnested.size(); i++) {
            w.append(i > 0 ? ", " : "").append(MongoJson.key(unnested.get(i))).append(": {\"$arrayElemAt\": [\"$__zip\", ")
                    .append(i).append("]}");
        }
        w.append("}}");
        st.begin(w).append("{\"$unset\": \"__zip\"}");
    }
}
