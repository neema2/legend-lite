package com.gs.legend.plan.printing;

import com.gs.legend.plan.sql.SqlRelation;
import com.gs.legend.sqlgen.SQLDialect;

/**
 * Render a {@link SqlRelation} tree as dialect-specific SQL text.
 *
 * <p><strong>This is where dialect lives.</strong> Lowering rules in
 * {@code plan.lowering.**} are dialect-free; every call to
 * {@link SQLDialect#quoteIdentifier}, {@link SQLDialect#sqlTypeName}, etc.
 * happens here or in {@link com.gs.legend.sqlgen.SqlExpr#toSql(SQLDialect)}.
 *
 * <p><strong>Fusion and subquery wrapping</strong> also live here: when two
 * adjacent MIR nodes can collapse into a single {@code SELECT} (e.g.,
 * {@code Filter(Filter(TableRef))} -> one {@code WHERE a AND b}), the printer
 * decides. When wrapping is needed (aggregation / window / DISTINCT + sort etc.),
 * the printer emits the wrap. Stage 5.5 adds a dedicated {@code needsWrap} predicate.
 *
 * <p>Stage 1 skeleton: handles only the trivial source cases needed to wire up
 * the three-IR pipeline end-to-end. All other variants throw
 * {@link UnsupportedOperationException} until their lowering rule is ported.
 */
public final class SqlRelationPrinter {
    private SqlRelationPrinter() {}

    public static String print(SqlRelation rel, SQLDialect dialect) {
        return switch (rel) {
            case SqlRelation.SourceExprRel r -> printSourceExpr(r, dialect);
            case SqlRelation.TableRef r      -> printTableRef(r, dialect);
            case SqlRelation.Values r        -> printValues(r, dialect);
            case SqlRelation.Filter r        -> printFilter(r, dialect);
            case SqlRelation.Project r       -> printProject(r, dialect);
            case SqlRelation.Sort r          -> printSort(r, dialect);
            case SqlRelation.Limit r         -> printLimit(r, dialect);
            case SqlRelation.Distinct r      -> printDistinct(r, dialect);
            case SqlRelation.Rename r        -> printRename(r, dialect);
            case SqlRelation.Select r        -> printSelect(r, dialect);
            case SqlRelation.SubqueryRel r   -> printSubqueryRel(r, dialect);
            case SqlRelation.Extend r        -> printExtend(r, dialect);
            case SqlRelation.GroupBy r       -> printGroupBy(r, dialect);
            case SqlRelation.Aggregate r     -> printAggregate(r, dialect);
            case SqlRelation.Union r         -> printUnion(r, dialect);
            case SqlRelation.Join r          -> printJoin(r, dialect);
            case SqlRelation.Flatten r       -> throw notImpl(rel);
            case SqlRelation.Pivot r         -> printPivot(r, dialect);
            case SqlRelation.AsOfJoin r      -> printAsOfJoin(r, dialect);
            case SqlRelation.WithCtes r      -> throw notImpl(rel);
        };
    }

    private static UnsupportedOperationException notImpl(SqlRelation rel) {
        return new UnsupportedOperationException(
                "[plangen-c0954a] SqlRelationPrinter: not yet implemented for "
                        + rel.getClass().getSimpleName()
                        + ". See plangen-typed-port-c0954a.md");
    }

    // ==================== Source printers ====================

    private static String printSourceExpr(SqlRelation.SourceExprRel r, SQLDialect dialect) {
        String colAlias = r.outputs().isEmpty() ? "result" : r.outputs().get(0).name();
        return "SELECT " + r.expr().toSql(dialect) + " AS " + dialect.quoteIdentifier(colAlias);
    }

    /**
     * {@code SELECT * FROM "TBL" AS "t0"}. Qualified (schema-prefixed) tables are
     * emitted as {@code "schema"."table"}. Column-level projection lives in
     * {@link SqlRelation.Project}; a bare TableRef prints as {@code SELECT *}.
     */
    private static String printTableRef(SqlRelation.TableRef r, SQLDialect dialect) {
        String qualifiedTable = r.schema() != null
                ? dialect.quoteIdentifier(r.schema()) + "." + dialect.quoteIdentifier(r.table())
                : dialect.quoteIdentifier(r.table());
        return "SELECT * FROM " + qualifiedTable
                + " AS " + dialect.quoteIdentifier(r.alias());
    }

    /**
     * {@code SELECT * FROM (VALUES (r1c1, r1c2, ...), (r2c1, ...)) AS "t0"("c1","c2",...)}.
     * The outer SELECT-star + inner VALUES form works cross-dialect and keeps
     * column aliasing explicit.
     */
    private static String printValues(SqlRelation.Values r, SQLDialect dialect) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM (VALUES ");
        for (int i = 0; i < r.rows().size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("(");
            var row = r.rows().get(i);
            for (int c = 0; c < row.size(); c++) {
                if (c > 0) sb.append(", ");
                sb.append(row.get(c).toSql(dialect));
            }
            sb.append(")");
        }
        sb.append(") AS ").append(dialect.quoteIdentifier(r.alias())).append("(");
        for (int i = 0; i < r.columnNames().size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(dialect.quoteIdentifier(r.columnNames().get(i)));
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * {@code <source> WHERE <pred>}. When {@code source} is a bare {@code SELECT}
     * (TableRef, Values, or SourceExprRel) we fuse into a single statement instead
     * of wrapping in a subquery. Otherwise we'll wrap — Stage 3 installs the full
     * {@code needsWrap} predicate.
     */
    private static String printFilter(SqlRelation.Filter r, SQLDialect dialect) {
        return printAsStatement(r.source(), dialect)
                + " WHERE " + r.predicate().toSql(dialect);
    }

    // ==================== Unary operator printers ====================

    /**
     * {@code SELECT a AS alias, b AS alias, ... FROM <source>}. When the source is
     * a naturally-aliased leaf we keep it inline; otherwise we wrap in a subquery.
     */
    private static String printProject(SqlRelation.Project r, SQLDialect dialect) {
        StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 0; i < r.projections().size(); i++) {
            if (i > 0) sb.append(", ");
            var p = r.projections().get(i);
            sb.append(p.expr().toSql(dialect))
              .append(" AS ")
              .append(dialect.quoteIdentifier(p.alias()));
        }
        sb.append(" FROM ").append(printAsFromItem(r.source(), dialect));
        return sb.toString();
    }

    /** {@code <source> ORDER BY k1 DIR, k2 DIR, ...}. */
    private static String printSort(SqlRelation.Sort r, SQLDialect dialect) {
        StringBuilder sb = new StringBuilder(printAsStatement(r.source(), dialect));
        sb.append(" ORDER BY ");
        for (int i = 0; i < r.keys().size(); i++) {
            if (i > 0) sb.append(", ");
            var k = r.keys().get(i);
            sb.append(k.expr().toSql(dialect))
              .append(" ")
              .append(k.direction().name());
        }
        return sb.toString();
    }

    /** {@code <source> LIMIT n OFFSET off}. Non-positive fields are omitted. */
    private static String printLimit(SqlRelation.Limit r, SQLDialect dialect) {
        StringBuilder sb = new StringBuilder(printAsStatement(r.source(), dialect));
        if (r.n() >= 0) sb.append(" LIMIT ").append(r.n());
        if (r.offset() > 0) sb.append(" OFFSET ").append(r.offset());
        return sb.toString();
    }

    /**
     * Renders a relation as a self-contained {@code SELECT} statement so
     * trailing clauses ({@code ORDER BY}, {@code LIMIT}) attach cleanly.
     *
     * <p>Most {@link SqlRelation} printers already emit a statement — only
     * FROM-item-shaped sources ({@link SqlRelation.SubqueryRel},
     * {@link SqlRelation.Join}, {@link SqlRelation.TableRef}) need wrapping
     * here; their bare {@link #print} output is an aliased FROM-item, not a
     * statement.
     */
    private static String printAsStatement(SqlRelation r, SQLDialect dialect) {
        if (r instanceof SqlRelation.SubqueryRel
                || r instanceof SqlRelation.Join
                || r instanceof SqlRelation.TableRef) {
            return "SELECT * FROM " + printAsFromItem(r, dialect);
        }
        return print(r, dialect);
    }

    /**
     * {@code SELECT DISTINCT * FROM <source>}. Project wrapped in Distinct is
     * a separate shape; see {@link SelectRenameLowering}.
     */
    private static String printDistinct(SqlRelation.Distinct r, SQLDialect dialect) {
        return "SELECT DISTINCT * FROM " + printAsFromItem(r.source(), dialect);
    }

    /**
     * {@code SELECT old1 AS new1, old2 AS new2, <passthrough cols> FROM <source>}.
     * Non-renamed columns are emitted identity so the outer schema order matches
     * {@code SqlRelation.Rename#outputs()}.
     */
    private static String printRename(SqlRelation.Rename r, SQLDialect dialect) {
        var alias = ensureFromAlias(r.source());
        StringBuilder sb = new StringBuilder("SELECT ");
        var cols = r.source().outputs();
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) sb.append(", ");
            String name = cols.get(i).name();
            String newName = r.renames().getOrDefault(name, name);
            sb.append(dialect.quoteIdentifier(alias)).append(".").append(dialect.quoteIdentifier(name));
            if (!newName.equals(name)) {
                sb.append(" AS ").append(dialect.quoteIdentifier(newName));
            }
        }
        sb.append(" FROM ").append(printAsFromItem(r.source(), dialect));
        return sb.toString();
    }

    /** {@code SELECT c1, c2, ... FROM <source>}. */
    private static String printSelect(SqlRelation.Select r, SQLDialect dialect) {
        StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 0; i < r.columns().size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(dialect.quoteIdentifier(r.columns().get(i)));
        }
        sb.append(" FROM ").append(printAsFromItem(r.source(), dialect));
        return sb.toString();
    }

    /**
     * {@code SELECT <alias>.*, e1 AS n1, e2 AS n2, ... FROM <source>}. Extend
     * preserves the source schema in full and appends computed columns; the
     * star reference uses the source's natural alias so downstream tools see
     * the original column set unchanged.
     */
    private static String printExtend(SqlRelation.Extend r, SQLDialect dialect) {
        String srcAlias = ensureFromAlias(r.source());
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(dialect.quoteIdentifier(srcAlias)).append(".*");
        for (var c : r.cols()) {
            sb.append(", ").append(c.expr().toSql(dialect))
              .append(" AS ").append(dialect.quoteIdentifier(c.name()));
        }
        sb.append(" FROM ").append(printAsFromItem(r.source(), dialect));
        return sb.toString();
    }

    /** {@code (<inner>) AS alias} — explicit subquery wrapping. */
    private static String printSubqueryRel(SqlRelation.SubqueryRel r, SQLDialect dialect) {
        return "(" + print(r.inner(), dialect) + ") AS " + dialect.quoteIdentifier(r.alias());
    }

    // ==================== Aggregation & set-ops ====================

    /**
     * {@code SELECT k1 AS "k0", ..., AGG1 AS a1, ... FROM <source> GROUP BY k1, ...}.
     * Key aliases follow {@link SqlRelation.GroupBy#outputs()}'s {@code k<i>}
     * convention so downstream consumers see a stable schema.
     */
    private static String printGroupBy(SqlRelation.GroupBy r, SQLDialect dialect) {
        StringBuilder sb = new StringBuilder("SELECT ");
        boolean first = true;
        for (int i = 0; i < r.keys().size(); i++) {
            if (!first) sb.append(", "); first = false;
            sb.append(r.keys().get(i).toSql(dialect))
              .append(" AS ").append(dialect.quoteIdentifier("k" + i));
        }
        for (var a : r.aggs()) {
            if (!first) sb.append(", "); first = false;
            sb.append(renderAgg(a, dialect))
              .append(" AS ").append(dialect.quoteIdentifier(a.alias()));
        }
        sb.append(" FROM ").append(printAsFromItem(r.source(), dialect));
        if (!r.keys().isEmpty()) {
            sb.append(" GROUP BY ");
            for (int i = 0; i < r.keys().size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(r.keys().get(i).toSql(dialect));
            }
        }
        return sb.toString();
    }

    /** Key-less aggregate → single-row result. */
    private static String printAggregate(SqlRelation.Aggregate r, SQLDialect dialect) {
        StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 0; i < r.aggs().size(); i++) {
            if (i > 0) sb.append(", ");
            var a = r.aggs().get(i);
            sb.append(renderAgg(a, dialect))
              .append(" AS ").append(dialect.quoteIdentifier(a.alias()));
        }
        sb.append(" FROM ").append(printAsFromItem(r.source(), dialect));
        return sb.toString();
    }

    private static String renderAgg(SqlRelation.Agg a, SQLDialect dialect) {
        var renderedArgs = a.args().stream().map(e -> e.toSql(dialect)).toList();
        if (a.distinct() && !renderedArgs.isEmpty()) {
            // {@code COUNT(DISTINCT x)} and friends.
            return a.function().toUpperCase() + "(DISTINCT " + String.join(", ", renderedArgs) + ")";
        }
        return dialect.renderFunction(a.function(), renderedArgs);
    }

    /** {@code <left> UNION [ALL] <right>}. Both sides rendered as full SELECTs. */
    private static String printUnion(SqlRelation.Union r, SQLDialect dialect) {
        return print(r.left(), dialect)
                + (r.all() ? " UNION ALL " : " UNION ")
                + print(r.right(), dialect);
    }

    /**
     * {@code <left> <kind> JOIN <right> ON <cond>}. Both sides are rendered as
     * {@code FROM}-items so aliases are bound for the ON clause.
     */
    private static String printJoin(SqlRelation.Join r, SQLDialect dialect) {
        String kind = switch (r.type()) {
            case INNER -> "INNER JOIN";
            case LEFT  -> "LEFT JOIN";
            case RIGHT -> "RIGHT JOIN";
            case FULL  -> "FULL JOIN";
            case CROSS -> "CROSS JOIN";
        };
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM ")
          .append(printAsFromItem(r.left(),  dialect))
          .append(" ").append(kind).append(" ")
          .append(printAsFromItem(r.right(), dialect));
        if (r.type() != SqlRelation.JoinType.CROSS && r.on() != null) {
            sb.append(" ON ").append(r.on().toSql(dialect));
        }
        return sb.toString();
    }

    /**
     * {@code PIVOT (<source>) ON <pivotCol> USING AGG(x) AS alias, ...}. DuckDB
     * pivot — grouping keys and pivot values are auto-inferred so the printer
     * only emits {@code USING} aggregates. Ported from legacy
     * {@code SqlBuilder.renderPivot}.
     */
    private static String printPivot(SqlRelation.Pivot r, SQLDialect dialect) {
        StringBuilder sb = new StringBuilder("PIVOT (");
        sb.append(print(r.source(), dialect));
        sb.append(") ON ").append(dialect.quoteIdentifier(r.spec().pivotKey()));
        sb.append(" USING ");
        var aggs = r.spec().aggs();
        for (int i = 0; i < aggs.size(); i++) {
            if (i > 0) sb.append(", ");
            var a = aggs.get(i);
            sb.append(renderAgg(a, dialect))
              .append(" AS ")
              .append(dialect.quoteIdentifier(a.alias()));
        }
        return sb.toString();
    }

    private static String printAsOfJoin(SqlRelation.AsOfJoin r, SQLDialect dialect) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM ")
          .append(printAsFromItem(r.left(), dialect))
          .append(" ASOF LEFT JOIN ")
          .append(printAsFromItem(r.right(), dialect));
        if (r.spec().matchPredicate() != null) {
            sb.append(" ON ").append(r.spec().matchPredicate().toSql(dialect));
        }
        return sb.toString();
    }

    // ==================== FROM-item / alias helpers ====================

    /**
     * Renders a relation as a {@code FROM}-clause item: aliased-leaf sources
     * pass through unchanged, everything else gets wrapped in a subquery.
     */
    private static String printAsFromItem(SqlRelation src, SQLDialect dialect) {
        if (src instanceof SqlRelation.TableRef tr) {
            String q = tr.schema() != null
                    ? dialect.quoteIdentifier(tr.schema()) + "." + dialect.quoteIdentifier(tr.table())
                    : dialect.quoteIdentifier(tr.table());
            return q + " AS " + dialect.quoteIdentifier(tr.alias());
        }
        if (src instanceof SqlRelation.SubqueryRel sq) {
            return printSubqueryRel(sq, dialect);
        }
        // Joins render inline as part of the enclosing FROM so their left/right
        // aliases remain visible to the outer predicate/projection (wrapping
        // them in {@code SELECT *} would hide the individual aliases).
        if (src instanceof SqlRelation.Join j) {
            return printJoinBody(j, dialect);
        }
        // Values / SourceExprRel / composite nodes — wrap in an anonymous subquery.
        String alias = src.alias() != null ? src.alias() : "_s";
        return "(" + print(src, dialect) + ") AS " + dialect.quoteIdentifier(alias);
    }

    /** Renders a {@link SqlRelation.Join} as a bare FROM-clause join chain. */
    private static String printJoinBody(SqlRelation.Join j, SQLDialect dialect) {
        String kind = switch (j.type()) {
            case INNER -> "INNER JOIN";
            case LEFT  -> "LEFT OUTER JOIN";
            case RIGHT -> "RIGHT OUTER JOIN";
            case FULL  -> "FULL OUTER JOIN";
            case CROSS -> "CROSS JOIN";
        };
        StringBuilder sb = new StringBuilder();
        sb.append(printAsFromItem(j.left(),  dialect))
          .append(" ").append(kind).append(" ")
          .append(printAsFromItem(j.right(), dialect));
        if (j.type() != SqlRelation.JoinType.CROSS && j.on() != null) {
            sb.append(" ON ").append(j.on().toSql(dialect));
        }
        return sb.toString();
    }

    /**
     * Best-effort alias for a relation we need to project columns off of.
     * For a {@link SqlRelation.Join} (rendered inline in FROM), walks down the
     * left spine until an aliased leaf — that's the "source row" side of
     * Extend / Rename that the outer {@code *} should pick columns from.
     */
    private static String ensureFromAlias(SqlRelation src) {
        SqlRelation cur = src;
        while (cur instanceof SqlRelation.Join j) cur = j.left();
        return cur.alias() != null ? cur.alias() : "_s";
    }
}
