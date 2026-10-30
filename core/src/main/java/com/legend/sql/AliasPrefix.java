// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

/**
 * Every source alias of a query, and every reference to one, under a
 * PREFIX — the statement-wide alias uniqueness the renderers rely on
 * ({@code SourceSpelling}: "aliases are unique per statement, so one
 * alias → outputs scope over the whole statement is correct"). A plan
 * lowered on its own mints {@code t0, t1, …}; when several such plans
 * are fused into ONE statement (leg 3.4 step 2: a frame's plan as a CTE
 * beside the sides that read it) their aliases would collide — and a
 * reader's {@code t0.id} over the frame CTE would re-spell the frame
 * body's own {@code t0.id} over its base table (witness: H2's
 * {@code Column "t0.id" not found}). A frame body is self-contained
 * (planned standalone), so prefixing every alias inside it is exact.
 */
public final class AliasPrefix extends SqlRewriter {

    private final String prefix;

    private AliasPrefix(String prefix) {
        this.prefix = prefix;
    }

    public static SqlQuery apply(String prefix, SqlQuery q) {
        return new AliasPrefix(prefix).rewriteRoot(q);
    }

    private String p(String alias) {
        return prefix + alias;
    }

    @Override
    protected SqlSource source(SqlSource s) {
        return switch (s) {
            case SqlSource.Dual d -> d;
            case SqlSource.Join j -> j;   // no alias of its own; children renamed
            case SqlSource.Table t -> new SqlSource.Table(t.name(), p(t.alias()), t.outputs(), t.call(), t.storedTypes(), t.quoting());
            case SqlSource.Cte c -> new SqlSource.Cte(c.name(), p(c.alias()), c.outputs());
            case SqlSource.SourceUrl u -> new SqlSource.SourceUrl(u.url(), p(u.alias()), u.outputs());
            case SqlSource.VarSetPlaceholder vp ->
                    new SqlSource.VarSetPlaceholder(vp.varName(), p(vp.alias()), vp.outputs());
            case SqlSource.RawSql r -> new SqlSource.RawSql(r.sql(), p(r.alias()), r.outputs(), r.writtenFor());
            case SqlSource.Catalog c -> new SqlSource.Catalog(c.grid(), c.schemaPattern(), c.tablePattern(),
                    c.columnPattern(), p(c.alias()), c.outputs());
            case SqlSource.Subselect sub ->
                    new SqlSource.Subselect(sub.inner(), p(sub.alias()), sub.frameName());
            case SqlSource.Values v ->
                    new SqlSource.Values(v.rows(), v.columns(), p(v.alias()), v.outputs());
            case SqlSource.Pivot pv -> new SqlSource.Pivot(pv.source(), pv.on(), pv.in(), pv.named(),
                    pv.usings(), p(pv.alias()), pv.outputs());
        };
    }

    @Override
    protected SqlExpr expr(SqlExpr e) {
        return switch (e) {
            case SqlExpr.Column c when c.table() != null ->
                    new SqlExpr.Column(p(c.table()), c.name(), c.type(), c.origin());
            case SqlExpr.Star st when st.table() != null -> new SqlExpr.Star(p(st.table()));
            case SqlExpr.RowOrder ro when ro.table() != null -> new SqlExpr.RowOrder(p(ro.table()));
            default -> e;
        };
    }

    /** The prefix a frame's body carries: the frame's own name, doubled
     * underscore — never a reader's alias ({@code <frame>_t<n>}). */
    public static String frameBody(String frameName) {
        return frameName + "__";
    }

    /** A reader's alias over the frame: the frame's name and the reader
     * lowering's own fresh alias — unique in the statement by construction. */
    public static String frameReader(String frameName, String freshAlias) {
        return frameName + "_" + freshAlias;
    }

}
