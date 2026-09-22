// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * THE STRATEGY PASS (CARRIER_REDESIGN.md §1): rewrites SEMANTIC
 * collection nodes — ReduceCollection landed (R1); Membership,
 * CollectionSource, CollectionValue follow rung by rung — into this
 * dialect's emission. A node with no rule on this dialect survives to
 * the renderer's typed {@link DialectCapability} wall, budget-counted
 * by the portability sweep.
 *
 * <p>SINGLE-COMPILER CONTRACT (tenet #1, user-set, HARD): the Lowerer
 * emits only semantic nodes; every backend idiom — including DuckDB's
 * native {@code list()}/UNNEST/array literals — exists ONLY as a rule
 * here or a renderer hook the strategy selects. Each rung deletes the
 * corresponding direct emission upstream in the same commit
 * ({@code CarrierPurityRatchetTest} enforces the burn-down).
 */
public final class CarrierStrategies extends SqlRewriter {

    /** The declared slot for {@code name} in a source's output list,
     * or null when the list makes no claim (outputs-from-projections:
     * the rewritten projection carries the slot it preserves). */
    private static com.legend.sql.@com.legend.base.Nullable OutputCol slot(
            List<com.legend.sql.OutputCol> outs, String name) {
        for (com.legend.sql.OutputCol c : outs) {
            if (c.name().equals(name)) {
                return c;
            }
        }
        return null;
    }

    /** The single slot of a one-column frame, or null (no claim). */
    private static com.legend.sql.@com.legend.base.Nullable OutputCol slot0(
            List<com.legend.sql.OutputCol> outs) {
        return outs.isEmpty() ? null : outs.get(0);
    }


    /** The dialect's collection CAPABILITIES (§2b: a record, not a
     * binary — SQLite/MariaDB have correlated explosion but no native
     * lists; H2 has neither; DuckDB has everything). Strategy rules
     * dispatch on these. */
    public record Caps(boolean nativeLists, boolean correlatedExplode,
            boolean jsonCarrier) {
        public static final Caps DUCKDB = new Caps(true, true, true);
        /** H2: no native lists, no correlated explosion (the ONLY probed
         * backend without it), JSON constructors only. */
        public static final Caps H2 = new Caps(false, false, false);
    }

    private final Caps caps;

    /** Whether a reduction over a COMPILE-TIME literal collection folds
     * to its scalar chain here (the execution dialects: yes); the
     * engine-TEXT renderer keeps the semantic node — its own spelling
     * hooks print the engine's flat forms (joinStringsFlat). */
    private final boolean foldLiteralReductions;

    public CarrierStrategies(Caps caps) {
        this(caps, true);
    }

    public CarrierStrategies(Caps caps, boolean foldLiteralReductions) {
        this.caps = caps;
        this.foldLiteralReductions = foldLiteralReductions;
    }

    @Override
    protected com.legend.sql.SqlQuery select(SqlSelect s) {
        if (caps.nativeLists()) {
            return s;
        }
        // FULL OUTER emulation (PV3, witnessed: H2 rejects FULL OUTER
        // JOIN outright, RIGHT works — probed): LEFT branch UNION ALL
        // RIGHT branch anti-joined on a fresh copy of the LEFT source
        // (rows already covered by the LEFT branch drop out).
        if (s.from() instanceof com.legend.sql.SqlSource.Join fj
                && fj.kind() == com.legend.sql.SqlSource.Join.Kind.FULL
                && fj.on() != null) {
            com.legend.sql.SqlSource leftCopy =
                    copyWithAlias(fj.left(), "_full");
            if (leftCopy != null) {
                // the select's ORDER BY / LIMIT / OFFSET belong to the UNION,
                // not to its branches (H2 rejects `… ORDER BY … UNION ALL …`):
                // the branches are the bare select, the outer select over the
                // union carries the sort keyed by OUTPUT NAME
                SqlSelect bare = s.withOrderBy(List.of()).withLimit(null)
                        .withOffset(null);
                SqlSelect leftBranch = bare.withFrom(new com.legend.sql
                        .SqlSource.Join(fj.left(), fj.right(),
                                com.legend.sql.SqlSource.Join.Kind.LEFT,
                                fj.on()));
                SqlExpr anti = SqlExpr.Call.of(com.legend.sql.SqlFn.NOT,
                        new SqlExpr.Exists(SqlSelect.starOf(leftCopy)
                                .withProjections(List.of(
                                        new SqlSelect.Projection(
                                                new SqlExpr.IntLit(1), null,
                                                null)))
                                .withWhere(remapAlias(fj.on(),
                                        fj.left().alias(), "_full"))));
                SqlSelect rightBranch = bare.withFrom(new com.legend.sql
                        .SqlSource.Join(fj.left(), fj.right(),
                                com.legend.sql.SqlSource.Join.Kind.RIGHT,
                                fj.on()))
                        .withWhere(s.where() == null ? anti
                                : SqlExpr.Call.of(com.legend.sql.SqlFn.AND,
                                        s.where(), anti));
                com.legend.sql.SqlUnion union = new com.legend.sql.SqlUnion(
                        List.of(leftBranch, rightBranch), true, s.outputs());
                if (s.orderBy().isEmpty() && s.limit() == null
                        && s.offset() == null) {
                    return union;
                }
                return SqlSelect.starOf(new com.legend.sql.SqlSource.Subselect(
                                union, "_fullu", null))
                        .withOrderBy(s.orderBy().stream()
                                .map(k -> outputKeyedSort(k, s, "_fullu")).toList())
                        .withLimit(s.limit()).withOffset(s.offset());
            }
        }
        // EXPLODE PLACEMENTS (R3a + R5b, witnessed): a single-projection
        // SELECT unnest(arg) with no other clauses — the portable form
        // is decided by the ARG shape (literal / NULL / collect
        // subselect / sorted collect / concat / through-subselect
        // literal cells). Unwitnessed args survive to the renderer wall.
        if (s.projections().size() == 1
                && s.where() == null && s.groupBy().isEmpty()
                && s.having() == null && s.qualify() == null
                && s.orderBy().isEmpty() && s.limit() == null
                && s.offset() == null && !s.distinct()
                && s.projections().get(0).expr() instanceof SqlExpr.Call u
                && u.fn() == com.legend.sql.SqlFn.UNNEST
                && u.args().size() == 1) {
            com.legend.sql.SqlQuery ex = explode(u.args().get(0), s,
                    s.projections().get(0).alias());
            if (ex != null) {
                return ex;
            }
        }
        return s;
    }

    /** The list under the ordered-dedup idiom, or null: either the bare
     * {@code LIST_FILTER(list, (x, i) -> ...)} two-parameter filter or its
     * subquery-carrying form {@code (SELECT LIST_FILTER(_ddc.l, ...) AS v
     * FROM (SELECT list AS l) AS _ddc)}. */
    private static @com.legend.base.Nullable SqlExpr dedupList(SqlExpr e) {
        if (e instanceof SqlExpr.Call f
                && f.fn() == com.legend.sql.SqlFn.LIST_FILTER
                && f.args().size() == 2
                && f.args().get(1) instanceof SqlExpr.Lambda l
                && l.params().size() == 2) {
            return f.args().get(0);
        }
        if (e instanceof SqlExpr.ScalarSubquery sq
                && sq.subquery() instanceof SqlSelect sel
                && sel.projections().size() == 1
                && sel.from() instanceof com.legend.sql.SqlSource.Subselect carry
                && carry.inner() instanceof SqlSelect cs
                && cs.projections().size() == 1
                && cs.from() instanceof com.legend.sql.SqlSource.Dual
                && sel.projections().get(0).expr() instanceof SqlExpr.Call f2
                && f2.fn() == com.legend.sql.SqlFn.LIST_FILTER
                && f2.args().size() == 2
                && f2.args().get(0) instanceof SqlExpr.Column lc
                && carry.alias().equals(lc.table())
                && f2.args().get(1) instanceof SqlExpr.Lambda l2
                && l2.params().size() == 2) {
            return cs.projections().get(0).expr();
        }
        return null;
    }

    /** Each branch of an exploded query gains the filter predicate as a
     * WHERE over its own projected value. */
    private static com.legend.sql.SqlQuery filterBranches(
            com.legend.sql.SqlQuery q, SqlExpr.Lambda pred) {
        if (q instanceof com.legend.sql.SqlUnion u) {
            List<com.legend.sql.SqlQuery> bs = new ArrayList<>();
            for (com.legend.sql.SqlQuery b : u.branches()) {
                bs.add(filterBranches(b, pred));
            }
            return new com.legend.sql.SqlUnion(bs, u.all(), u.outputs());
        }
        SqlSelect sel = (SqlSelect) q;
        SqlExpr value = sel.projections().get(0).expr();
        SqlExpr cond = substParam(pred.body(), pred.params().get(0), value);
        return sel.withWhere(sel.where() == null ? cond
                : SqlExpr.Call.of(com.legend.sql.SqlFn.AND, sel.where(), cond));
    }

    /** DISTINCT over an exploded query: a select marks itself distinct
     * (its order keys drop — a set has no order); a union wraps. */
    private static com.legend.sql.SqlQuery distinctOf(
            com.legend.sql.SqlQuery q, SqlSelect outer) {
        if (q instanceof SqlSelect sel) {
            return new SqlSelect(sel.projections(), true, sel.from(),
                    sel.where(), sel.groupBy(), sel.having(), sel.qualify(),
                    List.of(), sel.limit(), sel.offset(), sel.outputs());
        }
        return new SqlSelect(
                List.of(new SqlSelect.Projection(new SqlExpr.Star(null),
                        null, null)),
                true, new com.legend.sql.SqlSource.Subselect(q, "dedup_src",
                        null),
                null, List.of(), null, null, List.of(), null, null,
                outer.outputs());
    }

    /** STATIC PIVOT EMULATION (PV1, witnessed: the PCT pivot family):
     * a PIVOT whose IN values are compile-time literals is GROUP BY
     * over the non-pivoted columns with one filtered aggregate per
     * value x using — {@code AGG(CASE WHEN on = v THEN arg END) AS
     * "v__|__alias"} (the reference's pivot-column naming). Dynamic
     * pivot (empty IN) stays a loud wall — the output columns are
     * data-dependent. Single ON key only (the witnessed shape). */
    @Override
    protected com.legend.sql.SqlSource source(com.legend.sql.SqlSource s) {
        if (caps.nativeLists()) {
            return s;
        }
        if (s instanceof com.legend.sql.SqlSource.Join aj
                && aj.kind() == com.legend.sql.SqlSource.Join.Kind.ASOF_LEFT
                && aj.on() != null) {
            com.legend.sql.SqlSource em = asOfEmulation(aj);
            if (em != null) {
                return em;
            }
        }
        if (!(s instanceof com.legend.sql.SqlSource.Pivot p)
                || p.in().isEmpty() || p.on().size() != 1) {
            return s;
        }
        List<SqlSelect.Projection> ps = new ArrayList<>();
        List<SqlExpr> group = new ArrayList<>();
        for (com.legend.sql.OutputCol oc : p.outputs()) {
            if (!oc.name().contains("__|__")) {
                SqlExpr g = SqlExpr.Column.of(p.source().alias(), oc);
                group.add(g);
                ps.add(new SqlSelect.Projection(g, oc.name(), oc));
            }
        }
        for (SqlExpr v : p.in()) {
            for (com.legend.sql.SqlSource.Pivot.Using u : p.usings()) {
                SqlAgg.Reducer agg = u.agg();
                List<SqlExpr> args = new ArrayList<>(agg.args());
                if (args.isEmpty()) {
                    return s;   // COUNT(*)-style using: unwitnessed
                }
                args.set(0, new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                        SqlExpr.Call.of(com.legend.sql.SqlFn.EQUAL,
                                p.on().get(0), v),
                        args.get(0))), null));
                String pname = litText(v) + "__|__" + u.alias();
                ps.add(new SqlSelect.Projection(
                        new SqlAgg.Reducer(agg.fn(), args, agg.distinct(),
                                agg.orderBy()),
                        pname, slot(p.outputs(), pname)));
            }
        }
        SqlSelect sel = SqlSelect.starOf(p.source())
                .withProjections(ps)
                .withGroupBy(group);
        return new com.legend.sql.SqlSource.Subselect(sel, p.alias(), null);
    }

    /** AS-OF EMULATION (PV2): {@code l ASOF LEFT JOIN r ON eqs AND
     * ineq} joins each left row to THE right row with the extreme
     * as-of key satisfying the inequality — portable form: a plain
     * LEFT JOIN whose ON pins the right key to the correlated
     * MAX (key bounded above) / MIN (bounded below) over a fresh copy
     * of the right source. No match -> the pick is NULL -> the LEFT
     * JOIN null-extends, exactly ASOF's miss behavior. Right sources
     * beyond Table/Subselect (or an unrecognizable inequality) decline
     * to the loud wall. */
    private static com.legend.sql.@com.legend.base.Nullable SqlSource asOfEmulation(
            com.legend.sql.SqlSource.Join j) {
        String rightAlias = j.right().alias();
        List<SqlExpr> conjuncts = new ArrayList<>();
        flattenAnd(java.util.Objects.requireNonNull(j.on()), conjuncts);
        SqlExpr rightKey = null;
        boolean pickMax = false;
        for (SqlExpr c : conjuncts) {
            if (c instanceof SqlExpr.Call cc && cc.args().size() == 2) {
                boolean r0 = mentionsAlias(cc.args().get(0), rightAlias);
                boolean r1 = mentionsAlias(cc.args().get(1), rightAlias);
                if (r0 == r1) {
                    continue;
                }
                switch (cc.fn()) {
                    case GREATER_EQUAL, GREATER -> {
                        rightKey = r1 ? cc.args().get(1) : cc.args().get(0);
                        pickMax = r1;      // l >= r: bounded above -> MAX
                    }
                    case LESS_EQUAL, LESS -> {
                        rightKey = r0 ? cc.args().get(0) : cc.args().get(1);
                        pickMax = r0;      // r <= l: bounded above -> MAX
                    }
                    default -> {
                        continue;
                    }
                }
            }
        }
        if (rightKey == null) {
            return null;
        }
        com.legend.sql.SqlSource copy = copyWithAlias(j.right(), "_asof");
        if (copy == null) {
            return null;
        }
        List<SqlExpr> remapped = new ArrayList<>();
        for (SqlExpr c : conjuncts) {
            remapped.add(remapAlias(c, rightAlias, "_asof"));
        }
        SqlExpr where = null;
        for (SqlExpr c : remapped) {
            where = where == null ? c
                    : SqlExpr.Call.of(com.legend.sql.SqlFn.AND, where, c);
        }
        SqlSelect pick = SqlSelect.starOf(copy)
                .withProjections(List.of(new SqlSelect.Projection(
                        new SqlAgg.Reducer(pickMax ? SqlAgg.Fn.MAX
                                : SqlAgg.Fn.MIN,
                                List.of(remapAlias(rightKey, rightAlias,
                                        "_asof")),
                                false, List.of()),
                        null, null)))
                .withWhere(where);
        SqlExpr on = SqlExpr.Call.of(com.legend.sql.SqlFn.AND,
                java.util.Objects.requireNonNull(j.on()),
                SqlExpr.Call.of(com.legend.sql.SqlFn.EQUAL, rightKey,
                        new SqlExpr.ScalarSubquery(pick)));
        return new com.legend.sql.SqlSource.Join(j.left(), j.right(),
                com.legend.sql.SqlSource.Join.Kind.LEFT, on);
    }

    /** A fresh re-aliased copy of a simple source (Table / Subselect /
     * Values), or null — the correlated-copy pattern the ASOF and FULL
     * emulations share. */
    private static com.legend.sql.@com.legend.base.Nullable SqlSource copyWithAlias(
            com.legend.sql.SqlSource src, String alias) {
        return switch (src) {
            case com.legend.sql.SqlSource.Table t ->
                    new com.legend.sql.SqlSource.Table(t.name(), alias,
                            t.outputs());
            case com.legend.sql.SqlSource.Cte c ->
                    new com.legend.sql.SqlSource.Cte(c.name(), alias, c.outputs());
            case com.legend.sql.SqlSource.Subselect sub ->
                    new com.legend.sql.SqlSource.Subselect(sub.inner(),
                            alias, null);
            case com.legend.sql.SqlSource.Values v ->
                    new com.legend.sql.SqlSource.Values(v.rows(),
                            v.columns(), alias, v.outputs());
            default -> null;
        };
    }

    private static void flattenAnd(SqlExpr e, List<SqlExpr> out) {
        if (e instanceof SqlExpr.Call c
                && c.fn() == com.legend.sql.SqlFn.AND) {
            for (SqlExpr a : c.args()) {
                flattenAnd(a, out);
            }
            return;
        }
        out.add(e);
    }

    private static boolean mentionsAlias(SqlExpr e, String alias) {
        if (e instanceof SqlExpr.Column c && alias.equals(c.table())) {
            return true;
        }
        for (SqlExpr k : e.children()) {
            if (mentionsAlias(k, alias)) {
                return true;
            }
        }
        return false;
    }

    /** A sort key over the emulated union's OUTER select: the key addresses
     * the union's OUTPUT (by the key's own output name, else the projection
     * whose expression the key is) — a key naming neither is loud. */
    private static SqlSelect.SortKey outputKeyedSort(SqlSelect.SortKey k,
            SqlSelect s, String outerAlias) {
        com.legend.sql.OutputCol out = null;
        for (int i = 0; i < s.projections().size(); i++) {
            com.legend.sql.OutputCol o = s.outputs().get(i);
            if (o.name().equals(k.outputName())
                    || s.projections().get(i).expr().equals(k.expr())) {
                out = o;
                break;
            }
        }
        if (out == null && s.projections().isEmpty()
                && k.expr() instanceof SqlExpr.Column kc) {
            // a STAR select: the outputs are the expanded columns; the key
            // names one of them — uniquely, or loud (two sides of a join may
            // both carry the name; the union then has no addressable key)
            List<com.legend.sql.OutputCol> named = s.outputs().stream()
                    .filter(o -> o.name().equals(kc.name())).toList();
            if (named.size() == 1) {
                out = named.get(0);
            }
        }
        if (out == null) {
            throw new DialectCapability(
                    "FULL OUTER JOIN emulation: sort key " + k.expr()
                    + " is not one of the select's outputs "
                    + s.outputs().stream().map(com.legend.sql.OutputCol::name).toList());
        }
        // the union's output is a DERIVED frame column of the outer select
        return new SqlSelect.SortKey(SqlExpr.Column.of(outerAlias, out.name(),
                out.type(), out.nullable(), com.legend.sql.OutputCol.Origin.DERIVED),
                k.ascending(), k.nullOrder(), out.name());
    }

    private static SqlExpr remapAlias(SqlExpr e, String from, String to) {
        if (e instanceof SqlExpr.Column c && from.equals(c.table())) {
            // alias remap transports the stamped type (M2: a derived
            // reference never drops leaf knowledge)
            return new SqlExpr.Column(to, c.name(), c.type(), c.origin());
        }
        List<SqlExpr> kids = e.children();
        if (kids.isEmpty()) {
            return e;
        }
        List<SqlExpr> mapped = new ArrayList<>(kids.size());
        boolean changed = false;
        for (SqlExpr k : kids) {
            SqlExpr m = remapAlias(k, from, to);
            changed |= m != k;
            mapped.add(m);
        }
        return changed ? e.withChildren(mapped) : e;
    }

    /** A pivot IN literal's COLUMN-NAME text (the reference prints the
     * value verbatim: strings bare, numbers plain). */
    private static String litText(SqlExpr v) {
        return switch (v) {
            case SqlExpr.StringLit s2 -> s2.value();
            case SqlExpr.IntLit i -> String.valueOf(i.value());
            case SqlExpr.BoolLit b -> String.valueOf(b.value());
            default -> v.toString();
        };
    }

    /** The portable form of {@code SELECT unnest(arg) AS alias} for the
     * WITNESSED arg shapes (R5b), or null (the renderer wall stays).
     * Except the through-subselect arm, every rewriting arm requires a
     * bare Dual source (the exploded form replaces the whole select). */
    private @com.legend.base.Nullable com.legend.sql.SqlQuery explode(SqlExpr arg,
            SqlSelect s, @com.legend.base.Nullable String alias) {
        boolean dual = s.from() instanceof com.legend.sql.SqlSource.Dual;
        while (arg instanceof SqlExpr.CompactList cl) {
            // carrier compaction is a no-op over ROWS (a relation holds
            // no empties once its null-drop filter is a WHERE)
            arg = cl.list();
        }
        // an ARRAY-cast wrapper over a folded literal unwraps: the cast
        // only re-types the elements the literal already pins
        if (arg instanceof SqlExpr.Cast ac
                && ac.target() instanceof com.legend.sql.SqlType.Array) {
            String jl = jsonLiteral(ac.value());
            if (jl != null
                    && com.legend.sql.Json.parse(jl) instanceof List<?> ll) {
                List<SqlExpr> els = new ArrayList<>(ll.size());
                for (Object el : ll) {
                    els.add(jsonLitExpr(el));
                }
                arg = new SqlExpr.ArrayLit(els);
            }
        }
        // unnest(NULL) yields ZERO rows (probed on DuckDB) — keep the
        // select shape, kill it with WHERE FALSE.
        if (arg instanceof SqlExpr.NullLit) {
            return s.withProjections(List.of(new SqlSelect.Projection(
                            new SqlExpr.NullLit(), alias,
                            slot0(s.outputs()))))
                    .withWhere(new SqlExpr.BoolLit(false));
        }
        // LITERAL-COLLECTION EXPLODE (R3a): UNION ALL of one-row selects
        // (duplicates preserved, order = branch order).
        if (dual && arg instanceof SqlExpr.ArrayLit al
                && !al.elements().isEmpty()) {
            List<com.legend.sql.SqlQuery> branches = new ArrayList<>();
            for (SqlExpr el : al.elements()) {
                branches.add(s.withProjections(
                        List.of(new SqlSelect.Projection(el, alias,
                                slot0(s.outputs())))));
            }
            // a ONE-element literal explodes to its one row — a union
            // needs two branches (Phase 1, batch 135: 11 H2 TDG tests
            // threw here on single-element seed lists)
            return branches.size() == 1 ? branches.get(0)
                    : new com.legend.sql.SqlUnion(branches, true, s.outputs());
        }
        // EXPLODE-OF-COLLECT (R5b, witnessed): unnest((SELECT LIST(x)
        // FROM ...)) IS the collecting row set — the inner select
        // projecting the bare element, collect order keys carried over.
        if (dual) {
            SqlSelect coll = collectSelect(arg);
            if (coll != null) {
                SqlAgg.Reducer collect =
                        (SqlAgg.Reducer) coll.projections().get(0).expr();
                return coll.withProjections(
                                List.of(new SqlSelect.Projection(
                                        collect.args().get(0), alias,
                                        slot0(s.outputs()))))
                        .withOrderBy(collect.orderBy());
            }
        }
        // SORTED EXPLODE (R5b, witnessed): unnest(LIST_SORT(collect)) —
        // list_sort is ASC NULLS LAST (probed on DuckDB); the collect
        // keys stay secondary (stable-sort parity).
        if (dual && arg instanceof SqlExpr.Call so
                && so.fn() == com.legend.sql.SqlFn.LIST_SORT
                && so.args().size() == 1) {
            SqlSelect coll = collectSelect(so.args().get(0));
            if (coll != null) {
                SqlAgg.Reducer collect =
                        (SqlAgg.Reducer) coll.projections().get(0).expr();
                SqlExpr raw = collect.args().get(0);
                List<SqlSelect.SortKey> keys = new ArrayList<>();
                keys.add(new SqlSelect.SortKey(raw, true,
                        SqlSelect.SortKey.NullOrder.NULLS_LAST, null));
                keys.addAll(collect.orderBy());
                return coll.withProjections(
                                List.of(new SqlSelect.Projection(raw, alias,
                                        slot0(s.outputs()))))
                        .withOrderBy(keys);
            }
        }
        // CONCAT EXPLODE (R5b, witnessed): unnest(list_concat(a, b)) =
        // the branches of a then the branches of b.
        // a FILTERED explode (R5e, witnessed: `Product.all().name->concatenate(
        // Product.all().name)`, the null-dropping list_filter over a
        // concat of collects): explode the inner list, the predicate
        // becomes each branch's WHERE over its projected value
        if (dual && arg instanceof SqlExpr.Call lf1
                && lf1.fn() == com.legend.sql.SqlFn.LIST_FILTER
                && lf1.args().size() == 2
                && lf1.args().get(1) instanceof SqlExpr.Lambda flam1
                && flam1.params().size() == 1) {
            com.legend.sql.SqlQuery inner = explode(lf1.args().get(0), s, alias);
            if (inner != null) {
                return filterBranches(inner, flam1);
            }
        }
        // the ORDERED-DEDUP idiom (ListEncodings.orderedDedup: keep x at
        // index i iff its first position is i) over rows is DISTINCT —
        // witnessed by `->map(...)->distinct()` over a class query
        SqlExpr dedupped = dedupList(arg);
        if (dual && dedupped != null) {
            com.legend.sql.SqlQuery inner = explode(dedupped, s, alias);
            if (inner != null) {
                return distinctOf(inner, s);
            }
        }
        if (dual && arg instanceof SqlExpr.Call cc
                && cc.fn() == com.legend.sql.SqlFn.LIST_CONCAT
                && cc.args().size() >= 2) {
            List<com.legend.sql.SqlQuery> branches = new ArrayList<>();
            for (SqlExpr arm : cc.args()) {
                com.legend.sql.SqlQuery b = explode(arm, s, alias);
                if (b == null) {
                    return null;
                }
                if (b instanceof com.legend.sql.SqlUnion bu) {
                    branches.addAll(bu.branches());
                } else {
                    branches.add(b);
                }
            }
            return new com.legend.sql.SqlUnion(branches, true, s.outputs());
        }
        // THROUGH-SUBSELECT CELLS (R5b, witnessed): SELECT unnest(c)
        // FROM (SELECT [e1..ek] AS c FROM T ...) — k branches of the
        // INNER select each projecting one cell. k = 1 is exact; k > 1
        // is column-major row order where DuckDB unnest is row-major —
        // an observed divergence fails the sweep loudly, never silently.
        if (arg instanceof SqlExpr.Column c
                && s.from() instanceof com.legend.sql.SqlSource.Subselect us
                && us.inner() instanceof SqlSelect inner
                && !inner.distinct() && inner.groupBy().isEmpty()
                && inner.having() == null && inner.qualify() == null
                && inner.orderBy().isEmpty() && inner.limit() == null
                && inner.offset() == null) {
            SqlExpr src = null;
            for (SqlSelect.Projection ip : inner.projections()) {
                if (c.name().equals(ip.alias())) {
                    src = ip.expr();
                }
            }
            if (src instanceof SqlExpr.ArrayLit cells
                    && !cells.elements().isEmpty()) {
                List<com.legend.sql.SqlQuery> branches = new ArrayList<>();
                for (SqlExpr cell : cells.elements()) {
                    branches.add(inner.withProjections(
                            List.of(new SqlSelect.Projection(cell, alias,
                                    slot0(s.outputs())))));
                }
                return branches.size() == 1 ? branches.get(0)
                        : new com.legend.sql.SqlUnion(branches, true,
                                s.outputs());
            }
        }
        return null;
    }

    /** The collect SELECT beneath a ScalarSubquery — single projection,
     * a bare non-distinct one-arg LIST reducer, no other clauses — or
     * null. */
    private static @com.legend.base.Nullable SqlSelect collectSelect(SqlExpr e) {
        return e instanceof SqlExpr.ScalarSubquery sq
                && sq.subquery() instanceof SqlSelect sel
                && sel.projections().size() == 1
                && sel.projections().get(0).expr() instanceof SqlAgg.Reducer r
                && r.fn() == SqlAgg.Fn.LIST && !r.distinct()
                && r.args().size() == 1
                && sel.groupBy().isEmpty() && sel.having() == null
                && sel.qualify() == null && sel.orderBy().isEmpty()
                && sel.limit() == null && sel.offset() == null
                && !sel.distinct()
                ? sel : null;
    }

    /** LIST_* reducers over collection values ARE ReduceCollection —
     * mapped here so the fuse rules apply (portable mode only; DuckDB
     * keeps its native list fns). */
    private static final java.util.Map<com.legend.sql.SqlFn, SqlAgg.Fn>
            LIST_REDUCERS = java.util.Map.of(
                    com.legend.sql.SqlFn.LIST_MIN, SqlAgg.Fn.MIN,
                    com.legend.sql.SqlFn.LIST_MAX, SqlAgg.Fn.MAX,
                    com.legend.sql.SqlFn.LIST_SUM, SqlAgg.Fn.SUM,
                    com.legend.sql.SqlFn.LIST_AVG, SqlAgg.Fn.AVG,
                    com.legend.sql.SqlFn.LIST_MEDIAN, SqlAgg.Fn.MEDIAN);

    @Override
    protected SqlExpr expr(SqlExpr e) {
        if (caps.nativeLists()) {
            return e;
        }
        // F10 3b: the LITERAL carrier's marker cast is a LABEL device
        // (scalarRoot reads it) — on a list-less backend it must not
        // reach the renderer as a cast over an array; strip it here so
        // the EXISTING array strategies see the shapes they own (the
        // cells are self-describing text either way — the total-reader
        // property).
        if (e instanceof SqlExpr.Cast mk
                && (mk.target() == com.legend.sql.SqlType.Scalar.LITERAL
                        || (mk.target() instanceof com.legend.sql.SqlType.Array ma
                                && ma.element()
                                        == com.legend.sql.SqlType.Scalar.LITERAL))) {
            return expr(mk.value());
        }
        // LIST_CONCAT over compile-time collections FOLDS (R5b,
        // witnessed: month-name lists concatenated before explode).
        // Bottom-up walk: nested concats fold inside-out.
        if (e instanceof SqlExpr.Call cf
                && cf.fn() == com.legend.sql.SqlFn.LIST_CONCAT
                && !cf.args().isEmpty()
                && cf.args().stream()
                        .allMatch(x -> x instanceof SqlExpr.ArrayLit)) {
            List<SqlExpr> els = new ArrayList<>();
            for (SqlExpr x : cf.args()) {
                els.addAll(((SqlExpr.ArrayLit) x).elements());
            }
            return new SqlExpr.ArrayLit(els);
        }
        if (e instanceof SqlExpr.Call lc && lc.args().size() == 1) {
            SqlAgg.Fn red = LIST_REDUCERS.get(lc.fn());
            if (red != null) {
                SqlExpr fused = fuse(new SqlExpr.ReduceCollection(red,
                        lc.args().get(0), List.of()), foldLiteralReductions);
                if (fused != null) {
                    return fused;
                }
            }
        }
        // FUSION (R1, the engine's shape — pureToSQLQuery aggregates
        // inside the isolated grouped subselect, never a list value):
        // reducing a COLLECTING SUBSELECT pushes the reduction into it.
        //   ReduceCollection(name, (SELECT LIST(x) FROM ...), extras)
        //     -> (SELECT NAME(x, extras...) FROM ...)
        // The collect's ORDER KEYS carry over — the ordering contract
        // (insertion order via RowOrder) is preserved, not re-derived.
        // LIST_LENGTH (P2, witnessed: the at() out-of-bounds guard over
        // literal collections, and length over collects): a literal
        // collection's length is COMPILE-TIME (len counts elements,
        // NULLs included); a collect's length is COUNT(*) over the same
        // rows (a LIST of N rows has N elements, NULLs included).
        if (e instanceof SqlExpr.Call ll
                && ll.fn() == com.legend.sql.SqlFn.LIST_LENGTH
                && ll.args().size() == 1) {
            if (ll.args().get(0) instanceof SqlExpr.ArrayLit la) {
                return new SqlExpr.IntLit(la.elements().size());
            }
            // token count over a split (witnessed: the at() guard over
            // split tokens): separator occurrences + 1 — single-char
            // literal separator only (the count is LENGTH-difference).
            if (ll.args().get(0) instanceof SqlExpr.Call sp2
                    && sp2.fn() == com.legend.sql.SqlFn.SPLIT
                    && sp2.args().size() == 2
                    && sp2.args().get(1) instanceof SqlExpr.StringLit sl2
                    && sl2.value().length() == 1) {
                SqlExpr s0 = sp2.args().get(0);
                return SqlExpr.Call.of(com.legend.sql.SqlFn.PLUS,
                        SqlExpr.Call.of(com.legend.sql.SqlFn.MINUS,
                                SqlExpr.Call.of(com.legend.sql.SqlFn.LENGTH,
                                        s0),
                                SqlExpr.Call.of(com.legend.sql.SqlFn.LENGTH,
                                        SqlExpr.Call.of(
                                                com.legend.sql.SqlFn.REPLACE,
                                                s0, sp2.args().get(1),
                                                new SqlExpr.StringLit("")))),
                        new SqlExpr.IntLit(1));
            }
            SqlSelect sel = collectSelect(ll.args().get(0));
            if (sel != null) {
                return new SqlExpr.ScalarSubquery(sel.withProjections(
                        List.of(new SqlSelect.Projection(
                                new SqlAgg.Reducer(SqlAgg.Fn.COUNT,
                                        List.of(), false, List.of()),
                                sel.projections().get(0).alias(),
                                sel.projections().get(0).out()))));
            }
        }
        // LITERAL-VARIANT const-folds (PV3, witnessed: PCT navigates
        // CAST('[...]' AS JSON) literal chains): the JSON text is
        // compile-time — VARIANT_GET picks the sub-node, ELEMENTS
        // explodes to the literal array; each survivor re-emits as a
        // JSON cast literal so further navigation keeps folding.
        // elements of an already-literal collection ARE the collection
        if (e instanceof SqlExpr.Call ve
                && ve.fn() == com.legend.sql.SqlFn.VARIANT_ELEMENTS
                && ve.args().size() == 1
                && ve.args().get(0) instanceof SqlExpr.ArrayLit) {
            return ve.args().get(0);
        }
        if (e instanceof SqlExpr.Call vg
                && (vg.fn() == com.legend.sql.SqlFn.VARIANT_GET
                        || vg.fn() == com.legend.sql.SqlFn.VARIANT_ELEMENTS)) {
            String lit = jsonLiteral(vg.args().get(0));
            if (lit != null) {
                Object node = com.legend.sql.Json.parse(lit);
                if (vg.fn() == com.legend.sql.SqlFn.VARIANT_GET
                        && vg.args().size() == 2) {
                    Object picked = null;
                    boolean ok = false;
                    if (vg.args().get(1) instanceof SqlExpr.IntLit ix
                            && node instanceof List<?> l
                            && ix.value() >= 0 && ix.value() < l.size()) {
                        picked = l.get((int) ix.value());
                        ok = true;
                    } else if (vg.args().get(1) instanceof SqlExpr.StringLit k
                            && node instanceof java.util.Map<?, ?> m
                            && m.containsKey(k.value())) {
                        picked = m.get(k.value());
                        ok = true;
                    }
                    if (ok) {
                        return jsonLitExpr(picked);
                    }
                }
                if (vg.fn() == com.legend.sql.SqlFn.VARIANT_ELEMENTS
                        && node instanceof List<?> l) {
                    List<SqlExpr> els = new ArrayList<>(l.size());
                    for (Object el : l) {
                        els.add(jsonLitExpr(el));
                    }
                    return new SqlExpr.ArrayLit(els);
                }
            }
        }
        // TYPEOF date dispatch (R5d, witnessed: Fold.jsonDateWrap's
        // runtime precision probe): typeof(e) = 'DATE' is a LENGTH test
        // on the VARCHAR cast — probed both engines: a DATE casts to 10
        // chars, a TIMESTAMP to 19+.
        if (e instanceof SqlExpr.Call eq
                && eq.fn() == com.legend.sql.SqlFn.EQUAL
                && eq.args().size() == 2
                && eq.args().get(0) instanceof SqlExpr.Call tf
                && tf.fn() == com.legend.sql.SqlFn.TYPEOF
                && tf.args().size() == 1
                && eq.args().get(1) instanceof SqlExpr.StringLit ts
                && "DATE".equals(ts.value())) {
            return SqlExpr.Call.of(com.legend.sql.SqlFn.EQUAL,
                    SqlExpr.Call.of(com.legend.sql.SqlFn.LENGTH,
                            new SqlExpr.Cast(tf.args().get(0),
                                    com.legend.sql.SqlType.Scalar.VARCHAR)),
                    new SqlExpr.IntLit(10));
        }
        // sorted collect VALUE (R5d, witnessed): LIST_SORT(collect) IS
        // the collect ordered by its value — ASC NULLS LAST (probed
        // list_sort contract), original collect keys as tiebreak.
        if (e instanceof SqlExpr.Call ls
                && ls.fn() == com.legend.sql.SqlFn.LIST_SORT
                && ls.args().size() == 1) {
            SqlSelect sel = collectSelect(ls.args().get(0));
            if (sel != null) {
                SqlAgg.Reducer collect =
                        (SqlAgg.Reducer) sel.projections().get(0).expr();
                SqlExpr raw = collect.args().get(0);
                List<SqlSelect.SortKey> keys = new ArrayList<>();
                keys.add(new SqlSelect.SortKey(raw, true,
                        SqlSelect.SortKey.NullOrder.NULLS_LAST, null));
                keys.addAll(collect.orderBy());
                return new SqlExpr.ScalarSubquery(sel.withProjections(
                        List.of(new SqlSelect.Projection(
                                new SqlAgg.Reducer(SqlAgg.Fn.LIST,
                                        collect.args(), false, keys),
                                sel.projections().get(0).alias(),
                                sel.projections().get(0).out()))));
            }
        }
        // filtered collect VALUE (R5d, witnessed): LIST_FILTER(collect,
        // lam) pushes the element predicate into the collect's WHERE —
        // element-wise filter IS a row filter before aggregation, order
        // preserved.
        if (e instanceof SqlExpr.Call lf
                && lf.fn() == com.legend.sql.SqlFn.LIST_FILTER
                && lf.args().size() == 2
                && lf.args().get(1) instanceof SqlExpr.Lambda flam
                && flam.params().size() == 1) {
            SqlSelect sel = collectSelect(lf.args().get(0));
            if (sel != null) {
                SqlAgg.Reducer collect =
                        (SqlAgg.Reducer) sel.projections().get(0).expr();
                SqlExpr pred = substParam(flam.body(), flam.params().get(0),
                        collect.args().get(0));
                return new SqlExpr.ScalarSubquery(sel.withWhere(
                        sel.where() == null ? pred
                                : SqlExpr.Call.of(com.legend.sql.SqlFn.AND,
                                        sel.where(), pred)));
            }
        }
        // literal reducer folds (R5d, witnessed): BOOL_AND/BOOL_OR/SUM/
        // PRODUCT over a compile-time collection — the exact
        // null-ignoring aggregate fold (all-null -> NULL, probed).
        if (e instanceof SqlExpr.Call lr && lr.args().size() == 1
                && lr.args().get(0) instanceof SqlExpr.ArrayLit lra
                && !lra.elements().isEmpty()) {
            SqlExpr folded = switch (lr.fn()) {
                case LIST_BOOL_AND -> litFold(lra.elements(),
                        new SqlExpr.BoolLit(true), com.legend.sql.SqlFn.AND,
                        null);
                case LIST_BOOL_OR -> litFold(lra.elements(),
                        new SqlExpr.BoolLit(false), com.legend.sql.SqlFn.OR,
                        null);
                case LIST_SUM -> litFold(lra.elements(),
                        new SqlExpr.IntLit(0), com.legend.sql.SqlFn.PLUS,
                        null);
                // list product is DOUBLE on the reference (probed 4.0)
                // — the leading 1.0 factor pins the type.
                case LIST_PRODUCT -> litFold(lra.elements(),
                        new SqlExpr.IntLit(1), com.legend.sql.SqlFn.TIMES,
                        new SqlExpr.FloatLit(1.0));
                default -> null;
            };
            if (folded != null) {
                return folded;
            }
        }
        if (e instanceof SqlExpr.Call lg
                && lg.fn() == com.legend.sql.SqlFn.LIST_GET
                && lg.args().size() == 2) {
            SqlExpr got = listGetRule(lg.args().get(0), lg.args().get(1));
            if (got != null) {
                return got;
            }
        }
        if (e instanceof SqlExpr.Membership m) {
            SqlExpr rewritten = membershipRule(m);
            if (rewritten != null) {
                return rewritten;
            }
        }
        if (e instanceof SqlExpr.ReduceCollection rc) {
            SqlExpr fusedSub = fuse(rc, foldLiteralReductions);
            if (fusedSub != null) {
                return fusedSub;
            }
        }
        return e;
    }

    /** The fused grouped-subselect, or null when the collection operand
     * is not a recognized collect shape. Witnessed shapes (R1b, corpus):
     * a bare collect subselect, and LIST_TRANSFORM(collect, lambda) —
     * the element transform SUBSTITUTES into the collect projection
     * (same rows: the transform is element-wise). Order keys carry over
     * (the ordering contract, never re-derived). */
    private static @com.legend.base.Nullable SqlExpr fuse(
            SqlExpr.ReduceCollection rc, boolean foldLiterals) {
        SqlExpr coll = rc.collection();
        SqlExpr.Lambda transform = null;
        if (coll instanceof SqlExpr.Call c
                && c.fn() == com.legend.sql.SqlFn.LIST_TRANSFORM
                && c.args().size() == 2
                && c.args().get(1) instanceof SqlExpr.Lambda lam
                && lam.params().size() == 1) {
            transform = lam;
            coll = c.args().get(0);
        }
        // SORTED join (witnessed R1d: sort()->joinStrings): LIST_SORT
        // between collect and transform — the fused STRING_AGG orders by
        // the RAW collected value (sort precedes the element transform).
        boolean sorted = false;
        if (coll instanceof SqlExpr.Call sc
                && sc.fn() == com.legend.sql.SqlFn.LIST_SORT
                && sc.args().size() == 1) {
            sorted = true;
            coll = sc.args().get(0);
        }
        // LITERAL collection (witnessed R1c: makeString over TDS-row
        // cells): the elements are compile-time-known — STRING_AGG
        // expands to the CONCAT chain t(e1)||sep||t(e2)||…; no subquery
        // at all. STRING_AGG only (join semantics).
        if (coll instanceof SqlExpr.ArrayLit al
                && rc.reducer() == SqlAgg.Fn.STRING_AGG
                && rc.extras().size() == 1 && !al.elements().isEmpty()) {
            return foldLiterals
                    ? concatJoin(al.elements(), transform, rc.extras().get(0))
                    : null;
        }
        // SINGLETON-FLATTEN UNWRAP (witnessed R4: calendar date ranges —
        // each row carries a ONE-element ArrayLit; FLATTEN(collect) of
        // singletons IS a collect of the elements): rewrite the inner
        // projection to the bare element and drop the FLATTEN, then the
        // generic collect rules apply (MIN/STRING_AGG/sorted...).
        if (coll instanceof SqlExpr.Call ufl
                && ufl.fn() == com.legend.sql.SqlFn.LIST_FLATTEN
                && ufl.args().size() == 1
                && ufl.args().get(0) instanceof SqlExpr.ScalarSubquery usq
                && usq.subquery() instanceof SqlSelect usel
                && usel.projections().size() == 1
                && usel.projections().get(0).expr()
                        instanceof SqlAgg.Reducer ucollect
                && ucollect.fn() == SqlAgg.Fn.LIST
                && !ucollect.distinct()
                && ucollect.args().size() == 1
                && ucollect.args().get(0) instanceof SqlExpr.Column ucol
                && usel.from() instanceof com.legend.sql.SqlSource.Subselect
                        usub
                && usub.inner() instanceof SqlSelect uinner
                && uinner.projections().size() == 1
                && ucol.name().equals(uinner.projections().get(0).alias())
                && uinner.projections().get(0).expr()
                        instanceof SqlExpr.ArrayLit ual
                && ual.elements().size() == 1) {
            SqlSelect newInner = uinner.withProjections(
                    List.of(new SqlSelect.Projection(ual.elements().get(0),
                            uinner.projections().get(0).alias(),
                            uinner.projections().get(0).out())));
            coll = new SqlExpr.ScalarSubquery(usel.withFrom(
                    new com.legend.sql.SqlSource.Subselect(newInner,
                            usub.alias(), usub.frameName())));
        }
        // ROW-MAJOR cell collect (witnessed R1c: rowMajorCellList —
        // FLATTEN(collect-of-ArrayLit)): fuse to STRING_AGG over the
        // per-row CONCAT of transformed cells, sep between rows AND
        // between cells (row-major join is separator-uniform).
        if (coll instanceof SqlExpr.Call fl
                && fl.fn() == com.legend.sql.SqlFn.LIST_FLATTEN
                && fl.args().size() == 1
                && fl.args().get(0) instanceof SqlExpr.ScalarSubquery fsq
                && fsq.subquery() instanceof SqlSelect fsel
                && fsel.projections().size() == 1
                && fsel.projections().get(0).expr()
                        instanceof SqlAgg.Reducer fcollect
                && fcollect.fn() == SqlAgg.Fn.LIST
                && !fcollect.distinct()
                && fcollect.args().size() == 1
                && fcollect.args().get(0) instanceof SqlExpr.ArrayLit cells
                && rc.reducer() == SqlAgg.Fn.STRING_AGG
                && rc.extras().size() == 1
                && !cells.elements().isEmpty()) {
            SqlExpr sep = rc.extras().get(0);
            if (!sorted) {
                SqlExpr rowJoined = concatJoin(cells.elements(), transform,
                        sep);
                SqlAgg.Reducer fused = new SqlAgg.Reducer(
                        SqlAgg.Fn.STRING_AGG, List.of(rowJoined, sep),
                        false, fcollect.orderBy());
                return new SqlExpr.ScalarSubquery(fsel.withProjections(
                        List.of(new SqlSelect.Projection(fused,
                                fsel.projections().get(0).alias(),
                                fsel.projections().get(0).out()))));
            }
            // SORTED row-major join (witnessed R1d): cells sort GLOBALLY
            // across rows — per-row CONCAT cannot express it. The
            // no-explode portable form: UNION ALL one branch per
            // compile-time cell, then STRING_AGG(t(v), sep ORDER BY v).
            List<com.legend.sql.SqlQuery> branches = new ArrayList<>();
            for (SqlExpr cell : cells.elements()) {
                branches.add(fsel.withProjections(
                        List.of(new SqlSelect.Projection(cell, "v", null))));
            }
            // one compile-time cell = one branch, no union (batch 135)
            com.legend.sql.SqlQuery union = branches.size() == 1 ? branches.get(0)
                    : new com.legend.sql.SqlUnion(branches, true, List.of());
            SqlExpr vRead = SqlExpr.Column.derived("_cells", "v");
            SqlExpr tv = transform == null ? vRead
                    : substParam(transform.body(),
                            transform.params().get(0), vRead);
            SqlAgg.Reducer fused = new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG,
                    List.of(tv, sep), false,
                    List.of(SqlSelect.SortKey.asc(vRead)));
            return new SqlExpr.ScalarSubquery(SqlSelect.starOf(
                            new com.legend.sql.SqlSource.Subselect(union,
                                    "_cells", null))
                    .withProjections(List.of(new SqlSelect.Projection(
                            fused, null, null))));
        }
        // THROUGH-SUBSELECT ROW-MAJOR (R5c, witnessed): FLATTEN(collect)
        // where the collected COLUMN resolves to an ArrayLit in the
        // inner subselect's projection — the cells live in INNER scope,
        // so the per-row CONCAT substitutes into the INNER projection
        // (unsorted), and the sorted global-cell form explodes per-cell
        // branches of the INNER select.
        if (coll instanceof SqlExpr.Call fl2
                && fl2.fn() == com.legend.sql.SqlFn.LIST_FLATTEN
                && fl2.args().size() == 1
                && fl2.args().get(0) instanceof SqlExpr.ScalarSubquery fsq2
                && fsq2.subquery() instanceof SqlSelect fsel2
                && fsel2.projections().size() == 1
                && fsel2.projections().get(0).expr()
                        instanceof SqlAgg.Reducer fc2
                && fc2.fn() == SqlAgg.Fn.LIST && !fc2.distinct()
                && fc2.args().size() == 1
                && fc2.args().get(0) instanceof SqlExpr.Column fcol
                && fsel2.from() instanceof com.legend.sql.SqlSource.Subselect
                        fsub
                && fsub.inner() instanceof SqlSelect finner
                && !finner.distinct() && finner.groupBy().isEmpty()
                && finner.having() == null && finner.qualify() == null
                && finner.limit() == null && finner.offset() == null
                && rc.reducer() == SqlAgg.Fn.STRING_AGG
                && rc.extras().size() == 1) {
            SqlExpr src = null;
            int srcIx = -1;
            for (int i = 0; i < finner.projections().size(); i++) {
                if (fcol.name().equals(finner.projections().get(i).alias())) {
                    src = finner.projections().get(i).expr();
                    srcIx = i;
                }
            }
            if (src instanceof SqlExpr.ArrayLit cells2
                    && !cells2.elements().isEmpty()) {
                SqlExpr sep = rc.extras().get(0);
                if (!sorted) {
                    SqlExpr rowJoined = concatJoin(cells2.elements(),
                            transform, sep);
                    List<SqlSelect.Projection> np =
                            new ArrayList<>(finner.projections());
                    np.set(srcIx, new SqlSelect.Projection(rowJoined,
                            fcol.name(),
                            finner.projections().get(srcIx).out()));
                    SqlAgg.Reducer fused = new SqlAgg.Reducer(
                            SqlAgg.Fn.STRING_AGG, List.of(fcol, sep), false,
                            fc2.orderBy());
                    return new SqlExpr.ScalarSubquery(fsel2
                            .withFrom(new com.legend.sql.SqlSource.Subselect(
                                    finner.withProjections(np),
                                    fsub.alias(), fsub.frameName()))
                            .withProjections(List.of(
                                    new SqlSelect.Projection(fused,
                                            fsel2.projections().get(0)
                                                    .alias(),
                                            fsel2.projections().get(0)
                                                    .out()))));
                }
                List<com.legend.sql.SqlQuery> branches = new ArrayList<>();
                for (SqlExpr cell : cells2.elements()) {
                    branches.add(finner.withProjections(
                            List.of(new SqlSelect.Projection(cell, "v",
                                    null))));
                }
                com.legend.sql.SqlUnion union =
                        new com.legend.sql.SqlUnion(branches, true,
                                List.of());
                SqlExpr vRead = SqlExpr.Column.derived("_cells", "v");
                SqlExpr tv = transform == null ? vRead
                        : substParam(transform.body(),
                                transform.params().get(0), vRead);
                SqlAgg.Reducer fused = new SqlAgg.Reducer(
                        SqlAgg.Fn.STRING_AGG, List.of(tv, sep), false,
                        List.of(SqlSelect.SortKey.asc(vRead)));
                return new SqlExpr.ScalarSubquery(SqlSelect.starOf(
                                new com.legend.sql.SqlSource.Subselect(union,
                                        "_cells", null))
                        .withProjections(List.of(new SqlSelect.Projection(
                                fused, null, null))));
            }
        }
        if (!(coll instanceof SqlExpr.ScalarSubquery sq)
                || !(sq.subquery() instanceof SqlSelect sel)
                || sel.projections().size() != 1
                || !(sel.projections().get(0).expr()
                        instanceof SqlAgg.Reducer collect)
                || collect.fn() != SqlAgg.Fn.LIST
                || collect.distinct()
                || collect.args().size() != 1) {
            return null;
        }
        SqlExpr raw = collect.args().get(0);
        SqlExpr value = raw;
        if (transform != null) {
            value = substParam(transform.body(), transform.params().get(0),
                    raw);
        }
        List<SqlExpr> args = new ArrayList<>();
        args.add(value);
        args.addAll(rc.extras());
        SqlAgg.Reducer fused = new SqlAgg.Reducer(rc.reducer(), args, false,
                sorted ? List.of(SqlSelect.SortKey.asc(raw))
                        : collect.orderBy());
        return new SqlExpr.ScalarSubquery(sel.withProjections(
                List.of(new SqlSelect.Projection(fused,
                        sel.projections().get(0).alias(),
                        sel.projections().get(0).out()))));
    }

    /** {@code t(e1) || sep || t(e2) || …} over compile-time elements. */
    private static SqlExpr concatJoin(List<SqlExpr> elements,
            SqlExpr.@com.legend.base.Nullable Lambda transform, SqlExpr sep) {
        SqlExpr out = null;
        for (SqlExpr e : elements) {
            SqlExpr v = transform == null ? e
                    : substParam(transform.body(), transform.params().get(0),
                            e);
            out = out == null ? v
                    : SqlExpr.Call.of(com.legend.sql.SqlFn.CONCAT,
                            SqlExpr.Call.of(com.legend.sql.SqlFn.CONCAT,
                                    out, sep), v);
        }
        return java.util.Objects.requireNonNull(out);
    }

    /** Portable LIST_GET (R5c, witnessed shapes; list_extract contract
     * probed: 1-based, -1 = last, 0 and out-of-range = NULL). Returns
     * null when the shape is unwitnessed (the renderer wall stays). */
    private static @com.legend.base.Nullable SqlExpr listGetRule(SqlExpr coll,
            SqlExpr idx) {
        if (!(idx instanceof SqlExpr.IntLit ix)) {
            return null;
        }
        long i = ix.value();
        // compile-time pick over a literal collection
        if (coll instanceof SqlExpr.ArrayLit al) {
            int n = al.elements().size();
            long pos = i > 0 ? i : i < 0 ? n + i + 1 : 0;
            return pos >= 1 && pos <= n ? al.elements().get((int) (pos - 1))
                    : new SqlExpr.NullLit();
        }
        // first-non-null idiom: LIST_FILTER(lit, x | x IS NOT NULL)[1]
        // IS COALESCE over the elements
        if (i == 1 && coll instanceof SqlExpr.Call ft
                && ft.fn() == com.legend.sql.SqlFn.LIST_FILTER
                && ft.args().size() == 2
                && ft.args().get(0) instanceof SqlExpr.ArrayLit fal
                && !fal.elements().isEmpty()
                && ft.args().get(1) instanceof SqlExpr.Lambda lam
                && lam.params().size() == 1
                && lam.body() instanceof SqlExpr.Call nn
                && nn.fn() == com.legend.sql.SqlFn.IS_NOT_NULL
                && nn.args().size() == 1
                && nn.args().get(0) instanceof SqlExpr.Column pc
                && pc.table() == null
                && lam.params().get(0).equals(pc.name())) {
            return fal.elements().size() == 1 ? fal.elements().get(0)
                    : new SqlExpr.Call(com.legend.sql.SqlFn.COALESCE,
                            fal.elements());
        }
        // token pick over a split: LIST_GET(SPLIT(s, sep), n) is
        // SPLIT_PART(s, sep, n) GUARDED to NULL when the token is
        // missing (differential-caught: list_extract OOB is NULL,
        // split_part is '') — literal single-char separator and
        // positive literal index only (the H2 spelling's domain).
        if (i >= 1 && coll instanceof SqlExpr.Call sp
                && sp.fn() == com.legend.sql.SqlFn.SPLIT
                && sp.args().size() == 2
                && sp.args().get(1) instanceof SqlExpr.StringLit sepLit
                && sepLit.value().length() == 1) {
            SqlExpr s0 = sp.args().get(0);
            SqlExpr missing = SqlExpr.Call.of(com.legend.sql.SqlFn.LESS,
                    SqlExpr.Call.of(com.legend.sql.SqlFn.MINUS,
                            SqlExpr.Call.of(com.legend.sql.SqlFn.LENGTH, s0),
                            SqlExpr.Call.of(com.legend.sql.SqlFn.LENGTH,
                                    SqlExpr.Call.of(
                                            com.legend.sql.SqlFn.REPLACE,
                                            s0, sp.args().get(1),
                                            new SqlExpr.StringLit("")))),
                    new SqlExpr.IntLit(i - 1));
            SqlExpr part = SqlExpr.Call.of(com.legend.sql.SqlFn.SPLIT_PART,
                    s0, sp.args().get(1), idx);
            return i == 1 ? part
                    : new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                            missing, new SqlExpr.NullLit())), part);
        }
        // element pick over a collect: ORDER-carrying LIMIT/OFFSET.
        // i = -1 (last) needs keys to flip; keyless last is undefined
        // order — declined, the wall stays loud.
        SqlSelect sel = collectSelect(coll);
        if (sel != null) {
            SqlAgg.Reducer collect =
                    (SqlAgg.Reducer) sel.projections().get(0).expr();
            SqlSelect picked = sel.withProjections(
                    List.of(new SqlSelect.Projection(collect.args().get(0),
                            sel.projections().get(0).alias(),
                            sel.projections().get(0).out())));
            if (i >= 1) {
                return new SqlExpr.ScalarSubquery(picked
                        .withOrderBy(collect.orderBy())
                        .withLimit(1L)
                        .withOffset(i > 1 ? i - 1 : null));
            }
            if (i == -1 && !collect.orderBy().isEmpty()) {
                List<SqlSelect.SortKey> flipped = new ArrayList<>();
                for (SqlSelect.SortKey k : collect.orderBy()) {
                    flipped.add(new SqlSelect.SortKey(k.expr(),
                            !k.ascending(), flipNulls(k.nullOrder()),
                            k.outputName()));
                }
                return new SqlExpr.ScalarSubquery(picked
                        .withOrderBy(flipped).withLimit(1L));
            }
        }
        return null;
    }

    /** The compile-time JSON text of a literal variant, or null:
     * CAST('...' AS JSON) and bare JSON-text string literals. */
    private static @com.legend.base.Nullable String jsonLiteral(SqlExpr e) {
        if (e instanceof SqlExpr.Cast c
                && c.target() == com.legend.sql.SqlType.Scalar.JSON
                && c.value() instanceof SqlExpr.StringLit sl) {
            return sl.value();
        }
        return null;
    }

    /** A parsed JSON node re-emitted as the literal it prints as — a
     * JSON cast for composites (further navigation keeps folding),
     * plain literals for scalars. */
    private static SqlExpr jsonLitExpr(@com.legend.base.Nullable Object node) {
        if (node == null) {
            return new SqlExpr.NullLit();
        }
        if (node instanceof String s2) {
            return new SqlExpr.StringLit(s2);
        }
        if (node instanceof Long l) {
            return new SqlExpr.IntLit(l);
        }
        if (node instanceof Integer i) {
            return new SqlExpr.IntLit(i);
        }
        if (node instanceof Double d) {
            return new SqlExpr.FloatLit(d);
        }
        if (node instanceof Boolean b) {
            return new SqlExpr.BoolLit(b);
        }
        return new SqlExpr.Cast(new SqlExpr.StringLit(jsonText(node)),
                com.legend.sql.SqlType.Scalar.JSON);
    }

    /** Minimal JSON writer for re-emitting folded composite nodes. */
    private static String jsonText(Object node) {
        if (node == null) {
            return "null";
        }
        if (node instanceof String s2) {
            return '"' + s2.replace("\\", "\\\\")
                    .replace("\"", "\\\"") + '"';
        }
        if (node instanceof List<?> l) {
            StringBuilder b = new StringBuilder("[");
            for (int i = 0; i < l.size(); i++) {
                b.append(i > 0 ? "," : "").append(jsonText(l.get(i)));
            }
            return b.append("]").toString();
        }
        if (node instanceof java.util.Map<?, ?> m) {
            StringBuilder b = new StringBuilder("{");
            boolean first = true;
            for (var en : m.entrySet()) {
                if (!first) {
                    b.append(",");
                }
                first = false;
                b.append(jsonText(String.valueOf(en.getKey()))).append(":")
                        .append(jsonText(en.getValue()));
            }
            return b.append("}").toString();
        }
        return String.valueOf(node);
    }

    /** The exact null-ignoring aggregate fold over compile-time
     * elements: {@code CASE WHEN all null THEN NULL ELSE op-chain of
     * COALESCE(e, neutral) END} (probed: all-null -> NULL, otherwise
     * NULLs drop out). {@code lead} prepends a type-pinning factor. */
    private static SqlExpr litFold(List<SqlExpr> elements, SqlExpr neutral,
            com.legend.sql.SqlFn op, @com.legend.base.Nullable SqlExpr lead) {
        SqlExpr allNull = null;
        SqlExpr chain = lead;
        for (SqlExpr el : elements) {
            SqlExpr isNull = SqlExpr.Call.of(com.legend.sql.SqlFn.IS_NULL,
                    el);
            allNull = allNull == null ? isNull
                    : SqlExpr.Call.of(com.legend.sql.SqlFn.AND, allNull,
                            isNull);
            SqlExpr v = SqlExpr.Call.of(com.legend.sql.SqlFn.COALESCE, el,
                    neutral);
            chain = chain == null ? v : SqlExpr.Call.of(op, chain, v);
        }
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                java.util.Objects.requireNonNull(allNull),
                new SqlExpr.NullLit())),
                java.util.Objects.requireNonNull(chain));
    }

    private static SqlSelect.SortKey.@com.legend.base.Nullable NullOrder flipNulls(
            SqlSelect.SortKey.@com.legend.base.Nullable NullOrder n) {
        return n == null ? null
                : n == SqlSelect.SortKey.NullOrder.NULLS_FIRST
                        ? SqlSelect.SortKey.NullOrder.NULLS_LAST
                        : SqlSelect.SortKey.NullOrder.NULLS_FIRST;
    }

    /** Portable membership (R2). LITERAL collection: the OR-chain
     * {@code needle = e1 OR needle = e2 ...} — EXACT list_contains
     * semantics (probed): NULL needle -> NULL, absent -> FALSE once
     * NULL-literal elements are DROPPED (x = NULL is never true, which
     * is precisely list_contains's no-match-on-NULL-element), empty ->
     * FALSE. COLLECT subselect: EXISTS with the equality pushed into
     * the WHERE (correlation preserved; the emission sites wrap
     * COALESCE(_, false), which absorbs the NULL-needle edge). */
    private static @com.legend.base.Nullable SqlExpr membershipRule(
            SqlExpr.Membership m) {
        if (m.collection() instanceof SqlExpr.ArrayLit al) {
            SqlExpr chain = null;
            for (SqlExpr el : al.elements()) {
                if (el instanceof SqlExpr.NullLit) {
                    continue;
                }
                SqlExpr eq = SqlExpr.Call.of(com.legend.sql.SqlFn.EQUAL,
                        m.needle(), el);
                chain = chain == null ? eq
                        : SqlExpr.Call.of(com.legend.sql.SqlFn.OR, chain, eq);
            }
            return chain == null ? new SqlExpr.BoolLit(false) : chain;
        }
        // PUSH-DOWN arms (R5d, witnessed: membership over a runtime-
        // branched concat of literal collections): membership
        // distributes over LIST_CONCAT (OR of the arms) and over CASE
        // (into each branch). Bail whole when any arm has no rule —
        // the node survives to the wall, never half-rewritten.
        if (m.collection() instanceof SqlExpr.Call mc
                && mc.fn() == com.legend.sql.SqlFn.LIST_CONCAT
                && mc.args().size() >= 2) {
            SqlExpr chain = null;
            for (SqlExpr arm : mc.args()) {
                SqlExpr member = membershipRule(
                        new SqlExpr.Membership(m.needle(), arm));
                if (member == null) {
                    return null;
                }
                chain = chain == null ? member
                        : SqlExpr.Call.of(com.legend.sql.SqlFn.OR, chain,
                                member);
            }
            return chain;
        }
        if (m.collection() instanceof SqlExpr.Case cs) {
            List<SqlExpr.Case.When> whens = new ArrayList<>();
            for (SqlExpr.Case.When w : cs.whens()) {
                SqlExpr member = membershipRule(
                        new SqlExpr.Membership(m.needle(), w.then()));
                if (member == null) {
                    return null;
                }
                whens.add(new SqlExpr.Case.When(w.condition(), member));
            }
            SqlExpr otherwise = null;
            if (cs.otherwise() != null) {
                otherwise = membershipRule(
                        new SqlExpr.Membership(m.needle(), cs.otherwise()));
                if (otherwise == null) {
                    return null;
                }
            }
            return new SqlExpr.Case(whens, otherwise);
        }
        // a NON-NULL filter over the collection (LIST_FILTER(coll, x -> x
        // IS NOT NULL) — the many-property read's null-drop) is redundant
        // under membership: needle = NULL is never true in the EXISTS /
        // OR forms, so the rule sees through to the collection
        if (m.collection() instanceof SqlExpr.Call lf
                && lf.fn() == com.legend.sql.SqlFn.LIST_FILTER
                && lf.args().size() == 2
                && lf.args().get(1) instanceof SqlExpr.Lambda nn
                && nn.params().size() == 1
                && nn.body() instanceof SqlExpr.Call nnc
                && nnc.fn() == com.legend.sql.SqlFn.IS_NOT_NULL
                && nnc.args().size() == 1
                && nnc.args().get(0) instanceof SqlExpr.Column pc
                && pc.table() == null && nn.params().get(0).equals(pc.name())) {
            return membershipRule(new SqlExpr.Membership(m.needle(), lf.args().get(0)));
        }
        // the COMPACT list (nulls dropped) is the same redundancy
        if (m.collection() instanceof SqlExpr.CompactList cl) {
            return membershipRule(new SqlExpr.Membership(m.needle(), cl.list()));
        }
        if (m.collection() instanceof SqlExpr.ScalarSubquery sq
                && sq.subquery() instanceof SqlSelect sel
                && sel.projections().size() == 1
                && sel.projections().get(0).expr()
                        instanceof SqlAgg.Reducer collect
                && collect.fn() == SqlAgg.Fn.LIST
                && !collect.distinct()
                && collect.args().size() == 1) {
            SqlExpr eq = SqlExpr.Call.of(com.legend.sql.SqlFn.EQUAL,
                    collect.args().get(0), m.needle());
            SqlSelect inner = sel.withProjections(
                    List.of(new SqlSelect.Projection(
                            new SqlExpr.IntLit(1), null, null)));
            SqlSelect withEq = inner.withWhere(inner.where() == null ? eq
                    : SqlExpr.Call.of(com.legend.sql.SqlFn.AND,
                            inner.where(), eq));
            return new SqlExpr.Exists(withEq);
        }
        return null;
    }

    /** Replace bare reads of the lambda parameter with {@code value}. */
    private static SqlExpr substParam(SqlExpr body, String param,
            SqlExpr value) {
        if (body instanceof SqlExpr.Column c && c.table() == null
                && param.equals(c.name())) {
            return value;
        }
        List<SqlExpr> kids = body.children();
        if (kids.isEmpty()) {
            return body;
        }
        List<SqlExpr> mapped = new ArrayList<>(kids.size());
        boolean changed = false;
        for (SqlExpr k : kids) {
            SqlExpr m = substParam(k, param, value);
            changed |= m != k;
            mapped.add(m);
        }
        return changed ? body.withChildren(mapped) : body;
    }
}
