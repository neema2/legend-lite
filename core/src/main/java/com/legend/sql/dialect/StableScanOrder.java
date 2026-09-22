// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.ScanOrder;

/**
 * ENGINE-CORPUS-COMPAT root pass (flag-gated in {@link DuckDb}): the
 * HOST-channel statements of the corpus replay get the deterministic
 * scan-order key — the engine's tests assert positionally while
 * relying on H2's implicit scan order. The key itself (and the
 * always-on ASSERT-boundary application) lives in {@link ScanOrder} —
 * one owner.
 */
public final class StableScanOrder extends SqlRewriter {

    /** TEST-ONLY FEATURE, PINNED (user ruling 2026-09-20: product SQL
     * carries no order it did not ask for; the engine's insertion-order
     * emulation is a feature of the TEST lane, never dropped): the number
     * of statements this pass CHANGED — an order the statement lacked was
     * added. The corpus runner attributes firings to the test that ran and
     * pins that set per lane ({@code rcorpus/<lane>-engine-order-register.txt}).
     * The count lives HERE because the SQL layer is standalone (Invariant 6a:
     * it may not reach {@code com.legend.exec}); the one census owner
     * ({@code Census.Key.SCAN_ORDER_FIRINGS}) reads through to it, so the
     * lanes still read every count from one place. */
    private static final java.util.concurrent.atomic.AtomicLong FIRINGS =
            new java.util.concurrent.atomic.AtomicLong();

    public static long firings() {
        return FIRINGS.get();
    }

    /** Leg 3.4 step 2: the ordinals each frame CTE of the statement
     * exports (threaded through its own definition), so a reference to the
     * frame re-exports the frame's scan order exactly as a subselect frame
     * would — the CTE boundary keeps the engine's insertion order. Per
     * statement: filled at {@link #rewriteRoot}, read by the walk. */
    private final java.util.Map<String, java.util.List<String>> cteOrds =
            new java.util.HashMap<>();

    @Override
    public SqlQuery rewriteRoot(SqlQuery q) {
        SqlQuery in = q;
        cteOrds.clear();
        if (q instanceof com.legend.sql.SqlWith w) {
            // ONLY WHEN A READER NEEDS ORDER (user ruling 2026-09-20): a
            // frame CTE threads its scan ordinals only if some select of the
            // statement reads it by position (a LIMIT/OFFSET cap over it) or
            // joins its values through an order-sensitive aggregate; a
            // frame read by multiset compares stays the product plan, bare
            java.util.Set<String> needs = framesNeedingOrder(q);
            java.util.List<com.legend.sql.SqlWith.Cte> cs = new java.util.ArrayList<>();
            boolean changed = false;
            for (com.legend.sql.SqlWith.Cte c : w.ctes()) {
                Threaded t = needs.contains(c.name())
                        && c.query() instanceof com.legend.sql.SqlSelect body
                        ? threadScan(body) : null;
                if (t == null || t.ordNames().isEmpty()) {
                    cs.add(c);
                    continue;
                }
                cteOrds.put(c.name(), t.ordNames());
                cs.add(new com.legend.sql.SqlWith.Cte(c.name(), t.select(), c.materialized()));
                changed = true;
            }
            if (changed) {
                q = new com.legend.sql.SqlWith(cs, w.body());
            }
        }
        // deep walk FIRST (fires the select hook on every nested
        // select), then the root-shape special cases (cap wrappers)
        SqlQuery out = ScanOrder.stabilize(rewrite(q));
        if (!out.equals(in)) {
            FIRINGS.incrementAndGet();
        }
        return out;
    }

    /** The frame CTEs some select reads by POSITION (a LIMIT/OFFSET select
     * whose from tree scans the frame) or through an order-sensitive
     * aggregate (whose from tree, subselects included, scans the frame). */
    private static java.util.Set<String> framesNeedingOrder(SqlQuery q) {
        java.util.Set<String> out = new java.util.HashSet<>();
        new SqlRewriter() {
            @Override
            protected SqlQuery select(com.legend.sql.SqlSelect s) {
                if (s.limit() != null || s.offset() != null) {
                    frameNames(s.from(), false, out);
                }
                if (s.projections().stream().anyMatch(p -> p.expr()
                        instanceof com.legend.sql.SqlAgg.Reducer r && orderSensitive(r)
                        && r.orderBy().isEmpty())) {
                    frameNames(s.from(), true, out);
                }
                return s;
            }
        }.rewriteRoot(q);
        return out;
    }

    private static void frameNames(com.legend.sql.SqlSource src, boolean throughSubselects,
            java.util.Set<String> out) {
        if (src instanceof com.legend.sql.SqlSource.Cte c) {
            out.add(c.name());
        } else if (src instanceof com.legend.sql.SqlSource.Join j) {
            frameNames(j.left(), throughSubselects, out);
            frameNames(j.right(), throughSubselects, out);
        } else if (throughSubselects && src instanceof com.legend.sql.SqlSource.Subselect sub
                && sub.inner() instanceof com.legend.sql.SqlSelect inner) {
            frameNames(inner.from(), true, out);
        }
    }

    /** A frame reference with its threaded ordinals in scope: the
     * reference re-exports them (its outputs widened) so a reader can
     * order by them; null when the frame threads none. */
    private com.legend.sql.SqlSource.@com.legend.base.Nullable Cte widened(
            com.legend.sql.SqlSource.Cte c) {
        java.util.List<String> ords = cteOrds.get(c.name());
        if (ords == null) {
            return null;
        }
        java.util.List<com.legend.sql.OutputCol> outs = new java.util.ArrayList<>(c.outputs());
        for (String name : ords) {
            if (outs.stream().noneMatch(o -> o.name().equals(name))) {
                outs.add(new com.legend.sql.OutputCol(name,
                        com.legend.sql.SqlType.Scalar.BIGINT, true));
            }
        }
        return new com.legend.sql.SqlSource.Cte(c.name(), c.alias(), java.util.List.copyOf(outs));
    }

    /** A positional read over a frame reference — a LIMIT/OFFSET select
     * with no sort of its own whose leftmost scan is a threaded frame —
     * ORDERED BY the frame's ordinals: the engine's insertion order, the
     * order the pasted form reached through the base table's rowid. */
    private SqlQuery stabilizeOverFrame(com.legend.sql.SqlSelect s) {
        if (!s.orderBy().isEmpty() || s.distinct() || !s.groupBy().isEmpty()
                || (s.limit() == null && s.offset() == null)
                || s.projections().stream().anyMatch(p -> aggregates(p.expr()))) {
            return s;
        }
        com.legend.sql.SqlSource leftmost = s.from();
        while (leftmost instanceof com.legend.sql.SqlSource.Join j) {
            leftmost = j.left();
        }
        if (!(leftmost instanceof com.legend.sql.SqlSource.Cte c)) {
            return s;
        }
        com.legend.sql.SqlSource.Cte wide = widened(c);
        if (wide == null) {
            return s;
        }
        // SCAN-MAJOR (the driving table first — ScanOrder.stabilize's key
        // order for a positional cap over a join of scans): the threaded
        // ordinals are recorded PROBE-major (right before left, the hash
        // join's emission order the aggregates follow), so the cap reads
        // them reversed
        java.util.List<String> ords = new java.util.ArrayList<>(
                java.util.Objects.requireNonNull(cteOrds.get(c.name())));
        java.util.Collections.reverse(ords);
        java.util.List<com.legend.sql.SqlSelect.SortKey> keys = new java.util.ArrayList<>();
        for (String name : ords) {
            keys.add(new com.legend.sql.SqlSelect.SortKey(
                    com.legend.sql.SqlExpr.Column.of(wide.alias(), wide.outputs(), name),
                    true, null, null));
        }
        return s.withFrom(replaceLeftmost(s.from(), wide)).withOrderBy(keys);
    }

    private static com.legend.sql.SqlSource replaceLeftmost(com.legend.sql.SqlSource src,
            com.legend.sql.SqlSource.Cte wide) {
        if (src instanceof com.legend.sql.SqlSource.Join j) {
            return new com.legend.sql.SqlSource.Join(replaceLeftmost(j.left(), wide), j.right(),
                    j.kind(), j.on());
        }
        return wide;
    }
    // (A root-level union ORDER BY was BUILT AND REVERTED: ordering a
    // root fetch by union-leg ordinals broke aggregate/graph roots —
    // the unsorted two-renders compare is ASSERT-BOUNDARY comparison
    // policy (renderedArm's line multiset), per the user's ruling.)

    /** SCAN-ORDER AGGREGATION KEY THROUGH FRAMES (user ruling
     * 2026-08-31: tie order is undefined in the language — the
     * platform stays order-honest; replay determinism is ENGINE-COMPAT
     * ONLY, here): an order-sensitive STRING_AGG with no declared
     * order aggregates in H2's own order — the input's USER SORT keys
     * first (H2 sorts stably), then BASE-TABLE SCAN ORDER, probe-major
     * (H2's unindexed equi-joins emit probe/right-side major —
     * groupByAfterASort pins Smith*Johnson*Hill*Allen = personTable
     * scan order; testConcatenateWithJoin's ties are the same table's
     * scan order). Rowids thread through plain subselect frames as
     * appended hidden ordinal projections; union frames end the walk
     * (their legs' contribution is unaddressed — a named gap until a
     * witness demands leg ordinals). */
    @Override
    protected SqlQuery select(com.legend.sql.SqlSelect s) {
        if (!(s.from() instanceof com.legend.sql.SqlSource.Subselect sub)
                || !(sub.inner() instanceof com.legend.sql.SqlSelect inner)) {
            return baseTableReducers(s);
        }
        // the reducer's VALUE must read the from-subselect's own alias
        // (the witnesses' shape) — anything else (double-sort chains
        // whose collect embeds cross-scope aliases) stays untouched
        // (binder receipt: LIST(t2...) whose t2 is another scope)
        boolean wants = s.projections().stream().anyMatch(
                p -> p.expr() instanceof com.legend.sql.SqlAgg.Reducer r
                        && orderSensitive(r) && r.orderBy().isEmpty()
                        && !r.args().isEmpty()
                        && readsAlias(r.args().get(0), sub.alias()));
        if (!wants) {
            return s;
        }
        Threaded t = threadScan(inner);
        if (t == null || t.ordNames().isEmpty()) {
            return s;
        }
        // user sort keys: reference EXPORTED outputs by name; a key the
        // inner does not export is EXPORTED as a hidden projection
        // (binder receipt: ordering by t2.<unexported> resolves nothing)
        java.util.List<com.legend.sql.SqlSelect.SortKey> keys =
                new java.util.ArrayList<>();
        com.legend.sql.SqlSelect widened = t.select();
        for (com.legend.sql.SqlSelect.SortKey k : inner.orderBy()) {
            String name = k.outputName() != null ? k.outputName()
                    : k.expr() instanceof com.legend.sql.SqlExpr.Column kc
                            ? kc.name() : null;
            final String want = name;
            boolean exported = want != null && widened.outputs().stream()
                    .anyMatch(c -> c.name().equals(want));
            if (!exported) {
                String hidden = "__agg_key" + keys.size();
                com.legend.sql.OutputCol col = new com.legend.sql.OutputCol(
                        hidden, com.legend.sql.SqlType.Scalar.VARCHAR,
                        true);
                java.util.List<com.legend.sql.SqlSelect.Projection> wp =
                        new java.util.ArrayList<>(widened.projections());
                java.util.List<com.legend.sql.OutputCol> wo =
                        new java.util.ArrayList<>(widened.outputs());
                wp.add(new com.legend.sql.SqlSelect.Projection(k.expr(),
                        hidden, col));
                wo.add(col);
                widened = new com.legend.sql.SqlSelect(wp,
                        widened.distinct(), widened.from(),
                        widened.where(), widened.groupBy(),
                        widened.having(), widened.qualify(),
                        widened.orderBy(), widened.limit(),
                        widened.offset(), java.util.List.copyOf(wo));
                name = hidden;
            }
            keys.add(new com.legend.sql.SqlSelect.SortKey(
                    com.legend.sql.SqlExpr.Column.of(sub.alias(),
                            widened.outputs(),
                            java.util.Objects.requireNonNull(name)),
                    k.ascending(), k.nullOrder(), null));
        }
        for (String ord : t.ordNames()) {
            keys.add(new com.legend.sql.SqlSelect.SortKey(
                    com.legend.sql.SqlExpr.Column.of(sub.alias(),
                            widened.outputs(), ord),
                    true, null, null));
        }
        java.util.List<com.legend.sql.SqlSelect.Projection> out =
                new java.util.ArrayList<>(s.projections());
        for (int i = 0; i < out.size(); i++) {
            var p = out.get(i);
            if (p.expr() instanceof com.legend.sql.SqlAgg.Reducer r
                    && orderSensitive(r) && r.orderBy().isEmpty()
                    && !r.args().isEmpty()
                    && readsAlias(r.args().get(0), sub.alias())) {
                out.set(i, new com.legend.sql.SqlSelect.Projection(
                        new com.legend.sql.SqlAgg.Reducer(r.fn(), r.args(),
                                r.distinct(), keys),
                        p.outputName(), p.out()));
            }
        }
        return new com.legend.sql.SqlSelect(out, s.distinct(),
                new com.legend.sql.SqlSource.Subselect(widened,
                        sub.alias(), sub.frameName()),
                s.where(), s.groupBy(), s.having(), s.qualify(),
                s.orderBy(), s.limit(), s.offset(), s.outputs());
    }


    /** The value expression's EVERY column reference resolves in
     * {@code alias} (or is column-free). */
    /** An order-sensitive reducer whose value reads a BASE TABLE alias of
     * this select's own from tree (a group concat over a scan or a join of
     * scans) orders by that table's row number — the engine's H2 insertion
     * order the goldens captured. Test lane only (this pass is the corpus
     * runner's); the product emits no such order. */
    private static SqlQuery baseTableReducers(com.legend.sql.SqlSelect s) {
        java.util.List<com.legend.sql.SqlSelect.Projection> out = null;
        for (int i = 0; i < s.projections().size(); i++) {
            var p = s.projections().get(i);
            if (p.expr() instanceof com.legend.sql.SqlAgg.Reducer r
                    && orderSensitive(r) && r.orderBy().isEmpty()
                    && !r.args().isEmpty()
                    && r.args().get(0) instanceof com.legend.sql.SqlExpr.Column vc
                    && aliasIsBaseTable(s.from(), vc.table())) {
                if (out == null) {
                    out = new java.util.ArrayList<>(s.projections());
                }
                out.set(i, new com.legend.sql.SqlSelect.Projection(
                        new com.legend.sql.SqlAgg.Reducer(r.fn(), r.args(), r.distinct(),
                                java.util.List.of(new com.legend.sql.SqlSelect.SortKey(
                                        new com.legend.sql.SqlExpr.RowOrder(vc.table()),
                                        true, null, null))),
                        p.outputName(), p.out()));
            }
        }
        return out == null ? s : new com.legend.sql.SqlSelect(out, s.distinct(), s.from(),
                s.where(), s.groupBy(), s.having(), s.qualify(), s.orderBy(), s.limit(),
                s.offset(), s.outputs());
    }

    /** Whether {@code alias} names a BASE TABLE scan in the from tree —
     * the rowid pseudo-column is only valid there. */
    private static boolean aliasIsBaseTable(com.legend.sql.SqlSource src,
            @com.legend.base.Nullable String alias) {
        return switch (src) {
            case com.legend.sql.SqlSource.Table t -> t.alias().equals(alias);
            case com.legend.sql.SqlSource.Join j -> aliasIsBaseTable(j.left(), alias)
                    || aliasIsBaseTable(j.right(), alias);
            default -> false;
        };
    }

    private static boolean aggregates(com.legend.sql.SqlExpr e) {
        if (e instanceof com.legend.sql.SqlAgg.Reducer) {
            return true;
        }
        for (com.legend.sql.SqlExpr k : e.children()) {
            if (aggregates(k)) {
                return true;
            }
        }
        return false;
    }

    private static boolean readsAlias(com.legend.sql.SqlExpr e,
            String alias) {
        if (e instanceof com.legend.sql.SqlExpr.Column c) {
            return alias.equals(c.table());
        }
        for (com.legend.sql.SqlExpr k : e.children()) {
            if (!readsAlias(k, alias)) {
                return false;
            }
        }
        return true;
    }

    /** Aggregations whose RESULT depends on input order: group concat
     * and the LIST collect (a value collection's element order —
     * testConcatenateWithJoin's makeString rides a LIST collect).
     * DISTINCT forms are set-shaped and stay untouched. */
    private static boolean orderSensitive(com.legend.sql.SqlAgg.Reducer r) {
        return (r.fn() == com.legend.sql.SqlAgg.Fn.STRING_AGG
                || r.fn() == com.legend.sql.SqlAgg.Fn.LIST)
                && !r.distinct();
    }

    private record Threaded(com.legend.sql.SqlSelect select,
            java.util.List<String> ordNames) {
    }

    /** {@code sel} with hidden {@code __agg_ordN} projections appended
     * for every base-table rowid reachable probe-major through its
     * from tree (recursing through plain subselect frames); null when
     * the shape refuses (set semantics or star frames). */
    private @com.legend.base.Nullable Threaded threadScan(
            com.legend.sql.SqlSelect sel) {
        if (sel.distinct() || !sel.groupBy().isEmpty()
                || sel.limit() != null || sel.offset() != null
                // an AGGREGATED frame (a bare aggregate, no GROUP BY) has
                // one row and no scan order to thread — a rowid beside a
                // sum is a binder error (leg 3.4 step 2: the frame CTE
                // bodies are threaded too, and some frames aggregate)
                || sel.projections().stream().anyMatch(p -> aggregates(p.expr()))
                // EMPTY projections = an implicit star frame — appending
                // would REPLACE the whole row (binder receipt: a filtered
                // join frame reduced to its ordinal alone)
                || sel.projections().isEmpty()
                || sel.projections().stream().anyMatch(
                        p -> p.expr() instanceof com.legend.sql.SqlExpr.Star
                            || p.expr() instanceof com.legend.sql.SqlExpr
                                    .StarExcept
                            || p.out() == null)) {
            return null;
        }
        java.util.List<com.legend.sql.SqlExpr> ordExprs =
                new java.util.ArrayList<>();
        com.legend.sql.SqlSource from2 = walkFrom(sel.from(), ordExprs);
        if (ordExprs.isEmpty()) {
            return null;
        }
        java.util.List<com.legend.sql.SqlSelect.Projection> ps =
                new java.util.ArrayList<>(sel.projections());
        java.util.List<com.legend.sql.OutputCol> outs =
                new java.util.ArrayList<>(sel.outputs());
        java.util.List<String> names = new java.util.ArrayList<>();
        for (com.legend.sql.SqlExpr e : ordExprs) {
            String name = "__agg_ord" + names.size();
            com.legend.sql.OutputCol col = new com.legend.sql.OutputCol(
                    name, com.legend.sql.SqlType.Scalar.BIGINT, true);
            ps.add(new com.legend.sql.SqlSelect.Projection(e, name, col));
            outs.add(col);
            names.add(name);
        }
        return new Threaded(new com.legend.sql.SqlSelect(ps,
                sel.distinct(), from2, sel.where(), sel.groupBy(),
                sel.having(), sel.qualify(), sel.orderBy(), sel.limit(),
                sel.offset(), java.util.List.copyOf(outs)), names);
    }

    /** Probe-major rowid walk: right side before left on joins; a
     * plain base table contributes its rowid; a subselect frame
     * recurses and re-exports its ordinals; unions and other sources
     * contribute nothing. Returns the (possibly rewritten) source. */
    private com.legend.sql.SqlSource walkFrom(
            com.legend.sql.SqlSource src,
            java.util.List<com.legend.sql.SqlExpr> ordExprs) {
        if (src instanceof com.legend.sql.SqlSource.Table t) {
            ordExprs.add(new com.legend.sql.SqlExpr.RowOrder(t.alias()));
            return src;
        }
        if (src instanceof com.legend.sql.SqlSource.Cte c) {
            // a frame reference re-exports the ordinals its definition
            // threads (none when the frame's shape refused)
            com.legend.sql.SqlSource.Cte wide = widened(c);
            if (wide == null) {
                return src;
            }
            for (String name : java.util.Objects.requireNonNull(cteOrds.get(c.name()))) {
                ordExprs.add(com.legend.sql.SqlExpr.Column.of(wide.alias(), wide.outputs(), name));
            }
            return wide;
        }
        if (src instanceof com.legend.sql.SqlSource.Join j) {
            java.util.List<com.legend.sql.SqlExpr> rightOrds =
                    new java.util.ArrayList<>();
            com.legend.sql.SqlSource r = walkFrom(j.right(), rightOrds);
            java.util.List<com.legend.sql.SqlExpr> leftOrds =
                    new java.util.ArrayList<>();
            com.legend.sql.SqlSource l = walkFrom(j.left(), leftOrds);
            ordExprs.addAll(rightOrds);
            ordExprs.addAll(leftOrds);
            return r == j.right() && l == j.left() ? src
                    : new com.legend.sql.SqlSource.Join(l, r, j.kind(),
                            j.on());
        }
        if (src instanceof com.legend.sql.SqlSource.Subselect sub
                && sub.inner() instanceof com.legend.sql.SqlSelect inner) {
            Threaded t = threadScan(inner);
            if (t == null) {
                return src;
            }
            for (String name : t.ordNames()) {
                ordExprs.add(com.legend.sql.SqlExpr.Column.of(sub.alias(),
                        t.select().outputs(), name));
            }
            return new com.legend.sql.SqlSource.Subselect(t.select(),
                    sub.alias(), sub.frameName());
        }
        // UNION ALL frame: H2 executes legs SEQUENTIALLY (leg-major
        // scan order — testProjectThroughAsso's golden) — every leg
        // appends its LEG INDEX plus its own first scan ordinal
        // (arity-normalized: exactly two columns per leg, NULL when a
        // leg has no base scan), and the frame re-exports both.
        if (src instanceof com.legend.sql.SqlSource.Subselect sub
                && sub.inner() instanceof com.legend.sql.SqlUnion u
                && u.all()) {
            java.util.List<com.legend.sql.SqlQuery> branches =
                    new java.util.ArrayList<>();
            for (int i = 0; i < u.branches().size(); i++) {
                if (!(u.branches().get(i)
                        instanceof com.legend.sql.SqlSelect leg)) {
                    return src;
                }
                com.legend.sql.SqlSelect leg2 = unionLegOrdinals(leg, i);
                if (leg2 == null) {
                    return src;
                }
                branches.add(leg2);
            }
            java.util.List<com.legend.sql.OutputCol> outs =
                    new java.util.ArrayList<>(u.outputs());
            com.legend.sql.OutputCol legCol = new com.legend.sql.OutputCol(
                    "__agg_leg", com.legend.sql.SqlType.Scalar.BIGINT,
                    true);
            com.legend.sql.OutputCol ordCol = new com.legend.sql.OutputCol(
                    "__agg_legord", com.legend.sql.SqlType.Scalar.BIGINT,
                    true);
            outs.add(legCol);
            outs.add(ordCol);
            com.legend.sql.SqlUnion u2 = new com.legend.sql.SqlUnion(
                    branches, true, java.util.List.copyOf(outs));
            ordExprs.add(com.legend.sql.SqlExpr.Column.of(sub.alias(),
                    outs, "__agg_leg"));
            ordExprs.add(com.legend.sql.SqlExpr.Column.of(sub.alias(),
                    outs, "__agg_legord"));
            return new com.legend.sql.SqlSource.Subselect(u2,
                    sub.alias(), sub.frameName());
        }
        return src;
    }

    /** A union leg with {@code __agg_leg} (its index) and
     * {@code __agg_legord} (its first probe-major scan ordinal, NULL
     * when none) appended; null when the leg's shape refuses. */
    private com.legend.sql.@com.legend.base.Nullable SqlSelect
            unionLegOrdinals(com.legend.sql.SqlSelect leg, int index) {
        if (leg.distinct() || !leg.groupBy().isEmpty()
                || leg.limit() != null || leg.offset() != null
                || leg.projections().stream().anyMatch(
                        p -> p.expr() instanceof com.legend.sql.SqlExpr.Star
                            || p.expr() instanceof com.legend.sql.SqlExpr
                                    .StarExcept
                            || p.out() == null)) {
            return null;
        }
        java.util.List<com.legend.sql.SqlExpr> ords =
                new java.util.ArrayList<>();
        walkFrom(leg.from(), ords);
        com.legend.sql.SqlExpr ord = ords.isEmpty()
                ? new com.legend.sql.SqlExpr.Cast(
                        new com.legend.sql.SqlExpr.NullLit(),
                        com.legend.sql.SqlType.Scalar.BIGINT)
                : ords.get(0);
        java.util.List<com.legend.sql.SqlSelect.Projection> ps =
                new java.util.ArrayList<>(leg.projections());
        if (ps.isEmpty()) {
            // an implicit star frame keeps its row EXPLICITLY
            // (`SELECT *, <ordinals>`), never replaced by the ordinals
            ps.add(new com.legend.sql.SqlSelect.Projection(
                    new com.legend.sql.SqlExpr.Star(null), null, null));
        }
        java.util.List<com.legend.sql.OutputCol> outs =
                new java.util.ArrayList<>(leg.outputs());
        com.legend.sql.OutputCol legCol = new com.legend.sql.OutputCol(
                "__agg_leg", com.legend.sql.SqlType.Scalar.BIGINT, true);
        com.legend.sql.OutputCol ordCol = new com.legend.sql.OutputCol(
                "__agg_legord", com.legend.sql.SqlType.Scalar.BIGINT, true);
        ps.add(new com.legend.sql.SqlSelect.Projection(
                new com.legend.sql.SqlExpr.IntLit(index), "__agg_leg",
                legCol));
        ps.add(new com.legend.sql.SqlSelect.Projection(ord, "__agg_legord",
                ordCol));
        outs.add(legCol);
        outs.add(ordCol);
        return new com.legend.sql.SqlSelect(ps, leg.distinct(), leg.from(),
                leg.where(), leg.groupBy(), leg.having(), leg.qualify(),
                leg.orderBy(), leg.limit(), leg.offset(),
                java.util.List.copyOf(outs));
    }

    /** SUBSELECT inners stabilize too (disagree-9 burn): a verdict
     * statement compiles the asserted relation as a SUBQUERY (the
     * value-collection collect), where the root-only application
     * missed it — the HOST channel ran the same relation as its own
     * statement root and got the key, so the two channels read
     * different orders. Scoped to FROM-subselects (where ORDER BY is
     * legal SQL — a select hook also caught UNION BRANCHES and emitted
     * `... ORDER BY x UNION ALL ...`, a parser error). Same owner,
     * same orderable gate ({@link ScanOrder#stabilize} declines
     * DISTINCT/GROUP BY/aggregate/user-ordered shapes), same flag. */
    @Override
    protected com.legend.sql.SqlSource source(com.legend.sql.SqlSource s) {
        if (s instanceof com.legend.sql.SqlSource.Subselect sub
                && sub.inner() instanceof com.legend.sql.SqlSelect inner) {
            SqlQuery st = ScanOrder.stabilize(inner);
            if (st == inner) {
                st = stabilizeOverFrame(inner);
            }
            if (st != inner) {
                return new com.legend.sql.SqlSource.Subselect(st,
                        sub.alias(), sub.frameName());
            }
        }
        return s;
    }
}
