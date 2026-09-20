// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lineage;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlUnion;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Column lineage over the LOWERED SQL plan (feature track #44 — the
 * engine's {@code scanColumns} surface, computed from the REAL
 * pipeline's output instead of a parallel metamodel walk): every
 * physical {@code table.column} a query reads, tagged with the
 * engine's context vocabulary —
 * <ul>
 *   <li>{@code <TableAliasColumn>} — a VALUE read (root projections,
 *       filters, group/order keys);</li>
 *   <li>{@code <JoinTreeNode>} — a JOIN-KEY read (any join ON, at any
 *       depth).</li>
 * </ul>
 *
 * <p>DEMAND-DRIVEN: subselect projections are pass-throughs, not uses —
 * a column materialized by an inner select only appears if a root
 * output, a filter, or a join ON ultimately reads it, resolved through
 * the alias environment (union branches all resolve; correlated
 * subqueries see the outer scope). Silent drops are wrong lineage, so
 * the expression walk is TOTAL: unhandled composite nodes recurse over
 * their typed children ({@code SqlExpr.children()}, exhaustive over the
 * variants) — never over record components by reflection.
 */
public final class ScanColumns {

    private ScanColumns() {
    }

    /** Sorted {@code "table.column <Context>"} strings — the engine's
     * scanColumns test formatting. */
    public static List<String> strings(SqlQuery q) {
        List<String> sorted = new ArrayList<>();
        for (Entry e : entries(q)) {
            String bare = e.table().contains(".")
                    ? e.table().substring(e.table().lastIndexOf('.') + 1) : e.table();
            sorted.add(bare + "." + e.column() + " <" + e.context() + ">");
        }
        sorted.sort(String::compareTo);
        return sorted;
    }

    /** One read: the plan's table (as the lowering spelled it — schema-
     * qualified when the store qualifies), the column, the engine's
     * context vocabulary. */
    public record Entry(String table, String column, String context) {
    }

    /** The reads as DATA (ColumnLineageRows carries them as rows). */
    public static List<Entry> entries(SqlQuery q) {
        Set<Entry> out = new LinkedHashSet<>();
        scanQuery(q, Map.of(), out, true);
        return new ArrayList<>(out);
    }

    private static final String VALUE = "TableAliasColumn";
    private static final String JOIN = "JoinTreeNode";

    private interface Resolver {
        void resolve(String col, String ctx, Set<Entry> out);
    }

    private static void scanQuery(SqlQuery q, Map<String, Resolver> outer,
            Set<Entry> out, boolean root) {
        switch (q) {
            case SqlSelect s -> {
                Map<String, Resolver> env = new LinkedHashMap<>(outer);
                env.putAll(envOf(s.from(), outer, out));
                if (root) {
                    // a ROOT projection resolving through a JOINED VIEW
                    // frame (subselect) is the engine's
                    // RelationalOperationElementWithJoin context — the PM
                    // rode a @join|View.col emission (testView FIRSTNAME);
                    // joined plain TABLES keep TableAliasColumn (the PM is
                    // a plain column, the join is the navigation's).
                    Set<String> spine = new LinkedHashSet<>();
                    rootSpine(s.from(), spine);
                    Set<String> joinedViews = new LinkedHashSet<>();
                    joinedSubselects(s.from(), spine, joinedViews);
                    for (SqlSelect.Projection p : s.projections()) {
                        String ctx = p.expr() instanceof SqlExpr.Column pc
                                && joinedViews.contains(pc.table())
                                ? "RelationalOperationElementWithJoin" : VALUE;
                        use(p.expr(), env, ctx, out);
                    }
                }
                if (s.where() != null) {
                    use(s.where(), env, VALUE, out);
                }
                for (SqlExpr g : s.groupBy()) {
                    // an INNER select's PASS-THROUGH grouped key (grouped
                    // subselect joined back on the key): its use is the
                    // CONSUMER's — the outer ON already reports it as
                    // JoinTreeNode; engine golden carries no extra VALUE
                    // read (testAssociationMapping FIRMID)
                    if (!root && g instanceof SqlExpr.Column gc
                            && s.projections().stream().anyMatch(p ->
                                    p.expr() instanceof SqlExpr.Column pc
                                    && java.util.Objects.equals(
                                            pc.table(), gc.table())
                                    && pc.name().equals(gc.name()))) {
                        continue;
                    }
                    use(g, env, VALUE, out);
                }
                if (s.having() != null) {
                    use(s.having(), env, VALUE, out);
                }
                if (s.qualify() != null) {
                    use(s.qualify(), env, VALUE, out);
                }
                for (SqlSelect.SortKey k : s.orderBy()) {
                    use(k.expr(), env, VALUE, out);
                }
            }
            case SqlUnion u -> {
                for (SqlQuery b : u.branches()) {
                    scanQuery(b, outer, out, root);
                }
            }
            case com.legend.sql.SqlWith w -> {
                for (var c : w.ctes()) {
                    scanQuery(c.query(), outer, out, root);
                }
                scanQuery(w.body(), outer, out, root);
            }
        }
    }

    /** The ROOT SPINE of a FROM tree: the aliases reachable through LEFT
     * sides only — the extent's own row, before any navigation join. */
    private static void rootSpine(SqlSource src, Set<String> out) {
        switch (src) {
            case SqlSource.Join j -> rootSpine(j.left(), out);
            case SqlSource.Table t -> out.add(t.alias());
            case SqlSource.TableFunction f -> out.add(f.alias());
            case SqlSource.VarSetPlaceholder vp -> out.add(vp.alias());
            case SqlSource.RawSql raw -> out.add(raw.alias());
            case SqlSource.Subselect s -> out.add(s.alias());
            case SqlSource.Pivot p -> rootSpine(p.source(), out);
            case SqlSource.Values v -> out.add(v.alias());
            case SqlSource.SourceUrl u -> out.add(u.alias());
            case SqlSource.Dual d -> { }
        }
    }

    /** JOINED (non-root-spine) SUBSELECT aliases of a FROM tree — view
     * frames a navigation joined in. */
    private static void joinedSubselects(SqlSource src, Set<String> spine,
            Set<String> out) {
        switch (src) {
            case SqlSource.Join j -> {
                joinedSubselects(j.left(), spine, out);
                joinedSubselects(j.right(), spine, out);
            }
            case SqlSource.Subselect s -> {
                // only IDENTITY-CARRYING frames (views) qualify — an
                // isolation/exists subselect is plumbing, not a PM join
                if (!spine.contains(s.alias()) && s.frameName() != null
                        && !SqlSource.Subselect.EXISTS_KEYS_FRAME
                                .equals(s.frameName())) {
                    out.add(s.alias());
                }
            }
            default -> { }
        }
    }

    /** The FROM tree's alias environment; join ONs register their
     * JoinTreeNode uses here (they are uses regardless of what the
     * select above projects). */
    private static Map<String, Resolver> envOf(SqlSource src,
            Map<String, Resolver> outer, Set<Entry> out) {
        Map<String, Resolver> env = new LinkedHashMap<>();
        collectEnv(src, outer, env, out);
        return env;
    }

    private static void collectEnv(SqlSource src, Map<String, Resolver> outer,
            Map<String, Resolver> env, Set<Entry> out) {
        switch (src) {
            case SqlSource.Dual d -> {
                // FROM-less: no bindings
            }
            case SqlSource.Table t -> {
                env.put(t.alias(), (col, ctx, o) ->
                        o.add(new Entry(t.name(), col, ctx)));
            }
            // Lineage treats a tabular function as its own extent: the
            // function NAME is where the columns came from, exactly as
            // a table name is.
            case SqlSource.TableFunction f -> {
                env.put(f.alias(), (col, ctx, o) ->
                        o.add(new Entry(f.name(), col, ctx)));
            }
            case SqlSource.Join j -> {
                collectEnv(j.left(), outer, env, out);
                collectEnv(j.right(), outer, env, out);
                Map<String, Resolver> onEnv = new LinkedHashMap<>(outer);
                onEnv.putAll(env);
                if (j.on() != null) {
                    use(j.on(), onEnv, JOIN, out);
                }
            }
            case SqlSource.Subselect s -> {
                env.put(s.alias(), subResolver(s.inner(), outer, out));
                // the inner select's OWN clauses (filters, keys) are uses
                // even when nothing outside reads its columns
                scanQuery(s.inner(), outer, out, false);
            }
            case SqlSource.Values v -> env.put(v.alias(),
                    (col, ctx, o) -> { });
            case SqlSource.RawSql raw -> env.put(raw.alias(),
                    (col, ctx, o) -> { });
            case SqlSource.VarSetPlaceholder vp -> env.put(vp.alias(),
                    (col, ctx, o) -> { });
            case SqlSource.Pivot p ->
                    collectEnv(p.source(), outer, env, out);
            case SqlSource.SourceUrl u -> env.put(u.alias(),
                    (col, ctx, o) -> { });
        }
    }

    /** Resolve a demanded output column of a subquery to its source
     * columns — pass-through projections keep the DEMAND's context. */
    private static Resolver subResolver(SqlQuery inner,
            Map<String, Resolver> outer, Set<Entry> out) {
        return (col, ctx, o) -> resolveThrough(inner, col, ctx, outer, o);
    }

    private static void resolveThrough(SqlQuery inner, String col, String ctx,
            Map<String, Resolver> outer, Set<Entry> out) {
        switch (inner) {
            case SqlSelect s -> {
                Map<String, Resolver> env = new LinkedHashMap<>(outer);
                env.putAll(envOf(s.from(), outer, new LinkedHashSet<>()));
                for (SqlSelect.Projection p : s.projections()) {
                    String name = p.outputName();
                    if (col.equalsIgnoreCase(name)) {
                        use(p.expr(), env, ctx, out);
                        return;
                    }
                }
                // star pass-throughs expose the inner sources' columns
                for (SqlSelect.Projection p : s.projections()) {
                    if (p.expr() instanceof SqlExpr.Star st) {
                        Resolver r = env.get(st.table());
                        if (r != null) {
                            r.resolve(col, ctx, out);
                        }
                    } else if (p.expr() instanceof SqlExpr.StarExcept se
                            && se.except().stream()
                                    .noneMatch(col::equalsIgnoreCase)) {
                        Resolver r = env.get(se.table());
                        if (r != null) {
                            r.resolve(col, ctx, out);
                        }
                    }
                }
                if (s.projections().isEmpty()) {
                    // SELECT * subselect: every source column passes
                    for (Resolver r : env.values()) {
                        r.resolve(col, ctx, out);
                    }
                }
            }
            case SqlUnion u -> {
                for (SqlQuery b : u.branches()) {
                    resolveThrough(b, col, ctx, outer, out);
                }
            }
            case com.legend.sql.SqlWith w -> {
                for (var c : w.ctes()) {
                    resolveThrough(c.query(), col, ctx, outer, out);
                }
                resolveThrough(w.body(), col, ctx, outer, out);
            }
        }
    }

    /** One expression's column USES under {@code ctx}. TOTAL: known
     * composites recurse explicitly, anything else walks its record
     * components (a silently skipped node would drop lineage). */
    private static void use(SqlExpr e, Map<String, Resolver> env, String ctx,
            Set<Entry> out) {
        switch (e) {
            case SqlExpr.Column c -> {
                Resolver r = env.get(c.table());
                if (r != null) {
                    r.resolve(c.name(), ctx, out);
                }
            }
            case SqlExpr.Star ignored -> { }
            case SqlExpr.StarExcept ignored -> { }
            case SqlExpr.Exists x -> scanQuery(x.subquery(), env, out, false);
            case SqlExpr.ScalarSubquery sq -> {
                scanQuery(sq.subquery(), env, out, false);
                if (sq.subquery() instanceof SqlSelect ss) {
                    // the scalar's own projection IS demanded
                    Map<String, Resolver> ienv = new LinkedHashMap<>(env);
                    ienv.putAll(envOf(ss.from(), env, out));
                    for (SqlSelect.Projection p : ss.projections()) {
                        use(p.expr(), ienv, ctx, out);
                    }
                }
            }
            default -> useChildren(e, env, ctx, out);
        }
    }

    /** Every other expression: its TYPED children ({@code SqlExpr.children()},
     * the one traversal contract every SQL walker shares — exhaustive over
     * the variants, so a new node kind declares its children there or fails
     * to compile). Query-carrying nodes are the explicit arms above. */
    private static void useChildren(SqlExpr node, Map<String, Resolver> env,
            String ctx, Set<Entry> out) {
        for (SqlExpr child : node.children()) {
            use(child, env, ctx, out);
        }
    }
}
