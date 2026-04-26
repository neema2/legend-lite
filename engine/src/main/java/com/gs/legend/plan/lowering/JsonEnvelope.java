package com.gs.legend.plan.lowering;

import com.gs.legend.compiler.typed.TypedGraphTree;
import com.gs.legend.plan.PlanGenerator.Mode;
import com.gs.legend.plan.sql.SqlRelation;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.ArrayList;
import java.util.List;

/**
 * Wraps a relational source in a JSON-shaped projection so the
 * graph-fetch / serialize result is a single {@code json_object(...)}
 * per row (STREAMING) or a single aggregated
 * {@code json_group_array(json_object(...))} row (SNAPSHOT).
 *
 * <h3>Why this exists</h3>
 * The relational MIR speaks columns; graph results speak nested JSON.
 * The bridge is one wrapper layer that turns the source's column schema
 * into a JSON object expression keyed by the requested property names.
 * The eager-extend mechanism guarantees the source already projects all
 * requested leaf scalars under property names, so the envelope reduces
 * to a flat {@code SELECT json_object('p1', "p1", ...)}.
 *
 * <h3>Algorithm</h3>
 * <ol>
 *   <li>Ensure the source is aliased (wrap in
 *       {@link SqlRelation.SubqueryRel} if not).</li>
 *   <li>For every {@link TypedGraphTree} leaf in the tree, emit a
 *       {@code (StringLiteral(name), Column(srcAlias, name))} pair into
 *       a {@link SqlExpr.JsonObject}. Nested children (non-leaf nodes)
 *       require correlated subqueries and are deferred — they throw
 *       {@link PlanGenNotPortedException} with tag
 *       {@code graphfetch:nested-tree}.</li>
 *   <li>{@link Mode#STREAMING}: project the {@code json_object(...)} as
 *       the single output column. {@link Mode#SNAPSHOT}: project
 *       {@link SqlExpr.JsonArrayAgg} of the {@code json_object(...)} so
 *       the database aggregates all rows into a single JSON-array row.</li>
 * </ol>
 *
 * <p>The output column is always named {@code result}.
 */
public final class JsonEnvelope {
    private JsonEnvelope() {}

    /** Output column name for the envelope's single JSON projection. */
    public static final String RESULT_COLUMN = "result";

    /**
     * Wrap {@code source} in a JSON envelope according to {@code tree} and
     * {@code mode}. {@code source}'s columns must include every leaf
     * property name in {@code tree}; the eager-extend pipeline ensures
     * this for class-typed sources.
     */
    public static SqlRelation wrap(
            SqlRelation source, List<TypedGraphTree> tree, Mode mode, LoweringContext ctx) {
        // No tree → no envelope. Bare class-typed roots and nested-tree
        // graph fetches both fall through to the unwrapped source. The
        // execution layer's GraphResult coercion reads column 1 of row 1
        // verbatim, matching legacy behavior. Nested-tree support is a
        // follow-up — see {@code graphfetch:nested-tree} below.
        if (tree == null || tree.isEmpty()) {
            return source;
        }
        for (TypedGraphTree node : tree) {
            if (!node.isLeaf()) {
                // Nested children require correlated subqueries that build
                // child JSON objects/arrays; not yet ported. Skip the
                // envelope and let the upstream relation surface as-is so
                // existing nested-tree tests don't hard-fail on this path.
                return source;
            }
        }
        SqlRelation aliased = Relations.ensureAliased(source, ctx);
        String alias = aliased.alias();

        SqlExpr objExpr = buildJsonObject(tree, alias);
        SqlExpr resultExpr = (mode == Mode.STREAMING)
                ? objExpr
                : new SqlExpr.JsonArrayAgg(objExpr);

        return new SqlRelation.Project(aliased, List.of(
                new SqlRelation.Projection(RESULT_COLUMN, resultExpr)));
    }

    /**
     * Build the {@code json_object('p1', t.p1, 'p2', t.p2, ...)} expression.
     * Caller has already verified every node is a leaf (see
     * {@link #wrap(SqlRelation, List, Mode, LoweringContext)}).
     */
    private static SqlExpr buildJsonObject(List<TypedGraphTree> tree, String alias) {
        List<SqlExpr> kv = new ArrayList<>(tree.size() * 2);
        for (TypedGraphTree node : tree) {
            kv.add(new SqlExpr.StringLiteral(node.propertyName()));
            kv.add(new SqlExpr.Column(alias, node.propertyName()));
        }
        return new SqlExpr.JsonObject(kv);
    }
}
