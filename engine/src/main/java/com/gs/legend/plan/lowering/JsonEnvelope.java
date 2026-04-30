package com.gs.legend.plan.lowering;

import com.gs.legend.compiler.PureCompileException;
import com.gs.legend.compiler.StoreResolution;
import com.gs.legend.compiler.StoreResolution.JoinResolution;
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
 *       become correlated {@link SqlExpr.ScalarSubquery} expressions
 *       built via {@link Relations#joinTargetRelation(StoreResolution, String, String, LoweringContext)}
 *       — same target-resolution mechanism as association LEFT JOINs at
 *       the root level, so identity-mapped tables and JSON-derived
 *       mappings compose uniformly.</li>
 *   <li>{@link Mode#STREAMING}: project the {@code json_object(...)} as
 *       the single output column. {@link Mode#SNAPSHOT}: project
 *       {@link SqlExpr.JsonArrayAgg} of the {@code json_object(...)} so
 *       the database aggregates all rows into a single JSON-array row.</li>
 * </ol>
 *
 * <p>The output column is always named {@code result}.
 *
 * <h3>Nested-tree shape (mirrors legacy {@code generateGraphFetch})</h3>
 * For a nested {@link TypedGraphTree} hop, {@link #buildNestedSubquery}
 * emits:
 * <pre>
 *   (SELECT json_object('p', c."p", ...) FROM &lt;target&gt; AS c WHERE &lt;joinCond&gt;)
 *   -- to-many wraps the json_object in {@link SqlExpr.JsonArrayAgg}
 * </pre>
 * The join condition is lowered with {@code sourceParam} bound to the
 * outer alias and {@code targetParam} bound to the child alias —
 * identical to {@link Relations#installJoin} so association navs and
 * nested fetches share the same correlation semantics.
 */
public final class JsonEnvelope {
    private JsonEnvelope() {}

    /** Output column name for the envelope's single JSON projection. */
    public static final String RESULT_COLUMN = "result";

    /**
     * Wrap {@code source} in a JSON envelope according to {@code tree} and
     * {@code mode}.
     *
     * @param parentStore {@link StoreResolution} of the fetched class — used
     *                    to look up {@link JoinResolution}s for nested
     *                    {@link TypedGraphTree} children. {@code null} is
     *                    permitted only when {@code tree} is leaf-only
     *                    (e.g. TDS-rooted graph fetches).
     */
    public static SqlRelation wrap(
            SqlRelation source, List<TypedGraphTree> tree,
            StoreResolution parentStore, Mode mode, LoweringContext ctx) {
        // No tree → no envelope. Bare class-typed roots without an explicit
        // tree pass through unchanged.
        if (tree == null || tree.isEmpty()) {
            return source;
        }
        SqlRelation aliased = Relations.ensureAliased(source, ctx);
        String alias = aliased.alias();

        SqlExpr objExpr = buildJsonObject(tree, alias, parentStore, ctx);
        SqlExpr resultExpr = (mode == Mode.STREAMING)
                ? objExpr
                : new SqlExpr.JsonArrayAgg(objExpr);

        return new SqlRelation.Project(aliased, List.of(
                new SqlRelation.Projection(RESULT_COLUMN, resultExpr)));
    }

    /**
     * Build the {@code json_object(<k1>, <v1>, ...)} expression for a
     * graph-fetch tree. Leaf nodes resolve to a column reference under
     * {@code parentAlias}; non-leaf nodes resolve to a correlated
     * {@link SqlExpr.ScalarSubquery} via {@link #buildNestedSubquery}.
     */
    private static SqlExpr buildJsonObject(List<TypedGraphTree> tree, String parentAlias,
                                           StoreResolution parentStore, LoweringContext ctx) {
        List<SqlExpr> kv = new ArrayList<>(tree.size() * 2);
        for (TypedGraphTree node : tree) {
            kv.add(new SqlExpr.StringLiteral(node.propertyName()));
            if (node.isLeaf()) {
                kv.add(leafColumnRef(node.propertyName(), parentAlias, parentStore));
            } else {
                kv.add(buildNestedSubquery(node, parentAlias, parentStore, ctx));
            }
        }
        return new SqlExpr.JsonObject(kv);
    }

    /**
     * Resolve a leaf property to a column reference. Uses the parent
     * store's {@code propertyToColumn} map when available so identity
     * mappings (where the physical column name differs from the property
     * alias) and derived mappings (where the alias is the column) both
     * work uniformly. Falls back to the property name when no parent
     * store is in scope (TDS / synthetic source).
     */
    private static SqlExpr leafColumnRef(String propertyName, String parentAlias,
                                         StoreResolution parentStore) {
        String column = parentStore == null
                ? propertyName
                : parentStore.propertyToColumn().getOrDefault(propertyName, propertyName);
        return new SqlExpr.Column(parentAlias, column);
    }

    /**
     * Build a correlated scalar subquery for a nested {@link TypedGraphTree}
     * hop. The structure mirrors legacy {@code buildNestedSubquery}:
     * <ol>
     *   <li>Look up the {@link JoinResolution.FkJoin} for the property
     *       on the parent class store. Embedded / lateral-unnest variants
     *       are not yet supported in nested position and surface as a
     *       compile error so the gap is visible.</li>
     *   <li>Resolve the child relation via
     *       {@link Relations#joinTargetRelation(StoreResolution, String, String, LoweringContext)}
     *       — handles identity {@link SqlRelation.TableRef} and
     *       wrapped-mapping-body {@link SqlRelation.SubqueryRel} cases
     *       symmetrically with association LEFT JOINs at the root.</li>
     *   <li>Lower the join condition in a context that binds
     *       {@code sourceParam} to the parent alias and
     *       {@code targetParam} to the child alias — identical to
     *       {@link Relations#installJoin}.</li>
     *   <li>Recurse into {@code node.children()} to build the child
     *       {@code json_object(...)}; wrap in {@link SqlExpr.JsonArrayAgg}
     *       when {@code FkJoin.isToMany()}.</li>
     *   <li>Project a single result column over a {@link SqlRelation.Filter}
     *       and wrap in {@link SqlExpr.ScalarSubquery}.</li>
     * </ol>
     */
    private static SqlExpr buildNestedSubquery(TypedGraphTree node, String parentAlias,
                                               StoreResolution parentStore, LoweringContext ctx) {
        if (parentStore == null || parentStore.joins() == null
                || !parentStore.joins().containsKey(node.propertyName())) {
            throw new PureCompileException(
                    "graphFetch nested property '" + node.propertyName()
                            + "' has no resolved join in the parent class store");
        }
        JoinResolution jr = parentStore.joins().get(node.propertyName());
        if (!(jr instanceof JoinResolution.FkJoin fk)) {
            throw new PureCompileException(
                    "graphFetch nested property '" + node.propertyName()
                            + "' resolves to " + jr.getClass().getSimpleName()
                            + " — only FkJoin is supported in nested position");
        }

        String childAlias = ctx.nextAlias();
        SqlRelation child = Relations.joinTargetRelation(
                fk.targetResolution(), fk.targetTable(), childAlias, ctx);

        // Bind source/target params and lower the join condition to a SQL
        // boolean expression. Same shape as Relations.installJoin so
        // correlated reads against {@code parentAlias} (sourceColumns) are
        // wired the same way.
        LoweringContext condCtx = ctx
                .bindVar(fk.sourceParam(), new SqlExpr.Identifier(parentAlias), parentStore)
                .bindVar(fk.targetParam(), new SqlExpr.Identifier(childAlias), fk.targetResolution());
        SqlExpr cond = Lowerer.lowerScalar(fk.joinCondition(), condCtx);

        SqlExpr childObj = buildJsonObject(node.children(), childAlias, fk.targetResolution(), ctx);
        SqlExpr wrapped = fk.isToMany() ? new SqlExpr.JsonArrayAgg(childObj) : childObj;

        SqlRelation filtered = new SqlRelation.Filter(child, cond);
        SqlRelation projected = new SqlRelation.Project(filtered, List.of(
                new SqlRelation.Projection(RESULT_COLUMN, wrapped)));
        return new SqlExpr.ScalarSubquery(projected);
    }
}
