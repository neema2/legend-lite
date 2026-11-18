// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.plan;

import com.legend.compiler.spec.typed.TypedNativeCall;

/**
 * An execution plan AS ROWS (harness burn-down, group Q — metamodel-as-
 * relations homework §2e): the executor's own plan model ({@link PlanNode},
 * the lowering's product) becomes rows of the system store's
 * {@code plans} / {@code plan_nodes} / {@code plan_template_functions}
 * tables, keyed by the plan handle's content id, and the plan reads
 * ({@code $plan.rootExecutionNode.executionNodes->filter(n|$n->instanceOf(
 * RelationalInstantiationExecutionNode))->at(0)…->cast(@SQLExecutionNode)
 * .sqlQuery}) are ordinary navigation over those rows. THE ROWS RIDE THE
 * QUERY (user ruling 2026-09-02): they are inline relations under the
 * handle's scope; the system database is never written for them.
 *
 * <p>Column ORDER follows the store's table declarations
 * ({@code SystemMetamodel}): plans(id, root_node_id); plan_nodes(id,
 * plan_id, parent_id, ordinal, kind, sql_query, sql_comment);
 * plan_template_functions(plan_id, ordinal, text);
 * plan_function_parameters(node_id, ordinal, name, supports_stream);
 * plan_node_closure(ancestor_id, node_id, depth) — every ancestor/
 * descendant pair (self at depth 0): the engine's recursive
 * {@code allNodes} walk as rows.
 */
public final class PlanRows {

    private PlanRows() {
    }

    /** The plan handle's SCOPE id — a content id of the typed call (the
     * same node the resolver meets as the chain root, rebuilt or not). */
    public static String scopeId(TypedNativeCall executionPlanCall) {
        // the CALL SITE identifies the plan: its source span survives every
        // rebuild (the inliner's substitutions, α-renamed binders inside
        // the query lambda change the node's text, never its position);
        // a synthesized call without a span keys on its text
        var pos = executionPlanCall.pos();
        if (pos != null) {
            return "plan:" + pos.sourceId() + ":" + pos.startLine() + ":" + pos.startColumn();
        }
        String text = executionPlanCall.toString();
        return "plan:" + Integer.toHexString(text.hashCode()) + ":" + text.length();
    }
}
