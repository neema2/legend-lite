// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.plan;

import com.legend.compiler.spec.typed.TypedNativeCall;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    /** table &rarr; rows for one plan. */
    public static Map<String, List<List<String>>> rows(String planId, PlanNode root,
            List<String> templateFunctions) {
        List<List<String>> nodes = new ArrayList<>();
        List<List<String>> params = new ArrayList<>();
        List<List<String>> closure = new ArrayList<>();
        String rootId = emit(planId, root, null, 0, nodes, params, closure,
                new ArrayList<>());
        Map<String, List<List<String>>> out = new LinkedHashMap<>();
        List<String> plan = new ArrayList<>();
        plan.add(planId);
        plan.add(rootId);
        out.put("plans", List.of(plan));
        out.put("plan_nodes", nodes);
        List<List<String>> fns = new ArrayList<>();
        for (int i = 0; i < templateFunctions.size(); i++) {
            List<String> r = new ArrayList<>();
            r.add(planId);
            r.add(Integer.toString(i));
            r.add(templateFunctions.get(i));
            fns.add(r);
        }
        out.put("plan_template_functions", fns);
        out.put("plan_function_parameters", params);
        out.put("plan_node_closure", closure);
        List<List<String>> conns = new ArrayList<>();
        List<List<String>> connSqls = new ArrayList<>();
        connectionRows(planId, root, null, 0, conns, connSqls);
        out.put("plan_connections", conns);
        out.put("plan_connection_sqls", connSqls);
        return out;
    }

    /** A node's CONNECTION as rows: plan_connections(node_id, kind,
     * db_type, test_data_setup_csv, ds_kind, ds_test_data_setup_csv) and
     * plan_connection_sqls(node_id, owner, ordinal, text) — the engine's
     * plan carries the runtime connection on every SQLExecutionNode with
     * processRuntimeTestConnections' DDL expansion on testDataSetupSqls
     * (owner 'conn' = the connection's own sqls, 'ds' = its
     * LocalH2DatasourceSpecification's). Node ids follow {@link #emit}. */
    private static void connectionRows(String planId, PlanNode n, @com.legend.base.Nullable String parentId,
            int ordinal, List<List<String>> conns, List<List<String>> sqls) {
        String id = planId + "/" + (parentId == null ? "root" : parentId.substring(planId.length() + 1) + "." + ordinal);
        PlanConn c = n.connection();
        if (c != null) {
            PlanConn.DsSpec ds = c.datasourceSpecification();
            List<String> row = new ArrayList<>();
            row.add(id);
            row.add(c.kind());
            row.add(c.type());
            row.add(c.testDataSetupCsv());
            row.add(ds == null ? null : ds.kind());
            row.add(ds == null ? null : ds.testDataSetupCsv());
            conns.add(row);
            for (int i = 0; i < c.testDataSetupSqls().size(); i++) {
                sqls.add(List.of(id, "conn", Integer.toString(i), c.testDataSetupSqls().get(i)));
            }
            if (ds != null) {
                for (int i = 0; i < ds.testDataSetupSqls().size(); i++) {
                    sqls.add(List.of(id, "ds", Integer.toString(i), ds.testDataSetupSqls().get(i)));
                }
            }
        }
        for (int i = 0; i < n.children().size(); i++) {
            connectionRows(planId, n.children().get(i), id, i, conns, sqls);
        }
    }

    private static String emit(String planId, PlanNode n, @com.legend.base.Nullable String parentId,
            int ordinal, List<List<String>> sink, List<List<String>> params,
            List<List<String>> closure, List<String> ancestors) {
        String id = planId + "/" + (parentId == null ? "root" : parentId.substring(planId.length() + 1) + "." + ordinal);
        List<String> row = new ArrayList<>();
        row.add(id);
        row.add(planId);
        row.add(parentId);
        row.add(Integer.toString(ordinal));
        row.add(n.kind());
        row.add(n.sqlQuery());
        row.add(n.sqlComment());
        sink.add(row);
        for (int i = 0; i < n.functionParameters().size(); i++) {
            PlanNode.Param p = n.functionParameters().get(i);
            List<String> pr = new ArrayList<>();
            pr.add(id);
            pr.add(Integer.toString(i));
            pr.add(p.name());
            pr.add(Boolean.toString(p.supportsStream()));
            params.add(pr);
        }
        List<String> chain = new ArrayList<>(ancestors);
        chain.add(id);
        for (int d = 0; d < chain.size(); d++) {
            List<String> cr = new ArrayList<>();
            cr.add(chain.get(chain.size() - 1 - d));
            cr.add(id);
            cr.add(Integer.toString(d));
            closure.add(cr);
        }
        for (int i = 0; i < n.children().size(); i++) {
            emit(planId, n.children().get(i), id, i, sink, params, closure, chain);
        }
        return id;
    }
}
