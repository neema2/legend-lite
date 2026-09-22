// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.plan;

import java.util.List;

/**
 * The PLAN NODE MODEL — one tree consumed by the text printer, the
 * plan-handle walk vocabulary ({@code $plan.rootExecutionNode
 * ->allNodes(...)->filter(...)->cast(@SQLExecutionNode).sqlQuery}) and,
 * later, the JSON wire serializer. Kinds are the engine's class SIMPLE
 * names (instanceOf/cast match on them).
 *
 * @param kind e.g. {@code SequenceExecutionNode},
 *             {@code FunctionParametersValidationNode},
 *             {@code RelationalInstantiationExecutionNode},
 *             {@code SQLExecutionNode}
 */
public record PlanNode(String kind, List<PlanNode> children,
                       @com.legend.base.Nullable String sqlQuery, List<Param> functionParameters,
                       @com.legend.base.Nullable String sqlComment,
                       @com.legend.base.Nullable PlanConn connection) {

    /** The engine's default post-processor stamps EVERY SelectSQLQuery
     * with this comment and generateSQLExecutionNode copies it
     * (defaultPostProcessor.pure:71) — a constant, never gated. */
    public static final String EXEC_TRACE_COMMENT =
            "-- \"executionTraceID\" : \"${execID}\"";

    public PlanNode(String kind, List<PlanNode> children,
            @com.legend.base.Nullable String sqlQuery,
            List<Param> functionParameters) {
        this(kind, children, sqlQuery, functionParameters, null, null);
    }

    public PlanNode(String kind, List<PlanNode> children,
            @com.legend.base.Nullable String sqlQuery,
            List<Param> functionParameters,
            @com.legend.base.Nullable String sqlComment) {
        this(kind, children, sqlQuery, functionParameters, sqlComment, null);
    }

    public PlanNode {
        children = children == null ? List.of() : List.copyOf(children);
        functionParameters = functionParameters == null ? List.of()
                : List.copyOf(functionParameters);
    }

    /** {@code FunctionParameter}: {@code supportsStream} is TRUE for
     * COLLECTION-multiplicity parameters (the engine streams collection
     * inputs; scalars validate eagerly). */
    public record Param(String name, boolean supportsStream) {
    }

    /** This node plus every descendant ({@code allNodes}). */
    public java.util.List<PlanNode> allNodes() {
        java.util.List<PlanNode> out = new java.util.ArrayList<>();
        out.add(this);
        for (PlanNode c : children()) {
            out.addAll(c.allNodes());
        }
        return out;
    }
}
