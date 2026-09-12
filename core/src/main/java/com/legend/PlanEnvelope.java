// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.exec.ExecutionResult;

/** The plan-text ENVELOPE assembly (extracted from StatementExecutor at
 * the file guardrail): applies the temp-table IN protocol
 * (plan/InProtocol) and picks the envelope — the engine's rule:
 * let-allocations stay in the outer Sequence and the
 * RelationalBlockExecutionNode wraps only [in-allocs, Relational];
 * with no lets the block IS the envelope. */
final class PlanEnvelope {

    private PlanEnvelope() {
    }

    static ExecutionResult emit(StatementExecutor.EngineSql es,
            java.util.List<String> children, StatementExecutor.ExecEnv env,
            String rootClass, String[] impl, String mappingFqn,
            TypedSpec term, @com.legend.Nullable String connName,
            @com.legend.Nullable String dbType,
            com.legend.sql.dialect.EngineStyleH2 pd, boolean hasParams) {
        var inp = com.legend.plan.InProtocol.apply(es.plan(),
                com.legend.plan.InProtocol.thresholdFor(connName, dbType),
                dbType);
        java.util.List<String> inTexts = inp == null ? java.util.List.of()
                : com.legend.plan.InProtocol.allNodeTexts(inp, connName,
                        dbType, pd::collectionSplice);
        var planOut = inp != null ? inp.plan() : es.plan();
        boolean pushDownEnums = StatementExecutor.pushDownEnums(env, java.util.List.of(term));
        String rel = com.legend.plan.PlanText.single(env.ctx(), rootClass,
                mappingFqn, planOut,
                inp != null ? pd.render(planOut) : es.sql(),
                java.util.List.of(term), connName, java.util.List.of(), planOut, pushDownEnums);
        String tb = com.legend.plan.PlanText.typeBlock(env.ctx(), rootClass,
                impl, es.plan(), java.util.List.of(term), mappingFqn, pushDownEnums);
        boolean hasLets = children.size() > (hasParams ? 1 : 0);
        if (inp != null && hasLets) {
            java.util.List<String> blockKids =
                    new java.util.ArrayList<>(inTexts);
            blockKids.add(rel);
            children.add(com.legend.plan.PlanText.relationalBlock(tb,
                    blockKids));
            return new ExecutionResult.Scalar(
                    com.legend.plan.PlanText.sequence(tb, children),
                    com.legend.compiler.element.type.Type.Primitive.STRING);
        }
        children.addAll(inTexts);
        children.add(rel);
        return new ExecutionResult.Scalar(inp != null
                ? com.legend.plan.PlanText.relationalBlock(tb, children)
                : com.legend.plan.PlanText.sequence(tb, children),
                com.legend.compiler.element.type.Type.Primitive.STRING);
    }
}
