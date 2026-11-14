// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.spec.typed.TypedSpec;

/** The plan-text ENVELOPE assembly (extracted from BodyDriver at
 * the file guardrail): applies the temp-table IN protocol
 * (plan/InProtocol) and picks the envelope — the engine's rule:
 * let-allocations stay in the outer Sequence and the
 * RelationalBlockExecutionNode wraps only [in-allocs, Relational];
 * with no lets the block IS the envelope. The planner's since the seam
 * program's S2g (2026-10-31): plan text, written at planning. */
public final class PlanEnvelope {

    private PlanEnvelope() {
    }

    static String emit(EnginePlans.EngineSql es,
            java.util.List<String> children, PlanEnv env,
            String rootClass, String[] impl, String mappingFqn,
            TypedSpec term, @com.legend.base.Nullable String connName,
            com.legend.model.ConnectionDefinition.DatabaseType dbType,
            com.legend.sql.dialect.EngineTextDialect pd, boolean hasParams) {
        EngineText.InListTempTables inList = EngineText.inListTempTables(dbType);
        var inp = com.legend.plan.InProtocol.apply(es.plan(),
                com.legend.plan.InProtocol.thresholdFor(connName, inList.threshold()),
                inList.tablePrefix());
        java.util.List<String> inTexts = inp == null ? java.util.List.of()
                : com.legend.plan.InProtocol.allNodeTexts(inp, connName,
                        inList.threshold(), pd::collectionSplice, pd::inTempTableSelect);
        var planOut = inp != null ? inp.plan() : es.plan();
        boolean pushDownEnums = BodyPlanner.pushDownEnums(env, java.util.List.of(term));
        String rel = com.legend.plan.PlanText.single(env.ctx(), rootClass,
                mappingFqn, planOut,
                inp != null ? pd.render(planOut) : es.sql(),
                java.util.List.of(term), connName, java.util.List.of(), planOut, pushDownEnums, pd::selectList);
        String tb = com.legend.plan.PlanText.typeBlock(env.ctx(), rootClass,
                impl, es.plan(), java.util.List.of(term), mappingFqn, pushDownEnums);
        boolean hasLets = children.size() > (hasParams ? 1 : 0);
        if (inp != null && hasLets) {
            java.util.List<String> blockKids =
                    new java.util.ArrayList<>(inTexts);
            blockKids.add(rel);
            children.add(com.legend.plan.PlanText.relationalBlock(tb,
                    blockKids));
            return com.legend.plan.PlanText.sequence(tb, children);
        }
        children.addAll(inTexts);
        children.add(rel);
        return inp != null
                ? com.legend.plan.PlanText.relationalBlock(tb, children)
                : com.legend.plan.PlanText.sequence(tb, children);
    }
}
