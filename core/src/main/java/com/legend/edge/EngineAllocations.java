// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.planner.PlanEnv;
import com.legend.Compiler;
import com.legend.base.Nullable;
import com.legend.builtin.Pure;
import com.legend.compiler.ResolvedNames;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.SourceSubst;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.ExecutionContext;
import com.legend.compiler.spec.typed.Lets;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCDate;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.NotImplementedException;
import com.legend.lineage.ColumnLineageRows;
import com.legend.lineage.LineageRows;
import com.legend.lineage.ScanColumns;
import com.legend.lineage.ScanRelations;
import com.legend.lowering.SqlPostProcessors;
import com.legend.model.ConnectionDefinition;
import com.legend.plan.LazyRows;
import com.legend.plan.PlanNode;
import com.legend.plan.PlanRows;
import com.legend.plan.PlanSupportFunctions;
import com.legend.plan.PlanText;
import com.legend.plan.PurePrint;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;

/** legend-engine's Allocation plan node for one plan let (moved from the planner's {@code PlanAllocations} with
 *  {@link EnginePlans}, W9's planner half, 2026-11-16): upstream's plan text, the compatibility edge's. */
final class EngineAllocations {

    private EngineAllocations() {
    }

    /** The free PLAN VARIABLES the expression reads, in first-read
     * order, spelled {@code name(Type[mult])}. */
    private static void collectRequires(
            TypedSpec n,
            Map<String, String> paramSpells, StringBuilder out,
            Set<String> seen) {
        if (n instanceof TypedVariable v
                && paramSpells.containsKey(v.name())
                && seen.add(v.name())) {
            if (out.length() > 0) {
                out.append(", ");
            }
            out.append(v.name()).append('(')
                    .append(paramSpells.get(v.name())).append(')');
        }
        for (TypedSpec c : n.children()) {
            collectRequires(c, paramSpells, out, seen);
        }
    }

    /** An Allocation child for one plan let: LITERAL values print as
     * Constant nodes, scalar query values as SCALAR-projection
     * Relational nodes (bare-typed, alias-less select), and CLASS query
     * values as full Class-envelope Relational nodes — the engine's
     * three Allocation value forms. */
    static @Nullable String node(
            TypedLet let, String mappingFqn,
            SpecCompiler specs, PlanEnv env,
            Map<String, SqlExpr.PlanParam> params,
            Map<String, String> paramSpells,
            boolean quote, @Nullable String timeZone, ConnectionDefinition.DatabaseType dbType,
            @Nullable String connName) {
        String literal = switch (let.value()) {
            case TypedCString cs -> cs.value();
            case TypedCInteger ci ->
                    String.valueOf(ci.value());
            case TypedCFloat cf ->
                    String.valueOf(cf.value());
            case TypedCBoolean cb ->
                    String.valueOf(cb.value());
            case TypedCDate cd ->
                    cd.value().toEngineString();
            default -> null;
        };
        if (literal != null) {
            String typeName = com.legend.plan.PureSpelling
                    .typeName(let.info().type());
            String size = com.legend.plan.PureSpelling.sizeRange(let.info().multiplicity());
            return PlanText.allocation(let.name(),
                    PlanText.scalarTypeBlock(typeName, size),
                    PlanText.constant(typeName, literal));
        }
        String rootClass = PlanText.rootGetAllClass(List.of(let.value()));
        if (rootClass == null) {
            // NON-RELATIONAL expression let — the engine's
            // PureExpressionPlatformExecutionNode: the expression rides
            // as PURE SOURCE with its required plan variables
            String typeName = com.legend.plan.PureSpelling
                    .typeName(let.info().type());
            String size = com.legend.plan.PureSpelling.sizeRange(let.info().multiplicity());
            StringBuilder req = new StringBuilder();
            collectRequires(let.value(), paramSpells, req,
                    new LinkedHashSet<>());
            return PlanText.allocation(let.name(),
                    PlanText.scalarTypeBlock(typeName, size),
                    PlanText.pureExp(typeName, size,
                            req.toString(),
                            PurePrint.source(let.value())));
        }
        var renderer = env.style().enginePlanText(dbType, quote, timeZone);
        EnginePlans.EngineSql es = EnginePlans.engineSql(List.of(let.value()),
                mappingFqn, specs, env, renderer, params,
                UnaryOperator.identity());
        String[] impl = ScanRelations.rootImpl(
                env.ctx(), mappingFqn, rootClass);
        if (let.info().type()
                instanceof Type.ClassType) {
            // class-valued allocation: the full Class-envelope node, and
            // the Allocation's own type block is the impls form
            String inner = PlanText.single(env.ctx(),
                    rootClass, mappingFqn, es.plan(), es.sql(),
                    List.of(let.value()), connName, List.of(), es.plan(), false,
                    renderer::selectList);
            return PlanText.allocation(let.name(),
                    PlanText.typeBlock(env.ctx(), rootClass,
                            impl, es.plan(), List.of(let.value())),
                    inner);
        }
        String typeName = com.legend.plan.PureSpelling
                .typeName(let.info().type());
        String size = com.legend.plan.PureSpelling.sizeRange(let.info().multiplicity());
        if (!(es.plan() instanceof SqlSelect sel)) {
            throw new NotImplementedException(
                    "plan: Allocation value lowers to a non-select");
        }
        SqlSelect bareSel = new SqlSelect(
                sel.projections().stream().map(p ->
                        new SqlSelect.Projection(
                                p.expr(), null, p.out())).toList(),
                sel.distinct(), sel.from(), sel.where(), sel.groupBy(),
                sel.having(), sel.qualify(), sel.orderBy(), sel.limit(),
                sel.offset(), sel.outputs());
        String bareSql = renderer.render(bareSel);
        String inner = PlanText.scalarRelational(env.ctx(),
                impl[2], sel, typeName, size, bareSql,
                renderer::renderedAlias, connName);
        return PlanText.allocation(let.name(),
                PlanText.scalarTypeBlock(typeName, size),
                inner);
    }

    // ---- the plan handle AS ROWS (harness burn-down group Q, 2026-09-03):
    // the executor's plan model becomes inline rows the DATABASE navigates
    // (PlanRows) — nothing a verdict reads is computed here

}
