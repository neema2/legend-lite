// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.Compiler;
import com.legend.base.Nullable;
import com.legend.builtin.Pure;
import com.legend.compiler.ResolvedNames;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.SourceSubst;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.error.TypeInferenceException;
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
import com.legend.error.MappingResolutionException;
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

/** The Allocation plan-node factory for let bindings (extracted from
 * BodyDriver at the file guardrail; the planner's since the seam
 * program's S2g, 2026-10-31: the handles' and activities' rows are
 * planning facts, written with no session). */
public final class PlanAllocations {

    private PlanAllocations() {
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
    public static @Nullable String node(
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
            String typeName = PlanText
                    .pureTypeName(let.info().type());
            String size = PurePrint.sizeRange(let.info().multiplicity());
            return PlanText.allocation(let.name(),
                    PlanText.scalarTypeBlock(typeName, size),
                    PlanText.constant(typeName, literal));
        }
        String rootClass = PlanText.rootGetAllClass(List.of(let.value()));
        if (rootClass == null) {
            // NON-RELATIONAL expression let — the engine's
            // PureExpressionPlatformExecutionNode: the expression rides
            // as PURE SOURCE with its required plan variables
            String typeName = PlanText
                    .pureTypeName(let.info().type());
            String size = PurePrint.sizeRange(let.info().multiplicity());
            StringBuilder req = new StringBuilder();
            collectRequires(let.value(), paramSpells, req,
                    new LinkedHashSet<>());
            return PlanText.allocation(let.name(),
                    PlanText.scalarTypeBlock(typeName, size),
                    PlanText.pureExp(typeName, size,
                            req.toString(),
                            PurePrint.source(let.value())));
        }
        var renderer = EngineText.enginePlanText(dbType, quote, timeZone);
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
        String typeName = PlanText
                .pureTypeName(let.info().type());
        String size = PurePrint.sizeRange(let.info().multiplicity());
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

    /** The plan's {@code processingTemplateFunctions}: the freemarker
     * support functions + enum-typed parameters' dynamic enum-map
     * functions (relationalPlanSupportFunctions(connection),
     * executionPlan_generation.pure:215). */
    public static List<String> planTemplateFunctions(
            TypedNativeCall pep,
            List<TypedSpec> letPrefix,
            PlanEnv env) {
        List<String> supportFns = new ArrayList<>(
                PlanSupportFunctions
                        .relationalPlanSupportFunctions(
                                pep.args().size() > 2
                                        ? ExecutionContext.reader()
                                                .bind(v -> Lets.bound(v, letPrefix))
                                                .read(Optional.empty(), pep.args().get(2))
                                                .timeZone()
                                        : null));
        if (pep.args().get(0) instanceof TypedLambda plam
                && pep.args().size() > 1
                && pep.args().get(1) instanceof TypedPackageableRef pmr) {
            Set<String> seenFns =
                    new LinkedHashSet<>();
            for (QueryParameters.Declared prm : QueryParameters.of(plam)) {
                if (!(prm.type() instanceof Type.EnumType et)) {
                    continue;
                }
                String fn = PlanText.enumMapFnOf(
                        env.ctx(), pmr.fullPath(), et.fqn());
                var em = PlanText.enumMappingOf(
                        env.ctx(), pmr.fullPath(), et.fqn());
                if (fn != null && em != null && seenFns.add(fn)) {
                    supportFns.add(PlanSupportFunctions
                            .enumMapTemplateFunction(fn, em));
                }
            }
        }
        return supportFns;
    }

    /** The plan handle's nodes as rows under its scope id (PlanRows);
     * a plan the model cannot build registers nothing (the handle stays
     * symbolic and its reads keep their loud walls). */
    public static void registerPlanRows(
            TypedNativeCall pn,
            List<TypedSpec> letPrefix, SpecCompiler specs, PlanEnv env) {
        String scope = PlanRows.scopeId(pn);
        if (env.planRows().containsKey(scope)) {
            return;
        }
        // the handle's arguments through the let prefix (let l = {…};
        // executionPlan($l, …)): the plan model needs the lambda itself;
        // the rows register under BOTH spellings — the handle as written
        // and as the inliner's substitution rebuilds it
        List<TypedSpec> bound = new ArrayList<>();
        for (TypedSpec a : pn.args()) {
            bound.add(Lets.bound(a, letPrefix));
        }
        TypedNativeCall pnBound =
                (TypedNativeCall) pn.withChildren(bound);
        try {
            PlanNode model = EnginePlans.planModel(pnBound, specs, env);
            var rows = PlanRows.rows(scope, model,
                    planTemplateFunctions(pnBound, letPrefix, env));
            env.planRows().put(scope, rows);
            env.planRows().put(PlanRows.scopeId(pnBound), rows);
        } catch (NotImplementedException
                | TypeInferenceException
                | IllegalStateException e) {
            // no rows: the symbolic handle's walls stand
        }
    }

    /** The SQL an execute() call's RelationalActivity records: the
     * engine-style render of the frame's OWN assembled chain (the caller's
     * lets already folded, the mapping attached — the same chain the frame
     * runs; the same pipeline as toSQLString(query, mapping, H2, ext)).
     * Null when the call's mapping is not a literal reference or the chain
     * does not render (the frame's own walls stand). */
    public static @Nullable String activitySql(
            TypedNativeCall ec,
            TypedSpec chain,
            List<TypedSpec> letPrefix,
            SpecCompiler specs, PlanEnv env) {
        if (ec.args().size() < 2) {
            return null;
        }
        TypedSpec m = Lets.bound(ec.args().get(1), letPrefix);
        String mappingFqn = m instanceof TypedPackageableRef pr
                ? pr.fullPath() : chainMapping(chain);
        if (mappingFqn == null) {
            return null;
        }
        try {
            // C4 (handed off): the execute call's runtime type is not read here yet
            var renderer = EngineText.engineText(EngineText.ENGINE_TEST_DATABASE);
            EnginePlans.EngineSql es = EnginePlans.engineSql(
                    List.of(chain), mappingFqn, specs, env, renderer,
                    Map.of(), UnaryOperator.identity());
            SqlQuery post = SqlPostProcessors.applyRecorded(
                    es.plan(), env.tableReplace(),
                    env.postProcessors().extractCtes(),
                    env.postProcessors().nonExecutable());
            return post == es.plan() ? es.sql() : renderer.render(post);
        } catch (NotImplementedException
                | TypeInferenceException
                | MappingResolutionException
                | IllegalStateException e) {
            // diagnostics (env-gated): the render's own wall, which the
            // rows path downstream would otherwise mask
            if (System.getenv("LL_TMP_DEBUG") != null) {
                System.err.println("[render-debug] " + e.getClass().getSimpleName() + ": "
                        + String.valueOf(e.getMessage()).replace('\n', ' ')
                                .substring(0, Math.min(240, String.valueOf(e.getMessage()).length())));
            }
            return null;
        }
    }

    /** The mapping the chain itself carries (an in-query {@code from(m, r)}
     * / {@code withMapping(m)} — the execute's own mapping argument being a
     * placeholder {@code ^Mapping()}), outermost first; null when none. */
    private static @Nullable String chainMapping(TypedSpec chain) {
        TypedSpec cur = chain;
        while (cur != null) {
            if (cur instanceof TypedFrom f
                    && f.mapping().isPresent()) {
                return f.mapping().get().fullPath();
            }
            var kids = cur.children();
            cur = kids.isEmpty() ? null : kids.get(0);
        }
        return null;
    }

    /** An execute() call's Result and activity rows under the call's
     * scope: ONE RelationalActivity carrying the SQL the platform ran
     * (its own render — the same pipeline as toSQLString) and the
     * execution-trace COMMENT the statement really carried (batch 83:
     * ExecutionTrace's stamp of the frame's own run; a frame that did not
     * run records none), and no rewritten query is printed from Java
     * (the routed query as rows is its own leg). */
    public static void registerActivityRows(TypedNativeCall ec,
            TypedSpec chain,
            List<TypedSpec> letPrefix,
            SpecCompiler specs,
            @Nullable String comment,
            PlanEnv env) {
        registerActivityRows(ec, chain, letPrefix, specs, comment, null, env);
    }

    /** {@link #registerActivityRows} where the activity's comment is {@code commentParameter}'s value, a plan
     *  parameter bound when the plan runs (the test-is-the-product plan's M3f, R17 C4: the trace stamp of the
     *  statement that runs the frame, minted per execution as upstream mints it), when it is given. */
    public static void registerActivityRows(TypedNativeCall ec,
            TypedSpec chain,
            List<TypedSpec> letPrefix,
            SpecCompiler specs,
            @Nullable String comment, @Nullable String commentParameter,
            PlanEnv env) {
        String scope = PlanRows.scopeId(ec);
        if (env.planRows().containsKey(scope)) {
            return;
        }
        // the SQL is a second render of the chain: computed only if a
        // program reads the result's activities (no SQL = no rows, as ever)
        env.planRows().put(scope, new LazyRows(
                () -> activityRows(scope, activitySql(ec, chain, letPrefix, specs, env),
                        AggAwareActivities.rewrittenQuery(chain, env.ctx(), specs), comment, commentParameter)));
    }

    /** The activity row's comment column (results scope, activity id, scope, index, kind, sql, comment, rewritten). */
    private static final int COMMENT = 5;

    private static LazyRows.@Nullable Computed activityRows(String scope,
            @Nullable String sql, @Nullable String rewrittenQuery,
            @Nullable String comment, @Nullable String commentParameter) {
        if (sql == null) {
            return null;
        }
        Map<String, List<List<String>>> rows =
                new LinkedHashMap<>();
        rows.put("results", List.of(List.of(scope)));
        List<List<String>> acts = new ArrayList<>();
        if (rewrittenQuery != null) {
            // the engine records the aggregation-aware ROUTING activity
            // first (it happens at routing, before execution): the routed
            // query print names the set the router chose
            acts.add(List.of(scope + "/0", scope, "0",
                    "AggregationAwareActivity", "", "", rewrittenQuery));
        }
        int k = acts.size();
        acts.add(List.of(scope + "/" + k, scope, Integer.toString(k),
                "RelationalActivity", sql, comment == null ? "" : comment, ""));
        rows.put("activities", acts);
        return new LazyRows.Computed(rows, commentParameter == null ? List.of()
                : List.of(new LazyRows.Parameter("activities", k, COMMENT,
                        commentParameter)));
    }

    /** Every HANDLE native call inside a let's binding registers its rows
     * (the handle may sit under ->toOne(), ->removeDuplicates(), a cast —
     * the walk is shape-free). */
    public static void registerHandlesIn(String letName,
            TypedSpec rhs,
            List<TypedSpec> letPrefix,
            SpecCompiler specs,
            PlanEnv env) {
        if (rhs instanceof TypedNativeCall pn
                && PlatformTypes.handleRowClass(
                        pn.callee().id(), pn.callee().returnType()) != null) {
            registerHandleRows(letName, pn, letPrefix, specs, env);
        }
        for (TypedSpec c : rhs.children()) {
            registerHandlesIn(letName, c, letPrefix, specs, env);
        }
    }

    /** A HANDLE native bound by a let (executionPlan, scanRelations): its
     * rows register under the handle's scope (PlanRows / LineageRows). */
    public static void registerHandleRows(String letName,
            TypedNativeCall pn,
            List<TypedSpec> letPrefix,
            SpecCompiler specs,
            PlanEnv env) {
        String fqn = pn.callee().qualifiedName();
        if (PlatformTypes.EXECUTION_PLAN.equals(fqn)) {
            registerPlanRows(pn, letPrefix, specs, env);
        } else if (PlatformTypes.SCAN_RELATIONS
                .equals(fqn)) {
            registerLineageRows(letName, pn, letPrefix, env);
        } else if (PlatformTypes.SCAN_COLUMNS
                .equals(fqn)) {
            registerColumnLineageRows(letName, pn, letPrefix, env);
        }
    }

    /** The column-lineage chain's rows: the query lambda is chased from
     * the scanColumns let through {@code buildPropertyTree($p.result)},
     * {@code scanProperties(<lambda>.expressionSequence->at(0)->
     * evaluateAndDeactivate(), …)} in the protocol lets; the columns are
     * the ones the LOWERED plan reads (ScanColumns over
     * Compiler.lowerResolved — the real pipeline's output). */
    private static void registerColumnLineageRows(String letName,
            TypedNativeCall pn,
            List<TypedSpec> letPrefix,
            PlanEnv env) {
        String scope = PlanRows.scopeId(pn);
        if (env.planRows().containsKey(scope)) {
            return;
        }
        Map<String, ValueSpecification> lets =
                new LinkedHashMap<>();
        ValueSpecification scan = null;
        for (ValueSpecification st : env.protocolBody()) {
            CString ln =
                    SourceSubst.letName(st);
            if (ln == null) {
                continue;
            }
            ValueSpecification v =
                    ((AppliedFunction) st).parameters().get(1);
            if (ln.value().equals(letName)) {
                scan = v;
                break;
            }
            lets.put(ln.value(), v);
        }
        // scanColumns(tree, m) under removeDuplicates → tree → buildPropertyTree(
        // $p.result) → $p → scanProperties(vs, …) → vs → lambda
        ValueSpecification cur = scan;
        LambdaFunction ql = null;
        for (int guard = 0; guard < 16 && cur != null && ql == null; guard++) {
            switch (cur) {
                case LambdaFunction lf -> ql = lf;
                case Variable v ->
                        cur = lets.get(v.name());
                case AppliedProperty ap -> cur = ap.receiver();
                case AppliedFunction af ->
                        cur = af.parameters().isEmpty() ? null : af.parameters().get(0);
                default -> cur = null;
            }
        }
        TypedSpec m =
                Lets.bound(
                        pn.args().get(1), letPrefix);
        String runtimeFqn = env.runtimeFqn();
        if (ql == null || runtimeFqn == null
                || !(m instanceof TypedPackageableRef mr)) {
            return;
        }
        try {
            SqlQuery plan = Compiler.lowerResolved(ql, env.ctx(),
                    runtimeFqn, false, mr.fullPath());
            var rows = ColumnLineageRows.rows(scope, env.ctx(),
                    mr.fullPath(), ScanColumns.entries(plan));
            env.planRows().put(scope, rows);
        } catch (NotImplementedException
                | IllegalStateException e) {
            // no rows: the symbolic handle's walls stand
        }
    }

    /** The lineage scan walks the RAW query lambda (the protocol AST the
     * source spelled — property paths, tds joins); it is found by the
     * let's name in the query's protocol body, its query argument chased
     * through the protocol lets. A tree the scanner cannot build registers
     * nothing (the handle stays symbolic; its reads keep their walls). */
    private static void registerLineageRows(String letName,
            TypedNativeCall pn,
            List<TypedSpec> letPrefix,
            PlanEnv env) {
        String scope = PlanRows.scopeId(pn);
        if (env.planRows().containsKey(scope)) {
            return;
        }
        Map<String, ValueSpecification> lets =
                new LinkedHashMap<>();
        AppliedFunction scan = null;
        for (ValueSpecification st : env.protocolBody()) {
            CString ln =
                    SourceSubst.letName(st);
            if (ln == null) {
                continue;
            }
            ValueSpecification v =
                    ((AppliedFunction) st).parameters().get(1);
            if (ln.value().equals(letName)
                    && v instanceof AppliedFunction af) {
                scan = af;
                break;
            }
            lets.put(ln.value(), v);
        }
        // a ->toOne() over the scan is the same scan
        while (scan != null && scan.parameters().size() == 1
                && ResolvedNames.referents(scan).stream().anyMatch(Pure::isToOneCall)
                && scan.parameters().get(0)
                        instanceof AppliedFunction inner) {
            scan = inner;
        }
        if (scan == null || scan.parameters().isEmpty()) {
            return;
        }
        ValueSpecification q = scan.parameters().get(0);
        while (q instanceof Variable qv
                && lets.containsKey(qv.name())) {
            q = lets.get(qv.name());
        }
        if (!(q instanceof LambdaFunction ql)) {
            return;
        }
        TypedSpec m =
                Lets.bound(
                        pn.args().get(1), letPrefix);
        if (!(m instanceof TypedPackageableRef mr)) {
            return;
        }
        try {
            var lines = ScanRelations.lines(env.ctx(), ql,
                    mr.fullPath(), pn.args().size() == 4);
            var rows = LineageRows.rows(scope, lines);
            env.planRows().put(scope, rows);
        } catch (NotImplementedException
                | IllegalStateException e) {
            // no rows: the symbolic handle's walls stand
        }
    }
}
