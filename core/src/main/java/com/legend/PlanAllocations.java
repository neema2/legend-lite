// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

/** The Allocation plan-node factory for let bindings (extracted from
 * StatementExecutor at the file guardrail). */
final class PlanAllocations {

    private PlanAllocations() {
    }

    /** The free PLAN VARIABLES the expression reads, in first-read
     * order, spelled {@code name(Type[mult])}. */
    private static void collectRequires(
            com.legend.compiler.spec.typed.TypedSpec n,
            java.util.Map<String, String> paramSpells, StringBuilder out,
            java.util.Set<String> seen) {
        if (n instanceof com.legend.compiler.spec.typed.TypedVariable v
                && paramSpells.containsKey(v.name())
                && seen.add(v.name())) {
            if (out.length() > 0) {
                out.append(", ");
            }
            out.append(v.name()).append('(')
                    .append(paramSpells.get(v.name())).append(')');
        }
        for (com.legend.compiler.spec.typed.TypedSpec c : n.children()) {
            collectRequires(c, paramSpells, out, seen);
        }
    }

    /** An Allocation child for one plan let: LITERAL values print as
     * Constant nodes, scalar query values as SCALAR-projection
     * Relational nodes (bare-typed, alias-less select), and CLASS query
     * values as full Class-envelope Relational nodes — the engine's
     * three Allocation value forms. */
    static @com.legend.base.Nullable String node(
            com.legend.compiler.spec.typed.TypedLet let, String mappingFqn,
            com.legend.compiler.spec.SpecCompiler specs, StatementExecutor.ExecEnv env,
            java.util.Map<String, com.legend.sql.SqlExpr.PlanParam> params,
            java.util.Map<String, String> paramSpells,
            boolean quote, @com.legend.base.Nullable String timeZone, @com.legend.base.Nullable String dbType) {
        String literal = switch (let.value()) {
            case com.legend.compiler.spec.typed.TypedCString cs -> cs.value();
            case com.legend.compiler.spec.typed.TypedCInteger ci ->
                    String.valueOf(ci.value());
            case com.legend.compiler.spec.typed.TypedCFloat cf ->
                    String.valueOf(cf.value());
            case com.legend.compiler.spec.typed.TypedCBoolean cb ->
                    String.valueOf(cb.value());
            case com.legend.compiler.spec.typed.TypedCDate cd ->
                    cd.value().toEngineString();
            default -> null;
        };
        if (literal != null) {
            String typeName = com.legend.plan.PlanText
                    .pureTypeName(let.info().type());
            String size = com.legend.plan.PurePrint.sizeRange(let.info().multiplicity());
            return com.legend.plan.PlanText.allocation(let.name(),
                    com.legend.plan.PlanText.scalarTypeBlock(typeName, size),
                    com.legend.plan.PlanText.constant(typeName, literal));
        }
        String rootClass = com.legend.plan.PlanText.rootGetAllClass(java.util.List.of(let.value()));
        if (rootClass == null) {
            // NON-RELATIONAL expression let — the engine's
            // PureExpressionPlatformExecutionNode: the expression rides
            // as PURE SOURCE with its required plan variables
            String typeName = com.legend.plan.PlanText
                    .pureTypeName(let.info().type());
            String size = com.legend.plan.PurePrint.sizeRange(let.info().multiplicity());
            StringBuilder req = new StringBuilder();
            collectRequires(let.value(), paramSpells, req,
                    new java.util.LinkedHashSet<>());
            return com.legend.plan.PlanText.allocation(let.name(),
                    com.legend.plan.PlanText.scalarTypeBlock(typeName, size),
                    com.legend.plan.PlanText.pureExp(typeName, size,
                            req.toString(),
                            com.legend.plan.PurePrint.source(let.value())));
        }
        StatementExecutor.EngineSql es = StatementExecutor.engineSql(java.util.List.of(let.value()),
                mappingFqn, specs, env,
                StatementExecutor.planDialect(dbType, quote, timeZone), params,
                java.util.function.UnaryOperator.identity());
        String[] impl = com.legend.lineage.ScanRelations.rootImpl(
                env.ctx(), mappingFqn, rootClass);
        if (let.info().type()
                instanceof com.legend.compiler.element.type.Type.ClassType) {
            // class-valued allocation: the full Class-envelope node, and
            // the Allocation's own type block is the impls form
            String inner = com.legend.plan.PlanText.single(env.ctx(),
                    rootClass, mappingFqn, es.plan(), es.sql(),
                    java.util.List.of(let.value()));
            return com.legend.plan.PlanText.allocation(let.name(),
                    com.legend.plan.PlanText.typeBlock(env.ctx(), rootClass,
                            impl, es.plan(), java.util.List.of(let.value())),
                    inner);
        }
        String typeName = com.legend.plan.PlanText
                .pureTypeName(let.info().type());
        String size = com.legend.plan.PurePrint.sizeRange(let.info().multiplicity());
        if (!(es.plan() instanceof com.legend.sql.SqlSelect sel)) {
            throw new com.legend.error.NotImplementedException(
                    "plan: Allocation value lowers to a non-select");
        }
        com.legend.sql.SqlSelect bareSel = new com.legend.sql.SqlSelect(
                sel.projections().stream().map(p ->
                        new com.legend.sql.SqlSelect.Projection(
                                p.expr(), null, p.out())).toList(),
                sel.distinct(), sel.from(), sel.where(), sel.groupBy(),
                sel.having(), sel.qualify(), sel.orderBy(), sel.limit(),
                sel.offset(), sel.outputs());
        var renderer = StatementExecutor.planDialect(dbType, quote, timeZone);
        String bareSql = renderer.render(bareSel);
        String inner = com.legend.plan.PlanText.scalarRelational(env.ctx(),
                impl[2], sel, typeName, size, bareSql,
                renderer::renderedAlias);
        return com.legend.plan.PlanText.allocation(let.name(),
                com.legend.plan.PlanText.scalarTypeBlock(typeName, size),
                inner);
    }

    // ---- the plan handle AS ROWS (harness burn-down group Q, 2026-09-03):
    // the executor's plan model becomes inline rows the DATABASE navigates
    // (PlanRows) — nothing a verdict reads is computed here

    /** The plan's {@code processingTemplateFunctions}: the freemarker
     * support functions + enum-typed parameters' dynamic enum-map
     * functions (relationalPlanSupportFunctions(connection),
     * executionPlan_generation.pure:215). */
    static java.util.List<String> planTemplateFunctions(
            com.legend.compiler.spec.typed.TypedNativeCall pep,
            java.util.List<com.legend.compiler.spec.typed.TypedSpec> letPrefix,
            StatementExecutor.ExecEnv env) {
        java.util.List<String> supportFns = new java.util.ArrayList<>(
                com.legend.plan.PlanSupportFunctions
                        .relationalPlanSupportFunctions(
                                pep.args().size() > 2
                                        ? com.legend.compiler.spec.typed.ExecutionContext.reader()
                                                .bind(v -> com.legend.compiler.spec.typed.Lets.bound(v, letPrefix))
                                                .read(java.util.Optional.empty(), pep.args().get(2))
                                                .timeZone()
                                        : null));
        if (pep.args().get(0) instanceof com.legend.compiler.spec
                        .typed.TypedLambda plam
                && pep.args().size() > 1
                && pep.args().get(1) instanceof com.legend.compiler
                        .spec.typed.TypedPackageableRef pmr
                && com.legend.compiler.element.type.PlatformTypes
                        .functionTypeOf(plam.info().type())
                        instanceof com.legend.compiler
                        .element.type.Type.FunctionType pft) {
            java.util.Set<String> seenFns =
                    new java.util.LinkedHashSet<>();
            for (var prm : pft.params()) {
                if (!(prm.type() instanceof com.legend.compiler
                        .element.type.Type.EnumType et)) {
                    continue;
                }
                String fn = com.legend.plan.PlanText.enumMapFnOf(
                        env.ctx(), pmr.fullPath(), et.fqn());
                var em = com.legend.plan.PlanText.enumMappingOf(
                        env.ctx(), pmr.fullPath(), et.fqn());
                if (fn != null && em != null && seenFns.add(fn)) {
                    supportFns.add(com.legend.plan
                            .PlanSupportFunctions
                            .enumMapTemplateFunction(fn, em));
                }
            }
        }
        return supportFns;
    }

    /** The plan handle's nodes as rows under its scope id (PlanRows);
     * a plan the model cannot build registers nothing (the handle stays
     * symbolic and its reads keep their loud walls). */
    static void registerPlanRows(
            com.legend.compiler.spec.typed.TypedNativeCall pn,
            java.util.List<com.legend.compiler.spec.typed.TypedSpec> letPrefix, com.legend.compiler.spec.SpecCompiler specs, StatementExecutor.ExecEnv env) {
        String scope = com.legend.plan.PlanRows.scopeId(pn);
        if (env.planRows().containsKey(scope)) {
            return;
        }
        // the handle's arguments through the let prefix (let l = {…};
        // executionPlan($l, …)): the plan model needs the lambda itself;
        // the rows register under BOTH spellings — the handle as written
        // and as the inliner's substitution rebuilds it
        java.util.List<com.legend.compiler.spec.typed.TypedSpec> bound = new java.util.ArrayList<>();
        for (com.legend.compiler.spec.typed.TypedSpec a : pn.args()) {
            bound.add(com.legend.compiler.spec.typed.Lets.bound(a, letPrefix));
        }
        com.legend.compiler.spec.typed.TypedNativeCall pnBound =
                (com.legend.compiler.spec.typed.TypedNativeCall) pn.withChildren(bound);
        try {
            com.legend.plan.PlanNode model = StatementExecutor.planModel(pnBound, specs, env);
            var rows = com.legend.plan.PlanRows.rows(scope, model,
                    planTemplateFunctions(pnBound, letPrefix, env));
            env.planRows().put(scope, rows);
            env.planRows().put(com.legend.plan.PlanRows.scopeId(pnBound), rows);
        } catch (com.legend.error.NotImplementedException
                | com.legend.compiler.spec.TypeInferenceException
                | IllegalStateException e) {
            // no rows: the symbolic handle's walls stand
        }
    }

    /** The engine-style SQL pipeline shared by toSQLString and the plan
     * printer: G½ inline, H resolve against the MAPPING ARGUMENT, root
     * form, I lower — IR plus rendered text. */
    /** The SQL an execute() call's RelationalActivity records: the
     * engine-style render of the frame's OWN assembled chain (the caller's
     * lets already folded, the mapping attached — the same chain the frame
     * runs; the same pipeline as toSQLString(query, mapping, H2, ext)).
     * Null when the call's mapping is not a literal reference or the chain
     * does not render (the frame's own walls stand). */
    static @com.legend.base.Nullable String activitySql(
            com.legend.compiler.spec.typed.TypedNativeCall ec,
            com.legend.compiler.spec.typed.TypedSpec chain,
            java.util.List<com.legend.compiler.spec.typed.TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs, StatementExecutor.ExecEnv env) {
        if (ec.args().size() < 2) {
            return null;
        }
        com.legend.compiler.spec.typed.TypedSpec m = com.legend.compiler.spec.typed.Lets.bound(ec.args().get(1), letPrefix);
        String mappingFqn = m instanceof com.legend.compiler.spec.typed.TypedPackageableRef pr
                ? pr.fullPath() : chainMapping(chain);
        if (mappingFqn == null) {
            return null;
        }
        try {
            var renderer = new com.legend.sql.dialect.EngineStyleH2();
            StatementExecutor.EngineSql es = StatementExecutor.engineSql(
                    java.util.List.of(chain), mappingFqn, specs, env, renderer,
                    java.util.Map.of(), java.util.function.UnaryOperator.identity());
            com.legend.sql.SqlQuery post = com.legend.lowering.SqlPostProcessors.applyRecorded(
                    es.plan(), env.tableReplace(),
                    env.postProcessors().extractCtes(),
                    env.postProcessors().nonExecutable());
            return post == es.plan() ? es.sql() : renderer.render(post);
        } catch (com.legend.error.NotImplementedException
                | com.legend.compiler.spec.TypeInferenceException
                | com.legend.error.MappingResolutionException
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
    private static @com.legend.base.Nullable String chainMapping(com.legend.compiler.spec.typed.TypedSpec chain) {
        com.legend.compiler.spec.typed.TypedSpec cur = chain;
        while (cur != null) {
            if (cur instanceof com.legend.compiler.spec.typed.TypedFrom f
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
    static void registerActivityRows(com.legend.compiler.spec.typed.TypedNativeCall ec,
            @com.legend.base.Nullable String sql, @com.legend.base.Nullable String rewrittenQuery,
            @com.legend.base.Nullable String comment,
            StatementExecutor.ExecEnv env) {
        String scope = com.legend.plan.PlanRows.scopeId(ec);
        if (sql == null || env.planRows().containsKey(scope)) {
            return;
        }
        java.util.Map<String, java.util.List<java.util.List<String>>> rows =
                new java.util.LinkedHashMap<>();
        rows.put("results", java.util.List.of(java.util.List.of(scope)));
        java.util.List<java.util.List<String>> acts = new java.util.ArrayList<>();
        if (rewrittenQuery != null) {
            // the engine records the aggregation-aware ROUTING activity
            // first (it happens at routing, before execution): the routed
            // query print names the set the router chose
            acts.add(java.util.List.of(scope + "/0", scope, "0",
                    "AggregationAwareActivity", "", "", rewrittenQuery));
        }
        int k = acts.size();
        acts.add(java.util.List.of(scope + "/" + k, scope, Integer.toString(k),
                "RelationalActivity", sql, comment == null ? "" : comment, ""));
        rows.put("activities", acts);
        env.planRows().put(scope, rows);
    }

    /** Every HANDLE native call inside a let's binding registers its rows
     * (the handle may sit under ->toOne(), ->removeDuplicates(), a cast —
     * the walk is shape-free). */
    static void registerHandlesIn(String letName,
            com.legend.compiler.spec.typed.TypedSpec rhs,
            java.util.List<com.legend.compiler.spec.typed.TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs,
            StatementExecutor.ExecEnv env) {
        if (rhs instanceof com.legend.compiler.spec.typed.TypedNativeCall pn
                && com.legend.compiler.element.type.PlatformTypes.handleRowClass(
                        pn.callee().qualifiedName(), pn.callee().returnType()) != null) {
            registerHandleRows(letName, pn, letPrefix, specs, env);
        }
        for (com.legend.compiler.spec.typed.TypedSpec c : rhs.children()) {
            registerHandlesIn(letName, c, letPrefix, specs, env);
        }
    }

    /** A HANDLE native bound by a let (executionPlan, scanRelations): its
     * rows register under the handle's scope (PlanRows / LineageRows). */
    static void registerHandleRows(String letName,
            com.legend.compiler.spec.typed.TypedNativeCall pn,
            java.util.List<com.legend.compiler.spec.typed.TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs,
            StatementExecutor.ExecEnv env) {
        String fqn = pn.callee().qualifiedName();
        if (com.legend.compiler.element.type.PlatformTypes.EXECUTION_PLAN.equals(fqn)) {
            registerPlanRows(pn, letPrefix, specs, env);
        } else if (com.legend.compiler.element.type.PlatformTypes.SCAN_RELATIONS
                .equals(fqn)) {
            registerLineageRows(letName, pn, letPrefix, env);
        } else if (com.legend.compiler.element.type.PlatformTypes.SCAN_COLUMNS
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
            com.legend.compiler.spec.typed.TypedNativeCall pn,
            java.util.List<com.legend.compiler.spec.typed.TypedSpec> letPrefix,
            StatementExecutor.ExecEnv env) {
        String scope = com.legend.plan.PlanRows.scopeId(pn);
        if (env.planRows().containsKey(scope)) {
            return;
        }
        java.util.Map<String, com.legend.protocol.spec.ValueSpecification> lets =
                new java.util.LinkedHashMap<>();
        com.legend.protocol.spec.ValueSpecification scan = null;
        for (com.legend.protocol.spec.ValueSpecification st : env.protocolBody()) {
            com.legend.protocol.spec.CString ln =
                    com.legend.compiler.spec.SourceSubst.letName(st);
            if (ln == null) {
                continue;
            }
            com.legend.protocol.spec.ValueSpecification v =
                    ((com.legend.protocol.spec.AppliedFunction) st).parameters().get(1);
            if (ln.value().equals(letName)) {
                scan = v;
                break;
            }
            lets.put(ln.value(), v);
        }
        // scanColumns(tree, m) under removeDuplicates → tree → buildPropertyTree(
        // $p.result) → $p → scanProperties(vs, …) → vs → lambda
        com.legend.protocol.spec.ValueSpecification cur = scan;
        com.legend.protocol.spec.LambdaFunction ql = null;
        for (int guard = 0; guard < 16 && cur != null && ql == null; guard++) {
            switch (cur) {
                case com.legend.protocol.spec.LambdaFunction lf -> ql = lf;
                case com.legend.protocol.spec.Variable v ->
                        cur = lets.get(v.name());
                case com.legend.protocol.spec.AppliedProperty ap -> cur = ap.receiver();
                case com.legend.protocol.spec.AppliedFunction af ->
                        cur = af.parameters().isEmpty() ? null : af.parameters().get(0);
                default -> cur = null;
            }
        }
        com.legend.compiler.spec.typed.TypedSpec m =
                com.legend.compiler.spec.typed.Lets.bound(
                        pn.args().get(1), letPrefix);
        String runtimeFqn = env.runtimeFqn();
        if (ql == null || runtimeFqn == null
                || !(m instanceof com.legend.compiler.spec.typed.TypedPackageableRef mr)) {
            return;
        }
        try {
            com.legend.sql.SqlQuery plan = Compiler.lowerResolved(ql, env.ctx(),
                    runtimeFqn, false, mr.fullPath());
            var rows = com.legend.lineage.ColumnLineageRows.rows(scope, env.ctx(),
                    mr.fullPath(), com.legend.lineage.ScanColumns.entries(plan));
            env.planRows().put(scope, rows);
        } catch (com.legend.error.NotImplementedException
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
            com.legend.compiler.spec.typed.TypedNativeCall pn,
            java.util.List<com.legend.compiler.spec.typed.TypedSpec> letPrefix,
            StatementExecutor.ExecEnv env) {
        String scope = com.legend.plan.PlanRows.scopeId(pn);
        if (env.planRows().containsKey(scope)) {
            return;
        }
        java.util.Map<String, com.legend.protocol.spec.ValueSpecification> lets =
                new java.util.LinkedHashMap<>();
        com.legend.protocol.spec.AppliedFunction scan = null;
        for (com.legend.protocol.spec.ValueSpecification st : env.protocolBody()) {
            com.legend.protocol.spec.CString ln =
                    com.legend.compiler.spec.SourceSubst.letName(st);
            if (ln == null) {
                continue;
            }
            com.legend.protocol.spec.ValueSpecification v =
                    ((com.legend.protocol.spec.AppliedFunction) st).parameters().get(1);
            if (ln.value().equals(letName)
                    && v instanceof com.legend.protocol.spec.AppliedFunction af) {
                scan = af;
                break;
            }
            lets.put(ln.value(), v);
        }
        // a ->toOne() over the scan is the same scan
        while (scan != null && scan.parameters().size() == 1
                && com.legend.compiler.ResolvedNames.referents(scan).stream().anyMatch(com.legend.builtin.Pure::isToOneCall)
                && scan.parameters().get(0)
                        instanceof com.legend.protocol.spec.AppliedFunction inner) {
            scan = inner;
        }
        if (scan == null || scan.parameters().isEmpty()) {
            return;
        }
        com.legend.protocol.spec.ValueSpecification q = scan.parameters().get(0);
        while (q instanceof com.legend.protocol.spec.Variable qv
                && lets.containsKey(qv.name())) {
            q = lets.get(qv.name());
        }
        if (!(q instanceof com.legend.protocol.spec.LambdaFunction ql)) {
            return;
        }
        com.legend.compiler.spec.typed.TypedSpec m =
                com.legend.compiler.spec.typed.Lets.bound(
                        pn.args().get(1), letPrefix);
        if (!(m instanceof com.legend.compiler.spec.typed.TypedPackageableRef mr)) {
            return;
        }
        try {
            var lines = com.legend.lineage.ScanRelations.lines(env.ctx(), ql,
                    mr.fullPath(), pn.args().size() == 4);
            var rows = com.legend.lineage.LineageRows.rows(scope, lines);
            env.planRows().put(scope, rows);
        } catch (com.legend.error.NotImplementedException
                | IllegalStateException e) {
            // no rows: the symbolic handle's walls stand
        }
    }
}
