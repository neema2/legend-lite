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
import com.legend.plan.PlanRows;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;

/** The Allocation plan-node factory for let bindings (extracted from
 * BodyDriver at the file guardrail; the planner's since the seam
 * program's S2g, 2026-10-31: the handles' and activities' rows are
 * planning facts, written with no session). */
public final class PlanAllocations {

    private PlanAllocations() {
    }

    /** The execution context's time zone of an {@code executionPlan} call (its third argument), or null: a fact the
     *  plan's rows are built from (legend-engine's support functions, the edge's). */
    private static @Nullable String planTimeZone(TypedNativeCall pep, List<TypedSpec> letPrefix) {
        return pep.args().size() > 2
                ? ExecutionContext.reader().bind(v -> Lets.bound(v, letPrefix))
                        .read(Optional.empty(), pep.args().get(2)).timeZone()
                : null;
    }

    /** The query's mapping of an {@code executionPlan} call (its second argument, a bare reference), or null. */
    private static @Nullable String planMapping(TypedNativeCall pep) {
        return pep.args().get(0) instanceof TypedLambda && pep.args().size() > 1
                && pep.args().get(1) instanceof TypedPackageableRef pmr ? pmr.fullPath() : null;
    }

    /** The enumerations of the query's enum-typed parameters, in declaration order (none without a mapping). */
    private static List<String> planEnumParameters(TypedNativeCall pep) {
        List<String> out = new ArrayList<>();
        if (planMapping(pep) != null && pep.args().get(0) instanceof TypedLambda plam) {
            for (QueryParameters.Declared prm : QueryParameters.of(plam)) {
                if (prm.type() instanceof Type.EnumType et) {
                    out.add(et.fqn());
                }
            }
        }
        return out;
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
        // the style asked BEFORE the walk's refusals are caught (J16/K4 of the edge audit, 2026-11-17, G7): the product's
        // style refuses legend-engine's plan by name, and that refusal fails the body -- it is never "no rows"
        UpstreamPlans upstream = env.style().facet(UpstreamPlans.class);
        try {
            var rows = upstream.planRows(scope, pnBound,
                    planTimeZone(pnBound, letPrefix), planMapping(pnBound), planEnumParameters(pnBound), specs, env);
            env.planRows().put(scope, rows);
            env.planRows().put(PlanRows.scopeId(pnBound), rows);
        } catch (NotImplementedException | com.legend.sql.DialectCapability
                | com.legend.lowering.Lowerer.IsolationWall e) {
            // the plan model's NAMED refusals alone (C5-J item 4, 2026-11-15, G7: until then any IllegalStateException
            // and TypeInferenceException as well): a shape the plan walk does not build yet, a reduction the dialect
            // cannot encode, and a reference the lowering cannot resolve even after isolation (the census of core's
            // suites, PCT's channels and both corpora: 44, 12 and 84 of the 140 it swallowed; the orchestrator's
            // ruling (A): the walls named, Lowerer.IsolationWall). Anything else fails the body by name
            if (!noLineageRows(e)) {
                throw e;
            }
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
        // S310 (RULED by the user 2026-11-14, DE2 (a)): the style picks the activity's SQL; the one rule for an
        // activity's SQL or none (C5-J item 4 and the C5-J audit's nit, 2026-11-15; merged 2026-11-15, #106 into #104)
        return activityOrNone(() -> activitySql(List.of(chain), mappingFqn, specs, env, true));
    }

    /**
     * An activity's SQL, or none: the ONE rule for both activities, an {@code execute()}'s ({@link #activitySql}) and
     * an {@code executeLegendQuery}'s ({@code FramePlanner}) -- the C5-J audit's nit, 2026-11-15: one condition, one
     * handling. The one condition an activity means (C5-J item 4, G7: until then any NotImplementedException,
     * TypeInferenceException, MappingResolutionException, IllegalStateException or LegendCompileException): the
     * engine-style render reaches a reduction its dialect cannot encode, so the activity records no SQL, and a read of
     * it is a loud missing member. Anything else fails the body by name.
     */
    static @Nullable String activityOrNone(java.util.function.Supplier<String> render) {
        try {
            return render.get();
        } catch (com.legend.sql.DialectCapability e) {
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

    /** An execute's activity SQL (S310, RULED by the user 2026-11-14, DE2 (a)): the style's form of
     *  {@link ActivityTexts} -- the product's lite's own SQL, what runs; the edge's legend-engine's text for its test
     *  connection's database, {@code recorded} its runtime's post-processors applied (J40.PlanStyle, 2026-11-18: each
     *  form computes its own text alone). */
    static String activitySql(List<TypedSpec> chain, @Nullable String mappingFqn, SpecCompiler specs, PlanEnv env,
            boolean recorded) {
        return env.style().twoForm(ActivityTexts.class, LITE_ACTIVITY).activitySql(chain, mappingFqn, specs, env,
                recorded);
    }

    /** The product's form of {@link ActivityTexts}: lite's own SQL for the chain ({@link #activityLiteSql}). */
    static final ActivityTexts LITE_ACTIVITY = (chain, mappingFqn, specs, env, recorded) ->
            activityLiteSql(chain, mappingFqn, specs, env);

    /** LITE'S OWN SQL for an execute's chain (S310, RULED by the user 2026-11-14, DE2 (a): the product's activity
     *  record reports what actually runs): the chain inlined, resolved and lowered as a product statement is
     *  ({@link BodyPlanner#lowerAndPrepare}), written in the dialect of the database the statement runs on -- the
     *  planning's own where it chose one, else the style's for the runtime's database. */
    static String activityLiteSql(List<TypedSpec> raw, @Nullable String mappingFqn, SpecCompiler specs,
            PlanEnv env) {
        com.legend.sql.dialect.SqlDialect dialect = env.dialect() != null ? env.dialect()
                : env.style().dialect(com.legend.Compiler.executesOn(env.ctx(), env.runtimeFqn()).type());
        List<TypedSpec> body = new com.legend.compiler.spec.UserCallInliner(specs).inlineBody(raw);
        body = BodyPlanner.resolver(specs, env, false).resolve(body, env.runtimeFqn(), mappingFqn, List.of());
        return dialect.render(BodyPlanner.lowerAndPrepare(body, env, env.ctx(), dialect, false));
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
                        env.style().twoForm(RoutedQueries.class, NO_ROUTING_ACTIVITY).routingActivity(chain, env.ctx(), specs),
                        comment, commentParameter)));
    }

    /** The product's form of an execute's routing activity (J5 of the edge audit, 2026-11-17): none -- lite's activity
     *  rows are the statements it runs; legend-engine's AggregationAwareActivity, the routed query's print, is the
     *  compatibility edge's form ({@link RoutedQueries}). */
    static final RoutedQueries NO_ROUTING_ACTIVITY = (chain, ctx, specs) -> null;

    /** The activity row's comment column (results scope, activity id, scope, index, kind, sql, comment, rewritten). */
    private static final int COMMENT = 5;

    private static LazyRows.@Nullable Computed activityRows(String scope,
            @Nullable String sql, @Nullable List<String> routing,
            @Nullable String comment, @Nullable String commentParameter) {
        if (sql == null) {
            return null;
        }
        Map<String, List<List<String>>> rows =
                new LinkedHashMap<>();
        rows.put("results", List.of(List.of(scope)));
        List<List<String>> acts = new ArrayList<>();
        if (routing != null) {
            // a routing activity first (it happens at routing, before execution), the style's whole row (J37 of the edge
            // audit, 2026-11-18): the compatibility edge's legend-engine AggregationAwareActivity alone
            if (routing.size() != 4) {
                throw new IllegalStateException("a routing activity is its kind, sql, comment and rewritten query: "
                        + routing);
            }
            List<String> row = new ArrayList<>(List.of(scope + "/0", scope, "0"));
            row.addAll(routing);
            acts.add(List.copyOf(row));
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

    /** Whether a lineage or plan scan's failure leaves the handle with no rows (its walls stand): every one the
     *  lowering raises but the raw-SQL refusal, which propagates by name (the final audit of 9af16fbe58's J7,
     *  2026-11-14: the catches swallowed Compiler.lowerResolved's refusal, answering "no rows"). */
    static boolean noLineageRows(RuntimeException e) {
        return !QueryDoor.isRawSqlRefusal(e);
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
                    runtimeFqn, mr.fullPath());
            var rows = ColumnLineageRows.rows(scope, env.ctx(),
                    mr.fullPath(), ScanColumns.entries(plan));
            env.planRows().put(scope, rows);
        } catch (NotImplementedException e) {
            // the lowering's named refusal alone (C5-J item 4, 2026-11-15, G7: until then any IllegalStateException as
            // well): a query the column scan's lowering does not plan yet (a class-typed property used whole is graph
            // output). Anything else fails the body by name
            if (!noLineageRows(e)) {
                throw e;
            }
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
        // a scan that fails, fails the body by name (C5-J item 4, 2026-11-15, G7: until then a NotImplementedException
        // or an IllegalStateException registered nothing, and the handle's reads met its walls instead; the census of
        // core's suites, PCT's channels and both corpora found none)
        var lines = ScanRelations.lines(env.ctx(), ql,
                mr.fullPath(), pn.args().size() == 4);
        var rows = LineageRows.rows(scope, lines);
        env.planRows().put(scope, rows);
    }
}
