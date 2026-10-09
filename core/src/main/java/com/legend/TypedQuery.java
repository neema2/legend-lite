// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.List;

/**
 * A query typed ONCE against a compiled model ({@link Compiler#query}; C2b,
 * docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md): its result type, the target it names, its typed
 * expression, its lowering and its plan all come off this one object. Each replaces a {@code Compiler} static that took
 * the model as TEXT and compiled it again per question.
 */
public final class TypedQuery {

    private final ModelContext ctx;
    private final com.legend.protocol.spec.ValueSpecification resolved;
    private final SpecCompiler specs;
    private @com.legend.base.Nullable List<TypedSpec> body;

    TypedQuery(ModelContext ctx, com.legend.protocol.spec.ValueSpecification resolved) {
        this.ctx = ctx;
        this.resolved = resolved;
        this.specs = new SpecCompiler(ctx);
    }

    /** The compiled model this query is typed against. */
    public ModelContext context() {
        return ctx;
    }

    /** The query's statements typed as a query body (Phase G), once. */
    public List<TypedSpec> body() {
        List<TypedSpec> b = body;
        if (b == null) {
            b = List.copyOf(specs.typeQueryBody(resolved));
            body = b;
        }
        return b;
    }

    /**
     * The TYPE of the query's result, compile-only — upstream {@code pure/v1/compilation/lambdaRelationType}'s fact
     * (U2): the last statement's type. No runtime, no store resolution, no lowering.
     */
    public com.legend.compiler.element.type.ExprType resultType() {
        List<TypedSpec> b = body();
        return b.get(b.size() - 1).info();
    }

    /** The query's declared parameters ({@code {minQty: Integer[1]|...}}), in order, at the types its body is typed
     *  with; none for a query that declares none. A parameter must declare its type: a query lambda with an untyped
     *  parameter is typed as a lambda VALUE ({@link SpecCompiler#typeQueryBody}), not as a query over parameters, and is
     *  refused here by name. */
    public List<QueryParameters.Declared> parameters() {
        if (!(resolved instanceof com.legend.protocol.spec.LambdaFunction lf)) {
            return List.of();
        }
        List<QueryParameters.Declared> out = new java.util.ArrayList<>(lf.parameters().size());
        for (com.legend.protocol.spec.Variable p : lf.parameters()) {
            if (p.type() == null) {
                throw new IllegalArgumentException("query parameter '" + p.name()
                        + "' declares no type: a query's parameters are typed ({" + p.name() + ": String[1]|...})");
            }
            com.legend.compiler.element.type.ExprType t = specs.declaredParameterType(p);
            out.add(new QueryParameters.Declared(p.name(), t.type(), t.multiplicity()));
        }
        return List.copyOf(out);
    }

    /** The query typed as ONE expression (Phase G): its typed HIR, the front half only. */
    public TypedSpec expression() {
        return specs.typeExpression(resolved);
    }

    /** The {@link Compiler.Target} the query names: the runtime its {@code ->from(runtime)} binds and the database its
     *  {@code #>{db...}#} accessor reads, off the typed tree by node type. */
    public Compiler.Target target() {
        String runtime = null;
        String store = null;
        java.util.ArrayDeque<TypedSpec> work = new java.util.ArrayDeque<>(body());
        while (!work.isEmpty()) {
            TypedSpec n = work.poll();
            if (runtime == null && n instanceof com.legend.compiler.spec.typed.TypedFrom f
                    && f.runtime().isPresent()) {
                runtime = f.runtime().get().fullPath();
            }
            if (store == null
                    && n instanceof com.legend.compiler.spec.typed.TypedTableReference r) {
                store = r.store();
            }
            work.addAll(n.children());
        }
        return new Compiler.Target(runtime, store);
    }

    /** The query lowered for {@code runtime}: inlined (G½), resolved (H) against the runtime, every touched store
     *  checked against its connections, lowered (I) — every planning step, no database. {@code streaming}: the
     *  streaming graph root (one json_object per row). */
    public Compiler.LoweredQuery lower(@com.legend.base.Nullable String runtime, boolean streaming) {
        return lower(runtime, streaming, List.of());
    }

    /** {@link #lower(String, boolean)} with the query's declared parameters as {@code slots}: each {@code $name} of a
     *  slot lowers to it, a value the statement binds (a plan's parameters, docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md
     *  §9, step 2's landing 2). */
    Compiler.LoweredQuery lower(@com.legend.base.Nullable String runtime, boolean streaming,
            List<com.legend.sql.SqlExpr.PlanParam> slots) {
        List<TypedSpec> b = new com.legend.compiler.spec.UserCallInliner(specs).inlineBody(
                new java.util.ArrayList<>(body()));   // Phase G½
        boolean temporalRoot = com.legend.compiler.element.Temporal.anyTemporalGetAll(b, ctx);
        b = new com.legend.resolver.StoreResolver(ctx, specs).resolve(b, runtime);   // Phase H
        // the SQL is for this runtime's session: the runtime decided first (no runtime is refused there, by
        // name), then every store the query touches must be bound to it (C3b)
        Compiler.executesOn(ctx, runtime);
        CrossStoreGuard.check(b, ctx, runtime);
        TypedSpec root = b.get(b.size() - 1);
        com.legend.lowering.Lowerer planLw = new com.legend.lowering.Lowerer(
                t -> com.legend.compiler.element.ClassLayouts.layoutOf(ctx, t),
                f -> ctx.findClass(f).isPresent(), ctx.implementations());
        if (!temporalRoot) {
            planLw = planLw.withEngineExistsJoinForm();
        }
        if (streaming) {
            planLw = planLw.withStreamingGraphRoot();
        }
        for (com.legend.sql.SqlExpr.PlanParam slot : slots) {
            planLw.bindPlanParam(slot);
        }
        return new Compiler.LoweredQuery(planLw.lower(b), root, ctx);
    }

    /** The query planned for {@code runtime}: the SQL in the runtime's dialect, the root's type and its result shape —
     *  what the executor would consume, minus execution. */
    public com.legend.plan.QueryPlan plan(@com.legend.base.Nullable String runtime) {
        return plan(runtime, false);
    }

    /** {@link #plan} with the STREAMING graph root: one json_object per row, so a streaming executor stays O(one row). */
    public com.legend.plan.QueryPlan planStreaming(@com.legend.base.Nullable String runtime) {
        return plan(runtime, true);
    }

    /** The text a caller asks a plan for: CSV, JSON, or JSON whose rows stream as the database answers them (a
     *  relation's rows, a graph fetch's objects; a value's text is whole). */
    public enum Output { CSV, JSON, STREAMED_JSON }

    /** The query's execution plan for {@code runtime} (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9): the text the
     *  database writes in the form {@code output} asks for, final for its target, run by a model-free runner. */
    public com.legend.executionplan.ExecutionPlan executionPlan(@com.legend.base.Nullable String runtime,
            Output output) {
        return PlanMaker.plan(this, runtime, output);
    }

    private com.legend.plan.QueryPlan plan(@com.legend.base.Nullable String runtime, boolean streaming) {
        Compiler.LoweredQuery l = lower(runtime, streaming);
        String sql = Compiler.dialectOf(ctx, runtime).render(l.plan());
        return new com.legend.plan.QueryPlan(sql, l.root().info(), com.legend.plan.ResultShape.of(l.root()));
    }
}
