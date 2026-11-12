// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.planner.CrossStoreGuard;
import com.legend.planner.PlanMaker;
import com.legend.planner.QueryParameters;

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

    /** This query with {@code body} as its typed statements (a body the test door rewrote statement for statement, the
     *  places kept: {@code LetPlanner.unreadAsStatements}); the model, the resolved query and the spec compiler shared.
     *  An internal of the execution door's, public for the driver's tests as {@link #specs} is. */
    public TypedQuery withBody(List<TypedSpec> body) {
        TypedQuery q = new TypedQuery(ctx, resolved, specs);
        q.body = List.copyOf(body);
        return q;
    }

    private TypedQuery(ModelContext ctx, com.legend.protocol.spec.ValueSpecification resolved, SpecCompiler specs) {
        this.ctx = ctx;
        this.resolved = resolved;
        this.specs = specs;
    }

    /** The compiled model this query is typed against. */
    public ModelContext context() {
        return ctx;
    }

    /** The spec compiler the query's statements are typed by (a user function's body is compiled through it). */
    /** The spec compiler the query was typed by. This and the members below (resolved, lower,
     *  lowerer, writes) are the planner's and the driver's internals, public since step C (D73, 2026-10-30) because
     *  the machinery left this package; the product API of a typed query is plan, lower(runtime), resultType,
     *  expression, target and parameters. */
    public SpecCompiler specs() {
        return specs;
    }

    /** The name-resolved query the body is typed from (the body driver reads a parameterless lambda's protocol
     *  statements off it: the source-shaped lets a handle's rows are built from; track B's B2, 2026-10-29). */
    public com.legend.protocol.spec.ValueSpecification resolved() {
        return resolved;
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

    /**
     * Whether the query answers a diagnostic's call ({@code print}, {@code println}): a value the program wrote that
     * legend-engine refuses when the lambda is built (register S128), where a String or Boolean literal or a model's DDL
     * text is a value it folds (generatePlan answers upstream's ConstantExecutionNode for those: M5, the user's ruling of
     * 2026-11-11). Read off the last statement's typed root, as the planner's statement road reads it
     * ({@code BodyPlanner}'s effect dispatch).
     */
    public boolean answersADiagnostic() {
        List<TypedSpec> b = body();
        TypedSpec root = b.get(b.size() - 1);
        while (root instanceof com.legend.compiler.spec.typed.TypedFrom fr) {
            root = fr.source();
        }
        return root instanceof com.legend.compiler.spec.typed.TypedNativeCall nc
                && com.legend.builtin.NativeFn.Effect.isInertDiagnostic(nc.callee().id());
    }

    /** The query typed as ONE expression (Phase G): its typed HIR, the front half only. */
    public TypedSpec expression() {
        return specs.typeExpression(resolved);
    }

    /** The {@link Compiler.Target} the query names: the database its {@code #>{db...}#} accessor reads, off the typed
     *  tree by node type. Its runtime is {@link #runtime}'s, one rule for every door. */
    public Compiler.Target target() {
        String store = null;
        java.util.ArrayDeque<TypedSpec> work = new java.util.ArrayDeque<>(body());
        while (!work.isEmpty()) {
            TypedSpec n = work.poll();
            if (store == null
                    && n instanceof com.legend.compiler.spec.typed.TypedTableReference r) {
                store = r.store();
            }
            work.addAll(n.children());
        }
        return new Compiler.Target(store);
    }

    /** The runtime the query runs on: {@code given}, else the one its {@code ->from}s name, a from() inside a function
     *  it calls included (read after G½); a query naming several and given none is refused by name; null where it
     *  names none. Every door's one rule ({@code PlanMaker.runtimeOf}; the checkpoint tenets audit's 1, 2026-11-10). */
    public @com.legend.base.Nullable String runtime(@com.legend.base.Nullable String given) {
        return com.legend.planner.PlanMaker.runtimeOf(this, given);
    }

    /** The query lowered for {@code runtime}: inlined (G½), resolved (H) against the runtime, every touched store
     *  checked against its connections, lowered (I) — every planning step, no database. {@code streaming}: the
     *  streaming graph root (one json_object per row). */
    public Compiler.LoweredQuery lower(@com.legend.base.Nullable String runtime, boolean streaming) {
        return lower(runtime, streaming, List.of(), java.util.Set.of());
    }

    /** {@link #lower(String, boolean)} with the query's declared parameters as {@code slots}: each {@code $name} of a
     *  slot lowers to it, a value the statement binds (a plan's parameters, docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md
     *  §9, step 2's landing 2). */
    public Compiler.LoweredQuery lower(@com.legend.base.Nullable String runtime, boolean streaming,
            List<com.legend.sql.SqlExpr.PlanParam> slots, java.util.Set<com.legend.platform.Feature> features) {
        // the product's statement road (the test-is-the-product plan's M2, B11): the test road's front half, the
        // product's lowering
        com.legend.planner.ProductStatements.Statement s = com.legend.planner.ProductStatements.prepare(this,
                body(), runtime, null, features);
        com.legend.planner.Prepared.Planned p = com.legend.planner.ProductStatements.planned(s);
        com.legend.sql.SqlQuery plan = com.legend.planner.ProductStatements.lower(this, p, s.temporal(), streaming,
                slots, new com.legend.lowering.ObjectSites());
        return new Compiler.LoweredQuery(plan, p.body().get(p.body().size() - 1), ctx);
    }

    /** The Lowerer a planned statement lowers with (the wire path's): the model's object layouts (every object its
     *  identity and its class, docs/OBJECT_IDENTITY_2026_10_20.md §3.1; D91: equal() over keyed instances is the key
     *  relation on every lane, the plan's too), the engine's exists-join form unless a temporal class is read
     *  ({@code temporal}), the streaming graph root if asked, the query's feature flags, each slot a value the
     *  statement binds. */
    public com.legend.lowering.Lowerer lowerer(boolean temporal, boolean streaming,
            List<com.legend.sql.SqlExpr.PlanParam> slots, java.util.Set<com.legend.platform.Feature> features) {
        return lowerer(temporal, streaming, slots, features, new com.legend.lowering.ObjectSites());
    }

    /** {@link #lowerer(boolean, boolean, List, java.util.Set)} naming its objects' sites with {@code sites}: the
     *  program's one minter, shared by every statement of a program (the test-is-the-product plan's B9, 2026-10-31). */
    public com.legend.lowering.Lowerer lowerer(boolean temporal, boolean streaming,
            List<com.legend.sql.SqlExpr.PlanParam> slots, java.util.Set<com.legend.platform.Feature> features,
            com.legend.lowering.ObjectSites sites) {
        com.legend.lowering.Lowerer planLw = new com.legend.lowering.Lowerer(
                t -> com.legend.compiler.element.ClassLayouts.objectLayoutOf(ctx, t),
                f -> ctx.findClass(f).isPresent(), ctx.implementations())
                .withInstanceKeys(t -> com.legend.compiler.element.EqualityKeys.resolve(ctx, t))
                .withObjectIdentity(sites::idOf);
        if (!temporal) {
            planLw = planLw.withEngineExistsJoinForm();
        }
        if (streaming) {
            planLw = planLw.withStreamingGraphRoot();
        }
        if (!features.isEmpty()) {
            planLw = planLw.withFeatures(features);
        }
        for (com.legend.sql.SqlExpr.PlanParam slot : slots) {
            planLw.bindPlanParam(slot);
        }
        return planLw;
    }

    /** Whether the query is a program that changes a store (docs/WRITES_DESIGN_2026_10_18.md §8, leg 7): a statement
     *  writes a store table ({@code StatementEffects.writesStore}), or a let stages rows for the writes after it. */
    public boolean writes() {
        java.util.Map<com.legend.model.FunctionId, Boolean> memo = new java.util.HashMap<>();
        for (TypedSpec s : body()) {
            if (com.legend.compiler.spec.StatementEffects.writesStore(s, specs, memo)
                    || com.legend.resolver.StagedRows.rowsOf(s) != null) {
                return true;
            }
        }
        return false;
    }

    /**
     * The query planned for {@code runtime} as a TEXT plan: its execution plan's one answering statement -- its text,
     * the root's type and its result shape -- with the statement's target (its session statements and the LOADS that
     * establish it: test data, a project's Data, a {@code data:} source's session table) and its slots (a {@code file:}
     * source's path the run binds) carried beside it (B3, docs/TOTAL_WALL_START_HERE_2026_11_11.md §5.2: "the text plan
     * renders from the execution plan"; until then it lowered the query again and refused a live source by name). A
     * query whose plan answers through no one statement (a program that writes, a pivot whose columns the data names, a
     * value the program wrote) is refused by name: its text would not be what runs.
     */
    public com.legend.plan.QueryPlan plan(@com.legend.base.Nullable String runtime) {
        com.legend.executionplan.ExecutionPlan plan = executionPlan(runtime);
        if (!plan.answers() || !plan.sendsTheAnswer() || plan.root() instanceof com.legend.executionplan.ExecutionPlan.Sequence) {
            throw new com.legend.error.NotImplementedException("a text plan is the statement that answers, and this"
                    + " query's execution plan " + (!plan.answers() ? "answers nothing"
                    : !plan.sendsTheAnswer() ? "answers a value the program wrote, sending no statement"
                    : "is a program of steps (a write)") + ": run the execution plan");
        }
        com.legend.compiler.element.type.ExprType root = resultType();
        com.legend.plan.ResultShape shape = switch (plan.answerShape()) {
            case OBJECTS -> com.legend.plan.ResultShape.GRAPH;
            case RELATION -> com.legend.plan.ResultShape.TABULAR;
            // a value's elements: one or many by the root's multiplicity
            case VALUE -> root.multiplicity().requireBounded("a text plan").isMany()
                    ? com.legend.plan.ResultShape.COLLECTION : com.legend.plan.ResultShape.SCALAR;
        };
        return new com.legend.plan.QueryPlan(plan, root, shape);
    }

    /** The query's TYPED execution plan for {@code runtime} (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md §5, step 1):
     *  every statement final for its target, its answer typed rows read by the plan's column facts (a relation's rows,
     *  a value's elements, a graph fetch's objects), no output format in it -- a writer at the edge spells them. */
    public com.legend.executionplan.ExecutionPlan executionPlan(@com.legend.base.Nullable String runtime) {
        return executionPlan(runtime, java.util.Set.of());
    }

    /** {@link #executionPlan(String)} under the engine's execution feature flags ({@code features}: a lowering
     *  concern, {@code Lowerer.withFeatures}; a flag nothing consumes is refused by name). The one planning entry of a
     *  product query (slice 5, 2026-11-09: the door's and pure/v1's own copies of these lines are gone): planned under
     *  the wall's planning scope (THE WALL's rule 2: no session opens and no run starts while it is planned), and, where
     *  the test lanes ask ({@code -Dlegend.plan.roundtrip=true}, every junit lane: tools/junit/defs.bzl), refused if
     *  it does not survive its JSON (G11) -- a product query pays nothing for the check. */
    public com.legend.executionplan.ExecutionPlan executionPlan(@com.legend.base.Nullable String runtime,
            java.util.Set<com.legend.platform.Feature> features) {
        return planned(runtime, features, false);
    }

    /** The query planned as the server runs it (legend-engine's {@code pure/v1/execution/execute}): its target names
     *  the project's own database, made from the model's stores and filled with its Data rows where it is made
     *  ({@code ExecutionPlan.Project}). Every other road plans {@link #executionPlan}: the database itself (the full
     *  audit, 2026-11-09). */
    public com.legend.executionplan.ExecutionPlan projectPlan(@com.legend.base.Nullable String runtime) {
        return planned(runtime, java.util.Set.of(), true);
    }

    private com.legend.executionplan.ExecutionPlan planned(@com.legend.base.Nullable String runtime,
            java.util.Set<com.legend.platform.Feature> features, boolean projectDatabase) {
        try (var planning = com.legend.base.RunScope.plans("a query")) {
            com.legend.executionplan.ExecutionPlan plan = PlanMaker.plan(this, runtime, features, projectDatabase);
            com.legend.planner.PlanningObserver seen = com.legend.planner.PlanningObserver.installed();
            if (seen != null) {
                seen.planned(this, runtime);   // a test harness's observer (E-1): none in the product
            }
            if (Boolean.getBoolean("legend.plan.roundtrip")) {
                com.legend.executionplan.PlanJson.requireRoundTrip(plan);
            }
            return plan;
        }
    }

}
