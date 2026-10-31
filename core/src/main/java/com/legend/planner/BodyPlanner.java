// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;

/**
 * THE BODY PLANNER (the planner-driver seam program's S2, docs/PLANNER_DRIVER_SEAM_PROGRAM_2026_10_30.md §3.3), grown
 * kind by kind as a strangler: what the body driver asks to have a statement planned, over the body's planning half
 * ({@link PlanEnv}) and never a session. S2g's first move (2026-10-31): the statement resolver every site constructs
 * ({@link #resolver}: the query's lets and the handles' rows ride every resolution), the Lowerer a statement lowers
 * with and the plan-preparation ladder ({@link #lowererFor}, {@link #lowerAndPrepare}), the run's feature flags, an
 * execute call's bound context, and a value's road to its executable body (the lets spliced, the user calls inlined,
 * the natives staged, the stores resolved and checked: {@link #sideBody}). The driver keeps what runs.
 */
public final class BodyPlanner {

    private BodyPlanner() {
    }

    /** THE statement resolver: the query lets and the registered handle
     * rows ride every resolution (one construction, every site). */
    public static com.legend.resolver.StoreResolver resolver(
            com.legend.compiler.spec.SpecCompiler specs, PlanEnv env) {
        return new com.legend.resolver.StoreResolver(env.ctx(), specs)
                .withLetBindings(env.queryLets()).withPlanRows(env.planRows())
                .withHandleRegistrar(pn -> {
                    // an INLINE handle met by the resolver: build its rows
                    // now (the let-time path for let-bound handles)
                    PlanAllocations.registerHandleRows("", pn, java.util.List.of(), specs, env);
                    return env.planRows().get(com.legend.plan.PlanRows.scopeId(pn));
                });
    }

    /** The execution context an executionPlan / execute / toSQLString call
     * binds through its RUNTIME argument: the argument is brought to its
     * VALUE (helper calls inlined) and read ONCE. */
    /** The feature flags an execution runs under: the executing frame's own
     *  (its execute call's context argument) plus the runner's defaults
     *  (ExecuteOptions.features) — the two sources, merged ONCE here. */
    /** PUSH_DOWN_ENUM_TRANSFORM on this plan's context (the plan printer's
     *  enum-tuple form follows it). */
    public static boolean pushDownEnums(PlanEnv env, java.util.List<TypedSpec> body) {
        return featuresOf(env, body).contains(
                com.legend.platform.Feature.PUSH_DOWN_ENUM_TRANSFORM);
    }

    public static java.util.Set<com.legend.platform.Feature> featuresOf(PlanEnv env,
            java.util.List<TypedSpec> body) {
        java.util.Set<com.legend.platform.Feature> all =
                java.util.EnumSet.noneOf(com.legend.platform.Feature.class);
        if (env.frame() != null) {
            all.addAll(env.frame().features());
        }
        all.addAll(env.features());
        all.addAll(com.legend.compiler.spec.typed.ExecutionContext.treeFeatures(body));
        return all.isEmpty() ? java.util.Set.of() : java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(all));
    }

    public static com.legend.compiler.spec.typed.ExecutionContext boundContext(
            TypedSpec runtimeArg, SpecCompiler specs) {
        TypedSpec value = new com.legend.compiler.spec.UserCallInliner(specs)
                .inlineBody(java.util.List.of(runtimeArg)).get(0);
        return com.legend.compiler.spec.typed.ExecutionContext.reader()
                .read(java.util.Optional.empty(), value);
    }

    /** The same read under a statement: the argument and the values it
     * names (a let-bound runtime, a let-bound hook operand) chase the
     * statement's preceding lets. */
    public static com.legend.compiler.spec.typed.ExecutionContext boundContext(
            TypedSpec runtimeArg, java.util.List<TypedSpec> letPrefix, SpecCompiler specs) {
        TypedSpec value = new com.legend.compiler.spec.UserCallInliner(specs)
                .inlineBody(java.util.List.of(com.legend.compiler.spec.typed.Lets.bound(runtimeArg, letPrefix))).get(0);
        return com.legend.compiler.spec.typed.ExecutionContext.reader()
                .bind(v -> com.legend.compiler.spec.typed.Lets.bound(v, letPrefix))
                .read(java.util.Optional.empty(), value);
    }

    public static void collectStores(TypedSpec n, java.util.Set<String> out) {
        if (n instanceof com.legend.compiler.spec.typed.TypedTableReference tr) {
            out.add(tr.store());
        }
        for (TypedSpec c : n.children()) {
            collectStores(c, out);
        }
    }

    /** ONE VALUE expression through the ordinary back half (G½ inline →
     * H resolve → lower/execute) — the assert-verdict arm's side
     * evaluator. The same sequence as the generic statement tail. */
    /** LOWER → post-process → two-phase dynamic pivot → deferred
     * relation-toString resolution: the whole plan-preparation ladder
     * (split from executeTyped at the 250-line shape guard). */
    public static com.legend.sql.SqlQuery lowerAndPrepare(
            java.util.List<TypedSpec> body, PlanEnv env, ModelContext ctx,
            com.legend.sql.dialect.SqlDialect dialect, boolean identity) {
        return lowerAndPrepare(body, env, ctx, dialect, identity, false);
    }

    /** {@link #lowerAndPrepare(java.util.List, PlanEnv, ModelContext, com.legend.sql.dialect.SqlDialect,
     *  boolean)}; {@code named}: the query's columns named in its select, in its relation's
     *  order (Lowerer.namedColumns: a staged table takes them by position). The two schema reads here (a static
     *  pivot's keys, a deferred relation's columns) go through the session seam (track B's B1a, 2026-10-29). */
    public static com.legend.sql.SqlQuery lowerAndPrepare(
            java.util.List<TypedSpec> body, PlanEnv env, ModelContext ctx,
            com.legend.sql.dialect.SqlDialect dialect, boolean identity, boolean named) {
        com.legend.lowering.Lowerer lowerer = lowererFor(body, env, ctx, identity);
        com.legend.sql.SqlQuery plan =
                lowerer.lower(com.legend.lowering.SeedableLets
                        .withSeedableLetPrefix(body, env.queryLets(), ctx));
        // post-process; a dynamic pivot stays dynamic: its plan is keyed where it is planned (JudgePlans, PlanMaker.keyed:
        // its keys read when the plan runs), never staticized by a read at planning (the seam program's S3a, 2026-10-31)
        plan = com.legend.lowering.SqlPostProcessors.applyRecorded(plan,
                env.tableReplace(),
                env.postProcessors().extractCtes(),
                env.postProcessors().nonExecutable());
        // A DYNAMIC PIVOT's TEXT whose columns nothing names (its templates unresolved): its columns exist only once the
        // database is asked, and planning asks the database nothing (the seam program's S3a, 2026-10-30: the LIMIT-0
        // column probe deleted; the six channel B bodies that reached it named their columns by a cast, F10). Refused
        // by name: the pivot's keyed form writes no text yet; no body in the corpus or channel B reaches it (step H's
        // H1). A raw grid's text never comes here: toCSV is its report's texts, toString refused (Render, S3c).
        if (!lowerer.deferredTds().isEmpty()) {
            throw new com.legend.error.NotImplementedException("a dynamic pivot's text (toString/toCSV) over columns"
                    + " nothing names: its columns exist only once the database is asked, planning asks the database"
                    + " nothing, and the pivot's keyed form writes no text");
        }
        return named ? lowerer.namedColumns(plan) : plan;
    }

    /** The Lowerer a statement's body lowers with (lowerAndPrepare's and the store write's): the model's layouts,
     *  keys, time zone and the body's features. */
    public static com.legend.lowering.Lowerer lowererFor(java.util.List<TypedSpec> body, PlanEnv env,
            ModelContext ctx, boolean identity) {
        // F13: the verdict-side lane's layouts carry the canon rider too;
        // the value lane's are the object layout (docs/OBJECT_IDENTITY_2026_10_20.md
        // §3.1): every object its __id and __type, so equal() compares a
        // keyless class's objects by identity, never as structs.
        com.legend.lowering.Lowerer lowerer = new com.legend.lowering.Lowerer(
                t -> identity ? com.legend.compiler.element.ClassLayouts.layoutOf(ctx, t, true)
                        : com.legend.compiler.element.ClassLayouts.objectLayoutOf(ctx, t),
                f -> ctx.findClass(f).isPresent(), ctx.implementations()).withEngineExistsJoinForm()
                .withDbTimeZone(env.timeZone());
        // D91: the <<equality.Key>> resolver rides EVERY lane — equal()
        // over keyed instances is the key relation on the execute path
        // too (the structural fallback erased class identity and read
        // non-key fields). Identity minting stays verdict-lane-only.
        lowerer = lowerer.withInstanceKeys(t2 -> com.legend.compiler
                .element.EqualityKeys.resolve(ctx, t2));
        lowerer = identity ? lowerer.withInstanceIds(env.instanceIds()::idOf)
                : lowerer.withObjectIdentity(env.instanceIds()::idOf).withDeclaredEgress();
        // the query's feature flags: a LOWERING concern (Lowerer.withFeatures)
        lowerer = lowerer.withFeatures(featuresOf(env, body));
        return lowerer;
    }

    /** A VALUE as the executor's body: the let prefix spliced, user calls
     * inlined (with the caller's envelope hook), natives staged, stores
     * resolved — the ONE road both the driver's evalValue and
     * planValue take (leg 3.1b: the audit's one real duplication). */
    public static java.util.List<TypedSpec> sideBody(TypedSpec value,
            java.util.List<TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs, PlanEnv env,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> hook) {
        return stagedSide(inlinedSide(value, letPrefix, specs, env, hook), specs, env);
    }

    /** The inlined side: the let prefix spliced and user calls inlined — the
     * form a literal-only side can be FOLDED from (a helper-built golden text is
     * literal only after its call is inlined). */
    public static java.util.List<TypedSpec> inlinedSide(TypedSpec value,
            java.util.List<TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs, PlanEnv env,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> hook) {
        java.util.List<TypedSpec> single = new java.util.ArrayList<>(letPrefix);
        single.add(value);
        var inliner = hook == null
                ? new com.legend.compiler.spec.UserCallInliner(specs)
                : new com.legend.compiler.spec.UserCallInliner(specs, hook);
        java.util.List<TypedSpec> body = new java.util.ArrayList<>(
                inliner.inlineBody(single));
        env.queryLets().putAll(inliner.queryLets());
        return body;
    }

    /** The inlined side staged and resolved for execution. */
    public static java.util.List<TypedSpec> stagedSide(java.util.List<TypedSpec> body,
            com.legend.compiler.spec.SpecCompiler specs, PlanEnv env) {
        final java.util.List<TypedSpec> stageEnv = body;
        body.replaceAll(b -> com.legend.compiler.spec.NativeDispatch
                .stage(b, stageEnv, EnginePlans.nativeRoutines(specs, env)));
        return resolvedToExecute(resolver(specs, env), body, env);
    }

    /** Phase H for a body that EXECUTES on the env's session: resolved, then every store it touches checked
     *  against the runtime's connections (CrossStoreGuard — one session per query; a store the runtime does
     *  not bind refused). Every execution path resolves through here (C3b: four of the five ran unchecked). */
    public static java.util.List<TypedSpec> resolvedToExecute(com.legend.resolver.StoreResolver resolver,
            java.util.List<TypedSpec> body, PlanEnv env) {
        java.util.List<TypedSpec> resolved = resolver.resolve(body, env.runtimeFqn());
        // C2.2: stores bound to DIFFERENT connections cannot share
        // the one session connection — wall, never wrong-database rows
        CrossStoreGuard.check(resolved, env.ctx(), env.runtimeFqn());
        return resolved;
    }
}
