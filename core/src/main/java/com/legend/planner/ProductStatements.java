// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.ArrayList;
import java.util.List;

/**
 * THE PRODUCT'S STATEMENT ROAD (the test-is-the-product plan's M2, brief B11, 2026-10-31): a query's body, or one
 * statement of a program with the lets before it, planned by the functions a test statement is planned by, so the
 * product and the tests share one front half. The statement is
 * <ol>
 *   <li>inlined with the inliner's consumed lets KEPT, under the statement's mark in a program (B9);</li>
 *   <li>its natives staged ({@code planToString}, {@code toSQLString}: the plan-time strings they print);</li>
 *   <li>resolved with the lets' bindings and the plan's handle rows ({@link BodyPlanner#resolver}), then the
 *       runtime decided and the cross-store guard;</li>
 *   <li>prepared ({@link BodyPlanner#prepare}): the OUTERMOST {@code from()}'s context is the frame -- its
 *       connection's time zone, its features and post-processors -- and its driver-key and import-data-flow appends
 *       made;</li>
 * </ol>
 * and lowered with the PRODUCT's configuration ({@link TypedQuery#lowerer}: the object layout, the program's
 * minter, the exists-join form unless a temporal class is read, the streaming root where asked, the slots) plus the
 * frame's time zone and features and the seedable let prefix, then post-processed by the frame's CTE extraction and
 * nonExecutable pass. A {@code from()} frame's TABLE RENAMES are applied on neither road yet (the environment's
 * renames are an {@code execute(...)} frame's, M3's B10; a {@code toSQLString} call applies its own runtime's,
 * {@code EnginePlans}). The test road's own factors (the identity layout, declared egress, the canon) are not here:
 * they are the judges'. No dialect: nothing on this road writes a statement, its caller renders in the runtime's.
 */
public final class ProductStatements {

    private ProductStatements() {
    }

    /** The literal kinds a plan answers as its own constant, the database never asked: the kinds whose database
     *  spelling cannot differ from the literal's (D26). The one owner, read by the test road's fold too
     *  ({@code driver.LiteralFold.ADMITTED}); a new kind needs its differential leg in {@code ConstantPlanParityTest}
     *  first. */
    public static final java.util.Set<String> ADMITTED = java.util.Set.of("String", "Boolean");

    /** A statement on the product's road, prepared: its kind ({@link Prepared}), and whether it reads a temporal
     *  class (decided on the inlined body, before the resolver, as the product always has). */
    public record Statement(Prepared prepared, boolean temporal) {
    }

    /** {@code statements} (a query's body, or a program's statement after its lets) prepared for {@code runtime};
     *  {@code mark} the statement's position in a program planned statement by statement, null for a whole query. */
    public static Statement prepare(TypedQuery query, List<TypedSpec> statements,
            @com.legend.base.Nullable String runtime, @com.legend.base.Nullable Integer mark,
            java.util.Set<com.legend.platform.Feature> features) {
        ModelContext ctx = query.context();
        com.legend.compiler.spec.SpecCompiler specs = query.specs();
        // no dialect: nothing on this road writes a statement (its caller renders in the runtime's dialect), so the
        // runtime is decided after the resolver, as the product always decided it, and every refusal keeps its order
        PlanEnv env = PlanEnv.of(ctx, runtime, null, features, ProductStatements::noGenerator);
        com.legend.compiler.spec.UserCallInliner inliner = new com.legend.compiler.spec.UserCallInliner(specs);
        List<TypedSpec> body = new ArrayList<>(inliner.inlineBody(new ArrayList<>(statements), mark));   // Phase G½
        // the consumed lets the body still READS free (a graph-tree qualifier's argument keeps its source spelling):
        // the resolver's bindings and the lowering's seeds. A let dissolved everywhere is neither -- bound in the
        // resolver it would rename a lambda parameter of its name, seeded it would spend alias numbers
        java.util.Set<String> read = com.legend.compiler.spec.typed.FreeVars.ofBody(body);
        inliner.queryLets().forEach((name, value) -> {
            if (read.contains(name)) {
                env.queryLets().put(name, value);
            }
        });
        boolean temporal = com.legend.compiler.element.Temporal.anyTemporalGetAll(body, ctx);
        final List<TypedSpec> stageEnv = body;
        body.replaceAll(b -> com.legend.compiler.spec.NativeDispatch.stage(b, stageEnv,
                EnginePlans.nativeRoutines(specs, env)));
        body = BodyPlanner.resolver(specs, env).resolve(body, runtime);   // Phase H
        // the SQL is for this runtime's session: the runtime decided first (no runtime is refused there, by name),
        // then every store the statement touches must be bound to it (C3b)
        com.legend.Compiler.executesOn(ctx, runtime);
        CrossStoreGuard.check(body, ctx, runtime);
        return new Statement(BodyPlanner.prepare(body, env), temporal);
    }

    /** {@code s}'s answer as a value the program wrote, answered with nothing sent (MV12), or null: a DDL text the
     *  model gives (D17), or a String or Boolean literal (D26: the kinds whose database spelling cannot differ from
     *  the literal's, the test road's own rule, {@code LiteralFold}). */
    public static com.legend.executionplan.ExecutionPlan.@com.legend.base.Nullable Constant constant(Statement s) {
        return switch (s.prepared()) {
            case Prepared.DdlText d -> constant(d.type().typeName(), java.util.Collections.singletonList(d.text()), true);
            case Prepared.DdlTexts d -> {
                List<String> texts = new ArrayList<>(d.texts().size());
                for (Object t : d.texts()) {
                    if (!(t instanceof String text)) {
                        throw new IllegalStateException("a DDL text collection holds a " + t.getClass().getSimpleName());
                    }
                    texts.add(text);
                }
                yield constant("String", texts, false);
            }
            case Prepared.Planned p -> switch (p.root()) {
                case com.legend.compiler.spec.typed.TypedCString str when ADMITTED.contains("String") ->
                        constant("String", java.util.Collections.singletonList(str.value()), true);
                case com.legend.compiler.spec.typed.TypedCBoolean b when ADMITTED.contains("Boolean") ->
                        constant("Boolean", java.util.Collections.singletonList(String.valueOf(b.value())), true);
                default -> null;
            };
            // print and println: no statement, the argument never evaluated; their value, as the test road's
            // (R18, the test-is-the-product plan's B12 item 4: both doors)
            case Prepared.Effect e when com.legend.builtin.NativeFn.Effect.isInertDiagnostic(e.call().callee().id()) ->
                    constant("String", java.util.Collections.singletonList(null), true);
            case Prepared.Effect e -> null;
            case Prepared.EffectfulMap m -> null;
            case Prepared.StoreWrite w -> null;
            case Prepared.Handle h -> null;
        };
    }

    /** A one-value constant of {@code type} ({@code text} null: no value): a value a program's statement answers with
     *  nothing sent. */
    public static com.legend.executionplan.ExecutionPlan.Constant scalar(String type,
            @com.legend.base.Nullable String text) {
        return constant(type, java.util.Collections.singletonList(text), true);
    }

    /** Whether {@code s} is an orchestration handle -- a connection, a runtime, {@code connectionByElement}'s
     *  connection -- a value a program passes around, which plans no step before its last statement and has no
     *  value to answer as its last (B12 item 4). */
    public static boolean handle(Statement s) {
        if (s.prepared() instanceof Prepared.Effect e) {
            com.legend.compiler.spec.typed.TypedNativeCall call = e.call();
            return com.legend.builtin.NativeFn.Effect.of(call.callee().id()).orElse(null)
                    == com.legend.builtin.NativeFn.Effect.CONNECTION_BY_ELEMENT;
        }
        return s.prepared() instanceof Prepared.Handle;
    }

    /** A handle's type ({@link #handle}). */
    public static String handleType(Statement s) {
        return switch (s.prepared()) {
            case Prepared.Handle h -> h.type().typeName();
            case Prepared.Effect e when handle(s) -> e.call().info().type().typeName();
            default -> throw new IllegalStateException("not an orchestration handle: "
                    + s.prepared().getClass().getSimpleName());
        };
    }

    private static com.legend.executionplan.ExecutionPlan.Constant constant(String type, List<String> texts,
            boolean scalar) {
        com.legend.executionplan.ExecutionPlan.Multiplicity m = scalar
                ? new com.legend.executionplan.ExecutionPlan.Multiplicity(texts.get(0) == null ? 0 : 1, 1)
                : new com.legend.executionplan.ExecutionPlan.Multiplicity(0, null);
        List<List<String>> cells = new ArrayList<>(texts.size());
        for (String t : texts) {
            cells.add(java.util.Collections.singletonList(t));
        }
        return new com.legend.executionplan.ExecutionPlan.Constant(List.of(
                new com.legend.executionplan.ExecutionPlan.Column("value", type, m, null, null, null)), cells);
    }

    /** {@code s}'s body to lower: a planned statement's, or a refusal by name of a kind the product's query door does
     *  not plan as a query. A handle (a connection, a runtime) is refused as an answer: it has no value to write (the
     *  test road's driver answers it as a null its judges never compare); before a program's last statement it plans
     *  no step ({@code PlanMaker.program}). */
    public static Prepared.Planned planned(Statement s) {
        return switch (s.prepared()) {
            case Prepared.Planned p -> p;
            case Prepared.Effect e when handle(s) -> throw new com.legend.error.NotImplementedException("a "
                    + e.call().info().type().typeName() + " (an orchestration handle) answered by a query: it has no"
                    + " value a plan can answer");
            case Prepared.Effect e -> throw new com.legend.error.NotImplementedException("an effect ("
                    + e.call().callee().qualifiedName() + ") in a query: effects run in a test's program, and the"
                    + " product's query door plans no effect");
            case Prepared.EffectfulMap m -> throw new com.legend.error.NotImplementedException("a map over an effect"
                    + " in a query: effects run in a test's program, and the product's query door plans no effect");
            case Prepared.StoreWrite w -> throw new IllegalStateException("a store write planned as a query: a"
                    + " program that writes is planned as a sequence (PlanMaker.program)");
            case Prepared.DdlText d -> throw new com.legend.error.NotImplementedException("a DDL text has no"
                    + " statement: the execution plan answers it as a constant; a query's SQL text has none to show");
            case Prepared.DdlTexts d -> throw new com.legend.error.NotImplementedException("DDL texts have no"
                    + " statement: the execution plan answers them as a constant; a query's SQL text has none to show");
            case Prepared.Handle h -> throw new com.legend.error.NotImplementedException("a " + h.type().typeName()
                    + " (an orchestration handle) answered by a query: it has no value a plan can answer");
        };
    }

    /** {@code p} lowered on the product's road: {@code streaming} the streaming graph root, {@code slots} the query's
     *  declared parameters, {@code sites} the program's object-id minter. */
    public static com.legend.sql.SqlQuery lower(TypedQuery query, Prepared.Planned p, boolean temporal,
            boolean streaming, List<com.legend.sql.SqlExpr.PlanParam> slots, com.legend.lowering.ObjectSites sites) {
        com.legend.lowering.Lowerer lowerer = lowerer(query, p, temporal, streaming, slots, sites);
        com.legend.sql.SqlQuery plan = lowerer.lower(seeded(p.body(), p.env(), query.context()));
        return postProcessed(plan, p.env());
    }

    /** {@code body} after the seedable prefix of the lets it still READS (D9: a surviving {@code $let} read -- a
     *  graph-tree qualifier's argument -- resolves at the leaf). A let the inliner dissolved everywhere is not
     *  seeded: lowering it first would only spend the statement's alias numbers, and a lambda parameter of the same
     *  name would be renamed against it. */
    public static List<TypedSpec> seeded(List<TypedSpec> body, PlanEnv env, ModelContext ctx) {
        java.util.Set<String> read = com.legend.compiler.spec.typed.FreeVars.ofBody(body);
        java.util.Map<String, TypedSpec> lets = new java.util.LinkedHashMap<>();
        env.queryLets().forEach((name, value) -> {
            if (read.contains(name)) {
                lets.put(name, value);
            }
        });
        return lets.isEmpty() ? body : com.legend.lowering.SeedableLets.withSeedableLetPrefix(body, lets, ctx);
    }

    /** The Lowerer {@code p} lowers with on the product's road: the product's configuration, the frame's time zone,
     *  the features of the frame, the statement's tree and the caller. */
    public static com.legend.lowering.Lowerer lowerer(TypedQuery query, Prepared.Planned p, boolean temporal,
            boolean streaming, List<com.legend.sql.SqlExpr.PlanParam> slots, com.legend.lowering.ObjectSites sites) {
        return lowerer(query, p.body(), p.env(), temporal, streaming, slots, sites);
    }

    /** The Lowerer {@code body} (prepared on {@code env}) lowers with on the product's road. */
    public static com.legend.lowering.Lowerer lowerer(TypedQuery query, List<TypedSpec> body, PlanEnv env,
            boolean temporal, boolean streaming, List<com.legend.sql.SqlExpr.PlanParam> slots,
            com.legend.lowering.ObjectSites sites) {
        return query.lowerer(temporal, streaming, slots, BodyPlanner.featuresOf(env, body), sites)
                .withDbTimeZone(env.timeZone());
    }

    /** A store write prepared on the product's road, lowered: its statement, the seedable let prefix before it. A
     *  write under the frame's post-processors is refused by name, as the test road refuses it (the writes design's
     *  leg 2 writes the table it names). */
    public static com.legend.sql.SqlDml storeWrite(TypedQuery query, Prepared.StoreWrite w, boolean temporal,
            List<com.legend.sql.SqlExpr.PlanParam> slots, com.legend.lowering.ObjectSites sites) {
        PlanEnv env = w.env();
        if (!env.tableReplace().isEmpty() || env.postProcessors().extractCtes()
                || env.postProcessors().nonExecutable()) {
            throw new com.legend.error.NotImplementedException("a write into a store table under the execution's"
                    + " post-processors (tableReplace, extractCtes, nonExecutable) is not designed (the writes design's"
                    + " leg 2 writes the table it names)");
        }
        List<TypedSpec> statements = new ArrayList<>(w.body().subList(0, w.body().size() - 1));
        statements.add(w.write());
        return lowerer(query, statements, env, temporal, false, slots, sites).lowerStoreWrite(
                seeded(statements, env, query.context()));
    }

    /** {@code plan} post-processed by {@code env}'s frame (its connection's table renames and CTE extraction). */
    public static com.legend.sql.SqlQuery postProcessed(com.legend.sql.SqlQuery plan, PlanEnv env) {
        return com.legend.lowering.SqlPostProcessors.applyRecorded(plan, env.tableReplace(),
                env.postProcessors().extractCtes(), env.postProcessors().nonExecutable());
    }

    /** The product plans no test-data generator: its plan text is a test's ({@code PlanEnv}'s generator hook). */
    private static String noGenerator(com.legend.compiler.spec.typed.TypedTestDataGen generator) {
        throw new com.legend.error.NotImplementedException("a test-data generator in a query: the generator runs in"
                + " a test's program, and the product's query door plans none");
    }
}
