// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.builtin.NativeFn;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.Temporal;
import com.legend.compiler.spec.NativeDispatch;
import com.legend.compiler.spec.ResultEnvelopeSplice;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.UserCallInliner;
import com.legend.compiler.spec.typed.FreeVars;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTestDataGen;
import com.legend.error.NotImplementedException;
import com.legend.executionplan.ExecutionPlan;
import com.legend.lowering.Lowerer;
import com.legend.lowering.ObjectSites;
import com.legend.lowering.SeedableLets;
import com.legend.lowering.SqlPostProcessors;
import com.legend.platform.Feature;
import com.legend.sql.SqlDml;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 * nonExecutable pass. A statement that reads an {@code execute(...)}'s result splices its chain (the program's
 * {@link ProgramFrames}, M3f) and runs under that frame's table renames ({@link FramePlanner#renames}); a
 * {@code from()} frame's own renames are M3h's; a {@code toSQLString} call applies its own runtime's
 * ({@code EnginePlans}). The test road's own factors (the identity layout, declared egress, the canon) are not here:
 * they are the judges'. No dialect: nothing on this road writes a statement, its caller renders in the runtime's.
 */
public final class ProductStatements {

    private ProductStatements() {
    }

    /** The literal kinds a plan answers as its own constant, the database never asked: the kinds whose database
     *  spelling cannot differ from the literal's (D26). The one owner, read by the test road's fold too
     *  ({@code testplan.LiteralFold.ADMITTED}); a new kind needs its differential leg in {@code ConstantPlanParityTest}
     *  first. */
    public static final Set<String> ADMITTED = Set.of("String", "Boolean");

    /** A statement on the product's road, prepared: its kind ({@link Prepared}), and whether it reads a temporal
     *  class (decided on the inlined body, before the resolver, as the product always has). */
    public record Statement(Prepared prepared, boolean temporal, ProgramFrames.Reads reads, Set<String> stores) {
        public Statement {
            stores = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(stores));
        }
    }

    /** {@code statements} (a query's body, or a program's statement after its lets) prepared for {@code runtime};
     *  {@code mark} the statement's position in a program planned statement by statement, null for a whole query. */
    public static Statement prepare(TypedQuery query, List<TypedSpec> statements,
            @Nullable String runtime, @Nullable Integer mark,
            Set<Feature> features) {
        return prepare(query, statements, runtime, mark, features, new ProgramFrames());
    }

    /** {@link #prepare} for a statement of a program whose frames are {@code frames} (the test-is-the-product plan's
     *  M3f): the statement's reads of a frame and of an inline {@code execute(...)}'s result splice their chains
     *  (the splice hook), its plan rows are the program's, and its environment is widened with the renames of the
     *  frames it reads ({@link FramePlanner#renames}, after the resolver, as the walk widens it). */
    public static Statement prepare(TypedQuery query, List<TypedSpec> statements,
            @Nullable String runtime, @Nullable Integer mark,
            Set<Feature> features, ProgramFrames frames) {
        return prepare(query, statements, runtime, mark, features, frames, Map.of());
    }

    /** {@link #prepare} for a frame's own query, run under {@code runtimeRenames}: the table renames of the runtime
     *  the frame runs on (a frame let's eager run and its CTE, as the walk runs them in the frame's environment). */
    public static Statement prepare(TypedQuery query, List<TypedSpec> statements,
            @Nullable String runtime, @Nullable Integer mark,
            Set<Feature> features, ProgramFrames frames, Map<String, String> runtimeRenames) {
        return prepare(query, statements, runtime, mark, features, frames, runtimeRenames, false);
    }

    /** {@link #prepare}; {@code frameAnswered}: the statement is a TEST program's answer, and its root, an
     *  {@code execute(...)} of a relation-rooted query, answers its frame's rows (its chain) -- the walk's forced frame
     *  at a value position, whose run is the value (ExecuteFrameTest's executeInResultPosition, 2026-11-04, the core
     *  program). The product's query door plans no Result envelope as an answer, and keeps refusing it. */
    public static Statement prepare(TypedQuery query, List<TypedSpec> statements,
            @Nullable String runtime, @Nullable Integer mark,
            Set<Feature> features, ProgramFrames frames, Map<String, String> runtimeRenames, boolean frameAnswered) {
        ModelContext ctx = query.context();
        SpecCompiler specs = query.specs();
        // no dialect: nothing on this road writes a statement (its caller renders in the runtime's dialect), so the
        // runtime is decided after the resolver, as the product always decided it, and every refusal keeps its order
        ProgramFrames.StatementRows rows = frames.statementRows();
        PlanEnv env = runtimeRenames.isEmpty() ? env(ctx, runtime, features, rows)
                : env(ctx, runtime, features, rows).withTableReplace(runtimeRenames);
        List<TypedSpec> lets = statements.subList(0, statements.size() - 1);
        Inlined inlined = inline(specs, statements, mark, frames.recorder(lets, specs, env, rows), frameAnswered);
        // the consumed lets the body still READS free (a graph-tree qualifier's argument keeps its source spelling):
        // the resolver's bindings and the lowering's seeds. A let dissolved everywhere is neither -- bound in the
        // resolver it would rename a lambda parameter of its name, seeded it would spend alias numbers
        // a graph tree's qualifier arguments read too ({@code TypedGraphTree.args}, no child of the fetch or the
        // serialize): the corpus's `customer($processingDate, $businessDate)` over an executeLegendQuery lambda's
        // parameters, bound from its pairs, which the walk's frame run resolves from the frame's query lets (M10c
        // item 12; one walk with the plain scan's binders)
        Set<String> read = FreeVars.ofBodyReadingTrees(inlined.body());
        inlined.lets().forEach((name, value) -> {
            if (read.contains(name)) {
                env.queryLets().put(name, value);
            }
        });
        // the lets the program's frames' chains consumed, read free here (a frame's run or CTE, a reader's paste)
        frames.consumedLets().forEach((name, value) -> {
            if (read.contains(name)) {
                env.queryLets().putIfAbsent(name, value);
            }
        });
        List<TypedSpec> body = new ArrayList<>(inlined.body());
        boolean temporal = Temporal.anyTemporalGetAll(body, ctx);
        final List<TypedSpec> stageEnv = body;
        body.replaceAll(b -> NativeDispatch.stage(b, stageEnv,
                EnginePlans.nativeRoutines(specs, env)));
        body = BodyPlanner.resolver(specs, env).resolve(body, runtime);   // Phase H
        // the SQL is for this runtime's session: the runtime decided first (no runtime is refused there, by name),
        // then every store the statement touches must be bound to it (C3b)
        Compiler.executesOn(ctx, runtime);
        CrossStoreGuard.check(body, ctx, runtime);
        // one session per statement: none reads the system metamodel store beside a user store (the walk's route
        // refuses it alike, SystemRoutes.of)
        Set<String> stores = SystemRoutes.stores(body);
        CrossStoreGuard.oneSession(stores);
        // widened LAST, after resolution: the renames of the frames the statement reads (the walk's order)
        Map<String, String> renames = FramePlanner.renames(statements.get(statements.size() - 1),
                frames.renames(), env.tableReplace(), lets, specs);
        PlanEnv widened = renames == null ? env : env.withTableReplace(renames);
        return new Statement(BodyPlanner.prepare(body, widened), temporal, inlined.reads(), stores);
    }

    /** The planning environment of a statement on this road over {@code planRows}: the model, the runtime, the
     *  features, no dialect (its caller renders), the product's generator refusal. */
    public static PlanEnv env(ModelContext ctx, @Nullable String runtime, Set<Feature> features,
            Map<String, Map<String, List<List<String>>>> planRows) {
        return PlanEnv.of(ctx, runtime, null, features, ProductStatements::noGenerator).withPlanRows(planRows);
    }

    /** A statement inlined (Phase G½): its body, the lets the inliner consumed (in the order it met them), and what
     *  it read of the program's frames. */
    private record Inlined(List<TypedSpec> body, Map<String, TypedSpec> lets, ProgramFrames.Reads reads) {
    }

    /** {@code statements} inlined with the splice hook {@code first} records into; inlined again, its frames read as
     *  their CTE, where it pastes one more than once (M3g, B10). */
    private static Inlined inline(SpecCompiler specs, List<TypedSpec> statements, @Nullable Integer mark,
            ProgramFrames.Recorder first, boolean frameAnswered) {
        ProgramFrames.Recorder recorder = first;
        UserCallInliner inliner = new UserCallInliner(specs, ResultEnvelopeSplice.hook(recorder));
        List<TypedSpec> body = frameAnswered(inliner.inlineBody(new ArrayList<>(statements), mark), recorder,
                frameAnswered);
        ProgramFrames.Recorder again = recorder.again(body);
        if (again != null) {
            recorder = again;
            inliner = new UserCallInliner(specs, ResultEnvelopeSplice.hook(recorder));
            body = frameAnswered(inliner.inlineBody(new ArrayList<>(statements), mark), recorder, frameAnswered);
        }
        return new Inlined(body, Collections.unmodifiableMap(new LinkedHashMap<>(inliner.queryLets())),
                recorder.reads());
    }

    /** {@code body} with its last statement, an {@code execute(...)} of a relation-rooted query at its root, read as
     *  its frame's chain where {@code frameAnswered} (a test program's answer: {@link #prepare}); as it is otherwise. */
    private static List<TypedSpec> frameAnswered(List<TypedSpec> body, ProgramFrames.Recorder recorder,
            boolean frameAnswered) {
        if (!frameAnswered || body.isEmpty()
                || !(body.get(body.size() - 1) instanceof TypedNativeCall ec)
                || NativeFn.Handle.of(ec.callee().id()).orElse(null) != NativeFn.Handle.EXECUTE) {
            return body;
        }
        ResultEnvelopeSplice.View view = recorder.inlineExecute(ec, false);
        if (!view.relationRooted()) {
            return body;
        }
        List<TypedSpec> out = new ArrayList<>(body);
        out.set(out.size() - 1, view.chain());
        return out;
    }

    /** {@code s}'s answer as a value the program wrote, answered with nothing sent (MV12), or null: a DDL text the
     *  model gives (D17), or a String or Boolean literal (D26: the kinds whose database spelling cannot differ from
     *  the literal's, the test road's own rule, {@code LiteralFold}). */
    public static ExecutionPlan.@Nullable Constant constant(Statement s) {
        return switch (s.prepared()) {
            case Prepared.DdlText d -> constant(d.type().typeName(), Collections.singletonList(d.text()), true);
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
                case TypedCString str ->
                        constant("String", Collections.singletonList(str.value()), true);
                case TypedCBoolean b ->
                        constant("Boolean", Collections.singletonList(String.valueOf(b.value())), true);
                default -> null;
            };
            // print and println: no statement, the argument never evaluated; their value, as the test road's
            // (R18, the test-is-the-product plan's B12 item 4: both doors)
            case Prepared.Effect e when NativeFn.Effect.isInertDiagnostic(e.call().callee().id()) ->
                    constant("String", Collections.singletonList(null), true);
            case Prepared.Effect e -> null;
            case Prepared.EffectfulMap m -> null;
            case Prepared.StoreWrite w -> null;
            case Prepared.Handle h -> null;
        };
    }

    /** A one-value constant of {@code type} ({@code text} null: no value): a value a program's statement answers with
     *  nothing sent. */
    public static ExecutionPlan.Constant scalar(String type,
            @Nullable String text) {
        return constant(type, Collections.singletonList(text), true);
    }

    /** A test program's answer of NO VALUE: no row (Pure's {@code []}, as the walk answers a no-value side), where the
     *  product's constant writes a one-valued column a null cell (its answer for a query ending on {@code print},
     *  R18, kept: legend-engine has no {@code print} to match, upstream-api/m5-answers.json; the user, 2026-11-01). A
     *  constant holding a value is answered as it is: one value, Pure's multiplicity (the test-is-the-product plan's
     *  §9 item 2). */
    public static ExecutionPlan.Constant asATestAnswers(ExecutionPlan.Constant constant) {
        if (constant.columns().size() != 1 || constant.cells().size() != 1 || constant.cells().get(0).get(0) != null) {
            return constant;
        }
        ExecutionPlan.Column c = constant.columns().get(0);
        return new ExecutionPlan.Constant(List.of(new ExecutionPlan.Column(c.name(), c.type(),
                new ExecutionPlan.Multiplicity(0, 1), c.slot(), c.store(), c.precision())), List.of());
    }

    /** Whether {@code root} is a TO-ONE READER: {@code toOne()} over a relation, its exactly-one contract checked by
     *  the database at the statement's root ({@code lowering.ToOneGuard}, decision D2, 2026-11-07). One rule for the
     *  test door's body classes and its router. */
    public static boolean toOneReader(TypedSpec root) {
        return root instanceof TypedNativeCall call
                && com.legend.builtin.Pure.isToOneCall(call.callee().qualifiedName())
                && !call.args().isEmpty()
                && com.legend.compiler.element.type.Type.isRelation(call.args().get(0).info().type());
    }

    /** Whether {@code s} is an orchestration handle -- a connection, a runtime, {@code connectionByElement}'s
     *  connection -- a value a program passes around, which plans no step before its last statement and has no
     *  value to answer as its last (B12 item 4). */
    public static boolean handle(Statement s) {
        if (s.prepared() instanceof Prepared.Effect e) {
            TypedNativeCall call = e.call();
            return NativeFn.Effect.of(call.callee().id()).orElse(null)
                    == NativeFn.Effect.CONNECTION_BY_ELEMENT;
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

    private static ExecutionPlan.Constant constant(String type, List<String> texts,
            boolean scalar) {
        ExecutionPlan.Multiplicity m = scalar
                ? new ExecutionPlan.Multiplicity(texts.get(0) == null ? 0 : 1, 1)
                : new ExecutionPlan.Multiplicity(0, null);
        List<List<String>> cells = new ArrayList<>(texts.size());
        for (String t : texts) {
            cells.add(Collections.singletonList(t));
        }
        return new ExecutionPlan.Constant(List.of(
                new ExecutionPlan.Column("value", type, m, null, null, null)), cells);
    }

    /** {@code s}'s body to lower: a planned statement's, or a refusal by name of a kind the product's query door does
     *  not plan as a query. A handle (a connection, a runtime) is refused as an answer: it has no value to write (the
     *  test road's driver answers it as a null its judges never compare); before a program's last statement it plans
     *  no step ({@code PlanMaker.program}). */
    public static Prepared.Planned planned(Statement s) {
        return switch (s.prepared()) {
            case Prepared.Planned p -> p;
            case Prepared.Effect e when handle(s) -> throw new NotImplementedException("a "
                    + e.call().info().type().typeName() + " (an orchestration handle) answered by a query: it has no"
                    + " value a plan can answer");
            case Prepared.Effect e -> throw new NotImplementedException("an effect ("
                    + e.call().callee().qualifiedName() + ") in a query: effects run in a test's program, and the"
                    + " product's query door plans no effect");
            case Prepared.EffectfulMap m -> throw new NotImplementedException("a map over an effect"
                    + " in a query: effects run in a test's program, and the product's query door plans no effect");
            case Prepared.StoreWrite w -> throw new IllegalStateException("a store write planned as a query: a"
                    + " program that writes is planned as a sequence (PlanMaker.program)");
            case Prepared.DdlText d -> throw new NotImplementedException("a DDL text has no"
                    + " statement: the execution plan answers it as a constant; a query's SQL text has none to show");
            case Prepared.DdlTexts d -> throw new NotImplementedException("DDL texts have no"
                    + " statement: the execution plan answers them as a constant; a query's SQL text has none to show");
            case Prepared.Handle h -> throw new NotImplementedException("a " + h.type().typeName()
                    + " (an orchestration handle) answered by a query: it has no value a plan can answer");
        };
    }

    /** {@code p} lowered on the product's road: {@code streaming} the streaming graph root, {@code slots} the query's
     *  declared parameters, {@code sites} the program's object-id minter. */
    public static SqlQuery lower(TypedQuery query, Prepared.Planned p, boolean temporal,
            boolean streaming, List<SqlExpr.PlanParam> slots, ObjectSites sites) {
        Lowerer lowerer = lowerer(query, p, temporal, streaming, slots, sites);
        SqlQuery plan = lowerer.lower(seeded(p.body(), p.env(), query.context()));
        return postProcessed(plan, p.env());
    }

    /** {@code body} after the seedable prefix of the lets it still READS (D9: a surviving {@code $let} read -- a
     *  graph-tree qualifier's argument -- resolves at the leaf). A let the inliner dissolved everywhere is not
     *  seeded: lowering it first would only spend the statement's alias numbers, and a lambda parameter of the same
     *  name would be renamed against it. */
    public static List<TypedSpec> seeded(List<TypedSpec> body, PlanEnv env, ModelContext ctx) {
        Set<String> read = FreeVars.ofBody(body);
        Map<String, TypedSpec> lets = new LinkedHashMap<>();
        env.queryLets().forEach((name, value) -> {
            if (read.contains(name)) {
                lets.put(name, value);
            }
        });
        return lets.isEmpty() ? body : SeedableLets.withSeedableLetPrefix(body, lets, ctx);
    }

    /** The Lowerer {@code p} lowers with on the product's road: the product's configuration, the frame's time zone,
     *  the features of the frame, the statement's tree and the caller. */
    public static Lowerer lowerer(TypedQuery query, Prepared.Planned p, boolean temporal,
            boolean streaming, List<SqlExpr.PlanParam> slots, ObjectSites sites) {
        return lowerer(query, p.body(), p.env(), temporal, streaming, slots, sites);
    }

    /** The Lowerer {@code body} (prepared on {@code env}) lowers with on the product's road. */
    public static Lowerer lowerer(TypedQuery query, List<TypedSpec> body, PlanEnv env,
            boolean temporal, boolean streaming, List<SqlExpr.PlanParam> slots,
            ObjectSites sites) {
        return query.lowerer(temporal, streaming, slots, BodyPlanner.featuresOf(env, body), sites)
                .withDbTimeZone(env.timeZone());
    }

    /** A store write prepared on the product's road, lowered: its statement, the seedable let prefix before it. A
     *  write under the frame's post-processors is refused by name, as the test road refuses it (the writes design's
     *  leg 2 writes the table it names). */
    public static SqlDml storeWrite(TypedQuery query, Prepared.StoreWrite w, boolean temporal,
            List<SqlExpr.PlanParam> slots, ObjectSites sites) {
        PlanEnv env = w.env();
        if (!env.tableReplace().isEmpty() || env.postProcessors().extractCtes()
                || env.postProcessors().nonExecutable()) {
            throw new NotImplementedException("a write into a store table under the execution's"
                    + " post-processors (tableReplace, extractCtes, nonExecutable) is not designed (the writes design's"
                    + " leg 2 writes the table it names)");
        }
        List<TypedSpec> statements = new ArrayList<>(w.body().subList(0, w.body().size() - 1));
        statements.add(w.write());
        return lowerer(query, statements, env, temporal, false, slots, sites).lowerStoreWrite(
                seeded(statements, env, query.context()));
    }

    /** {@code plan} post-processed by {@code env}'s frame (its connection's table renames and CTE extraction). */
    public static SqlQuery postProcessed(SqlQuery plan, PlanEnv env) {
        return SqlPostProcessors.applyRecorded(plan, env.tableReplace(),
                env.postProcessors().extractCtes(), env.postProcessors().nonExecutable());
    }

    /** The product plans no test-data generator: its plan text is a test's ({@code PlanEnv}'s generator hook). */
    public static String noGenerator(TypedTestDataGen generator) {
        throw new NotImplementedException("a test-data generator in a query: the generator runs in"
                + " a test's program, and the product's query door plans none");
    }
}
