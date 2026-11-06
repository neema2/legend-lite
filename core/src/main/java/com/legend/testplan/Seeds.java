// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.base.Nullable;
import com.legend.builtin.NativeFn;
import com.legend.cache.Hash;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.spec.typed.ExecutionContext;
import com.legend.compiler.spec.typed.Literals;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition;
import com.legend.model.RuntimeDefinition;
import com.legend.setup.CsvSeed;
import com.legend.sql.dialect.SqlDialect;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A STATEMENT'S SEEDS (D35; the test-is-the-product plan's M3e, brief B12 item 7): the test data a statement's
 * sources name -- each {@code from()}'s inline setups and the runtime it names, and the program's own runtime -- as
 * the plan's setup steps ({@code CsvSeed.planned}, their one owner), with the value they are a pure function of. One
 * owner for the walk ({@code BodyDriver.establishContexts}, until its body classes move) and the test door's program
 * road ({@link TestKinds}: a {@code Setup} step). Since M10c item 7c also the one reader of a SEED FORM mapped over an
 * effect ({@link #seedForm}, {@link #oneEffectMapper}: the corpus's setup helpers, unfolded into the body), for the
 * walk's {@code SeedSqlForms} and the test door's effect script alike.
 *
 * <p>ONCE PER SESSION: the engine runs a LocalH2 connection's {@code testDataSetupSqls} / {@code testDataSetupCsv}
 * when it ESTABLISHES the connection, not before every query. A session that already established exactly these
 * sources, and has run no WRITING statement since, is in the established state; a write in between re-seeds. THE KEY
 * IS THE SEED'S SOURCES, never its rendered text (2026-09-16: keying by the text meant regenerating it before EVERY
 * statement only to find the session already seeded; the DuckDB shared lane went 20.7 s to 50 s at the DDL leg): the
 * from()s' inline setup values and the named runtimes' definitions with their connections' definitions, RECORDS
 * compared by value -- never the model's identity (the test runner overlays a fresh model per test over the same
 * data). A plan names its seeds by their content ({@link #key}), so a plan stays one value that a runner can hold.
 */
public final class Seeds {

    private Seeds() {
    }

    /** WHAT A SEED IS MADE OF: each from()'s inline setups and the runtime it names; the program's runtime. A
     *  runtime contributes its definition and its bound connections' definitions (a LocalH2 specification carries
     *  the CSV / SQL strings as written). */
    public record Sources(List<From> froms, @Nullable String runtimeFqn,
            @Nullable DeclaredSeed runtime) {
        public record From(List<String> sqlSetups,
                List<ExecutionContext.CsvSetup> csvSetups,
                @Nullable DeclaredSeed runtime) {
        }

        public record DeclaredSeed(String runtimeFqn,
                com.legend.model.@Nullable RuntimeDefinition definition,
                List<ConnectionDefinition> connections) {
        }

        /** Whether nothing names test data: no from(), no runtime. */
        public boolean namesNothing() {
            return froms.isEmpty() && runtimeFqn == null;
        }
    }

    /** {@code statement}'s seed sources, {@code runtime} the program's. */
    public static Sources sources(TypedSpec statement, @Nullable String runtime, ModelContext ctx) {
        List<TypedFrom> froms =
                ExecutionContext.froms(statement);
        List<Sources.From> fromSources = new ArrayList<>(froms.size());
        for (TypedFrom fr : froms) {
            String rtFqn = fr.context().runtime().map(r -> r.fullPath()).orElse(null);
            fromSources.add(new Sources.From(fr.sqlSetups(), fr.csvSetups(),
                    rtFqn == null ? null : declaredSeed(rtFqn, ctx)));
        }
        return new Sources(fromSources, runtime, runtime == null ? null : declaredSeed(runtime, ctx));
    }

    /** {@code statement}'s seeds as the plan's setup steps, final for {@code dialect}'s database, each with the text
     *  the referee's ledger records for it. */
    public static List<CsvSeed.Planned> planned(TypedSpec statement,
            @Nullable String runtime, ModelContext ctx, SqlDialect dialect) {
        List<TypedFrom> froms =
                ExecutionContext.froms(statement);
        List<CsvSeed.Step> setups = new ArrayList<>();
        for (TypedFrom fr : froms) {
            for (String sql : fr.sqlSetups()) {
                // written in the database the from()'s connection declares (decision D2)
                setups.add(new CsvSeed.Step.Sql(sql, fr.context().rawLanguage()));
            }
            setups.addAll(CsvSeed.setupSteps(fr, ctx, dialect));
            // an ELEMENT runtime named by the from(): its declared connections' test data (the instance form above
            // carries the same facts on the instance)
            fr.context().runtime().ifPresent(ref ->
                    setups.addAll(CsvSeed.declaredSteps(ref.fullPath(), ctx, dialect)));
        }
        // the program's runtime: its declared connections' test data seeds the session the same way
        if (runtime != null) {
            setups.addAll(CsvSeed.declaredSteps(runtime, ctx, dialect));
        }
        return CsvSeed.planned(setups, dialect);
    }

    /**
     * A SEED FORM mapped over an effect (the test-is-the-product plan's M10c item 7c; M9's T9 kind): the source of
     * {@code map} is {@code setUpDataSQLs(...)} or {@code setUpDataSQLsV2(...)}, its data argument folds to a String
     * over {@code lets} ({@link Literals#fold}: literals joined by {@code +} and {@code joinStrings}), its store argument
     * is read as a reference's name or as none -- exactly as the walk reads it ({@code SeedSqlForms}); null where the
     * data does not fold (the walk evaluates it as a statement there; the program road refuses by name). One reader
     * for both roads: the texts are {@link CsvSeed#sqls} on the program's dialect.
     */
    public static @Nullable SeedForm seedForm(TypedMap map, List<TypedSpec> lets) {
        // the source as spelled or through the let that binds it ($sqls, the unfolded helper's): the walk sees the
        // call once its lets are dissolved; the class rule and the test door read the body before that
        return seedFormCall(com.legend.compiler.spec.typed.Lets.bound(map.source(), lets), lets);
    }

    /** {@link #seedForm(TypedMap, List)} for the form's call itself ({@code let sqls = csv->setUpDataSQLs(db)}: a value
     *  let whose texts are planning-time constants), or null where {@code value} is not one or its data does not fold. */
    public static @Nullable SeedForm seedFormCall(TypedSpec value, List<TypedSpec> lets) {
        if (!(value instanceof TypedNativeCall sg) || sg.args().size() < 2
                || !(PlatformTypes.SET_UP_DATA_SQLS.equals(sg.callee().qualifiedName())
                        || PlatformTypes.SET_UP_DATA_SQLS_V2.equals(sg.callee().qualifiedName()))) {
            return null;
        }
        if (!(Literals.fold(sg.args().get(0), lets) instanceof String csv)) {
            return null;
        }
        // the store as spelled or through the let that binds it: the walk reads the call once its lets are dissolved
        // (a reference), the class rule and the test door before that (a variable) -- one reading
        String dbFqn = com.legend.compiler.spec.typed.Lets.bound(sg.args().get(1), lets) instanceof TypedPackageableRef pr
                ? pr.fullPath() : null;
        return new SeedForm(csv, dbFqn);
    }

    /** A seed form's folded data and its store's name (none where the store is a variable: the walk's reading). */
    public record SeedForm(String csv, @Nullable String dbFqn) {
        /** The statements the form spells, in the program's dialect. */
        public List<String> texts(ModelContext ctx, SqlDialect dialect) {
            return CsvSeed.sqls(csv, dbFqn, ctx, dialect);
        }
    }

    /**
     * A DDL TEXT COLLECTION mapped over an effect ({@code [dropSchemaStatement('s'), createTableStatement(db, 's', 't'),
     * ...]->map(s | executeInDb($s, $connection))}: the aggregation-aware package's setUp, the corpus burn-down of
     * 2026-11-06): the map's source, as spelled or through the let that binds it, a literal collection whose every
     * element is a text at planning -- a toDDL string generator over literal arguments (the planner spells it from the
     * compiled store, {@code EnginePlans.ddlText}: build-time model text, a typed constant) or a String literal folded
     * over {@code lets}. Its elements, or null where it is not one.
     */
    public static @Nullable List<TypedSpec> ddlTextSource(TypedMap map, List<TypedSpec> lets) {
        if (!(com.legend.compiler.spec.typed.Lets.bound(map.source(), lets)
                instanceof com.legend.compiler.spec.typed.TypedCollection coll) || coll.elements().isEmpty()) {
            return null;
        }
        for (TypedSpec e : coll.elements()) {
            if (e instanceof TypedNativeCall call && NativeFn.DdlStatement.of(call.callee().id()).isPresent()) {
                // the generator's arguments as it reads them: the store a reference, every other a String literal
                for (TypedSpec a : call.args()) {
                    if (!(a instanceof TypedPackageableRef) && !(a instanceof com.legend.compiler.spec.typed.TypedCString)) {
                        return null;
                    }
                }
            } else if (!(Literals.fold(e, lets) instanceof String)) {
                return null;
            }
        }
        return coll.elements();
    }

    /** {@link #ddlTextSource}'s elements spelled at planning, in order: a generator's text from the compiled store
     *  (the engine's own spelling, as the walk's {@code Prepared.DdlTexts} sends it), a literal folded. */
    public static List<String> ddlTexts(List<TypedSpec> elements, List<TypedSpec> lets, ModelContext ctx) {
        com.legend.planner.PlanEnv env = com.legend.planner.ProductStatements.env(ctx, null, java.util.Set.of(),
                java.util.Map.of());
        List<String> texts = new ArrayList<>(elements.size());
        for (TypedSpec e : elements) {
            if (e instanceof TypedNativeCall call && NativeFn.DdlStatement.of(call.callee().id()).isPresent()) {
                texts.add(com.legend.planner.EnginePlans.ddlText(call, env));
            } else {
                texts.add((String) java.util.Objects.requireNonNull(Literals.fold(e, lets)));
            }
        }
        return texts;
    }

    /** Whether {@code map}'s lambda is ONE db-effect statement the test door writes, over its one parameter ({@code sql |
     *  executeInDb($sql, $connection)}): the shape the seed-form map runs (item 7c), by the rule a literal effect
     *  statement is admitted by ({@code BodyClass.literalEffect}, the audit's B): not {@code loadCsvToDbTable} (its CSV a
     *  test resource), every String argument the parameter itself or a literal folded over {@code lets}, and the
     *  parameter read nowhere else (the binder, {@code UserCallInliner.bindStringParam}, replaces it at an argument's
     *  root alone). */
    public static boolean oneEffectMapper(TypedMap map, List<TypedSpec> lets) {
        if (map.mapper().parameters().size() != 1 || map.mapper().body().size() != 1
                || !(map.mapper().body().get(0) instanceof TypedNativeCall c)
                || !NativeFn.Effect.isDbEffect(c.callee().id())
                || NativeFn.Effect.of(c.callee().id()).orElse(null) == NativeFn.Effect.LOAD_CSV_TO_DB_TABLE) {
            return false;
        }
        String param = map.mapper().parameters().get(0);
        List<TypedSpec> stringArgs = EffectPlanner.stringArgs(c);
        for (TypedSpec arg : stringArgs) {
            boolean theParameter = arg instanceof com.legend.compiler.spec.typed.TypedVariable v && param.equals(v.name());
            if (!theParameter && !(Literals.fold(arg, lets) instanceof String)) {
                return false;
            }
        }
        for (TypedSpec arg : c.args()) {
            if (!stringArgs.contains(arg) && com.legend.compiler.spec.UserCallInliner.referencesVar(arg, param)) {
                return false;
            }
        }
        return true;
    }

    /** The seeds' name in a plan: the content of the steps the session runs (a step's executed text, a load's rows),
     *  so equal seeds share it and seeds that run differently never do (the M3 audit: the authored text alone named
     *  two languages' runs of one text alike). */
    public static String key(List<ExecutionPlan.SetupStep> steps) {
        // each field length-prefixed, a cell's absence marked (the design audit's H1, 2026-11-02: a record's toString
        // joins fields with no escape, so two different seeds could spell one key and the second be skipped)
        StringBuilder sb = new StringBuilder();
        for (ExecutionPlan.SetupStep step : steps) {
            switch (step) {
                case ExecutionPlan.SetupStep.Statement st -> field(sb.append('S'), st.sql());
                case ExecutionPlan.SetupStep.Rows r -> {
                    field(field(field(field(sb.append('R'), r.stagingTable()), r.createStaging()), r.copy()),
                            r.dropStaging());
                    sb.append(r.rows().size()).append(':');
                    for (List<String> row : r.rows()) {
                        sb.append(row.size()).append(':');
                        for (String cell : row) {
                            if (cell == null) {
                                sb.append('N');
                            } else {
                                field(sb.append('V'), cell);
                            }
                        }
                    }
                }
            }
        }
        return "seeds:" + Hash.ofUtf8(sb.toString()).hex();
    }

    /** {@code text} into {@code sb} prefixed by its length: no two field sequences spell one string. */
    private static StringBuilder field(StringBuilder sb, String text) {
        return sb.append(text.length()).append(':').append(text);
    }

    /** A named runtime's contribution to the seed sources. */
    private static Sources.DeclaredSeed declaredSeed(String runtimeFqn, ModelContext ctx) {
        Optional<RuntimeDefinition> rt = ctx.findRuntime(runtimeFqn);
        List<ConnectionDefinition> conns = new ArrayList<>();
        rt.ifPresent(r -> {
            for (var binding : r.connectionBindings().entrySet()) {
                for (String connFqn : binding.getValue()) {
                    ctx.findConnection(connFqn).ifPresent(conns::add);
                }
            }
        });
        return new Sources.DeclaredSeed(runtimeFqn, rt.orElse(null), conns);
    }
}
