// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.GeneratorSplice;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTestDataGen;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.setup.CsvSeed;
import com.legend.sql.SqlQuery;
import com.legend.sql.dialect.SqlDialect;
import com.legend.testdatagen.TestDataCensus;
import com.legend.testdatagen.TestDataGenerationNatives;
import com.legend.testdatagen.TestDataGenerator;
import com.legend.testdatagen.TestDataPlanner;

import java.util.ArrayList;
import java.util.List;

/**
 * THE TEST-DATA GENERATOR ON THE PROGRAM ROAD (B2d.2 of the total-wall program, 2026-11-11; the design the user approved
 * on 2026-11-10, docs/data-delivery-homework/TEST_DATA_GENERATOR_DESIGN_2026_11_10.md): a test body's
 * {@code generateTestData(...)} PLANNED before the body is classed, by {@link TestDataPlanner} -- one read-only statement
 * whose one cell is the test-data text, and one text per fetch, each written in the body's dialect -- and its reads
 * spliced by the compiler ({@link GeneratorSplice}), so the body is classed and planned as any other. Planning only:
 * nothing is sent, no value evaluated.
 */
final class GeneratedTestData {

    private GeneratedTestData() {
    }

    /** {@code given} with each census replaced by its constant and each test-data generator it binds planned, or as
     *  it is where it holds neither;
     *  statements written in {@code dialect}, the body's, whose runtime is {@code runtimeFqn}; a timestamp printed as
     *  {@code printing} says (the body's judging rules: L9, 2026-11-12). */
    static TypedQuery planned(TypedQuery given, SqlDialect dialect, @com.legend.base.Nullable String runtimeFqn,
            TestDataPlanner.Printing printing) {
        TypedQuery typed = withSeedStrings(withCensus(given), runtimeFqn);
        if (!GeneratorSplice.holdsAGenerator(typed.body())) {
            return typed;
        }
        ModelContext ctx = typed.context();
        List<TypedSpec> body = typed.body();
        return typed.withBody(GeneratorSplice.spliced(body, ctx, typed.specs(), new GeneratorSplice.Generator() {
            @Override
            public GeneratorSplice.Generated plan(TypedTestDataGen g) {
                return generated(g, typed, dialect, body, runtimeFqn, printing);
            }

            @Override
            public GeneratorSplice.Same same(GeneratorSplice.Generated generated, String expected, String dbFqn,
                    String lead) {
                return compared(((Generated) generated).planned(), expected, ctx, dbFqn, lead);
            }

            @Override
            public GeneratorSplice.Slice slice(GeneratorSplice.Generated generated, String dbFqn) {
                return new GeneratorSplice.Slice(TestDataPlanner.reads(ctx, dbFqn),
                        TestDataPlanner.slice(((Generated) generated).planned(), ctx, dbFqn));
            }
        }));
    }

    /** {@code typed} with each seed string ({@code generateSeedDataString}, the checker's {@code seedString}
     *  carrier) the planned statement whose one cell is its text ({@link TestDataPlanner#seedString}, B2d.3): the seed
     *  of its query over its mapping on the body's runtime, a root table only (upstream's seed string refuses a view
     *  root, TDG:222); the query's parameters beyond none refused by name. */
    private static TypedQuery withSeedStrings(TypedQuery typed, @com.legend.base.Nullable String runtimeFqn) {
        List<TypedSpec> body = new ArrayList<>(typed.body().size());
        boolean changed = false;
        // each seed string's planned value, by identity: a database effect between its let and a read of it is
        // refused by name (the B2d.3 audit's M5; read where read, S196)
        java.util.Set<TypedSpec> planned = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        for (TypedSpec s : typed.body()) {
            TypedSpec r = seedStrings(s, typed, runtimeFqn, planned);
            changed |= r != s;
            body.add(r);
        }
        GeneratorSplice.refuseEffectsBeforeAReadOf(body, planned, typed.specs());
        return changed ? typed.withBody(body) : typed;
    }

    private static TypedSpec seedStrings(TypedSpec n, TypedQuery typed, @com.legend.base.Nullable String runtimeFqn,
            java.util.Set<TypedSpec> planned) {
        if (n instanceof TypedTestDataGen g && "seedString".equals(g.flavor())) {
            List<ValueSpecification> ps = g.params();
            theBodysRuntime(ps.get(2), typed.body(), runtimeFqn);
            if (ps.size() > 4 && !(ps.get(4) instanceof com.legend.protocol.spec.PureCollection c
                    && c.values().isEmpty())) {
                throw new com.legend.error.NotImplementedException("generateSeedDataString with the query's parameters"
                        + " bound: not planned (B2d.3 seeds a query with none)");
            }
            LambdaFunction query = (LambdaFunction) ps.get(0);
            String mapping = ((PackageableElementPtr) ps.get(1)).fullPath();
            TestDataPlanner.Seed seed = TestDataPlanner.seed(typed.context(), query, mapping, false,
                    queries(typed, mapping, runtimeFqn));
            TypedSpec value = GeneratorSplice.plannedValue(TestDataPlanner.seedString(typed.context(), seed), g.info());
            planned.add(value);
            return value;
        }
        List<TypedSpec> kids = n.children();
        if (kids.isEmpty()) {
            return n;
        }
        List<TypedSpec> next = new ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec r = seedStrings(k, typed, runtimeFqn, planned);
            changed |= r != k;
            next.add(r);
        }
        return changed ? n.withChildren(next) : n;
    }

    /** The user's query lowered as a product query on the body's runtime, read through {@code mapping} whatever
     *  mappings that runtime declares ({@link TestDataPlanner#underMapping}, as upstream executes the seed's function
     *  with the given mapping): typed, inlined, resolved and lowered by the planner's one entry for a seed
     *  ({@link com.legend.planner.ProductStatements#lowerTestDataSeed}, which refuses raw SQL by name; the seed carries
     *  its keys through it). */
    static TestDataPlanner.Queries queries(TypedQuery typed, String mapping,
            @com.legend.base.Nullable String runtimeFqn) {
        return q -> {
            String runtime = java.util.Objects.requireNonNull(runtimeFqn,
                    "the seed's query is planned on the body's runtime, and the body names none");
            return com.legend.planner.ProductStatements.lowerTestDataSeed(com.legend.Compiler.query(typed.context(),
                    TestDataPlanner.underMapping(q, mapping, runtime, com.legend.Compiler::parseQuery)), runtime);
        };
    }

    /** {@code typed} with each test-data census ({@code getRelationalCSVDataFromQuery}, the checker's
     *  {@link com.legend.compiler.spec.typed.TypedCsvCensus}) the instance it is, a planning constant
     *  ({@link TestDataCensus}, B2d.3): a model fact, nothing executed. */
    private static TypedQuery withCensus(TypedQuery typed) {
        List<TypedSpec> body = new ArrayList<>(typed.body().size());
        boolean changed = false;
        for (TypedSpec s : typed.body()) {
            TypedSpec r = census(s, typed.context());
            changed |= r != s;
            body.add(r);
        }
        return changed ? typed.withBody(body) : typed;
    }

    private static TypedSpec census(TypedSpec n, ModelContext ctx) {
        if (n instanceof com.legend.compiler.spec.typed.TypedCsvCensus c) {
            return GeneratorSplice.relationalCsvData(TestDataCensus.rows(ctx, c.query(), c.mappingFqn()));
        }
        List<TypedSpec> kids = n.children();
        if (kids.isEmpty()) {
            return n;
        }
        List<TypedSpec> next = new ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec r = census(k, ctx);
            changed |= r != k;
            next.add(r);
        }
        return changed ? n.withChildren(next) : n;
    }

    /** Whether the loads of {@code planned}'s text and of the test-data text {@code expected} on {@code dbFqn} are the
     *  same, as upstream decides it: the expected text read at planning by lite's test-data reader
     *  ({@link CsvSeed#blocks}, quote-aware, each cell as written), the comparison {@link TestDataPlanner#same}'s; a
     *  text the reader refuses ({@link CsvSeed.Malformed}) the comparison's failure, as upstream's reading of it is
     *  (S191). */
    private static GeneratorSplice.Same compared(TestDataPlanner.Planned planned, String expected, ModelContext ctx,
            String dbFqn, String lead) {
        List<TestDataPlanner.Expected> blocks = new ArrayList<>();
        try {
            for (CsvSeed.Block b : CsvSeed.blocks(expected)) {
                blocks.add(new TestDataPlanner.Expected(b.schema(), b.table(), b.header(), b.rows()));
            }
        } catch (CsvSeed.Malformed e) {
            return new GeneratorSplice.Same.Fails(lead + "the expected test data: " + e.getMessage());
        }
        TestDataPlanner.Comparison c = TestDataPlanner.same(planned, blocks, ctx, dbFqn, lead);
        return c.fails() != null ? new GeneratorSplice.Same.Fails(c.fails())
                : new GeneratorSplice.Same.Compared(java.util.Objects.requireNonNull(c.holds()),
                        java.util.Objects.requireNonNull(c.differences()));
    }

    /** One generator planned: its fetches' texts and its text's statement. */
    private record Generated(List<String> sqls, SqlQuery csv, TestDataPlanner.Planned planned)
            implements GeneratorSplice.Generated {
    }

    /** {@code g} planned by {@link TestDataPlanner}: its runtime argument the body's ({@link #theBodysRuntime}), its
     *  arguments after the query, the mapping and the runtime classified (the row identifiers, the milestoning dates,
     *  the hash flag; each a literal, a non-literal refused by name), its fetches' texts written in {@code dialect}. */
    private static Generated generated(TypedTestDataGen g, TypedQuery typed, SqlDialect dialect, List<TypedSpec> body,
            @com.legend.base.Nullable String runtimeFqn, TestDataPlanner.Printing printing) {
        ModelContext ctx = typed.context();
        List<ValueSpecification> ps = g.params();
        theBodysRuntime(ps.get(2), body, runtimeFqn);
        LambdaFunction query = (LambdaFunction) ps.get(0);
        String mapping = ((PackageableElementPtr) ps.get(1)).fullPath();
        List<TestDataGenerator.TableRowIds> rowIds = new ArrayList<>();
        TestDataGenerator.MilestoningDates[] dates = new TestDataGenerator.MilestoningDates[1];
        boolean[] hash = new boolean[1];
        for (int i = 3; i < ps.size(); i++) {
            TestDataGenerationNatives.classifyArg(ps.get(i), rowIds, dates, hash);
        }
        TestDataPlanner.Planned p = TestDataPlanner.plan(ctx, query, mapping, rowIds, dates[0], hash[0],
                views(typed, runtimeFqn, dialect), printing);
        List<String> texts = new ArrayList<>(p.fetches().size());
        for (SqlQuery f : p.fetches()) {
            texts.add(dialect.render(f));
        }
        return new Generated(List.copyOf(texts), p.csv(), p);
    }

    /** A view's own fetch, planned by the planner over the generator's clauses
     *  ({@link com.legend.planner.BodyPlanner#viewFetch}): the body's model and compiler, its runtime, written in the
     *  body's dialect, no session. */
    static TestDataPlanner.Views views(TypedQuery typed, @com.legend.base.Nullable String runtimeFqn,
            SqlDialect dialect) {
        com.legend.planner.PlanEnv env = com.legend.planner.PlanEnv.of(typed.context(), runtimeFqn, dialect,
                java.util.Set.of(), com.legend.planner.ProductStatements::noGenerator);
        return (db, view, reads, tables) -> com.legend.planner.BodyPlanner.viewFetch(db, view, reads, tables,
                typed.specs(), env);
    }

    /**
     * The generator's runtime argument {@code runtime} is the body's ({@code runtimeFqn}), else refused by name (B2d.2's
     * audit, M5): the generator's statement is sent on the body's session, so a runtime that names another element
     * would read another database. A runtime element, named ({@code t::RT}, {@code t::RT.runtimeValue}) or a let's,
     * must be the body's; a runtime the body constructs -- by a call ({@code testRuntime($db)}, upstream's tests), a
     * new instance or a copy -- is the harness's, as {@code execute(...)}'s runtime argument is
     * (ExecuteChainAssembly.chain: its connection content is harness-ambient); any other -- an unbound variable, a let
     * bound more than once -- is refused by name.
     */
    private static void theBodysRuntime(ValueSpecification runtime, List<TypedSpec> body,
            @com.legend.base.Nullable String runtimeFqn) {
        String named = switch (runtime) {
            case PackageableElementPtr p -> p.fullPath();
            case com.legend.protocol.spec.AppliedProperty ap when "runtimeValue".equals(ap.property())
                    && ap.receiver() instanceof PackageableElementPtr p -> p.fullPath();
            case com.legend.protocol.spec.EnumValue e when "runtimeValue".equals(e.value()) -> e.fullPath();
            case com.legend.protocol.spec.AppliedFunction call -> null;
            case com.legend.protocol.spec.NewInstance made -> null;
            case com.legend.protocol.spec.Variable v -> namedBy(v.name(), body);
            default -> throw notTheBodys("a " + runtime.getClass().getSimpleName());
        };
        if (named != null && !named.equals(runtimeFqn)) {
            throw notTheBodys(named + ", not the body's runtime " + runtimeFqn);
        }
    }

    /** The runtime element the body's one let of {@code name} binds, or null where it binds a constructed runtime; a
     *  name the body binds never or more than once refused by name. */
    private static @com.legend.base.Nullable String namedBy(String name, List<TypedSpec> body) {
        TypedSpec value = null;
        for (TypedSpec s : body) {
            if (s instanceof TypedLet let && let.name().equals(name)) {
                if (value != null) {
                    throw notTheBodys("$" + name + ", bound more than once");
                }
                value = let.value();
            }
        }
        return switch (value) {
            case null -> throw notTheBodys("$" + name + ", which no statement of the body binds");
            case com.legend.compiler.spec.typed.TypedPackageableRef r -> r.fullPath();
            case com.legend.compiler.spec.typed.TypedPropertyAccess pa when "runtimeValue".equals(pa.property())
                    && pa.source() instanceof com.legend.compiler.spec.typed.TypedPackageableRef r -> r.fullPath();
            case com.legend.compiler.spec.typed.TypedVariable v -> namedBy(v.name(), body);
            case com.legend.compiler.spec.typed.TypedUserCall call -> null;
            case com.legend.compiler.spec.typed.TypedNativeCall call -> null;
            case com.legend.compiler.spec.typed.TypedNewInstance made -> null;
            case com.legend.compiler.spec.typed.TypedCopyInstance made -> null;
            default -> throw notTheBodys("$" + name + ", a " + value.getClass().getSimpleName());
        };
    }

    private static com.legend.error.NotImplementedException notTheBodys(String what) {
        return new com.legend.error.NotImplementedException("the test-data generator's runtime argument is " + what
                + ": not planned (B2d sends the generator's statement on the body's session, so its runtime must be"
                + " the body's)");
    }
}
