// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.ArrayList;
import java.util.List;

/**
 * A STATEMENT'S SEEDS (D35; the test-is-the-product plan's M3e, brief B12 item 7): the test data a statement's
 * sources name -- each {@code from()}'s inline setups and the runtime it names, and the program's own runtime -- as
 * the plan's setup steps ({@code CsvSeed.planned}, their one owner), with the value they are a pure function of. One
 * owner for the walk ({@code BodyDriver.establishContexts}, until its body classes move) and the test door's program
 * road ({@link TestKinds}: a {@code Setup} step).
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
    public record Sources(List<From> froms, @com.legend.base.Nullable String runtimeFqn,
            @com.legend.base.Nullable DeclaredSeed runtime) {
        public record From(List<String> sqlSetups,
                List<com.legend.compiler.spec.typed.ExecutionContext.CsvSetup> csvSetups,
                @com.legend.base.Nullable DeclaredSeed runtime) {
        }

        public record DeclaredSeed(String runtimeFqn,
                com.legend.model.@com.legend.base.Nullable RuntimeDefinition definition,
                List<com.legend.model.ConnectionDefinition> connections) {
        }

        /** Whether nothing names test data: no from(), no runtime. */
        public boolean namesNothing() {
            return froms.isEmpty() && runtimeFqn == null;
        }
    }

    /** {@code statement}'s seed sources, {@code runtime} the program's. */
    public static Sources sources(TypedSpec statement, @com.legend.base.Nullable String runtime, ModelContext ctx) {
        List<com.legend.compiler.spec.typed.TypedFrom> froms =
                com.legend.compiler.spec.typed.ExecutionContext.froms(statement);
        List<Sources.From> fromSources = new ArrayList<>(froms.size());
        for (com.legend.compiler.spec.typed.TypedFrom fr : froms) {
            String rtFqn = fr.context().runtime().map(r -> r.fullPath()).orElse(null);
            fromSources.add(new Sources.From(fr.sqlSetups(), fr.csvSetups(),
                    rtFqn == null ? null : declaredSeed(rtFqn, ctx)));
        }
        return new Sources(fromSources, runtime, runtime == null ? null : declaredSeed(runtime, ctx));
    }

    /** {@code statement}'s seeds as the plan's setup steps, final for {@code dialect}'s database, each with the text
     *  the referee's ledger records for it. */
    public static List<com.legend.setup.CsvSeed.Planned> planned(TypedSpec statement,
            @com.legend.base.Nullable String runtime, ModelContext ctx, com.legend.sql.dialect.SqlDialect dialect) {
        List<com.legend.compiler.spec.typed.TypedFrom> froms =
                com.legend.compiler.spec.typed.ExecutionContext.froms(statement);
        List<com.legend.setup.CsvSeed.Step> setups = new ArrayList<>();
        for (com.legend.compiler.spec.typed.TypedFrom fr : froms) {
            for (String sql : fr.sqlSetups()) {
                // written in the database the from()'s connection declares (decision D2)
                setups.add(new com.legend.setup.CsvSeed.Step.Sql(sql, fr.context().rawLanguage()));
            }
            setups.addAll(com.legend.setup.CsvSeed.setupSteps(fr, ctx, dialect));
            // an ELEMENT runtime named by the from(): its declared connections' test data (the instance form above
            // carries the same facts on the instance)
            fr.context().runtime().ifPresent(ref ->
                    setups.addAll(com.legend.setup.CsvSeed.declaredSteps(ref.fullPath(), ctx, dialect)));
        }
        // the program's runtime: its declared connections' test data seeds the session the same way
        if (runtime != null) {
            setups.addAll(com.legend.setup.CsvSeed.declaredSteps(runtime, ctx, dialect));
        }
        return com.legend.setup.CsvSeed.planned(setups, dialect);
    }

    /** The seeds' name in a plan: their content (the steps' texts), so equal seeds share it. */
    public static String key(List<com.legend.setup.CsvSeed.Planned> planned) {
        StringBuilder sb = new StringBuilder();
        for (com.legend.setup.CsvSeed.Planned p : planned) {
            sb.append(p.text()).append('\u0000');
        }
        return "seeds:" + com.legend.cache.Hash.ofUtf8(sb.toString()).hex();
    }

    /** A named runtime's contribution to the seed sources. */
    private static Sources.DeclaredSeed declaredSeed(String runtimeFqn, ModelContext ctx) {
        java.util.Optional<com.legend.model.RuntimeDefinition> rt = ctx.findRuntime(runtimeFqn);
        List<com.legend.model.ConnectionDefinition> conns = new ArrayList<>();
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
