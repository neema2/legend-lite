// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.exec.ExecutionResult;

/**
 * The TWO faces of {@code setUpDataSQLs} (engine toDDL.pure): the
 * ASSERT surface carries the engine's H2 statement TEXT verbatim
 * ({@link com.legend.exec.Ddl#setUpDataSqlsText}); the EXECUTION surface
 * keeps the DuckDB-safe {@link com.legend.exec.CsvSeed} forms — the
 * engine runs its text on EPHEMERAL per-connection H2 dbs, while our
 * DuckDB catalog is SHARED across a family's seeds, where the text's
 * schema cascades would destroy sibling seeds.
 */
final class SeedSqlForms {

    private SeedSqlForms() {
    }

    private static boolean isSetUpDataSqls(
            com.legend.compiler.spec.typed.TypedNativeCall c) {
        return com.legend.compiler.element.type.PlatformTypes.SET_UP_DATA_SQLS_V2
                        .equals(c.callee().qualifiedName())
                || com.legend.compiler.element.type.PlatformTypes.SET_UP_DATA_SQLS
                        .equals(c.callee().qualifiedName());
    }

    /** The ENGINE-TEXT statement list (golden-asserted). Unresolvable
     *  db keeps the CsvSeed execution form (nothing asserts its text). */
    static ExecutionResult assertForm(java.util.List<TypedSpec> body,
            com.legend.compiler.spec.typed.TypedNativeCall gen,
            StatementExecutor.ExecEnv env) {
        java.util.List<java.util.List<String>> records =
                recordsArg(gen.args().get(0));
        if (records != null) {
            String dbFqn2 = gen.args().get(1)
                    instanceof com.legend.compiler.spec.typed.TypedPackageableRef p2
                    ? p2.fullPath() : null;
            var db2 = dbFqn2 == null ? null
                    : env.ctx().findDatabase(dbFqn2).orElse(null);
            if (db2 != null) {
                return new ExecutionResult.Collection(new java.util.ArrayList<>(
                        com.legend.exec.Ddl.setUpDataSqlsTextFromRecords(
                                records, db2,
                                f -> env.ctx().findDatabase(f), StatementExecutor.ENGINE_TEXT)),
                        com.legend.compiler.element.type.Type.Primitive.STRING);
            }
        }
        String csv = StatementExecutor.evalStringArg(body, gen.args().get(0), env);
        String dbFqn = gen.args().get(1)
                instanceof com.legend.compiler.spec.typed.TypedPackageableRef pr
                ? pr.fullPath() : null;
        var dbDef = dbFqn == null ? null
                : env.ctx().findDatabase(dbFqn).orElse(null);
        return new ExecutionResult.Collection(new java.util.ArrayList<>(
                dbDef != null ? com.legend.exec.Ddl.setUpDataSqlsText(csv,
                        dbDef, f -> env.ctx().findDatabase(f), StatementExecutor.ENGINE_TEXT)
                        : com.legend.exec.CsvSeed.sqls(csv, dbFqn, env.ctx(), env.dialect())),
                com.legend.compiler.element.type.Type.Primitive.STRING);
    }

    /** The literal records shape — a collection of {@code list([...])}
     *  calls over string literals; null for anything else (the string
     *  form takes over). */
    private static java.util.@com.legend.base.Nullable List<java.util.List<String>>
            recordsArg(TypedSpec arg) {
        java.util.List<TypedSpec> els =
                arg instanceof com.legend.compiler.spec.typed.TypedCollection tc
                        ? tc.elements() : java.util.List.of(arg);
        java.util.List<java.util.List<String>> out = new java.util.ArrayList<>();
        for (TypedSpec el : els) {
            if (!(el instanceof com.legend.compiler.spec.typed.TypedNativeCall lc)
                    || !"meta::pure::functions::collection::list"
                            .equals(lc.callee().qualifiedName())
                    || lc.args().size() != 1) {
                return null;
            }
            TypedSpec v = lc.args().get(0);
            java.util.List<TypedSpec> cells =
                    v instanceof com.legend.compiler.spec.typed.TypedCollection vc
                            ? vc.elements() : java.util.List.of(v);
            java.util.List<String> row = new java.util.ArrayList<>();
            for (TypedSpec c : cells) {
                if (!(c instanceof com.legend.compiler.spec.typed.TypedCString cs)) {
                    return null;
                }
                row.add(cs.value());
            }
            out.add(row);
        }
        return out;
    }

    /** The EXECUTION form for a mapped {@code ->map(executeInDb)} source;
     *  null when the source is not a setUpDataSQLs call. */
    static ExecutionResult.@com.legend.base.Nullable Collection mappedExecutionForm(
            java.util.List<TypedSpec> body,
            com.legend.compiler.spec.typed.TypedMap tm,
            StatementExecutor.ExecEnv env) {
        if (!(tm.source() instanceof com.legend.compiler.spec.typed.TypedNativeCall sg)
                || !isSetUpDataSqls(sg)) {
            return null;
        }
        String seedCsv = StatementExecutor.evalStringArg(body, sg.args().get(0), env);
        String seedDb = sg.args().get(1)
                instanceof com.legend.compiler.spec.typed.TypedPackageableRef spr
                ? spr.fullPath() : null;
        return new ExecutionResult.Collection(new java.util.ArrayList<>(
                com.legend.exec.CsvSeed.sqls(seedCsv, seedDb, env.ctx(), env.dialect())),
                com.legend.compiler.element.type.Type.Primitive.STRING);
    }
}
