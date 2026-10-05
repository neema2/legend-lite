// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.exec.ExecutionResult;
import com.legend.exec.Executor;

import java.util.List;

/**
 * The {@code loadCsvToDbTable(filePath, table, connection)} EFFECT arm
 * (batch 85): the engine's native (legend-pure LoadCsvToDbTable) reads
 * the classpath CSV, DROPS its header row and inserts every row
 * positionally into the table's columns (a value that does not fit the
 * column's type is its own loud error there; here the database's). The
 * table is the store navigation the call names
 * ({@code db->schema('s')->toOne()->table('t')->toOne()}); the CSV text is
 * test input the harness resolves (TestResources). Java orchestrates the
 * INSERT statements — the database executes them.
 */
final class CsvLoad {
    private CsvLoad() {
    }

    static ExecutionResult loadCsvToDbTable(List<TypedSpec> body,
            TypedNativeCall call, StatementExecutor.ExecEnv env) {
        String path = StatementExecutor.evalStringArg(body, call.args().get(0), env);
        String[] ref = tableRef(com.legend.compiler.spec.typed.Lets.bound(call.args().get(1), body), body);
        String qualified = "default".equals(ref[1]) ? ref[2] : ref[1] + "." + ref[2];
        var tableType = env.ctx().findTable(ref[0], ref[2]).orElseThrow(() ->
                new com.legend.error.NotImplementedException("loadCsvToDbTable:"
                        + " table '" + ref[2] + "' is not declared in " + ref[0]));
        String[] cols = tableType.columns().stream().map(c -> c.name())
                .toArray(String[]::new);
        var resources = env.options().resources();
        if (resources == null) {
            throw new com.legend.error.NotImplementedException(
                    "test resource '" + path + "': this execution carries no"
                    + " resource resolver (ExecuteOptions.resources)");
        }
        String[] lines = resources.apply(path).split("\r?\n");
        List<String[]> rows = new java.util.ArrayList<>();
        for (int i = 1; i < lines.length; i++) {   // the header row is dropped
            if (lines[i].isBlank()) {
                continue;
            }
            String[] vals = lines[i].split(",", -1);
            if (vals.length != cols.length) {
                throw new IllegalStateException("loadCsvToDbTable: CSV row "
                        + i + " has " + vals.length + " value(s), table "
                        + qualified + " has " + cols.length + " column(s)");
            }
            rows.add(vals);
        }
        // the seed's rows (CsvSeed) — one producer, the dialect spells the
        // insert, the database casts every cell
        com.legend.exec.RowLoad load = com.legend.exec.CsvSeed.rowLoad(env.dialect(),
                "default".equals(ref[1]) ? null : ref[1], ref[2], cols, rows);
        if (load != null) {
            StatementExecutor.sendEffect(env, env.dialect().render(load.values()), null,
                    com.legend.exec.StatementOrigin.SEED_GENERATED, true);
        }
        return new ExecutionResult.Scalar(null, call.info().type());
    }

    /** {@code [dbFqn, schema, table]} of a store-navigation chain
     * ({@code table(schema(db, 's')->toOne(), 't')}), lets chased. */
    private static String[] tableRef(TypedSpec t, List<TypedSpec> body) {
        var r = com.legend.compiler.spec.typed.StoreElementIdentity.tableRef(t, x -> peel(x, body));
        if (r == null) {
            throw new com.legend.error.NotImplementedException("loadCsvToDbTable: the"
                    + " table argument is not a db->schema(...)->table(...) navigation: "
                    + t.getClass().getSimpleName() + " " + String.valueOf(t).substring(0,
                            Math.min(400, String.valueOf(t).length())));
        }
        return new String[]{r.dbFqn(), r.schema(), r.table()};
    }

    private static TypedSpec peel(TypedSpec n, List<TypedSpec> body) {
        TypedSpec cur = com.legend.compiler.spec.typed.Lets.bound(n, body);
        while (cur instanceof TypedNativeCall nc && nc.args().size() == 1
                && com.legend.builtin.Pure.isToOneCall(nc.callee().qualifiedName())) {
            cur = com.legend.compiler.spec.typed.Lets.bound(nc.args().get(0), body);
        }
        return cur;
    }
}
