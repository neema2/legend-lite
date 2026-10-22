// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * THE CAPABILITY MATRIX'S REACH ({@link CapabilityMatrix}): how many PCT cases reach each function and aggregate on
 * each dialect a database type is planned with. Each recorded case (a PCT lane's {@code pct-cases.tsv}, written under
 * {@code -Dlegend.diagnostics=pct-cases}: Base64 model TAB Base64 expression) is planned once, as its lane plans it
 * up to the dialect ({@link #lower}), then put through each dialect's own passes, as that dialect's render sees it;
 * every function and aggregate left in it is reached there. A function no case reaches on a dialect is an arm no PCT test exercises
 * on its database.
 *
 * <p>A measurement, not a build step (it needs the lanes' recorded cases): re-measure when the dialects' passes or the
 * PCT suites move, as docs/dialect-capability/README.md's preamble says, with
 * {@code bazel run //core:capability_reach -- docs/dialect-capability/reach.tsv <cases.tsv>...} (paths from where it
 * is run), then {@code bazel run //docs:update_dialect_capability}.
 */
public final class CapabilityReach {

    private CapabilityReach() {
    }

    public static void main(String[] args) throws Exception {
        Set<String> cases = new LinkedHashSet<>();
        for (int i = 1; i < args.length; i++) {
            cases.addAll(Files.readAllLines(arg(args[i]), StandardCharsets.UTF_8));
        }
        cases.removeIf(String::isBlank);
        Map<String, AnsiSqlRenderer> dialects = new TreeMap<>();
        for (Map.Entry<Class<?>, List<DatabaseType>> d : CapabilityMatrix.databases().entrySet()) {
            dialects.put(d.getKey().getSimpleName(),
                    (AnsiSqlRenderer) com.legend.database.Databases.dialect(d.getValue().get(0)));
        }
        Map<String, Integer> reach = new TreeMap<>();
        Map<String, Integer> refusedByPass = new TreeMap<>();
        Map<String, Object> models = new HashMap<>();
        int lowered = 0;
        for (String line : cases) {
            String[] f = line.split("\t", -1);
            SqlQuery q;
            try {
                q = lower(models, decode(f[0]), decode(f[1]));
            } catch (RuntimeException | StackOverflowError refusedBeforeSql) {
                continue;
            }
            lowered++;
            for (Map.Entry<String, AnsiSqlRenderer> d : dialects.entrySet()) {
                SqlQuery p = q;
                try {
                    for (SqlRewriter pass : d.getValue().renderPasses()) {
                        p = pass.rewriteRoot(p);
                    }
                } catch (RuntimeException | StackOverflowError e) {
                    // a pass refusing the case: it reaches no arm of this dialect
                    refusedByPass.merge(d.getKey(), 1, Integer::sum);
                    continue;
                }
                Recorder r = new Recorder();
                r.rewriteRoot(p);
                for (String member : r.seen) {
                    reach.merge(d.getKey() + "\t" + member, 1, Integer::sum);
                }
            }
        }
        StringBuilder out = new StringBuilder();
        out.append("# The capability matrix's reach (CapabilityReach; docs/dialect-capability/README.md): dialect TAB\n")
                .append("# vocabulary TAB member TAB the recorded PCT cases whose lowering holds it after the\n")
                .append("# dialect's own passes. ").append(cases.size()).append(" distinct cases, ").append(lowered)
                .append(" lowered (the rest refused before SQL).\n");
        for (Map.Entry<String, Integer> e : refusedByPass.entrySet()) {
            out.append("# ").append(e.getKey()).append(": ").append(e.getValue())
                    .append(" cases refused by one of its passes\n");
        }
        for (Map.Entry<String, Integer> e : reach.entrySet()) {
            out.append(e.getKey()).append('\t').append(e.getValue()).append('\n');
        }
        Files.writeString(arg(args[0]), out.toString(), StandardCharsets.UTF_8);
        System.out.println(cases.size() + " cases, " + lowered + " lowered; reach written to " + args[0]);
    }

    /**
     * The case planned as its lane plans it, up to the dialect: on the storeless runtime ({@code StorelessRuntime},
     * the PCT adapter's), through the plan-only seam ({@code TypedQuery.lower}: G½, H, the runtime's checks, I), a
     * relation result wrapped in the PCT wire text ({@code TdsTextWrap}) as the lane wraps it. Not applied: the lane's
     * feature flags and its database-side probes (a dynamic pivot's wrap needs the database's answer, so a pivoting
     * relation stays unwrapped).
     */
    private static SqlQuery lower(Map<String, Object> models, String model, String expression) {
        Object built = models.computeIfAbsent(model, m -> {
            try {
                return Compiler.compileModel(com.legend.test.StorelessRuntime.with(m, DatabaseType.DuckDB));
            } catch (RuntimeException e) {
                return e;
            }
        });
        if (built instanceof RuntimeException e) {
            throw e;
        }
        Compiler.LoweredQuery l = Compiler.query((ModelContext) built, expression)
                .lower(com.legend.test.StorelessRuntime.RUNTIME, false);
        var type = l.root().info().type();
        if (com.legend.compiler.element.type.Type.isRelation(type)
                && com.legend.lowering.TdsTextWrap.pivots(l.plan()).isEmpty()) {
            return com.legend.lowering.TdsTextWrap.wrap(l.plan(),
                    com.legend.compiler.element.type.Type.requireRelationSchema(type), null,
                    com.legend.exec.Executor::pureOfSqlType);
        }
        return l.plan();
    }

    /** Records every function and aggregate a statement holds, changing nothing. */
    private static final class Recorder extends SqlRewriter {
        final Set<String> seen = new LinkedHashSet<>();

        @Override
        protected SqlExpr expr(SqlExpr e) {
            switch (e) {
                case SqlExpr.Call c -> seen.add("call\t" + c.fn().name());
                case SqlAgg.Reducer r -> seen.add("reducer\t" + r.fn().name());
                case SqlExpr.WindowCall w when w.fn() instanceof SqlAgg.Reducer r ->
                        seen.add("reducer\t" + r.fn().name());
                default -> { }
            }
            return super.expr(e);
        }
    }

    private static String decode(String b64) {
        return new String(Base64.getDecoder().decode(b64), StandardCharsets.UTF_8);
    }

    /** A path argument, from the directory {@code bazel run} was started in (it runs the program in its runfiles). */
    private static Path arg(String path) {
        String cwd = System.getenv("BUILD_WORKING_DIRECTORY");
        return cwd == null ? Path.of(path) : Path.of(cwd).resolve(path);
    }
}
