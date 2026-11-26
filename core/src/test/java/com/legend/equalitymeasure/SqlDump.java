// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.equalitymeasure;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.json.Json;
import com.legend.compiler.element.ModelContext;
import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.StorelessRuntime;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * THE PRODUCT'S SQL, DUMPED (tooling for the equality program; tools/equality-measure/README.md): every query of the
 * product's measured sets planned by the product's one planning entry ({@code TypedQuery.executionPlan}, lite's own
 * door and comparison setting, no edge style), each one's SQL statements or its refusal's first line, one row of
 * {@code sql.tsv}. {@code channel_a.jsonl} is planned on every in-process database (its models declare no store);
 * {@code core.jsonl} on the runtime each query was captured with, as {@code LeanSqlBaseline.measureOwn} plans it.
 *
 * <p>Args: {@code <lean-sql dir> <out dir>}.
 */
public final class SqlDump {

    private static final List<DatabaseType> DATABASES = com.legend.testbackend.TestBackends.inProcess().stream()
            .map(com.legend.testbackend.TestBackend::type).toList();

    private SqlDump() {
    }

    public static void main(String[] args) throws IOException {
        Path dir = Path.of(args[0]);
        Path out = Path.of(args[1]);
        Files.createDirectories(out);
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out.resolve("sql.tsv"), StandardCharsets.UTF_8))) {
            w.println("set\tindex\tdatabase\tquery\toutcome\ttext");
            channelA(dir.resolve("channel_a.jsonl"), w);
            core(dir.resolve("core.jsonl"), w);
        }
        System.exit(0);
    }

    private static void channelA(Path file, PrintWriter w) throws IOException {
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        for (DatabaseType type : DATABASES) {
            Map<String, Object> compiled = new HashMap<>();
            int index = 0;
            for (String line : lines) {
                if (line.isBlank()) {
                    continue;
                }
                Json.Obj c = Json.parseObject(line);
                String model = ((Json.Str) c.get("model")).value();
                String expression = ((Json.Str) c.get("expression")).value();
                String[] outcome;
                Object ctx = compiled.computeIfAbsent(model, m -> {
                    try {
                        return Compiler.compileModel(StorelessRuntime.with(m, type));
                    } catch (RuntimeException e) {
                        return e;
                    }
                });
                if (ctx instanceof RuntimeException e) {
                    outcome = refused(e);
                } else {
                    outcome = plan((ModelContext) ctx, expression, StorelessRuntime.RUNTIME, true);
                }
                w.println(String.join("\t", "channel_a", String.valueOf(index++), type.name(), esc(expression),
                        outcome[0], esc(outcome[1])));
            }
        }
    }

    private static void core(Path file, PrintWriter w) throws IOException {
        Map<Long, String> models = new HashMap<>();
        Map<Long, Object> compiled = new HashMap<>();
        int index = 0;
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }
            Json.Obj c = Json.parseObject(line);
            long id = ((Json.Num) c.get("model_id")).longValue();
            if (c.has("model")) {
                models.put(id, ((Json.Str) c.get("model")).value());
                continue;
            }
            String query = ((Json.Str) c.get("query")).value();
            String runtime = ((Json.Str) c.get("runtime")).value();
            Object ctx = compiled.computeIfAbsent(id, k -> {
                try {
                    return Compiler.compileModel(models.get(k));
                } catch (RuntimeException e) {
                    return e;
                }
            });
            String[] outcome = ctx instanceof RuntimeException e ? refused(e)
                    : plan((ModelContext) ctx, query, runtime.isEmpty() ? null : runtime, false);
            w.println(String.join("\t", "core", String.valueOf(index++), outcome.length > 2 ? outcome[2] : "-",
                    esc(query), outcome[0], esc(outcome[1])));
        }
    }

    /** {outcome, text, database}: "SQL" and the plan's statements joined by a line of {@code ;;}, or "REFUSED" /
     *  "ERROR" and the throwable's class and first line. */
    private static String[] plan(ModelContext ctx, String query, @com.legend.base.Nullable String runtime,
            boolean substring) {
        try {
            TypedQuery q = Compiler.query(ctx, query);
            ExecutionPlan plan = substring
                    ? q.executionPlan(runtime, Set.of(com.legend.platform.Feature.CORRECT_SQL_SUBSTRING_INDEXING))
                    : q.executionPlan(runtime);
            List<String> statements = new ArrayList<>();
            com.legend.leansql.LeanSqlBaseline.statements(plan.root(), statements);
            String database = plan.answers() && plan.sendsTheAnswer()
                    ? plan.answerSql().target().database().type().name() : "constant";
            return new String[] {statements.isEmpty() ? "CONSTANT" : "SQL", String.join("\n;;\n", statements),
                database};
        } catch (RuntimeException e) {
            return refused(e);
        }
    }

    private static String[] refused(RuntimeException e) {
        String kind = com.legend.leansql.LeanSqlBaseline.isRefusal(e) ? "REFUSED" : "ERROR";
        String m = String.valueOf(e.getMessage());
        int nl = m.indexOf('\n');
        return new String[] {kind, e.getClass().getSimpleName() + ": " + (nl < 0 ? m : m.substring(0, nl)), "refused"};
    }

    private static String esc(String s) {
        return s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "");
    }
}
