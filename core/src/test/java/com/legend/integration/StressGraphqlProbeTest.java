package com.legend.integration;

import com.legend.compiler.element.ModelContext;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * DEVELOPER EXPLORERS for {@link StressGraphqlTwinsTest}'s findings (run only with {@code -Dgql.probe=true} and the
 * mode's own switch; {@code bazel test //core:core_tests_integration --test_filter=...StressGraphqlProbeTest
 * --nocache_test_results --jvmopt=-Dgql.probe=true --jvmopt=-Dgql.probe.pure=true}):
 * <ul>
 *   <li>{@code gql.probe.pure}: each line of /tmp/stress-pure.txt planned over the stress model (a line
 *       {@code @runtime::Fqn} switches the runtime; stress::RT first), its SQL or its failure with the resolved tree
 *       to /tmp/stress-pure-out.md;</li>
 *   <li>{@code gql.probe.mini}: /tmp/mini.txt -- a model, a line {@code ----}, the runtime's name, one query per
 *       line -- the same, to /tmp/mini-out.md: the small reproduction of a stress finding; a line {@code sql: ...}
 *       runs on one DuckDB connection, and a query line starting {@code !} executes there.</li>
 * </ul>
 */
@org.junit.jupiter.api.condition.EnabledIfSystemProperty(named = "gql.probe", matches = "true")
class StressGraphqlProbeTest {

    @Test
    @org.junit.jupiter.api.condition.EnabledIfSystemProperty(named = "gql.probe.pure", matches = "true")
    void pure() throws Exception {
        var module = com.legend.Compiler.parseSources(StressCorpus.sources(List.of()));
        ModelContext ctx = com.legend.Compiler.buildModel(module.model());
        StringBuilder out = new StringBuilder();
        String runtime = "stress::RT";
        for (String q : Files.readAllLines(Path.of("/tmp/stress-pure.txt"))) {
            if (q.isBlank()) {
                continue;
            }
            if (q.startsWith("@")) {
                runtime = q.substring(1).strip();
                continue;
            }
            plan(ctx, q, runtime, out);
        }
        Files.writeString(Path.of("/tmp/stress-pure-out.md"), out.toString());
    }

    @Test
    @org.junit.jupiter.api.condition.EnabledIfSystemProperty(named = "gql.probe.mini", matches = "true")
    void mini() throws Exception {
        String[] parts = Files.readString(Path.of("/tmp/mini.txt")).split("\n----\n");
        ModelContext ctx = com.legend.Compiler.compileModel(parts[0]);
        List<String> lines = parts[1].lines().filter(l -> !l.isBlank()).toList();
        StringBuilder out = new StringBuilder();
        // `sql: ...` lines run first on one DuckDB connection; `!|query` lines execute there, the rest plan
        try (var c = com.legend.testbackend.TestBackends.platform().open(); var st = c.createStatement()) {
            for (String q : lines.subList(1, lines.size())) {
                if (q.startsWith("sql: ")) {
                    st.execute(q.substring(5));
                } else if (q.startsWith("!")) {
                    out.append("### ").append(q).append("\n");
                    java.io.StringWriter w = new java.io.StringWriter();
                    try {
                        com.legend.Execution.executeWire(parts[0], q.substring(1), lines.get(0), c,
                                com.legend.lowering.WireRender.Format.JSON, w);
                        out.append(w).append("\n");
                    } catch (Exception e) {
                        out.append(e).append("\n");
                    }
                } else {
                    plan(ctx, q, lines.get(0), out);
                }
            }
        }
        Files.writeString(Path.of("/tmp/mini-out.md"), out.toString());
    }

    private static void plan(ModelContext ctx, String q, String runtime, StringBuilder out) {
        out.append("### ").append(q).append("\n");
        try {
            out.append(com.legend.Compiler.query(ctx, q).plan(runtime).sql()).append("\n");
        } catch (RuntimeException e) {
            try {
                var tq = com.legend.Compiler.query(ctx, q);
                var specs = new com.legend.compiler.spec.SpecCompiler(tq.context());
                var b = new com.legend.compiler.spec.UserCallInliner(specs).inlineBody(
                        new java.util.ArrayList<>(tq.body()));
                b = new com.legend.resolver.StoreResolver(tq.context(), specs).resolve(b, runtime);
                out.append("resolved: ").append(b.get(b.size() - 1)).append("\n");
            } catch (RuntimeException e2) {
                out.append("resolve failed: ").append(e2).append("\n");
            }
            java.io.StringWriter sw = new java.io.StringWriter();
            e.printStackTrace(new java.io.PrintWriter(sw));
            String s = sw.toString();
            out.append(s.length() > 6000 ? s.substring(0, 6000) + "..." : s).append("\n");
        }
    }
}
