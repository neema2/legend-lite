// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.json.Json;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * THE EDGE GOLDEN NET's pure/v1 answers (2026-11-16, the edge program's Phase 0, item (d)): a fixed list of execute
 * requests sent in-process as PureV1ApiTest sends them, each answer's status and JSON written, ids masked, under
 * {@code -Dlegend.edge.purev1=<file>}: an error answer for each raise pure/v1 reaches, a cell of each kind, one TDS
 * answer and one value answer -- exactly what the edge's Phases 2 and 3 change. Recorded at PR1's frozen head
 * 26717de291 (//core:update_edge_pure_v1); a difference is a regression unless it is a ruled move.
 */
class PureV1EdgeGoldenTest {

    /** name -> the lambda's grammar, in the file's order: core/src/test/resources/edge/pure_v1_requests.tsv, the
     *  list slice R's plan oracle reads too, so the two cannot drift (B1 of the presentation program, 2026-11-19). */
    private static final Map<String, String> REQUESTS = requests();

    private static Map<String, String> requests() {
        Map<String, String> out = new LinkedHashMap<>();
        try {
            for (String line : resource("edge/pure_v1_requests.tsv").split("\n", -1)) {
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                int tab = line.indexOf('\t');
                if (tab < 0 || out.put(line.substring(0, tab), line.substring(tab + 1)) != null) {
                    throw new IllegalStateException("pure_v1_requests.tsv: not a name, a tab and a lambda, or a name"
                            + " twice: " + line);
                }
            }
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return out;
    }

    private static String resource(String name) throws IOException {
        try (InputStream in = PureV1EdgeGoldenTest.class.getClassLoader().getResourceAsStream(name)) {
            if (in == null) {
                throw new IOException("no resource " + name);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static final java.util.regex.Pattern IDS = java.util.regex.Pattern.compile(
            "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

    /** The runner the net records pure/v1 through: the SHIPPED route's, the one the server mounts
     *  ({@code edge.EdgeServer.runner}, planned in the edge's dialects; F31, 2026-11-17 -- until then the product's
     *  ConnectionResolver.RUNNER, a road no client of pure/v1 runs). Held so by {@link #theRecorderIsTheShippedRoute}. */
    static final LiteV1Api.Runner RECORDER = com.legend.edge.EdgeServer.runner(null);

    /** THE GUARD (the orchestrator's condition 2 on F31, 2026-11-17): the net records through the shipped edge route
     *  it claims, by type -- the product's runner, a planted violator, is refused. */
    @Test
    void theRecorderIsTheShippedRoute() {
        org.junit.jupiter.api.Assertions.assertTrue(shipped(RECORDER), "the pure/v1 net records the shipped route");
        org.junit.jupiter.api.Assertions.assertFalse(shipped(ConnectionResolver.RUNNER),
                "the product's runner is not the shipped edge route: a recorder running it records a road no pure/v1"
                        + " client runs");
    }

    private static boolean shipped(LiteV1Api.Runner runner) {
        return runner instanceof com.legend.edge.EdgeServer.Shipped;
    }

    private static String answer(java.util.function.Supplier<LiteV1Api.Answer> call) {
        return answer(call, null);
    }

    /** An answer's status and compact JSON; with {@code sql}, its activities (lite's SQL for the execution) are taken
     *  out into {@code sql} and the answer is recorded without them (S0, 2026-11-18: an SQL-only change then moves the
     *  plan-text file alone, never the answers). */
    private static String answer(java.util.function.Supplier<LiteV1Api.Answer> call,
            java.util.function.@com.legend.base.Nullable Consumer<String> sql) {
        try {
            LiteV1Api.Answer a = call.get();
            String json = a.json();
            try {
                Json.Obj o = Json.parseObject(json);
                if (sql != null && o.has("activities")) {
                    sql.accept(Json.toCompact(o.get("activities")));
                    o.fields().remove("activities");
                }
                json = Json.toCompact(o);   // compact, as the net has always written it
            } catch (RuntimeException notJson) {
                // an answer that is no JSON object is recorded as it is
            }
            return a.status() + "\t" + json;
        } catch (RuntimeException e) {
            return "THREW\t" + e.getClass().getSimpleName() + ": " + e.getMessage();
        }
    }

    private static void line(StringBuilder out, String name, String line) {
        out.append(name).append('\t')
                // a backslash escaped first (S0, 2026-11-18): a raw newline and an escaped one are two texts
                .append(IDS.matcher(line).replaceAll("<id>").replace("\\", "\\\\").replace("\n", "\\n")
                        .replace("\r", "")).append('\n');
    }

    /** THE PLAN-TEXT SPLIT, PLANTED (the review of PR #123, 2026-11-19): an execute answer's activities -- lite's SQL
     *  -- go to the plan-text file and leave the answer line (S0, 2026-11-18). An SQL-only change moves the plan-text
     *  line and never the answer; a value change moves the answer. */
    @Test
    void anSqlOnlyChangeMovesThePlanTextAlone() {
        String sql = "SELECT t0.YEAR AS year FROM TRADES AS t0";
        String renamed = sql.replace("t0", "t1");
        StringBuilder planText = new StringBuilder();
        StringBuilder renamedPlanText = new StringBuilder();
        String answer = answer(() -> executed(sql, 2023), a -> line(planText, "activities.x", a));
        String renamedAnswer = answer(() -> executed(renamed, 2023), a -> line(renamedPlanText, "activities.x", a));
        org.junit.jupiter.api.Assertions.assertEquals(answer, renamedAnswer, "an alias rename is lite's SQL alone");
        org.junit.jupiter.api.Assertions.assertFalse(answer.contains("activities"), "the answer line holds no SQL");
        org.junit.jupiter.api.Assertions.assertNotEquals(planText.toString(), renamedPlanText.toString(),
                "the plan-text file shows the rename");
        org.junit.jupiter.api.Assertions.assertNotEquals(answer,
                answer(() -> executed(sql, 2024), a -> line(new StringBuilder(), "activities.x", a)),
                "a value change moves the answer");
    }

    /** An execute answer as the server writes it: legend-engine's TDS JSON with its activities' SQL. */
    private static LiteV1Api.Answer executed(String sql, int year) {
        return new LiteV1Api.Answer(200, "{\"builder\":{\"_type\":\"tdsBuilder\",\"columns\":[{\"name\":\"year\","
                + "\"type\":\"Integer\"}]},\"activities\":[{\"_type\":\"relational\",\"sql\":\"" + sql + "\"}],"
                + "\"result\":{\"columns\":[\"year\"],\"rows\":[{\"values\":[" + year + "]}]}}");
    }

    @Test
    void everyRequestsAnswer() throws IOException {
        String model = Json.toCompact(Map.of("_type", "text", "code", resource("upstream-api/trades-h2.pure")));
        StringBuilder out = new StringBuilder();
        // the plan text the edge prints, lite's own SQL (S0, 2026-11-18): each execute answer's activities and each
        // generated plan, in a file of their own, so a slice that moves the SQL names that move and no answer moves
        StringBuilder planText = new StringBuilder();
        for (Map.Entry<String, String> r : REQUESTS.entrySet()) {
            String body;
            try {
                String function = com.legend.edge.PureV1Routes.grammarToJsonLambda(r.getValue(), false).json();
                body = "{\"clientVersion\":\"vX_X_X\",\"function\":" + function + ",\"model\":" + model
                        + ",\"context\":{\"_type\":\"BaseExecutionContext\"}}";
            } catch (RuntimeException e) {
                line(out, r.getKey(), "THREW\t" + e.getClass().getSimpleName() + ": " + e.getMessage());
                continue;
            }
            // the execute answer, its activities (the SQL text the edge prints) in the plan-text file: Phase 6 moves
            // the edge's plan text, so it is the net's to see (the orchestrator, 2026-11-17), apart from the answers
            line(out, r.getKey(), answer(() -> com.legend.edge.PureV1Routes.execute(body, RECORDER),
                    activities -> line(planText, "activities." + r.getKey(), activities)));
            // the plan the edge generates for the same request (pure/v1's generatePlan): legend-engine's plan text
            line(planText, "plan." + r.getKey(), answer(() -> com.legend.edge.PureV1Routes.generatePlan(body)));
        }
        String path = System.getProperty("legend.edge.purev1");
        if (path != null) {
            Files.writeString(Path.of(path), out);
        }
        String planPath = System.getProperty("legend.edge.purev1.plantext");
        if (planPath != null) {
            Files.writeString(Path.of(planPath), planText);
        }
        assertFalse(out.isEmpty());
    }
}
