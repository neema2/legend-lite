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

    /** name -> the lambda's grammar: the net's list, {@code edge/pure_v1_requests.tsv} (moved there from this class
     *  by slice R of the presentation program, 2026-11-18, so the plan oracle sends legend-engine the same requests:
     *  //tools/engine-runner:pure_v1_oracle_test). One TDS answer and one value answer; an error answer for each raise
     *  pure/v1 reaches; a cell of each kind. */
    private static Map<String, String> requests() throws IOException {
        Map<String, String> requests = new LinkedHashMap<>();
        for (String l : resource("edge/pure_v1_requests.tsv").split("\\R")) {
            if (l.isBlank() || l.startsWith("#")) {
                continue;
            }
            int tab = l.indexOf('\t');
            if (tab < 0 || requests.put(l.substring(0, tab), l.substring(tab + 1)) != null) {
                throw new IOException("a request line is a new name, a tab and a lambda: " + l);
            }
        }
        return requests;
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
        try {
            LiteV1Api.Answer a = call.get();
            String json = a.json();
            try {
                json = Json.toCompact(Json.parseObject(json));   // compact, as the net has always written it
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
                .append(IDS.matcher(line).replaceAll("<id>").replace("\n", "\\n").replace("\r", "")).append('\n');
    }

    @Test
    void everyRequestsAnswer() throws IOException {
        String model = Json.toCompact(Map.of("_type", "text", "code", resource("upstream-api/trades-h2.pure")));
        StringBuilder out = new StringBuilder();
        for (Map.Entry<String, String> r : requests().entrySet()) {
            String body;
            try {
                String function = com.legend.edge.PureV1Routes.grammarToJsonLambda(r.getValue(), false).json();
                body = "{\"clientVersion\":\"vX_X_X\",\"function\":" + function + ",\"model\":" + model
                        + ",\"context\":{\"_type\":\"BaseExecutionContext\"}}";
            } catch (RuntimeException e) {
                line(out, r.getKey(), "THREW\t" + e.getClass().getSimpleName() + ": " + e.getMessage());
                continue;
            }
            // the execute answer, its plan's activities (the SQL text the edge prints) included: Phase 6 moves the
            // edge's plan text, so it is the net's to see (the orchestrator, 2026-11-17; recorded before, dropped)
            line(out, r.getKey(), answer(() -> com.legend.edge.PureV1Routes.execute(body, RECORDER)));
            // the plan the edge generates for the same request (pure/v1's generatePlan): legend-engine's plan text
            line(out, "plan." + r.getKey(), answer(() -> com.legend.edge.PureV1Routes.generatePlan(body)));
        }
        String path = System.getProperty("legend.edge.purev1");
        if (path != null) {
            Files.writeString(Path.of(path), out);
        }
        assertFalse(out.isEmpty());
    }
}
