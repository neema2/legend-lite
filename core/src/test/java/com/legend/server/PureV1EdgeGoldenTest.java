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

    private static final String RT = "->from(trades::h2::RT)";
    private static final String TRADES = "#>{trades::h2::DB.TRADES_SCHEMA.TRADES}#";

    /** name -> the lambda's grammar. */
    private static final Map<String, String> REQUESTS = new LinkedHashMap<>();

    static {
        // one TDS answer and one value answer
        REQUESTS.put("tds", "|" + TRADES + "->select(~[year, region])->sort([~year->ascending(), ~region->ascending()])->limit(2)" + RT);
        REQUESTS.put("value", "|" + TRADES + "->size()" + RT);
        // an error answer for each raise pure/v1 reaches
        REQUESTS.put("raise.divideByZero", "|" + TRADES + "->limit(1)->extend(~d: r|$r.year->toOne() / 0)->select(~[d])" + RT);
        REQUESTS.put("raise.atOutOfBounds", "|" + TRADES + "->limit(1)->extend(~a: r|[1, 2]->at(5))->select(~[a])" + RT);
        REQUESTS.put("raise.toOneOverMany", "|" + TRADES + "->limit(1)->extend(~o: r|[1, 2]->toOne())->select(~[o])" + RT);
        REQUESTS.put("raise.sqrtOfNegative", "|" + TRADES + "->limit(1)->extend(~s: r|sqrt(-2.0))->select(~[s])" + RT);
        REQUESTS.put("raise.parseInteger", "|" + TRADES + "->limit(1)->extend(~p: r|'abc'->parseInteger())->select(~[p])" + RT);
        // a cell of each kind
        REQUESTS.put("cell.dateTimeCoarse", "|" + TRADES + "->limit(1)->extend(~t: r|%2024-01-02T10:30:00)->select(~[t])" + RT);
        REQUESTS.put("cell.dateTimeSubMicro",
                "|" + TRADES + "->limit(1)->extend(~t: r|%2024-01-02T10:30:00.123456789)->select(~[t])" + RT);
        REQUESTS.put("cell.float", "|" + TRADES + "->limit(1)->extend(~f: r|1.0 / 3.0)->select(~[f])" + RT);
        REQUESTS.put("cell.floatWhole", "|" + TRADES + "->limit(1)->extend(~f: r|2.0 * 3.0)->select(~[f])" + RT);
        REQUESTS.put("cell.empty",
                "|" + TRADES + "->limit(1)->extend(~e: r|[]->cast(@String)->first())->select(~[e])" + RT);
        REQUESTS.put("cell.emptyIf",
                "|" + TRADES + "->limit(1)->extend(~e: r|if(false, |'x', |[]->cast(@String)))->select(~[e])" + RT);
        REQUESTS.put("cell.strictDate", "|" + TRADES + "->limit(1)->extend(~d: r|%2024-01-02)->select(~[d])" + RT);
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
    static final PureV1Api.Runner RECORDER = com.legend.edge.EdgeServer.runner(null);

    /** THE GUARD (the orchestrator's condition 2 on F31, 2026-11-17): the net records through the shipped edge route
     *  it claims, by type -- the product's runner, a planted violator, is refused. */
    @Test
    void theRecorderIsTheShippedRoute() {
        org.junit.jupiter.api.Assertions.assertTrue(shipped(RECORDER), "the pure/v1 net records the shipped route");
        org.junit.jupiter.api.Assertions.assertFalse(shipped(ConnectionResolver.RUNNER),
                "the product's runner is not the shipped edge route: a recorder running it records a road no pure/v1"
                        + " client runs");
    }

    private static boolean shipped(PureV1Api.Runner runner) {
        return runner instanceof com.legend.edge.EdgeServer.Shipped;
    }

    @Test
    void everyRequestsAnswer() throws IOException {
        String model = Json.toCompact(Map.of("_type", "text", "code", resource("upstream-api/trades-h2.pure")));
        StringBuilder out = new StringBuilder();
        for (Map.Entry<String, String> r : REQUESTS.entrySet()) {
            String line;
            try {
                String function = com.legend.edge.PureV1Routes.grammarToJsonLambda(r.getValue(), false).json();
                PureV1Api.Answer a = com.legend.edge.PureV1Routes.execute("{\"clientVersion\":\"vX_X_X\",\"function\":"
                        + function + ",\"model\":" + model + ",\"context\":{\"_type\":\"BaseExecutionContext\"}}",
                        RECORDER);
                // the answer without its plan's activities (the SQL: lean SQL moves it; not the edge's text)
                String json = a.json();
                try {
                    Json.Obj o = Json.parseObject(json);
                    Map<String, Object> kept = new LinkedHashMap<>();
                    for (Map.Entry<String, Json.Node> f : o.fields().entrySet()) {
                        if (!f.getKey().equals("activities")) {
                            kept.put(f.getKey(), f.getValue());
                        }
                    }
                    json = Json.toCompact(kept);
                } catch (RuntimeException notJson) {
                    // an answer that is no JSON object is recorded as it is
                }
                line = a.status() + "\t" + json;
            } catch (RuntimeException e) {
                line = "THREW\t" + e.getClass().getSimpleName() + ": " + e.getMessage();
            }
            out.append(r.getKey()).append('\t')
                    .append(IDS.matcher(line).replaceAll("<id>").replace("\n", "\\n").replace("\r", "")).append('\n');
        }
        String path = System.getProperty("legend.edge.purev1");
        if (path != null) {
            Files.writeString(Path.of(path), out);
        }
        assertFalse(out.isEmpty());
    }
}
