// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.queries.SavedQueries;
import com.legend.server.LegendHttpServer;
import com.legend.server.LiteV1Api;

import java.io.IOException;
import java.util.List;

/**
 * THE SHIPPED SERVER'S COMPOSITION ROOT (phase D's D1.0b, the orchestrator's ruling of 2026-11-12): lite's own server
 * ({@link LegendHttpServer}, its own routes) with legend-engine's API mounted beside them, for upstream's clients --
 * the pure/v1 routes ({@link PureV1Routes}, planned and run through the edge's dialects, {@link PureText#UPSTREAM}),
 * the saved-query store under legend-engine's path (an adapter over the product's store, the same one lite's own
 * {@code /api/lite/v1/query} reaches), and its current user. The product server runs alone without any of it (G5:
 * the edge depends on the product, never the reverse).
 */
public final class EdgeServer {

    private EdgeServer() {
    }

    /** The store's path upstream's clients call (legend-engine's query store). */
    static final String QUERY_STORE = "/api/pure/v1/query";

    /** legend-engine's API, mounted beside lite's own routes. */
    public static final LegendHttpServer.Mount UPSTREAM = context -> List.of(
            pureV1(runner(context.sources())),
            LegendHttpServer.storeRoutes(QUERY_STORE, context.queryStore(), context.cubeStore(), EdgeServer::storeWords),
            currentUser());

    /** The run of an upstream execute: the product's door, planned in the edge's dialects, its file: sources fenced
     *  to {@code sources} as the server's own runner fences them. */
    public static Shipped runner(@com.legend.base.Nullable String sources) {
        return (query, runtime, output) -> com.legend.Execution.runOnProjectDatabases(query, runtime, sources,
                PureText.PURE_V1, output);
    }

    /** The shipped route's runner, by its type: what an edge golden net that claims to record pure/v1 must run
     *  ({@code PureV1EdgeGoldenTest}; the orchestrator's condition 2 on F31, 2026-11-17). */
    public interface Shipped extends LiteV1Api.Runner {
    }

    /** {@code /api/pure/v1/...}: POST only, as upstream serves it. */
    static LegendHttpServer.Routes pureV1(LiteV1Api.Runner runner) {
        return new LegendHttpServer.Routes() {
            @Override
            public String prefix() {
                return PureV1Routes.PREFIX;
            }

            @Override
            public LiteV1Api.Answer answer(String method, String path, @com.legend.base.Nullable String rawQuery,
                    String body) {
                if (!"POST".equals(method)) {
                    return new LiteV1Api.Answer(405, "{\"error\":\"Method not allowed\"}");
                }
                return PureV1Routes.route(path, rawQuery, body, runner);
            }
        };
    }

    /** {@code /api/server/v1/currentUser}: legend-engine's current user, the store's anonymous one. */
    static LegendHttpServer.Routes currentUser() {
        return new LegendHttpServer.Routes() {
            @Override
            public String prefix() {
                return "/api/server/v1/currentUser";
            }

            @Override
            public LiteV1Api.Answer answer(String method, String path, @com.legend.base.Nullable String rawQuery,
                    String body) {
                return new LiteV1Api.Answer(200, "\"" + SavedQueries.ANONYMOUS + "\"");
            }
        };
    }

    /** The shipped server on {@code port} of the loopback interface, loopback origins only, no saved-query store and no
     *  file: source read: the development server, with legend-engine's API mounted. */
    public static LegendHttpServer loopback(int port) throws IOException {
        return new LegendHttpServer(port, java.net.InetAddress.getLoopbackAddress(), LegendHttpServer.Origins.LOOPBACK,
                null, LegendHttpServer.NO_SOURCES, UPSTREAM);
    }

    /** The shipped server, as {@link LegendHttpServer#serve} starts one, with legend-engine's API mounted. */
    public static void main(String[] args) throws IOException {
        LegendHttpServer.serve(args, UPSTREAM);
    }
    /** LEGEND-ENGINE'S SENTENCE OF A QUERY STORE'S REFUSAL: {@link StoreWords} (moved there 2026-11-16, the P2
     *  checkpoint audit's M2: the query-store page words upstream's path with it too). */
    public static String storeWords(com.legend.queries.SavedQueries.Refusal r) {
        return StoreWords.of(r);
    }
}
