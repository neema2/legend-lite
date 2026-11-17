// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.queries.SavedQueries;

/**
 * LEGEND-ENGINE'S SENTENCE OF A QUERY STORE'S REFUSAL (D1.8, W8 of docs/records/d-plan-2026-11-16.md, 2026-11-16): a
 * refusal's kind as legend-engine's query API words it (its {@code MongoDBQueryStoreManager}), for a store served at
 * legend-engine's path -- the server's mount ({@code EdgeServer}) and the query-store page, which serves
 * {@code /api/pure/v1/query} in the tab (the P2 checkpoint audit's M2, 2026-11-16: it answered in lite's words). Its own
 * target ({@code //core:edge_store_words}): it depends on the query store alone, so the page carries no server and no
 * planner. A refusal with no kind is its own message.
 */
public final class StoreWords {

    private StoreWords() {
    }

    public static String of(SavedQueries.Refusal r) {
        SavedQueries.Refusal.Kind k = r.kind();
        if (k == null) {
            return String.valueOf(r.getMessage());
        }
        java.util.List<String> o = r.operands();
        return switch (k) {
            case QUERY_NOT_FOUND -> "Can't find query with ID '" + o.get(0) + "'";
            case QUERIES_NOT_FOUND -> "Can't find queries for the following ID(s):\n" + o.get(0);
            case VERSION_NOT_FOUND -> "Can't find version '" + o.get(0) + "' for query with ID '" + o.get(1) + "'";
            // legend-engine's path, the edge's mount's (B7, the orchestrator's ruling (3) of 2026-11-17)
            case NO_SUCH_ROUTE -> "no such legend-engine API in legend-lite: /api/pure/v1/query" + o.get(0);
        };
    }
}
