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
            // J9 of the edge audit (2026-11-17): legend-engine's sentences of the store's refusals, moved from the
            // product's queries.SavedQueries and SavedCubes, byte for byte
            case FIELD_MISSING -> named(MISSING, "missing field", o.get(1));
            case UNSERVED_CONTEXT -> "Query execution context of _type '" + o.get(0) + "' is not served by legend-lite ("
                    + o.get(1) + ")";
            case INVALID_GROUP_ID -> "Query project group ID is invalid";
            case INVALID_ARTIFACT_ID -> "Query project artifact ID is invalid";
            case ID_TAKEN -> "Query with ID '" + o.get(0) + "' already existed";
            case ID_CHANGED -> "Updating query ID is not supported";
            case NOT_OWNER -> "Only owner can " + o.get(0) + " the query";
            case LIMIT_NOT_POSITIVE -> "Limit should be greater than 0";
            case TOO_MANY -> "Can't fetch more than " + o.get(0) + " queries";
            case NO_SEARCH_TERM -> "Query search spec expecting a search term";
            case STORE_NOT_CONFIGURED -> named(STORES, "store", o.get(0))
                    + " has not been configured properly (legend-lite: start the server with --query-store DIR)";
            case MALFORMED -> o.get(0) + ": " + o.get(1);
        };
    }

    /** legend-engine's name of a store not configured, by the store the product names. */
    private static final java.util.Map<String, String> STORES = java.util.Map.of(
            "query", "Query store", "page", "DataCube query store");

    /** legend-engine's sentence of a required field missing, by the field (its {@code validateQuery}; a DataCube
     *  page's content, its own validate). */
    private static final java.util.Map<String, String> MISSING = java.util.Map.ofEntries(
            java.util.Map.entry("id", "Query ID is missing or empty"),
            java.util.Map.entry("name", "Query name is missing or empty"),
            java.util.Map.entry("groupId", "Query project group ID is missing or empty"),
            java.util.Map.entry("artifactId", "Query project artifact ID is missing or empty"),
            java.util.Map.entry("versionId", "Query project version is missing or empty"),
            java.util.Map.entry("content", "Query content is missing or empty"),
            java.util.Map.entry("mapping", "Query mapping is missing or empty"),
            java.util.Map.entry("runtime", "Query runtime is missing or empty"),
            java.util.Map.entry("dataSpacePath", "Query data Space execution context dataSpace path is missing or empty"),
            java.util.Map.entry("pageContent", "Query content is missing"));

    /** {@code key}'s entry in {@code table}; a key it lacks is refused by name, never defaulted. */
    private static String named(java.util.Map<String, String> table, String what, String key) {
        String words = table.get(key);
        if (words == null) {
            throw new IllegalStateException("no legend-engine words for the " + what + " '" + key + "'");
        }
        return words;
    }
}
