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
            case FIELD_MISSING -> missing(o.get(1));
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
            case STORE_NOT_CONFIGURED -> ("page".equals(o.get(0)) ? "DataCube query store" : "Query store")
                    + " has not been configured properly (legend-lite: start the server with --query-store DIR)";
            case MALFORMED -> o.get(0) + ": " + o.get(1);
        };
    }

    /** legend-engine's sentence of a required field missing, by the field (its {@code validateQuery}). */
    private static String missing(String field) {
        return switch (field) {
            case "id" -> "Query ID is missing or empty";
            case "name" -> "Query name is missing or empty";
            case "groupId" -> "Query project group ID is missing or empty";
            case "artifactId" -> "Query project artifact ID is missing or empty";
            case "versionId" -> "Query project version is missing or empty";
            case "content" -> "Query content is missing or empty";
            case "mapping" -> "Query mapping is missing or empty";
            case "runtime" -> "Query runtime is missing or empty";
            case "dataSpacePath" -> "Query data Space execution context dataSpace path is missing or empty";
            // a DataCube page's content (its own validate)
            case "pageContent" -> "Query content is missing";
            default -> throw new IllegalStateException("no legend-engine sentence for the missing field '" + field + "'");
        };
    }

    /** The kinds legend-engine answers in its error envelope ({@code {"code":-1,"message",...,"status":"error"}}): a
     *  store not configured, a path it does not serve, an exception's 500; every other refusal is {@code {"message"}}
     *  alone. */
    static boolean enveloped(String kind) {
        return kind.equals("store_not_configured") || kind.equals("no_such_route") || kind.equals("malformed");
    }

    /** The product store's answer ({@code {"kind", "message"}}) in legend-engine's shape (J9 of the edge audit,
     *  2026-11-17: the envelope moved here from queries.SavedQueries), byte for byte: an enveloped kind's
     *  {@code {"code":-1,"message",...,"status":"error"}}, any other refusal's {@code {"message"}}; a success as it is. */
    static String engineShape(int status, String json) {
        if (status < 400 || !(com.legend.json.Json.parse(json) instanceof com.legend.json.Json.Obj o)
                || !(o.fields().get("kind") instanceof com.legend.json.Json.Str k)
                || !(o.fields().get("message") instanceof com.legend.json.Json.Str m)) {
            return json;
        }
        java.util.Map<String, Object> out = new java.util.LinkedHashMap<>();
        if (enveloped(k.value())) {
            out.put("code", -1);
            out.put("message", m.value());
            out.put("status", "error");
        } else {
            out.put("message", m.value());
        }
        return com.legend.json.Json.toCompact(out);
    }
}
