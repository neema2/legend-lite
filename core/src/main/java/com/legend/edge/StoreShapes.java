// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

/**
 * legend-engine's SHAPE OF A QUERY STORE'S ANSWER (J9 of the edge audit, 2026-11-17): the product's store answers a
 * refusal as {@code {"kind", "message"}}; at legend-engine's path the edge's mount ({@code EdgeServer}) rewrites it as
 * legend-engine answers -- its error envelope for a store not configured, a path it does not serve and an exception's
 * 500, {@code {"message"}} for every other refusal -- the words already legend-engine's ({@link StoreWords}).
 */
public final class StoreShapes {

    private StoreShapes() {
    }

    /** The kinds legend-engine answers in its error envelope ({@code {"code":-1,"message",...,"status":"error"}}): a
     *  store not configured, a path it does not serve, an exception's 500; every other refusal is {@code {"message"}}
     *  alone. */
    public static boolean enveloped(String kind) {
        return ENVELOPED.contains(kind);
    }

    private static final java.util.Set<String> ENVELOPED = java.util.Set.of(
            "store_not_configured", "no_such_route", "malformed");

    /** The product store's answer ({@code {"kind", "message"}}) in legend-engine's shape (J9 of the edge audit,
     *  2026-11-17: the envelope moved here from queries.SavedQueries), byte for byte: an enveloped kind's
     *  {@code {"code":-1,"message",...,"status":"error"}}, any other refusal's {@code {"message"}}; a success as it is. */
    public static String engineShape(int status, String json) {
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
