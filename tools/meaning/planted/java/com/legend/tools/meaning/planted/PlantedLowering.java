// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.meaning.planted;

/** PLANTED (census B2): product code holding upstream's meaning, one shape per lens. */
public final class PlantedLowering {

    private final PlantedHook hook;

    public PlantedLowering(PlantedHook hook) {
        this.hook = hook;
    }

    /** M3 BRANCH: a branch on the hook's answer, which is a constant in the product. */
    public String select(String body) {
        String frame = hook.frame();
        if (frame != null) {
            return "SELECT * FROM (" + body + ") AS \"" + frame + "_0\"";
        }
        return body;
    }

    /** M3 BRANCH, one call deeper: the answer handed to a method that branches on it. */
    public String framed(String body) {
        return wrap(body, hook.frame());
    }

    static String wrap(String body, String frame) {
        return frame == null ? body : "(" + body + ") " + frame;
    }

    /** M3 UNUSED: upstream's sentence computed eagerly in the product and handed to a form that never reads it. */
    public String fail(String kind) {
        return hook.raise(kind, "Assert failed");
    }

    /** M3 EDGE_LIVE: a parameter every product call passes as null, that the edge passes live. */
    public static String prune(String query, String keptFrame) {
        return keptFrame == null ? query : query + " /* " + keptFrame + " */";
    }

    public static String pruned(String query) {
        return prune(query, null);
    }

    /** M1 TEXT: a product string holding a legend-engine class name and a legend-engine sentence. */
    public static String envelope() {
        return "{\"_type\": \"RelationalTdsInstantiationExecutionNode\", \"message\": \"Query project group ID is invalid\"}";
    }
}
