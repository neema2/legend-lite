// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge.wasm;

/**
 * THE EMBEDDED HOSTS' ENGINE ROUTES, AT THE EDGE (F's separation census line A1, 2026-11-17): legend-engine's
 * {@code pure/v1} API and execute's plan and answer halves, as the tab (TeaVM, {@link TabExports}) and Python
 * (native's {@code EngineEntries}) answer them -- through the compatibility edge ({@code //core:edge}: its routes,
 * {@code PureV1Routes}, and its host answers, {@code HostAnswers}, in upstream's style), as the server does. Moved
 * from {@code planner.Boundary} unchanged; the boundary keeps lite's own operations, and the bundles that carry these
 * are rooted at the edge as the server is (the orchestrator's extension of ruling A4). This host's models are the
 * boundary's ({@code Boundary.withModelText}).
 */
public final class EdgeBoundary {

    private EdgeBoundary() {
    }

    // ---- legend-engine's pure/v1 API, as legend-lite's server answers it (LiteV1Api, the plan side): how the tab's
    // ---- apps ask what they would ask a server, and how Python's engine answers DataCube
    // ---- (docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md)

    /**
     * One {@code pure/v1} call by its path and raw query string ({@code ""} for none), routed and answered as
     * legend-lite's server answers it -- lite's own {@code /api/lite/v1/<operation>} routes included. An execute's
     * run is the host's (its rows come from {@link #executePlan}), so one asked for here is refused, naming the
     * format that is served.
     */
    public static com.legend.server.LiteV1Api.Answer pureV1(String path, String rawQuery, String body) {
        com.legend.server.LiteV1Api.Runner noRun = (query, runtime, output) -> {
            throw new IllegalArgumentException("this engine runs no execute itself: the host runs the plan"
                    + " (executePlan) and asks for its answer (executeJsonAnswer, executeArrowAnswer)");
        };
        // legend-engine's routes are the edge's, lite's own the product's (phase D's D1.0b, 2026-11-12): routed by the
        // path's prefix, as the server mounts them
        return path.startsWith(com.legend.edge.PureV1Routes.PREFIX)
                ? com.legend.edge.PureV1Routes.route(path, rawQuery.isEmpty() ? null : rawQuery, body, noRun, HELD)
                : com.legend.server.LiteV1Api.route(path, body, (query, runtime, values, output) -> {
                    throw new IllegalArgumentException("this engine runs no query itself: the host runs the plan"
                            + " (query/plan) with its own runner");
                }, HELD);
    }

    /** A request's model given as text: the one this host holds for that text, compiled no more, else compiled for the
     *  request (HeldModels.withText). */
    private static final com.legend.server.LiteV1Api.Models HELD = new com.legend.server.LiteV1Api.Models() {
        @Override
        public <T> T with(String text, java.util.function.Function<com.legend.compiler.element.ModelContext, T> use) {
            return planner.Boundary.withModelText(text, use);
        }
    };

    /**
     * Execute's plan half in upstream's Arrow format (LiteV1Api.arrowPlan): the SQL to run and the Arrow schema
     * metadata, or the refusal to send. {@code modelsJson}: the only models the host runs, a JSON array of their
     * texts.
     */
    public static com.legend.server.LiteV1Api.Answer executePlan(String body, String modelsJson) {
        // pure/v1's execute plans in the compatibility edge's pure/v1 style (the D part 1 audit's M2, 2026-11-15; B7,
        // 2026-11-17: legend-engine's unchecked raises plain, PureText.PURE_V1)
        return com.legend.edge.HostAnswers.arrowPlan(body, models(modelsJson), com.legend.edge.PureText.PURE_V1);
    }

    /**
     * Execute's plan half in the engine's JSON (LiteV1Api.jsonPlan): the statement to run, the answer's shape and its
     * result columns with their type facts, and everything the answer holds but its rows -- the TDS builder, the
     * activities, the columns' names, or a graph fetch's JSON builder -- for a host whose database runs the statement
     * (the tab; docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C1).
     * {@code modelsJson}: the only models the host runs, a JSON array of their texts.
     */
    public static com.legend.server.LiteV1Api.Answer executeJsonPlan(String body, String modelsJson) {
        return com.legend.edge.HostAnswers.jsonPlan(body, models(modelsJson), com.legend.edge.PureText.PURE_V1);
    }

    /**
     * Execute's answer for a host that ran {@link #executeJsonPlan}'s {@code run} statement (LiteV1Api.jsonAnswer):
     * that plan's answer and the host's rows (a JSON array of objects, each a column's name and its cell as the
     * host's driver read it), decoded by the plan's columns and written by the writer legend-lite's server answers
     * with.
     */
    public static com.legend.server.LiteV1Api.Answer executeJsonAnswer(String plan, String rows) {
        return com.legend.edge.HostAnswers.jsonAnswer(plan, rows);
    }

    /**
     * Execute's answer in upstream's Arrow format for a host that ran {@link #executeJsonPlan}'s {@code run}
     * statement (LiteV1Api.arrowAnswer; track A, 2026-10-29): that plan's answer and the host's rows, decoded by the
     * plan's columns and written by lite's Arrow writer as the one zstd frame upstream sends -- folded as base64
     * (Folded.http).
     */
    public static com.legend.server.LiteV1Api.Answer executeArrowAnswer(String plan, String rows) {
        return com.legend.edge.HostAnswers.arrowAnswer(plan, rows);
    }

    /** A host's refusal of a call it could not finish -- its database refusing the SQL, say -- in legend-engine's
     *  error shape ({@link com.legend.edge.EngineEnvelope#refused}): 500, no {@code errorType}. Python's native
     *  {@code lite_pure_v1_refusal} (its edge half, EngineEntries) since planner.Boundary's refusal went with lite API
     *  v1's step 2c (2026-11-17, the Python host's ask). */
    public static com.legend.server.LiteV1Api.Answer refused(String message) {
        return com.legend.edge.EngineEnvelope.refused(message);
    }

    /** The models a host runs, from their JSON array of texts. */
    private static java.util.List<String> models(String modelsJson) {
        java.util.List<String> models = new java.util.ArrayList<>();
        if (!(com.legend.json.Json.parse(modelsJson) instanceof com.legend.json.Json.Arr arr)) {
            throw new IllegalArgumentException("the models: not a JSON array");
        }
        for (com.legend.json.Json.Node n : arr.items()) {
            if (!(n instanceof com.legend.json.Json.Str text)) {
                throw new IllegalArgumentException("the models: an entry that is not a model's text");
            }
            models.add(text.value());
        }
        return models;
    }
}
