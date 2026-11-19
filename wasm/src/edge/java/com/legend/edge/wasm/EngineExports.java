package com.legend.edge.wasm;

import org.teavm.jso.JSExport;
import planner.Folded;

/**
 * THE ENGINE MODULE'S ENTRY, AT THE EDGE (the user's K2 ruling, ~06:00 2026-11-18: "Product root, edge opt-in"; the
 * orchestrator's option A, the same day): TeaVM's entry class of {@code //wasm:engine_planner}, whose {@code @JSExport}
 * functions are legend-engine's routes in the tab -- {@code pure/v1} and execute's plan and answer halves, each a
 * one-line delegation to {@link EdgeBoundary} through {@link Folded}, as the server's edge answers them. They were the
 * product module's ({@code planner.TabExports}; {@code com.legend.edge.wasm.TabExports} from F's census line A1,
 * 2026-11-17, until K2) and are moved here unchanged, names and answers byte for byte (//wasm:engine_routes_test holds
 * them to pins captured before the split).
 *
 * <p>The module is loaded beside the product's ({@code //wasm:planner}) only where an app is configured
 * {@code engine: 'legend-engine'}; it depends on the product, never the reverse. Lite's own operations stay the product
 * module's alone: this one exports nothing else.
 */
public final class EngineExports {

    private EngineExports() {
    }

    /** One {@code pure/v1} call, as legend-lite's server answers it ({@link EdgeBoundary#pureV1}). */
    @JSExport
    public static String pureV1OrError(String path, String rawQuery, String body) {
        return Folded.http(() -> EdgeBoundary.pureV1(path, rawQuery, body));
    }

    /** {@link EdgeBoundary#executeJsonPlan}: execute's plan half -- the SQL the tab runs, and the answer but its rows (C1). */
    @JSExport
    public static String executeJsonPlanOrError(String body, String modelsJson) {
        return Folded.http(() -> EdgeBoundary.executeJsonPlan(body, modelsJson));
    }

    /** {@link EdgeBoundary#executeJsonAnswer}: the answer, the rows the tab's database answered written as the server's
     *  (C1). */
    @JSExport
    public static String executeJsonAnswerOrError(String plan, String rows) {
        return Folded.http(() -> EdgeBoundary.executeJsonAnswer(plan, rows));
    }

    /** {@link EdgeBoundary#executeArrowAnswer}: the answer in upstream's Arrow format, the rows the tab's database
     *  answered written by lite's Arrow writer, folded as base64 (track A, 2026-10-29). */
    @JSExport
    public static String executeArrowAnswerOrError(String plan, String rows) {
        return Folded.http(() -> EdgeBoundary.executeArrowAnswer(plan, rows));
    }

    /** TeaVM's entry point: the module's exports are its API, and nothing runs on load. */
    public static void main(String[] args) {
        // nothing: the product module's main is the demo's plan, and this module answers legend-engine's routes alone
    }
}
