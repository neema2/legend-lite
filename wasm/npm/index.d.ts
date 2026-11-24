// The package's types (//wasm:engine_planner_package): what a host outside this repository calls, and no more.
//
// The module is the ENGINE module (//wasm:engine_planner, com.legend.edge.wasm.EngineExports): legend-engine's routes
// answered in the tab, a module of its own since the K2 ruling (2026-11-18, "product root, edge opt-in"). It has four
// exports; this declares ONE, `pureV1OrError`, because its contract is legend-engine's own API and so is the one
// surface that does not move with legend-lite. A host that needs execute's halves asks for them to be declared here.
// legend-lite's own operations are the product module's (//wasm:planner) and are not offered to an outside host.

/** What `load` takes. The two below are what legend-lite's own worker passes (engine-client's planner-worker.ts). */
export interface LoadOptions {
  /** `{ enabled: false }` unless the module's debug information is served beside it. */
  readonly stackDeobfuscator?: { readonly enabled: boolean };
  /** Called with the module's imports before it is instantiated: where a host silences `teavmConsole`. */
  installImports?(imports: Record<string, unknown>): void;
}

/** The module, loaded. */
export interface PlannerModule {
  readonly exports: {
    /**
     * One route of legend-engine's `pure/v1` API, answered in-process by the code legend-lite's server answers it
     * with (com.legend.server.PureV1Api).
     *
     * @param path the route's full path, e.g. `/api/pure/v1/execution/generatePlan`
     * @param rawQuery its query string without the `?`, `''` for none
     * @param body its request body, as text
     * @returns a folded answer, never a thrown error:
     *   `OK\n<status>\n<content type>\n<body>` when the route answered (any HTTP status, a refusal's included), or
     *   `ERR\n<kind>\n<message>` when the call itself failed.
     */
    pureV1OrError(path: string, rawQuery: string, body: string): string;
  };
}

/**
 * Loads the module.
 *
 * @param src where `classes.wasm` is: a URL in a browser or a worker, a filesystem path under Node
 */
export function load(src: string, options?: LoadOptions): Promise<PlannerModule>;
