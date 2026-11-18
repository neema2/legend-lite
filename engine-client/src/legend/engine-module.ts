// LEGEND-ENGINE'S ROUTES IN THE TAB: THE ENGINE MODULE, THE EDGE'S (the user's K2 ruling, ~06:00 2026-11-18: "Product
// root, edge opt-in"; the orchestrator's option A, the same day). legend-lite's planner module (//wasm:planner) starts
// at lite's own entry and answers lite's operations alone; legend-engine's pure/v1 and execute's plan and answer halves
// are a module of their own (//wasm:engine_planner, com.legend.edge.wasm.EngineExports), loaded beside the planner only
// where an app is configured `engine: 'legend-engine'` -- any other word refused by name, nothing loaded. Until K2 the
// three requests here were kinds of the planner protocol (planner-protocol.ts) and their exports the planner module's
// (com.legend.edge.wasm.TabExports, from F's census line A1, 2026-11-17).
//
// The module's answers are the planner module's before the split, byte for byte (//wasm:engine_routes_test). It is
// stateless: each request carries its own model, so it holds nothing the planner module holds.

import { planning } from '../plan-guard.ts';

/** The one engine whose routes have a module in the tab. */
export const ENGINE = 'legend-engine';

/** A request of legend-engine's routes, as the edge asks it. */
export type EngineRequest =
  // one pure/v1 call: its path (`/api/pure/v1/...`), its raw query string ('' for none) and its body
  | { readonly kind: 'pureV1'; readonly path: string; readonly query: string; readonly body: string }
  // an ExecuteInput's plan half (its body) over the only models the host runs (a JSON array of their texts); then that
  // plan's answer from the rows the host's database answered running it, a JSON array of objects (HostRows)
  | { readonly kind: 'executePlan'; readonly body: string; readonly models: string }
  | { readonly kind: 'executeAnswer'; readonly plan: string; readonly rows: string };

/** The engine module's exports (com.legend.edge.wasm.EngineExports): each folded `OK\n<status>\n<type>\n<body>`. */
export interface EngineModule {
  readonly exports: {
    pureV1OrError(path: string, rawQuery: string, body: string): string;
    executeJsonPlanOrError(body: string, modelsJson: string): string;
    executeJsonAnswerOrError(plan: string, rows: string): string;
  };
}

/** Where the edge asks legend-engine's routes: the engine module, in this thread or another. */
export interface EnginePort {
  ask(request: EngineRequest): Promise<string>;
}

/** One request, answered by the engine module's export of the same name, with no logic of its own. */
export function answerEngine(m: EngineModule, r: EngineRequest): string {
  // a run plans nothing: legend-engine's routes are planning too (plan-guard.ts, as planner-answer.ts asks)
  planning(r.kind);
  switch (r.kind) {
    case 'pureV1': return m.exports.pureV1OrError(r.path, r.query, r.body);
    case 'executePlan': return m.exports.executeJsonPlanOrError(r.body, r.models);
    case 'executeAnswer': return m.exports.executeJsonAnswerOrError(r.plan, r.rows);
  }
}

/** The engine module asked in this thread. */
export function enginePort(m: EngineModule): EnginePort {
  return { ask: (r) => Promise.resolve().then(() => answerEngine(m, r)) };
}

/** Opens the engine module whose files (classes.wasm and its runtime) are in `dir`. */
export type EngineOpener = (dir: string) => Promise<EngineModule>;

/**
 * The engine module for an app configured `engine`, its files in `dir`: opened only for `engine: 'legend-engine'`. Any
 * other word is refused by name before anything is fetched -- no other engine has a module in the tab, and a word
 * mistyped never falls back to one (G7).
 */
export async function loadEngineModule(engine: string, dir: string, open: EngineOpener = openInBrowser): Promise<EngineModule> {
  if (engine !== ENGINE) {
    throw new Error(`the app is configured engine: ${JSON.stringify(engine)}, and only engine: '${ENGINE}' loads a module`
      + " in the tab (legend-engine's routes); lite's own operations are the planner module's");
  }
  return open(dir);
}

/** The module's files fetched by the browser from `dir`, as the planner's worker fetches the planner's. */
async function openInBrowser(dir: string): Promise<EngineModule> {
  const runtime = await import(/* @vite-ignore */ `${dir}wasm-gc-module-runtime.js`) as {
    load(src: string, options?: unknown): Promise<EngineModule>;
  };
  return runtime.load(`${dir}classes.wasm`, {
    stackDeobfuscator: { enabled: false },
    installImports(imports: Record<string, unknown>) {
      imports.teavmConsole = { putcharStdout() {}, putcharStderr() {} };
    },
  });
}
