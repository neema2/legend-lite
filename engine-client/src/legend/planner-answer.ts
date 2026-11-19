// The planner's (WebAssembly) answer to one request of the planner protocol (planner-protocol.ts): the module's export
// of the same name, with no logic of its own. The tab's worker (planner-worker.ts) and the tests' in-process ports both
// answer through it, so they cannot answer differently -- and Python answers the same kinds with the native library's
// functions, each delegating to the same Boundary function as the export here (legend_lite/_planner.py).

import type { RequestBody } from './planner-protocol.ts';
import { planning } from '../plan-guard.ts';

/** The module's exports the protocol's kinds name (planner.TabExports, lite's own entry: the user's K2 ruling, 2026-11-18;
 *  com.legend.edge.wasm.TabExports from 2026-11-17 until then, when legend-engine's routes were among them -- the engine
 *  module's now, legend/engine-module.ts). */
export interface PlannerModule {
  readonly exports: {
    holdModelOrError(model: string): string;
    holdModelOverOrError(model: string, library: string): string;
    releaseModelOrError(handle: number): string;
    // lite API v1's query/plan and query/type over a model held: its handle, the request without its model
    queryPlanHeldOrError(handle: number, request: string): string;
    queryTypeHeldOrError(handle: number, request: string): string;
    tableModelOrError(table: string): string;
    catalogColumnsSqlOrError(schema: string, table: string): string;
    // lite API v1's operations (docs/records/lite-api-v1-2026-11-17.md §2): the request's JSON in, the answer's out
    modelCheckOrError(request: string): string;
    modelElementsOrError(request: string): string;
    queryParseOrError(request: string): string;
    queryPrintOrError(request: string): string;
    queryTypeOrError(request: string): string;
    queryPlanOrError(request: string): string;
    modelCreateClassOrError(request: string): string;
    modelCreateAssociationOrError(request: string): string;
    modelCreateDataSpaceOrError(request: string): string;
    tsItemOrError(request: string): string;
    tsStatementOrError(request: string): string;
    fileLoadOrError(request: string): string;
    remoteMountOrError(request: string): string;
    snapStreamOrError(request: string): string;
    snapCopyOrError(request: string): string;
    outputPlanOrError(request: string): string;
    synthesizeOrError(request: string): string;
    authoredOrError(request: string): string;
    sessionSetupOrError(databaseType: string): string;
    projectDataOrError(model: string, databaseType: string): string;
    hostOperationsOrError(databaseType: string, operations: string): string;
    warmModelOrError(model: string): string;
  };
}

/** One request, answered by the module's export of the same name: the export's own tagged answer, every kind folded. */
export function answer(m: PlannerModule, r: RequestBody): string {
  // every in-thread call of the planner comes here: refused inside a run of its flow (run-frame.ts, the witness)
  planning(r.kind);
  switch (r.kind) {
    case 'hold': return r.library === undefined ? m.exports.holdModelOrError(r.model)
      : m.exports.holdModelOverOrError(r.model, r.library);
    case 'release': return m.exports.releaseModelOrError(r.handle);
    case 'queryPlanHeld': return m.exports.queryPlanHeldOrError(r.handle, r.request);
    case 'queryTypeHeld': return m.exports.queryTypeHeldOrError(r.handle, r.request);
    case 'tableModel': return m.exports.tableModelOrError(r.table);
    case 'catalogColumnsSql': return m.exports.catalogColumnsSqlOrError(r.schema, r.table);
    case 'modelCheck': return m.exports.modelCheckOrError(r.request);
    case 'modelElements': return m.exports.modelElementsOrError(r.request);
    case 'queryParse': return m.exports.queryParseOrError(r.request);
    case 'queryPrint': return m.exports.queryPrintOrError(r.request);
    case 'queryType': return m.exports.queryTypeOrError(r.request);
    case 'queryPlan': return m.exports.queryPlanOrError(r.request);
    case 'modelCreateClass': return m.exports.modelCreateClassOrError(r.request);
    case 'modelCreateAssociation': return m.exports.modelCreateAssociationOrError(r.request);
    case 'modelCreateDataSpace': return m.exports.modelCreateDataSpaceOrError(r.request);
    case 'tsItem': return m.exports.tsItemOrError(r.request);
    case 'tsStatement': return m.exports.tsStatementOrError(r.request);
    case 'fileLoad': return m.exports.fileLoadOrError(r.request);
    case 'remoteMount': return m.exports.remoteMountOrError(r.request);
    case 'snapStream': return m.exports.snapStreamOrError(r.request);
    case 'snapCopy': return m.exports.snapCopyOrError(r.request);
    case 'outputPlan': return m.exports.outputPlanOrError(r.request);
    case 'synthesize': return m.exports.synthesizeOrError(r.request);
    case 'authored': return m.exports.authoredOrError(r.request);
    case 'sessionSetup': return m.exports.sessionSetupOrError(r.databaseType);
    case 'projectData': return m.exports.projectDataOrError(r.model, r.databaseType);
    case 'hostOperations': return m.exports.hostOperationsOrError(r.databaseType, r.operations);
    case 'warm': return m.exports.warmModelOrError(r.model);
  }
}
