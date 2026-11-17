// The planner's (WebAssembly) answer to one request of the planner protocol (planner-protocol.ts): the module's export
// of the same name, with no logic of its own. The tab's worker (planner-worker.ts) and the tests' in-process ports both
// answer through it, so they cannot answer differently -- and Python answers the same kinds with the native library's
// functions, each delegating to the same Boundary function as the export here (legend_lite/_planner.py).

import type { RequestBody } from './planner-protocol.ts';
import { planning } from '../plan-guard.ts';

/** The module's exports the protocol's kinds name (com.legend.edge.wasm.TabExports; planner.TabExports until 2026-11-17). */
export interface PlannerModule {
  readonly exports: {
    holdModelOrError(model: string): string;
    holdModelOverOrError(model: string, library: string): string;
    releaseModelOrError(handle: number): string;
    planHeldOrError(handle: number, query: string, runtime: string): string;
    relationTypeHeldOrError(handle: number, query: string): string;
    tableModelOrError(table: string): string;
    catalogColumnsSqlOrError(schema: string, table: string): string;
    planJsonHeldOrError(handle: number, lambdaJson: string, runtime: string): string;
    executionPlanJsonHeldOrError(handle: number, lambdaJson: string, runtime: string): string;
    planJsonOrError(model: string, lambdaJson: string, runtime: string): string;
    // legend-engine's pure/v1 -- and lite's own /api/lite/v1/compilation/compile -- routed as legend-lite's server
    // routes them (PureV1Api), and execute's two halves for a host that runs the SQL (PureV1Api.jsonPlan, jsonAnswer):
    // `OK\n<status>\n<type>\n<body>`
    pureV1OrError(path: string, rawQuery: string, body: string): string;
    // lite's own grammar, typing and routes, each its product boundary's answer (A1; H2, 2026-11-17): `OK\n<answer>`
    modelElementsOrError(text: string, sourceInformation: boolean): string;
    parseLambdaOrError(text: string, sourceInformation: boolean): string;
    printLambdaOrError(lambdaJson: string, style: string): string;
    lambdaRelationTypeOrError(model: string, lambdaJson: string): string;
    compileErrorsOrError(model: string): string;
    modelActionOrError(action: string, body: string): string;
    tsItemOrError(text: string): string;
    jsonToTsOrError(lambdaJson: string, model: string): string;
    executeJsonPlanOrError(body: string, modelsJson: string): string;
    executeJsonAnswerOrError(plan: string, rows: string): string;
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
    case 'plan': return m.exports.planHeldOrError(r.handle, r.query, r.runtime);
    case 'relationType': return m.exports.relationTypeHeldOrError(r.handle, r.query);
    case 'tableModel': return m.exports.tableModelOrError(r.table);
    case 'catalogColumnsSql': return m.exports.catalogColumnsSqlOrError(r.schema, r.table);
    case 'planJson': return m.exports.planJsonHeldOrError(r.handle, r.lambda, r.runtime);
    case 'executionPlan': return m.exports.executionPlanJsonHeldOrError(r.handle, r.lambda, r.runtime);
    case 'planModel': return m.exports.planJsonOrError(r.model, r.lambda, r.runtime);
    case 'pureV1': return m.exports.pureV1OrError(r.path, r.query, r.body);
    case 'modelElements': return m.exports.modelElementsOrError(r.text, r.sourceInformation);
    case 'parseLambda': return m.exports.parseLambdaOrError(r.text, r.sourceInformation);
    case 'printLambda': return m.exports.printLambdaOrError(r.lambda, r.style);
    case 'lambdaRelationType': return m.exports.lambdaRelationTypeOrError(r.model, r.lambda);
    case 'compileErrors': return m.exports.compileErrorsOrError(r.model);
    case 'modelAction': return m.exports.modelActionOrError(r.action, r.body);
    case 'tsItem': return m.exports.tsItemOrError(r.text);
    case 'jsonToTs': return m.exports.jsonToTsOrError(r.lambda, r.model);
    case 'executePlan': return m.exports.executeJsonPlanOrError(r.body, r.models);
    case 'executeAnswer': return m.exports.executeJsonAnswerOrError(r.plan, r.rows);
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
