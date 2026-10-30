// The planner's (WebAssembly) answer to one request of the planner protocol (planner-protocol.ts): the module's export
// of the same name, with no logic of its own. The tab's worker (planner-worker.ts) and the tests' in-process ports both
// answer through it, so they cannot answer differently -- and Python answers the same kinds with the native library's
// functions, each delegating to the same Boundary function as the export here (legend_lite/_planner.py).

import type { RequestBody } from './planner-protocol.ts';

/** The module's exports the protocol's kinds name (planner.TabExports). */
export interface PlannerModule {
  readonly exports: {
    holdModelOrError(model: string): string;
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
    executeJsonPlanOrError(body: string, modelsJson: string): string;
    executeJsonAnswerOrError(plan: string, rows: string): string;
    testDataSqlOrError(model: string, database: string, tablesJson: string): string;
    sessionSetupOrError(databaseType: string): string;
    projectDataOrError(model: string, databaseType: string): string;
    hostOperationsOrError(databaseType: string, operations: string): string;
    warmModelOrError(model: string): string;
  };
}

/** One request, answered by the module's export of the same name: the export's own tagged answer, every kind folded. */
export function answer(m: PlannerModule, r: RequestBody): string {
  switch (r.kind) {
    case 'hold': return m.exports.holdModelOrError(r.model);
    case 'release': return m.exports.releaseModelOrError(r.handle);
    case 'plan': return m.exports.planHeldOrError(r.handle, r.query, r.runtime);
    case 'relationType': return m.exports.relationTypeHeldOrError(r.handle, r.query);
    case 'tableModel': return m.exports.tableModelOrError(r.table);
    case 'catalogColumnsSql': return m.exports.catalogColumnsSqlOrError(r.schema, r.table);
    case 'planJson': return m.exports.planJsonHeldOrError(r.handle, r.lambda, r.runtime);
    case 'executionPlan': return m.exports.executionPlanJsonHeldOrError(r.handle, r.lambda, r.runtime);
    case 'planModel': return m.exports.planJsonOrError(r.model, r.lambda, r.runtime);
    case 'pureV1': return m.exports.pureV1OrError(r.path, r.query, r.body);
    case 'executePlan': return m.exports.executeJsonPlanOrError(r.body, r.models);
    case 'executeAnswer': return m.exports.executeJsonAnswerOrError(r.plan, r.rows);
    case 'testData': return m.exports.testDataSqlOrError(r.model, r.database, r.tables);
    case 'sessionSetup': return m.exports.sessionSetupOrError(r.databaseType);
    case 'projectData': return m.exports.projectDataOrError(r.model, r.databaseType);
    case 'hostOperations': return m.exports.hostOperationsOrError(r.databaseType, r.operations);
    case 'warm': return m.exports.warmModelOrError(r.model);
  }
}
