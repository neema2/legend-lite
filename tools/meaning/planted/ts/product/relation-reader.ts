// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

// PLANTED (census B2): a TypeScript product module reading legend-engine's generatePlan shape (M1), with an export only
// the edge uses (M6), a name naming upstream (M2), a reproduction declared (M5), and the product entry's static import
// of an edge module (M7).
import { legendEngineRoutes } from '../edge/engine-routes.ts';

export interface Column { name: string; type: string }

// ENGINE QUIRK: legend-engine answers the columns under the root node's result type
export function tdsColumnsOf(plan: { rootExecutionNode: { resultType: { tdsColumns: Column[] } } }): Column[] {
  return plan.rootExecutionNode.resultType.tdsColumns;
}

export const legendEngineShape = 'rootExecutionNode';

export function boot(): string {
  return legendEngineRoutes();
}
