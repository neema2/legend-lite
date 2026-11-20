/**
 * Copyright (c) 2026-present, Goldman Sachs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { beforeAll, describe, expect, test } from '@jest/globals';
import { V1_EngineServerClient } from '@finos/legend-graph';
import { LegendDataCubeApplicationPlugin } from '@finos/legend-application-data-cube';
import {
  LEGEND_LITE_DATA_CUBE_PLUGIN_NAME,
  LEGEND_LITE_PLANNER_KEY,
  LegendLiteDataCubePlugin,
} from '../LegendLiteDataCubePlugin.js';
import { LegendLiteEngineClient } from '../LegendLiteEngineClient.js';
import type {
  LegendLiteEngineConnection,
  LegendLiteEngineRequest,
  LegendLiteEngineResponse,
} from '../LegendLiteEngineTransport.js';

/*
 * NOTE: importing `@finos/legend-application-data-cube` loads the whole DataCube application, whose DuckDB
 * engine uses `import.meta`, which jest cannot parse. legend-studio's own tests mock that module; here, the
 * jest configuration must stub it, e.g. with
 *
 *   moduleNameMapper: { 'LegendDataCubeDuckDBEngine(\\.js)?$': '<rootDir>/test/LegendDataCubeDuckDBEngineStub.js' }
 *
 * where the stub exports `LegendDataCubeDuckDBEngine`, a class with `initialize()` and `dispose()` that
 * resolve. See README.md, "Tests".
 */

/** jsdom has no `Response`; this has what the client reads of one (see LegendLiteEngineClient.test.ts). */
class TEST__Response {
  readonly status: number;
  readonly statusText = '';
  readonly url = '';
  readonly ok: boolean;

  constructor(_body: string | null, init: { status: number }) {
    this.status = init.status;
    this.ok = init.status >= 200 && init.status < 300;
  }
}

beforeAll(() => {
  (globalThis as { Response?: unknown }).Response ??= TEST__Response;
});

/** A connection whose worker answers every request with the answer given, in the module's format. */
const TEST__connectionFactory = (answer: string) => {
  const state = { connections: 0, requests: [] as LegendLiteEngineRequest[] };
  const createConnection = async (): Promise<LegendLiteEngineConnection> => {
    state.connections++;
    let onResponse: ((response: LegendLiteEngineResponse) => void) | undefined;
    return {
      port: {
        send: (request) => {
          state.requests.push(request);
          queueMicrotask(() => onResponse?.({ id: request.id, ok: true, answer }));
        },
        listen: (listener) => {
          onResponse = listener;
        },
        close: () => {},
      },
      runtimeUrl: 'http://test/runtime.js',
      moduleUrl: 'http://test/classes.wasm',
    };
  };
  return { state, createConnection };
};

const TEST__PLAN = { authDependent: false, kerberos: undefined };

describe('legend-lite DataCube plugin', () => {
  test('is a DataCube application plugin which contributes one planner, legend-lite', () => {
    const plugin = new LegendLiteDataCubePlugin();
    expect(plugin).toBeInstanceOf(LegendDataCubeApplicationPlugin);
    expect(plugin.getName()).toBe(LEGEND_LITE_DATA_CUBE_PLUGIN_NAME);
    const planners = plugin.getExtraDuckDBQueryPlanners();
    expect(planners).toHaveLength(1);
    expect(planners[0]?.key).toBe(LEGEND_LITE_PLANNER_KEY);
    expect(planners[0]?.label).toBe('legend-lite');
  });

  test("the planner's client is an engine server client, and nothing is loaded until it is asked", async () => {
    const { state, createConnection } = TEST__connectionFactory(
      `OK\n200\napplication/json\n${JSON.stringify(TEST__PLAN)}`,
    );
    const planner = new LegendLiteDataCubePlugin(
      createConnection,
    ).getExtraDuckDBQueryPlanners()[0];
    const client = planner?.createEngineServerClient();

    expect(client).toBeInstanceOf(V1_EngineServerClient);
    expect(client).toBeInstanceOf(LegendLiteEngineClient);
    expect(state.connections).toBe(0);

    const plan = await client?.generatePlan({
      clientVersion: 'vX_X_X',
      function: { _type: 'lambda', body: [], parameters: [] },
      model: { _type: 'data', elements: [] },
      context: { _type: 'BaseExecutionContext' },
    });
    expect(plan).toEqual(TEST__PLAN);
    expect(state.connections).toBe(1);
    expect(state.requests.map((request) => request.path)).toEqual([
      '/api/pure/v1/execution/generatePlan',
    ]);
  });

  test("the planner is not asked for a pivot DataCube runs to discover the pivot's columns", () => {
    const planner = new LegendLiteDataCubePlugin().getExtraDuckDBQueryPlanners()[0];
    const pivot = {
      _type: 'lambda',
      parameters: [],
      body: [{ _type: 'func', function: 'pivot', parameters: [] }],
    };
    const filter = {
      _type: 'lambda',
      parameters: [],
      body: [{ _type: 'func', function: 'filter', parameters: [] }],
    };
    expect(planner?.isQuerySupported?.(pivot)).toBe(false);
    expect(planner?.isQuerySupported?.(filter)).toBe(true);
  });
});
