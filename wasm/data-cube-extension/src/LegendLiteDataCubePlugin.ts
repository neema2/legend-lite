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

import {
  LegendDataCubeApplicationPlugin,
  type LegendDataCubeDuckDBQueryPlanner,
} from '@finos/legend-application-data-cube';
import { LegendLiteEngineClient } from './LegendLiteEngineClient.js';
import {
  LegendLiteEngineTransport,
  type LegendLiteEngineConnection,
} from './LegendLiteEngineTransport.js';
import { isQuerySupportedByLegendLite } from './LegendLiteQuerySupport.js';

export const LEGEND_LITE_DATA_CUBE_PLUGIN_NAME =
  '@legend-lite/data-cube-extension';
export const LEGEND_LITE_DATA_CUBE_PLUGIN_VERSION = '0.1.0';

/** The key of legend-lite's planner: its settings are `debugger.planner.legend-lite.enabled` and `.shadow` */
export const LEGEND_LITE_PLANNER_KEY = 'legend-lite';
export const LEGEND_LITE_PLANNER_LABEL = 'legend-lite';

/**
 * Where the connection to legend-lite's engine module comes from.
 *
 * NOTE: by default it comes from `LegendLiteEngineAssets.ts`, loaded by a dynamic `import()` when legend-lite
 * is first asked, and not before, so that registering this plugin fetches nothing of the module (about 9 MB)
 * unless a setting is turned on and a query is planned with legend-lite. A test can give its own.
 */
export type LegendLiteEngineConnectionFactory =
  () => Promise<LegendLiteEngineConnection>;

const createDefaultConnection: LegendLiteEngineConnectionFactory = async () =>
  (await import('./LegendLiteEngineAssets.js')).createLegendLiteEngineConnection();

/**
 * Contributes legend-lite as a planner for DataCube's DuckDB-backed sources (cached sources, local files
 * and cached Iceberg tables): legend-lite's engine module, a WebAssembly build of a Legend compiler,
 * answers `generatePlan` in the browser, in a worker, instead of the engine server.
 *
 * Registering this plugin adds two settings to DataCube, both off by default (see
 * `LegendDataCubeApplicationPlugin.getExtraDuckDBQueryPlanners()` in `@finos/legend-application-data-cube`).
 */
export class LegendLiteDataCubePlugin extends LegendDataCubeApplicationPlugin {
  private readonly _createConnection: LegendLiteEngineConnectionFactory;

  constructor(createConnection = createDefaultConnection) {
    super(
      LEGEND_LITE_DATA_CUBE_PLUGIN_NAME,
      LEGEND_LITE_DATA_CUBE_PLUGIN_VERSION,
    );
    this._createConnection = createConnection;
  }

  override getExtraDuckDBQueryPlanners(): LegendDataCubeDuckDBQueryPlanner[] {
    return [
      {
        key: LEGEND_LITE_PLANNER_KEY,
        label: LEGEND_LITE_PLANNER_LABEL,
        createEngineServerClient: () =>
          new LegendLiteEngineClient(
            new LegendLiteEngineTransport(this._createConnection),
          ),
        isQuerySupported: isQuerySupportedByLegendLite,
      },
    ];
  }
}
