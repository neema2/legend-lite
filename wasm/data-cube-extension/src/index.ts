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

// NOTE: `LegendLiteEngineAssets.js` is deliberately not exported: it must only be reached by the dynamic
// `import()` in `LegendLiteDataCubePlugin.ts`, so that nothing of the engine module is fetched unless used
export {
  LegendLiteDataCubePlugin,
  LEGEND_LITE_DATA_CUBE_PLUGIN_NAME,
  LEGEND_LITE_DATA_CUBE_PLUGIN_VERSION,
  LEGEND_LITE_PLANNER_KEY,
  LEGEND_LITE_PLANNER_LABEL,
  type LegendLiteEngineConnectionFactory,
} from './LegendLiteDataCubePlugin.js';
export {
  LegendLiteEngineClient,
  liftEmbeddedConnections,
  type LegendLiteEngineRouter,
} from './LegendLiteEngineClient.js';
export {
  LegendLiteEngineError,
  LegendLiteEngineTransport,
  LEGEND_LITE_ENGINE_ERROR_KIND,
  unfoldLegendLiteEngineAnswer,
  type LegendLiteEngineAnswer,
  type LegendLiteEngineConnection,
  type LegendLiteEnginePort,
  type LegendLiteEngineRequest,
  type LegendLiteEngineResponse,
} from './LegendLiteEngineTransport.js';
export {
  hasCastlessPivot,
  isQuerySupportedByLegendLite,
} from './LegendLiteQuerySupport.js';
