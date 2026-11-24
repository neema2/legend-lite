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

/**
 * Where legend-lite's engine module is, and the worker that holds it.
 *
 * IMPORTANT: this is the only file that refers to the module's files, and it must only ever be loaded
 * by a dynamic `import()`, so that nothing of the module is fetched (and none of it is required at
 * build time by an application that does not reach this file) unless legend-lite is used.
 */

import engineModule from '@legend-lite/planner-wasm/classes.wasm';
import type {
  LegendLiteEngineConnection,
  LegendLiteEngineResponse,
} from './LegendLiteEngineTransport.js';

// NOTE: each URL is created on its own, as we do for DuckDB's worker: written as
// `new Worker(new URL(...))`, the bundler would bundle the worker's script instead of emitting the file
const WORKER_URL = new URL(
  './LegendLiteEngine.worker.js',
  import.meta.url,
).toString();
const RUNTIME_URL = new URL(
  '@legend-lite/planner-wasm/wasm-gc-module-runtime.js',
  import.meta.url,
).toString();

export const createLegendLiteEngineConnection =
  (): LegendLiteEngineConnection => {
    const worker = new Worker(WORKER_URL, { type: 'module' });
    return {
      port: {
        send: (request) => worker.postMessage(request),
        listen: (onResponse, onFailure) => {
          worker.onmessage = (event) =>
            onResponse(event.data as LegendLiteEngineResponse);
          worker.onerror = (event) => onFailure(event.message);
        },
        close: () => worker.terminate(),
      },
      runtimeUrl: RUNTIME_URL,
      // absolute, since the worker would resolve a relative URL against its own
      moduleUrl: new URL(engineModule as string, document.baseURI).toString(),
    };
  };
