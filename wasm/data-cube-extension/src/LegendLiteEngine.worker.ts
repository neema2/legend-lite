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
 * The worker that holds legend-lite's engine module (see `LegendLiteEngineTransport.ts`).
 *
 * IMPORTANT: this file must stay self-contained, i.e. it must import nothing at runtime (type imports are
 * erased). It is served as a file of its own and started as a module worker; it is not bundled.
 */

import type {
  LegendLiteEngineRequest,
  LegendLiteEngineResponse,
} from './LegendLiteEngineTransport.js';

type EngineModule = {
  exports: {
    pureV1OrError(path: string, rawQuery: string, body: string): string;
  };
};

type EngineModuleRuntime = {
  load(
    moduleUrl: string,
    options: {
      stackDeobfuscator: { enabled: boolean };
      installImports(imports: Record<string, unknown>): void;
    },
  ): Promise<EngineModule>;
};

const scope = globalThis as unknown as {
  onmessage: ((event: { data: LegendLiteEngineRequest }) => void) | null;
  postMessage(response: LegendLiteEngineResponse): void;
};

// the module is loaded once, by the first request, and kept for as long as this worker lives
let engineModule: Promise<EngineModule> | undefined;

const loadEngineModule = async (
  runtimeUrl: string,
  moduleUrl: string,
): Promise<EngineModule> => {
  // NOTE: the runtime is loaded from its URL by the browser itself. It must not be bundled: it decides
  // where it is running by checking for Node's globals, and it imports by computed names
  const runtime = (await import(
    /* webpackIgnore: true */ runtimeUrl
  )) as EngineModuleRuntime;
  return runtime.load(moduleUrl, {
    stackDeobfuscator: { enabled: false },
    installImports(imports) {
      // the module writes its diagnostics one character at a time; what matters comes back in its answers
      imports.teavmConsole = {
        putcharStdout: () => {},
        putcharStderr: () => {},
      };
    },
  });
};

scope.onmessage = (event) => {
  const { id, runtimeUrl, moduleUrl, path, query, body } = event.data;
  engineModule ??= loadEngineModule(runtimeUrl, moduleUrl);
  engineModule
    .then((module) =>
      scope.postMessage({
        id,
        ok: true,
        answer: module.exports.pureV1OrError(path, query, body),
      }),
    )
    .catch((error: unknown) => {
      // a module that failed to load, or that failed while answering, is not kept: nothing says it can
      // still answer, so the next request loads it again
      engineModule = undefined;
      scope.postMessage({
        id,
        ok: false,
        error: error instanceof Error ? error.message : String(error),
      });
    });
};
