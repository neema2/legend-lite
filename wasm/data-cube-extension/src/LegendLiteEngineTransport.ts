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
 * The way to legend-lite's engine module: a reimplementation of the Legend compiler, compiled to
 * WebAssembly, which answers routes of the engine's `pure/v1` API in the browser instead of on the
 * engine server.
 *
 * The module runs in a worker (see `LegendLiteEngine.worker.ts`), because loading it is long synchronous
 * work that would freeze the page. This sends the worker one request per call and matches each answer
 * to its request by id. It knows nothing of what a route takes or gives: see `LegendLiteEngineClient`
 * for that, which is what the rest of the application should use.
 */

/** What the worker is sent: one `pure/v1` request, and where the module's two files are. */
export type LegendLiteEngineRequest = {
  id: number;
  /** URL of the module's runtime (`wasm-gc-module-runtime.js`), an ES module */
  runtimeUrl: string;
  /** URL of the WebAssembly module (`classes.wasm`) */
  moduleUrl: string;
  /** The route's full path, e.g. `/api/pure/v1/execution/generatePlan` */
  path: string;
  /** The route's query string without the leading `?`; empty for none */
  query: string;
  body: string;
};

/** What the worker sends back: the module's raw answer, or why the worker could not ask it. */
export type LegendLiteEngineResponse =
  | { id: number; ok: true; answer: string }
  | { id: number; ok: false; error: string };

/**
 * What the client needs of a worker. The real worker is wrapped into one of these where it is
 * created (see `LegendLiteEngineAssets.ts`); tests provide their own.
 */
export interface LegendLiteEnginePort {
  send(request: LegendLiteEngineRequest): void;
  listen(
    onResponse: (response: LegendLiteEngineResponse) => void,
    onFailure: (message: string) => void,
  ): void;
  close(): void;
}

export type LegendLiteEngineConnection = {
  port: LegendLiteEnginePort;
  runtimeUrl: string;
  moduleUrl: string;
};

/** A route's answer: what the engine server would have sent over HTTP. */
export type LegendLiteEngineAnswer = {
  status: number;
  contentType: string;
  body: string;
};

export enum LEGEND_LITE_ENGINE_ERROR_KIND {
  /** The worker failed, or could not load the module */
  WORKER = 'worker',
  /** The module's answer is not in the form it documents */
  MALFORMED_ANSWER = 'malformed-answer',
}

/**
 * A failure of legend-lite itself, as opposed to a route answering with an error status
 * (which is an answer, see {@link LegendLiteEngineAnswer}).
 *
 * NOTE: the message does not name legend-lite: DataCube prefixes the message of any failure of a
 * planner with the planner's label (`[legend-lite] ...`).
 */
export class LegendLiteEngineError extends Error {
  /** One of {@link LEGEND_LITE_ENGINE_ERROR_KIND}, or the kind the module itself reported */
  readonly kind: string;

  constructor(kind: string, message: string) {
    super(message);
    this.name = 'LegendLiteEngineError';
    this.kind = kind;
  }
}

const ANSWER_PREFIX__OK = 'OK\n';
const ANSWER_PREFIX__ERROR = 'ERR\n';

/**
 * Reads the module's answer. The module answers every call with one string:
 *
 * - `OK\n<status>\n<content type>\n<body>` when the route answered, whatever its status
 * - `ERR\n<kind>\n<message>` when the call itself failed
 *
 * @throws a {@link LegendLiteEngineError} for the second form, and for anything in neither form
 */
export const unfoldLegendLiteEngineAnswer = (
  raw: string,
): LegendLiteEngineAnswer => {
  if (raw.startsWith(ANSWER_PREFIX__OK)) {
    const statusEnd = raw.indexOf('\n', ANSWER_PREFIX__OK.length);
    const contentTypeEnd =
      statusEnd < 0 ? -1 : raw.indexOf('\n', statusEnd + 1);
    const statusText =
      statusEnd < 0 ? '' : raw.slice(ANSWER_PREFIX__OK.length, statusEnd);
    if (contentTypeEnd < 0 || !/^\d{3}$/.test(statusText)) {
      throw new LegendLiteEngineError(
        LEGEND_LITE_ENGINE_ERROR_KIND.MALFORMED_ANSWER,
        `Can't read the answer of legend-lite: expected a status, a content type and a body`,
      );
    }
    return {
      status: Number(statusText),
      contentType: raw.slice(statusEnd + 1, contentTypeEnd),
      body: raw.slice(contentTypeEnd + 1),
    };
  }
  if (raw.startsWith(ANSWER_PREFIX__ERROR)) {
    const kindEnd = raw.indexOf('\n', ANSWER_PREFIX__ERROR.length);
    throw new LegendLiteEngineError(
      kindEnd < 0
        ? raw.slice(ANSWER_PREFIX__ERROR.length)
        : raw.slice(ANSWER_PREFIX__ERROR.length, kindEnd),
      kindEnd < 0 ? '' : raw.slice(kindEnd + 1),
    );
  }
  const firstLineEnd = raw.indexOf('\n');
  throw new LegendLiteEngineError(
    LEGEND_LITE_ENGINE_ERROR_KIND.MALFORMED_ANSWER,
    `Can't read the answer of legend-lite: it starts with '${(firstLineEnd < 0
      ? raw
      : raw.slice(0, firstLineEnd)
    ).slice(0, 100)}'`,
  );
};

type PendingRequest = {
  resolve: (answer: string) => void;
  reject: (error: Error) => void;
};

export class LegendLiteEngineTransport {
  private readonly _connect: () => Promise<LegendLiteEngineConnection>;
  private _connection?: Promise<LegendLiteEngineConnection> | undefined;
  private _nextRequestId = 1;
  private readonly _pendingRequests = new Map<number, PendingRequest>();

  /**
   * @param connect creates the worker; called on the first request, not here, so that
   * nothing of the module is fetched until it is used
   */
  constructor(connect: () => Promise<LegendLiteEngineConnection>) {
    this._connect = connect;
  }

  /**
   * Asks the module one route of the engine's `pure/v1` API.
   *
   * The answer's status is the route's: a refusal (e.g. `400`, `500`) is returned, not thrown.
   *
   * @throws a {@link LegendLiteEngineError} when the worker fails or the module cannot answer at all
   */
  async pureV1(
    path: string,
    query: string,
    body: string,
  ): Promise<LegendLiteEngineAnswer> {
    const connection = await this._getConnection();
    const id = this._nextRequestId++;
    const raw = await new Promise<string>((resolve, reject) => {
      this._pendingRequests.set(id, { resolve, reject });
      connection.port.send({
        id,
        runtimeUrl: connection.runtimeUrl,
        moduleUrl: connection.moduleUrl,
        path,
        query,
        body,
      });
    });
    return unfoldLegendLiteEngineAnswer(raw);
  }

  /** Stops the worker. Requests still waiting are rejected; the next request starts a new worker. */
  dispose(): void {
    this._reset(`legend-lite was stopped`);
  }

  private _getConnection(): Promise<LegendLiteEngineConnection> {
    if (!this._connection) {
      const connection = this._connect().then((result) => {
        result.port.listen(
          (response) => this._settle(response),
          (message) => this._reset(message, result.port),
        );
        return result;
      });
      // a worker that could not be created is not remembered: the next request tries again
      connection.catch(() => {
        if (this._connection === connection) {
          this._connection = undefined;
        }
      });
      this._connection = connection;
    }
    return this._connection;
  }

  private _settle(response: LegendLiteEngineResponse): void {
    const pending = this._pendingRequests.get(response.id);
    if (!pending) {
      return;
    }
    this._pendingRequests.delete(response.id);
    if (response.ok) {
      pending.resolve(response.answer);
    } else {
      pending.reject(
        new LegendLiteEngineError(
          LEGEND_LITE_ENGINE_ERROR_KIND.WORKER,
          response.error,
        ),
      );
    }
  }

  /**
   * Rejects what is waiting and lets go of the worker, so that the next request starts a new one.
   * A worker that reported a failure is not asked again: nothing says it can still answer.
   */
  private _reset(message: string, failedPort?: LegendLiteEnginePort): void {
    const connection = this._connection;
    this._connection = undefined;
    const pendingRequests = Array.from(this._pendingRequests.values());
    this._pendingRequests.clear();
    pendingRequests.forEach((pending) =>
      pending.reject(
        new LegendLiteEngineError(
          LEGEND_LITE_ENGINE_ERROR_KIND.WORKER,
          message,
        ),
      ),
    );
    if (failedPort) {
      failedPort.close();
    } else {
      connection?.then((result) => result.port.close()).catch(() => {});
    }
  }
}
