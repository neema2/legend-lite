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

import { V1_EngineServerClient } from '@finos/legend-graph';
import {
  ContentType,
  HttpHeader,
  HttpMethod,
  HttpStatus,
  NetworkClientError,
  createRequestHeaders,
  isPlainObject,
  isString,
  makeUrl,
  type Parameters,
  type Payload,
  type PlainObject,
  type RequestHeaders,
  type RequestProcessConfig,
  type ResponseProcessConfig,
} from '@finos/legend-shared';
import type { LegendLiteEngineAnswer } from './LegendLiteEngineTransport.js';

/**
 * The engine server client, answered by legend-lite's engine module in the browser instead of by the
 * engine server.
 *
 * Every method of the engine server client ends in `request()`. This overrides that one method to ask
 * the module, so each method keeps its name, its input, its output and its errors, and a caller chooses
 * a client and nothing else changes.
 *
 * - Nothing here is ever sent to a server: `request()` never reaches the network.
 * - A route legend-lite does not have answers as a server would: with an error status, i.e. a
 *   `NetworkClientError`. NOTE: by design, nothing falls back to the engine server, so that what
 *   legend-lite cannot do is seen rather than hidden.
 * - Whatever must be adjusted in a request for legend-lite to accept it is adjusted here, so that
 *   callers send what they would send the engine server.
 * - What only makes sense for a server is ignored: compression, credentials, tracing, and aborting
 *   a request.
 *
 * NOTE: this is experimental. DataCube creates this client only when one of legend-lite's settings is
 * turned on and a query is planned with it (see `LegendLiteDataCubePlugin`).
 */

// NOTE: this only gives the routes their paths (e.g. `/api/pure/v1/execution/generatePlan`), which is
// what legend-lite routes on. `.invalid` is reserved and never resolves, should a request ever escape.
const LEGEND_LITE_ENGINE_BASE_URL = 'http://legend-lite.invalid/api';

const GENERATE_PLAN_PATH_SUFFIX = '/pure/v1/execution/generatePlan';

const PROTOCOL_TYPE__MODEL_DATA = 'data';
const PROTOCOL_TYPE__RUNTIME = 'runtime';
const PROTOCOL_TYPE__CONNECTION = 'connection';
const PROTOCOL_TYPE__CONNECTION_POINTER = 'connectionPointer';
const PROTOCOL_TYPE__RELATIONAL_DATABASE_CONNECTION =
  'RelationalDatabaseConnection';

// a response with one of these statuses cannot have a body
const NULL_BODY_STATUSES: number[] = [
  HttpStatus.NO_CONTENT,
  HttpStatus.RESET_CONTENT,
  HttpStatus.NOT_MODIFIED,
];

/**
 * Moves each connection embedded in a runtime into an element of its own, which the runtime then
 * points at. The model given is left as is; a new one is returned.
 *
 * TEMPORARY: the model DataCube synthesizes for a DuckDB-backed source embeds its connection in the
 * runtime, and legend-lite does not plan over such a connection; nor does legend-lite read a relational
 * connection without `postProcessorWithParameter`, which we leave out when it is empty.
 * Remove this when legend-lite accepts both.
 */
export const liftEmbeddedConnections = (model: PlainObject): PlainObject => {
  if (
    model._type !== PROTOCOL_TYPE__MODEL_DATA ||
    !Array.isArray(model.elements)
  ) {
    return model;
  }
  const elements = model.elements as unknown[];
  const takenPaths = new Set(
    elements
      .filter(isPlainObject)
      .map((element) => `${String(element.package)}::${String(element.name)}`),
  );
  const liftedConnections: PlainObject[] = [];

  const liftConnection = (
    runtime: PlainObject,
    identifiedConnection: PlainObject,
  ): PlainObject => {
    const connection = identifiedConnection.connection;
    if (
      !isPlainObject(connection) ||
      connection._type === PROTOCOL_TYPE__CONNECTION_POINTER
    ) {
      return identifiedConnection;
    }
    const packagePath = String(runtime.package);
    const baseName = `${String(runtime.name)}__${String(identifiedConnection.id)}`;
    let name = baseName;
    for (let i = 1; takenPaths.has(`${packagePath}::${name}`); i++) {
      name = `${baseName}_${i}`;
    }
    takenPaths.add(`${packagePath}::${name}`);
    liftedConnections.push({
      _type: PROTOCOL_TYPE__CONNECTION,
      name,
      package: packagePath,
      connectionValue:
        connection._type === PROTOCOL_TYPE__RELATIONAL_DATABASE_CONNECTION
          ? {
              ...connection,
              postProcessorWithParameter:
                connection.postProcessorWithParameter ?? [],
            }
          : connection,
    });
    return {
      ...identifiedConnection,
      connection: {
        _type: PROTOCOL_TYPE__CONNECTION_POINTER,
        connection: `${packagePath}::${name}`,
      },
    };
  };

  const liftedElements = elements.map((element) => {
    if (
      !isPlainObject(element) ||
      element._type !== PROTOCOL_TYPE__RUNTIME ||
      !isPlainObject(element.runtimeValue) ||
      !Array.isArray(element.runtimeValue.connections)
    ) {
      return element;
    }
    const runtime = element;
    const runtimeValue = element.runtimeValue;
    return {
      ...runtime,
      runtimeValue: {
        ...runtimeValue,
        connections: (runtimeValue.connections as unknown[]).map(
          (storeConnections) =>
            isPlainObject(storeConnections) &&
            Array.isArray(storeConnections.storeConnections)
              ? {
                  ...storeConnections,
                  storeConnections: (
                    storeConnections.storeConnections as unknown[]
                  ).map((identifiedConnection) =>
                    isPlainObject(identifiedConnection)
                      ? liftConnection(runtime, identifiedConnection)
                      : identifiedConnection,
                  ),
                }
              : storeConnections,
        ),
      },
    };
  });

  return liftedConnections.length
    ? { ...model, elements: [...liftedElements, ...liftedConnections] }
    : model;
};

/**
 * Adjusts what a route is sent so that legend-lite accepts it.
 *
 * TEMPORARY: see `liftEmbeddedConnections()`.
 */
const adjustRequestData = (path: string, data: unknown): unknown =>
  path.endsWith(GENERATE_PLAN_PATH_SUFFIX) &&
  isPlainObject(data) &&
  isPlainObject(data.model)
    ? { ...data, model: liftEmbeddedConnections(data.model) }
    : data;

/** What the client needs to reach legend-lite's engine module (see `LegendLiteEngineTransport`). */
export interface LegendLiteEngineRouter {
  pureV1(
    path: string,
    query: string,
    body: string,
  ): Promise<LegendLiteEngineAnswer>;
  dispose(): void;
}

export class LegendLiteEngineClient extends V1_EngineServerClient {
  private readonly _router: LegendLiteEngineRouter;

  constructor(router: LegendLiteEngineRouter) {
    super({ baseUrl: LEGEND_LITE_ENGINE_BASE_URL });
    this._router = router;
  }

  /** Stops the engine module; the next request starts it again. */
  dispose(): void {
    this._router.dispose();
  }

  /**
   * Asks legend-lite's engine module what would be asked of the engine server, and gives its answer
   * the way the answer of the engine server is given.
   *
   * @throws a `NetworkClientError` when the route answers with an error status, as a server would;
   * and a `LegendLiteEngineError` when the module could not be asked at all
   */
  override async request<T>(
    method: HttpMethod,
    url: string,
    data: unknown,
    options: RequestInit,
    headers?: RequestHeaders | undefined,
    parameters?: Parameters | undefined,
    requestProcessConfig?: RequestProcessConfig | undefined,
    responseProcessConfig?: ResponseProcessConfig | undefined,
  ): Promise<T> {
    const requestUrl = new URL(makeUrl(this.baseUrl, url, parameters ?? {}));
    const path = requestUrl.pathname;
    const adjustedData = adjustRequestData(path, data);
    const answer = await this._router.pureV1(
      path,
      requestUrl.search.substring(1),
      method === HttpMethod.GET ||
        adjustedData === undefined ||
        adjustedData === null
        ? ''
        : isString(adjustedData)
          ? adjustedData
          : JSON.stringify(adjustedData),
    );

    const response = new Response(
      NULL_BODY_STATUSES.includes(answer.status) ? null : answer.body,
      {
        status: answer.status,
        headers: { [HttpHeader.CONTENT_TYPE]: answer.contentType },
      },
    );
    // NOTE: from here, this is what the network client does with the response of a server
    if (!response.ok) {
      let payload: Payload | undefined = answer.body;
      try {
        payload = JSON.parse(answer.body) as Payload;
      } catch {
        // the body is not JSON: it is given as it is
      }
      throw new NetworkClientError(response, payload);
    }
    if (responseProcessConfig?.skipProcessing) {
      return response as unknown as T;
    }
    if (answer.status === HttpStatus.NO_CONTENT) {
      return undefined as unknown as T;
    }
    const accept = createRequestHeaders(method, headers).Accept;
    switch (accept) {
      case ContentType.APPLICATION_JSON:
        return JSON.parse(answer.body) as T;
      case ContentType.TEXT_PLAIN:
        return answer.body as unknown as T;
      default:
        throw new NetworkClientError(
          response,
          `Can't process response for request with Content-Type '${accept}'`,
        );
    }
  }
}
