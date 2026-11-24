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

import { unitTest } from '@finos/legend-shared/test';
import { beforeAll, describe, expect, test } from '@jest/globals';
import {
  ContentType,
  HttpHeader,
  HttpMethod,
  NetworkClientError,
  isPlainObject,
  type PlainObject,
} from '@finos/legend-shared';
import {
  DatabaseType,
  PackageableElementPointerType,
  V1_Column,
  V1_Database,
  V1_DuckDBDatasourceSpecification,
  V1_EngineRuntime,
  V1_EngineServerClient,
  V1_IdentifiedConnection,
  V1_PackageableElementPointer,
  V1_PackageableRuntime,
  V1_PureModelContextData,
  V1_RelationalDatabaseConnection,
  V1_Schema,
  V1_StoreConnections,
  V1_Table,
  V1_TestAuthenticationStrategy,
  V1_VarChar,
  V1_serializePureModelContextData,
} from '@finos/legend-graph';
import { TEST__getTestGraphManagerState } from '@finos/legend-graph/test';
import {
  LEGEND_LITE_ENGINE_ERROR_KIND,
  LegendLiteEngineError,
  type LegendLiteEngineAnswer,
} from '../LegendLiteEngineTransport.js';
import {
  LegendLiteEngineClient,
  liftEmbeddedConnections,
} from '../LegendLiteEngineClient.js';

/**
 * jsdom has no `Response`, which the client makes of the answer of legend-lite, the way the network
 * client is given one by `fetch()`. This has what the client and its errors read of it.
 */
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

// the serializers of the model are set up when a graph manager is initialized, as they are in the application
beforeAll(async () => {
  (globalThis as { Response?: unknown }).Response ??= TEST__Response;
  await TEST__getTestGraphManagerState().graphManager.initialize({
    env: 'test',
    tabSize: 2,
    clientConfig: {},
  });
});

/**
 * The model DataCube synthesizes for a DuckDB-backed source, built and serialized the way
 * `LegendDataCubeDataCubeEngine._synthesizeMinimalModelContext()` does.
 */
const TEST__synthesizeMinimalModel = (): PlainObject => {
  const column = new V1_Column();
  column.name = 'region';
  column.type = new V1_VarChar();
  const table = new V1_Table();
  table.name = 'cache1';
  table.columns = [column];
  const schema = new V1_Schema();
  schema.name = 'main';
  schema.tables = [table];
  const database = new V1_Database();
  database.name = 'db';
  database.package = 'local';
  database.schemas = [schema];

  const connection = new V1_RelationalDatabaseConnection();
  connection.databaseType = DatabaseType.DuckDB;
  connection.type = DatabaseType.DuckDB;
  const dataSourceSpec = new V1_DuckDBDatasourceSpecification();
  dataSourceSpec.path = '/tmpFile';
  connection.store = database.path;
  connection.datasourceSpecification = dataSourceSpec;
  connection.authenticationStrategy = new V1_TestAuthenticationStrategy();

  const runtime = new V1_EngineRuntime();
  const storeConnections = new V1_StoreConnections();
  storeConnections.store = new V1_PackageableElementPointer(
    PackageableElementPointerType.STORE,
    database.path,
  );
  const identifiedConnection = new V1_IdentifiedConnection();
  identifiedConnection.connection = connection;
  identifiedConnection.id = 'c0';
  storeConnections.storeConnections = [identifiedConnection];
  runtime.connections = [storeConnections];

  const packageableRuntime = new V1_PackageableRuntime();
  packageableRuntime.runtimeValue = runtime;
  packageableRuntime.package = 'local';
  packageableRuntime.name = 'rt';

  const model = new V1_PureModelContextData();
  model.elements = [database, packageableRuntime];
  return V1_serializePureModelContextData(model);
};

const TEST__getElements = (model: PlainObject): PlainObject[] =>
  (model.elements as unknown[]).filter(isPlainObject);

const TEST__getElement = (model: PlainObject, type: string): PlainObject => {
  const element = TEST__getElements(model).find((el) => el._type === type);
  if (!element) {
    throw new Error(`The model has no element of type '${type}'`);
  }
  return element;
};

/** The connection a runtime element holds at its first store's first connection. */
const TEST__getRuntimeConnection = (runtime: PlainObject): PlainObject =>
  (
    (
      ((runtime.runtimeValue as PlainObject).connections as PlainObject[])[0]
        ?.storeConnections as PlainObject[]
    )[0] as PlainObject
  ).connection as PlainObject;

describe(unitTest('legend-lite engine client: embedded connections'), () => {
  test('the synthesized model embeds its connection, without postProcessorWithParameter', () => {
    // NOTE: this is what the shim exists for; if this test fails, the model no longer
    // has the shape legend-lite refuses, and the shim may no longer be needed
    const connection = TEST__getRuntimeConnection(
      TEST__getElement(TEST__synthesizeMinimalModel(), 'runtime'),
    );
    expect(connection._type).toBe('RelationalDatabaseConnection');
    expect(connection).not.toHaveProperty('postProcessorWithParameter');
  });

  test('an embedded connection becomes an element the runtime points at', () => {
    const model = TEST__synthesizeMinimalModel();
    const embedded = TEST__getRuntimeConnection(
      TEST__getElement(model, 'runtime'),
    );
    const lifted = liftEmbeddedConnections(model);

    expect(TEST__getElements(lifted).map((el) => el._type)).toEqual([
      'relational',
      'runtime',
      'connection',
    ]);
    const connection = TEST__getElement(lifted, 'connection');
    expect(connection.package).toBe('local');
    expect(connection.name).toBe('rt__c0');
    expect(connection.connectionValue).toEqual({
      ...embedded,
      postProcessorWithParameter: [],
    });
    expect(
      TEST__getRuntimeConnection(TEST__getElement(lifted, 'runtime')),
    ).toEqual({ _type: 'connectionPointer', connection: 'local::rt__c0' });
    // everything else is carried over as it was
    expect(TEST__getElement(lifted, 'relational')).toEqual(
      TEST__getElement(model, 'relational'),
    );
  });

  test('the model given is not changed', () => {
    const model = TEST__synthesizeMinimalModel();
    const before = JSON.stringify(model);
    liftEmbeddedConnections(model);
    expect(JSON.stringify(model)).toBe(before);
  });

  test('a model with nothing embedded is returned as it is', () => {
    const lifted = liftEmbeddedConnections(TEST__synthesizeMinimalModel());
    expect(liftEmbeddedConnections(lifted)).toBe(lifted);
  });

  test('postProcessorWithParameter is kept when the connection has it', () => {
    const model = TEST__synthesizeMinimalModel();
    TEST__getRuntimeConnection(
      TEST__getElement(model, 'runtime'),
    ).postProcessorWithParameter = [{ pp: 'x' }];
    expect(
      (
        TEST__getElement(liftEmbeddedConnections(model), 'connection')
          .connectionValue as PlainObject
      ).postProcessorWithParameter,
    ).toEqual([{ pp: 'x' }]);
  });

  test('the new element does not take the path of an existing one', () => {
    const model = TEST__synthesizeMinimalModel();
    (model.elements as unknown[]).push({
      _type: 'class',
      package: 'local',
      name: 'rt__c0',
    });
    const lifted = liftEmbeddedConnections(model);
    expect(TEST__getElement(lifted, 'connection').name).toBe('rt__c0_1');
    expect(
      TEST__getRuntimeConnection(TEST__getElement(lifted, 'runtime'))
        .connection,
    ).toBe('local::rt__c0_1');
  });

  test('a model that is not model data is returned as it is', () => {
    const pointer = { _type: 'pointer', sdlcInfo: {} };
    expect(liftEmbeddedConnections(pointer)).toBe(pointer);
  });
});

describe(unitTest('legend-lite engine client: requests'), () => {
  /** What stands for the engine module: it answers every request the same way, and keeps what it was asked. */
  const TEST__setup = (answer: LegendLiteEngineAnswer) => {
    const calls: { path: string; query: string; body: string }[] = [];
    let disposed = false;
    const client = new LegendLiteEngineClient({
      pureV1: async (path: string, query: string, body: string) => {
        calls.push({ path, query, body });
        return answer;
      },
      dispose: () => {
        disposed = true;
      },
    });
    return { client, calls, isDisposed: () => disposed };
  };
  const TEST__json = (body: unknown, status = 200): LegendLiteEngineAnswer => ({
    status,
    contentType: 'application/json',
    body: JSON.stringify(body),
  });
  const TEST__input = () => ({
    clientVersion: 'vX_X_X',
    function: { _type: 'lambda', body: [], parameters: [] },
    model: TEST__synthesizeMinimalModel(),
    context: { _type: 'BaseExecutionContext' },
    parameterValues: [],
  });
  const TEST__catch = async (promise: Promise<unknown>): Promise<unknown> => {
    try {
      await promise;
    } catch (error) {
      return error;
    }
    throw new Error(`Expected the request to fail`);
  };

  test('it is an engine server client', () => {
    expect(TEST__setup(TEST__json({})).client).toBeInstanceOf(
      V1_EngineServerClient,
    );
  });

  test('generatePlan asks legend-lite the route of the engine, with the model made acceptable', async () => {
    const plan = { _type: 'simple', rootExecutionNode: {} };
    const { client, calls } = TEST__setup(TEST__json(plan));
    const input = TEST__input();

    expect(await client.generatePlan(input)).toEqual(plan);
    expect(calls).toHaveLength(1);
    expect(calls[0]?.path).toBe('/api/pure/v1/execution/generatePlan');
    expect(calls[0]?.query).toBe('');
    expect(JSON.parse(calls[0]?.body ?? '')).toEqual({
      ...input,
      model: liftEmbeddedConnections(input.model),
    });
  });

  test('a model that is not model data is sent as it is', async () => {
    const { client, calls } = TEST__setup(TEST__json({}));
    const pointer = { _type: 'pointer', sdlcInfo: {} };
    await client.generatePlan({ ...TEST__input(), model: pointer });
    expect((JSON.parse(calls[0]?.body ?? '') as PlainObject).model).toEqual(
      pointer,
    );
  });

  test('a method that was not written here works too: text in, query parameters, JSON out', async () => {
    const lambda = { _type: 'lambda', body: [], parameters: [] };
    const { client, calls } = TEST__setup(TEST__json(lambda));

    expect(
      await client.grammarToJSON_lambda(
        '|1 + 1',
        'source',
        undefined,
        undefined,
        false,
      ),
    ).toEqual(lambda);
    expect(calls).toEqual([
      {
        path: '/api/pure/v1/grammar/grammarToJson/lambda',
        // a parameter with no value is left out, as it is for the engine server
        query: 'sourceId=source&returnSourceInformation=false',
        body: '|1 + 1',
      },
    ]);
  });

  test('a route that answers text gives the text', async () => {
    const { client } = TEST__setup({
      status: 200,
      contentType: 'text/plain',
      body: '|1 + 1',
    });
    expect(
      await client.request(
        HttpMethod.POST,
        `${client.baseUrl}/pure/v1/grammar/jsonToGrammar/lambda`,
        {},
        {},
        { [HttpHeader.ACCEPT]: ContentType.TEXT_PLAIN },
      ),
    ).toBe('|1 + 1');
  });

  test('a request with no body sends none', async () => {
    const { client, calls } = TEST__setup(TEST__json({}));
    await client.request(
      HttpMethod.GET,
      `${client.baseUrl}/pure/v1/protocol/pure/getSubtypeInfo`,
      {},
      {},
    );
    expect(calls[0]?.body).toBe('');
  });

  test('a refusal is the error the engine server client throws, with the message of legend-lite, and legend-lite is asked once', async () => {
    const { client, calls } = TEST__setup(
      TEST__json(
        {
          code: -1,
          message: 'IllegalStateException: no row scope',
          status: 'error',
        },
        500,
      ),
    );

    const error = await TEST__catch(client.generatePlan(TEST__input()));
    expect(error).toBeInstanceOf(NetworkClientError);
    expect((error as NetworkClientError).response.status).toBe(500);
    expect((error as NetworkClientError).message).toBe(
      'IllegalStateException: no row scope',
    );
    expect((error as NetworkClientError).payload).toMatchObject({
      message: 'IllegalStateException: no row scope',
    });
    expect(calls).toHaveLength(1);
  });

  test('a route legend-lite does not have fails as it would on a server, and is not sent anywhere else', async () => {
    const { client, calls } = TEST__setup({
      status: 404,
      contentType: 'text/plain',
      body: 'no route',
    });

    const error = await TEST__catch(
      client.grammarToJSON_valueSpecification('1 + 1'),
    );
    expect(error).toBeInstanceOf(NetworkClientError);
    expect((error as NetworkClientError).response.status).toBe(404);
    expect((error as NetworkClientError).message).toBe('no route');
    // NOTE: a request to a server would have thrown: `fetch()` is blocked in tests
    expect(calls).toHaveLength(1);
  });

  test('a failure to ask legend-lite at all is not a network error', async () => {
    const client = new LegendLiteEngineClient({
      pureV1: async () => {
        throw new LegendLiteEngineError(
          LEGEND_LITE_ENGINE_ERROR_KIND.WORKER,
          'could not load',
        );
      },
      dispose: () => {},
    });
    const error = await TEST__catch(client.generatePlan(TEST__input()));
    expect(error).toBeInstanceOf(LegendLiteEngineError);
    expect(error).not.toBeInstanceOf(NetworkClientError);
  });

  test('stopping the client stops the engine module', () => {
    const { client, isDisposed } = TEST__setup(TEST__json({}));
    expect(isDisposed()).toBe(false);
    client.dispose();
    expect(isDisposed()).toBe(true);
  });
});
