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
import { describe, expect, test } from '@jest/globals';
import {
  LEGEND_LITE_ENGINE_ERROR_KIND,
  LegendLiteEngineTransport,
  LegendLiteEngineError,
  unfoldLegendLiteEngineAnswer,
  type LegendLiteEngineConnection,
  type LegendLiteEngineRequest,
  type LegendLiteEngineResponse,
} from '../LegendLiteEngineTransport.js';

/** What stands for the worker: it answers only when the test tells it to. */
class TEST__EnginePort {
  readonly requests: LegendLiteEngineRequest[] = [];
  closed = false;
  private _onResponse?: (response: LegendLiteEngineResponse) => void;
  private _onFailure?: (message: string) => void;

  send(request: LegendLiteEngineRequest): void {
    this.requests.push(request);
  }

  listen(
    onResponse: (response: LegendLiteEngineResponse) => void,
    onFailure: (message: string) => void,
  ): void {
    this._onResponse = onResponse;
    this._onFailure = onFailure;
  }

  close(): void {
    this.closed = true;
  }

  respond(response: LegendLiteEngineResponse): void {
    this._onResponse?.(response);
  }

  fail(message: string): void {
    this._onFailure?.(message);
  }
}

const TEST__setup = () => {
  const ports: TEST__EnginePort[] = [];
  const client = new LegendLiteEngineTransport(
    async (): Promise<LegendLiteEngineConnection> => {
      const port = new TEST__EnginePort();
      ports.push(port);
      return {
        port,
        runtimeUrl: 'http://test/runtime.js',
        moduleUrl: 'http://test/classes.wasm',
      };
    },
  );
  return { client, ports };
};

/** Lets the promises that are already settled run their reactions. */
const TEST__flush = async (): Promise<void> => {
  for (let i = 0; i < 5; i++) {
    await Promise.resolve();
  }
};

const TEST__getPort = (
  ports: TEST__EnginePort[],
  index = 0,
): TEST__EnginePort => {
  const port = ports[index];
  if (!port) {
    throw new Error(`No worker was created at index ${index}`);
  }
  return port;
};

const TEST__getRequest = (
  port: TEST__EnginePort,
  index = 0,
): LegendLiteEngineRequest => {
  const request = port.requests[index];
  if (!request) {
    throw new Error(`No request was sent at index ${index}`);
  }
  return request;
};

describe(unitTest('legend-lite engine transport: answer'), () => {
  test('an answered route gives its status, content type and body', () => {
    expect(
      unfoldLegendLiteEngineAnswer('OK\n200\napplication/json\n{"a":1}'),
    ).toEqual({
      status: 200,
      contentType: 'application/json',
      body: '{"a":1}',
    });
  });

  test('a body keeps its own line breaks', () => {
    expect(
      unfoldLegendLiteEngineAnswer('OK\n200\ntext/plain\nline 1\nline 2\n')
        .body,
    ).toBe('line 1\nline 2\n');
  });

  test('a body may be empty', () => {
    expect(unfoldLegendLiteEngineAnswer('OK\n204\ntext/plain\n')).toEqual({
      status: 204,
      contentType: 'text/plain',
      body: '',
    });
  });

  test('a route that refuses is an answer, not an error', () => {
    expect(
      unfoldLegendLiteEngineAnswer(
        'OK\n500\napplication/json\n{"message":"refused"}',
      ).status,
    ).toBe(500);
  });

  test('a failed call throws with the kind and the message legend-lite gave', () => {
    let thrown: unknown;
    try {
      unfoldLegendLiteEngineAnswer('ERR\ninternal\nline 1\nline 2');
    } catch (error) {
      thrown = error;
    }
    expect(thrown).toBeInstanceOf(LegendLiteEngineError);
    expect((thrown as LegendLiteEngineError).kind).toBe('internal');
    expect((thrown as LegendLiteEngineError).message).toBe(
      'line 1\nline 2',
    );
  });

  test.each([
    ['nothing', ''],
    ['an unknown first line', 'WHAT\n200\n'],
    ['no content type', 'OK\n200'],
    ['no body separator', 'OK\n200\napplication/json'],
    ['a status that is not a number', 'OK\nfine\ntext/plain\nbody'],
  ])('an answer with %s is refused', (_, raw) => {
    let thrown: unknown;
    try {
      unfoldLegendLiteEngineAnswer(raw);
    } catch (error) {
      thrown = error;
    }
    expect(thrown).toBeInstanceOf(LegendLiteEngineError);
    expect((thrown as LegendLiteEngineError).kind).toBe(
      LEGEND_LITE_ENGINE_ERROR_KIND.MALFORMED_ANSWER,
    );
  });
});

describe(unitTest('legend-lite engine transport: requests'), () => {
  test('no worker is created until the first request', async () => {
    const { client, ports } = TEST__setup();
    await TEST__flush();
    expect(ports).toHaveLength(0);

    const answer = client.pureV1('/api/pure/v1/a', '', 'body');
    await TEST__flush();
    expect(ports).toHaveLength(1);

    const port = TEST__getPort(ports);
    port.respond({
      id: TEST__getRequest(port).id,
      ok: true,
      answer: 'OK\n200\nt\nb',
    });
    await answer;
  });

  test('a request carries the route and where the module is', async () => {
    const { client, ports } = TEST__setup();
    const answer = client.pureV1(
      '/api/pure/v1/execution/generatePlan',
      'x=1',
      '{"a":1}',
    );
    await TEST__flush();

    const port = TEST__getPort(ports);
    const request = TEST__getRequest(port);
    expect(request).toEqual({
      id: request.id,
      runtimeUrl: 'http://test/runtime.js',
      moduleUrl: 'http://test/classes.wasm',
      path: '/api/pure/v1/execution/generatePlan',
      query: 'x=1',
      body: '{"a":1}',
    });

    port.respond({
      id: request.id,
      ok: true,
      answer: 'OK\n200\napplication/json\n{}',
    });
    expect(await answer).toEqual({
      status: 200,
      contentType: 'application/json',
      body: '{}',
    });
  });

  test('answers arriving out of order reach their own requests', async () => {
    const { client, ports } = TEST__setup();
    const first = client.pureV1('/first', '', '');
    const second = client.pureV1('/second', '', '');
    await TEST__flush();

    const port = TEST__getPort(ports);
    expect(ports).toHaveLength(1);
    const firstRequest = TEST__getRequest(port, 0);
    const secondRequest = TEST__getRequest(port, 1);
    expect(firstRequest.id).not.toBe(secondRequest.id);

    port.respond({
      id: secondRequest.id,
      ok: true,
      answer: 'OK\n200\nt\nsecond',
    });
    port.respond({
      id: firstRequest.id,
      ok: true,
      answer: 'OK\n200\nt\nfirst',
    });
    expect((await first).body).toBe('first');
    expect((await second).body).toBe('second');
  });

  test('an answer to an unknown request is ignored', async () => {
    const { client, ports } = TEST__setup();
    const answer = client.pureV1('/a', '', '');
    await TEST__flush();

    const port = TEST__getPort(ports);
    port.respond({ id: -1, ok: true, answer: 'OK\n200\nt\nstray' });
    port.respond({
      id: TEST__getRequest(port).id,
      ok: true,
      answer: 'OK\n200\nt\nmine',
    });
    expect((await answer).body).toBe('mine');
  });

  test('a request the worker could not ask is rejected, and the worker is kept', async () => {
    const { client, ports } = TEST__setup();
    const failed = client.pureV1('/a', '', '');
    await TEST__flush();

    const port = TEST__getPort(ports);
    port.respond({
      id: TEST__getRequest(port).id,
      ok: false,
      error: 'could not load',
    });
    await expect(failed).rejects.toMatchObject({
      kind: LEGEND_LITE_ENGINE_ERROR_KIND.WORKER,
      message: 'could not load',
    });

    const next = client.pureV1('/b', '', '');
    await TEST__flush();
    expect(ports).toHaveLength(1);
    port.respond({
      id: TEST__getRequest(port, 1).id,
      ok: true,
      answer: 'OK\n200\nt\nb',
    });
    await next;
  });

  test('a failed worker rejects what is waiting, is closed, and is replaced by the next request', async () => {
    const { client, ports } = TEST__setup();
    const first = client.pureV1('/first', '', '');
    const second = client.pureV1('/second', '', '');
    await TEST__flush();

    const port = TEST__getPort(ports);
    port.fail('script error');
    await expect(first).rejects.toMatchObject({
      kind: LEGEND_LITE_ENGINE_ERROR_KIND.WORKER,
    });
    await expect(second).rejects.toBeInstanceOf(LegendLiteEngineError);
    expect(port.closed).toBe(true);

    const next = client.pureV1('/next', '', '');
    await TEST__flush();
    expect(ports).toHaveLength(2);
    const newPort = TEST__getPort(ports, 1);
    newPort.respond({
      id: TEST__getRequest(newPort).id,
      ok: true,
      answer: 'OK\n200\nt\nnext',
    });
    expect((await next).body).toBe('next');
  });

  test('a worker that cannot be created fails the request, and the next request tries again', async () => {
    let attempts = 0;
    const port = new TEST__EnginePort();
    const client = new LegendLiteEngineTransport(async () => {
      attempts++;
      if (attempts === 1) {
        throw new Error('no worker');
      }
      return { port, runtimeUrl: 'r', moduleUrl: 'm' };
    });

    await expect(client.pureV1('/a', '', '')).rejects.toThrow('no worker');

    const next = client.pureV1('/b', '', '');
    await TEST__flush();
    expect(attempts).toBe(2);
    port.respond({
      id: TEST__getRequest(port).id,
      ok: true,
      answer: 'OK\n200\nt\nb',
    });
    expect((await next).body).toBe('b');
  });

  test('stopping the client rejects what is waiting and closes the worker', async () => {
    const { client, ports } = TEST__setup();
    const waiting = client.pureV1('/a', '', '');
    await TEST__flush();

    client.dispose();
    await expect(waiting).rejects.toBeInstanceOf(LegendLiteEngineError);
    await TEST__flush();
    expect(TEST__getPort(ports).closed).toBe(true);
  });
});
