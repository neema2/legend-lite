// The shared saved-query fixture (fixtures/saved-queries): the records legend-lite's query store
// answers, read as Query reads them -- each execution context resolves through Query's own rules
// (persist.ts contextOf) to what the fixture's README says, each content parses, its parameters are
// the ones its defaultParameterValues name, and the graph fetch is the one that answers objects.
// DataCube tests its own reader against the same files, so the two cannot drift.

import { strict as assert } from 'node:assert';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import { functionsCalled } from '../../pure-protocol/src/index.ts';
import { contextOf } from '../src/app/persist.ts';
import type { LoadedProject } from '../src/app/context.ts';
import type { Query } from '../src/backend/wire.ts';
import { demoModel, grammar } from './lite.ts';
import { runfileNamed } from '../../tools/js/runfiles.mts';

const { context, graph } = await demoModel();
const project = {
  config: { groupId: 'demo', artifactId: 'trading', versionId: '0.0.0', models: [] },
  gav: 'demo:trading:0.0.0', context, graph,
} as LoadedProject;

const record = (file: string): Query =>
  JSON.parse(readFileSync(runfileNamed('SAVED_QUERIES', `${file}.json`), 'utf8')) as Query;

const EXPECTED: Readonly<Record<string, { mapping: string; runtime: string; parameters: string[]; objects: boolean }>> = {
  'explicit-context': { mapping: 'demo::trading::TradingMapping', runtime: 'demo::trading::Runtime', parameters: [], objects: false },
  'data-space-context': { mapping: 'demo::trading::TradingMapping', runtime: 'demo::trading::Runtime', parameters: [], objects: false },
  'default-parameter-values': { mapping: 'demo::trading::TradingMapping', runtime: 'demo::trading::Runtime', parameters: ['minQty'], objects: false },
  'graph-fetch': { mapping: 'demo::trading::TradingMapping', runtime: 'demo::trading::Runtime', parameters: [], objects: true },
};

describe('the shared saved-query fixture, read as Query reads it', () => {
  for (const [file, want] of Object.entries(EXPECTED)) {
    it(file, async () => {
      const q = record(file);
      assert.equal(`${q.groupId}:${q.artifactId}:${q.versionId}`, project.gav);
      const ctx = contextOf(project, q);
      assert.equal(ctx.mapping, want.mapping);
      assert.equal(ctx.runtime, want.runtime);
      const lambda = await grammar.lambdaJson(q.content);
      assert.deepEqual(lambda.parameters.map((p) => p.name), want.parameters);
      assert.deepEqual((q.defaultParameterValues ?? []).map((p) => p.name), want.parameters);
      const called = [...functionsCalled(lambda)].map((f) => f.slice(f.lastIndexOf(':') + 1));
      assert.equal(called.includes('graphFetch'), want.objects, 'answers objects (a graph fetch), not rows');
      assert.ok(!called.includes('from'), 'the content carries no ->from(): the execution context does');
    });
  }

  it('the data space record names its context; it resolves through the data space', () => {
    const q = record('data-space-context');
    assert.equal(q.executionContext?._type, 'dataSpaceExecutionContext');
    assert.deepEqual(contextOf(project, q).dataSpace, { path: 'demo::trading::TradingDataSpace', context: 'Production' });
  });
});
