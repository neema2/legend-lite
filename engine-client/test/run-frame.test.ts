// THE RUN FRAME (run-frame.ts, the witness, 2026-11-13): a planner entry asked inside a run of its own flow is refused by
// name; outside any run, after a run ends, and on another flow while one flow's run is open, it is not. The planner's
// in-thread answer and a worker's port are entries.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { NOT_PLANNING, currentRun, frameKept, inRun, planning } from '../src/run-frame.ts';
import { answer, type PlannerModule } from '../src/legend/planner-answer.ts';
import { WorkerPort } from '../src/legend/wasm-grammar.ts';
import { HttpEngine, RoutedEngine } from '../src/legend/engine.ts';

const REFUSED = /the planner was asked 'plan' inside a run \(a test's run\): a run sends its plan's statements and plans nothing \(G4\)/;

/** A module whose exports answer a plan and let a model go. */
const released: number[] = [];
const module = { exports: { planHeldOrError: () => 'OK\nSELECT 1',
  releaseModelOrError: (handle: number) => { released.push(handle); return 'OK\n'; } } } as unknown as PlannerModule;

describe('the run frame', () => {
  it('is kept under Node: the witness is not blind here', () => {
    assert.equal(frameKept, true);
  });

  it('refuses a planner entry inside a run, across its awaits, and not outside it or after it', async () => {
    planning('plan');
    await assert.rejects(inRun("a test's run", async () => {
      await new Promise((done) => setTimeout(done, 0));
      assert.equal(currentRun(), "a test's run");
      planning('plan');
    }), REFUSED);
    assert.equal(currentRun(), undefined, 'the run ended with it');
    planning('plan');
  });

  it('is the flow\'s: a second flow plans while the first flow\'s run waits', async () => {
    let open!: () => void;
    const gate = new Promise<void>((done) => { open = done; });
    const first = inRun("a test's run", async () => {
      await gate;
      planning('plan');
    });
    await new Promise((done) => setTimeout(done, 0));
    planning('plan');
    open();
    await assert.rejects(first, REFUSED);
  });

  it('lets a held model go inside a run: a release plans nothing (T6), and nothing else is let through', async () => {
    // the final audit of 9af16fbe58's T6: a release inside a run was refused, LitePlanner's let-go swallowed it, and
    // the model stayed held
    assert.deepEqual([...NOT_PLANNING], ['release']);
    await inRun("a test's run", async () => {
      planning('release');
      assert.equal(answer(module, { kind: 'release', handle: 7 }), 'OK\n');
    });
    assert.deepEqual(released, [7], 'the model was let go');
    await assert.rejects(inRun("a test's run", async () => answer(module, { kind: 'hold', model: 'm' })),
      /the planner was asked 'hold' inside a run/);
  });

  it('refuses a server engine\'s generatePlan inside a run, before anything is sent (T2)', async () => {
    const sent: string[] = [];
    const fetcher = (async (url: string) => {
      sent.push(String(url));
      return new Response('{}', { status: 200, headers: { 'Content-Type': 'application/json' } });
    }) as unknown as typeof fetch;
    const http = new HttpEngine('http://engine.invalid', fetcher);
    const routed = new RoutedEngine({} as never, http);
    const input = { function: {}, model: {}, runtime: {} } as never;
    for (const engine of [http, routed]) {
      await assert.rejects(inRun("a test's run", async () => engine.generatePlan(input)),
        /the planner was asked 'generatePlan' inside a run \(a test's run\)/);
    }
    assert.deepEqual(sent, [], 'nothing sent to the server inside the run');
  });

  it('refuses a server engine\'s typing inside a run, before anything is sent (the last list, T2\'s neighbours)', async () => {
    const sent: string[] = [];
    const fetcher = (async (url: string) => {
      sent.push(String(url));
      return new Response('{}', { status: 200, headers: { 'Content-Type': 'application/json' } });
    }) as unknown as typeof fetch;
    const http = new HttpEngine('http://engine.invalid', fetcher);
    const routed = new RoutedEngine({} as never, http);
    const model = { _type: 'text', code: '' } as never;
    const lambda = {} as never;
    for (const engine of [http, routed]) {
      const asks: [string, () => Promise<unknown>][] = [
        ['relationType', () => engine.relationType(model, lambda)],
        ['compile', () => engine.compile(model)],
        ['returnType', () => engine.returnType(model, lambda)],
      ];
      if (engine === http) asks.push(['compileErrors', () => http.compileErrors('')]);
      for (const [kind, ask] of asks) {
        await assert.rejects(inRun("a test's run", async () => ask()),
          new RegExp(`the planner was asked '${kind}' inside a run \\(a test's run\\)`), kind);
      }
    }
    assert.deepEqual(sent, [], 'nothing sent to the server inside the run');
  });

  it('refuses the planner\'s in-thread answer inside a run, and answers it outside one', async () => {
    const asked = { kind: 'plan', handle: 1, query: 'a', runtime: 'r' } as const;
    await assert.rejects(inRun("a test's run", async () => answer(module, asked)), REFUSED);
    assert.equal(answer(module, asked), 'OK\nSELECT 1');
  });

  it('refuses a worker\'s port inside a run, before the request is posted', async () => {
    const g = globalThis as unknown as { Worker?: unknown };
    const posted: unknown[] = [];
    g.Worker = class {
      onmessage: unknown = null;
      postMessage(m: unknown): void {
        posted.push(m);
      }
    };
    try {
      const port = new WorkerPort('worker.js', 'http://planner.invalid/');
      await assert.rejects(inRun("a test's run", async () => port.ask({ kind: 'plan', handle: 1, query: 'a', runtime: 'r' })),
        REFUSED);
      assert.deepEqual(posted, [], 'nothing posted to the worker');
      void port.ask({ kind: 'plan', handle: 1, query: 'a', runtime: 'r' });
      assert.equal(posted.length, 1, 'outside the run, posted');
    } finally {
      delete g.Worker;
    }
  });
});
