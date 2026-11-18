// THE ENGINE MODULE IS OPT-IN (the user's K2 ruling, ~06:00 2026-11-18: "Product root, edge opt-in"; the orchestrator's
// option A): legend-engine's routes in the tab are a module of their own, loaded only for engine: 'legend-engine' --
// any other word refused by name, nothing opened (G7: no fallback to some module) -- and asked as the planner is, never
// inside a run (plan-guard.ts).

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { ENGINE, answerEngine, enginePort, loadEngineModule, type EngineModule } from '../src/legend/engine-module.ts';
import { inRun } from '../src/run-frame.ts';
import { runfileDirUrl } from '../../tools/js/runfiles.mts';
import { openInNode } from './planner.ts';

describe('the engine module, opted into', () => {
  it("refuses every engine word but 'legend-engine' by name, opening nothing", async () => {
    for (const word of ['', 'lite', 'legend_engine', 'Legend-Engine', 'legend-engine ']) {
      let opened = 0;
      await assert.rejects(loadEngineModule(word, 'file:///nowhere/', async () => { opened++; return {} as EngineModule; }),
        (e: unknown) => e instanceof Error && e.message.includes(`engine: ${JSON.stringify(word)}`)
          && e.message.includes(`only engine: '${ENGINE}' loads a module`), word);
      assert.equal(opened, 0, `${JSON.stringify(word)} opened a module`);
    }
  });

  it("loads for engine: 'legend-engine', and answers legend-engine's pure/v1 as the server's edge does", async () => {
    const engine = enginePort(await loadEngineModule(ENGINE, runfileDirUrl('WASM_ENGINE_PLANNER'), openInNode));
    const answer = await engine.ask({ kind: 'pureV1', path: '/api/pure/v1/compilation/compile', query: '',
      body: JSON.stringify({ _type: 'text', code: 'Class a::B { x: String[1]; }' }) });
    assert.equal(answer, 'OK\n200\napplication/json\n{"message":"OK","defects":[]}');
  });

  it('is never asked inside a run: a run plans nothing (G4)', async () => {
    const m = await loadEngineModule(ENGINE, runfileDirUrl('WASM_ENGINE_PLANNER'), openInNode);
    await assert.rejects(inRun("a test's run", async () => answerEngine(m, { kind: 'executeAnswer', plan: '{}', rows: '[]' })),
      /the planner was asked 'executeAnswer' inside a run/);
  });
});
