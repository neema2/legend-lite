// The screens' engine is pointed at a project's database only for the navigation still current (2026-11-15, the
// TypeScript hunt's T-A). Making a project's database takes a while; Back pressed while it said "Opening…" used to leave
// the newer screen reading the older project's rows when the older one finished, with no error.

import { strict as assert } from 'node:assert';
import { describe, it } from 'node:test';
import { AppContext, type LoadedProject } from '../src/app/context.ts';
import type { ProjectEngine } from '../../engine-client/src/project-data.ts';
import type { QueryEngine } from '../../engine-client/src/query-engine.ts';

/** A project by its model text alone: all `activate` reads of it. */
const project = (code: string): LoadedProject => ({ context: { _type: 'text', code } }) as unknown as LoadedProject;

describe("the screens' engine, pointed as a project opens", () => {
  it('is pointed for the navigation still current, never for one a later navigation overtook', async () => {
    const ctx = new AppContext({} as never, {} as never, {} as never, undefined, [], 'someone', {} as never);
    /** Each project's database made once let go: the engine it answers names its project. */
    const made = new Map<string, { opened: Promise<void>; open(): void }>();
    for (const code of ['P1', 'P2', 'P3']) {
      let open!: () => void;
      made.set(code, { opened: new Promise((done) => { open = done; }), open: () => open() });
    }
    const pointed: string[] = [];
    ctx.tabProjects = {
      databases: {
        engineFor: async (model: string): Promise<ProjectEngine> => {
          await made.get(model)!.opened;
          return { name: model, engine: { name: model } } as unknown as ProjectEngine;
        },
      },
      runner: { use: (engine) => { pointed.push((engine as unknown as { name: string }).name); } },
      host: {} as QueryEngine,
    };
    let navigations = 0;
    const navigate = (): (() => boolean) => {
      const at = (navigations += 1);
      return () => at === navigations;
    };

    // P1 opens slowly; Back moves to P2, which opens and is pointed; then P1 finishes
    const first = ctx.activate(project('P1'), navigate());
    const second = ctx.activate(project('P2'), navigate());
    made.get('P2')!.open();
    await second;
    assert.deepEqual(pointed, ['P2']);
    made.get('P1')!.open();
    await first;
    assert.deepEqual(pointed, ['P2'], "P1, finishing late, does not move the engine under P2's screen");

    // and a navigation still current when its database is there is pointed
    const third = ctx.activate(project('P3'), navigate());
    made.get('P3')!.open();
    await third;
    assert.deepEqual(pointed, ['P2', 'P3']);
  });
});
