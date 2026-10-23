import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import type { CubeDraft } from '../src/ui/panel-kit.ts';
import { element } from '../../pure-protocol/src/index.ts';

const SNAPSHOT: CubeSnapshot = {
  source: { query: element('trades') },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'desk', type: 'String' },
    { name: 'notional', type: 'Float' },
  ],
  derived: [],
  rows: ['region'],
  pivotOn: [],
  sorts: [],
  epoch: 1,
};

describe('applying the editor', () => {
  it("keeps the Dimensions tab's hierarchies on the configuration", async () => {
    const { asGridDraft } = await import('../src/app.ts');
    const { applyGridCommands, draftCommands } = await import('../src/grid-commands.ts');
    const base: CubeDraft = { snapshot: SNAPSHOT, config: DEFAULT_CONFIGURATION, dimensions: [] };
    const geo = [{ name: 'Geography', columns: ['region', 'desk'] }];
    // Apply's commands (grid-commands.ts draftCommands) on the grid as it is
    const applied = (edited: CubeDraft) => applyGridCommands({ snapshot: SNAPSHOT, config: DEFAULT_CONFIGURATION },
      draftCommands(asGridDraft(base), asGridDraft(edited)));
    assert.deepEqual(applied({ ...base, dimensions: geo }).config.dimensions, geo);
    // Untouched, nothing is written: the host's stay in charge.
    assert.equal(applied(base).config.dimensions, undefined);
  });
});
