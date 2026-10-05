// Leg B's guardrail (docs/DATACUBE_LEG_B_STATE_OWNER_2026_09_28.md): the cube's state -- its
// snapshot, configuration, open groups and history -- has ONE owner, `CubeStateOwner`
// (src/cube-state.ts). Before it, the app and the controller each held a copy, each wrote its
// own, and a refusal put back one of them (P2-100). A "cache for convenience" is how a second
// owner comes back, so this holds, mechanically, that nothing else keeps or assigns it.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { Sources } from '../../tools/js/runfiles.mts';

// the sources this reads, as the BUILD target declares them (Bazel workplan P3-29)
const SOURCES = new Sources('SOURCES', 'datacube');

const OWNER = 'src/state-owner.ts';
/** The two that held copies: the app and the controller. */
const HOLDERS = ['src/app.ts', 'src/cube.ts'];
/** The cube's state, by the names the holders used for it. */
const STATE = '(snapshot|config|configuration|tree|history|lastState|past|future|view|treeRows)';

const code = (file: string): { line: string; at: number }[] =>
  SOURCES.read(file).split('\n').map((line, i) => ({ line, at: i + 1 }))
    .filter(({ line }) => {
      const t = line.trim();
      return !(t.startsWith('//') || t.startsWith('*') || t.startsWith('/*'));
    });

describe('one owner of the cube\'s state', () => {
  it('the app and the controller declare no field of it (a getter reading the owner is fine)', () => {
    const field = new RegExp(`^\\s*(readonly\\s+)?#${STATE}\\b\\s*[:=;]`);
    const bad = HOLDERS.flatMap((file) => code(file)
      .filter(({ line }) => field.test(line)).map(({ line, at }) => `${file}:${at}: ${line.trim()}`));
    assert.deepEqual(bad, []);
  });

  it('the app and the controller assign none of it', () => {
    // The editors' DRAFTS (the Filter window's condition tree, the Properties draft) and the Ad
    // Hoc session are not the cube's state; B4 and B5 bring them under the same rules.
    const assign = new RegExp(`this\\.#${STATE}\\s*=(?!=)`);
    const bad = HOLDERS.flatMap((file) => code(file)
      .filter(({ line }) => assign.test(line)).map(({ line, at }) => `${file}:${at}: ${line.trim()}`));
    assert.deepEqual(bad, []);
  });

  it('there is one history: the owner\'s', () => {
    const bad = SOURCES.under('src', '.ts').filter((f) => f !== OWNER)
      .flatMap((file) => code(file)
        .filter(({ line }) => /new UndoStack\b|class \w*History\b/.test(line))
        .map(({ line, at }) => `${file}:${at}: ${line.trim()}`));
    assert.deepEqual(bad, []);
  });

  it('scans what it claims to', () => {
    // a guardrail that scans nothing passes for the wrong reason
    for (const file of HOLDERS) assert.ok(code(file).length > 100, file);
    assert.ok(code(OWNER).some(({ line }) => /this\.#committed = /.test(line)), 'the owner does assign it');
  });
});
