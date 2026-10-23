// THE SAVED-PAGE CORPUS (docs/DATACUBE_COMMAND_API_HOMEWORK_2026_10_17.md §11, part 3): real page documents written by
// DataCube's own writers (test/fixtures/pages/make.ts), kept as files. Each must still be what the writers write, read
// as a page, open each cube with nothing to say, come back from the screen's open-and-save as the same cube, read and
// draw each chart, and be written again character for character -- so a change to how a page is written or read is a
// diff of these files, decided, never a surprise to a person's saved pages. What it pins is today's version: an older
// version of any part is refused in words (the user, 2026-10-17: no older document is read), so the files are made
// again when a version moves, and the diff read. python/tests/test_page.py loads the frame page in Python, over its
// frame and over the frame with a column more, and holds what it writes to the same file.

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { basename } from 'node:path';
import { describe, it } from 'node:test';

import { element } from '../../pure-protocol/src/index.ts';
import { runfilesFromEnv } from '../../tools/js/runfiles.mts';
import { chartProblems, readChart } from '../src/chart-spec.ts';
import { cubeToJson, openCube, writeCube } from '../src/cube-document.ts';
import { openedState } from '../src/cube-state.ts';
import { pageToJson, readPage } from '../src/page-document.ts';
import { corpus } from './fixtures/pages/make.ts';

/** The corpus as kept: each file's name and its text. */
const kept = new Map(runfilesFromEnv('CORPUS').map((path) => [basename(path), readFileSync(path, 'utf8')]));

describe('the saved-page corpus (test/fixtures/pages)', () => {
  it('holds every page the corpus makes, each as DataCube writes it today', async () => {
    const made = await corpus();
    assert.deepEqual([...kept.keys()].sort(), Object.keys(made).sort(), 'one file per page: make.ts writes them');
    for (const [name, page] of Object.entries(made)) {
      // a difference here is a change to how a page is written: make.ts writes the files again, and the diff is read
      assert.equal(kept.get(name), `${pageToJson(page)}\n`, `${name} is not what DataCube writes now`);
    }
  });

  for (const [name, text] of kept) {
    describe(name, () => {
      it('reads as a page, and is written again character for character', () => {
        assert.equal(`${pageToJson(readPage(text))}\n`, text);
      });

      it('opens each cube over its source with nothing to say and nothing left out', () => {
        for (const { id, cube } of readPage(text).cubes) {
          const source = cube.source as { readonly columns?: readonly { readonly name: string; readonly type: string }[] };
          assert.ok(source.columns, `${id}: its source records its columns`);
          const opened = openCube(cube, { query: element('source') }, source.columns);
          assert.deepEqual(opened.notes, [], id);
          assert.equal(opened.changed, false, id);
        }
      });

      it('each cube, opened as the screen opens it and written again, is the cube kept (no writer wrote it unfolded)', () => {
        for (const { id, cube } of readPage(text).cubes) {
          const source = cube.source as { readonly columns: readonly { readonly name: string; readonly type: string }[] };
          const opened = openCube(cube, { query: element('source') }, source.columns);
          const state = openedState(opened.snapshot, opened.configuration, opened.tree);
          const again = writeCube({ name: cube.name, source: cube.source, snapshot: state.snapshot,
            configuration: state.configuration, tree: state.tree, ...(cube.unknown ? { unknown: cube.unknown } : {}) });
          assert.equal(cubeToJson(again), cubeToJson(cube), `${id}: the screen would save another document`);
        }
      });

      it('reads each chart and draws it: no problem the charts panel would say', () => {
        const page = readPage(text);
        for (const view of page.views) {
          if (view.kind !== 'chart') continue;
          const read = readChart(JSON.parse(JSON.stringify(view.spec)));
          assert.notEqual(typeof read, 'string', `${view.id}: ${String(read)}`);
          const cube = page.cubes.find((c) => c.id === view.cube)?.cube;
          assert.ok(cube, `${view.id}'s cube`);
          const source = cube.source as { readonly columns: readonly { readonly name: string; readonly type: string }[] };
          const opened = openCube(cube, { query: element('source') }, source.columns);
          assert.deepEqual(chartProblems(view.spec, opened.snapshot), [], `${view.id} (${view.title})`);
        }
      });
    });
  }
});

describe('an older part of a page is refused in words, never read', () => {
  const page = (): Record<string, unknown> => JSON.parse(kept.get('trades.page.json') ?? '{}') as Record<string, unknown>;

  it('a page of an older version', () => {
    const old = { ...page(), version: 3 };
    assert.throws(() => readPage(old), /saved by an older version \(3 < 6\): it is no longer read/);
  });

  it('a page of a newer version', () => {
    assert.throws(() => readPage({ ...page(), version: 99 }), /written by a newer version \(99 > 6\); upgrade to open it/);
  });

  it('a cube of an older version inside a current page', () => {
    const p = page();
    const cubes = (p['cubes'] as { id: string; cube: Record<string, unknown> }[]).map((c) => ({ ...c, cube: { ...c.cube, version: 1 } }));
    assert.throws(() => readPage({ ...p, cubes }), /cannot read saved cube: saved by an older version \(1 < 2\).*: it is no longer read/);
  });

  it('a chart of an older spec version inside a current page', () => {
    const p = page();
    const views = (p['views'] as Record<string, unknown>[]).map((v) =>
      v['kind'] === 'chart' ? { ...v, spec: { ...(v['spec'] as Record<string, unknown>), version: 1 } } : v);
    assert.throws(() => readPage({ ...p, views }), /has no chart it can draw: its version 1 is not one this DataCube reads \(2\)/);
  });
});
