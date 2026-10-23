// The Cubes window (src/ui/cube-library.ts): opening a saved cube -- asked first over unsaved
// changes -- and the cube on screen marked. Saving is its own window (save-dialog.test.ts).

import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { SAVED_SORTS, savedSearch, type CubeStore, type DataCubeQuery, type QuerySearchSpecification } from '../src/cube-store.ts';
import { pageSearch } from '../src/headless/page.ts';
import { CubeLibrary, type CubeLibraryHost } from '../src/ui/cube-library.ts';

const tick = () => new Promise((r) => setTimeout(r, 0));

/** A store of two pages, as the window lists them (the store's rules are core's SavedCubes, held to
 *  test/saved-pages/conformance.ts; this window only lists, opens and deletes). */
function twoPages(): CubeStore {
  const pages = new Map<string, DataCubeQuery>([
    ['a', { id: 'a', name: 'Alpha', content: { kind: 'datacube.cube' }, owner: 'me' }],
    ['b', { id: 'b', name: 'Beta', content: { kind: 'datacube.cube' }, owner: 'me' }],
  ]);
  return {
    currentUser: async () => 'me',
    search: async () => [...pages.values()].map(({ content: _content, ...light }) => light),
    get: async (id) => pages.get(id) ?? Promise.reject(new Error(`no page ${id}`)),
    create: async (q) => { pages.set(q.id, q); return q; },
    update: async (_id, q) => { pages.set(q.id, q); return q; },
    delete: async (id) => { pages.delete(id); },
  };
}

describe('the Cubes window', () => {
  let root: HTMLElement;
  let calls: string[];
  let state: { dirty: boolean; id?: string };
  let library: CubeLibrary;

  beforeEach(async () => {
    const dom = new JSDOM('<!doctype html><body><div id="r"></div></body>');
    root = dom.window.document.getElementById('r') as HTMLElement;
    calls = [];
    state = { dirty: false, id: 'a' };
    const store = twoPages();
    const host: CubeLibraryHost = {
      currentId: () => state.id,
      open: async (id) => { calls.push(`open ${id}`); },
      openText: async () => {},
      forget: async () => {},
      dirty: () => state.dirty,
    };
    library = new CubeLibrary(root, store, host);
    await library.refresh();
  });

  const button = (label: string, within: ParentNode = root): HTMLButtonElement =>
    [...within.querySelectorAll<HTMLButtonElement>('button')].find((b) => b.textContent === label)
      ?? assert.fail(`no button ${label}`);
  const row = (name: string): HTMLElement =>
    [...root.querySelectorAll<HTMLElement>('.dc-lib-row')].find((r) => r.textContent?.includes(name))
      ?? assert.fail(`no row ${name}`);

  it('marks the cube on screen', () => {
    assert.ok(row('Alpha').classList.contains('dc-lib-current'));
    assert.ok(!row('Beta').classList.contains('dc-lib-current'));
    state.id = 'b';
    library.sync();
    assert.ok(row('Beta').classList.contains('dc-lib-current'));
  });

  it('opens straight away with nothing unsaved', async () => {
    button('Open', row('Beta')).click();
    await tick();
    assert.deepEqual(calls, ['open b']);
  });

  it('asks before opening another cube over unsaved changes', async () => {
    state.dirty = true;
    button('Open', row('Beta')).click();
    await tick();
    assert.deepEqual(calls, [], 'not opened yet');
    assert.match(root.textContent ?? '', /unsaved changes\. Open "Beta" anyway\?/);
    button('Open anyway').click();
    await tick();
    assert.deepEqual(calls, ['open b']);
  });

});

describe("the saved pages searched as Open… searches them (savedSearch; Python's ll.Page.saved(), headless pageSearch: K15)", () => {
  it("the window asks the store the one search: its term trimmed, its menu's order (the first, as it opens), its rows' count", async () => {
    const asked: QuerySearchSpecification[] = [];
    const store: CubeStore = { ...twoPages(), search: async (spec) => { asked.push(spec); return []; } };
    const dom = new JSDOM('<!doctype html><body><div id="r"></div></body>');
    const root = dom.window.document.getElementById('r') as HTMLElement;
    const library = new CubeLibrary(root, store, { currentId: () => undefined, open: async () => {}, openText: async () => {}, forget: async () => {} });
    await library.refresh();
    (root.querySelector('.dc-lib-search') as HTMLInputElement).value = '  Q3 ';
    (root.querySelector('.dc-lib-sort') as HTMLSelectElement).value = 'SORT_BY_UPDATE';
    await library.refresh();
    assert.deepEqual([...root.querySelectorAll('.dc-lib-sort option')].map((o) => o.textContent), SAVED_SORTS.map(([, label]) => label));
    assert.deepEqual(asked, [savedSearch('', null, 50), savedSearch('Q3', 'SORT_BY_UPDATE', 50)]);
    assert.deepEqual(asked, [{ sortByOption: 'SORT_BY_VIEW', limit: 50 },
      { searchTermSpecification: { searchTerm: 'Q3' }, sortByOption: 'SORT_BY_UPDATE', limit: 50 }]);
  });

  it("Python's the same, of as many as the store answers; what is none of a search refused in words", () => {
    assert.deepEqual(JSON.parse(pageSearch(' Q3 ', null, null)), { spec: { searchTermSpecification: { searchTerm: 'Q3' }, sortByOption: 'SORT_BY_VIEW' } });
    assert.deepEqual(JSON.parse(pageSearch('', 'SORT_BY_CREATE', 5)), { spec: { sortByOption: 'SORT_BY_CREATE', limit: 5 } });
    assert.deepEqual(JSON.parse(pageSearch(3, 'byName', 1.5)).problems, [
      'a search is text, not 3',
      'a sort is one of SORT_BY_VIEW, SORT_BY_UPDATE, SORT_BY_CREATE, not "byName"',
      'a limit is a whole number, 0 or more, not 1.5',
    ]);
  });
});
