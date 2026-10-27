// Join… on a host's page (src/app/join-flow.ts hostJoinFlow; docs/DATACUBE_ONE_API_PLAN_2026_10_21.md B11): a page with
// no database of its own where it is shown (Python's) asks the host each of the window's answers -- the two sides, what
// the join will do, the join's grid -- with the page as the tab holds it; the grid the host made opened as a saved join
// of the page is, and added beside the grid joined. A grid whose rows are read elsewhere is refused in words before the
// window opens.

import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { accessor } from '../../pure-protocol/src/index.ts';
import { hostJoinFlow, type HostJoinFlowOf } from '../src/app/join-flow.ts';
import type { HostJoin } from '../src/app/services.ts';
import type { Opened, PageSources } from '../src/app/sources.ts';
import type { CubeDocument, CubeSource } from '../src/cube-document.ts';
import type { JoinSide, JoinSummaryCounts } from '../src/join.ts';
import { pageToJson, type PageDocument } from '../src/page-document.ts';
import type { PageApp } from '../src/page/page-app.ts';
import { corpus } from './fixtures/pages/make.ts';

// the saved-page corpus's frame page: grids grid and grid-2 (By desk), each over a frame
const trades = (await corpus())['trades.page.json']!;

let doc: Document;
const was = globalThis.document;
beforeEach(() => {
  doc = new JSDOM('<!doctype html><body></body>').window.document;
  // the flow opens its window on the page's document, as the tab's does
  (globalThis as { document: Document }).document = doc;
});
afterEach(() => {
  (globalThis as { document: Document | undefined }).document = was;
});
const settle = async (): Promise<void> => {
  for (let i = 0; i < 10; i += 1) await new Promise((r) => setTimeout(r, 0));
};
/** Long enough for the window's 250 ms wait after a change. */
const debounce = async (): Promise<void> => {
  await new Promise((r) => setTimeout(r, 300));
  await settle();
};

const side = (name: string, columns: [string, string][]): JoinSide => ({
  name, relation: accessor('local::DB', name), columns: columns.map(([n, type]) => ({ name: n, type })),
});
const MINE = side('trades', [['id', 'Integer'], ['customer_id', 'Integer'], ['qty', 'Float']]);
const THEIRS = side('customers', [['id', 'Integer'], ['region', 'String']]);
const COUNTS: JoinSummaryCounts = { mine: 5, matched: 3, joined: 6, repeated: { keys: 1, most: 2 } };
/** What the host made: any cube document, carried through as it is. */
const MADE: CubeDocument = trades.cubes[0]!.cube;
const MAKE = { made: true } as unknown as Opened['make'];

/** The flow over a page and a host that record what they were asked and what was added. */
function over(page: PageDocument, sourceOf: (id: string) => CubeSource | undefined = (id) => page.cubes.find((c) => c.id === id)?.cube.source,
  counts: JoinSummaryCounts = COUNTS) {
  const asked: string[] = [];
  const pages: string[] = [];
  const added: { make: unknown; near: string | undefined }[] = [];
  const reopened: CubeDocument[] = [];
  const said: string[] = [];
  const join: HostJoin = {
    sides: async (p, mine, theirs) => { pages.push(p); asked.push(`sides ${mine} ${theirs}`); return { left: MINE, right: THEIRS, notes: [] }; },
    questions: async (p, mine, theirs, how) => { pages.push(p); asked.push(`questions ${mine} ${theirs} ${how.form}`); return counts; },
    cube: async (p, mine, theirs, how, sheet) => { pages.push(p); asked.push(`cube ${mine} ${theirs} ${how.form} ${sheet}`); return MADE; },
    // a timeseries' grids read together: no join of the window's asks it
    together: async () => { throw new Error('no timeseries here'); },
  };
  const app = {
    title: page.name,
    grids: page.cubes.map((c) => c.id),
    cubes: page.cubes.map((c) => c.id),
    shownSheet: page.sheets[0]!.id,
    document: () => page,
    addGrid: (make: unknown, how: { near?: string }) => { added.push({ make, near: how.near }); return 'grid-9'; },
    // what the grid made is told: on its own status bar
    grid: (id: string) => ({ note: (text: string) => said.push(`${id}: ${text}`) }),
  } as unknown as PageApp;
  const sources = {
    place: (id: string) => ({ cubeSource: sourceOf(id) }),
    // what a grid reads: its source, which holds no link here
    reads: (id: string) => sourceOf(id),
    reopened: async (d: CubeDocument, at: string | undefined, several: boolean) => {
      assert.equal(at, undefined);
      assert.equal(several, true, 'one of the page\'s grids: its panel folded');
      reopened.push(d);
      return { make: MAKE, label: d.name, notes: ['trades: a column gone'] } satisfies Opened;
    },
  } as unknown as PageSources;
  const o: HostJoinFlowOf = {
    cube: 'grid', page: app, sources, join, parse: async () => { throw new Error('no Pure here'); },
    stillShown: () => true,
  };
  return { o, asked, pages, added, reopened, said };
}

describe("Join… on a host's page: each of the window's answers the host's (B11)", () => {
  it('asks the host the sides and what the join will do, then its grid, added beside the grid joined', async () => {
    const { o, asked, pages, added, reopened, said } = over(trades);
    const flow = hostJoinFlow(o);
    await settle();
    await debounce();
    assert.deepEqual(asked, ['sides grid grid-2', 'questions grid grid-2 join']);
    assert.match([...doc.querySelectorAll('.dc-join-will-line')].map((l) => l.textContent).join(' '), /3 rows of my 5 rows find a match/);
    doc.querySelector<HTMLButtonElement>('.dc-join .dc-primary')!.click();
    await flow;
    assert.deepEqual(asked.at(-1), 'cube grid grid-2 join null', "beside the grid joined, joinCube's placement (the S7 audit's Y-3)");
    assert.ok(pages.every((p) => p === pageToJson(trades)), 'the page as the tab holds it, with every ask');
    assert.deepEqual(reopened, [MADE], 'opened as a saved join of the page is');
    assert.deepEqual(added, [{ make: MAKE, near: 'grid' }]);
    assert.deepEqual(said, ['grid-9: trades: a column gone'], 'what opening it left out, on the grid made');
  });

  // the user, 2026-10-23: a right-click's Join ▸ joins at once to a grid sharing a column
  it('a quick join is made at once, what it did noted on its grid -- no window', async () => {
    const clean: JoinSummaryCounts = { mine: 5, matched: 3, joined: 5 };
    const { o, asked, added, said } = over(trades, undefined, clean);
    await hostJoinFlow({ ...o, ask: { their: 'grid-2', row: { left: 'customer_id', op: '==', right: 'id' }, quick: 'left' } });
    assert.equal(doc.querySelector('.dc-join'), null, 'no window');
    assert.deepEqual(asked, ['questions grid grid-2 join', 'cube grid grid-2 join null']);
    assert.deepEqual(added, [{ make: MAKE, near: 'grid' }]);
    assert.deepEqual(said, ['grid-9: 3 rows of my 5 rows find a match, 2 rows do not. trades: a column gone']);
  });

  it('a quick join that would repeat rows of mine opens the window on it, its key and form as asked', async () => {
    const { o, asked, added } = over(trades);
    const flow = hostJoinFlow({ ...o, ask: { their: 'grid-2', row: { left: 'customer_id', op: '==', right: 'id' }, quick: 'inner' } });
    await settle();
    await debounce();
    assert.ok(doc.querySelector('.dc-join'), 'the window, to show why');
    assert.deepEqual(added, [], 'nothing made yet');
    assert.equal(doc.querySelector<HTMLSelectElement>('.dc-join-their')!.value, 'grid-2');
    assert.deepEqual([...doc.querySelectorAll<HTMLSelectElement>('.dc-join-rows .dc-join-row select')].map((x) => x.value),
      ['customer_id', '==', 'id'], 'the key it was asked on');
    assert.equal(doc.querySelector('.dc-join-kind[aria-checked="true"]')?.getAttribute('data-kind'), 'inner');
    assert.match([...doc.querySelectorAll('.dc-join-will-line')].map((l) => l.textContent).join(' '), /appears more than once/);
    assert.ok(asked.includes('questions grid grid-2 join'));
    doc.querySelector<HTMLButtonElement>('.dc-join .dc-picker-close')!.click();
    await flow;
  });

  it('refuses a grid whose rows are read elsewhere, and one with no other grid to join, before the window opens', async () => {
    const cls = { _type: 'class', name: 'Trade' } as unknown as CubeSource;
    const mine = over(trades, (id) => (id === 'grid' ? cls : trades.cubes.find((c) => c.id === id)?.cube.source));
    await assert.rejects(hostJoinFlow(mine.o), /a join reads both its inputs in one place.*Trade is a class/);
    const theirs = over(trades, (id) => (id === 'grid-2' ? cls : trades.cubes.find((c) => c.id === id)?.cube.source));
    await assert.rejects(hostJoinFlow(theirs.o), /a join needs another grid on the page over a dataframe, a file or a remote file/);
    assert.deepEqual([...mine.asked, ...theirs.asked], [], 'the host asked nothing');
    assert.equal(doc.querySelector('.dc-join'), null, 'no window');
  });
});
