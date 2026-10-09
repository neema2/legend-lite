// The share link (src/share/link.ts, milestone 1b): the saved page, deflated against the frozen p1
// dictionary, after the `#`. It must give back EXACTLY the page, stay small, refuse what it cannot
// read by name, and its dictionary must never change once links exist.

import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { describe, it } from 'node:test';

import { col, fn, lambda, lit, accessor } from '../../pure-protocol/src/index.ts';
import { DEFAULT_CONFIGURATION, type CubeConfiguration } from '../src/config.ts';
import { writeCube, type FileSource } from '../src/cube-document.ts';
import { pageDefinitionText, writePage, type PageDocument } from '../src/page-document.ts';
import {
  LINK_BUDGET, ShareLinkError, isPageFragment, pageFragment, readPageFragment, shareLink,
} from '../src/share/link.ts';
import { LINK_DICTIONARY_P1 } from '../src/share/link-p1.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { TreeState } from '../src/tree.ts';

const NAMES = ['trade_id', 'trade_date', 'region', 'country', 'desk', 'book', 'trader', 'counterparty', 'currency',
  'notional', 'quantity', 'price', 'pnl', 'fees', 'status', 'venue', 'strategy', 'sector', 'rating', 'maturity',
  'coupon', 'spread', 'yield', 'duration', 'client_segment', 'sales_person', 'product_type', 'asset_class',
  'legal_entity', 'cost_center', 'risk_bucket', 'tenor', 'strike', 'barrier', 'underlying', 'exchange',
  'clearing_house', 'margin', 'collateral', 'haircut', 'exposure', 'limit_used', 'approval', 'comment',
  'source_system', 'batch_id', 'created_at', 'updated_at', 'version', 'region_group', 'sub_desk', 'portfolio',
  'account', 'sub_account', 'benchmark', 'settle_date', 'delta', 'gamma', 'vega', 'theta'];

/** A realistic page, through the product's own writers: `n` columns, four calculated columns, a filter, formats, a chart. */
function page(n: number, o: { inList?: number; title?: string } = {}): PageDocument {
  const typeOf = (name: string): string =>
    /date|_at$/.test(name) ? 'StrictDate' : /_id$|version|quantity/.test(name) ? 'Integer' : /^(region|country|desk|book|trader|counterparty|currency|status|venue|strategy|sector|rating|comment)/.test(name) ? 'String' : 'Float';
  const columns = NAMES.slice(0, n).map((name) => ({ name, type: typeOf(name) }));
  const x = 'x';
  const snapshot: CubeSnapshot = {
    source: { query: accessor('file::DB', 'RATES') },
    columns,
    derived: [
      { name: 'pnl_per_mm', lambda: lambda([x], fn('divide', col(x, 'pnl'), fn('divide', col(x, 'notional'), lit.integer(1000000)))), kind: 'measure' },
      { name: 'size_bucket', lambda: lambda([x], fn('if', fn('greaterThan', col(x, 'notional'), lit.integer(10000000)), lambda([], lit.string('large')), lambda([], lit.string('small')))), kind: 'dimension' },
      { name: 'desk_region', lambda: lambda([x], fn('plus', fn('toOne', col(x, 'region')), lit.string(' / '), fn('toOne', col(x, 'desk')))), kind: 'dimension' },
      { name: 'fee_bps', lambda: lambda([x], fn('times', fn('divide', col(x, 'fees'), col(x, 'notional')), lit.decimal('10000.50'))), kind: 'measure' },
    ],
    rows: ['region', 'desk'],
    pivotOn: ['currency'],
    measures: [
      { name: 'Notional', column: 'notional', fn: 'sum' },
      { name: 'PnL', column: 'pnl', fn: 'sum' },
      { name: 'Avg price', column: 'price', fn: 'average' },
    ],
    sorts: [{ column: 'PnL', direction: 'desc' }],
    filter: { kind: 'and', children: [
      { kind: 'condition', column: 'status', operator: 'equal', value: 'LIVE' },
      { kind: 'condition', column: 'trade_date', operator: 'greaterThan', value: '2026-07-01' },
      ...(o.inList ? [{ kind: 'condition' as const, column: 'counterparty', operator: 'in' as const,
        value: Array.from({ length: o.inList }, (_, i) => `CPTY-${(i * 7919 % 99991).toString(36).toUpperCase()}`) }] : []),
    ] },
    epoch: 1,
  } as CubeSnapshot;
  const configuration: CubeConfiguration = {
    ...DEFAULT_CONFIGURATION,
    reportTitle: o.title ?? 'Rates book, Q3 by desk',
    columns: {
      notional: { format: { kind: 'number', decimals: 0 } },
      pnl: { format: { kind: 'number', decimals: 0 }, pinned: 'left' },
      price: { format: { kind: 'number', decimals: 4 } },
      comment: { hidden: true },
    },
  };
  const source: FileSource = { _type: 'file', name: 'rates_trades_2026Q3.csv', format: 'csv', size: 48234567,
    sha256: '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08', columns };
  const cube = writeCube({ name: o.title ?? 'Rates book, Q3 by desk', source, snapshot, configuration,
    tree: TreeState.fromPaths([['EMEA'], ['EMEA', 'Swaps'], ['AMER']]) });
  return writePage({ name: cube.name, cube, views: {
    views: [
      { id: 'grid', kind: 'grid', cube: 'cube' },
      { id: 'pnl-by-desk', kind: 'chart', cube: 'cube', title: 'PnL by desk', spec: { version: 1, mark: 'bar', x: 'desk',
        y: [{ column: 'pnl', fn: 'sum' }], options: { orientation: 'vertical', stack: 'none', sort: { by: 'y', direction: 'desc' },
          limit: 20, labels: false, legend: 'none' } } },
    ],
    layout: { kind: 'bands', fit: false, bands: [{ height: 0.25, node: { split: 'row', parts: [
      { node: { tile: 'grid' }, size: 2 / 3 }, { node: { tile: 'pnl-by-desk' }, size: 1 / 3 },
    ] } }] },
  } });
}

const same = (a: PageDocument, b: PageDocument): void => {
  assert.equal(b.name, a.name);
  assert.equal(pageDefinitionText(b), pageDefinitionText(a));
};

/** Each shipped dictionary version's hash: a version is frozen the moment links can be made with it. */
const PINNED: Record<string, string> = {
  p1: '2201c3d451efb867fd77d850a1fd351c35dbcdb90593ac3b6fcf920bb9708bd4',
};

describe('the p1 dictionary is FROZEN', () => {
  it('its bytes never change: every link made with it must open forever', () => {
    const hash = createHash('sha256').update(LINK_DICTIONARY_P1).digest('hex');
    // A change here breaks every p1 link ever shared. Never re-pin: make the next version instead
    // (bazel run //datacube:cut_link_dictionary).
    assert.equal(hash, PINNED.p1);
  });

  it('every version in the tree is pinned, and the next one is not cut yet', () => {
    // datacube/BUILD.bazel passes the versions it finds (src/share/link-*.ts) and _NEXT_LINK_VERSION
    const versions = (process.env.LINK_VERSIONS ?? '').split(' ').filter((v) => v !== '');
    assert.ok(versions.includes('p1'), `LINK_VERSIONS is ${JSON.stringify(process.env.LINK_VERSIONS)}`);
    for (const v of versions) {
      assert.ok(v in PINNED, `src/share/link-${v}.ts is cut: pin its hash in PINNED here, and move`
        + ' _NEXT_LINK_VERSION on in datacube/BUILD.bazel');
    }
    const next = process.env.NEXT_LINK_VERSION ?? '';
    assert.ok(next !== '' && !versions.includes(next), `${next} is shipped: move _NEXT_LINK_VERSION on in datacube/BUILD.bazel`);
  });

  it('holds no user text: only the format\'s own words and placeholders', () => {
    const names = new Set([...LINK_DICTIONARY_P1.matchAll(/"(?:name|property|title|reportTitle|displayName)":"([^"]*)"/g)].map((m) => m[1]));
    assert.deepEqual([...names].sort(), ['', 'a', 'x']);
  });
});

describe('a link gives back EXACTLY the page', () => {
  it('a realistic page: query, calculated columns, filter, formats, open rows, a chart, the layout', () => {
    const p = page(12);
    same(p, readPageFragment(pageFragment(p)));
  });

  it('text of every kind: accents, CJK, emoji, quotes, a newline; exact decimals and big integers', () => {
    const p = page(12, { title: 'Résumé «Q3» 中文 📈 "quoted" \'single\'\nline 2' });
    const got = readPageFragment(pageFragment(p));
    same(p, got);
    assert.equal(got.name, p.name);
    assert.match(pageDefinitionText(got), /10000\.50/, 'the decimal kept exactly');
  });

  it('with or without its #; and a link read from a whole address', () => {
    const p = page(12);
    const link = shareLink('http://127.0.0.1:8000/demo/index.html?plane=local#stale', p);
    assert.ok(link.url.startsWith('http://127.0.0.1:8000/demo/index.html?plane=local#p1.'), 'an old fragment is replaced');
    const fragment = link.url.slice(link.url.indexOf('#'));
    assert.ok(isPageFragment(fragment));
    same(p, readPageFragment(fragment));
    same(p, readPageFragment(fragment.slice(1)));
  });
});

describe('a link that cannot be read fails LOUDLY, saying why', () => {
  const good = pageFragment(page(12));
  it('a newer version is refused by name', () => {
    assert.throws(() => readPageFragment(`p9.${good.slice(3)}`), (e: unknown) => e instanceof ShareLinkError && /newer DataCube \(link version p9\)/.test(e.message));
  });
  it('a link cut short when pasted', () => {
    assert.throws(() => readPageFragment(good.slice(0, Math.floor(good.length / 2))), (e: unknown) => e instanceof ShareLinkError && /damaged or incomplete/.test(e.message));
  });
  it('something that is not a share link', () => {
    assert.throws(() => readPageFragment('section-2'), (e: unknown) => e instanceof ShareLinkError && /not a DataCube share link/.test(e.message));
    assert.equal(isPageFragment('#section-2'), false);
  });
  it('characters no link of ours holds', () => {
    assert.throws(() => readPageFragment('p1.abc+def/=='), ShareLinkError);
  });
});

describe('a link stays small (the budget: what mail and chat keep)', () => {
  const lengthOf = (p: PageDocument): number => shareLink('http://127.0.0.1:8000/demo/index.html', p).length;

  it('a realistic 12-column page is well under the budget', (t) => {
    const n = lengthOf(page(12));
    t.diagnostic(`12 columns, 4 calculated columns, a filter, formats, a chart: ${n} characters`);
    assert.ok(n < 1100, `12 columns: ${n} characters`);
    assert.ok(n < LINK_BUDGET);
  });

  it('a 60-column page too: the table\'s width barely matters', (t) => {
    const n = lengthOf(page(60));
    t.diagnostic(`60 columns: ${n} characters`);
    assert.ok(n < 1700, `60 columns: ${n} characters`);
  });

  it('a long hand-typed list is what makes a link long -- and is said so', (t) => {
    const link = shareLink('http://127.0.0.1:8000/demo/index.html', page(12, { inList: 2000 }));
    t.diagnostic(`12 columns + a filter of 2,000 hand-typed ids: ${link.length} characters`);
    assert.equal(link.long, true, `${link.length} characters: over the budget, the person is told`);
  });
});
