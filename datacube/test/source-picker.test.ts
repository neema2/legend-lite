// The source picker (src/ui/source-picker.ts): only the sections the host offers, each choice read
// INSIDE the window (a refusal stays in view), Escape and the close button cancel, and the
// warehouse section signs in before it lists.

import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { pickSource, type PickerSections } from '../src/ui/source-picker.ts';

let dom: JSDOM;
let doc: Document;
const settle = async (): Promise<void> => {
  for (let i = 0; i < 10; i += 1) await new Promise((r) => setTimeout(r, 0));
};
const $ = <T extends Element>(sel: string): T => {
  const e = doc.querySelector<T>(sel);
  assert.ok(e, sel);
  return e;
};
const tabs = (): string[] => [...doc.querySelectorAll<HTMLElement>('.dc-picker-tab')].map((t) => t.dataset['section']!);

beforeEach(() => {
  dom = new JSDOM('<!doctype html><body></body>');
  doc = dom.window.document;
  const g = globalThis as unknown as Record<string, unknown>;
  g['window'] = dom.window;
  g['document'] = doc;
});

const files = (open: (f: File) => Promise<string>): NonNullable<PickerSections<string>['files']> =>
  ({ accept: '.csv', formats: ['CSV'], open });

describe('the source picker', () => {
  it('offers only the sections the host gives, in order, starting on the first', async () => {
    const chosen = pickSource<string>(doc, {
      purpose: 'add',
      sections: {
        remote: { detect: () => 'Parquet', open: async () => 'r' },
        files: files(async () => 'f'),
      },
    });
    assert.deepEqual(tabs(), ['files', 'remote']);
    assert.equal($('.dc-picker-tab[aria-selected="true"]').getAttribute('data-section'), 'files');
    assert.equal($('.dc-picker-title').textContent, 'Add a source');
    $<HTMLButtonElement>('.dc-picker-close').click();
    assert.equal(await chosen, undefined, 'closed: nothing chosen');
    assert.equal(doc.querySelector('.dc-picker'), null, 'the window is gone');
  });

  it('a file is read inside the window, and its answer is the choice', async () => {
    const chosen = pickSource<string>(doc, { purpose: 'open', sections: { files: files(async (f) => `read ${f.name}`) } });
    const input = $<HTMLInputElement>('.dc-picker-file');
    Object.defineProperty(input, 'files', { value: [new dom.window.File(['a,b\n1,2\n'], 'trades.csv')] });
    input.dispatchEvent(new dom.window.Event('change'));
    assert.equal(await chosen, 'read trades.csv');
  });

  it('a refusal stays in the window, which stays open for another try', async () => {
    let tries = 0;
    const chosen = pickSource<string>(doc, {
      purpose: 'add',
      sections: { examples: { list: [{ id: 'a', name: 'A', description: 'first' }], open: async () => {
        tries += 1;
        if (tries === 1) throw new Error('the bucket refused this page (CORS)');
        return 'ok';
      } } },
    });
    $<HTMLButtonElement>('.dc-picker-card').click();
    $<HTMLButtonElement>('.dc-picker-choice .dc-primary').click();
    await settle();
    assert.match($('.dc-picker-status').textContent ?? '', /CORS/);
    assert.ok($('.dc-picker-status').classList.contains('dc-failed'));
    $<HTMLButtonElement>('.dc-picker-choice .dc-primary').click();
    assert.equal(await chosen, 'ok');
  });

  it('an example is chosen, then opened at the rows asked for -- or downloaded instead', async () => {
    const downloads: string[] = [];
    const chosen = pickSource<string>(doc, {
      purpose: 'open',
      sections: { examples: {
        list: [{ id: 'trades', name: 'Trades', description: 'a bit of everything', rows: 5000 },
          { id: 'wide', name: 'Wide', description: '60 columns', rows: 2000 }],
        open: async (id, rows) => `${id} x ${rows}`,
        download: (id, rows) => { downloads.push(`${id} x ${rows}`); },
      } },
    });
    assert.equal(doc.querySelector('.dc-picker-choice'), null, 'nothing chosen yet');
    $<HTMLButtonElement>('.dc-picker-card[data-example="wide"]').click();
    assert.equal($('.dc-picker-choice-name').textContent, 'Wide');
    const rows = $<HTMLInputElement>('.dc-picker-rows');
    assert.equal(rows.value, '2000', 'its own row count to start');
    rows.value = '300';
    $<HTMLButtonElement>('.dc-picker-choice .dc-quiet').click();
    assert.deepEqual(downloads, ['wide x 300']);
    $<HTMLButtonElement>('.dc-picker-choice .dc-primary').click();
    assert.equal(await chosen, 'wide x 300');
  });

  it('Escape cancels', async () => {
    const chosen = pickSource<string>(doc, { purpose: 'add', sections: { files: files(async () => 'f') } });
    doc.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
    assert.equal(await chosen, undefined);
  });

  it('the database section signs in, then lists what the user may read, filtered as typed', async () => {
    const signedIn: string[] = [];
    const chosen = pickSource<string>(doc, {
      purpose: 'add',
      start: 'database',
      sections: {
        files: files(async () => 'f'),
        database: {
          url: 'http://wh',
          signIn: async (url, user, password) => {
            signedIn.push(`${url}|${user}|${password}`);
            return { principal: user, where: 'wh', objects: [
              { schema: 'sales', name: 'orders', kind: 'table', columns: 7 },
              { schema: 'sales', name: 'v_daily', kind: 'view', columns: 3 },
            ] };
          },
          open: async (o) => `${o.schema}.${o.name}`,
        },
      },
    });
    const inputs = [...doc.querySelectorAll<HTMLInputElement>('.dc-picker-form .dc-picker-input')];
    assert.equal(inputs[0]!.value, 'http://wh', 'the page\'s warehouse is offered');
    inputs[1]!.value = 'rita';
    inputs[2]!.value = 'pw';
    $<HTMLFormElement>('.dc-picker-form').dispatchEvent(new dom.window.Event('submit', { cancelable: true }));
    await settle();
    assert.deepEqual(signedIn, ['http://wh|rita|pw']);
    assert.equal($('.dc-picker-status').textContent, '', 'signing in is no longer said once it is done');
    const rows = (): string[] => [...doc.querySelectorAll<HTMLElement>('.dc-picker-row')].map((r) => r.dataset['object']!);
    assert.deepEqual(rows(), ['sales.orders', 'sales.v_daily']);
    const filter = $<HTMLInputElement>('.dc-picker-search');
    filter.value = 'daily';
    filter.dispatchEvent(new dom.window.Event('input'));
    assert.deepEqual(rows(), ['sales.v_daily']);
    $<HTMLButtonElement>('.dc-picker-row').click();
    assert.equal(await chosen, 'sales.v_daily');
  });

  it('reopening a cube over a table: says why, signs in there, and opens the table without a click', async () => {
    const chosen = pickSource<string>(doc, {
      purpose: 'open',
      start: 'database',
      reason: '“Orders” reads sales.orders on wh: sign in there to open it.',
      sections: { database: {
        url: 'http://wh',
        want: { schema: 'sales', name: 'orders' },
        signIn: async (_url, user) => ({ principal: user, where: 'wh', objects: [
          { schema: 'sales', name: 'orders', kind: 'table', columns: 7 },
        ] }),
        open: async (o) => `reopened ${o.schema}.${o.name}`,
      } },
    });
    assert.equal($('.dc-picker-subtitle').textContent, '“Orders” reads sales.orders on wh: sign in there to open it.');
    const inputs = [...doc.querySelectorAll<HTMLInputElement>('.dc-picker-form .dc-picker-input')];
    assert.equal(inputs[0]!.value, 'http://wh');
    inputs[1]!.value = 'rita';
    inputs[2]!.value = 'pw';
    $<HTMLFormElement>('.dc-picker-form').dispatchEvent(new dom.window.Event('submit', { cancelable: true }));
    assert.equal(await chosen, 'reopened sales.orders');
  });

  it('a wanted table not granted to whoever signed in: said, and the tables they may read listed', async () => {
    const chosen = pickSource<string>(doc, {
      purpose: 'open',
      start: 'database',
      sections: { database: {
        session: { principal: 'sam', where: 'wh', objects: [{ schema: 'sales', name: 'v_daily', kind: 'view', columns: 3 }] },
        want: { schema: 'sales', name: 'orders' },
        signIn: async () => { throw new Error('not asked'); },
        open: async (o) => `${o.schema}.${o.name}`,
      } },
    });
    await settle();
    assert.match($('.dc-picker-status').textContent ?? '', /sales\.orders is not granted to sam at wh/);
    assert.deepEqual([...doc.querySelectorAll<HTMLElement>('.dc-picker-row')].map((r) => r.dataset['object']), ['sales.v_daily']);
    $<HTMLButtonElement>('.dc-picker-close').click();
    assert.equal(await chosen, undefined);
  });

  it('reopening a cube over a private remote file: its URL offered, the keys open to fill in', async () => {
    const asked: string[] = [];
    const chosen = pickSource<string>(doc, {
      purpose: 'open',
      start: 'remote',
      sections: { remote: {
        detect: () => 'Parquet',
        url: 's3://bucket/trades.parquet',
        keys: true,
        open: async (url, c) => { asked.push(`${url}|${c?.keyId ?? ''}`); return 'reopened'; },
      } },
    });
    assert.equal($<HTMLInputElement>('.dc-picker-url').value, 's3://bucket/trades.parquet');
    assert.equal($<HTMLDetailsElement>('.dc-picker-more').open, true);
    assert.equal($('.dc-picker-badge').textContent, 'Parquet');
    const creds = [...doc.querySelectorAll<HTMLInputElement>('.dc-picker-creds .dc-picker-input')];
    creds[1]!.value = 'AKIA1';
    $<HTMLFormElement>('.dc-picker-form').dispatchEvent(new dom.window.Event('submit', { cancelable: true }));
    assert.equal(await chosen, 'reopened');
    assert.deepEqual(asked, ['s3://bucket/trades.parquet|AKIA1']);
  });

  it('a saved query that cannot be a source is listed, disabled, and says why', async () => {
    const chosen = pickSource<string>(doc, {
      purpose: 'add',
      sections: { saved: {
        search: async () => [
          { id: '1', name: 'Big trades', owner: 'rita' },
          { id: '2', name: 'Trade graph', unusable: 'returns objects, not rows' },
        ],
        open: async (id) => `query ${id}`,
      } },
    });
    // the first search runs at once (only typing is debounced): its answer, not a clock, is awaited
    await settle();
    const rows = [...doc.querySelectorAll<HTMLButtonElement>('.dc-picker-row')];
    assert.equal(rows.length, 2);
    assert.equal(rows[1]!.disabled, true);
    assert.match(rows[1]!.textContent ?? '', /returns objects, not rows/);
    rows[0]!.click();
    assert.equal(await chosen, 'query 1');
  });

  it('a saved query row copies its link beside it, and the window stays open saying so', async () => {
    const asked: string[] = [];
    const chosen = pickSource<string>(doc, {
      purpose: 'add',
      sections: { saved: {
        where: 'in this browser',
        search: async () => [{ id: '1', name: 'Big trades' }],
        open: async (id) => `query ${id}`,
        copyLink: async (id) => { asked.push(id); return 'Link copied (500 characters)'; },
      } },
    });
    // the first search runs at once (only typing is debounced): its answer, not a clock, is awaited
    await settle();
    assert.match($('.dc-picker-panel').textContent ?? '', /Saved in this browser/);
    $<HTMLButtonElement>('.dc-picker-row-link[data-query-link="1"]').click();
    await settle();
    assert.deepEqual(asked, ['1']);
    assert.ok(doc.querySelector('.dc-picker'), 'the window stays open');
    assert.equal($('.dc-picker-status').textContent, 'Link copied (500 characters)');
    assert.match($('.dc-picker-status').className, /dc-done/);
    $<HTMLButtonElement>('.dc-picker-row[data-query="1"]').click();
    assert.equal(await chosen, 'query 1');
  });

  it('the arrow keys move between sections', async () => {
    const chosen = pickSource<string>(doc, {
      purpose: 'add',
      sections: { files: files(async () => 'f'), examples: { list: [], open: async () => 'e' } },
    });
    $('.dc-picker-tab[data-section="files"]').dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'ArrowDown' }));
    assert.equal($('.dc-picker-tab[aria-selected="true"]').getAttribute('data-section'), 'examples');
    assert.match($('.dc-picker-panel').textContent ?? '', /No examples/);
    $<HTMLButtonElement>('.dc-picker-close').click();
    await chosen;
  });
});
