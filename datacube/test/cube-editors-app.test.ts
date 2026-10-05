// Leg B through the APP (docs/DATACUBE_LEG_B_STATE_OWNER_2026_09_28.md): the editors work on live state (B4). Each test is
// an audit entry reproduced the way a person meets it, over the fixture's gated engine.

import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it } from 'node:test';

import { CubeController } from '../src/cube.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';
import { TreeState } from '../src/tree.ts';
import { GateEngine, SNAPSHOT, StubPlanner, tearDown } from './cube-fixture.ts';
import {
  app, dom, menu, root, settle, setUp,
} from './cube-fixture.ts';

beforeEach(setUp);
afterEach(tearDown);

describe('a compile refusal is RETURNED, never thrown (B4, P2-152)', () => {
  it('pivoting on a JSON column: compile answers with the refusal', async () => {
    const c = new CubeController(new GateEngine(), new StubPlanner());
    const snapshot: CubeSnapshot = { ...SNAPSHOT, columns: [...SNAPSHOT.columns, { name: 'meta', type: 'Variant' }],
      rows: ['region'], pivotOn: ['meta'] };
    const out = await c.compile({ snapshot, tree: TreeState.empty() }, null);
    assert.match(out?.refusal ?? '', /holds JSON/);
  });
});

describe('the Filters window works on the LIVE filter (B4, P2-144)', () => {
  const filtersWindow = (): HTMLElement | null => root.querySelector('[data-window="Filters"]:not([hidden])');
  const ok = (): void => {
    ([...(filtersWindow() ?? assert.fail('no Filters window')).querySelectorAll<HTMLButtonElement>('button')]
      .find((b) => b.textContent === 'OK') ?? assert.fail('no OK')).click();
  };

  it('unedited, it shows a filter added elsewhere', async () => {
    app.openFilters();
    menu('EMEA', "Add Filter: region = 'EMEA'");
    await settle();
    const shown = [...(filtersWindow() ?? assert.fail('closed')).querySelectorAll<HTMLInputElement>('input')].map((i) => i.value);
    assert.ok(shown.includes('EMEA'), `the window shows the cube's filter: ${shown.join(' | ')}`);
  });

  const valueInput = (v: string): HTMLInputElement =>
    [...(filtersWindow() ?? assert.fail('closed')).querySelectorAll<HTMLInputElement>('input')]
      .find((x) => x.value === v) ?? assert.fail(`no ${v} value in the window`);
  const values = (): string[] => {
    const f = app.snapshot.filter;
    const all = f === undefined ? [] : f.kind === 'condition' ? [f] : 'children' in f ? f.children : [f];
    return all.map((c) => ('value' in c ? String(c.value) : '?'));
  };

  it('the audit\'s case: a condition added elsewhere, THEN an edit here -- OK keeps both', async () => {
    menu('EMEA', "Add Filter: region = 'EMEA'");
    await settle();
    app.openFilters();
    menu('AMER', "Add Filter: region != 'AMER'");
    await settle();
    const value = valueInput('EMEA');
    value.value = 'APAC';
    value.dispatchEvent(new dom.window.Event('change'));
    ok();
    await settle();
    assert.deepEqual(values(), ['APAC', 'AMER'], 'the edit landed on the filter the cube had');
    assert.equal(filtersWindow(), null);
  });

  it('an edit here, THEN a condition added elsewhere: OK says so instead of dropping it; a second OK replaces it', async () => {
    menu('EMEA', "Add Filter: region = 'EMEA'");
    await settle();
    app.openFilters();
    const value = valueInput('EMEA');
    value.value = 'APAC';
    value.dispatchEvent(new dom.window.Event('change'));
    menu('AMER', "Add Filter: region != 'AMER'");
    await settle();
    ok();
    await settle();
    assert.deepEqual(values(), ['EMEA', 'AMER'], 'the condition added elsewhere is still there');
    assert.ok(filtersWindow(), 'the window stays open, the edit kept');
    assert.match(filtersWindow()?.textContent ?? '', /changed while this window was open/);
    ok();
    await settle();
    assert.deepEqual(values(), ['APAC'], 'the second OK replaces it, knowingly');
    assert.equal(filtersWindow(), null);
  });
});
