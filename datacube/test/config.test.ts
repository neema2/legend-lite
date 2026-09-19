import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  DEFAULT_CONFIGURATION,
  applyToSnapshot,
  columnConfig,
  fromSnapshot,
  labelFor,
  resolvedWidths,
  toColumnAppearance,
  toColumnLayout,
  toFormats,
  withColumn,
  type CubeConfiguration,
} from '../src/config.ts';
import type { CubeSnapshot } from '../src/snapshot.ts';

const CUBE: CubeSnapshot = {
  source: { expression: 't' },
  columns: [
    { name: 'region', type: 'String' },
    { name: 'year', type: 'Integer' },
    { name: 'notional', type: 'Float' },
  ],
  derived: [],
  rows: ['region'],
  pivotOn: [],
  measures: [{ name: 'total', column: 'notional', fn: 'sum' }],
  sorts: [],
  epoch: 1,
};

describe('withColumn', () => {
  it('returns the SAME configuration when nothing changed', () => {
    // Identity lets a caller skip a re-render rather than rebuilding
    // the grid because a dropdown was reopened on its current value.
    const c = withColumn(DEFAULT_CONFIGURATION, 'region', { pinned: 'left' });
    assert.equal(withColumn(c, 'region', { pinned: 'left' }), c);
  });

  it('merges into a column rather than replacing it', () => {
    let c = withColumn(DEFAULT_CONFIGURATION, 'region', { pinned: 'left' });
    c = withColumn(c, 'region', { hidden: true });
    assert.deepEqual(columnConfig(c, 'region'), {
      pinned: 'left',
      hidden: true,
    });
  });

  it('clearing a setting leaves NO trace of it', () => {
    // `{pinned: undefined}` serialises as `"pinned": null`, which a
    // reader cannot tell from a deliberate null.
    let c = withColumn(DEFAULT_CONFIGURATION, 'region', { pinned: 'left' });
    c = withColumn(c, 'region', { pinned: undefined });
    assert.equal('pinned' in columnConfig(c, 'region'), false);
    assert.equal(JSON.stringify(c.columns), '{}', 'and the column goes too');
  });

  it('leaves other columns untouched', () => {
    let c = withColumn(DEFAULT_CONFIGURATION, 'region', { hidden: true });
    c = withColumn(c, 'year', { hidden: true });
    assert.deepEqual(Object.keys(c.columns).sort(), ['region', 'year']);
  });
});

describe('width modes', () => {
  it('fixed and range are different intentions, not different numbers', () => {
    // A user switching to (Any) and back must not have lost what
    // they typed, so all three values are kept and the MODE decides.
    const c = { widthMode: 'any', width: 120, minWidth: 80 } as const;
    assert.deepEqual(resolvedWidths(c), {});
    assert.deepEqual(resolvedWidths({ ...c, widthMode: 'fixed' }), {
      width: 120,
    });
    assert.deepEqual(resolvedWidths({ ...c, widthMode: 'range' }), {
      minWidth: 80,
    });
  });

  it('infers fixed from a bare width, for a config written by hand', () => {
    assert.deepEqual(resolvedWidths({ width: 90 }), { width: 90 });
  });
});

describe('projections', () => {
  const config: CubeConfiguration = {
    ...DEFAULT_CONFIGURATION,
    columnOrder: ['year', 'region'],
    columns: {
      region: {
        hidden: true,
        blurred: true,
        pinned: 'left',
        displayName: 'Region',
        widthMode: 'range',
        minWidth: 80,
        maxWidth: 200,
        format: { kind: 'text' },
        appearance: { bold: true },
      },
      year: { widthMode: 'fixed', width: 60 },
    },
  };

  it('splits one column record into the four consumers', () => {
    const layout = toColumnLayout(config);
    assert.deepEqual(layout.order, ['year', 'region']);
    assert.deepEqual(layout.hidden, ['region']);
    assert.deepEqual(layout.blurred, ['region']);
    assert.deepEqual(layout.pinned, { region: 'left' });
    assert.deepEqual(layout.displayNames, { region: 'Region' });
    assert.deepEqual(layout.widths, { year: 60 });
    assert.deepEqual(layout.minWidths, { region: 80 });
    assert.deepEqual(layout.maxWidths, { region: 200 });
    assert.deepEqual(toFormats(config), { region: { kind: 'text' } });
    assert.deepEqual(toColumnAppearance(config), { region: { bold: true } });
  });

  it('emits nothing for settings nobody set', () => {
    // An empty object per consumer, not an object of empty objects:
    // a saved view must not record opinions nobody held.
    assert.deepEqual(toColumnLayout(DEFAULT_CONFIGURATION), {});
    assert.deepEqual(toFormats(DEFAULT_CONFIGURATION), {});
    assert.deepEqual(toColumnAppearance(DEFAULT_CONFIGURATION), {});
  });

  it('labels a column by its display name, else its own name', () => {
    assert.equal(labelFor(config, 'region'), 'Region');
    assert.equal(labelFor(config, 'year'), 'year');
  });
});

describe('the snapshot boundary', () => {
  it('carries only the settings that change the SQL', () => {
    let config = withColumn(DEFAULT_CONFIGURATION, 'year', {
      kind: 'dimension',
      excludedFromPivot: true,
      // Colour cannot reach a query, and must not force a refetch.
      appearance: { bold: true },
    });
    config = { ...config, maxRows: 50, treeColumnSort: 'desc' };

    const s = applyToSnapshot(CUBE, config);
    assert.equal(s.maxRows, 50);
    assert.equal(s.treeColumnSort, 'desc');
    const year = s.columns.find((c) => c.name === 'year');
    assert.deepEqual(year, {
      name: 'year',
      type: 'Integer',
      kind: 'dimension',
      excludedFromPivot: true,
    });
    assert.equal('appearance' in (year as object), false);
  });

  it('does NOT bump the epoch', () => {
    // A caller that means to refetch bumps it; one that is only
    // reconciling must not, or every reconcile costs a round trip.
    assert.equal(applyToSnapshot(CUBE, DEFAULT_CONFIGURATION).epoch, 1);
  });

  it('reads a snapshot back, so the editor opens on what is running', () => {
    const running: CubeSnapshot = {
      ...CUBE,
      maxRows: 25,
      treeColumnSort: 'desc',
      columns: [
        { name: 'region', type: 'String' },
        { name: 'year', type: 'Integer', kind: 'dimension' },
        { name: 'notional', type: 'Float' },
      ],
    };
    const config = fromSnapshot(running);
    assert.equal(config.maxRows, 25);
    assert.equal(config.treeColumnSort, 'desc');
    assert.deepEqual(config.columns, { year: { kind: 'dimension' } });
  });

  it('round-trips', () => {
    const config = { ...fromSnapshot(CUBE), maxRows: 77 } as CubeConfiguration;
    const back = fromSnapshot(applyToSnapshot(CUBE, config));
    assert.equal(back.maxRows, 77);
  });
});
