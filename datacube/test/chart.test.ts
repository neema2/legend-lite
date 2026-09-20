// Plot and treemap.
//
// A chart is easy to test badly -- assert it produced some SVG and
// move on. These assert the GEOMETRY instead, because every way a
// chart lies is geometric: a bar whose length is not proportional to
// its value, an axis that does not include zero, treemap rectangles
// that overlap or spill outside their bounds, or areas that do not
// match the numbers they claim to encode.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { chartData, squarify, toBarChart, toTreemap } from '../src/chart.ts';
import type { ResultTable } from '../src/result.ts';
import { TREE_COLUMN } from '../src/treeview.ts';

function table(
  labels: (string | null)[],
  values: (number | null)[],
  labelName = 'region',
): ResultTable {
  return {
    columns: [
      { name: labelName, type: 'String', values: labels },
      { name: 'amount', type: 'Float', values },
    ],
    rowCount: labels.length,
    epoch: 1,
    elapsedMs: 0,
  };
}

const rects = (svg: string) =>
  [...svg.matchAll(
    /<rect x="([-\d.]+)" y="([-\d.]+)" width="([\d.]+)" height="([\d.]+)"/g,
  )].map((m) => ({
    x: Number(m[1]),
    y: Number(m[2]),
    w: Number(m[3]),
    h: Number(m[4]),
  }));

describe('choosing what to chart', () => {
  it('takes the tree column as the label when there is one', () => {
    const t: ResultTable = {
      columns: [
        { name: TREE_COLUMN, type: 'String', values: ['EMEA', 'AMER'] },
        { name: 'amount', type: 'Float', values: [1, 2] },
      ],
      rowCount: 2,
      epoch: 1,
      elapsedMs: 0,
    };
    assert.deepEqual(chartData(t).map((d) => d.label), ['EMEA', 'AMER']);
  });

  it('skips rows whose value is not a number', () => {
    const d = chartData(table(['a', 'b', 'c'], [1, null, 3]));
    assert.deepEqual(d.map((p) => p.label), ['a', 'c']);
  });

  it('returns nothing when no column holds numbers', () => {
    const t: ResultTable = {
      columns: [{ name: 'a', type: 'String', values: ['x'] }],
      rowCount: 1,
      epoch: 1,
      elapsedMs: 0,
    };
    assert.deepEqual(chartData(t), []);
  });
});

describe('bar chart geometry', () => {
  it('makes bar length proportional to value', () => {
    const svg = toBarChart(table(['a', 'b', 'c'], [10, 20, 40]));
    const bars = rects(svg);
    assert.equal(bars.length, 3);
    const [a, b, c] = bars;
    assert.ok(a && b && c);
    // 10 : 20 : 40 within a pixel of rounding.
    assert.ok(Math.abs(b.h / a.h - 2) < 0.05, `${a.h} ${b.h}`);
    assert.ok(Math.abs(c.h / a.h - 4) < 0.05, `${a.h} ${c.h}`);
  });

  it('includes zero in the domain rather than starting at the minimum', () => {
    // An axis that silently starts at the minimum makes a small
    // difference look enormous. In a financial grid that is not
    // cosmetic.
    const svg = toBarChart(table(['a', 'b'], [100, 102]));
    const bars = rects(svg);
    const [a, b] = bars;
    assert.ok(a && b);
    // With zero included the two bars are nearly the same height.
    assert.ok(Math.abs(b.h / a.h - 1) < 0.05, `${a.h} vs ${b.h}`);
  });

  it('draws negatives on the other side of the zero line', () => {
    const svg = toBarChart(table(['a', 'b'], [10, -10]));
    const bars = rects(svg);
    const [a, b] = bars;
    assert.ok(a && b);
    // The positive bar ends where the negative one starts: the zero line.
    assert.ok(Math.abs((a.y + a.h) - b.y) < 0.01, `${a.y + a.h} vs ${b.y}`);
  });

  it('says so rather than drawing nothing when there is no data', () => {
    const svg = toBarChart(table([], []));
    assert.match(svg, /nothing numeric to plot/);
    assert.equal(rects(svg).length, 0);
  });

  it('escapes a label that would otherwise break the SVG', () => {
    const svg = toBarChart(table(['<script>&"'], [1]));
    assert.ok(!svg.includes('<script>'), svg);
    assert.match(svg, /&lt;script&gt;/);
  });

  it('carries a title and an accessible name', () => {
    const svg = toBarChart(table(['a'], [1]), { title: 'Q3 by region' });
    assert.match(svg, /aria-label="Q3 by region"/);
    assert.match(svg, /role="img"/);
  });
});

describe('treemap layout', () => {
  const bounds = { x: 0, y: 0, w: 400, h: 300 };

  it('gives every value a rectangle', () => {
    const out = squarify([5, 3, 2, 1], bounds);
    assert.equal(out.length, 4);
    assert.ok(out.every((r) => r.w > 0 && r.h > 0));
  });

  it('makes area proportional to value', () => {
    const values = [50, 25, 15, 10];
    const out = squarify(values, bounds);
    const total = bounds.w * bounds.h;
    const sum = values.reduce((a, b) => a + b, 0);
    out.forEach((r, i) => {
      const expected = ((values[i] ?? 0) / sum) * total;
      const actual = r.w * r.h;
      assert.ok(
        Math.abs(actual - expected) / expected < 0.02,
        `area ${actual} vs expected ${expected}`,
      );
    });
  });

  it('fills the bounds without spilling outside them', () => {
    const out = squarify([7, 5, 4, 3, 2, 1], bounds);
    for (const r of out) {
      assert.ok(r.x >= bounds.x - 0.01, `x ${r.x}`);
      assert.ok(r.y >= bounds.y - 0.01, `y ${r.y}`);
      assert.ok(r.x + r.w <= bounds.x + bounds.w + 0.01, `right ${r.x + r.w}`);
      assert.ok(r.y + r.h <= bounds.y + bounds.h + 0.01, `bottom ${r.y + r.h}`);
    }
  });

  it('never overlaps two rectangles', () => {
    // The failure that makes a treemap a lie: two boxes claiming the
    // same pixels means neither area means what it says.
    const out = squarify([9, 6, 5, 4, 3, 2, 1], bounds);
    for (let i = 0; i < out.length; i++) {
      for (let j = i + 1; j < out.length; j++) {
        const a = out[i];
        const b = out[j];
        if (!a || !b) continue;
        const overlap =
          a.x < b.x + b.w - 0.01
          && b.x < a.x + a.w - 0.01
          && a.y < b.y + b.h - 0.01
          && b.y < a.y + a.h - 0.01;
        assert.ok(!overlap, `rect ${i} overlaps ${j}`);
      }
    }
  });

  it('covers the whole area between them', () => {
    const values = [8, 5, 4, 2, 1];
    const out = squarify(values, bounds);
    const covered = out.reduce((a, r) => a + r.w * r.h, 0);
    const total = bounds.w * bounds.h;
    assert.ok(
      Math.abs(covered - total) / total < 0.02,
      `covered ${covered} of ${total}`,
    );
  });

  it('handles a single value by filling the bounds', () => {
    const [only] = squarify([42], bounds);
    assert.ok(only);
    assert.ok(Math.abs(only.w * only.h - bounds.w * bounds.h) < 1);
  });
});

describe('treemap rendering', () => {
  it('drops non-positive values and says how many', () => {
    // Area cannot be negative. Clamping would hide the row; drawing
    // its absolute value would state the opposite of the truth.
    const svg = toTreemap(table(['a', 'b', 'c'], [10, -5, 0]));
    assert.match(svg, /2 rows not shown/);
    assert.equal(rects(svg).length, 1);
  });

  it('uses the singular for one dropped row', () => {
    const svg = toTreemap(table(['a', 'b'], [10, -5]));
    assert.match(svg, /1 row not shown/);
  });

  it('says so when nothing can be laid out', () => {
    const svg = toTreemap(table(['a'], [-1]));
    assert.match(svg, /nothing positive to lay out/);
  });

  it('labels only the boxes big enough to hold a label', () => {
    // A clipped word in a sliver is noise, not information.
    const svg = toTreemap(
      table(['big', 'tiny'], [10000, 1]),
    );
    assert.ok(svg.includes('>big<'), 'the large box is labelled');
    assert.ok(!svg.includes('>tiny<'), 'the sliver is not');
  });
});
