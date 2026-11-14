// FROZEN ANSWERS (docs/TS_ONE_TREE_PLAN_2026_10_31.md §6, step 1): every series of every example page's timeseries
// tiles, answered on the pages' own samples, held to the answer it gave when it was frozen. The gate every later step
// of the plan is checked against: a test comparing #TS with #TS cannot see a change of meaning, a frozen answer can.
//
// An answer is held by its points -- each date and line with a value, its figures to nine significant digits (a
// figure under 1e-9 in size is 0) -- as their count, a digest of them all, and its first and last two, which a
// reader can check by eye. A series whose answer differs fails, named, with its points before and after.
//
// A number changed on purpose (a ruling of the plan's §4, a fix) changes its frozen answer, with a dated reason in the
// commit that does it. The test writes the answers it got to its undeclared outputs (`bazel-testlogs/<target>/
// test.outputs/`) on every run; to take them, run it with `<ENV>_UPDATE` naming the fixture to write, from outside the
// sandbox:
//   bazel test //datacube:timeseries_ts_parity_test --strategy=TestRunner=local \
//     --test_env=TS_ANSWERS_UPDATE=$PWD/datacube/test/fixtures/timeseries/gallery-answers.json
// and read the diff before committing it.

import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { runfileFromEnv } from '../../tools/js/runfiles.mts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import { TS_HIGH, TS_LOW, TS_LOWER, TS_OPEN, TS_UPPER, TS_VALUE, TS_WIDTH } from '../src/timeseries-query.ts';

/** A series' answer, frozen: its points' count, a digest of them all, and its first and last two. */
export interface Frozen {
  readonly points: number;
  readonly digest: string;
  readonly head: readonly string[];
  readonly tail: readonly string[];
}

/** The columns a series' answer draws: a band's three, a candle's four, else its value. */
export function drawnColumns(q: { readonly band: boolean; readonly candles?: boolean }): readonly string[] {
  if (q.band) return [TS_LOWER, TS_UPPER, TS_WIDTH];
  if (q.candles === true) return [TS_OPEN, TS_HIGH, TS_LOW, TS_VALUE];
  return [TS_VALUE];
}

/** A cell as the frozen answer spells it: a date as its ISO text, anything else as its text. */
function key(v: unknown): string {
  if (v instanceof Date) return v.toISOString();
  return typeof v === 'bigint' ? v.toString() : String(v);
}

/** A figure to nine significant digits, one under 1e-9 in size as 0; no figure as `-`. A big integer or a decimal comes
 *  as text or a bigint (DuckDB's HUGEINT and DECIMAL: an on-balance volume's), and is read as its number. */
function figure(v: unknown): string {
  const n = typeof v === 'bigint' ? Number(v) : typeof v === 'string' && v.trim() !== '' ? Number(v) : v;
  if (n === null || n === undefined || typeof n !== 'number' || !Number.isFinite(n)) return '-';
  if (Math.abs(n) < 1e-9) return '0';
  return String(Number(n.toPrecision(9)));
}

function values(t: ResultTable, name: string): readonly unknown[] {
  const c = t.columns.find((x) => x.name === name);
  if (!c) throw new Error(`the answer has no column ${name} (it has ${t.columns.map((x) => x.name).join(', ')})`);
  return c.values;
}

/** `t`'s points -- each row with a figure in one of `names`, by its date and its line (`split`'s value) -- in order. */
export function points(t: ResultTable, date: string, split: string | undefined, names: readonly string[]): string[] {
  const dates = values(t, date);
  const lines = split === undefined ? undefined : values(t, split);
  const cols = names.map((n) => values(t, n));
  const out: string[] = [];
  for (let i = 0; i < t.rowCount; i++) {
    const figures = cols.map((c) => figure(c[i]));
    if (figures.every((f) => f === '-')) continue;
    out.push(`${key(dates[i])}|${lines === undefined ? '' : key(lines[i])}=${figures.join(',')}`);
  }
  return out.sort();
}

/** `t`'s answer, frozen. */
export function frozenOf(t: ResultTable, date: string, split: string | undefined, names: readonly string[]): Frozen {
  const p = points(t, date, split, names);
  return {
    points: p.length,
    digest: createHash('sha256').update(p.join('\n')).digest('hex').slice(0, 16),
    head: p.slice(0, 2),
    tail: p.slice(-2),
  };
}

/** What changed between a series' frozen answer and the one it gave. */
function changed(was: Frozen, now: Frozen): string {
  const say = (f: Frozen): string => `${f.points} points, ${f.head.join(' ')} ... ${f.tail.join(' ')}`;
  return `was ${say(was)}; now ${say(now)}`;
}

/**
 * Every answer of `got` held to the fixture the environment variable `env` names (a runfile), each series by its id:
 * none changed, none missing, none new. With `<env>_UPDATE` naming a file, `got` is written there instead, sorted by
 * id, and held to nothing. Either way `got` is written to the test's undeclared outputs, as `<what>.json`.
 */
export function holdFrozen(env: string, got: ReadonlyMap<string, Frozen>, what: string): void {
  const sorted = Object.fromEntries([...got.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)));
  const text = `${JSON.stringify(sorted, null, 2)}\n`;
  const outputs = process.env['TEST_UNDECLARED_OUTPUTS_DIR'];
  if (outputs) writeFileSync(join(outputs, `${what}.json`), text);
  const update = process.env[`${env}_UPDATE`];
  if (update) {
    const empty = [...got].filter(([, f]) => f.points === 0).map(([id]) => id);
    assert.deepEqual(empty, [], `${what}: a series that draws nothing is never frozen as right`);
    writeFileSync(update, text);
    return;
  }
  const frozen = JSON.parse(readFileSync(runfileFromEnv(env), 'utf8')) as Record<string, Frozen>;
  const problems: string[] = [];
  for (const [id, now] of got) {
    const was = frozen[id];
    if (now.points === 0) problems.push(`${id}: no points -- a series that draws nothing is never frozen as right`);
    else if (was === undefined) problems.push(`${id}: new, not frozen`);
    else if (was.digest !== now.digest || was.points !== now.points) problems.push(`${id}: ${changed(was, now)}`);
  }
  for (const id of Object.keys(frozen)) if (!got.has(id)) problems.push(`${id}: frozen, but not answered`);
  assert.deepEqual(problems, [], `${what}: every series answers as frozen (a change on purpose updates the fixture: `
    + 'datacube/test/timeseries-frozen.ts says how)');
}
