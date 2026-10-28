// THE TRADING DESK'S MODEL (demo/deephaven/market/market.pure; the user, 2026-10-27: "screens as model"): every source
// the desk's screens read is a function of the desk's own model, and each is planned here on both of its data space's
// contexts (environments.pure: docs/ONE_CONNECTION_DESK_2026_10_28.md D2) by the product's planner -- no server: the
// model compiles, each function's body types against the columns it declares, and each plan is Deephaven's table
// operations on the one context and DuckDB's SQL on the other. Running the plans is the desk's own lanes' (on a server,
// //datacube:deephaven_desk_test; in the tab, //datacube:verify_live_desk_test).

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import { runfileFromEnv } from '../../tools/js/runfiles.mts';
import { DESK_MODEL, IN_TAB, ON_DEEPHAVEN } from '../demo/deephaven/market/model.ts';
import * as screens from '../demo/deephaven/market/screens.ts';
import { plannerFor } from './catalog-builder.ts';

const corpus = readFileSync(runfileFromEnv('TRADING_MODEL'), 'utf8');
const desk = readFileSync(runfileFromEnv('MARKET_PURE'), 'utf8');
const environments = readFileSync(runfileFromEnv('ENVIRONMENTS_PURE'), 'utf8');
const model = DESK_MODEL(corpus, desk, environments);
const planner = plannerFor(model, ON_DEEPHAVEN.runtime);
/** The same model on the data space's other context: the tab's DuckDB. */
const inTab = plannerFor(model, IN_TAB.runtime);

/** The module's texts, by the name each is exported under. */
const texts: readonly (readonly [string, string])[] = Object.entries(screens as Readonly<Record<string, unknown>>)
  .flatMap(([name, value]) => (typeof value === 'string' ? [[name, value] as const] : []));

/** Every source the screens name: a call of one of the desk's model's functions. */
const sources = texts.filter(([, text]) => text.startsWith('market::screens::'));

/** A source's plan: its operations, as the Deephaven target writes them. */
async function opsOf(text: string): Promise<{ readonly ops: readonly Record<string, Record<string, unknown>>[]; readonly columns: number }> {
  const plan = await planner.planText(`|${text}`);
  return { ops: (JSON.parse(plan.sql) as { ops: Record<string, Record<string, unknown>>[] }).ops, columns: plan.columns.length };
}

describe('the trading desk\'s model', () => {
  it('every source a screen reads is a function of the desk\'s model, its arguments the only text the screens write', () => {
    assert.ok(sources.length >= 20, `the screens' sources: ${sources.map(([n]) => n).join(', ')}`);
    for (const [name, text] of sources) {
      assert.match(text, /^market::screens::\w+\((?:[\d.]+(?:, [\d.]+)*)?\)$/, `${name} is a call of a screen's function: ${text}`);
    }
    // and no query written here: no class read, no relation function applied
    for (const [name, text] of texts) assert.doesNotMatch(text, /\.all\(\)|->/, `${name} writes a query of its own: ${text}`);
  });

  for (const [name, text] of sources) {
    it(`${name}, ${text}, plans on the market's Deephaven runtime, and is a grid's base the server can store`, async () => {
      const { ops, columns } = await opsOf(text);
      assert.ok(ops.length > 0 && columns > 0, `${text}: ${ops.length} operations, ${columns} columns`);
      // a grid over it keeps its base stored on the server (src/server-base.ts B1): its plan has a stored form
      // (2026-10-27: a view's shared subtrees' helpers, dropped after it, had none -- three of the desk's tabs empty)
      const stored = await planner.deephaven('stored', { plan: (await planner.planText(`|${text}`)).sql });
      assert.ok(stored.startsWith('OK\n'), `${text}: its base has no stored form: ${stored.replace(/\n/g, ' | ')}`);
    });

    it(`${name} plans in the tab too: the same query, DuckDB's SQL, the same columns`, async () => {
      const [onServer, here] = await Promise.all([planner.planText(`|${text}`), inTab.planText(`|${text}`)]);
      assert.match(here.sql, /^\s*(SELECT|WITH)\b/i, `${text} in the tab: ${here.sql.slice(0, 120)}`);
      assert.deepEqual(here.columns.map((c) => [c.name, c.type]), onServer.columns.map((c) => [c.name, c.type]));
    });
  }

  it('Black and Scholes, written as a textbook writes it, reaches the server as short formulas: each shared piece once', async () => {
    // the model's functions inlined write d1 wherever they read it -- each greek a thousand characters and more, which
    // the server parses in time growing faster than their length; the Deephaven target computes a subtree its columns
    // share once, as a helper column (DeephavenBatch.withShared), so no formula is long
    for (const text of [screens.OPTION_CHAIN, screens.OPTION_BOOK]) {
      const { ops } = await opsOf(text);
      const formulas = ops.flatMap((o) => (Object.values(o)[0]?.['columnSpecs'] as string[] | undefined) ?? []);
      const longest = formulas.reduce((a, b) => (b.length > a.length ? b : a), '');
      assert.ok(longest.length < 1_000, `${text}: its longest formula ${longest.length} characters: ${longest.slice(0, 200)}`);
      assert.ok(formulas.some((f) => /^price = .*__e\d+/.test(f)), `${text}: the price reads its shared pieces by name`);
    }
  });

  it('a watchlist starts on the market\'s five largest names, ranked on the server', async () => {
    // the watchlist itself is the person's (its pane's filter, saved with the page); its default is the model's ranking
    const { ops } = await opsOf(screens.WATCHLIST_START);
    const kinds = ops.map((o) => Object.keys(o)[0]);
    const head = ops.findIndex((o) => o['head']?.['numRows'] === '5');
    assert.ok(head > 0 && kinds[head - 1] === 'sort', `the five largest: ${kinds.join(' ')}`);
  });
});
