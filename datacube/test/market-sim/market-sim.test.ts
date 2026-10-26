// The market world in the tab (demo/market-sim, docs/DUCKDB_LIVE_2026_10_25.md L2): Java's random exactly (the server's
// world's draws, held to values Java printed), every table's shape the store's declaration, and a world run on a clock
// of the test's own -- its reference, its morning's tape, its beats' quotes, prints, stories, orders, fills, rates and
// requests, each as a market's are -- the same rows from the same seed.

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';

import type { MadeColumn } from '../../../engine-client/src/arrow-ipc.ts';
import { runfile } from '../../../tools/js/runfiles.mts';
import { MarketWorld, type MarketSettings } from '../../demo/market-sim/market.ts';
import { JavaRandom } from '../../demo/market-sim/random.ts';
import { MARKET_TABLES, type ColumnKind, type MarketTable } from '../../demo/market-sim/tables.ts';

/** A column's values, row by row: text as its strings, every other kind as made. */
function values(c: MadeColumn, count: number): unknown[] {
  const out: unknown[] = [];
  for (let i = 0; i < count; i++) {
    if ('text' in c) {
      const k = c.pick(i);
      out.push(k === null ? null : c.text[k]);
    } else if ('doubles' in c) out.push(c.doubles(i));
    else if ('integers' in c) out.push(c.integers(i));
    else if ('instants' in c) out.push(c.instants(i));
    else if ('days' in c) out.push(c.days(i));
    else out.push(c.truths(i));
  }
  return out;
}

/** Every row the world wrote, by table: each a record of its columns. */
type Written = Map<string, Record<string, unknown>[]>;

/** A world on a clock of the test's own (no timers, no feed), its writes kept. */
async function world(settings: MarketSettings = {}): Promise<{ world: MarketWorld; written: Written; tick: (ms: number) => void }> {
  let now = Date.UTC(2026, 9, 26, 15, 0, 0);
  const written: Written = new Map();
  const w = await MarketWorld.open({ feed: 'off', timer: false, names: 40, morning: 120, clock: () => now, ...settings },
    async (table, count, rows) => {
      const columns = Object.fromEntries(Object.entries(rows).map(([name, c]) => [name, values(c, count)]));
      const out = written.get(table.name) ?? [];
      for (let i = 0; i < count; i++) out.push(Object.fromEntries(Object.entries(columns).map(([name, v]) => [name, v[i]])));
      written.set(table.name, out);
    });
  return { world: w, written, tick: (ms) => { now += ms; } };
}

/** `seconds` of the world's clock, a beat every 50 ms, a flush every 250. */
async function run(w: { world: MarketWorld; tick: (ms: number) => void }, seconds: number): Promise<void> {
  for (let b = 0; b < seconds * 20; b++) {
    w.tick(50);
    w.world.beat();
    if (b % 5 === 4) await w.world.flush();
  }
  await w.world.flush();
}

describe('the market world in the tab', () => {
  it('draws as Java\'s Random does, seed for seed (values Java printed: the server\'s world\'s draws)', () => {
    const r = new JavaRandom(20261024);
    assert.deepEqual([r.nextInt(), r.nextInt(26), r.nextInt(64), r.nextInt(1000000007)], [-1849726331, 23, 42, 89086152]);
    assert.deepEqual([r.nextDouble(), r.nextBoolean()], [0.18881116249226948, false]);
    assert.deepEqual([r.nextGaussian(), r.nextGaussian(), r.nextGaussian()], [0.7224297159524398, -0.5206664772544368, 0.5805108930557601]);
    assert.deepEqual(r.shuffle([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]), [8, 5, 4, 7, 1, 9, 6, 3, 0, 2]);
    let sum = 0;
    for (let i = 0; i < 100000; i++) sum += r.nextGaussian() + r.nextDouble() + r.nextInt(7);
    assert.equal(sum, 350275.735684089);
  });

  it('writes every table the trading model declares, every column, of its type', () => {
    const model = readFileSync(runfile(process.env.TRADING_MODEL!), 'utf8');
    const kinds: Record<string, ColumnKind> = { VARCHAR: 'text', DOUBLE: 'double', INTEGER: 'integer', TIMESTAMP: 'instant', BIT: 'truth', DATE: 'day' };
    for (const table of MARKET_TABLES.filter((t: MarketTable) => t.name !== 'world')) {
      const declared = new RegExp(`^\\s*Table ${table.name} \\((.*)\\)\\s*$`, 'm').exec(model)?.[1];
      assert.ok(declared !== undefined, `the model declares ${table.name}`);
      const columns = Object.fromEntries(declared.split(/,\s*(?![^(]*\))/).map((c) => {
        const [name, type] = c.trim().split(/\s+/) as [string, string];
        return [name, kinds[type.replace(/\(.*$/, '')]];
      }));
      assert.deepEqual(table.shape, columns, table.name);
    }
  });

  it('writes the reference once, then the morning\'s tape and wire, then each beat\'s rows', async () => {
    const w = await world();
    const { written } = w;
    const count = (t: string): number => written.get(t)?.length ?? 0;
    assert.equal(count('EXCHANGE'), 2);
    assert.equal(count('SECTOR'), 12);
    assert.equal(count('EQUITY'), 40);
    assert.equal(count('INSTRUMENT'), 40 + 8 + w.world.bank.options.length);
    assert.equal(w.world.bank.options.length, 4 * 3 * 9 * 2);
    assert.deepEqual([count('BUSINESS_UNIT'), count('TRADER'), count('DESK'), count('BOOK'), count('COUNTERPARTY')], [1, 18, 6, 10, 20]);
    assert.equal(count('RISK_LIMIT'), 10, 'a day loss and a gross limit for each desk that holds a position (FX holds none)');
    assert.equal(count('FX_RATE'), 10, 'every pair priced at the start');
    // the morning: the session opened two minutes ago, each print in order, numbered by its place
    const tape = written.get('MARKET_TRADE')!;
    assert.ok(tape.length > 50, `a morning's prints: ${tape.length}`);
    tape.forEach((p, i) => {
      assert.equal(p.SEQUENCE_NUMBER, i + 1);
      assert.ok((p.PRINT_TIME as number) >= w.world.sessionOpen && (p.PRINT_TIME as number) < w.world.sessionOpen + 120_000);
      if (i > 0) assert.ok((p.PRINT_TIME as number) >= (tape[i - 1]!.PRINT_TIME as number));
    });
    assert.equal(count('MARKET_QUOTE'), 0, 'a quote is the latest: the morning writes none');
    const morning = tape.length;
    assert.equal(w.world.counts.morningPrints, morning);

    await run(w, 20);
    const quotes = written.get('MARKET_QUOTE')!;
    assert.ok(quotes.length >= 40, 'every equity quoted at the first beat');
    for (const q of quotes) {
      assert.ok((q.BID as number) < (q.ASK as number), `${String(q.QUOTE_ID)}: bid under ask`);
      assert.equal(q.MID, ((q.BID as number) + (q.ASK as number)) / 2);
      assert.ok((q.BID_SIZE as number) > 0 && (q.ASK_SIZE as number) > 0);
    }
    const prints = written.get('MARKET_TRADE')!.slice(morning);
    assert.ok(prints.length > 100, `twenty seconds' prints: ${prints.length}`);
    for (const p of prints) {
      assert.ok(['B', 'S', null].includes(p.AGGRESSOR_SIDE as string | null));
      assert.equal(p.IS_OFF_EXCHANGE, p.AGGRESSOR_SIDE === null, 'only an off-exchange print has no aggressor');
      assert.ok(['@', 'I'].includes(p.TRADE_CONDITION as string));
    }
    assert.deepEqual(written.get('MARKET_TRADE')!.map((p) => p.SEQUENCE_NUMBER), written.get('MARKET_TRADE')!.map((_, i) => i + 1));
    // the bank's day: orders worked into fills and routes, priced requests, the world said
    const orders = written.get('TRADE_ORDER') ?? [];
    assert.ok(orders.length > 0, 'orders arrive');
    const executions = written.get('EXECUTION') ?? [];
    const ids = new Set(orders.map((o) => o.ORDER_ID));
    for (const e of executions) assert.ok(ids.has(e.ORDER_ID), 'a fill is of an order');
    for (const r of written.get('FX_RATE')!) assert.ok((r.BID_RATE as number) < (r.ASK_RATE as number));
    const rfqs = written.get('RFQ') ?? [];
    assert.ok(rfqs.length > 0, 'clients ask for prices');
    assert.ok(rfqs.every((r) => ['NEW', 'QUOTED', 'DECLINED', 'DONE', 'MISSED', 'EXPIRED'].includes(r.STATUS as string)));
    assert.deepEqual((written.get('world') ?? []).slice(0, 4).map((r) => [r.item, r.state]),
      [['equities', 'simulated'], ['crypto', 'simulated'], ['news', 'simulated'], ['filings', 'simulated']]);
    w.world.close();
  });

  it('a minute of news: stories in time order, each with a source, a category and an urgency; a story\'s move its sign', async () => {
    const w = await world({ news: 120 });
    await run(w, 60);
    const stories = w.written.get('NEWS_STORY')!;
    assert.ok(stories.length > 50, `a busy wire's minute: ${stories.length}`);
    stories.forEach((s, i) => {
      assert.match(s.STORY_ID as string, /^NS-\d{7}$/);
      assert.ok(typeof s.HEADLINE === 'string' && (s.HEADLINE as string).length > 10);
      assert.ok(['FLASH', 'STORY', 'BRIEF'].includes(s.URGENCY as string));
      if (i > 0) assert.ok((s.PUBLISH_TIME as number) > (stories[i - 1]!.PUBLISH_TIME as number), 'a story a millisecond after the last at least');
      const sentiment = s.SENTIMENT as number | null;
      if (sentiment !== null) assert.ok(sentiment >= -1 && sentiment <= 1);
    });
    const categories = new Set(stories.map((s) => s.CATEGORY));
    for (const c of ['Macro', 'Industry', 'Analyst']) assert.ok(categories.has(c), `a minute has ${c} stories`);
    w.world.close();
  });

  it('the same seed, the same world; another seed, another', async () => {
    const a = await world();
    const b = await world();
    const c = await world({ seed: 7 });
    await run(a, 5);
    await run(b, 5);
    await run(c, 5);
    const first = (w: { written: Written }, t: string): unknown => w.written.get(t)!.slice(0, 200);
    assert.deepEqual(first(a, 'MARKET_TRADE'), first(b, 'MARKET_TRADE'));
    assert.deepEqual(first(a, 'INSTRUMENT'), first(b, 'INSTRUMENT'));
    assert.notDeepEqual(first(a, 'INSTRUMENT'), first(c, 'INSTRUMENT'));
    for (const w of [a, b, c]) w.world.close();
  });
});
