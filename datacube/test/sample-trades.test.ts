// The demo's sample trades (src/sample-trades.ts): the table legend-lite's SYNTHESIZE plan makes for the demo's request
// (register S303), run by the hosts' runner on DuckDB-WASM as the page runs it -- its rows the database's, the plan
// holding none -- and the CSV file of the same rows, which a page saved over the start's grid reopens.

import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { after, before, describe, it } from 'node:test';
import { openDatabase } from '#test-host';

import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import { runLoad } from '../../engine-client/src/plan-runner.ts';
import { runfileFromEnv } from '../../tools/js/runfiles.mts';
import { DESK_TRADES, DESK_TRADES_FILE, SAMPLE_TRADES, sampleTradesCsv, sampleTradesRequest } from '../src/sample-trades.ts';
import { sampleById, sampleFileName } from '../src/samples.ts';
import { ingestFile, type DuckDbFiles } from '../src/upload.ts';
import { plannerFor } from './catalog-builder.ts';

/** The demo's model (demo/trades.pure): the table the sample fills, as the page's planner reads it. */
const MODEL = readFileSync(runfileFromEnv('TRADES_MODEL'), 'utf8');

let engine: DuckDbEngine;
let files: DuckDbFiles;

before(async () => {
  const db = await openDatabase();
  engine = new DuckDbEngine(db.connection);
  files = db.files;
  // the start's table, as the page makes it (demo/boot.ts synthesizeTrades): the planner's plan, run by the hosts' runner
  const plan = await plannerFor(MODEL, 'trades::RT').synthesize(sampleTradesRequest(MODEL, engine.databaseType));
  await engine.exclusive((held) => runLoad(plan, (sql, params) => held.rows(sql, params)));
});

after(async () => {
  await engine?.close();
});

const one = (sql: string): Promise<unknown> => engine.run(sql, 0).then((t) => t.columns[0]?.values[0]);
/** A table's columns and their types, each name as DuckDB matches it: without regard to case (the synthesized table's
 *  columns are spelled as the model's seed makes them, upper case; the file's as its header has them). */
const columns = async (table: string): Promise<string[]> => {
  const t = await engine.run(`SELECT lower(column_name), data_type FROM duckdb_columns()
    WHERE lower(table_name) = lower('${table}') ORDER BY column_index`, 0);
  return t.columns[0]!.values.map((name, i) => `${String(name)} ${String(t.columns[1]!.values[i])}`);
};
/** The same rows both ways, every row counted (EXCEPT ALL keeps duplicates). */
const differing = (a: string, b: string): Promise<unknown> => one(`SELECT count(*) FROM ((SELECT * FROM ${a} EXCEPT ALL
  SELECT * FROM ${b}) UNION ALL (SELECT * FROM ${b} EXCEPT ALL SELECT * FROM ${a}))`);

describe("the demo's sample trades, made by the database as the plan says", () => {
  it('the table the model declares, every row the database made', async () => {
    assert.deepEqual(await columns('TRADES'), ['region VARCHAR', 'desk VARCHAR', 'book VARCHAR', 'year INTEGER',
      'qtr VARCHAR', 'notional DOUBLE', 'pnl DOUBLE', 'qty INTEGER']);
    assert.equal(Number(await one('SELECT count(*) FROM TRADES')), SAMPLE_TRADES);
    // each knob's values -- every value of a list and of a whole-number range, a decimal range's within it -- and no NULL
    assert.deepEqual((await engine.run(`SELECT count(DISTINCT region), count(DISTINCT desk), count(DISTINCT qtr),
      count(DISTINCT book), count(DISTINCT year), min(year), max(year), count(DISTINCT qty), min(qty), max(qty),
      CAST(min(notional) >= 0 AND max(notional) <= 9999.99 AND min(pnl) >= -1000 AND max(pnl) <= 999.99 AS INTEGER),
      count(*) FILTER (WHERE region IS NULL OR desk IS NULL OR year IS NULL OR qtr IS NULL OR book IS NULL
        OR notional IS NULL OR pnl IS NULL OR qty IS NULL) FROM TRADES`, 0))
      .columns.map((c) => Number(c.values[0])), [3, 5, 4, 4, 5, 2021, 2025, 97, 1, 97, 1, 0]);
    // the columns independent (synthesize's mixed radix and a multiplier per range, b58d24265): every combination of the
    // lists and the year occurs, as the demo's pivots by year and desk, or by quarter and book, show them
    const combinations = async (columns: string): Promise<number> =>
      Number(await one(`SELECT count(*) FROM (SELECT DISTINCT ${columns} FROM TRADES)`));
    assert.deepEqual([await combinations('qtr, book'), await combinations('desk, year'),
      await combinations('region, desk, year, qtr, book')], [16, 25, 1200]);
  });
});

describe("the start's grid written down as the sample file of its rows (the user, 2026-10-23: the start page could not be saved)", () => {
  it('the file is the same rows: what a page saved over the start reopens', async () => {
    // read in as the tab reads a sample it rebuilds (sources.ts fileFor, then upload.ts ingestFile)
    const read = await ingestFile(engine, files, new File([sampleTradesCsv()], DESK_TRADES_FILE.name), plannerFor('', 'local::RT'));
    // the same columns, each the same Pure type: a whole number read from the file is a BIGINT where the model declares
    // an INTEGER, both Pure's Integer
    assert.deepEqual(await columns(read.table), ['region VARCHAR', 'desk VARCHAR', 'year BIGINT', 'qtr VARCHAR',
      'book VARCHAR', 'notional DOUBLE', 'pnl DOUBLE', 'qty BIGINT']);
    const asTheFile = '(SELECT region, desk, year, qtr, book, notional, pnl, qty FROM TRADES)';
    assert.equal(Number(await differing(`"${read.table}"`, asTheFile)), 0);
  });

  it('its record is that file\'s name, size and SHA-256, known without making it as the page starts', () => {
    const bytes = new TextEncoder().encode(sampleTradesCsv(SAMPLE_TRADES));
    assert.deepEqual(DESK_TRADES_FILE, {
      name: DESK_TRADES_FILE.name, size: bytes.length, sha256: createHash('sha256').update(bytes).digest('hex'),
    });
  });

  it('a sample the page rebuilds: the same file, under the name its record has', () => {
    const sample = sampleById(DESK_TRADES);
    assert.ok(sample, 'a sample of the same rows');
    assert.equal(sample.build(1000), sampleTradesCsv(1000));
    assert.equal(sampleFileName(sample), DESK_TRADES_FILE.name);
  });
});
