// The demo's sample trades (src/sample-trades.ts), held to the statement that made them before D5: every row of
// 200,000 and every column's type the same as DuckDB's own `range()` made them, put in by DuckDB's Arrow reader -- and
// the Parquet file the site ships them as (the user's ruling of 2026-11-12, "Demo has to go through platform": made at
// build time, BUILD.bazel :sample_trades_parquet), read as the demo reads it, by the planned load of a person's file.

import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { after, before, describe, it } from 'node:test';
import { openDatabase } from '#test-host';

import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import { DESK_TRADES, DESK_TRADES_FILE, SAMPLE_TRADES, sampleTrades, sampleTradesCsv } from '../src/sample-trades.ts';
import { sampleById, sampleFileName } from '../src/samples.ts';
import { ingestFile, loadFileInto, type DuckDbFiles } from '../src/upload.ts';
import { runfileFromEnv } from '../../tools/js/runfiles.mts';
import { plannerFor } from './catalog-builder.ts';

let engine: DuckDbEngine;
let files: DuckDbFiles;

before(async () => {
  const db = await openDatabase();
  engine = new DuckDbEngine(db.connection);
  files = db.files;
});

after(async () => {
  await engine?.close();
});

/** The statement demo/boot.ts ran before D5, as it was. */
const BEFORE = `CREATE OR REPLACE TABLE before AS
     SELECT
       CASE (i % 3) WHEN 0 THEN 'EMEA' WHEN 1 THEN 'AMER' WHEN 2 THEN 'APAC' END AS region,
       CASE ((i // 3) % 5) WHEN 0 THEN 'Rates' WHEN 1 THEN 'Credit' WHEN 2 THEN 'FX' WHEN 3 THEN 'Equity'
         WHEN 4 THEN 'Commodities' END AS desk,
       (2021 + ((i // 15) % 5))                AS year,
       ('Q' || (1 + ((i // 75) % 4)))          AS qtr,
       ('Book ' || (1 + ((i // 300) % 4)))     AS book,
       ((i * 7919) % 1000000) / 100.0  AS notional,
       ((i * 104729) % 200000) / 100.0 - 1000.0 AS pnl,
       ((i * 31) % 97) + 1             AS qty
     FROM range(${SAMPLE_TRADES}) t(i)`;

const one = (sql: string): Promise<unknown> => engine.run(sql, 0).then((t) => t.columns[0]?.values[0]);
/** The generator's rows put in by DuckDB's own Arrow import, on the held connection (no statement made them). */
const arrowMade = (table: string): Promise<void> => engine.exclusive((held) => held.insertArrow(sampleTrades(), { table }, true));
const columns = async (table: string): Promise<string[]> => {
  const t = await engine.run(`SELECT column_name, data_type FROM duckdb_columns() WHERE table_name = '${table}'
    ORDER BY column_index`, 0);
  return t.columns[0]!.values.map((name, i) => `${String(name)} ${String(t.columns[1]!.values[i])}`);
};

describe("the demo's sample trades", () => {
  it('every row and every column type as the statement before D5 made them', async () => {
    await engine.run(BEFORE, 0);
    await arrowMade('made');
    assert.deepEqual(await columns('made'), await columns('before'));
    assert.equal(Number(await one('SELECT count(*) FROM made')), SAMPLE_TRADES);
    // both ways, every row counted (EXCEPT ALL keeps duplicates)
    assert.equal(Number(await one(`SELECT count(*) FROM ((SELECT * FROM made EXCEPT ALL SELECT * FROM before)
      UNION ALL (SELECT * FROM before EXCEPT ALL SELECT * FROM made))`)), 0);
  });
});

describe("the demo's sample as the site ships it: a Parquet file made at build time, read as a person's file is", () => {
  it('every row and every column type as the generator makes them, through the planned load of a host\'s file', async () => {
    const bytes = new Uint8Array(readFileSync(runfileFromEnv('SAMPLE_TRADES_PARQUET')));
    const file = { at: 'sample-trades.parquet', name: 'sample-trades.parquet', format: 'parquet' as const,
      digest: createHash('sha256').update(bytes).digest('hex'), size: bytes.length };
    await files.registerFileBuffer(file.at, bytes.slice());
    // the function the demo's start reads it with (demo/boot.ts loadSample), a person's upload too
    await loadFileInto(engine, file, 'parquet_made', plannerFor('', 'local::RT'));
    await engine.run(BEFORE, 0);
    assert.deepEqual(await columns('parquet_made'), await columns('before'));
    assert.equal(Number(await one('SELECT count(*) FROM parquet_made')), SAMPLE_TRADES);
    assert.equal(Number(await one(`SELECT count(*) FROM ((SELECT * FROM parquet_made EXCEPT ALL SELECT * FROM before)
      UNION ALL (SELECT * FROM before EXCEPT ALL SELECT * FROM parquet_made))`)), 0);
    // and the generator's own rows, as its Arrow import puts them in: the same table
    await arrowMade('arrow_twin');
    assert.deepEqual(await columns('parquet_made'), await columns('arrow_twin'));
    assert.equal(Number(await one(`SELECT count(*) FROM ((SELECT * FROM parquet_made EXCEPT ALL SELECT * FROM arrow_twin)
      UNION ALL (SELECT * FROM arrow_twin EXCEPT ALL SELECT * FROM parquet_made))`)), 0);
  });

  it('a file that is not the one named is refused before a row of it is read', async () => {
    const bytes = new Uint8Array(readFileSync(runfileFromEnv('SAMPLE_TRADES_PARQUET')));
    await files.registerFileBuffer('other.parquet', bytes.slice());
    await assert.rejects(() => loadFileInto(engine, { at: 'other.parquet', name: 'other.parquet', format: 'parquet',
      digest: '0'.repeat(64), size: bytes.length }, 'never_made', {
      // the planner names the file by what it was told; this store holds another under that name
      fileLoad: async (request) => plannerFor('', 'local::RT').fileLoad({ ...request, file: { ...request.file, digest: 'f'.repeat(64) } }),
    }), /is not the one this read holds/);
    assert.equal(Number(await one("SELECT count(*) FROM duckdb_tables() WHERE table_name = 'never_made'")), 0);
  });
});

describe("the start's grid written down as the sample file of its rows (the user, 2026-10-23: the start page could not be saved)", () => {
  it('the file is the same rows, every column read as the same type: what a page saved over the start reopens', async () => {
    await arrowMade('arrow_made');
    // read in as the tab reads a sample it rebuilds (sources.ts fileFor, then upload.ts ingestFile)
    const read = await ingestFile(engine, files, new File([sampleTradesCsv()], DESK_TRADES_FILE.name), plannerFor('', 'local::RT'));
    assert.deepEqual(await columns(read.table), await columns('arrow_made'));
    assert.equal(Number(await one(`SELECT count(*) FROM ((SELECT * FROM "${read.table}" EXCEPT ALL SELECT * FROM arrow_made)
      UNION ALL (SELECT * FROM arrow_made EXCEPT ALL SELECT * FROM "${read.table}"))`)), 0);
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
