// DuckDB in a Node test (node-require.ts startNodeDuckDb): its extensions are the pinned ones every site serves the
// tab, and no other -- one that is not pinned is refused, never fetched from extensions.duckdb.org. DuckDB's own fetch
// waited forever where the network is closed, which is how eight tests hung in a cloud container (2026-10-17).

import assert from 'node:assert/strict';
import { before, describe, it } from 'node:test';

import { startNodeDuckDb } from '../src/node-require.ts';
import { runfilesFromEnv } from '../../tools/js/runfiles.mts';

interface Connection {
  query(sql: string): { toArray(): Array<Record<string, unknown>> };
  prepare(sql: string): { query(...params: unknown[]): { toArray(): Array<Record<string, unknown>> }; close(): void };
}

let conn: Connection;

before(async () => {
  conn = (await startNodeDuckDb(runfilesFromEnv('DUCKDB_EXTENSIONS'))).connect() as Connection;
});

describe('DuckDB in a Node test', () => {
  it('loads a pinned extension from its file: ICU, which a session time zone needs', () => {
    conn.query('LOAD icu');
    conn.query("SET TimeZone = 'America/New_York'");
    const [row] = conn.query("SELECT current_setting('TimeZone') AS zone").toArray();
    assert.equal(String(row!['zone']), 'America/New_York');
  });

  it('refuses an extension that is not pinned instead of fetching it', () => {
    assert.throws(() => conn.query('LOAD spatial'), /spatial/);
  });

  it('answers no other request, and an extension that is not pinned with a 404 -- its own contract, asked directly', () => {
    // asked of the stand-in itself: DuckDB refuses an http:// path before it makes any request, so no query reaches this
    interface Request { open(method: string, url: string): void; send(): void; readonly status: number }
    const Stand = (globalThis as { XMLHttpRequest?: new () => Request }).XMLHttpRequest;
    assert.ok(Stand, 'startNodeDuckDb defines one');
    const other = new Stand();
    other.open('GET', 'http://127.0.0.1:9/rows.csv');
    assert.throws(() => other.send(), /DuckDB in Node makes no request: http:\/\/127\.0\.0\.1:9\/rows\.csv/);
    const unpinned = new Stand();
    unpinned.open('GET', 'https://extensions.duckdb.org/v1.5.4/wasm_eh/spatial.duckdb_extension.wasm');
    unpinned.send();
    assert.equal(unpinned.status, 404);
  });

  it('makes an S3 secret from bound values, its statement holding none (B3: credentials by name, measured on DuckDB-WASM)', () => {
    // the platform's statement (DuckDbHostOperations' secret: every field, then the scope, a ?), the values a sentinel
    conn.query('LOAD httpfs');
    const made = conn.prepare('CREATE OR REPLACE TEMPORARY SECRET "datacube_s3_t" (TYPE S3, KEY_ID ?, SECRET ?, SESSION_TOKEN ?,'
      + ' REGION ?, ENDPOINT ?, URL_STYLE ?, USE_SSL ?, SCOPE ?)');
    try {
      made.query('SENTINEL-key', 'SENTINEL-secret', 'SENTINEL-token', 'eu-west-1', 'minio:9000', 'path', false, 's3://bucket/sales/');
    } finally {
      made.close();
    }
    // DuckDB's own listing redacts the secret and the session token (an access key's id it shows, as AWS does)
    const [row] = conn.query("SELECT name, type, persistent, CAST(scope AS VARCHAR) AS scope,"
      + " secret_string LIKE '%SENTINEL-secret%' OR secret_string LIKE '%SENTINEL-token%' AS written FROM duckdb_secrets()").toArray();
    assert.deepEqual({ ...row }, { name: 'datacube_s3_t', type: 's3', persistent: false, scope: "['s3://bucket/sales/']", written: false });
    conn.query('DROP TEMPORARY SECRET IF EXISTS "datacube_s3_t"');
    conn.query('DROP TEMPORARY SECRET IF EXISTS "datacube_s3_t"');
    assert.equal(Number(conn.query('SELECT count(*) AS n FROM duckdb_secrets()').toArray()[0]!['n']), 0);
  });

  it('is given the pinned files, each laid out as DuckDB asks for it', async () => {
    await assert.rejects(startNodeDuckDb([]), /no pinned DuckDB extensions given/);
    await assert.rejects(startNodeDuckDb(['/vendor/icu.wasm']), /is not laid out <version>\/<build>\/<name>/);
  });
});
