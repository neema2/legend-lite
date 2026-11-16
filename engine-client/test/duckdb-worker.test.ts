// DuckDB's worker for a tab prints no link's query string (2026-11-16, the TypeScript hunt's T-G): DuckDB-WASM's runtime
// prints a remote file's URL as it is on paths a presigned link takes as a matter of course, so the worker the door
// starts (engine-client/vendor/duckdb-redacting.worker.js) hands every console call its text with each URL's query
// string left out, then runs DuckDB's own worker. Here its script runs with a stand-in console and importScripts, the
// "library" logging what DuckDB-WASM's runtime logs.

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import { runInNewContext } from 'node:vm';

const WORKER = readFileSync(new URL('../vendor/duckdb-redacting.worker.js', import.meta.url), 'utf8');

/** A presigned link, its signature a sentinel. */
const PRESIGNED = 'https://bucket.s3.example.invalid/trades.parquet?X-Amz-Signature=SENTINEL-SIGNATURE&X-Amz-Expires=600';

describe("a tab DuckDB's worker", () => {
  it('prints a link with its query string left out, then runs DuckDB\'s own worker', () => {
    const printed: unknown[][] = [];
    const loaded: string[] = [];
    const record = (level: string) => (...args: unknown[]): void => { printed.push([level, ...args]); };
    const console = { log: record('log'), info: record('info'), warn: record('warn'), error: record('error'), debug: record('debug'), trace: record('trace') };
    const context: Record<string, unknown> = { console, Error };
    // the library's worker, run where the script asks for it: it logs as DuckDB-WASM's runtime does
    context['importScripts'] = (url: string): void => {
      loaded.push(url);
      runInNewContext(`
        console.log('HEAD request failed: ${PRESIGNED}, with full http reads are disabled');
        console.warn('Range request for ${PRESIGNED} did not return a partial response: 200 "OK"');
        console.error('FAIL WITH: IO Error: Cannot open file "${PRESIGNED}"');
        console.log(new Error('HTTP GET error on ${PRESIGNED} (HTTP 403)'));
        console.log('a statement with no link', 42);
      `, context);
    };
    runInNewContext(WORKER, context);
    assert.deepEqual(loaded, ['./duckdb-browser-eh.worker.js'], "DuckDB's own worker runs after");
    const text = JSON.stringify(printed.map((call) => call.map((v) => (v instanceof Error ? `${v.name}: ${v.message}` : v))));
    assert.ok(!text.includes('SENTINEL'), text);
    assert.deepEqual(printed.map((call) => call.map((v) => (v instanceof Error ? `${v.name}: ${v.message}` : v))), [
      ['log', 'HEAD request failed: https://bucket.s3.example.invalid/trades.parquet?[query string not shown], with full http reads are disabled'],
      ['warn', 'Range request for https://bucket.s3.example.invalid/trades.parquet?[query string not shown] did not return a partial response: 200 "OK"'],
      ['error', 'FAIL WITH: IO Error: Cannot open file "https://bucket.s3.example.invalid/trades.parquet?[query string not shown]"'],
      ['log', 'Error: HTTP GET error on https://bucket.s3.example.invalid/trades.parquet?[query string not shown] (HTTP 403)'],
      ['log', 'a statement with no link', 42],
    ]);
  });
});
