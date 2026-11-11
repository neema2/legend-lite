// Pointing a cube at object storage.
//
// These test the STATEMENTS, because that is the part with decisions
// in it: which extension is loaded, how a URL is escaped, whether a
// credential can escape into an error message. The page names the
// operations (remote.ts) and legend-lite's planner writes them (D5). Whether the bytes
// actually arrive is proved in the browser, in verify-remote.mjs -- the
// node build of duckdb-wasm does not make real HTTP requests (it
// answers a synthetic 404), so a node test claiming to have fetched
// something would be testing nothing.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { SecretStore, secretBinds, type HostOperation } from '../../engine-client/src/host-operations.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import { plannerFor } from './catalog-builder.ts';
import {
  credentialOperations,
  credentialScope,
  credentialSecret,
  secretStore,
  inferFormat,
  mountOperations,
  mountRemote,
  redactSecrets,
  viewOperation,
  type RemoteOptions,
  type RemoteSource,
  type S3Credentials,
} from '../src/remote.ts';
import { FakeEngine } from './fake-engine.ts';

/** legend-lite's planner, which writes each operation's statements (D5): the module in-thread, or Python's library in
 *  the headless lane -- the statements under test are the platform's, as every host runs them. */
const planner = plannerFor('', '');
const statements = async (operations: readonly HostOperation[]): Promise<string[]> =>
  (await planner.hostOperations('DuckDB', operations)).flat();
const viewStatement = async (source: RemoteSource): Promise<string> => (await statements([viewOperation(source)]))[0]!;
/** A view's scan: what it reads its rows from. */
const scanExpression = async (source: RemoteSource): Promise<string> =>
  (await viewStatement(source)).replace(/^.* AS SELECT \* FROM /, '');
const credentialStatements = (creds: S3Credentials | undefined, source: { readonly name: string; readonly url: string }): Promise<string[]> =>
  statements(credentialOperations(creds, source));
const mountStatements = (options: RemoteOptions): Promise<string[]> => statements(mountOperations(options));

const empty = (epoch: number): ResultTable => ({
  columns: [],
  rowCount: 0,
  epoch,
  elapsedMs: 0,
});

class RecordingEngine extends FakeEngine {
  readonly name = 'recording';
  readonly sql: string[] = [];
  /** What each statement bound, by its place in `sql`. */
  readonly params: (readonly unknown[])[] = [];
  failOn?: RegExp;
  async answer(sql: string, epoch: number): Promise<ResultTable> {
    this.sql.push(sql);
    if (this.failOn?.test(sql)) throw new Error(`boom running ${sql}`);
    return empty(epoch);
  }
  /** The connection held for a mount (DuckDbEngine.exclusive): each statement recorded with what it binds. */
  exclusive<T>(work: (held: { rows(sql: string, params: readonly (string | number | boolean | null)[]): Promise<unknown[][]> }) => Promise<T>): Promise<T> {
    return work({
      rows: async (sql, params) => {
        this.params[this.sql.length] = params;
        await this.answer(sql, 0);
        return [];
      },
    });
  }
}

describe('choosing a format', () => {
  it('reads the extension when there is one', () => {
    assert.equal(inferFormat('https://x/y.parquet'), 'parquet');
    assert.equal(inferFormat('https://x/y.csv'), 'csv');
    assert.equal(inferFormat('https://x/y.csv.gz'), 'csv');
  });

  it('ignores a query string when guessing', () => {
    // A presigned URL carries its signature as a query string, and it
    // is the common case for object storage.
    assert.equal(
      inferFormat('https://b.s3.amazonaws.com/t.csv?X-Amz-Signature=abc'),
      'csv',
    );
  });

  it('recognises an Iceberg table by shape, not extension', () => {
    // An Iceberg table is a DIRECTORY, so there is no extension to
    // read.
    assert.equal(inferFormat('s3://bucket/table/metadata'), 'iceberg');
    assert.equal(inferFormat('s3://bucket/table#iceberg'), 'iceberg');
  });

  it('defaults to parquet', () => {
    assert.equal(inferFormat('s3://bucket/table'), 'parquet');
  });
});

describe('the scan expression', () => {
  it('uses the right reader per format', async () => {
    assert.match(
      await scanExpression({ name: 't', url: 'https://x/y.parquet' }),
      /^read_parquet\('https:\/\/x\/y\.parquet'\)$/,
    );
    assert.match(
      await scanExpression({ name: 't', url: 'https://x/y.csv' }),
      /^read_csv_auto\(/,
    );
    assert.match(
      await scanExpression({ name: 't', url: 's3://b/t', format: 'iceberg' }),
      /^iceberg_scan\('s3:\/\/b\/t'\)$/,
    );
  });

  it('strips the iceberg marker from the URL it passes on', async () => {
    assert.equal(
      await scanExpression({ name: 't', url: 's3://b/t#iceberg' }),
      "iceberg_scan('s3://b/t')",
    );
  });

  it('escapes a quote in the URL by doubling it', async () => {
    // SQL doubles the quote; it does NOT use a backslash. The same
    // class of bug as the Pure escaping already fixed here.
    assert.match(
      await scanExpression({ name: 't', url: "s3://b/it's.parquet" }),
      /'s3:\/\/b\/it''s\.parquet'/,
    );
  });
});

describe('the view that hides the remoteness', () => {
  it('takes the name the model already uses', async () => {
    // The whole design: the planner keeps emitting SELECT ... FROM
    // TRADES and never learns the data is somewhere else.
    assert.equal(
      await viewStatement({ name: 'TRADES', url: 'https://x/y.parquet' }),
      'CREATE OR REPLACE VIEW "TRADES" AS SELECT * FROM'
      + " read_parquet('https://x/y.parquet')",
    );
  });

  it('quotes a name that needs it', async () => {
    const s = await viewStatement({ name: 'with space', url: 'https://x/y.parquet' });
    assert.match(s, /VIEW "with space"/);
  });

  it('replaces rather than failing on a remount', async () => {
    // Pointing at yesterday's file and back is a normal thing to do.
    assert.match(
      await viewStatement({ name: 'T', url: 'https://x/y.parquet' }),
      /CREATE OR REPLACE VIEW/,
    );
  });
});

describe('credentials', () => {
  const source = { name: 'trades', url: 's3://bucket/trades/2024.parquet' };

  it('writes nothing when there are none', async () => {
    assert.deepEqual(await credentialStatements(undefined, source), []);
    assert.deepEqual(await credentialStatements({}, source), []);
  });

  it('writes one secret per source, scoped to its own path, rather than global settings: by name, every value bound', async () => {
    const creds = { accessKeyId: 'AKIA', secretAccessKey: 'shh', region: 'eu-west-2' };
    const [stmt] = await credentialStatements(creds, source);
    // credentials by name (B3, 2026-11-11): the statement holds no value; each field's, then the scope, is bound
    assert.equal(stmt, 'CREATE OR REPLACE TEMPORARY SECRET "datacube_s3_trades" (TYPE S3, KEY_ID ?, SECRET ?, REGION ?, SCOPE ?)');
    const secret = credentialSecret(creds, source)!;
    assert.deepEqual(secret.operation, { op: 'secret', name: 'datacube_s3_trades', scope: 's3://bucket/trades/2024.parquet', fields: ['KEY_ID', 'SECRET', 'REGION'] });
    assert.deepEqual(secretBinds(secret.operation), ['KEY_ID', 'SECRET', 'REGION', 'SCOPE']);
    assert.deepEqual(secretStore({ sources: [source], s3: creds }).bound(secret.operation),
      ['AKIA', 'shh', 'eu-west-2', 's3://bucket/trades/2024.parquet']);
  });

  it('carries the settings an S3-compatible endpoint needs, bound', async () => {
    const creds = { endpoint: 'minio:9000', urlStyle: 'path' as const, useSsl: false };
    const [stmt] = await credentialStatements(creds, source);
    assert.match(stmt ?? '', /ENDPOINT \?, URL_STYLE \?, USE_SSL \?, SCOPE \?\)$/);
    assert.deepEqual(secretStore({ sources: [source], s3: creds }).bound(credentialSecret(creds, source)!.operation),
      ['minio:9000', 'path', false, 's3://bucket/trades/2024.parquet']);
  });

  it('writes no credential into a statement, a quote in one included: each is bound as it is', async () => {
    const creds = { secretAccessKey: "a'b" };
    const [stmt] = await credentialStatements(creds, source);
    assert.ok(!(stmt ?? '').includes("a'b") && !(stmt ?? '').includes("a''b"), stmt);
    assert.deepEqual(secretStore({ sources: [source], s3: creds }).bound(credentialSecret(creds, source)!.operation),
      ["a'b", 's3://bucket/trades/2024.parquet']);
  });

  it('refuses a secret the host does not hold, or a field it holds no value for, by name', () => {
    const op = { op: 'secret', name: 'datacube_s3_x', scope: 's3://b/', fields: ['KEY_ID'] } as const;
    assert.throws(() => new SecretStore().bound(op), /the secret 'datacube_s3_x' is not one this host holds/);
    const store = new SecretStore();
    store.hold('datacube_s3_x', new Map([['SECRET', 'v']]));
    assert.throws(() => store.bound(op), /the secret 'datacube_s3_x' holds no KEY_ID/);
  });

  it('scopes an Iceberg table to its table, and a glob to the path before it', () => {
    assert.equal(credentialScope('s3://b/warehouse/orders/metadata#iceberg'), 's3://b/warehouse/orders');
    assert.equal(credentialScope('s3://b/trades/*.parquet'), 's3://b/trades/');
    assert.equal(credentialScope('https://host/x.csv'), 'https://host/x.csv');
  });

  it('gives two sources two secrets: one page\'s keys never replace another\'s', async () => {
    const secrets = (await mountStatements({
      sources: [{ name: 'a', url: 's3://one/a.parquet' }, { name: 'b', url: 's3://two/b.parquet' }],
      s3: { accessKeyId: 'AKIA', secretAccessKey: 'shh' },
    })).filter((s) => s.startsWith('CREATE OR REPLACE TEMPORARY SECRET'));
    assert.equal(secrets.length, 2);
    assert.match(secrets[0]!, /"datacube_s3_a"/);
    assert.match(secrets[1]!, /"datacube_s3_b"/);
    assert.deepEqual(credentialOperations({ accessKeyId: 'AKIA' }, { name: 'a', url: 's3://one/a.parquet' })
      .map((o) => (o as { readonly scope: string }).scope), ['s3://one/a.parquet']);
    assert.deepEqual(credentialOperations({ accessKeyId: 'AKIA' }, { name: 'b', url: 's3://two/b.parquet' })
      .map((o) => (o as { readonly scope: string }).scope), ['s3://two/b.parquet']);
  });
});

describe('keeping secrets out of messages', () => {
  const creds = {
    accessKeyId: 'AKIAEXAMPLE',
    secretAccessKey: 'super-secret-value',
    sessionToken: 'tok-123',
  };

  it('redacts every credential by value', () => {
    const text = 'failed: KEY_ID AKIAEXAMPLE SECRET super-secret-value'
      + ' SESSION_TOKEN tok-123';
    const out = redactSecrets(text, creds);
    assert.ok(!out.includes('super-secret-value'), out);
    assert.ok(!out.includes('AKIAEXAMPLE'), out);
    assert.ok(!out.includes('tok-123'), out);
    assert.match(out, /\*\*\*/);
  });

  it('leaves text alone when there are no credentials', () => {
    assert.equal(redactSecrets('plain', undefined), 'plain');
  });

  it('does not leak a secret through a failed mount', async () => {
    // QueryError carries the SQL that failed, so without this a live
    // access key reaches an error banner and whatever collects it.
    const engine = new RecordingEngine();
    engine.failOn = /SECRET/;
    let message = '';
    try {
      await mountRemote(engine, planner, {
        sources: [{ name: 'T', url: 'https://x/y.parquet' }],
        s3: creds,
      });
    } catch (e) {
      message = e instanceof Error ? e.message : String(e);
    }
    assert.ok(message.length > 0, 'it did fail');
    assert.ok(!message.includes('super-secret-value'), message);
    assert.ok(!message.includes('AKIAEXAMPLE'), message);
  });

  it('sends no credential in any statement\'s text: a sentinel is bound, never written (the B3 guard)', async () => {
    const sentinel = `SENTINEL-${'x'.repeat(8)}`;
    const engine = new RecordingEngine();
    await mountRemote(engine, planner, {
      sources: [{ name: 'A', url: 's3://one/a.parquet' }, { name: 'B', url: 'https://two/b.csv' }],
      s3: { accessKeyId: `${sentinel}-key`, secretAccessKey: `${sentinel}-secret`, sessionToken: `${sentinel}-token`,
        region: 'eu-west-1', endpoint: 'minio:9000', urlStyle: 'path', useSsl: true },
    });
    assert.ok(engine.sql.length > 0);
    for (const sql of engine.sql) assert.ok(!sql.includes('SENTINEL'), `a credential in a statement's text: ${sql}`);
    // each secret's values arrive as its bound parameters, in its fields' order then its scope
    const secrets = engine.sql.flatMap((sql, i) => (sql.startsWith('CREATE OR REPLACE TEMPORARY SECRET') ? [engine.params[i]] : []));
    assert.deepEqual(secrets, [
      [`${sentinel}-key`, `${sentinel}-secret`, `${sentinel}-token`, 'eu-west-1', 'minio:9000', 'path', true, 's3://one/a.parquet'],
      [`${sentinel}-key`, `${sentinel}-secret`, `${sentinel}-token`, 'eu-west-1', 'minio:9000', 'path', true, 'https://two/b.csv'],
    ]);
  });
});

describe('the mount sequence', () => {
  it('loads httpfs before anything references a URL', async () => {
    const stmts = await mountStatements({
      sources: [{ name: 'T', url: 'https://x/y.parquet' }],
    });
    assert.equal(stmts[0], 'INSTALL httpfs');
    assert.equal(stmts[1], 'LOAD httpfs');
    assert.match(stmts[stmts.length - 1] ?? '', /CREATE OR REPLACE VIEW/);
  });

  it('loads iceberg only when an iceberg source is present', async () => {
    const without = await mountStatements({
      sources: [{ name: 'T', url: 'https://x/y.parquet' }],
    });
    assert.ok(!without.some((s) => /iceberg/.test(s)), without.join('; '));

    const with_ = await mountStatements({
      sources: [{ name: 'T', url: 's3://b/t', format: 'iceberg' }],
    });
    assert.ok(with_.includes('LOAD iceberg'));
  });

  it('creates the secret before the views that need it', async () => {
    const stmts = await mountStatements({
      sources: [{ name: 'T', url: 's3://b/y.parquet' }],
      s3: { accessKeyId: 'a', secretAccessKey: 'b' },
    });
    const secret = stmts.findIndex((s) => s.startsWith('CREATE OR REPLACE TEMPORARY SECRET'));
    const view = stmts.findIndex((s) => s.includes('VIEW'));
    assert.ok(secret >= 0 && view > secret, stmts.join('; '));
  });

  it('mounts several sources in one pass', async () => {
    const engine = new RecordingEngine();
    await mountRemote(engine, planner, {
      sources: [
        { name: 'TRADES', url: 'https://x/trades.parquet' },
        { name: 'BOOKS', url: 'https://x/books.csv' },
      ],
    });
    assert.ok(engine.sql.some((s) => /VIEW "TRADES"/.test(s)));
    assert.ok(engine.sql.some((s) => /VIEW "BOOKS"/.test(s)));
    assert.ok(engine.sql.some((s) => /read_csv_auto/.test(s)));
  });
});
