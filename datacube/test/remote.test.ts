// Pointing a cube at object storage.
//
// These test the STATEMENTS and what they bind, because that is the part with decisions in it: which extension is
// loaded, how a URL is escaped, whether a credential or a bearer URL's token can escape into a statement, a plan or an
// error message. A remote file's mount is a PLANNED LOAD (B3's Q3, 2026-11-11, the user's ruling: "Yes do option 2"):
// legend-lite's planner writes it from names and fields only (remoteMount), and the hosts' runner binds what the host
// holds. Whether the bytes actually arrive is proved in the browser, in verify-remote.mjs -- the node build of
// duckdb-wasm does not make real HTTP requests (it answers a synthetic 404), so a node test claiming to have fetched
// something would be testing nothing.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { SecretStore, type RemoteMountRequest, type RemoteMountSource } from '../../engine-client/src/host-operations.ts';
import { heldDataHost, type DataHost, type HeldDatabase } from '../src/data-host.ts';
import { ConnectionSettings } from '../../engine-client/src/plan-runner.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import { plannerFor } from './catalog-builder.ts';
import {
  bearerName,
  carriesUserinfo,
  inferFormat,
  isBearer,
  mountRemote,
  mountRequest,
  redactSecrets,
  secretName,
  shownAddress,
  type RemoteOptions,
  type RemoteSource,
  type S3Credentials,
} from '../src/remote.ts';
import { FakeEngine } from './fake-engine.ts';

/** legend-lite's planner, which plans each mount (remoteMount): the module in-thread, or Python's library in the
 *  headless lane -- the plans under test are the platform's, as every host runs them. */
const planner = plannerFor('', '');

/** The tab's host over `engine` (data-host.ts heldDataHost: the #6 bridge), its mounts planned by `mounts` -- the
 *  planner's, or one that records what it is asked. */
const hostOf = (engine: HeldDatabase, mounts: RemoteMountSource = planner): DataHost => heldDataHost(engine, {
  remoteMount: (r) => mounts.remoteMount(r),
  fileLoad: (r) => planner.fileLoad(r),
  hostOperations: (d, o) => planner.hostOperations(d, o),
  catalogColumnsSql: (schema, table) => planner.catalogColumnsSql(schema, table),
});

interface Step { readonly _type: string; readonly sql: string; readonly reach?: string; readonly binds?: readonly unknown[] }
/** A source's planned mount: its load's steps, in order. */
const planned = async (source: RemoteSource, creds?: S3Credentials, view = true): Promise<Step[]> =>
  (JSON.parse(await planner.remoteMount(mountRequest('DuckDB', source, creds, view))) as { root: { steps: Step[] } }).root.steps;
const sqlOf = async (source: RemoteSource, creds?: S3Credentials): Promise<string[]> =>
  (await planned(source, creds)).map((s) => s.sql);
const viewStatement = async (source: RemoteSource): Promise<string> => (await sqlOf(source)).at(-1)!;
/** A view's scan: what it reads its rows from. */
const scanExpression = async (source: RemoteSource): Promise<string> =>
  (await viewStatement(source)).replace(/^.* AS SELECT \* FROM /, '');
const mountStatements = async (options: RemoteOptions): Promise<string[]> =>
  (await Promise.all(options.sources.map((s) => sqlOf(s, options.s3)))).flat();

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
  /** The settings a mount keeps for the connections this engine's host opens (the tab's). */
  readonly settings = new ConnectionSettings();
  failOn?: RegExp;
  /** What a failure says: DuckDB's own words repeat the URL a read was given (measured, B3's Q3). */
  failWith?: string;
  async answer(sql: string, epoch: number): Promise<ResultTable> {
    this.sql.push(sql);
    if (this.failOn?.test(sql)) throw new Error(this.failWith ?? `boom running ${sql}`);
    return empty(epoch);
  }
  /** The connection held for a statement that binds (DuckDbEngine.exclusive): each recorded with what it binds. */
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

/** The planner, every request it is asked and every plan it answers recorded: what reached it, and what it wrote. */
const recordingPlanner = (): { readonly asked: RemoteMountRequest[]; readonly plans: string[]; remoteMount(r: RemoteMountRequest): Promise<string> } => {
  const asked: RemoteMountRequest[] = [];
  const plans: string[] = [];
  return {
    asked, plans,
    async remoteMount(request) {
      asked.push(request);
      const plan = await planner.remoteMount(request);
      plans.push(plan);
      return plan;
    },
  };
};

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
  const secretStep = async (creds: S3Credentials): Promise<Step | undefined> =>
    (await planned(source, creds)).find((s) => s.sql.startsWith('CREATE OR REPLACE TEMPORARY SECRET'));
  const boundOf = async (creds: S3Credentials): Promise<readonly unknown[]> => {
    const engine = new RecordingEngine();
    await mountRemote(hostOf(engine), { sources: [source], s3: creds });
    return engine.params[engine.sql.findIndex((s) => s.startsWith('CREATE OR REPLACE TEMPORARY SECRET'))]!;
  };

  it('plans no secret when there are none', async () => {
    assert.equal(await secretStep({}), undefined);
    assert.ok(!(await sqlOf(source)).some((s) => /SECRET/.test(s)));
  });

  it('plans one secret per source, scoped to its own path: by name, every field held, the scope the plan\'s own', async () => {
    const creds = { accessKeyId: 'AKIA', secretAccessKey: 'shh', region: 'eu-west-2' };
    const step = (await secretStep(creds))!;
    assert.equal(step.sql, 'CREATE OR REPLACE TEMPORARY SECRET "datacube_s3_trades" (TYPE S3, KEY_ID ?, SECRET ?, REGION ?, SCOPE ?)');
    assert.equal(step.reach, 'DATABASE');
    assert.deepEqual(step.binds, [
      { _type: 'held', name: 'datacube_s3_trades', field: 'KEY_ID' },
      { _type: 'held', name: 'datacube_s3_trades', field: 'SECRET' },
      { _type: 'held', name: 'datacube_s3_trades', field: 'REGION' },
      { _type: 'value', text: 's3://bucket/trades/2024.parquet' },
    ]);
    assert.deepEqual(await boundOf(creds), ['AKIA', 'shh', 'eu-west-2', 's3://bucket/trades/2024.parquet']);
  });

  it('carries the settings an S3-compatible endpoint needs, bound', async () => {
    const creds = { endpoint: 'minio:9000', urlStyle: 'path' as const, useSsl: false };
    assert.match((await secretStep(creds))!.sql, /ENDPOINT \?, URL_STYLE \?, USE_SSL \?, SCOPE \?\)$/);
    assert.deepEqual(await boundOf(creds), ['minio:9000', 'path', false, 's3://bucket/trades/2024.parquet']);
  });

  it('writes no credential into a statement, a quote in one included: each is bound as it is', async () => {
    const creds = { secretAccessKey: "a'b" };
    for (const sql of await sqlOf(source, creds)) assert.ok(!sql.includes("a'b") && !sql.includes("a''b"), sql);
    assert.deepEqual(await boundOf(creds), ["a'b", 's3://bucket/trades/2024.parquet']);
  });

  it('refuses a name the host does not hold, or a field it holds no value for, by name', () => {
    assert.throws(() => new SecretStore().value('datacube_s3_x', 'KEY_ID'), /'datacube_s3_x' is not a secret this host holds/);
    const store = new SecretStore();
    store.hold('datacube_s3_x', new Map([['SECRET', 'v']]));
    assert.throws(() => store.value('datacube_s3_x', 'KEY_ID'), /the secret 'datacube_s3_x' holds no KEY_ID/);
  });

  it('scopes an Iceberg table to its table, and a glob to the path before it', async () => {
    const scope = async (url: string, format?: 'iceberg'): Promise<unknown> =>
      (await planned({ name: 't', url, ...(format ? { format } : {}) }, { accessKeyId: 'AKIA' }))
        .find((s) => s.sql.includes('SECRET'))!.binds!.at(-1);
    assert.deepEqual(await scope('s3://b/warehouse/orders/metadata#iceberg', 'iceberg'), { _type: 'value', text: 's3://b/warehouse/orders' });
    assert.deepEqual(await scope('s3://b/trades/*.parquet'), { _type: 'value', text: 's3://b/trades/' });
    assert.deepEqual(await scope('https://host/x.csv'), { _type: 'value', text: 'https://host/x.csv' });
  });

  it('gives two sources two secrets: one page\'s keys never replace another\'s', async () => {
    const secrets = (await mountStatements({
      sources: [{ name: 'a', url: 's3://one/a.parquet' }, { name: 'b', url: 's3://two/b.parquet' }],
      s3: { accessKeyId: 'AKIA', secretAccessKey: 'shh' },
    })).filter((s) => s.startsWith('CREATE OR REPLACE TEMPORARY SECRET'));
    assert.equal(secrets.length, 2);
    assert.match(secrets[0]!, /"datacube_s3_a"/);
    assert.match(secrets[1]!, /"datacube_s3_b"/);
  });

  it('names a source\'s store entries by a word, an identifier as it is', () => {
    assert.equal(secretName('remote_keyed'), 'datacube_s3_remote_keyed');
    assert.equal(secretName('with space'), 'datacube_s3_with_20_space');
    assert.equal(bearerName('a-b'), 'datacube_url_a_2d_b');
  });
});

describe('a bearer URL: a presigned link, its query string its token (B3\'s Q3)', () => {
  const token = 'X-Amz-Signature=SENTINEL-token-0123';
  const url = `https://b.s3.amazonaws.com/trades.parquet?X-Amz-Expires=300&${token}`;
  const source = { name: 'trades', url };

  it('is named, never written: the planner is asked its name and its address without the query string', () => {
    assert.ok(isBearer(url) && !isBearer('s3://b/t.parquet'));
    assert.equal(shownAddress(url), 'https://b.s3.amazonaws.com/trades.parquet');
    const request = mountRequest('DuckDB', source, undefined, true);
    assert.deepEqual(request.location, { bearer: 'datacube_url_trades', shown: 'https://b.s3.amazonaws.com/trades.parquet' });
    assert.ok(!JSON.stringify(request).includes('SENTINEL'), JSON.stringify(request));
  });

  it('is bound to the connection\'s variable, which the view reads: no statement and no plan holds the token', async () => {
    const engine = new RecordingEngine();
    const asked = recordingPlanner();
    await mountRemote(hostOf(engine, asked), { sources: [source] });
    for (const plan of asked.plans) assert.ok(!plan.includes('SENTINEL') && !plan.includes('amazonaws.com/trades.parquet?'), plan);
    for (const sql of engine.sql) assert.ok(!sql.includes('SENTINEL'), `a token in a statement's text: ${sql}`);
    const set = engine.sql.findIndex((s) => s.startsWith('SET VARIABLE '));
    assert.ok(set >= 0, engine.sql.join('; '));
    assert.deepEqual(engine.params[set], [url], 'the bearer URL bound, as the host holds it');
    assert.match(engine.sql.at(-1)!, /^CREATE OR REPLACE VIEW "trades" AS SELECT \* FROM read_parquet\(getvariable\('lite_url_[0-9a-f]+'\)\)$/);
    // the connection's setting, by name: kept by the host for the connections it opens (data-host.ts: the host's to keep)
    const replayed: (readonly unknown[])[] = [];
    await engine.settings.replay(async (sql, params) => {
      assert.ok(sql.startsWith('SET VARIABLE '), sql);
      replayed.push(params);
    });
    assert.deepEqual(replayed, [[url]], 'replayed on a new connection, bound again');
  });

  it('is refused, never asked of the planner, when its address names a user or a password (the tenets audit\'s LOW 5)', async () => {
    let asked = 0;
    const counting = { remoteMount: async (r: RemoteMountRequest) => { asked += 1; return planner.remoteMount(r); } };
    for (const named of ['https://sam:SENTINEL-pw@b.example/trades.parquet', `https://sam:SENTINEL-pw@b.example/trades.parquet?${token}`]) {
      await assert.rejects(() => mountRemote(hostOf(new RecordingEngine(), counting), { sources: [{ name: 'trades', url: named }] }),
        (e: Error) => /names a user or a password/.test(e.message) && !e.message.includes('SENTINEL'));
    }
    assert.equal(asked, 0);
    // an '@' in the path is no user
    assert.equal(carriesUserinfo('https://b.example/a@b.parquet'), false);
  });

  it('is refused for an Iceberg table, a folder that has no presigned link', async () => {
    await assert.rejects(() => mountRemote(hostOf(new RecordingEngine()), { sources: [{ name: 't', url: `s3://b/t?${token}`, format: 'iceberg' }] }),
      (e: Error) => /a folder/.test(e.message) && !e.message.includes('SENTINEL'));
  });

  it('is redacted from a failed mount\'s error, which repeats the URL it read', async () => {
    const engine = new RecordingEngine();
    engine.failOn = /VIEW/;
    engine.failWith = `HTTP Error: HTTP GET error on '${url}' (HTTP 403)`;
    await assert.rejects(() => mountRemote(hostOf(engine), { sources: [source] }),
      (e: Error) => /could not mount remote data/.test(e.message) && !e.message.includes('SENTINEL'));
    assert.ok(!redactSecrets(`on '${url}'`, undefined, [url]).includes('SENTINEL'));
    // its query string alone, as a message may quote it
    assert.equal(redactSecrets(`signed by ${url.slice(url.indexOf('?') + 1)}`, undefined, [url]), 'signed by ***');
    // and one parameter's value alone, raw or decoded (the tenets audit's M2, 2026-11-11)
    for (const [, value] of new URL(url).searchParams) {
      if (value.length >= 8) assert.ok(!redactSecrets(`expired: ${value}`, undefined, [url]).includes(value), value);
    }
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
    // A database's error may carry what a statement bound, so without this a live
    // access key reaches an error banner and whatever collects it.
    const engine = new RecordingEngine();
    engine.failOn = /SECRET/;
    engine.failWith = 'boom: KEY_ID AKIAEXAMPLE SECRET super-secret-value';
    let message = '';
    try {
      await mountRemote(hostOf(engine), {
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

  it('sends no credential and no token in any statement\'s text or plan: a sentinel is bound, never written (the B3 guard)', async () => {
    const sentinel = `SENTINEL-${'x'.repeat(8)}`;
    const engine = new RecordingEngine();
    const asked = recordingPlanner();
    await mountRemote(hostOf(engine, asked), {
      sources: [{ name: 'A', url: 's3://one/a.parquet' }, { name: 'B', url: 'https://two/b.csv' },
        { name: 'C', url: `https://three/c.parquet?sig=${sentinel}-url` }],
      s3: { accessKeyId: `${sentinel}-key`, secretAccessKey: `${sentinel}-secret`, sessionToken: `${sentinel}-token`,
        region: 'eu-west-1', endpoint: 'minio:9000', urlStyle: 'path', useSsl: true },
    });
    assert.ok(engine.sql.length > 0);
    for (const sql of engine.sql) assert.ok(!sql.includes('SENTINEL'), `a credential in a statement's text: ${sql}`);
    for (const request of asked.asked) assert.ok(!JSON.stringify(request).includes('SENTINEL'), JSON.stringify(request));
    for (const plan of asked.plans) assert.ok(!plan.includes('SENTINEL'), `a credential in a plan: ${plan}`);
    // each secret's values arrive as its bound parameters, in its fields' order then its scope
    const secrets = engine.sql.flatMap((sql, i) => (sql.startsWith('CREATE OR REPLACE TEMPORARY SECRET') ? [engine.params[i]] : []));
    assert.deepEqual(secrets, [
      [`${sentinel}-key`, `${sentinel}-secret`, `${sentinel}-token`, 'eu-west-1', 'minio:9000', 'path', true, 's3://one/a.parquet'],
      [`${sentinel}-key`, `${sentinel}-secret`, `${sentinel}-token`, 'eu-west-1', 'minio:9000', 'path', true, 'https://two/b.csv'],
      [`${sentinel}-key`, `${sentinel}-secret`, `${sentinel}-token`, 'eu-west-1', 'minio:9000', 'path', true, 'https://three/c.parquet'],
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
    await mountRemote(hostOf(engine), {
      sources: [
        { name: 'TRADES', url: 'https://x/trades.parquet' },
        { name: 'BOOKS', url: 'https://x/books.csv' },
      ],
    });
    assert.ok(engine.sql.some((s) => /VIEW "TRADES"/.test(s)));
    assert.ok(engine.sql.some((s) => /VIEW "BOOKS"/.test(s)));
    assert.ok(engine.sql.some((s) => /read_csv_auto/.test(s)));
  });

  it('plans its access alone, no view, where a host checks a table\'s files first', async () => {
    const steps = await planned({ name: 'T', url: 's3://b/t' }, { accessKeyId: 'a' }, false);
    assert.ok(!steps.some((s) => /VIEW/.test(s.sql)), steps.map((s) => s.sql).join('; '));
  });
});
