// THE DESK'S STREAM TABLES ON THE PINNED DEEPHAVEN, MADE BY THE PLATFORM (docs/DESK_DATA_ROADMAP_2026_10_29.md B1's
// and B2's done-when; docs/DEPLOYMENT_DESIGN_2026_11_14.md step 1): the market world behind the door (the feed: the desk
// model's topics, the world writing every table a stream reads), and the pinned Deephaven running lite's fixed loader
// (core/src/main/deephaven/legend-loader.groovy) over the plan the planner rendered for the desk's deployment -- data:
// each landing table its topic on the brokers the deployment's source names, its columns, what it keeps -- with a
// check beside it (test/deephaven/door/check.groovy): each table its declared columns, each of the type the store
// declares, and every row once: an appended table's rows each read, none twice; a keyed table's keys.

import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, describe, it } from 'node:test';

import { runfileFromEnv } from '../../../tools/js/runfiles.mts';
import { installDeephaven, planOf } from '../../demo/deephaven/install.ts';
import { DESK_MODEL, DESK_ON_DEEPHAVEN, ON_DEEPHAVEN } from '../../demo/deephaven/market/model.ts';
import { writeDeliveries } from '../../demo/market-sim/door/deliveries.ts';
import { startFeed } from '../../demo/market-sim/door/feed.ts';
import { plannerFor } from '../catalog-builder.ts';

const SECONDS = Number(process.env['DOOR_SECONDS'] ?? 15);
const BROKERS = "bootstrapServers: '127.0.0.1:19092';";
const DROP = "location: 'file:///tmp/legend-desk/positions/';";

/** A file the test and the server share, in the test's own directory. */
const shared = (name: string): string => join(process.env['TEST_TMPDIR'] ?? tmpdir(), name);

/** The pinned Deephaven with `app` as its application (DOOR_FIXTURE), `env` beside the test's own: up once the fixture
 *  says its address (its first JSON line); `stop` ends it. */
async function startServer(app: string, env: Record<string, string>): Promise<{ stop(): Promise<void> }> {
  const server = spawn(runfileFromEnv('DOOR_FIXTURE'), [`--jvm_flag=-Dembedded.deephaven.application=${app}`],
    { stdio: ['pipe', 'pipe', 'pipe'], env: { ...process.env, ...env } });
  server.stderr.on('data', (b: Buffer) => {
    for (const line of String(b).split('\n')) if (line && !/ INFO | DEBUG /.test(line)) process.stderr.write(`[deephaven] ${line}\n`);
  });
  let said = '';
  await new Promise<void>((resolve, reject) => {
    server.stdout.on('data', (b: Buffer) => {
      said += String(b);
      if (said.split('\n').some((l) => l.startsWith('{'))) resolve();
    });
    server.on('exit', (code) => { reject(new Error(`the Deephaven fixture exited (${code}) before it was up`)); });
  });
  return {
    async stop() {
      server.stdin.end();
      await new Promise((r) => setTimeout(r, 2000));
      if (server.exitCode === null) server.kill('SIGKILL');
    },
  };
}

describe('the desk\'s stream tables on the pinned Deephaven, made by the loader from the deployment\'s plan', () => {
  let stop: (() => Promise<void>) | null = null;
  after(async () => { await stop?.(); });

  it('each table typed as its store declares it, every row once', { timeout: 300_000 }, async () => {
    const corpus = readFileSync(runfileFromEnv('TRADING_MODEL'), 'utf8');
    const desk = readFileSync(runfileFromEnv('MARKET_PURE'), 'utf8');
    const environments = readFileSync(runfileFromEnv('ENVIRONMENTS_PURE'), 'utf8');
    assert.ok(environments.includes(BROKERS), 'the desk\'s environments name the topics\' brokers');
    assert.ok(environments.includes(DROP), 'the desk\'s environments name the positions\' drop location');
    const model = DESK_MODEL(corpus, desk, environments);
    const planner = plannerFor(model, ON_DEEPHAVEN.runtime);
    // a small world, no crypto feed (nothing asked of the network), a short morning: under every retention
    const feed = await startFeed({ model, planner, settings: { names: 30, feed: 'off', morning: 600, morningPrints: 2000 } });
    stop = async () => { await feed.close(); };
    const { port, topics, counts, kept } = feed;

    // the deployment's plan for the desk's Deephaven server, its brokers this test's door and its drop location this
    // test's directory (a test's copy of the environments): data, never code -- the loader beside it is lite's own
    const atDoor = DESK_MODEL(corpus, desk, environments.replaceAll(BROKERS, `bootstrapServers: '127.0.0.1:${port}';`)
      .replaceAll(DROP, `location: 'file://${shared('drop')}/';`));
    const plan = planOf(await plannerFor(atDoor, ON_DEEPHAVEN.runtime).deephaven('plan', { model: atDoor, deployment: DESK_ON_DEEPHAVEN }));
    const files = plan.tables.filter((t) => t.source.kind === 'file');
    assert.deepEqual(plan.tables.filter((t) => t.source.kind === 'topic').map((t) => t.name).sort(),
      topics.map((t) => t.table).sort(), 'a table a stream');
    assert.deepEqual(files.map((t) => t.name), ['POSITION', 'BOOK'], 'a table a delivered file');
    // position keeping drops its files before the server starts: the start of the day, whole
    const dropped = writeDeliveries(files, kept);
    const installed = installDeephaven(shared('door-app'), plan, runfileFromEnv('DOOR_LOADER'), [runfileFromEnv('DOOR_CHECK')]);
    // what each table should hold: a topic's records written and keys, a file's rows and keys
    const landed = [
      ...topics.map((t) => ({ table: t.table, from: t.topic, key: t.key, columns: t.columns,
        written: () => counts.records[t.topic] ?? 0, keys: () => counts.keys[t.topic] ?? 0 })),
      ...files.map((t) => {
        const key = t.keep.key ?? [];
        const rows = kept.get(t.name) ?? [];
        return { table: t.name, from: 'a delivered file', key, columns: t.columns, written: () => dropped[t.name] ?? 0,
          keys: () => new Set(rows.map((r) => JSON.stringify(key.map((k) => r[k] ?? null)))).size };
      }),
    ];
    // the tables the check reads: each one's id (its key, else its first column, every desk dataset's own key) and its
    // columns with the types the store declares -- a keyed (batch milestoned) table's record columns, then the three
    // its landing adds (docs/DEPLOYMENT_DESIGN_2026_11_14.md §5: LAKE_DIGEST a string, the two batch ids longs)
    const MILESTONES = ['LAKE_DIGEST:java.lang.String', 'LAKE_IN_ID:long', 'LAKE_OUT_ID:long'];
    const tablesFile = shared('door-tables.tsv');
    writeFileSync(tablesFile, landed.map((t) => [t.table, (t.key.length > 0 ? t.key : [t.columns[0]!.name]).join(','),
      [...t.columns.map((c) => `${c.name}:${c.type}`), ...(t.key.length > 0 ? MILESTONES : [])].join(',')].join('\t'))
      .join('\n') + '\n');
    const doneFile = shared('door-done.tsv');
    const outFile = shared('door-out.tsv');
    const deephaven = await startServer(installed.app,
      { ...installed.env, DOOR_TABLES: tablesFile, DOOR_DONE: doneFile, DOOR_OUT: outFile });
    stop = async () => {
      await deephaven.stop();
      await feed.close();
    };

    await new Promise((r) => setTimeout(r, SECONDS * 1000));
    await feed.stopWorld();
    writeFileSync(doneFile, landed.map((t) => [t.table, t.written(), t.key.length > 0 ? t.keys() : ''].join('\t'))
      .join('\n') + '\n');

    let out = '';
    for (let i = 0; i < 1200 && !out.includes('done\n'); i++) {
      await new Promise((r) => setTimeout(r, 100));
      try { out = readFileSync(outFile, 'utf8'); } catch { /* not yet written */ }
    }
    assert.ok(out.includes('done\n'), `the check finished: ${out}`);
    assert.ok(!out.includes('failed:'), out);
    const read = new Map(out.split('\n').filter((l) => l.split('\t').length === 4).map((l) => {
      const [t, rows, distinct, typed] = l.split('\t');
      return [t!, { rows: Number(rows), distinct: Number(distinct), typed: typed! }];
    }));
    const report: string[] = [];
    for (const t of landed) {
      const written = t.written();
      const got = read.get(t.table);
      report.push(`${t.table} (${t.from}): wrote ${written}${t.key.length > 0 ? ` (${t.keys()} keys)` : ''},`
        + ` read ${got?.rows} (${got?.distinct} distinct), ${got?.typed}`);
      assert.ok(written > 0, `the world wrote ${t.from}`);
      assert.ok(got !== undefined, `the loader made ${t.table}`);
      assert.equal(got.typed, 'typed', `${t.table}: each column as its store declares it`);
      if (t.key.length > 0) {
        assert.equal(got.distinct, t.keys(), `${t.table}: every key read`);
        assert.equal(got.rows, got.distinct, `${t.table}: one row a key`);
      } else {
        assert.equal(got.rows, written, `${t.table}: every row read`);
        assert.equal(got.distinct, written, `${t.table}: none twice`);
      }
    }
    console.log(report.join('\n'));
  });
});
