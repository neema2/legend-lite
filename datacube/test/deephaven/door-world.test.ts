// THE DESK'S STREAM TABLES ON THE PINNED DEEPHAVEN, MADE BY THE PLATFORM (docs/DESK_DATA_ROADMAP_2026_10_29.md B1's
// and B2's done-when): the market world behind the door (the feed: the desk model's topics, the world writing every table a
// stream reads), and the pinned Deephaven running the startup program the planner generated from the model for the
// desk's Deephaven runtime -- each landing table a Kafka subscription to its topic on the brokers the runtime binds its
// store to -- with a check beside it (test/deephaven/door/check.groovy): each table its declared columns, each of the
// type the store declares, and every row once: an appended table's rows each read, none twice; a keyed table's keys.

import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { copyFileSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, describe, it } from 'node:test';

import { runfileFromEnv } from '../../../tools/js/runfiles.mts';
import { DESK_MODEL, ON_DEEPHAVEN } from '../../demo/deephaven/market/model.ts';
import { startFeed } from '../../demo/market-sim/door/feed.ts';
import { plannerFor } from '../catalog-builder.ts';

const SECONDS = Number(process.env['DOOR_SECONDS'] ?? 15);
const BROKERS = "bootstrapServers: '127.0.0.1:19092';";

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

describe('the desk\'s stream tables on the pinned Deephaven, made by the generated startup program', () => {
  let stop: (() => Promise<void>) | null = null;
  after(async () => { await stop?.(); });

  it('each table typed as its store declares it, every row once', { timeout: 300_000 }, async () => {
    const corpus = readFileSync(runfileFromEnv('TRADING_MODEL'), 'utf8');
    const desk = readFileSync(runfileFromEnv('MARKET_PURE'), 'utf8');
    const environments = readFileSync(runfileFromEnv('ENVIRONMENTS_PURE'), 'utf8');
    assert.ok(environments.includes(BROKERS), 'the desk\'s environments name the topics\' brokers');
    const model = DESK_MODEL(corpus, desk, environments);
    const planner = plannerFor(model, ON_DEEPHAVEN.runtime);
    // a small world, no crypto feed (nothing asked of the network), a short morning: under every retention
    const feed = await startFeed({ model, planner, settings: { names: 30, feed: 'off', morning: 600, morningPrints: 2000 } });
    stop = async () => { await feed.close(); };
    const { port, topics, counts } = feed;

    // the program for the desk's Deephaven runtime, its brokers this test's door (a test's copy of the environments)
    const atDoor = DESK_MODEL(corpus, desk, environments.replaceAll(BROKERS, `bootstrapServers: '127.0.0.1:${port}';`));
    const answer = await plannerFor(atDoor, ON_DEEPHAVEN.runtime).deephaven('program', { model: atDoor, runtime: ON_DEEPHAVEN.runtime });
    assert.ok(answer.startsWith('OK\n'), answer.slice(0, 400));
    const { program } = JSON.parse(answer.slice(3)) as { readonly program: string };
    assert.equal(program.match(/ = KafkaTools\.consumeToTable\(/g)?.length, topics.length, 'a subscription a stream table');
    const dir = shared('door-app');
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, 'program.groovy'), program);
    copyFileSync(runfileFromEnv('DOOR_CHECK'), join(dir, 'check.groovy'));
    writeFileSync(join(dir, 'desk.app'), ['type=script', 'scriptType=groovy', 'enabled=true', 'id=legend.desk.program',
      'name=the desk\'s startup program, generated (B2), and a test\'s check', 'file_0=program.groovy', 'file_1=check.groovy',
      ''].join('\n'));
    // the tables the check reads: each one's id (its key, else its first column, every desk dataset's own key) and its
    // columns with the types the store declares
    const tablesFile = shared('door-tables.tsv');
    writeFileSync(tablesFile, topics.map((t) => [t.table, (t.key.length > 0 ? t.key : [t.columns[0]!.name]).join(','),
      t.columns.map((c) => `${c.name}:${c.type}`).join(',')].join('\t')).join('\n') + '\n');
    const doneFile = shared('door-done.tsv');
    const outFile = shared('door-out.tsv');
    const deephaven = await startServer(join(dir, 'desk.app'), { DOOR_TABLES: tablesFile, DOOR_DONE: doneFile, DOOR_OUT: outFile });
    stop = async () => {
      await deephaven.stop();
      await feed.close();
    };

    await new Promise((r) => setTimeout(r, SECONDS * 1000));
    await feed.stopWorld();
    writeFileSync(doneFile, topics.map((t) => [t.table, counts.records[t.topic] ?? 0,
      t.key.length > 0 ? counts.keys[t.topic] ?? 0 : ''].join('\t')).join('\n') + '\n');

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
    for (const t of topics) {
      const written = counts.records[t.topic] ?? 0;
      const got = read.get(t.table);
      report.push(`${t.table} (${t.topic}): wrote ${written}${t.key.length > 0 ? ` (${counts.keys[t.topic] ?? 0} keys)` : ''},`
        + ` read ${got?.rows} (${got?.distinct} distinct), ${got?.typed}`);
      assert.ok(written > 0, `the world wrote ${t.topic}`);
      assert.ok(got !== undefined, `the program made ${t.table}`);
      assert.equal(got.typed, 'typed', `${t.table}: each column as its store declares it`);
      if (t.key.length > 0) {
        assert.equal(got.distinct, counts.keys[t.topic], `${t.table}: every key read`);
        assert.equal(got.rows, got.distinct, `${t.table}: one row a key`);
      } else {
        assert.equal(got.rows, written, `${t.table}: every row read`);
        assert.equal(got.distinct, written, `${t.table}: none twice`);
      }
    }
    console.log(report.join('\n'));
  });
});
