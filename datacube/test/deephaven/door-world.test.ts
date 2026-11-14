// THE WORLD AS A FEED, BEHIND THE DOOR, READ BY THE PINNED DEEPHAVEN (docs/DESK_DATA_ROADMAP_2026_10_29.md B1's
// done-when): the desk model's topics as the planner answers them, the door holding them, the market world writing
// every table a stream reads through it for a few seconds, and the pinned Deephaven consuming each topic from its
// beginning with its own Kafka reader (test/deephaven/door/door.groovy, the probe) -- every row once: an appended
// topic's rows each read, none twice; a compacted topic's keys each read.

import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, describe, it } from 'node:test';

import { runfileFromEnv } from '../../../tools/js/runfiles.mts';
import { DESK_MODEL, ON_DEEPHAVEN } from '../../demo/deephaven/market/model.ts';
import { startFeed } from '../../demo/market-sim/door/feed.ts';
import { plannerFor } from '../catalog-builder.ts';

const SECONDS = Number(process.env['DOOR_SECONDS'] ?? 15);

/** The pinned Deephaven with the probe as its application (DOOR_FIXTURE), `env` beside the test's own: up once the
 *  fixture says its address (its first JSON line); `stop` ends it. */
async function startProbe(env: Record<string, string>): Promise<{ stop(): Promise<void> }> {
  const server = spawn(runfileFromEnv('DOOR_FIXTURE'), [], { stdio: ['pipe', 'pipe', 'pipe'], env: { ...process.env, ...env } });
  server.stderr.on('data', (b: Buffer) => {
    for (const line of String(b).split('\n')) if (line && !/ INFO | DEBUG /.test(line)) process.stderr.write(`[deephaven] ${line}\n`);
  });
  let said = '';
  const up = new Promise<void>((resolve, reject) => {
    server.stdout.on('data', (b: Buffer) => {
      said += String(b);
      if (said.split('\n').some((l) => l.startsWith('{'))) resolve();
    });
    server.on('exit', (code) => { reject(new Error(`the Deephaven fixture exited (${code}) before it was up`)); });
  });
  await up;
  return {
    async stop() {
      server.stdin.end();
      await new Promise((r) => setTimeout(r, 2000));
      if (server.exitCode === null) server.kill('SIGKILL');
    },
  };
}

/** A file the test and the probe share, in the test's own directory. */
const shared = (name: string): string => join(process.env['TEST_TMPDIR'] ?? tmpdir(), name);

describe('the pinned Deephaven reads every table the world writes through the door', () => {
  let stop: (() => Promise<void>) | null = null;
  after(async () => { await stop?.(); });

  it('every row once', { timeout: 300_000 }, async () => {
    const model = DESK_MODEL(readFileSync(runfileFromEnv('TRADING_MODEL'), 'utf8'),
      readFileSync(runfileFromEnv('MARKET_PURE'), 'utf8'), readFileSync(runfileFromEnv('ENVIRONMENTS_PURE'), 'utf8'));
    const planner = plannerFor(model, ON_DEEPHAVEN.runtime);
    // a small world, no crypto feed (nothing asked of the network), a short morning: under every retention
    const feed = await startFeed({ model, planner, settings: { names: 30, feed: 'off', morning: 600, morningPrints: 2000 } });
    stop = async () => { await feed.close(); };
    const { port, topics, counts } = feed;
    // the topics the probe reads: each one's id (its key, else its first column, every desk dataset's own key) and its
    // columns with their Deephaven types
    const topicsFile = shared('door-topics.tsv');
    writeFileSync(topicsFile, topics.map((t) => [t.topic, (t.key.length > 0 ? t.key : [t.columns[0]!.name]).join(','),
      t.columns.map((c) => `${c.name}:${c.type}`).join(',')].join('\t')).join('\n') + '\n');
    const doneFile = shared('door-done.tsv');
    const outFile = shared('door-out.tsv');
    const deephaven = await startProbe({ DOOR_PORT: String(port), DOOR_TOPICS: topicsFile, DOOR_DONE: doneFile, DOOR_OUT: outFile });
    stop = async () => {
      await deephaven.stop();
      await feed.close();
    };

    await new Promise((r) => setTimeout(r, SECONDS * 1000));
    await feed.stopWorld();
    writeFileSync(doneFile, topics.map((t) => [t.topic, counts.records[t.topic] ?? 0,
      t.key.length > 0 ? counts.keys[t.topic] ?? 0 : ''].join('\t')).join('\n') + '\n');

    let out = '';
    for (let i = 0; i < 1200 && !out.includes('done\n'); i++) {
      await new Promise((r) => setTimeout(r, 100));
      try { out = readFileSync(outFile, 'utf8'); } catch { /* not yet written */ }
    }
    assert.ok(out.includes('done\n'), `the probe finished: ${out}`);
    assert.ok(!out.includes('failed:'), out);
    const read = new Map(out.split('\n').filter((l) => l.split('\t').length === 3)
      .map((l) => { const [t, rows, distinct] = l.split('\t'); return [t!, { rows: Number(rows), distinct: Number(distinct) }]; }));
    const report: string[] = [];
    for (const t of topics) {
      const written = counts.records[t.topic] ?? 0;
      const got = read.get(t.topic);
      report.push(`${t.topic}: wrote ${written}${t.key.length > 0 ? ` (${counts.keys[t.topic] ?? 0} keys)` : ''}, read ${got?.rows} (${got?.distinct} distinct)`);
      assert.ok(written > 0, `the world wrote ${t.topic}`);
      assert.ok(got !== undefined, `the probe read ${t.topic}`);
      if (t.key.length > 0) {
        assert.equal(got.distinct, counts.keys[t.topic], `${t.topic}: every key read`);
      } else {
        assert.equal(got.rows, written, `${t.topic}: every row read`);
        assert.equal(got.distinct, written, `${t.topic}: none twice`);
      }
    }
    console.log(report.join('\n'));
  });
});
