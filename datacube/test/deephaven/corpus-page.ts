// THE STRESS CORPUS ON DEEPHAVEN, SENT (//datacube:deephaven_corpus_test; docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md,
// task 15): each corpus test's plan -- planned by //core:stress_deephaven on a Deephaven runtime -- sent to the pinned
// Deephaven holding the corpus's tables (test/deephaven/corpus/corpus.groovy), as DataCube's engine sends a plan (its
// session, the planner's own requests), and its rows read back as the engine reads them (an Arrow stream, each column
// by the plan's place and type). Each answer, or the server's refusal, is posted to the driver (corpus.mjs), which hands
// them to the judge. Nothing is judged here: the stress suites' own judge does that, in Java.

import { DeephavenSession, type DeephavenServer } from '../../../engine-client/src/deephaven.ts';
import { byPosition, deephavenRows } from '../../../engine-client/src/deephaven-rows.ts';
import { relationColumns, type Plan } from '../../../engine-client/src/relation-type.ts';
// @ts-ignore the module's runtime, served beside the page from //wasm:planner
import { load } from './wasm-gc-module-runtime.js';

interface Case {
  readonly id: string;
  readonly sql?: string;
  readonly type?: unknown;
  /** the model's enumerations among its columns' types: each read as its names' text */
  readonly enumerations?: readonly string[];
}

const verdict: { sent: number; answered: number; refused: number; setupError?: string; done: boolean } =
  { sent: 0, answered: 0, refused: 0, done: false };
(window as unknown as { __corpus: typeof verdict }).__corpus = verdict;

/** How many plans are in flight at once, and how long one may take. */
const LANES = 4;
const PLAN_MS = 60_000;

const withTimeout = <T>(p: Promise<T>, ms: number, what: string): Promise<T> => Promise.race([p,
  new Promise<T>((_, refuse) => setTimeout(() => refuse(new Error(`${what}: no answer in ${ms / 1000} s`)), ms))]);

try {
  const config = await (await fetch('config.json')).json() as { url: string; psk: string };
  const server: DeephavenServer = {
    url: config.url,
    signIn: { type: 'io.deephaven.authentication.psk.PskAuthenticationHandler', token: config.psk },
  };
  const cases = (await (await fetch('cases.json')).json() as Case[]).filter((c) => c.sql !== undefined);
  const teavm = await load('classes.wasm', { stackDeobfuscator: { enabled: false } });
  const exports = teavm.exports as { deephavenOrError(op: string, args: string): string };
  const session = await DeephavenSession.open(server, async (op, args) => exports.deephavenOrError(op, JSON.stringify(args)));

  let pending: unknown[] = [];
  const flush = async (): Promise<void> => {
    if (pending.length === 0) return;
    const lines = pending.map((r) => JSON.stringify(r)).join('\n');
    pending = [];
    await fetch('results', { method: 'POST', body: `${lines}\n` });
  };
  /** One case: its plan kept alive, its rows read, let go -- its rows as columns, or what refused them. */
  const run = async (c: Case): Promise<unknown> => {
    let release: (() => void) | undefined;
    try {
      // a column of a type the tab reads no value of is refused here, as the tab refuses it: the case's answer
      const plan: Plan = { sql: c.sql!, columns: relationColumns(c.type as never, undefined, new Set(c.enumerations ?? [])) };
      const rows = await withTimeout((async () => {
        const kept = await session.keepAlive(plan.sql);
        release = () => kept.release();
        return byPosition(deephavenRows(await session.rows(kept.exportId), 0, 0), plan);
      })(), PLAN_MS, c.id);
      verdict.answered++;
      return {
        id: c.id, rowCount: rows.rowCount,
        columns: rows.columns.map((col) => ({ name: col.name, type: col.type,
          values: col.values.map((v) => (typeof v === 'bigint' ? v.toString() : v)) })),
      };
    } catch (e) {
      verdict.refused++;
      return { id: c.id, error: String((e as Error)?.message ?? e).slice(0, 600) };
    } finally {
      release?.();
    }
  };
  let next = 0;
  const lane = async (): Promise<void> => {
    while (next < cases.length) {
      const c = cases[next++]!;
      verdict.sent++;
      // the answer first, then the list it joins: the list is swapped out by a flush while it is awaited
      const answer = await run(c);
      pending.push(answer);
      if (pending.length >= 50) await flush();
    }
  };
  await Promise.all(Array.from({ length: LANES }, lane));
  await flush();
  await session.close();
} catch (e) {
  verdict.setupError = String((e as Error)?.stack ?? e);
}
verdict.done = true;
