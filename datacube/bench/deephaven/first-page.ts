// THE FIRST-ROWS BENCHMARK'S PAGE (README.md; first-bench.mjs drives it): how a result the grid has not shown before
// reaches the page best -- read whole by Arrow Flight's DoGet, as DeephavenEngine.execute reads it, or by publishing it
// and subscribing with Deephaven's JS library, as DeephavenEngine.follow reads it after. Each trial plans two new
// queries over the risk demo's source (demo/deephaven/), one for each way, alternating which goes first, and times each
// step; the planning is done before the clock starts (the cube plans in a worker, the same for either way). A query is
// new by a literal no other trial has, placed so the source's tables the session keeps alive are read (`warm`: a
// regroup's or a filter's case) or so every join is made again (`cold`: the first query over a source).

import { DeephavenSession } from '../../../engine-client/src/deephaven.ts';
import { LiveClient, type Followed, type FollowedRows } from '../../../engine-client/src/deephaven-live.ts';
import { byPosition, deephavenRows, followedTable } from '../../../engine-client/src/deephaven-rows.ts';
import type { Plan } from '../../../engine-client/src/relation-type.ts';
import { LitePlanner } from '../../src/lite-planner.ts';
import { MODEL } from '../../demo/deephaven/model.ts';

const JOIN = 'meta::pure::functions::relation::JoinKind.LEFT';
/** The cube's source (demo/deephaven/cube.ts), its positions filtered first by `positions` (cold's literal). */
const source = (positions: string): string => `#>{risk::Store.position}#${positions}`
  + `->join(#>{risk::Store.book}#, ${JOIN}, {p, b | $p.book_id == $b.bid})`
  + `->join(#>{risk::Store.instrument}#, ${JOIN}, {p, i | $p.inst_id == $i.iid})`
  + `->join(#>{risk::Store.price}#, ${JOIN}, {p, q | $p.inst_id == $q.px_iid})`
  + `->join(#>{risk::Store.fx}#, ${JOIN}, {p, f | $p.ccy == $f.fx_ccy})`
  + `->extend(~[mv:x|$x.qty * $x.px * $x.rate])`;
/** Every position's id is below 400: a filter on a larger literal keeps every row, and makes a table no other has. */
const keepAll = (n: number): string => `->filter(x|$x.pid < ${1000 + n})`;

/** The results timed: a grid level (four desks) and a flat page of every position. */
const SHAPES: Readonly<Record<string, string>> = {
  level: '->groupBy(~[desk], ~[n:x|$x.pid:y|$y->count(), mv:x|$x.mv:y|$y->sum()])->sort(~desk->ascending())',
  flat: '->select(~[pid, desk, sector, mv])->sort(~pid->ascending())->limit(500)',
};

/** One way's steps, ms from its start; `rows`, how many it read. */
interface Timed {
  readonly how: 'doGet' | 'subscribe';
  readonly shape: string;
  readonly source: 'warm' | 'cold';
  readonly made: number;
  readonly sent: number;
  readonly steps: Readonly<Record<string, number>>;
  readonly total: number;
  readonly rows: number;
}

async function main(): Promise<void> {
  const config = await (await fetch('config.json')).json() as { url: string; psk: string };
  const at = new URL(config.url);
  const server = {
    url: config.url,
    signIn: { type: 'io.deephaven.authentication.psk.PskAuthenticationHandler', token: config.psk },
  };
  const planner = new LitePlanner({ model: MODEL(at.hostname, Number(at.port)), runtime: 'risk::LiveRuntime',
    assetBaseUrl: './' });
  await planner.warmUp();
  const session = await DeephavenSession.open(server, (op, args) => planner.deephaven(op, args));
  const live = await LiveClient.open(server, () => { /* the run fails on its own */ });
  // the source made and kept alive once, as the cube's first view makes it
  (await session.keepAlive((await planner.planText(source('') + SHAPES.level)).sql)).release();
  let literal = 0;

  const doGet = async (plan: Plan): Promise<Omit<Timed, 'shape' | 'source'>> => {
    const t0 = performance.now();
    const kept = await session.keepAlive(plan.sql);
    const t1 = performance.now();
    try {
      const stream = await session.rows(kept.exportId);
      const t2 = performance.now();
      const table = byPosition(deephavenRows(stream, 1, t2 - t0), plan);
      const t3 = performance.now();
      return { how: 'doGet', made: t1 - t0, sent: kept.sent, steps: { make: t1 - t0, read: t2 - t1, type: t3 - t2 },
        total: t3 - t0, rows: table.rowCount };
    } finally {
      kept.release();
    }
  };
  const subscribe = async (plan: Plan): Promise<Omit<Timed, 'shape' | 'source'>> => {
    const t0 = performance.now();
    const kept = await session.keepAlive(plan.sql);
    const t1 = performance.now();
    let followed: Followed | null = null;
    try {
      const name = await session.publishKept(kept.exportId);
      const t2 = performance.now();
      let first!: (rows: FollowedRows) => void;
      let failed!: (error: Error) => void;
      const landed = new Promise<FollowedRows>((resolve, refuse) => { first = resolve; failed = refuse; });
      followed = await live.follow(name, (rows) => first(rows), (e) => failed(e));
      const t3 = performance.now();
      const rows = await Promise.race([landed, new Promise<never>((_, refuse) => {
        setTimeout(() => refuse(new Error('no first rows in 10 s')), 10_000);
      })]);
      const t4 = performance.now();
      const table = followedTable(rows, plan, 1, 0);
      const t5 = performance.now();
      return { how: 'subscribe', made: t1 - t0, sent: kept.sent,
        steps: { make: t1 - t0, publish: t2 - t1, open: t3 - t2, first: t4 - t3, type: t5 - t4 },
        total: t5 - t0, rows: table.rowCount };
    } finally {
      followed?.close();
      kept.release();
    }
  };

  (window as unknown as { bench: unknown }).bench = {
    ready: true,
    async run(trials: number): Promise<Timed[]> {
      const out: Timed[] = [];
      for (let t = 0; t < trials; t++) {
        for (const [shape, top] of Object.entries(SHAPES)) {
          for (const where of ['warm', 'cold'] as const) {
            const plans: Plan[] = [];
            for (let i = 0; i < 2; i++) {
              const n = literal++;
              const q = where === 'warm' ? source('') + keepAll(n) + top : source(keepAll(n)) + top;
              plans.push(await planner.planText(q));
            }
            const [a, b] = plans as [Plan, Plan];
            const ways = t % 2 === 0
              ? [() => doGet(a), () => subscribe(b)]
              : [() => subscribe(a), () => doGet(b)];
            for (const way of ways) out.push({ ...(await way()), shape, source: where });
          }
        }
      }
      return out;
    },
  };
}

main().catch((e: unknown) => {
  (window as unknown as { bench: unknown }).bench = { ready: true, error: String(e instanceof Error ? e.stack : e) };
});
