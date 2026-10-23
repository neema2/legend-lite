// THE RISK DEMO'S SOURCE (docs/DEEPHAVEN_PLAN_2026_10_22.md D0, DH23): what a cube page over the demo's Deephaven reads
// -- the model and its runtime (model.ts), the planner and the engine, the source every grid reads (each position with
// its book, instrument, live price and fx rate, valued in dollars) and its columns as a cube shows them. Shared by the
// cube page (cube.ts) and the page of several grids (board.ts).

import type { CubeSource, QuerySource } from '../../src/cube-document.ts';
import { LitePlanner } from '../../src/lite-planner.ts';
import type { ColumnSpec, CubeSnapshot } from '../../src/snapshot.ts';
import type { ValueSpecification } from '../../../pure-protocol/src/index.ts';
import { DeephavenEngine } from '../../../engine-client/src/deephaven-engine.ts';
import { MODEL } from './model.ts';

export const RUNTIME = 'risk::LiveRuntime';
/** The grids' connection to the server, and the store it binds: each base's table is bound to the same (B1). */
export const CONNECTION = 'risk::Live';
export const STORES = ['risk::Store'];
const JOIN = 'meta::pure::functions::relation::JoinKind.LEFT';
/** The cube's source: every position, with its book, instrument, live price and fx rate, valued in dollars. */
export const SOURCE = `#>{risk::Store.position}#`
  + `->join(#>{risk::Store.book}#, ${JOIN}, {p, b | $p.book_id == $b.bid})`
  + `->join(#>{risk::Store.instrument}#, ${JOIN}, {p, i | $p.inst_id == $i.iid})`
  + `->join(#>{risk::Store.price}#, ${JOIN}, {p, q | $p.inst_id == $q.px_iid})`
  + `->join(#>{risk::Store.fx}#, ${JOIN}, {p, f | $p.ccy == $f.fx_ccy})`
  + `->extend(~[mv:x|$x.qty * $x.px * $x.rate])`;

/** The columns the cube shows, and how each aggregates. */
const SHOWN: Readonly<Record<string, ColumnSpec['aggregate'] | null>> = {
  desk: null, sector: null, region: null, ccy: null, inst_id: null, pid: 'count', qty: 'sum', px: 'average', mv: 'sum',
};

/**
 * THE SOURCE AS A SAVED PAGE WRITES IT DOWN (docs/DEEPHAVEN_PLAN_2026_10_22.md B2): the query it is -- its text and
 * the runtime it runs on, a saved query of the demo's own project -- and its columns, never its rows: they are the
 * server's. A page opens a grid over it again where its text is the source's (`isRiskSource`).
 */
export function riskSource(columns: readonly ColumnSpec[]): QuerySource {
  return {
    _type: 'savedQuery',
    name: 'Risk positions',
    query: {
      name: 'Risk positions', groupId: 'org.finos.legend.lite.demo', artifactId: 'risk', versionId: 'local',
      // the source reads its stores straight: a runtime, no mapping
      executionContext: { _type: 'explicitExecutionContext', mapping: '', runtime: RUNTIME },
      content: SOURCE,
    },
    columns: columns.map((c) => ({ name: c.name, type: c.type })),
  };
}

/** Whether a saved cube's source is the risk source: its query's text. */
export function isRiskSource(source: CubeSource): boolean {
  return source._type === 'savedQuery' && source.query.content === SOURCE;
}

/** The demo's server as the page reaches it: its address and its (test) key, from the page's config.json. */
export interface RiskConfig {
  readonly url: string;
  readonly psk: string;
}

/** What a page over the risk source starts from: its model, planner and engine, and a cube of the source. */
export interface Risk {
  readonly model: string;
  readonly planner: LitePlanner;
  readonly engine: DeephavenEngine;
  /** The source's query and its columns as a cube shows them, and the source as a saved page writes it down. */
  readonly query: ValueSpecification;
  readonly columns: readonly ColumnSpec[];
  readonly source: QuerySource;
  /** The source, grouped by `rows` and pivoted on `pivotOn` (none: flat). */
  cube(rows: readonly string[], pivotOn?: readonly string[]): CubeSnapshot;
}

/** Plans in this tab on the risk runtime, signs in to the server, and types the source's columns. */
export async function openRisk(config: RiskConfig): Promise<Risk> {
  const at = new URL(config.url);
  const model = MODEL(at.hostname, Number(at.port));
  const planner = new LitePlanner({ model, runtime: RUNTIME, assetBaseUrl: './' });
  await planner.warmUp();
  const engine = await DeephavenEngine.open({
    url: config.url,
    signIn: { type: 'io.deephaven.authentication.psk.PskAuthenticationHandler', token: config.psk },
  }, (op, args) => planner.deephaven(op, args));
  const source = await planner.parse(SOURCE);
  const query = source.body[0];
  if (query === undefined) throw new Error('the risk source parsed to no expression');
  const types = new Map((await planner.relationType(source)).map((c) => [c.name, c.type]));
  const columns: ColumnSpec[] = Object.entries(SHOWN).map(([name, aggregate]) => {
    const type = types.get(name);
    if (type === undefined) throw new Error(`the risk source has no column '${name}'`);
    return aggregate ? { name, type, aggregate } : { name, type };
  });
  return {
    model,
    planner,
    engine,
    query,
    columns,
    source: riskSource(columns),
    cube: (rows, pivotOn = []) => ({
      source: { query },
      columns,
      derived: [],
      rows: [...rows],
      pivotOn: [...pivotOn],
      sorts: [],
      epoch: 1,
    }),
  };
}
