// The results grid is a DataCube: a CubeApp over the query the person built, as upstream Legend
// Query hands its result to DataCube. The query is the cube's SOURCE (a relation); the cube adds
// its own paging, sorting, grouping, pivots and formatting on top, planned by the same legend-lite
// planner. Where the rows come from is the plane's (`AppContext.cubeRows`): the tab's SQL engine
// with the tab's planner, or a legend-lite server's own plan and run (the app's engine, LiteServer:
// the edge program's H7, 2026-11-17 -- legend-engine's pure/v1 is the edge's, never asked here).
//
// A query that is not one relation -- a graph fetch's objects -- is not a cube source; the
// results panel shows those as JSON.

import { findAll, functionsCalled, isFunction, isLambda, transform, type Lambda, type ValueSpecification } from '../../../pure-protocol/src/index.ts';
import { CubeApp, RemoteRun, sourceColumns, type CubeAppOptions, type CubeSnapshot, type CubeView } from '../../../datacube/src/embed.ts';
import { runtimeOf, statementOf, type LiteEngine } from '../../../engine-client/src/legend/browser-engine.ts';
import type { PureModelContext } from '../../../engine-client/src/legend/wire.ts';
import { pureType } from '../../../engine-client/src/relation-type.ts';
import type { ResultColumn, Scalar } from '../../../engine-client/src/result.ts';
import { CubePlanner } from '../backend/cube-planner.ts';
import type { AppContext } from './context.ts';
import { executionLambda, parameterSpecs, type ParameterSpec } from './run.ts';
import type { Session } from './session.ts';

/**
 * The relation a cube reads: the query's last expression with each parameter replaced by its value
 * and each constant (`let name = value;` ahead of it) by its value, in order -- a cube's source
 * is one expression. Undefined when a statement ahead is not a `let`, or an inner lambda reuses a
 * parameter's or constant's name -- such a query is not rewritten here.
 */
export function cubeSource(l: Lambda, values: readonly ParameterSpec[]): ValueSpecification | undefined {
  const lets = l.body.slice(0, -1).map(letBinding);
  if (lets.some((b) => b === undefined)) return undefined;
  const names = new Set([...l.parameters.map((p) => p.name), ...lets.map((b) => b!.name)]);
  if (l.body.some((s) => findAll(s, isLambda).some((inner) => inner.parameters.some((p) => names.has(p.name))))) return undefined;
  const byName = new Map(values.map((v) => [v.name, v.value]));
  const bind = (s: ValueSpecification): ValueSpecification => transform(s, (n) => (n._type === 'var' && byName.has(n.name) ? byName.get(n.name)! : n));
  for (const b of lets) byName.set(b!.name, bind(b!.value));
  return bind(l.body[l.body.length - 1]!);
}

function letBinding(s: ValueSpecification): { name: string; value: ValueSpecification } | undefined {
  if (!isFunction(s, 'letFunction') || s.parameters.length !== 2) return undefined;
  const [left, value] = s.parameters;
  return left?._type === 'string' && value ? { name: left.value, value } : undefined;
}

/** Functions whose answer is objects, not rows: such a query is not a cube source. */
const OBJECT_ANSWERS = new Set(['graphFetch', 'graphFetchChecked', 'serialize']);

/**
 * A cube over the session's query, in `host`; undefined when the query is not a cube source: a
 * graph fetch (objects), or one `cubeSource` does not rewrite. Its columns are typed by the
 * compiler before it opens; a query that does not compile refuses here, with the compiler's error.
 */
export async function openCube(app: AppContext, session: Session, host: HTMLElement,
  onView: (view: CubeView) => void): Promise<CubeApp | undefined> {
  if (session.query.graph && !session.text) return undefined;
  const l = executionLambda(session, undefined);
  if ([...functionsCalled(l)].some((f) => OBJECT_ANSWERS.has(f.slice(f.lastIndexOf(':') + 1)))) return undefined;
  const source = cubeSource(l, parameterSpecs(session, l));
  if (!source) return undefined;
  const runtime = runtimeOf(l);
  const model = session.project.context;
  const graph = session.project.graph;
  let rows: Pick<CubeAppOptions, 'engine' | 'planner' | 'runner'>;
  if (app.cubeRows.kind === 'sql') {
    // typing, parsing and printing are the tab's planner (the same compiler)
    if (!app.planner) throw new Error('queries run in the browser, which needs the planner (config.planner)');
    rows = { engine: app.cubeRows.engine, planner: new CubePlanner(app.planner, model, runtime, (t) => graph.enumerations.has(t)) };
  } else {
    rows = { runner: new RemoteRun(liteRows(app.engine, model, (t) => graph.enumerations.has(t))) };
  }
  const columns = await sourceColumns((rows.planner ?? rows.runner)!, source);
  const snapshot: CubeSnapshot = {
    source: { query: source }, columns, derived: [], rows: [], pivotOn: [], sorts: [], epoch: 1,
  };
  return new CubeApp(host, snapshot, {
    ...rows,
    // the grid alone, in DataCube's own look: its controls come back from the right-click menu
    controlsHidden: true,
    onView,
    writeClipboard: (text) => navigator.clipboard?.writeText(text),
    // an export is text (CSV, HTML) or bytes (Excel, PDF)
    download: (name, mime, content) => {
      const a = document.createElement('a');
      const part: BlobPart = typeof content === 'string' ? content : new Uint8Array(content);
      a.href = URL.createObjectURL(new Blob([part], { type: mime }));
      a.download = name;
      a.click();
      setTimeout(() => URL.revokeObjectURL(a.href), 5000);
    },
  } as CubeAppOptions);
}

/**
 * The cube's rows on a server: legend-lite's own plan and run, through the app's engine (a LiteServer: its
 * /api/lite/v1), each answer lite's typed rows read as the cube's columns -- typing, parsing and printing the same
 * engine's (the orchestrator's Q-c ruling of 2026-11-17: no product app reads rows through legend-engine's pure/v1).
 */
function liteRows(engine: LiteEngine, model: PureModelContext, isEnumeration: (type: string) => boolean):
  ConstructorParameters<typeof RemoteRun>[0] {
  // a column of an enumeration reads as String, its values' names, as the tab's cube reads it (cube-planner.ts)
  const typeOf = (type: string): string => (isEnumeration(type) ? 'String' : pureType(type));
  return {
    async execute(query, epoch, signal) {
      const started = Date.now();
      // lite's query/run, and its statement for the receipt from query/plan (spec §2: query/run answers rows alone)
      const [answer, plan] = await Promise.all([engine.queryRun(model.code, query, runtimeOf(query), {}, signal),
        engine.queryPlan(model.code, query, runtimeOf(query))]);
      const sql = statementOf(plan);
      if (answer.shape !== 'RELATION') throw new Error(`the cube's query answered ${answer.shape.toLowerCase()}s, not a relation's rows`);
      const columns: ResultColumn[] = answer.columns.map((c, i) => ({
        name: c.name, type: typeOf(c.type), values: answer.rows.map((r) => scalar(r[i], c.name)),
      }));
      return {
        rows: {
          columns, rowCount: answer.rows.length, epoch, elapsedMs: Date.now() - started,
          receipt: { plane: 'engine', where: `the server at ${engine.name}`, ...(sql ? { serverSql: sql } : {}) },
        },
        sql,
      };
    },
    async relationType(query) {
      return (await engine.queryType(model.code, query)).map((c) => ({ name: c.name, type: typeOf(c.type) }));
    },
    parse: (text) => engine.queryParse(text),
    // engine-remote's RemoteExecutor speaks legend-engine's styles ('PRETTY' | 'STANDARD'): each read as lite's word
    print: (query, style = 'PRETTY') => engine.queryPrint(query, style === 'PRETTY' ? 'pretty' : 'standard'),
  };
}

/** A cell of lite's typed rows as a cube's value: a string, a number, a bigint, a boolean or null; any other refused by
 *  its column's name. */
function scalar(v: unknown, column: string): Scalar {
  if (v === null || typeof v === 'string' || typeof v === 'number' || typeof v === 'bigint' || typeof v === 'boolean') return v;
  throw new Error(`the column '${column}' holds a ${typeof v}, which no cube cell is`);
}
