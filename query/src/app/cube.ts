// The results grid is a DataCube: a CubeApp over the query the person built, as upstream Legend
// Query hands its result to DataCube. The query is the cube's SOURCE (a relation); the cube adds
// its own paging, sorting, grouping, pivots and formatting on top, planned by the same legend-lite
// planner. Where the rows come from is the plane's (`AppContext.cubeRows`): the tab's SQL engine
// with the tab's planner, or a legend server's pure/v1 execute.
//
// A query that is not one relation -- a graph fetch's objects -- is not a cube source; the
// results panel shows those as JSON.

import { findAll, functionsCalled, isFunction, isLambda, transform, type Lambda, type ValueSpecification } from '../../../pure-protocol/src/index.ts';
import { CubeApp, RemoteRun, sourceColumns, type CubeAppOptions, type CubeSnapshot, type CubeView } from '../../../datacube/src/embed.ts';
import { LegendEngineExecutor } from '../../../engine-client/src/engine-remote.ts';
import { runtimeOf } from '../backend/browser-engine.ts';
import { CubePlanner } from '../backend/cube-planner.ts';
import type { ParameterValue } from '../backend/wire.ts';
import type { AppContext } from './context.ts';
import { executionLambda, parameterValues } from './run.ts';
import type { Session } from './session.ts';

/**
 * The relation a cube reads: the query's last expression with each parameter replaced by its value
 * and each constant (`let name = value;` ahead of it) by its value, in order -- a cube's source
 * is one expression. Undefined when a statement ahead is not a `let`, or an inner lambda reuses a
 * parameter's or constant's name -- such a query is not rewritten here.
 */
export function cubeSource(l: Lambda, values: readonly ParameterValue[]): ValueSpecification | undefined {
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
  const source = cubeSource(l, parameterValues(session, l));
  if (!source) return undefined;
  const runtime = runtimeOf(l);
  const model = session.project.context;
  const graph = session.project.graph;
  // typing, parsing and printing are the tab's planner wherever there is one (the same compiler)
  const planner = app.planner && new CubePlanner(app.planner, model, runtime, (t) => graph.enumerations.has(t));
  let rows: Pick<CubeAppOptions, 'engine' | 'planner' | 'runner'>;
  if (app.cubeRows.kind === 'sql') {
    if (!planner) throw new Error('queries run in the browser, which needs the planner (config.planner)');
    rows = { engine: app.cubeRows.engine, planner };
  } else {
    const server = new LegendEngineExecutor({ baseUrl: app.cubeRows.baseUrl, model: model.code, runtime });
    rows = {
      runner: new RemoteRun(planner
        ? {
          execute: (q, s, signal) => server.execute(q, s, signal),
          relationType: (q, signal) => planner.relationType(q, signal),
          parse: (text) => planner.parse(text),
          print: (q, style) => planner.print(q, style),
        }
        : server),
    };
  }
  const columns = await sourceColumns((rows.planner ?? rows.runner)!, source);
  const snapshot: CubeSnapshot = {
    source: { query: source }, columns, derived: [], rows: [], pivotOn: [], measures: [], sorts: [], epoch: 1,
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
