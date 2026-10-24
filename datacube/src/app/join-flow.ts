// JOIN… IN THE TAB (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17.5): a grid joined to another of the page -- each a
// grid's result as it is now, read over one model of the tables they read (join-open.ts), the window asking and showing
// (ui/join-dialog.ts), the join made a grid beside the first by the page's own addGrid. On a host's page (Python's),
// which has no database of its own there, the host answers each of the window's asks by DataCube's own code over its
// tables (hostJoinFlow; docs/DATACUBE_ONE_API_PLAN_2026_10_21.md B11). Fetched with the first Join…: a page with no join
// loads none of it (test/bundle-budget.test.ts).

import type { Lambda } from '../../../pure-protocol/src/index.ts';
import { gridCapped, leavesOf, type JoinInput, type JoinSource } from '../cube-document.ts';
import { HOST_TABLES, JoinInputs, TAB_TABLES, leafRefusal, openInputs, summaryCounts, type OpenedInputs } from '../join-open.ts';
import {
  joinRepeats, joinSummary, joinTitle, quickHow, summaryWords, type JoinHow, type JoinRow, type JoinSide, type JoinSummaryCounts,
  type QuickForm,
} from '../join.ts';
import { joinInputOf } from '../page-commands.ts';
import { pageToJson, type PageDocument } from '../page-document.ts';
import type { PageApp } from '../page/page-app.ts';
import { CubeRefusal } from '../snapshot.ts';
import { joinDialog, type JoinCandidate } from '../ui/join-dialog.ts';
import { catalogColumns } from '../upload.ts';
import type { GridRows, HostJoin, LocalEngine, Services } from './services.ts';
import type { PageSources } from './sources.ts';

/**
 * WHAT JOIN… IS ASKED (a right-click's Join ▸, the user 2026-10-23): the window, at their grid and on a key when the
 * column clicked gave one; or a QUICK join -- `quick`, on `their` grid by `row` -- made at once, unless it would repeat
 * rows of mine (join.ts joinRepeats), when the window opens on it instead, its warning shown before anything is made.
 * None: the window as the grid's menu opens it.
 */
export interface JoinAsk {
  readonly their?: string;
  readonly row?: JoinRow;
  readonly quick?: QuickForm;
}

/** A join's two inputs as each flow reads them, and its grid made: the window's asks, or a quick join's. */
interface JoinSession {
  readonly mine: string;
  readonly others: readonly JoinCandidate[];
  inputs(their: string): Promise<{ readonly left: JoinSide; readonly right: JoinSide }>;
  parse(text: string): Promise<Lambda>;
  summary(their: string, how: JoinHow): Promise<JoinSummaryCounts>;
  /** The join made a grid beside mine: its id, and what opening it left out (none when the page moved on). */
  make(their: string, how: JoinHow): Promise<{ readonly id: string; readonly notes: readonly string[] } | undefined>;
}

/**
 * A join asked (JoinAsk) over a session: a quick one made at once when it repeats no row of mine, what it did noted on
 * its grid; else the window, starting where the ask says. What a made grid's opening left out is noted on it too --
 * where it is seen, not in a window that may be closed.
 */
async function asked(page: PageApp, ask: JoinAsk | undefined, s: JoinSession): Promise<void> {
  const noted = (made: { readonly id: string; readonly notes: readonly string[] } | undefined, words: readonly string[]): void => {
    const text = [...words, ...(made?.notes ?? [])].join(' ');
    if (made !== undefined && text !== '') page.grid(made.id)?.note(text);
  };
  if (ask?.quick !== undefined && ask.their !== undefined && ask.row !== undefined) {
    const how = quickHow(ask.row, ask.quick);
    const counts = await s.summary(ask.their, how);
    if (!joinRepeats(how, counts)) {
      noted(await s.make(ask.their, how), summaryWords(how, counts).slice(0, 1));
      return;
    }
  }
  const start = {
    ...(ask?.their !== undefined ? { their: ask.their } : {}),
    ...(ask?.row !== undefined ? { row: ask.row } : {}),
    ...(ask?.quick !== undefined ? { form: ask.quick } : {}),
  };
  await joinDialog(document, {
    mine: s.mine, others: s.others, inputs: (their) => s.inputs(their), parse: (text) => s.parse(text),
    summary: (their, how) => s.summary(their, how),
    apply: async (their, how) => noted(await s.make(their, how), []),
    ...(Object.keys(start).length > 0 ? { start } : {}),
  });
}

export interface JoinFlowOf {
  /** The grid joined, by its tile's id. */
  readonly cube: string;
  /** What was asked: the window (none), at a grid and a key, or a quick join. */
  readonly ask?: JoinAsk;
  readonly page: PageApp;
  readonly sources: PageSources;
  readonly services: Services & { readonly engine: { readonly local: LocalEngine } };
  /** Whether the page on screen is still the one the join began on (another opened meanwhile: no grid of this one). */
  readonly stillShown: () => boolean;
}

/**
 * A grid of the page as a join reads it -- its result as it is now, under the name it shows (page-commands.ts
 * joinInputOf, as Python's page.join asks it) -- and the tables of this tab's database it reads.
 */
function inputOf(o: JoinFlowOf, now: PageDocument, id: string): { readonly input: JoinInput; readonly tables: readonly string[] } | undefined {
  const tables = o.sources.place(id)?.tables;
  return tables ? { input: joinInputOf(now, id), tables } : undefined;
}

/**
 * Join… on grid `cube`: the window, and -- applied -- the join's grid beside it, its first input (as Python's page.join
 * places one: headless joinCube). Refused in words before the window opens.
 */
export async function joinFlow(o: JoinFlowOf): Promise<void> {
  const { page, sources, services } = o;
  const local = services.engine.local;
  const now = page.document(page.title);
  const mine = now && inputOf(o, now, o.cube);
  if (!now || !mine) throw new Error('this grid reads no table of this tab\'s database');
  // a pivoted grid joins as no fixed columns (refused, in words, when chosen): offered after the others, so the window
  // opens on one that joins (the start's grid is pivoted: the first offered, it opened on a refusal, 2026-10-23)
  const others = new Map(page.grids.filter((id) => id !== o.cube).flatMap((id) => {
    const i = inputOf(o, now, id);
    return i ? [[id, i] as const] : [];
  }).sort(([, a], [, b]) => Number(a.input.query.pivotOn.length > 0) - Number(b.input.query.pivotOn.length > 0)));
  if (others.size === 0) {
    throw new Error('a join needs another grid on the page over a file or a remote file: add one first (Create ▸ Data Source…)');
  }
  // each grid's inputs with mine over one model, opened once while the window is open, their planners let go when it
  // closes -- after any question still being answered over them (join-open.ts JoinInputs)
  const opened = new JoinInputs<{ readonly inputs: OpenedInputs; readonly rows: GridRows; dispose(): void }>(async (their) => {
    const other = others.get(their)!;
    let rows: GridRows | undefined;
    try {
      const inputs = await openInputs(mine.input, other.input, {
        here: TAB_TABLES,
        tables: [...mine.tables, ...other.tables],
        models: local.tables,
        catalog: (table) => catalogColumns(local.engine, local.tables, table),
        databaseType: local.engine.databaseType,
        typer: (model, runtime) => {
          rows = services.engine.rows(model, runtime);
          return rows.planner!;
        },
      });
      const held = rows!;
      return { inputs, rows: held, dispose: () => held.planner?.dispose?.() };
    } catch (e) {
      rows?.planner?.dispose?.();
      throw e;
    }
  });
  try {
    await asked(page, o.ask, {
      mine: mine.input.name,
      others: [...others].map(([id, i]) => ({ id, name: i.input.name })),
      inputs: async (their) => (await opened.get(their)).inputs,
      parse: (text) => local.planner.parse(text),
      // over their grid's own planner, held until it is answered
      summary: (their, how) => opened.use(their, ({ inputs, rows }) => summaryCounts(joinSummary(inputs.left, inputs.right, how),
        async (query) => rows.engine!.run((await rows.planner!.plan(query)).sql, 0))),
      make: async (their, how) => {
        const other = others.get(their);
        if (!other) throw new Error('that grid is no longer on the page');
        const source: JoinSource = {
          _type: 'join', name: joinTitle(how, mine.input.name, other.input.name), left: mine.input, right: other.input, how, columns: [],
        };
        const made = await sources.openedJoin(source, {
          capped: gridCapped(page.cubes.length + 1), tables: [...mine.tables, ...other.tables],
        });
        if (!made) return undefined;
        if (!o.stillShown()) {
          await made.discard?.();
          return undefined;
        }
        return { id: page.addGrid(made.make, { near: o.cube }), notes: made.notes };
      },
    });
  } finally {
    await opened.close();
  }
}

/** Join… on a host's page (HostJoin): the grid joined, the page, the host's join, and what reads a condition. */
export interface HostJoinFlowOf {
  /** The grid joined, by its tile's id. */
  readonly cube: string;
  /** What was asked: the window (none), at a grid and a key, or a quick join. */
  readonly ask?: JoinAsk;
  readonly page: PageApp;
  readonly sources: PageSources;
  readonly join: HostJoin;
  /** A condition written in Pure, read by the compiler where the host has it (QueryReading). */
  readonly parse: (text: string) => Promise<Lambda>;
  readonly stillShown: () => boolean;
}

/** Why a grid of a host's page is no input of a join there (join-open.ts leafRefusal over HOST_TABLES), or undefined. */
function hostRefusal(o: HostJoinFlowOf, id: string): string | undefined {
  const source = o.sources.place(id)?.cubeSource;
  if (source === undefined) return 'this grid is written down by no source: a join reads none';
  for (const leaf of leavesOf(source)) {
    const refused = leafRefusal(leaf, HOST_TABLES);
    if (refused !== undefined) return refused;
  }
  return undefined;
}

/**
 * Join… on grid `cube` of a host's page (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md B11): the same window, each of its
 * asks answered by the host over its tables (HostJoin: headless joinSides, joinQuestions, joinCube, as Python's
 * page.join asks them), the page as the tab holds it sent with each; applied, the join's grid -- the cube document the
 * host made -- opened as a saved join of the page is (sources.reopened) and added beside its first input. Refused in
 * words before the window opens.
 */
export async function hostJoinFlow(o: HostJoinFlowOf): Promise<void> {
  const { page, sources, join } = o;
  const mine = page.document(page.title);
  if (!mine) throw new Error('this page has no grid to join');
  const refused = hostRefusal(o, o.cube);
  if (refused !== undefined) throw new CubeRefusal(refused);
  const others = page.grids.filter((id) => id !== o.cube && hostRefusal(o, id) === undefined)
    .map((id) => ({ id, name: joinInputOf(mine, id).name }));
  if (others.length === 0) {
    throw new Error('a join needs another grid on the page over a dataframe, a file or a remote file: add one first');
  }
  // the page as it is when each ask is made: a grid changed while the window is open is joined as it is then
  const now = (): string => {
    const doc = page.document(page.title);
    if (!doc) throw new Error('the page has no grid now');
    return pageToJson(doc);
  };
  await asked(page, o.ask, {
    mine: joinInputOf(mine, o.cube).name,
    others,
    inputs: (their) => join.sides(now(), o.cube, their),
    parse: o.parse,
    summary: (their, how) => join.questions(now(), o.cube, their, how),
    make: async (their, how) => {
      // beside the grid joined, on its sheet: joinCube's placement, the one rule (the S7 audit's Y-3)
      const made = await join.cube(now(), o.cube, their, how, null);
      const opened = await sources.reopened(made, undefined, true, undefined, true);
      if (!opened) return undefined;
      if (!o.stillShown()) {
        await opened.discard?.();
        return undefined;
      }
      return { id: page.addGrid(opened.make, { near: o.cube }), notes: opened.notes };
    },
  });
}
