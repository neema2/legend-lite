// JOIN… IN THE TAB (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17.5): a grid joined to another of the page -- each a
// grid's result as it is now, read over one model of the tables they read (join-open.ts), the window asking and showing
// (ui/join-dialog.ts), the join made a grid beside the first by the page's own addGrid. On a host's page (Python's),
// which has no database of its own there, the host answers each of the window's asks by DataCube's own code over its
// tables (hostJoinFlow; docs/DATACUBE_ONE_API_PLAN_2026_10_21.md B11). Fetched with the first Join…: a page with no join
// loads none of it (test/bundle-budget.test.ts).

import type { Lambda } from '../../../pure-protocol/src/index.ts';
import { gridCapped, isLive, leavesOf, type JoinInput, type JoinSource } from '../cube-document.ts';
import { gridInput } from '../live-inputs.ts';
import { boundIn } from '../page-parameters.ts';
import { namedApart, newPick } from '../dynamic-input.ts';
import { pageCommand } from '../page-gestures.ts';
import { HOST_TABLES, JoinInputs, TAB_TABLES, leafRefusal, openInputs, summaryCounts, type OpenedInputs } from '../join-open.ts';
import {
  joinRepeats, joinSummary, joinTitle, quickHow, summaryWords, type JoinHow, type JoinOp, type JoinRow, type JoinSide, type JoinSummaryCounts,
  type QuickForm,
} from '../join.ts';
import { joinInputOf, type PageCommand } from '../page-commands.ts';
import { pageToJson, type PageDocument } from '../page-document.ts';
import type { PageApp } from '../page/page-app.ts';
import { CubeRefusal, sameValue } from '../snapshot.ts';
import { joinDialog, type Choice, type JoinCandidate } from '../ui/join-dialog.ts';
import { FLOW_TOOLS, pageBoxName, readersOf } from '../flow-boxes.ts';
import type { LineEnd } from '../layout/flow-board.ts';
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
async function asked(page: PageApp, ask: JoinAsk | undefined, s: JoinSession, words: { readonly lead?: string; readonly action?: string;
  readonly how?: JoinHow; readonly form?: Choice; readonly op?: JoinOp; readonly once?: true } = {}): Promise<void> {
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
  const begun = { ...start, ...(words.how !== undefined ? { how: words.how } : {}), ...(words.form !== undefined ? { form: words.form } : {}),
    ...(words.op !== undefined ? { op: words.op } : {}), ...(words.once !== undefined ? { once: words.once } : {}) };
  await joinDialog(document, {
    mine: s.mine, others: s.others, inputs: (their) => s.inputs(their), parse: (text) => s.parse(text),
    summary: (their, how) => s.summary(their, how),
    apply: async (their, how) => noted(await s.make(their, how), []),
    ...(Object.keys(begun).length > 0 ? { start: begun } : {}),
    ...(words.lead !== undefined ? { lead: words.lead } : {}), ...(words.action !== undefined ? { action: words.action } : {}),
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
      // each read with the page's parameters' values now (page-parameters.ts boundIn), as a grid's base is
      const inputs = await openInputs(boundIn(mine.input, now.parameters ?? []), boundIn(other.input, now.parameters ?? []), {
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
        // opened over the page as it is, its inputs read with its parameters' values (the page keeps them as `$name`)
        const made = await sources.openedJoin(source, {
          capped: gridCapped(page.cubes.length + 1), tables: [...mine.tables, ...other.tables], on: { page: now, cube: '' },
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
  const source = o.sources.reads(id);
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

/** A flow's Join or Union box, made or set again by the Join window (docs/DATACUBE_FLOW_MODE_2026_10_24.md M2). */
export interface FlowJoinOf {
  /** The box joined: the new box's Left -- or, `box` given, the Join box set again. */
  readonly cube: string;
  readonly box?: string;
  /** The tool it is: a Union's window starts at Append (a Dynamic Input's too: made, it keeps the rows of the one a new
   *  parameter of the page names), an Append Fields' at Every pair, a Fuzzy Match's at matching rows only on similar
   *  text. */
  readonly tool: 'join' | 'union' | 'dynamicInput' | 'appendFields' | 'fuzzyMatch' | 'findReplace';
  /** The flow's boxes, as its canvas names them: the boxes its Right is chosen among. */
  readonly boxes: readonly string[];
  /** Dropped on a line: the box that line went into, which reads the new box from then on (one step). */
  readonly into?: LineEnd;
  readonly page: PageApp;
  readonly sources: PageSources;
  readonly services: Services & { readonly engine: { readonly local: LocalEngine } };
  readonly stillShown: () => boolean;
}

/**
 * A FLOW'S JOIN BOX (M2; FL6, new joins are live): the Join window over box `cube` (its Left) and another box of the flow
 * (its Right), each read as it is now -- its leaves' tables found or read in, the boxes' grids not open (sources.ts
 * openedInputs) -- applied, a box after the Left reading both live, made as a saved join of the page opens. A Join box set
 * again (`box`): the window over its two inputs, starting at its join; applied, its join and its Right told as one step
 * (setJoin, setInput). A box whose Left is a copy is linked first.
 */
export async function flowJoin(o: FlowJoinOf): Promise<void> {
  const { page, sources, services } = o;
  const local = services.engine.local;
  const now = page.document(page.title);
  if (!now) throw new Error('this page has no box to join');
  const view = o.box === undefined ? undefined : now.views.find((v) => v.id === o.box);
  const was = view === undefined ? undefined : now.cubes.find((c) => c.id === view.cube)?.cube.source;
  if (o.box !== undefined && was?._type !== 'join') throw new Error(`${o.box} is no join`);
  const joined = was?._type === 'join' ? was : undefined;
  if (joined !== undefined && !isLive(joined.left)) throw new CubeRefusal('its Left reads a copy: link it to a box first (its settings\' Left)');
  const left = joined !== undefined && isLive(joined.left) ? joined.left.grid : o.cube;
  const right = joined !== undefined && isLive(joined.right) ? joined.right.grid : undefined;
  const cubeOf = (id: string) => {
    const v = now.views.find((x) => x.id === id);
    return v === undefined ? undefined : now.cubes.find((c) => c.id === v.cube)?.cube;
  };
  // each box called as its canvas calls it: the name a person gave it, else its tool's (flow-boxes.ts pageBoxName)
  const named = (id: string): string => pageBoxName(now, id);
  // its Right: any other box of the flow -- set again, none that reads the Join box (it would read its own rows)
  const readers = o.box === undefined ? new Set<string>() : readersOf(o.box, o.boxes, cubeOf);
  const others = o.boxes.filter((b) => b !== left && b !== o.box && !readers.has(b)).map((id) => ({ id, name: named(id) }));
  if (others.length === 0) throw new CubeRefusal('a join needs another box on the flow: add one first (an Input)');
  const opened = new JoinInputs<{ readonly inputs: OpenedInputs; readonly rows: GridRows; dispose(): Promise<void> }>(async (their) => {
    const at = page.document(page.title) ?? now;
    const bound = (grid: string) => boundIn(gridInput(at, grid), at.parameters ?? []);
    const got = await sources.openedInputs(bound(left), bound(their), named(left));
    if (got === undefined) throw new Error('a file it reads was not given');
    return got;
  });
  try {
    await asked(page, right !== undefined ? { their: right } : undefined, {
      mine: named(left),
      others: others.map((b) => ({ id: b.id, name: b.name })),
      inputs: async (their) => (await opened.get(their)).inputs,
      parse: (text) => local.planner.parse(text),
      summary: (their, how) => opened.use(their, ({ inputs, rows }) => summaryCounts(joinSummary(inputs.left, inputs.right, how),
        async (query) => rows.engine!.run((await rows.planner!.plan(query)).sql, 0))),
      make: async (their, asked) => {
        if (joined !== undefined) {
          // a Dynamic Input set again an append still: its pick kept (its settings' Keep the rows of sets it)
          const how = asked.form === 'append' && joined.how.form === 'append' && joined.how.pick !== undefined ? { ...asked, pick: joined.how.pick } : asked;
          // set again: its Right, then its join -- one step, one undo
          const commands: PageCommand[] = [
            ...(their !== right ? [pageCommand('setInput', { grid: o.box!, side: 'right', input: { grid: their } })] : []),
            ...(sameValue(how, joined.how) ? [] : [pageCommand('setJoin', { grid: o.box!, how })]),
          ];
          if (commands.length > 0 && !page.editPage(`Set ${FLOW_TOOLS[o.tool].name}`, commands)) {
            throw new Error('the page refused it');
          }
          return undefined;
        }
        const at = page.document(page.title);
        if (!at) throw new Error('the page has no grid now');
        // a Dynamic Input (M8; dynamic-input.ts): an append that keeps the rows of the one input a new parameter of the
        // page names -- its inputs' names its choices, each box no one named that would share one named as the canvas
        // names it -- opened over the page with them, set in the same step as the box
        const apart = o.tool === 'dynamicInput' && asked.form === 'append'
          ? namedApart(at, undefined, [{ grid: left }, { grid: their }], (g) => pageBoxName(at, g)) : undefined;
        const picked = apart === undefined ? undefined : newPick(at.parameters ?? [], apart.names);
        const how = picked?.how ?? asked;
        const source: JoinSource = {
          _type: 'join', name: joinTitle(how, named(left), named(their)), left: { grid: left }, right: { grid: their }, how, columns: [],
        };
        const onPage = apart === undefined || picked === undefined ? at
          : { ...apart.page, parameters: [...(at.parameters ?? []), picked.parameter] };
        const made = await sources.openedJoin(source, { capped: gridCapped(page.cubes.length + 1), on: { page: onPage, cube: '' } });
        if (!made) return undefined;
        if (!o.stillShown()) {
          await made.discard?.();
          return undefined;
        }
        const into = o.into;
        const parameter = picked === undefined ? [] : [...apart!.titles, pageCommand('setParameter', { parameter: picked.parameter })];
        const id = page.addGrid(made.make, {
          near: left,
          ...(into !== undefined ? {
            label: 'Insert Box', after: (id: string) => [...parameter, pageCommand('setInput', { grid: into.tile, side: into.side, input: { grid: id } })],
          } : picked !== undefined ? { label: 'Add Dynamic Input', after: () => parameter } : {}),
        });
        // a Find Replace: the join made, its column replaced in its own window, docked (TOOL_EDITORS)
        if (o.tool === 'findReplace') {
          page.boxAddedAs(id, 'findReplace');
          page.grid(id)?.openStep?.({ window: 'findReplace' });
        }
        return { id, notes: made.notes };
      },
    }, {
      lead: joined !== undefined ? `How ${named(left)} and its Right combine: the box's rows, read again as they are now.`
        : o.tool === 'dynamicInput' ? `Pick between ${named(left)} and another box of the flow as it runs: a box after them that keeps the rows of the one a new parameter of the page names.`
        : o.tool === 'findReplace' ? `Find each row of ${named(left)} among another box's rows -- a column of yours, theirs it is found in -- then, beside, the column of theirs it becomes.`
          : `Combine ${named(left)} with another box of the flow: a box after it that reads both, live.`,
      action: joined !== undefined ? 'Apply' : `Add ${FLOW_TOOLS[o.tool].name}`,
      // a Union's window starts at Append, an Append Fields' at Every pair; a box set again, at its own join
      ...(joined !== undefined ? { how: joined.how } : o.tool === 'union' || o.tool === 'dynamicInput' ? { form: 'append' as const }
        : o.tool === 'appendFields' ? { form: 'cross' as const } : o.tool === 'fuzzyMatch' ? { form: 'inner' as const, op: '~' as const }
          // a Find Replace is a lookup: each of mine with one of theirs at most (C13)
          : o.tool === 'findReplace' ? { form: 'left' as const, once: true as const } : {}),
    });
  } finally {
    await opened.close();
  }
}
