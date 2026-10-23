// PAGE FILTERS (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §16.2): a dashboard's filters, by a class's property -- each a
// class, a condition on its properties (the grid's own filter editor's: a property is read `$x.p` as a column is), and,
// made by a chart's click, that chart. A filter reaches every grid over its class (a class source): it is put into the
// class's rows before the grid's columns are taken -- `Trade.all()->filter(x|...)->project(~[...])` -- as a rewrite of
// every query the grid runs (runner.ts QueryLens), never the grid's own state, so a grid exported alone, its undo and its
// copies are the grid alone. A grid over another source is not reached. No DOM, no planner: tested alone.

import { and, fn, isIdentifier, lambda, transform, type Lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import type { ClassSource, CubeSource } from './cube-document.ts';
import { OPERATORS } from './filter-operators.ts';
import { readFilterNode, type GridCommand } from './grid-commands.ts';
import type { PageCommand } from './page-commands.ts';
import { filterNode } from './query.ts';
import type { QueryLens } from './runner.ts';
import { CubeRefusal, isRelativeDate, sameValue, type FilterNode, type FilterValue } from './snapshot.ts';

/** One filter of the page. */
export interface PageFilter {
  /** `filter-N`: given once, never again on the page (page-commands.ts). */
  readonly id: string;
  /** The class it filters, by its path. */
  readonly class: string;
  /** What it lets through, on the class's properties. */
  readonly condition: FilterNode;
  /** The chart whose click made it: that chart's own grid is not filtered by it (the chart keeps every mark). */
  readonly from?: string;
}

/** A page filter's id: `filter-N`. */
export const FILTER_ID = /^filter-\d+$/;

/** The properties a condition names: each condition's column, and a `*Column` operator's other column. */
export function namedProperties(node: FilterNode): Set<string> {
  const out = new Set<string>();
  const visit = (n: FilterNode): void => {
    if (n.kind === 'condition') {
      out.add(n.column);
      if (n.rightColumn !== undefined) out.add(n.rightColumn);
    } else if (n.kind === 'not') visit(n.child);
    else n.children.forEach(visit);
  };
  visit(node);
  return out;
}

/**
 * A CHART'S MARK ON A GRID OVER A CLASS (§16.2: one rule, the screen's click's and Python's chart.select's): the mark's
 * conditions on the class's properties are a page filter from that chart, replacing its last, reaching every other grid
 * over the class; the rest -- a calculated column's -- are the grid's own, as on any grid (`own`; null: none given, the
 * selection cleared). `command`, decided on the page's filters as the edits before it leave them: the chart's filter set,
 * or taken off when none of the mark is on the class -- or, a click's `toggle`, when the chart's filter is the same mark
 * again (Python names the mark it wants and keeps it: D8). Undefined: nothing to do.
 */
export function classPick(
  source: ClassSource, chart: string, conditions: readonly FilterNode[] | null, toggle: boolean,
): {
  readonly own: readonly FilterNode[] | null;
  readonly command: (filters: readonly PageFilter[]) => PageCommand | undefined;
} {
  const properties = new Set(source.columns.map((c) => c.name));
  const onClass = (c: FilterNode): boolean => [...namedProperties(c)].every((p) => properties.has(p));
  const mine = (conditions ?? []).filter(onClass);
  const own = conditions === null ? null : conditions.filter((c) => !onClass(c));
  const condition: FilterNode | undefined = mine.length === 0 ? undefined
    : mine.length === 1 ? mine[0]! : { kind: 'and', children: mine };
  return {
    own,
    command: (filters) => {
      const last = filters.find((f) => f.from === chart);
      // as data, whatever order its writer put the keys in (F43: a page read back writes them in its reader's)
      const same = last !== undefined && condition !== undefined && sameValue(last.condition, condition);
      if (condition === undefined || (toggle && same)) return last ? { kind: 'removePageFilter', filter: last.id } : undefined;
      if (same) return undefined;
      return { kind: 'setPageFilter', ...(last ? { id: last.id } : {}), class: source.class, condition, from: chart };
    },
  };
}

/**
 * A CHART'S MARK PICKED, the one rule (the screen's click on a mark and on its chip, page/cube-page.ts #select; Python's
 * chart.select, headless chartPick): the mark's `conditions` (null: the selection cleared) against the chart's selection
 * now (`old`) -- on a grid over a class (`source`), those on its properties the chart's page filter (classPick, decided
 * on the page's `filters`), the rest its grid's own -- as its grid's `refilter` (the old selection off, the new on) and
 * the page's commands: the page filter's, then the chart's `select`. `toggle`: a click's -- the same mark again takes it
 * off; Python names the mark it wants and keeps it (D8). Compared as data, whatever order a writer put a condition's
 * keys in (F43; the S7 audit's Y-9: the screen compared JSON text).
 */
export function chartPicked(o: {
  readonly source: CubeSource | undefined;
  readonly chart: string;
  readonly conditions: readonly FilterNode[] | null;
  readonly old: readonly FilterNode[];
  readonly filters: readonly PageFilter[];
  readonly toggle: boolean;
}): { readonly refilter: GridCommand | undefined; readonly page: readonly PageCommand[] } {
  const page: PageCommand[] = [];
  let next: readonly FilterNode[] = o.conditions ?? [];
  if (o.source?._type === 'class') {
    const pick = classPick(o.source, o.chart, o.conditions, o.toggle);
    const command = pick.command(o.filters);
    if (command !== undefined) page.push(command);
    next = pick.own ?? [];
  }
  // the same mark again: a click takes it off, a mark named is kept
  const add = o.toggle && next.length > 0 && sameValue(next, o.old) ? [] : next;
  if (sameValue(add, o.old)) return { refilter: undefined, page };
  return {
    refilter: { kind: 'refilter', remove: [...o.old], add: [...add] },
    page: [...page, { kind: 'select', chart: o.chart, conditions: [...add] }],
  };
}

/**
 * A page filter as the page document keeps it, read field by field, or why not (CubeRefusal): its id, its class, its
 * condition (the grid filter's own reader), and the chart it came from -- one of `charts`, the page's chart views. A
 * property it names is a property's name (`$x.p`: a name the grammar reads bare).
 */
export function readPageFilter(raw: unknown, charts: ReadonlySet<string>): PageFilter {
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) throw new CubeRefusal(`a page filter is an object, not ${JSON.stringify(raw)}`);
  const o = raw as Record<string, unknown>;
  const odd = Object.keys(o).filter((k) => !['id', 'class', 'condition', 'from'].includes(k));
  if (odd.length > 0) throw new CubeRefusal(`a page filter takes id, class, condition and from, not ${odd.join(', ')}`);
  if (typeof o['id'] !== 'string' || !FILTER_ID.test(o['id'])) throw new CubeRefusal(`a page filter's id is filter-N, not ${JSON.stringify(o['id'])}`);
  if (typeof o['class'] !== 'string' || o['class'] === '') throw new CubeRefusal(`page filter ${o['id']} names no class`);
  const condition = readFilterNode(o['condition']);
  const bad = [...namedProperties(condition)].find((p) => !isIdentifier(p));
  if (bad !== undefined) throw new CubeRefusal(`page filter ${o['id']} names '${bad}', which is no property's name`);
  const from = o['from'];
  if (from !== undefined && (typeof from !== 'string' || !charts.has(from))) {
    throw new CubeRefusal(`page filter ${o['id']} came from ${JSON.stringify(from)}, which is no chart of the page`);
  }
  return { id: o['id'], class: o['class'], condition, ...(typeof from === 'string' ? { from } : {}) };
}

/** The page's filters, read: a list, each id once. */
export function readPageFilters(raw: unknown, charts: ReadonlySet<string>): readonly PageFilter[] {
  if (!Array.isArray(raw)) throw new CubeRefusal("a page's filters are a list");
  const filters = raw.map((f) => readPageFilter(f, charts));
  const ids = new Set<string>();
  for (const f of filters) {
    if (ids.has(f.id)) throw new CubeRefusal(`two page filters are ${f.id}`);
    ids.add(f.id);
  }
  return filters;
}

// ---------------------------------------------------------------- in words

const LABEL = new Map<string, string>(OPERATORS.map((o) => [o.op, o.label]));

function valueWords(v: FilterValue): string {
  if (isRelativeDate(v)) return v.relative;
  if (typeof v === 'object') return v.json;
  return String(v);
}

/** A condition in words: `region = EMEA`, `region in EMEA, AMER`, `notional is null`, `a and (b or c)`. */
export function conditionWords(node: FilterNode): string {
  switch (node.kind) {
    case 'condition': {
      const op = LABEL.get(node.operator) ?? node.operator;
      if (node.rightColumn !== undefined) return `${node.column} ${op} ${node.rightColumn}`;
      if (node.value === undefined) return `${node.column} ${op}`;
      const value = Array.isArray(node.value) ? (node.value as readonly FilterValue[]).map(valueWords).join(', ')
        : valueWords(node.value as FilterValue);
      return `${node.column} ${op} ${value}`;
    }
    case 'not':
      return `not (${conditionWords(node.child)})`;
    case 'and':
    case 'or':
      return node.children.map((c) => (c.kind === 'and' || c.kind === 'or' ? `(${conditionWords(c)})` : conditionWords(c)))
        .join(` ${node.kind} `);
  }
}

/** A page filter in words, its class first: `Trade: region = EMEA`. */
export function filterWords(f: PageFilter): string {
  return `${f.class.slice(f.class.lastIndexOf(':') + 1)}: ${conditionWords(f.condition)}`;
}

// ---------------------------------------------------------------- what reaches a grid

/**
 * Every `getAll` of `cls` in `query`, its rows filtered by `where` (over `$x`) first. A query that reads no `getAll` of
 * the class is refused: the filter the page says is on the grid is never left off it unsaid.
 */
export function filteredByClass(query: Lambda, cls: string, where: ValueSpecification): Lambda {
  let found = false;
  const isAll = (n: ValueSpecification): boolean => n._type === 'func' && n.function === 'getAll'
    && n.parameters.length === 1 && n.parameters[0]!._type === 'packageableElementPtr' && n.parameters[0]!.fullPath === cls;
  const out = transform(query, (n) => {
    if (!isAll(n)) return n;
    found = true;
    return fn('filter', n, lambda(['x'], where));
  }) as Lambda;
  if (!found) throw new CubeRefusal(`the page's filters are on ${cls}, and this query reads no ${cls.slice(cls.lastIndexOf(':') + 1)}.all()`);
  return out;
}

/** What the page's filters do to one grid. */
export interface GridLens {
  /** The rewrite of its queries; none when no filter reaches it. */
  readonly lens?: QueryLens;
  /** The filters that reach it, by id, in the page's order. */
  readonly taken: readonly string[];
  /** What its lens is made of: the same key, the same lens (none: ''). */
  readonly key: string;
  /** The filters on its class it cannot take, each why: a property its class (at its pin) has not. */
  readonly refused: readonly { readonly id: string; readonly why: string }[];
}

/**
 * What the page's filters do to a grid over `source` whose charts are `charts`: none to a grid over anything but a
 * class; to a class's grid, every filter on that class but one its own chart's click made, ANDed, each value a literal
 * of its property's type (the class source's columns) -- a filter naming a property the grid's class has not is refused
 * for it, never dropped unsaid.
 */
export function gridLens(filters: readonly PageFilter[], source: CubeSource | undefined, charts: ReadonlySet<string>): GridLens {
  if (source?._type !== 'class') return { taken: [], key: '', refused: [] };
  const cls: ClassSource = source;
  const types = new Map(cls.columns.map((c) => [c.name, c.type]));
  const taken: PageFilter[] = [];
  const refused: { id: string; why: string }[] = [];
  for (const f of filters) {
    if (f.class !== cls.class || (f.from !== undefined && charts.has(f.from))) continue;
    const missing = [...namedProperties(f.condition)].filter((p) => !types.has(p));
    if (missing.length > 0) refused.push({ id: f.id, why: `${cls.name} has no ${missing.join(', ')}` });
    else taken.push(f);
  }
  if (taken.length === 0) return { taken: [], key: '', refused };
  const where = and(...taken.map((f) => filterNode(f.condition, 'x', (c) => types.get(c))));
  return {
    lens: (query) => filteredByClass(query, cls.class, where),
    taken: taken.map((f) => f.id),
    key: JSON.stringify(taken.map((f) => [f.id, f.condition])),
    refused,
  };
}
