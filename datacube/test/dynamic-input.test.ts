// A DYNAMIC INPUT (dynamic-input.ts; docs/DATACUBE_FLOW_MODE_2026_10_24.md M8): a Union that keeps the rows of the one
// input a String parameter of the page names -- its commands (a new parameter of its inputs' names, its pick), its pick
// read and written as a join's, said in words, and refused where it cannot pick.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { fn, lambda, lit, toJson, variable } from '../../pure-protocol/src/index.ts';
import { joinInputs, rawRows, writeCube, type CubeDocument, type CubeSource, type FileSource, type JoinSource } from '../src/cube-document.ts';
import { DEFAULT_CONFIGURATION } from '../src/config.ts';
import { inputNames, namedApart, newPick, pickCommands, pickedBy } from '../src/dynamic-input.ts';
import { joinTool } from '../src/flow-boxes.ts';
import { heldHow, joinWords, readJoinHow, type JoinHow } from '../src/join.ts';
import { add, NEW_SHEET, type Bands } from '../src/layout/bands.ts';
import { applyPageCommands, idsOf } from '../src/page-commands.ts';
import { PAGE_GESTURES } from '../src/page-gestures.ts';
import { writePageOf, type PageDocument, type PageView } from '../src/page-document.ts';
import type { PageParameter } from '../src/page-parameters.ts';
import { CubeRefusal } from '../src/snapshot.ts';
import { TreeState } from '../src/tree.ts';
import { accessor } from '../../pure-protocol/src/index.ts';

const COLUMNS = [{ name: 'region', type: 'String' }, { name: 'notional', type: 'Float' }];
const FILE: FileSource = { _type: 'file', name: 'trades.csv', format: 'csv', size: 120, sha256: 'ab'.repeat(32), columns: COLUMNS };

const cube = (source: CubeSource): CubeDocument => writeCube({
  name: 'flow', source, snapshot: rawRows(accessor('local::DB', 'trades'), COLUMNS),
  configuration: DEFAULT_CONFIGURATION, tree: TreeState.fromPaths([], false),
});
const union = (how: JoinHow = { form: 'append' }): JoinSource => ({
  _type: 'join', name: 'Union', left: { grid: 'grid-1' }, right: { grid: 'grid-2' }, how, columns: COLUMNS,
});

/** Two Inputs, the second named Big (`named`), and a Union of them; `parameters`, the page's. */
function page(source: JoinSource = union(), parameters: readonly PageParameter[] = [], named = true): PageDocument {
  const cubes: Record<string, CubeDocument> = { 'grid-1': cube(FILE), 'grid-2': cube(FILE), 'grid-3': cube(source) };
  const ids = Object.keys(cubes);
  const views: PageView[] = ids.map((id) => ({ id, kind: 'grid', cube: id, ...(id === 'grid-2' && named ? { title: 'Big' } : {}) }));
  const doc = writePageOf({
    name: 'flow', cubes: ids.map((id) => ({ id, cube: cubes[id]! })),
    views: { views, sheets: [{ id: 'sheet-1', layout: { kind: 'bands', ...ids.reduce<Bands>((l, id) => add(l, id), NEW_SHEET) } }] },
  });
  return parameters.length > 0 ? { ...doc, parameters } : doc;
}
const applied = (doc: PageDocument, commands: Parameters<typeof applyPageCommands>[1]): PageDocument =>
  applyPageCommands({ page: doc, held: idsOf(doc) }, commands).draft.page;
const sourceOf = (doc: PageDocument, grid = 'grid-3'): CubeSource => doc.cubes.find((c) => c.id === grid)!.cube.source;

describe("a Union kept to the rows of the one input a parameter names", () => {
  it('reads its inputs by the names they are read under; a new pick a parameter of them, the first its value', () => {
    const doc = page();
    assert.deepEqual(inputNames(doc, 'grid-3', joinInputs(union())), ['trades.csv', 'Big']);
    const made = newPick([], ['trades.csv', 'Big']);
    assert.deepEqual(made.parameter, { name: 'source', type: 'String', value: 'trades.csv', choices: ['trades.csv', 'Big'] });
    assert.equal(toJson(made.how), toJson({ form: 'append', pick: lambda([], variable('source')) }));
    const taken: PageParameter[] = [{ name: 'source', type: 'Integer', value: 1 }, { name: 'source2', type: 'String', value: 'a' }];
    assert.equal(newPick(taken, ['a', 'b']).parameter.name, 'source3', 'a name the page has not');
    assert.throws(() => newPick([], ['trades.csv', 'trades.csv']),
      (e: unknown) => e instanceof CubeRefusal && /two of its inputs are named trades.csv/.test(e.message));
  });

  it('its commands: a new parameter and its pick in one step; one of the page\'s; every input again', () => {
    const made = pickCommands(page(), 'grid-3', { made: true });
    assert.deepEqual(made.map((c) => c.kind), ['setParameter', 'setJoin']);
    const after = applied(page(), made);
    assert.deepEqual(after.parameters?.map((p) => p.name), ['source']);
    const how = (sourceOf(after) as JoinSource).how;
    assert.equal(pickedBy(how), 'source');
    assert.equal(joinTool(how), 'dynamicInput');
    assert.equal(joinWords(how), 'the rows of the one $source names');
    assert.equal(joinTool({ form: 'append' }), 'union');
    // set so already: nothing; back to every input's rows
    assert.deepEqual(pickCommands(after, 'grid-3', { parameter: 'source' }), []);
    const back = applied(after, pickCommands(after, 'grid-3', null));
    assert.deepEqual((sourceOf(back) as JoinSource).how, { form: 'append' });
    assert.deepEqual(pickCommands(back, 'grid-3', null), [], 'every input\'s already');
    // one of the page's: its pick alone
    const own = page(union(), [{ name: 'which', type: 'String', value: 'Big' }]);
    const picked = pickCommands(own, 'grid-3', { parameter: 'which' });
    assert.deepEqual(picked.map((c) => c.kind), ['setJoin']);
    assert.equal(pickedBy((sourceOf(applied(own, picked)) as JoinSource).how), 'which');
  });

  it('two boxes no one named, read under one source\'s name, named as the canvas names them first -- in the same step', () => {
    const unnamed = page(union(), [], false);
    assert.throws(() => pickCommands(unnamed, 'grid-3', { made: true }), /two of its inputs are named trades.csv: a Dynamic Input picks one by its name -- rename one/);
    const called = (g: string): string => ({ 'grid-1': 'Input', 'grid-2': 'Filter' })[g]!;
    const apart = namedApart(unnamed, 'grid-3', joinInputs(union()), called);
    assert.deepEqual(apart.names, ['Input', 'Filter']);
    assert.deepEqual(apart.titles, [{ kind: 'renameTile', tile: 'grid-1', title: 'Input' }, { kind: 'renameTile', tile: 'grid-2', title: 'Filter' }]);
    const made = pickCommands(unnamed, 'grid-3', { made: true }, called);
    assert.deepEqual(made.map((c) => c.kind), ['renameTile', 'renameTile', 'setParameter', 'setJoin']);
    const after = applied(unnamed, made);
    assert.deepEqual(after.parameters?.[0]?.choices, ['Input', 'Filter']);
    assert.deepEqual(namedApart(page(), 'grid-3', joinInputs(union()), called).titles, [], 'named apart already: none named');
    // a box named by a person is never renamed: one no one named takes the canvas's name, if that names them apart
    const one = namedApart(page(union(), [], false), 'grid-3', joinInputs(union()), () => 'Filter');
    assert.deepEqual(one.names, ['Filter', 'Filter'], 'one name still');
    assert.throws(() => newPick([], one.names), /two of its inputs are named Filter/);
  });

  it('refuses in words what it cannot pick by, and a box that is no Union', () => {
    const doc = page(union(), [{ name: 'n', type: 'Integer', value: 1 }]);
    assert.throws(() => pickCommands(doc, 'grid-3', { parameter: 'nope' }), /'nope' is no parameter of the page/);
    assert.throws(() => pickCommands(doc, 'grid-3', { parameter: 'n' }), /n is no String, but Integer/);
    assert.throws(() => pickCommands(doc, 'grid-1', { made: true }), /'grid-1' is no Union/);
    const joined = page(union({ form: 'semi', on: { rows: [{ left: 'region', op: '==', right: 'region' }] } }));
    assert.throws(() => pickCommands(joined, 'grid-3', { made: true }), /'grid-3' is no Union/);
  });

  it("Python's grid.pick asks the same rule by name (page-gestures.ts 'flow.pick')", () => {
    const at = { page: JSON.parse(JSON.stringify(page())) as unknown };
    assert.deepEqual(PAGE_GESTURES['flow.pick']({ ...at, grid: 'grid-3', parameter: true }).map((c) => c.kind), ['setParameter', 'setJoin']);
    assert.deepEqual(PAGE_GESTURES['flow.pick']({ ...at, grid: 'grid-3', parameter: null }), []);
    assert.throws(() => PAGE_GESTURES['flow.pick']({ ...at, grid: 'grid-3', parameter: 3 }), /a parameter's name, true for a new one, or null/);
  });
});

describe("an append's pick, as a join keeps it", () => {
  it('reads back exactly; one of a parameter, or of several expressions, refused', () => {
    const how = { form: 'append', pick: lambda([], variable('source')) };
    assert.equal(toJson(readJoinHow(JSON.parse(toJson(how)))), toJson(how));
    assert.deepEqual(readJoinHow({ form: 'append' }), { form: 'append' });
    assert.throws(() => readJoinHow(JSON.parse(toJson({ form: 'append', pick: lambda(['x'], variable('x')) }))),
      (e: unknown) => e instanceof CubeRefusal && /one expression of no parameter/.test(e.message));
    assert.throws(() => readJoinHow({ form: 'append', pick: 3 }), /an append's pick is an expression in Pure/);
    assert.equal(joinWords({ form: 'append', pick: lambda([], fn('toUpper', lit.string('a'))) }), 'the rows of the one an expression names');
  });

  it('a grid it opens writes its pick as the page holds it, `$name`, never its value now (heldHow)', () => {
    const held: JoinHow = { form: 'append', pick: lambda([], variable('source')) };
    assert.equal(heldHow(held, { form: 'append', pick: lambda([], lit.string('trades')) }), held);
    // a join's: what opening reconciled (a kept column gone) taken, the rest as held
    const on = { rows: [{ left: 'region', op: '==' as const, right: 'region' }] };
    const join: JoinHow = { form: 'join', kind: 'left', on, prefix: 'b_', keep: ['region', 'gone'] };
    assert.deepEqual(heldHow(join, { ...join, keep: ['region'] }), { ...join, keep: ['region'] });
  });
});
