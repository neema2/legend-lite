// A class as a grid's source (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §16.1): which properties a grid over a class
// shows, read from the project's elements; the class's rows as the grid reads them, a page filter put in before its
// columns are taken; and the whole of it through the tab's own module -- a class made by the writer from a grid's
// source, read back by its class source and planned over the project's files, its rows from DuckDB.

import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';
import { openDatabase } from '#test-host';

import { accessor, eq, from, lambda, lit, property, toJson, variable } from '../../pure-protocol/src/index.ts';
import { projectText } from '../../model-changes/src/project.ts';
import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import { classGiven, classLambda, classOpened, classProperties, classRelation, type ClassElement } from '../src/class-source.ts';
import { CubeController } from '../src/cube.ts';
import { rawRows, type ClassSource } from '../src/cube-document.ts';
import { gridLens } from '../src/page-filters.ts';
import type { ClassAction } from '../src/model-writer.ts';
import { plannerFor } from './catalog-builder.ts';

type Property = NonNullable<ClassElement['properties']>[number];
const STRING: Property['genericType'] = { rawType: { _type: 'packageableType', fullPath: 'String' } };
const ONE: Property['multiplicity'] = { lowerBound: 1, upperBound: 1 };
const prop = (name: string, genericType = STRING, multiplicity = ONE): Property => ({ name, genericType, multiplicity });

const ELEMENTS: ClassElement[] = [
  { _type: 'class', package: 'a', name: 'Party', properties: [prop('name'), prop('id')] },
  {
    _type: 'class', package: 'a', name: 'Trade', superTypes: [{ fullPath: 'a::Party' }],
    properties: [
      prop('region'),
      prop('notional', { rawType: { _type: 'packageableType', fullPath: 'Float' } }, { lowerBound: 0, upperBound: 1 }),
      prop('tags', STRING, { lowerBound: 0 }),
      prop('book', { rawType: { _type: 'packageableType', fullPath: 'a::Book' } }),
      prop('side', { rawType: { _type: 'packageableType', fullPath: 'a::Side' } }),
      // a supertype's property declared again: the class's own, once
      prop('name'),
    ],
  },
  { _type: 'class', package: 'a', name: 'Book', properties: [prop('code')] },
  { _type: 'Enumeration', package: 'a', name: 'Side' },
];

describe("where a new grid's class is read: the one rule every editor names it by (classGiven: K16, K17)", () => {
  const MODEL = { project: 'p:q', workspace: 'datacube', package: 'p', tables: {}, revision: 'r1' };
  it("a class of the page's model by its name or path, at its pin or a revision named; another project's by its path, at a revision", () => {
    const at = { class: 'p::domain::Trade', project: 'p:q', workspace: 'datacube', revision: 'r1', mapping: 'p::mapping::Model', runtime: 'p::runtime::Browser' };
    assert.deepEqual(classGiven(MODEL, { class: 'Trade' }, 'Browser'), at);
    assert.deepEqual(classGiven(MODEL, { class: 'p::domain::Trade', revision: 'r2' }, 'Browser'), { ...at, revision: 'r2' });
    assert.deepEqual(classGiven(null, { class: 'o::domain::Book', project: 'o:r', revision: 'v3' }, 'Python'), {
      class: 'o::domain::Book', project: 'o:r', workspace: 'datacube', revision: 'v3', mapping: 'o::mapping::Model', runtime: 'o::runtime::Python',
    });
  });

  it('refused in words', () => {
    assert.equal(classGiven(MODEL, {}, 'Browser'), 'a class is named by its path or its name, not undefined');
    assert.match(classGiven(null, { class: 'Trade' }, 'Browser') as string, /the page has no model of its own/);
    assert.match(classGiven(MODEL, { class: 'Book', project: 'o:r', revision: 'v3' }, 'Browser') as string, /is named by its path as DataCube's writer makes one/);
    assert.match(classGiven(MODEL, { class: 'o::domain::Book', project: 'o:r' }, 'Browser') as string, /is read at a revision of it/);
    const { revision: _r, ...unpinned } = MODEL;
    assert.match(classGiven(unpinned, { class: 'Trade' }, 'Browser') as string, /has no saved revision yet/);
  });
});

describe('the properties a grid over a class shows', () => {
  it('its own to-one properties of a type a column has, then its supertypes\', in order, each once', () => {
    const { shown, left } = classProperties(ELEMENTS, 'a::Trade');
    assert.deepEqual(shown, ['region', 'notional', 'name', 'id']);
    assert.deepEqual(left, ['tags (it holds many values)', 'book (it is a Book)', 'side (it is an enumeration (Side))']);
  });

  it('refused by name: a class the project has not, and one with nothing a grid shows', () => {
    assert.throws(() => classProperties(ELEMENTS, 'a::Nope'), /the project has no class a::Nope/);
    const only = [{ _type: 'class', package: 'a', name: 'Bag', properties: [prop('items', STRING, { lowerBound: 0 })] }];
    assert.throws(() => classProperties(only, 'a::Bag'), /Bag has no property a grid can show: items \(it holds many values\)/);
  });

  it('a supertype that is its own supertype is walked once', () => {
    const loop: ClassElement[] = [
      { _type: 'class', package: 'a', name: 'X', superTypes: ['a::Y'], properties: [prop('x')] },
      { _type: 'class', package: 'a', name: 'Y', superTypes: ['a::X'], properties: [prop('y')] },
    ];
    assert.deepEqual(classProperties(loop, 'a::X').shown, ['x', 'y']);
  });
});

/** The grid's source as the tab's table model writes it: its own `local::DB`. */
const SOURCE = `
###Relational
Database local::DB ( Table trades ( trade_id INTEGER NOT NULL, region VARCHAR(10), notional DOUBLE ) )
###Connection
RelationalDatabaseConnection local::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }
###Runtime
Runtime local::RT { mappings: []; connections: [ local::DB: [ c1: local::Conn ] ]; }
`;
const TRADE: ClassAction = {
  query: from(accessor('local::DB', 'trades')).lambda(),
  source: { model: SOURCE, runtime: 'local::RT' },
  name: 'Trade',
  columns: {},
  where: { from: 'local::DB', as: 'Browser' },
};

describe('a class made by the writer, read back by its class source', () => {
  const writer = plannerFor('', 'local::RT');
  let engine: DuckDbEngine;

  before(async () => {
    const db = await openDatabase();
    engine = new DuckDbEngine(db.connection);
    await engine.run('CREATE TABLE trades (trade_id INTEGER NOT NULL, region VARCHAR, notional DOUBLE)', 0);
    await engine.run("INSERT INTO trades VALUES (1, 'EMEA', 100.5), (2, 'AMER', 20), (3, 'EMEA', NULL), (4, 'APAC', 7)", 0);
  });

  after(async () => {
    await engine?.close();
  });

  it('its properties from the project\'s elements; its rows, filtered before its columns are taken, from the table', async () => {
    const made = await writer.createClass('', 'trades', TRADE);
    const cls = made.classes[0]!;
    const text = projectText(made.changes.map((c) => ({ pureCode: c.pureCode ?? '' })));
    // the project's files at the pin, planned by the host's compiler through the class's mapping and runtime
    const planner = plannerFor(text, cls.runtime, { mapping: cls.mapping });
    try {
      const { shown } = classProperties(await planner.modelElements(text) as ClassElement[], cls.class);
      assert.deepEqual(shown, ['tradeId', 'region', 'notional']);
      const types = await planner.relationType(classLambda(cls.class, shown));
      assert.deepEqual(types.map((c) => c.name), ['tradeId', 'region', 'notional'], 'a column per property, under its own name');
      const where = eq(property(variable('x'), 'region'), lit.string('EMEA'));
      const query = classLambda(cls.class, shown, where);
      // the same query as the grammar reads it: `Trade.all()->filter(x|...)->project(~[...])`
      const parsed = await planner.parse(
        `|${cls.class}.all()->filter(x|$x.region == 'EMEA')->project(~[tradeId: x|$x.tradeId, region: x|$x.region, notional: x|$x.notional])`);
      assert.equal(toJson(query), toJson(parsed));
      const plan = await planner.plan(query);
      const rows = await engine.run(plan.sql, 0);
      const ids = rows.columns[0]!.values.map(Number).sort();
      assert.deepEqual(ids, [1, 3], 'the EMEA trades, the filter on the class');
    } finally {
      planner.dispose();
    }
  });

  it("opened by classOpened -- the one reading the tab's own class grid (sources.ts openedClass) and the host's (host-tables.ts) take -- its columns the class's properties, its rows the table's (L12: class)", async () => {
    const made = await writer.createClass('', 'trades', TRADE);
    const cls = made.classes[0]!;
    const files = made.changes.map((c) => ({ pureCode: c.pureCode ?? '' }));
    const planner = plannerFor(projectText(files), cls.runtime, { mapping: cls.mapping });
    try {
      const at = { class: cls.class, project: 'p:q', workspace: 'datacube', revision: '1', mapping: cls.mapping, runtime: cls.runtime };
      const opened = await classOpened(at, files, async (text) => await planner.modelElements(text) as ClassElement[]);
      assert.deepEqual([opened.shown, opened.left], [['tradeId', 'region', 'notional'], []]);
      assert.equal(toJson(opened.source), toJson(classRelation(cls.class, ['tradeId', 'region', 'notional'])));
      const rows = await engine.run((await planner.plan(lambda([], opened.source))).sql, 0);
      assert.deepEqual(rows.columns[0]!.values.map(Number).sort(), [1, 2, 3, 4], 'every trade, read through its mapping');
    } finally {
      planner.dispose();
    }
  });

  it('a grid over it, given the page\'s lens: its queries read the class\'s rows filtered, its own state unchanged', async () => {
    const made = await writer.createClass('', 'trades', TRADE);
    const cls = made.classes[0]!;
    const text = projectText(made.changes.map((c) => ({ pureCode: c.pureCode ?? '' })));
    const planner = plannerFor(text, cls.runtime, { mapping: cls.mapping });
    try {
      const columns = [{ name: 'tradeId', type: 'Integer' }, { name: 'region', type: 'String' }, { name: 'notional', type: 'Float' }];
      const source: ClassSource = { _type: 'class', name: 'Trade', class: cls.class, project: 'p', workspace: 'w', revision: '1',
        mapping: cls.mapping, runtime: cls.runtime, columns };
      const snapshot = rawRows(classRelation(cls.class, columns.map((c) => c.name)), columns);
      const controller = new CubeController(engine, planner);
      const all = await controller.level(snapshot);
      assert.equal(all.rowCount, 4);
      // the page's filter on Trade: notional over 10, as the filter bar or a chart's click sets it
      const { lens } = gridLens([{ id: 'filter-1', class: cls.class,
        condition: { kind: 'condition', column: 'notional', operator: 'greaterThan', value: 10 } }], source, new Set());
      controller.setLens(lens);
      const some = await controller.level(snapshot);
      assert.deepEqual(some.columns[0]!.values.map(Number).sort(), [1, 2], 'the trades over 10 (a null notional is none)');
      assert.equal(JSON.stringify(snapshot.source.query), JSON.stringify(classRelation(cls.class, columns.map((c) => c.name))),
        'the grid\'s own source is its class, unfiltered');
      controller.setLens(undefined);
      assert.equal((await controller.level(snapshot)).rowCount, 4, 'taken off: every row again');
    } finally {
      planner.dispose();
    }
  });
});
