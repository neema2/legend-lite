// THE ENGINE CONFORMANCE SUITE (docs/DATACUBE_ENGINE_CONTRACT_2026_11_12.md R7): every engine behind DataCube's
// `Engine` is held to the same asks -- the same answers, or the same refusals by name. A plain async runner, no test
// framework, so the one suite runs unchanged in Node (the tab's engine, step 4) and inside Python's V8 (Python headless,
// over the bridge: python/tests/test_contract.py), each case's outcome handed back for its host's test to assert.
//
// The fixture is the ENGINE's: a source it serves and the model it wrote over it (Q3), never one the suite writes --
// so no case hands an engine a connection, a path or a statement.

import { colSpecs, fn, lambda, lit, type Lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import {
  ENGINE_CONTRACT_VERSION, isEngineError, refusalOf, requireContract,
  type Capabilities, type Engine, type EngineError, type EngineSession, type QueryAsk, type ResultColumn,
} from '../src/engine-contract.ts';

/** A source the engine serves, as the engine modelled it, and what the suite expects of it. */
export interface ConformanceFixture {
  readonly model: string;
  readonly runtime: string;
  /** The relation a query over the source starts from. */
  readonly source: ValueSpecification;
  /** Its columns as the compiler types them (Pure types). */
  readonly columns: readonly ResultColumn[];
  readonly rowCount: number;
  /** A text column of it, whose values do not read as integers: what makes the database refuse a cast at run. */
  readonly textColumn: string;
  /** A column whose values are unique: what a merge is keyed by. */
  readonly keyColumn: string;
  /** The source's own table (the engine's: a write to it is refused) and the Database its model declares. */
  readonly ownTable: string;
  readonly store: string;
  /** A table of the engine's database no source is: where the write cases write (made by the first). */
  readonly writeTarget: string;
}

/** One case's outcome. */
export interface CaseResult {
  readonly name: string;
  readonly ok: boolean;
  readonly error?: string;
}

interface Case {
  readonly name: string;
  run(engine: Engine, session: EngineSession, caps: Capabilities, f: ConformanceFixture): Promise<void>;
}

class Failed extends Error {}

function check(ok: boolean, what: string): void {
  if (!ok) throw new Failed(what);
}

function equal(actual: unknown, expected: unknown, what: string): void {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a !== e) throw new Failed(`${what}: expected ${e}, got ${a}`);
}

/** `work`'s rejection, required to be an engine's error of `kind` (and, refused, naming `naming`). */
async function rejects(work: () => Promise<unknown>, kind: EngineError['kind'], what: string, naming?: string): Promise<EngineError> {
  let caught: unknown;
  try {
    await work();
  } catch (e) {
    caught = e;
  }
  if (caught === undefined) throw new Failed(`${what}: it answered, and was to fail as ${kind}`);
  if (!isEngineError(caught)) throw new Failed(`${what}: it failed with no engine error: ${String(caught)}`);
  if (caught.kind !== kind) throw new Failed(`${what}: it failed as ${caught.kind} (${caught.message}), not ${kind}`);
  if (naming !== undefined && !caught.message.includes(naming)) throw new Failed(`${what}: its refusal does not name '${naming}': ${caught.message}`);
  return caught;
}

const ask = (f: ConformanceFixture, query: Lambda): QueryAsk => ({ model: f.model, runtime: f.runtime, query });

async function rowsOf(engine: EngineSession, a: QueryAsk): Promise<{ count: number; id: string; schema: readonly ResultColumn[] }> {
  const answer = await engine.query(a);
  let count = 0;
  for await (const batch of answer.rows) count += batch.numRows;
  return { count, id: answer.executionId, schema: answer.schema };
}

export const CASES: readonly Case[] = [
  {
    name: 'C1 capabilities: the contract version, and another version refused by name',
    async run(engine, _s, caps) {
      equal(caps.contract, ENGINE_CONTRACT_VERSION, 'the contract version');
      check(typeof caps.engine.name === 'string' && caps.engine.name.length > 0, 'the engine names itself');
      equal((await engine.capabilities()).contract, ENGINE_CONTRACT_VERSION, 'asked again');
      await rejects(async () => requireContract({ ...caps, contract: ENGINE_CONTRACT_VERSION + 1 }), 'refused',
        'a later contract version', `contract version ${ENGINE_CONTRACT_VERSION + 1}`);
    },
  },
  {
    name: 'C2 typeOf: the compiler\'s columns and Pure types; an ill-typed query a compile error',
    async run(_e, s, caps, f) {
      if (!caps.calls.typeOf) {
        await rejects(() => s.typeOf(ask(f, lambda([], f.source))), 'refused', 'typeOf, not offered', 'typeOf');
        return;
      }
      equal((await s.typeOf(ask(f, lambda([], f.source)))).columns.map((c) => ({ name: c.name, type: c.type })),
        f.columns.map((c) => ({ name: c.name, type: c.type })), 'the columns');
      await rejects(() => s.typeOf(ask(f, lambda([], fn('select', f.source, colSpecs(['no_such_column']))))), 'compile',
        'a column the source has not');
    },
  },
  {
    name: 'C3 query: Arrow rows typed by the compiler, a distinct execution id each run',
    async run(_e, s, caps, f) {
      if (!caps.calls.query) {
        await rejects(() => s.query(ask(f, lambda([], f.source))), 'refused', 'query, not offered', 'query');
        return;
      }
      const one = await rowsOf(s, ask(f, lambda([], f.source)));
      equal(one.count, f.rowCount, 'the rows');
      equal(one.schema.map((c) => ({ name: c.name, type: c.type })), f.columns.map((c) => ({ name: c.name, type: c.type })), 'the schema');
      check(one.id.length > 0, 'an execution id');
      const two = await rowsOf(s, ask(f, lambda([], f.source)));
      check(two.id !== one.id, `each run its own execution id (${one.id} twice)`);
    },
  },
  {
    name: 'C4 query: the row cap is in the Pure (cap + 1 asked, cap + 1 answered)',
    async run(_e, s, caps, f) {
      if (!caps.calls.query) return;
      check(f.rowCount >= 3, 'the fixture has at least 3 rows');
      equal((await rowsOf(s, ask(f, lambda([], fn('limit', f.source, lit.integer(2)))))).count, 2, 'a cap of 1, asked as 2');
    },
  },
  {
    name: 'C5 query: what the database refuses is a run error in its own words, with its execution id',
    async run(_e, s, caps, f) {
      if (!caps.calls.query) return;
      const bad = lambda([], fn('extend', f.source, {
        _type: 'classInstance', type: 'colSpec',
        value: { name: 'not_a_number', function1: lambda(['x'], fn('parseInteger', fn('toOne', { _type: 'property', property: f.textColumn, parameters: [{ _type: 'var', name: 'x' }] } as unknown as ValueSpecification))) },
      } as unknown as ValueSpecification));
      const e = await rejects(() => rowsOf(s, ask(f, bad)), 'run', 'a cast the database cannot make');
      check(e.message.length > 0, 'the database\'s words');
      check((e.executionId ?? '').length > 0, 'its execution id');
    },
  },
  {
    name: 'C6 a call not in the capabilities is refused by name before anything crosses',
    async run(_e, s, caps, f) {
      const stub: Capabilities = { ...caps, calls: { ...caps.calls, typeOf: false } };
      const r = refusalOf(stub, 'typeOf');
      check(r !== undefined && r.kind === 'refused' && r.message.includes('typeOf'), 'the client\'s own check bites');
      if (caps.calls.write === undefined) {
        await rejects(() => s.write({ box: ask(f, lambda([], f.source)), target: { table: 'conformance_out' }, mode: 'create' }),
          'refused', 'write, not offered', 'write');
      }
      if (caps.calls.addSource === undefined) {
        await rejects(() => s.addSource({ url: 'https://example.com/a.csv' }, 'live'), 'refused', 'addSource, not offered', 'addSource');
      }
    },
  },
  {
    name: 'C7 an unknown handle and a path-shaped one are refused by name; cancel of an unknown request is no error',
    async run(_e, s, caps) {
      for (const handle of ['h-no-such-handle', '/etc/passwd']) {
        await rejects(() => s.describe({ handle }), 'refused', `describe of '${handle}'`, caps.calls.describe ? handle : 'describe');
        await rejects(() => s.drop({ handle }), 'refused', `drop of '${handle}'`, caps.calls.drop ? handle : 'drop');
      }
      if (caps.calls.cancel) await s.cancel('no-such-request');
    },
  },
  {
    name: 'C8 the reading calls: parse, print, elements, compile-only',
    async run(_e, s, caps, f) {
      if (caps.calls.parse) {
        const parsed = await s.parse('|1 + 1');
        equal(parsed._type, 'lambda', 'a parse');
        await rejects(() => s.parse('|1 +'), 'compile', 'text that does not parse');
        if (caps.calls.print) check((await s.print(parsed)).includes('1 + 1'), 'a print of it');
      }
      if (caps.calls.elements) check((await s.elements(f.model)).length > 0, 'the model\'s elements');
    },
  },
];

/** The suite's planted violator of R3a: the fixture's model with a connection of its own (test input, never DataCube's). */
const withConnection = (f: ConformanceFixture): string =>
  `${f.model}\n###Connection\nRelationalDatabaseConnection conformance::Planted { store: ${f.store}; type: DuckDB; specification: DuckDB { }; auth: Test; }\n`;

const WRITES: readonly Case[] = [
  {
    name: 'C10 a model declaring its own connection is refused by name, the engine owning its runtimes (R3a)',
    async run(_e, s, caps, f) {
      if (caps.calls.typeOf) await rejects(() => s.typeOf({ ...ask(f, lambda([], f.source)), model: withConnection(f) }), 'refused', 'typeOf over a model with a connection', 'connection');
      await rejects(() => s.typeOf({ ...ask(f, lambda([], f.source)), runtime: 'conformance::NoSuchRuntime' }), 'refused', 'a runtime the engine does not own', 'conformance::NoSuchRuntime');
    },
  },
  {
    name: 'C11 write, one call: each of the five modes answers the rows the database wrote, with its execution id',
    async run(_e, s, caps, f) {
      if (caps.calls.write === undefined) {
        await rejects(() => s.write({ box: ask(f, lambda([], f.source)), target: { table: f.writeTarget }, mode: 'create' }), 'refused', 'write, not offered', 'write');
        return;
      }
      const box = ask(f, lambda([], f.source));
      const ids = new Set<string>();
      for (const mode of ['create', 'append', 'deleteAppend', 'overwrite', { merge: { keys: [f.keyColumn] } }] as const) {
        const done = await s.write({ box, target: { table: f.writeTarget }, mode });
        equal(done.rowsWritten, f.rowCount, `${JSON.stringify(mode)}'s rows`);
        check(done.executionId.length > 0 && !ids.has(done.executionId), `${JSON.stringify(mode)}: an execution id of its own`);
        ids.add(done.executionId);
      }
    },
  },
  {
    name: "C12 write's refusals: the engine's own table by name; a create of a table there already in the database's words; a box model with a connection",
    async run(_e, s, caps, f) {
      if (caps.calls.write === undefined) return;
      const box = ask(f, lambda([], f.source));
      await rejects(() => s.write({ box, target: { table: f.ownTable }, mode: 'append' }), 'refused', "a write to the engine's own table", f.ownTable);
      const e = await rejects(() => s.write({ box, target: { table: f.writeTarget }, mode: 'create' }), 'run', 'a create of a table there already');
      check((e.executionId ?? '').length > 0, 'its execution id');
      await rejects(() => s.write({ box: { ...box, model: withConnection(f) }, target: { table: f.writeTarget }, mode: 'append' }), 'refused', 'a write over a model with a connection', 'connection');
      await rejects(() => s.write({ box, target: { table: f.writeTarget }, mode: { merge: { keys: [] } } }), 'refused', 'a merge of no key');
    },
  },
  {
    name: "C13 a mode the capabilities omit is refused by name before anything crosses",
    async run(_e, _s, caps) {
      const stub: Capabilities = { ...caps, calls: { ...caps.calls, write: { modes: ['create'] } } };
      const r = refusalOf(stub, 'write', { mode: 'deleteAppend' });
      check(r !== undefined && r.message.includes("mode 'deleteAppend'"), 'the client refuses the mode by name');
    },
  },
];

/** A case run alone on a session of its own, then that session closed: and, closed, the session refuses a call. */
const CLOSED: Case = {
  name: 'C9 close: a closed session refuses its calls by name',
  async run(engine, _s, _caps, f) {
    const s = await engine.open();
    await s.close();
    await rejects(() => s.typeOf(ask(f, lambda([], f.source))), 'refused', 'a call on a closed session', 'closed');
  },
};

/** Every case run against `engine` over `fixture`: each one's outcome, in order. */
export async function runConformance(engine: Engine, fixture: ConformanceFixture): Promise<CaseResult[]> {
  const out: CaseResult[] = [];
  let caps: Capabilities;
  try {
    caps = requireContract(await engine.capabilities());
  } catch (e) {
    return [{ name: 'C0 the engine answers its capabilities', ok: false, error: e instanceof Error ? e.message : String(e) }];
  }
  const session = await engine.open();
  try {
    for (const c of [...CASES, ...WRITES, CLOSED]) {
      try {
        await c.run(engine, session, caps, fixture);
        out.push({ name: c.name, ok: true });
      } catch (e) {
        out.push({ name: c.name, ok: false, error: e instanceof Error ? `${e.name}: ${e.message}` : String(e) });
      }
    }
  } finally {
    await session.close();
  }
  return out;
}
