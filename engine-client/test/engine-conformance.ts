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
  type Capabilities, type Engine, type EngineError, type EngineSession, type Handle, type QueryAsk, type ResultColumn,
  type ProjectAt, type ProjectCoordinates, type RevisionCoordinates, type SourceAnswer,
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
  /** A CSV of `id,name` an upload carries, and how many rows it holds. */
  readonly upload: { readonly name: string; readonly text: string; readonly rows: number };
  /** A text a secret is made to hold (a bearer URL's query): it must never come back, anywhere the suite reads. */
  readonly sentinel: string;
  /** A file the engine already holds, by the handle it issued (an engine that offers `held`). */
  readonly held?: Handle;
  /** Where a remote file the engine reads is served (`<base>/remote.csv`, `upload`'s rows; `<base>/gone.csv` is not):
   *  absent where the engine's database makes no request from this test (DuckDB-WASM in Node), and the remote cases
   *  then say they did not run, and why. */
  readonly served?: { readonly base: string };
  /** A project the engine resolves by its coordinates (Q19), as its model home serves it: its runtime and mapping,
   *  a query of its class, how many rows its own Data holds, and its text (which, sent as a model, R3a refuses). */
  readonly project?: {
    readonly coordinates: ProjectCoordinates;
    readonly runtime: string;
    readonly mapping: string;
    readonly query: string;
    readonly rows: number;
    readonly text: string;
    readonly cls: string;
    /** The same project's workspace at a revision (Q23), its text as that revision's files join. */
    readonly revision?: { readonly coordinates: RevisionCoordinates; readonly text: string };
  };
}

/** One case's outcome: passed, failed (its error), or not run here (why: its host's test pins each). */
export interface CaseResult {
  readonly name: string;
  readonly ok: boolean;
  readonly error?: string;
  readonly notRun?: string;
}

/** A case that cannot run against this fixture, by its reason: reported, never passed silently. */
class NotRun extends Error {}

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

const offers = (caps: Capabilities, kind: 'upload' | 'held' | 'url' | 'remote' | 'query'): boolean =>
  caps.calls.addSource?.sources.includes(kind) ?? false;
const over = (a: SourceAnswer, query: Lambda['body'][number] = a.relation): QueryAsk =>
  ({ model: a.model, runtime: a.runtime, query: lambda([], query) });
const bytes = (text: string): Uint8Array => new TextEncoder().encode(text);
/** Nothing the suite reads holds the sentinel. */
const clean = (f: ConformanceFixture, what: string, value: unknown): void =>
  check(!JSON.stringify(value ?? null).includes(f.sentinel), `${what} holds the secret`);

const SOURCES: readonly Case[] = [
  {
    name: 'C14 an upload: a handle the engine issued, described, queried over the model the engine wrote, dropped',
    async run(_e, s, caps, f) {
      if (!offers(caps, 'upload')) {
        await rejects(() => s.addSource({ upload: { name: f.upload.name, bytes: bytes(f.upload.text) } }, 'snap'), 'refused', 'an upload, not offered', 'upload');
        return;
      }
      const added = await s.addSource({ upload: { name: f.upload.name, bytes: bytes(f.upload.text) } }, 'snap');
      check(added.handle.handle.length > 0 && added.executionId.length > 0, 'a handle and an execution id');
      equal(added.size, bytes(f.upload.text).length, 'its size');
      check(!added.model.includes('###Connection') && !added.model.includes('###Runtime'), 'R3a (c): its model names its runtime alone');
      equal((await s.describe(added.handle)).columns.map((c) => c.name), ['id', 'name'], 'its columns');
      equal((await rowsOf(s, over(added))).count, f.upload.rows, 'its rows');
      await rejects(() => s.addSource({ upload: { name: f.upload.name, bytes: bytes(f.upload.text) } }, 'live'), 'refused', 'a file added live', 'live');
      await s.drop(added.handle);
      await rejects(() => s.describe(added.handle), 'refused', 'a dropped handle', added.handle.handle);
      await rejects(() => rowsOf(s, over(added)), 'refused', "a dropped source's runtime", added.runtime);
    },
  },
  {
    name: 'C15 a held file by the handle the engine issued; a foreign or path-shaped handle refused by name',
    async run(_e, s, caps, f) {
      if (!offers(caps, 'held')) {
        await rejects(() => s.addSource({ held: { handle: 'held-none' } }, 'snap'), 'refused', 'a held file, not offered', 'held');
        return;
      }
      if (f.held === undefined) throw new NotRun('the engine offers held files, and the fixture holds none');
      const added = await s.addSource({ held: f.held }, 'snap');
      equal((await rowsOf(s, over(added))).count, f.upload.rows, "the held file's rows");
      for (const handle of ['held-none', '/etc/passwd']) await rejects(() => s.addSource({ held: { handle } }, 'snap'), 'refused', `the handle '${handle}'`, handle);
      await s.drop(added.handle);
    },
  },
  {
    name: 'C16 bindSecret is write-only: it answers nothing, a bad field refused without its value, a secret unknown by name refused',
    async run(_e, s, caps, f) {
      if (!caps.calls.bindSecret) {
        await rejects(() => s.bindSecret('s', { URL: f.sentinel }), 'refused', 'bindSecret, not offered', 'bindSecret');
        return;
      }
      equal(await s.bindSecret('conformance_url', { URL: `https://example.invalid/remote.csv?sig=${f.sentinel}` }), undefined, 'its answer');
      const bad = await rejects(() => s.bindSecret('bad', { PASSWORD: f.sentinel } as never), 'refused', 'a field no secret has', 'PASSWORD');
      clean(f, 'the refusal of a bad field', { message: bad.message });
      if (offers(caps, 'remote')) {
        await rejects(() => s.addSource({ remote: { kind: 'bearer', ref: 'https://example.invalid/x.csv', secret: 'no_such_secret' } }, 'live'), 'refused', 'an unknown secret', 'no_such_secret');
      }
    },
  },
  {
    name: "C17 a remote file by its secret's name, read live; its secret never echoed, the database's own words included; dropped with its session",
    async run(engine, s, caps, f) {
      if (!offers(caps, 'remote')) return;
      const served = f.served;
      if (served === undefined) throw new NotRun("the engine's database makes no request from this test: no served file to read");
      await s.bindSecret('remote_url', { URL: `${served.base}/remote.csv?sig=${f.sentinel}` });
      const added = await s.addSource({ remote: { kind: 'bearer', ref: `${served.base}/remote.csv`, secret: 'remote_url' } }, 'live');
      clean(f, 'the source', added);
      const answer = await s.query(over(added));
      clean(f, 'the explain text and receipt', { explain: answer.explain, receipt: answer.receipt });
      let n = 0;
      for await (const b of answer.rows) n += b.numRows;
      equal(n, f.upload.rows, "the remote file's rows");
      await s.bindSecret('gone_url', { URL: `${served.base}/gone.csv?sig=${f.sentinel}` });
      const e = await rejects(() => s.addSource({ remote: { kind: 'bearer', ref: `${served.base}/gone.csv`, secret: 'gone_url' } }, 'live'), 'run', 'a remote file that is not there');
      clean(f, "the database's words", { message: e.message });
      const other = await engine.open();
      try {
        await rejects(() => other.addSource({ remote: { kind: 'bearer', ref: `${served.base}/remote.csv`, secret: 'remote_url' } }, 'live'), 'refused', "another session's secret", 'remote_url');
      } finally {
        await other.close();
      }
      await s.drop(added.handle);
    },
  },
  {
    name: "C18 a query's rows snapped behind a handle (the pull, never SQL); a query's rows added live refused",
    async run(_e, s, caps, f) {
      if (!offers(caps, 'query')) return;
      const added = await s.addSource({ query: ask(f, lambda([], f.source)) }, 'snap');
      equal((await s.describe(added.handle)).columns.map((c) => ({ name: c.name, type: c.type })), f.columns.map((c) => ({ name: c.name, type: c.type })), 'its columns');
      equal((await rowsOf(s, over(added))).count, f.rowCount, 'its rows');
      await rejects(() => s.addSource({ query: ask(f, lambda([], f.source)) }, 'live'), 'refused', "a query's rows added live", 'live');
      await s.drop(added.handle);
    },
  },
  {
    name: "C25 a query's snap answers its rows' count (Q26): the engine's own, equal to the rows the copy holds",
    async run(_e, s, caps, f) {
      if (!offers(caps, 'query')) return;
      const added = await s.addSource({ query: ask(f, lambda([], f.source)) }, 'snap');
      try {
        equal(added.rowCount, f.rowCount, "the snap's count");
        equal((await rowsOf(s, over(added))).count, added.rowCount, 'the rows the copy holds');
      } finally {
        await s.drop(added.handle);
      }
    },
  },
  {
    name: 'C26 a source of a kind whose rows the engine did not count answers no rowCount -- never a guessed 0 (Q26)',
    async run(_e, s, caps, f) {
      if (!offers(caps, 'upload')) throw new NotRun('the engine offers no upload: no kind but a query to ask');
      const added = await s.addSource({ upload: { name: f.upload.name, bytes: bytes(f.upload.text) } }, 'snap');
      try {
        check(!('rowCount' in added), `an upload's answer carries no rowCount (it said ${JSON.stringify((added as { rowCount?: unknown }).rowCount)})`);
      } finally {
        await s.drop(added.handle);
      }
    },
  },
  {
    name: "C19 a closed session's sources are dropped with it: another session finds its runtime gone",
    async run(engine, _s, caps, f) {
      if (!offers(caps, 'upload')) return;
      const one = await engine.open();
      const added = await one.addSource({ upload: { name: f.upload.name, bytes: bytes(f.upload.text) } }, 'snap');
      await one.close();
      const two = await engine.open();
      try {
        await rejects(() => rowsOf(two, over(added)), 'refused', "a closed session's source", added.runtime);
      } finally {
        await two.close();
      }
    },
  },
];

// a library's functions and its enumerations (Q17, Q27): DataCube's own declares both
const LIBRARY = '###Pure\nEnum conformance::lib::Size\n{\n  small, large\n}\n\nfunction conformance::lib::twice(x: Integer[1]): Integer[1]\n{\n  $x * 2\n}\n';
// a function generic over relations (lite's declared extension S112), as DataCube's library writes them: upstream's grammar
// refuses its type parameter, lite's own dialect -- the planner's, and an engine's own reading (Q27) -- reads it
const GENERIC = '###Pure\nfunction conformance::ext::firstRows<T>(t: Relation<T>[1], n: Integer[1]): Relation<T>[1]\n{\n  $t->limit($n)\n}\n';
const NOT_A_LIBRARY = '###Pure\nClass conformance::lib::Row\n{\n  id: Integer[0..1];\n}\n';

const READING: readonly Case[] = [
  {
    name: "C20 a library beside the model: its functions called, an enumeration in it admitted; a library holding anything else refused by name (Q17, Q27)",
    async run(_e, s, caps, f) {
      if (!caps.calls.query) return;
      const query = lambda([], fn('extend', f.source, {
        _type: 'classInstance', type: 'colSpec',
        value: { name: 'twice', function1: lambda(['x'], fn('conformance::lib::twice', fn('toOne', { _type: 'property', property: f.keyColumn, parameters: [{ _type: 'var', name: 'x' }] } as unknown as ValueSpecification))) },
      } as unknown as ValueSpecification));
      for (let i = 0; i < 2; i += 1) equal((await rowsOf(s, { ...ask(f, query), library: LIBRARY })).count, f.rowCount, `the library's call, asked ${i + 1}`);
      await rejects(() => rowsOf(s, { ...ask(f, query), library: NOT_A_LIBRARY }), 'refused', 'a library declaring a class', 'conformance::lib::Row');
      await rejects(() => rowsOf(s, ask(f, query)), 'compile', 'a call of a library not sent');
    },
  },
  {
    name: "C27 a model using one of lite's declared extensions (a function generic over relations, S112) passes R3a and plans, read in lite's dialect (Q27)",
    async run(_e, s, caps, f) {
      if (!caps.calls.query) return;
      const query = lambda([], fn('conformance::ext::firstRows', f.source, lit.integer(2)));
      equal((await rowsOf(s, { ...ask(f, query), model: `${f.model}\n${GENERIC}` })).count, Math.min(2, f.rowCount), "the generic function's rows");
    },
  },
  {
    name: 'C21 modelOf: ONE model over several sources, one runtime the engine owns; a handle given twice or not this session\'s refused by name (Q15)',
    async run(engine, s, caps, f) {
      if (!caps.calls.modelOf || !offers(caps, 'upload')) return;
      const one = await s.addSource({ upload: { name: f.upload.name, bytes: bytes(f.upload.text) } }, 'snap');
      const two = await s.addSource({ upload: { name: f.upload.name, bytes: bytes(f.upload.text) } }, 'snap');
      const over = await s.modelOf([one.handle, two.handle]);
      check(!over.model.includes('###Connection') && !over.model.includes('###Runtime'), 'R3a (c): one model naming its runtime alone');
      check(over.runtime !== one.runtime && over.runtime !== two.runtime, 'a runtime of its own');
      equal(over.relations.length, 2, 'a relation per source');
      for (const r of over.relations) equal((await rowsOf(s, { model: over.model, runtime: over.runtime, query: lambda([], r) })).count, f.upload.rows, "each source's rows, read together");
      await rejects(() => s.modelOf([one.handle, one.handle]), 'refused', 'a handle given twice', 'twice');
      const other = await engine.open();
      try {
        await rejects(() => other.modelOf([one.handle]), 'refused', "another session's handle", one.handle.handle);
      } finally {
        await other.close();
      }
      await s.drop(one.handle);
      await s.drop(two.handle);
    },
  },
  {
    name: 'C22 a class read through a mapping (how.mapping): its rows as the source\'s',
    async run(_e, s, caps, f) {
      if (!caps.calls.query || !caps.calls.parse) return;
      const mapped = `${f.model}\n###Pure\nClass conformance::Row\n{\n  ${f.keyColumn}: Integer[0..1];\n}\n\n###Mapping\nMapping conformance::M\n(\n  conformance::Row: Relational\n  {\n    ~mainTable [${f.store}]${f.ownTable}\n    ${f.keyColumn}: [${f.store}]${f.ownTable}.${f.keyColumn}\n  }\n)\n`;
      const query = await s.parse(`|conformance::Row.all()->project(~[${f.keyColumn}: x|$x.${f.keyColumn}])`);
      equal((await rowsOf(s, { model: mapped, runtime: f.runtime, query, how: { mapping: 'conformance::M' } })).count, f.rowCount, "the class's rows");
    },
  },
  {
    name: "C23 a project by its coordinates (Q19): its rows its own; an unknown project, its text sent as a model, a mapping not its own -- each refused by name",
    async run(_e, s, caps, f) {
      if (!caps.calls.query || !caps.calls.parse) return;
      const p = f.project;
      if (p === undefined) throw new NotRun('the fixture names no project the engine resolves');
      const query = await s.parse(p.query);
      const over = (how: { mapping: string; project: ProjectAt }, model = ''): QueryAsk => ({ model, runtime: p.runtime, query, how });
      const how = { mapping: p.mapping, project: p.coordinates };
      equal((await rowsOf(s, over(how))).count, p.rows, "the project's own rows");
      equal((await s.typeOf(over(how))).columns.length > 0, true, "the project's query typed");
      const unknown = { ...p.coordinates, versionId: '999.0.0' };
      await rejects(() => rowsOf(s, over({ ...how, project: unknown })), 'refused', 'an unknown project', '999.0.0');
      await rejects(() => rowsOf(s, over(how, p.text)), 'refused', "a project's text sent as a model (R3a)", 'connection');
      await rejects(() => rowsOf(s, over({ ...how, mapping: 'conformance::NoSuchMapping' })), 'refused', 'a mapping not the project\'s', 'conformance::NoSuchMapping');
      if (caps.calls.elements) {
        const elements = (await s.elements({ project: p.coordinates })) as { _type?: string; package?: string; name?: string }[];
        check(elements.some((e) => `${e.package}::${e.name}` === p.cls), "the project's class among its elements");
        check(!elements.some((e) => e._type === 'connection' || e._type === 'runtime'), "no connection or runtime among them (the engine's)");
      }
    },
  },
  {
    name: "C24 a workspace's revision by its SDLC coordinates (Q23): typed over the engine's model home's files; unknown coordinates, the revision's text sent as a model -- each refused by name",
    async run(_e, s, caps, f) {
      if (!caps.calls.typeOf || !caps.calls.parse) return;
      const p = f.project;
      const r = p?.revision;
      if (p === undefined || r === undefined) throw new NotRun("the fixture names no workspace revision the engine's model home resolves");
      const query = await s.parse(p.query);
      const over = (project: RevisionCoordinates, model = ''): QueryAsk => ({ model, runtime: p.runtime, query, how: { mapping: p.mapping, project } });
      // its rows are the engine's own tables (the page that made the class opened them): typed here, never read
      equal((await s.typeOf(over(r.coordinates))).columns.map((c) => c.name).join(','), 'id,name', "the revision's query typed");
      const unknown = { ...r.coordinates, revision: 'no-such-revision' };
      await rejects(() => s.typeOf(over(unknown)), 'refused', 'an unknown revision', 'no-such-revision');
      await rejects(() => s.typeOf(over(r.coordinates, r.text)), 'refused', "a revision's text sent as a model (R3a)", 'connection');
      if (caps.calls.elements) {
        const elements = (await s.elements({ project: r.coordinates })) as { _type?: string; package?: string; name?: string }[];
        check(elements.some((e) => `${e.package}::${e.name}` === p.cls), "the revision's class among its elements");
        check(!elements.some((e) => e._type === 'connection' || e._type === 'runtime'), "no connection or runtime among them (the engine's)");
      }
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
    for (const c of [...CASES, ...WRITES, ...SOURCES, ...READING, CLOSED]) {
      try {
        await c.run(engine, session, caps, fixture);
        out.push({ name: c.name, ok: true });
      } catch (e) {
        if (e instanceof NotRun) {
          out.push({ name: c.name, ok: true, notRun: e.message });
          continue;
        }
        out.push({ name: c.name, ok: false, error: e instanceof Error ? `${e.name}: ${e.message}` : String(e) });
      }
    }
  } finally {
    await session.close();
  }
  return out;
}
