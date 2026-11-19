// LITE API V1'S CONFORMANCE SUITE, THE SERVER BINDING (docs/records/lite-api-v1-2026-11-17.md §5, the user's go of
// 2026-11-17): each case of test/fixtures/lite-api-cases.json -- DataCube's suite, the one file every binding reads --
// asked of legend-lite's server (`//core:server`, started for this file) as one POST to `/api/lite/v1/<op>` -- the
// store's reads as a GET of the record by its id (`queries/get`, `queries/dataCube/get`) -- and its
// answer held to the case's: an exact answer equal, an included one held, a refusal its kind, in lite's one shape,
// answered with its kind's status (§3). The browser's operations (core's LiteApiConformanceTest) and Python's native
// entries are held to the same file by their own readers, so the three bindings cannot drift.

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';

import { runfileFromEnv } from '../../tools/js/runfiles.mts';
import { liteServer } from '../../query-store/test/lite-server.ts';
import { parseExact } from '../src/values.ts';
import { liteError, LITE_ERROR_STATUS, LiteRefusal, type LiteErrorKind } from '../src/lite-error.ts';
import { LiteServer } from '../src/doors/lite-server.ts';

const server = liteServer();

interface Case {
  readonly name: string;
  readonly op: string;
  readonly request: unknown;
  /** The bindings the case is asked of (browser, server, python); none named, every one. */
  readonly bindings?: readonly string[];
  readonly answer?: unknown;
  readonly answerIncludes?: unknown;
  /** Each key the answer holds and its JSON type (string, number, boolean, object, array): a value the host's own. */
  readonly answerShape?: Readonly<Record<string, string>>;
  readonly refusal?: { readonly kind: LiteErrorKind };
}

const BINDINGS = new Set(['browser', 'server', 'python']);

/** Whether the case is the server binding's: it names no bindings, or names `server`. A binding that is none of the
 *  three fails by name: a typo would skip the case silently everywhere. A case naming the server is asked of it, and an
 *  operation the server does not answer fails by name (its refusal is no answer). */
function servedHere(c: Case): boolean {
  if (c.bindings === undefined) return true;
  for (const b of c.bindings) {
    if (!BINDINGS.has(b)) throw new Error(`the case '${c.name}' names a binding that is none of browser, server, python: ${b}`);
  }
  return c.bindings.includes('server');
}

/** Whether `answer` holds each key `shape` names, of the JSON type it names (the Java reader's `shaped`). */
function shaped(shape: Readonly<Record<string, string>>, answer: unknown): boolean {
  if (answer === null || typeof answer !== 'object' || Array.isArray(answer)) return false;
  const a = answer as Record<string, unknown>;
  return Object.entries(shape).every(([key, type]) => {
    const v = a[key];
    switch (type) {
      case 'string': return typeof v === 'string';
      case 'number': return typeof v === 'number' || typeof v === 'bigint';
      case 'boolean': return typeof v === 'boolean';
      case 'object': return v !== null && typeof v === 'object' && !Array.isArray(v);
      case 'array': return Array.isArray(v);
      default: throw new Error(`a shape's type this reader does not know: ${type}`);
    }
  });
}

/** The status a refusal of each kind is answered with over HTTP (spec §3): the product's own, generated (H4's P2). */
const STATUS = LITE_ERROR_STATUS;

/** Lite's operations, each one POST of its request to `/api/lite/v1/<op>` (spec §2). */
const POSTED: ReadonlySet<string> = new Set(['model/check', 'model/elements', 'model/createClass', 'model/createAssociation',
  'model/createDataSpace', 'query/parse', 'query/print', 'query/type', 'query/plan', 'query/run', 'ts/item',
  'ts/statement', 'user']);

/** The store's reads (the user, 2026-11-17 ~23:31: a record that does not exist is notFound): a GET of the record by
 *  its id, under the store's path. */
const READS: Readonly<Record<string, string>> = { 'queries/get': 'queries', 'queries/dataCube/get': 'queries/dataCube' };

/** How the server binding asks the case's operation: its URL under `/api/lite/v1` and its request. An operation this
 *  reader does not know fails by name -- a new one is taught here, never asked as a guess. */
function asked(c: Case, body: string): { readonly path: string; readonly init: RequestInit } {
  if (POSTED.has(c.op)) {
    return { path: c.op, init: { method: 'POST', headers: { 'Content-Type': 'application/json' }, body } };
  }
  const under = READS[c.op];
  if (under !== undefined) {
    const request = c.request as Record<string, unknown> | null;
    const keys = request === null || typeof request !== 'object' ? [] : Object.keys(request);
    if (keys.length !== 1 || typeof request?.['id'] !== 'string') {
      throw new Error(`the case '${c.name}': a ${c.op} request is {id}, a string: ${JSON.stringify(c.request)}`);
    }
    return { path: `${under}/${encodeURIComponent(request['id'] as string)}`, init: { method: 'GET' } };
  }
  throw new Error(`the case '${c.name}' asks ${c.op}, an operation this reader does not know: teach it here`);
}

/** Whether `actual` holds `expected`, as the Java reader asks it: each key it names equal, recursively; each array item
 *  it names held by some item of the actual array; any other value equal. */
function includes(expected: unknown, actual: unknown): boolean {
  if (Array.isArray(expected)) {
    return Array.isArray(actual) && expected.every((e) => actual.some((a) => includes(e, a)));
  }
  if (expected !== null && typeof expected === 'object') {
    if (actual === null || typeof actual !== 'object' || Array.isArray(actual)) return false;
    const a = actual as Record<string, unknown>;
    return Object.entries(expected as Record<string, unknown>).every(([k, v]) => k in a && includes(v, a[k]));
  }
  return expected === actual;
}

// the cases, their numbers exact (2^63-1 a bigint, as the answers are read)
const fixture = parseExact(readFileSync(runfileFromEnv('LITE_API_CASES'), 'utf8')) as { readonly cases: readonly Case[] };

describe("lite API v1's conformance suite over legend-lite's server", () => {
  it('holds cases', () => {
    assert.ok(fixture.cases.length > 0, 'the suite holds cases');
  });

  it('an operation the reader does not know fails by name (a planted one); a store read is a GET by its id', () => {
    assert.throws(() => asked({ name: 'planted', op: 'query/guess', request: {} }, '{}'),
      /the case 'planted' asks query\/guess, an operation this reader does not know/);
    assert.deepEqual(asked({ name: 'read', op: 'queries/dataCube/get', request: { id: 'a b' } }, ''),
      { path: 'queries/dataCube/a%20b', init: { method: 'GET' } });
    assert.throws(() => asked({ name: 'read', op: 'queries/get', request: { id: 1 } }, ''), /a queries\/get request is \{id\}/);
  });

  it('a case naming a binding none of the three is refused by name (a planted typo), never skipped', () => {
    const typo: Case = { name: 'planted', op: 'user', request: {}, bindings: ['sever'] };
    assert.throws(() => servedHere(typo), /the case 'planted' names a binding that is none of browser, server, python: sever/);
    assert.equal(servedHere({ ...typo, bindings: ['browser', 'python'] }), false);
    assert.equal(servedHere({ ...typo, bindings: ['server'] }), true);
  });

  // each case's bindings read before any is asked: a name none of the three fails the suite by name
  for (const c of fixture.cases.filter(servedHere)) {
    it(c.name, async () => {
      // the request as written in the file, its numbers exact
      const body = JSON.stringify(c.request, (_k, v: unknown) => (typeof v === 'bigint' ? `__big__${v}` : v))
        .replace(/"__big__(-?\d+)"/g, '$1');
      const { path, init } = asked(c, body);
      const res = await fetch(`${server.api()}/lite/v1/${path}`, init);
      const text = await res.text();
      if (c.refusal !== undefined) {
        // a refusal case names its kind alone (§3: the kind is the contract, the words the product's), answered with
        // its kind's status and in lite's one shape
        const refusal = liteError(JSON.parse(text));
        assert.equal(refusal.kind, c.refusal.kind, text);
        assert.equal(res.status, STATUS[c.refusal.kind], text);
        assert.ok(includes(c.refusal, refusal), text);
        return;
      }
      assert.equal(res.status, 200, text);
      const answer = parseExact(text);
      if (c.answerShape !== undefined) assert.ok(shaped(c.answerShape, answer), text);
      else if (c.answer !== undefined) assert.deepEqual(answer, c.answer, text);
      else assert.ok(includes(c.answerIncludes, answer), text);
    });
  }
});

// THE SERVER BINDING'S OWN: query/run (the browser binding plans and never runs, so the shared file holds no run). The
// TS client (LiteServer) asks the one server for the same rows in each format: Arrow, as the apps ask (spec §4), and
// JSON. Both answers must be the same typed rows: an Integer's 2^63-1 is exact (a bigint) in each, a StrictDate its
// text, the shape and each column's Pure type read from the Arrow stream's metadata as the JSON answer states them.

/** A project with its own Data rows, which the server loads into the in-memory DuckDB it makes (LiteV1PlanningTest's). */
const PARTY = `Class proj::Party { id: Integer[1]; name: String[1]; joined: StrictDate[1]; }
###Relational
Database proj::PartyDB ( Schema PARTY ( Table PARTY (ID BIGINT PRIMARY KEY, NAME VARCHAR(200), JOINED DATE) ) )
###Mapping
Mapping proj::PartyMapping
(
  *proj::Party: Relational
  {
    ~primaryKey ([proj::PartyDB] PARTY.PARTY.ID)
    ~mainTable [proj::PartyDB] PARTY.PARTY
    id: [proj::PartyDB] PARTY.PARTY.ID,
    name: [proj::PartyDB] PARTY.PARTY.NAME,
    joined: [proj::PartyDB] PARTY.PARTY.JOINED
  }
)
###Connection
RelationalDatabaseConnection proj::Conn { store: proj::PartyDB; type: DuckDB; specification: DuckDB { }; auth: Test; }
###Runtime
Runtime proj::RT { mappings: [ proj::PartyMapping ]; connections: [ proj::PartyDB: [ db: proj::Conn ] ]; }
###Data
Data proj::PartyData
{
  Relational
  #{
    PARTY.PARTY:
      'ID,NAME,JOINED\n1,Meridian Capital,2021-03-04\n9223372036854775807,Halberd Securities,2022-05-06\n';
  }#
}
`;

const RUNTIME = 'proj::RT';
const SINCE = '{since: StrictDate[1]|proj::Party.all()->filter(p|$p.joined >= $since)'
  + '->project(~[id: p|$p.id, name: p|$p.name, joined: p|$p.joined])->sort(~id->ascending())'
  + '->from(proj::PartyMapping, proj::RT)}';
const UP_TO = '{n: Integer[1]|proj::Party.all()->filter(p|$p.id <= $n)'
  + '->project(~[id: p|$p.id])->sort(~id->ascending())->from(proj::PartyMapping, proj::RT)}';

describe("query/run on legend-lite's server through the TS client, in Arrow and in JSON", () => {
  const arrow = (): LiteServer => new LiteServer(server.api());
  const json = (): LiteServer => new LiteServer(server.api(), globalThis.fetch.bind(globalThis), 'json');

  it('the same typed rows in each format: the shape, each column its Pure type, the cells exact', async () => {
    const query = await arrow().queryParse(SINCE);
    const expected = {
      shape: 'RELATION',
      columns: [{ name: 'id', type: 'Integer' }, { name: 'name', type: 'String' }, { name: 'joined', type: 'StrictDate' }],
      rows: [[1, 'Meridian Capital', '2021-03-04'], [9223372036854775807n, 'Halberd Securities', '2022-05-06']],
    };
    assert.deepEqual(await json().queryRun(PARTY, query, RUNTIME, { since: '2020-01-01' }), expected);
    assert.deepEqual(await arrow().queryRun(PARTY, query, RUNTIME, { since: '2020-01-01' }), expected);
  });

  it('an Integer parameter in the 64-bit range is bound exact, 2^63-1 given as a bigint', async () => {
    const query = await arrow().queryParse(UP_TO);
    for (const server of [arrow(), json()]) {
      const answer = await server.queryRun(PARTY, query, RUNTIME, { n: 9223372036854775807n });
      assert.deepEqual(answer.rows, [[1], [9223372036854775807n]]);
    }
  });

  it("a refusal is lite's, thrown as a LiteRefusal with its kind's status: a parameter missing is kind parameters", async () => {
    const query = await arrow().queryParse(SINCE);
    for (const server of [arrow(), json()]) {
      await assert.rejects(server.queryRun(PARTY, query, RUNTIME, {}),
        (e: unknown) => e instanceof LiteRefusal && e.kind === 'parameters' && e.status === 400 && /since/.test(e.message));
    }
  });
});
