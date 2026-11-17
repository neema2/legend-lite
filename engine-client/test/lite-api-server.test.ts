// LITE API V1'S CONFORMANCE SUITE, THE SERVER BINDING (docs/records/lite-api-v1-2026-11-17.md §5, the user's go of
// 2026-11-17): each case of test/fixtures/lite-api-cases.json -- DataCube's suite, the one file every binding reads --
// asked of legend-lite's server (`//core:server`, started for this file) as one POST to `/api/lite/v1/<op>`, and its
// answer held to the case's: an exact answer equal, an included one held, a refusal its kind, in lite's one shape,
// answered with its kind's status (§3). The browser's operations (core's LiteApiConformanceTest) and Python's native
// entries are held to the same file by their own readers, so the three bindings cannot drift.

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';

import { runfileFromEnv } from '../../tools/js/runfiles.mts';
import { liteServer } from '../../query-store/test/lite-server.ts';
import { parseExact } from '../src/values.ts';
import { liteError, LiteRefusal, type LiteErrorKind } from '../src/lite-error.ts';
import { LiteServer } from '../src/doors/lite-server.ts';

const server = liteServer();

interface Case {
  readonly name: string;
  readonly op: string;
  readonly request: unknown;
  readonly answer?: unknown;
  readonly answerIncludes?: unknown;
  readonly refusal?: { readonly kind: LiteErrorKind };
}

/** The status a refusal of each kind is answered with over HTTP (spec §3). */
const STATUS: Readonly<Record<LiteErrorKind, number>> = {
  parse: 400, compile: 400, unsupported: 400, request: 400, parameters: 400, data: 422, internal: 500,
};

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

  for (const c of fixture.cases) {
    it(c.name, async () => {
      // the request as written in the file, its numbers exact
      const body = JSON.stringify(c.request, (_k, v: unknown) => (typeof v === 'bigint' ? `__big__${v}` : v))
        .replace(/"__big__(-?\d+)"/g, '$1');
      const res = await fetch(`${server.api()}/lite/v1/${c.op}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body,
      });
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
      if (c.answer !== undefined) assert.deepEqual(answer, c.answer, text);
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
