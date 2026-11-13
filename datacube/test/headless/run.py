"""The headless lane (//datacube:headless_test; docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md §6): DataCube's own
non-screen tests run in V8 inside Python -- each test file's bundle in a Headless of its own, its planner answered by
legend-lite's native library and its database Python's DuckDB, no WebAssembly -- and the host itself, over the
product's bundle: its start-up check, and what it refuses.

The bundles are //datacube:headless_test_bundles (the test files, bundled with the `headless` condition and the
node:test and node:assert stand-ins); the product's is //datacube:headless_bundle.
"""

import base64
import io
import json
import os
import sys
import time
import unittest
from pathlib import Path

from python.runfiles import runfiles

# every file a runfiles path, resolved by the runfiles library before legend_lite reads one (python/tests/main.py's
# rule): never the working directory, which this lane moves into each test's own folder
RUNFILES = runfiles.Create()


def located(name: str) -> Path:
    path = RUNFILES.Rlocation(os.environ[name])
    if not path or not Path(path).exists():
        sys.exit(f'{name}: {os.environ[name]} is not in the runfiles')
    return Path(path)


os.environ['LEGEND_LITE_LIBRARY'] = str(located('LEGEND_LITE_LIBRARY'))
BUNDLES = located('HEADLESS_TEST_BUNDLES')
PRODUCT = located('HEADLESS_BUNDLE').read_text(encoding='utf-8')
TMP = Path(os.environ['TEST_TMPDIR'])

from legend_lite._headless import KINDS, OVER, REFUSED, Headless, InterfaceMismatch, check_interface  # noqa: E402 -- after the library's path
from legend_lite._library import library  # noqa: E402

# the product host's calls (calls.ts HostCalls): a test's own database is none of them
PRODUCT_CALLS = ['planner', 'query', 'statement', 'digest', 'fetch', 'engine']


class TestHost(Headless):
    """THE LANE'S HOST (2026-11-12, the total wall): the product's, and a test's own database beside it
    (test/host/headless.ts) -- a test runs its own SQL on Python's DuckDB as written (a fixture's table, a planner's
    statement it checks), which the product's host never takes (its bridge admits only what its planner answered,
    _planned.py across_the_bridge); and registers a file its database reads by name. Its calls are the lane's, on the
    same global; the product's bundle has none of them."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # the product's statements that crossed the product's bridge (the C final audit's H7, 2026-11-12): a test's own
        # go to the test calls, every other one to the product's `query` and `statement`, checked there as the
        # bundle's are -- counted, so a lane that stopped crossing it is red (DataCubeTests)
        self.crossed = 0

    def _calls(self):
        return {**super()._calls(), 'testQuery': self._test_query, 'testStatement': self._test_statement,
                'testRegisterFile': self._test_register_file}

    async def _planner(self, request):
        """A host operation's texts a test typed itself (test/host/headless.ts `testsOwn`): its fake planner's answers,
        recorded as the session's planner's before the request is checked (the re-audit's S3). The lane's alone: the
        product's host reads no such field."""
        asked = json.loads(request)
        for text in asked.pop('testsOwn', []):
            self.answered.record('plan', 'OK\n' + json.dumps({'sql': text}))
        return await super()._planner(json.dumps(asked))

    async def _query(self, sql):
        answered = await super()._query(sql)
        self.crossed += 1
        return answered

    async def _statement(self, sql, params):
        answered = await super()._statement(sql, params)
        self.crossed += 1
        return answered

    def _arrow(self, result):
        import pyarrow as pa

        table = result.to_arrow_table() if result.description is not None else pa.table({})
        sink = io.BytesIO()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        return base64.b64encode(sink.getvalue()).decode('ascii')

    async def _test_query(self, sql):
        return self._arrow(self._db._con.execute(sql))

    async def _test_statement(self, sql, params):
        return self._arrow(self._db._con.execute(sql, json.loads(params)))

    async def _test_register_file(self, name, data):
        root = self.folder
        target = (root / name).resolve()
        if root not in target.parents:
            raise ValueError(f"a registered file stays in the engine's folder: {name!r} does not")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(base64.b64decode(data))


class Host(unittest.TestCase):
    """The host over the product's bundle."""

    def test_the_bundle_and_the_library_agree_on_every_request_kind(self):
        with Headless(PRODUCT, folder=TMP) as h:
            kinds = h.evaluate('legendLiteHeadless.REQUEST_KINDS')
        # the host's table, and the kinds it refuses by name (REFUSED: the authored door, Studio's alone, 2026-11-12)
        self.assertEqual(sorted(kinds), sorted([*KINDS, *REFUSED]), "the bundle's kinds are the host's table's and its refusals'")
        for kind, (function, _args) in [*KINDS.items(), *OVER.items()]:
            self.assertTrue(library().exports(function), f'{kind}: the library exports {function}')

    def test_a_kind_the_host_does_not_answer_fails_at_start_naming_both_sides(self):
        with self.assertRaisesRegex(InterfaceMismatch, r"the bundle sends \[.*'novel'.*\]; this host answers"):
            check_interface([*KINDS, 'novel'], library())

    def test_there_is_no_webassembly_and_the_web_layer_is_there(self):
        with Headless(PRODUCT, folder=TMP) as h:
            self.assertEqual(h.evaluate('typeof WebAssembly'), 'undefined')
            self.assertEqual(h.evaluate("[typeof TextEncoder, typeof URL, typeof AbortController, typeof crypto.subtle.digest]"),
                             ['function'] * 4)

    def test_a_plan_through_the_worker_runs_on_the_database(self):
        model = ('###Relational\nDatabase a::DB ( Table t ( x INTEGER, s VARCHAR(8) ) )\n###Connection\n'
                 'RelationalDatabaseConnection a::C { store: a::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }\n'
                 '###Runtime\nRuntime a::RT { mappings: []; connections: [ a::DB: [ c: a::C ] ]; }')
        with TestHost(PRODUCT, folder=TMP) as h:
            # the test's own table, through its own host's call; the planner's statement through the product's
            h.evaluate("legendLiteHost.testQuery(\"CREATE TABLE t AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) v(x, s)\")")
            h.execute("globalThis.planned = (async (model) => {"
                      " const w = legendLite.worker();"
                      " const ask = (m) => new Promise((ok) => { w.onmessage = (e) => ok(e.data); w.postMessage(m); });"
                      " const held = await ask({ id: 1, kind: 'hold', model });"
                      " const handle = Number(held.answer.split('\\n')[1]);"
                      " const plan = await ask({ id: 2, kind: 'plan', handle, query: '|#>{a::DB.t}#->filter(r|$r.x > 1)->select(~[s])', runtime: 'a::RT' });"
                      " const refused = await ask({ id: 3, kind: 'plan', handle, query: '|#>{a::DB.t}#', runtime: 'a::NoSuch' });"
                      " const db = legendLite.database.connection;"
                      " const t = await db.query(JSON.parse(plan.answer.slice(3)).sql);"
                      " const rows = []; for (let i = 0; i < t.numRows; i++) rows.push(t.getChildAt(0).get(i));"
                      " return { rows, refused: [refused.ok, refused.answer.split('\\n')[0]] };"
                      f" }})({json.dumps(model)});")
            self.assertEqual(h.evaluate('planned'), {'rows': ['b', 'c'], 'refused': [True, 'ERR']},
                             "planned by the native library, run on Python's DuckDB; a refusal is an answer, not a failure")

    def test_every_timeout_fires_as_a_browsers_does(self):
        # HTML's setTimeout: no delay, 0 and a negative one are all 0 (mini-racer's own throws or never fires for them,
        # https://github.com/bpcreech/PyMiniRacer/issues/97: the host holds each to a millisecond)
        with Headless(PRODUCT, folder=TMP) as h:
            fired = h.evaluate("new Promise((done) => { const got = [];"
                               " const note = (k) => () => { got.push(k); if (got.length === 4) done(got.sort()); };"
                               " setTimeout(note('none')); setTimeout(note('zero'), 0); setTimeout(note('negative'), -5);"
                               " setTimeout(note('ten'), 10); })")
            self.assertEqual(fired, ['negative', 'none', 'ten', 'zero'])
            self.assertEqual(h.evaluate("new Promise((done) => { const t = setTimeout(() => done('fired'), 5);"
                                        " clearTimeout(t); setTimeout(() => done('cleared'), 20); })"), 'cleared')

    def test_a_call_past_the_memory_cap_fails_and_the_engine_takes_no_more(self):
        with Headless(PRODUCT, folder=TMP, memory_limit=256 * 1024 * 1024) as h:
            with self.assertRaises(MemoryError):
                h.evaluate('(() => { const keep = []; for (;;) keep.push(new Array(1e6).fill(1.5)); })()')
            self.assertTrue(h.broken)
            with self.assertRaisesRegex(RuntimeError, 'ran out of its memory'):
                h.evaluate('1')

    def test_a_call_that_never_answers_times_out_and_the_next_is_answered(self):
        with Headless(PRODUCT, folder=TMP) as h:
            with self.assertRaises(TimeoutError):
                h.evaluate('new Promise(() => {})', timeout=0.5)
            self.assertEqual(h.evaluate('1 + 1'), 2)

    def test_a_registered_file_stays_in_the_folder(self):
        with TestHost(PRODUCT, folder=TMP / 'files') as h:
            refused = h.evaluate("legendLiteHost.testRegisterFile('../out.csv', btoa('a')).then(() => 'written', (e) => String(e))")
            self.assertIn("stays in the engine's folder", refused)
            self.assertEqual(h.evaluate("legendLiteHost.testRegisterFile('in.csv', btoa('a\\n1\\n')).then(() => 'written')"), 'written')
        self.assertEqual((TMP / 'files' / 'in.csv').read_text(), 'a\n1\n')

    def test_the_products_host_hands_the_planner_no_sql_in_a_host_operation(self):
        # the final audit's item 1 (2026-11-12; until then the re-audit's S3 admitted a text the planner had answered): a
        # host operation takes no SQL, so the product's host refuses one carrying any -- whatever else the request names
        # (the lane's own `testsOwn` is its TestHost's alone)
        with Headless(PRODUCT, folder=TMP) as h:
            said = h.evaluate("(async () => { const w = legendLite.worker();"
                              " const ask = (m) => new Promise((ok) => { w.onmessage = (e) => ok(e.data); w.postMessage(m); });"
                              " return ask({ id: 1, kind: 'hostOperations', databaseType: 'DuckDB', testsOwn: ['DROP TABLE t'],"
                              " operations: JSON.stringify([{ op: 'count', from: { query: 'DROP TABLE t' } }]) }); })()")
            self.assertFalse(said['ok'])
            self.assertIn('takes no SQL, and its query is SQL', said['error'])

    def test_the_hosted_codes_http_to_a_sql_api_crosses_the_bridge(self):
        # the re-audit's S9 (2026-11-12): a fetch whose JSON carries a statement (a warehouse's submit) is refused by
        # name unless the session's planner answered it -- before any connection is made (the port below is closed)
        with Headless(PRODUCT, folder=TMP) as h:
            said = h.evaluate("legendLiteHost.fetch('POST', 'http://127.0.0.1:9/sql/v1/statements',"
                              " JSON.stringify({ sql: 'DROP TABLE t', catalog: 'c' })).then(() => 'sent', (e) => String(e))")
            self.assertIn("not a statement this session's planner answered", said)

    def test_a_tests_own_database_is_no_call_of_the_products(self):
        # 2026-11-12: the tests' setup SQL is the lane's own host's; the product's host takes only what its planner
        # answered, and refuses a test's statement by name
        with Headless(PRODUCT, folder=TMP) as h:
            self.assertEqual(h.evaluate('Object.keys(legendLiteHost)'), PRODUCT_CALLS)
            refused = h.evaluate("legendLite.database.connection.query('CREATE TABLE u (x INTEGER)').then(() => 'run', (e) => String(e))")
            self.assertIn("not a statement this session's planner answered", refused)
        with TestHost(PRODUCT, folder=TMP) as h:
            self.assertEqual(h.evaluate('Object.keys(legendLiteHost)'),
                             [*PRODUCT_CALLS, 'testQuery', 'testStatement', 'testRegisterFile'])


class DataCubeTests(unittest.TestCase):
    """Each test file's bundle, run in a Headless of its own: every test passes."""

    def test_every_bundle(self):
        bundles = sorted(BUNDLES.glob('*.test.js'))
        self.assertGreater(len(bundles), 40, f'the bundles are not there: {BUNDLES}')
        total = {'pass': 0, 'fail': 0, 'skip': 0}
        failed: list[str] = []
        crossed = 0
        for bundle in bundles:
            name = bundle.name[:-len('.test.js')]
            folder = TMP / name
            folder.mkdir(parents=True, exist_ok=True)
            # the database writes a relative file where the process stands: the test's own folder, never the runfiles
            os.chdir(folder)
            start = time.monotonic()
            with self.subTest(name):
                with TestHost(bundle.read_text(encoding='utf-8'), folder=folder) as h:
                    results = h.evaluate('legendLiteHeadless.run()')
                crossed += h.crossed
                counts = {s: sum(1 for r in results if r['status'] == s) for s in total}
                for s in total:
                    total[s] += counts[s]
                print(f'{name:22} {counts["pass"]:4} pass {counts["fail"]:3} fail {counts["skip"]:3} skip'
                      f'  ({time.monotonic() - start:.1f} s)', flush=True)
                bad = [f"{name}: {r['name']}: {r.get('error', '')}" for r in results if r['status'] == 'fail']
                failed += bad
                self.assertGreater(len(results), 0, f'{name}: no test ran')
                self.assertEqual(bad, [])
                # none skipped: a test the lane does not run says why in BUILD (its file left out), never by a skip
                self.assertEqual([r['name'] for r in results if r['status'] == 'skip'], [], f'{name}: skipped')
        print(f'\n{len(bundles)} files: {total["pass"]} pass, {total["fail"]} fail, {total["skip"]} skip; '
              f"{crossed} of the product's statements crossed its bridge", flush=True)
        # the lane exercises the product's bridge (H7): the product's statements are checked there, as the bundle's are.
        # Measured 2026-11-12: 205 crossed (0 with every statement sent to the test calls, as before H7); the floor
        # leaves the tests room to change
        self.assertGreater(crossed, 150, "the product's statements no longer cross the product's bridge: the lane's "
                                          'database sends a test\'s own statements alone to the test calls '
                                          '(test/host/headless.ts)')
        if failed:
            print('\n'.join(failed[:50]), file=sys.stderr)


if __name__ == '__main__':
    unittest.main(verbosity=2)
