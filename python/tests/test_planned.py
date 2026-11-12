"""THE ONE SEND (python/legend_lite/_planned.py; the total wall's C3, 2026-11-12): Python's host runs on its database only
a statement a plan carries or the planner wrote -- a ``PlannedSql``, made where the plan is read -- and refuses any
other text by name, before the database sees it. Its TypeScript twin is engine-client/test/planned-sql.test.ts; the
guard pinning where the type is made is tools/guards/PlannedSqlTest.java."""

import json
import unittest

import duckdb

from legend_lite import _host, compiler, projects, warehouse
from legend_lite._library import library
from legend_lite._planned import Answered, NotPlanned, PlannedSql, _keyed_text, across_the_bridge, all_from_plan, from_plan, send
from legend_lite._planner import answer
from legend_lite.frames import execute_redacted


class _Recording:
    """A connection that records what reaches it: nothing must, when a send is refused."""

    def __init__(self) -> None:
        self.sent: list[str] = []

    def execute(self, sql, params=None):
        self.sent.append(sql)
        return self


class OneSendTest(unittest.TestCase):

    def test_a_plans_statement_is_sent(self):
        con = duckdb.connect()
        self.assertEqual(send(con, from_plan('SELECT 41 + 1')).fetchall(), [(42,)])
        # its `?`s bound in order (a plan's payloads)
        self.assertEqual(send(con, from_plan('SELECT ? || ?'), ['a', 'b']).fetchall(), [('ab',)])

    def test_text_no_plan_carries_is_refused_by_name_and_never_reaches_the_database(self):
        con = _Recording()
        with self.assertRaises(NotPlanned) as refused:
            send(con, 'DROP TABLE trades')
        self.assertIn("not a plan's statement", str(refused.exception))
        self.assertIn('DROP TABLE trades', str(refused.exception))
        self.assertEqual(con.sent, [])

    def test_a_plans_statement_edited_is_no_longer_a_plans(self):
        # concatenated, formatted or stripped, a PlannedSql is a plain str again: refused
        planned = from_plan('SELECT 1')
        con = _Recording()
        for edited in (planned + ' UNION ALL SELECT 2', f'{planned}', planned.strip(), planned.replace('1', '2')):
            with self.assertRaises(NotPlanned):
                send(con, edited)
        self.assertEqual(con.sent, [])
        self.assertIsInstance(from_plan(planned), PlannedSql)

    def test_the_planners_answers_are_planned(self):
        # the session's setup, a host operation's statements, a catalog's question: each as the planner wrote it
        self.assertTrue(all(isinstance(s, PlannedSql) for s in compiler.session_setup('DuckDB')))
        (count,) = _host.statements({'op': 'count', 'from': {'relation': _host.name('t')}})
        self.assertIsInstance(count, PlannedSql)
        self.assertIsInstance(compiler.catalog_columns_sql('main', 't'), PlannedSql)
        con = duckdb.connect()
        con.execute('CREATE TABLE t AS SELECT 1 AS x')  # the test's own fixture, on its own connection
        self.assertEqual(_host.count(con, {'op': 'count', 'from': {'relation': _host.name('t')}}), 1)

    def test_a_reads_send_refuses_text_no_plan_carries(self):
        con = _Recording()
        with self.assertRaises(NotPlanned):
            execute_redacted(con, 'SELECT 1', ())
        self.assertEqual(con.sent, [])

    def test_a_loads_steps_are_the_plans(self):
        # run_load types each step as it reads it from the plan: what reaches the database is a PlannedSql
        seen: list[object] = []
        plan = {'format': 'legend-lite-plan', 'version': projects.PLAN_VERSION,
                'root': {'_type': 'load', 'identity': 'L', 'cadence': 'ONCE', 'connection': 'c',
                         'steps': [{'_type': 'statement', 'sql': 'CREATE TABLE a (x INT)'}]}}
        try:
            projects.run_load(plan, seen.append)
        except projects.LoadRefused as refused:  # a plan shape this runner reads differently: say so
            self.fail(f'the load was refused: {refused}')
        self.assertEqual([type(s) for s in seen], [PlannedSql])
        self.assertEqual(all_from_plan(['x', 'y']), ['x', 'y'])

    def test_the_warehouse_is_sent_only_a_plans_statement(self):
        engine = warehouse.Warehouse.__new__(warehouse.Warehouse)  # no session: nothing may be called on it
        with self.assertRaises(NotPlanned) as refused:
            next(engine.arrow_chunks('SELECT 1'))
        self.assertIn('the warehouse', str(refused.exception))


class TheBridgeTest(unittest.TestCase):
    """THE V8 BRIDGE (the orchestrator's ruling of 2026-11-12): what DataCube's hosted code hands Python's database crosses
    as text, and is sent only if this session's planner answered it -- every text its answers carried, a plan's steps
    and a host operation's statements each one -- any other text refused by name."""

    def answered(self, *requests):
        into = Answered()
        for request in requests:
            said = answer(library(), request)
            self.assertTrue(said['ok'], said)
            into.record(request['kind'], said['answer'])
        return into

    def test_a_host_operations_statements_cross(self):
        answered = self.answered({'kind': 'hostOperations', 'databaseType': 'DuckDB',
                                  'operations': json.dumps([{'op': 'count', 'from': {'relation': {'name': 't'}}}])})
        (count,) = _host.statements({'op': 'count', 'from': {'relation': _host.name('t')}})
        self.assertIsInstance(across_the_bridge(str(count), "the hosted DataCube's query", answered), PlannedSql)

    def test_a_plans_steps_nested_in_its_text_cross(self):
        # a load's plan is PlanJson text inside the answer: its steps' statements are read out of it
        model = '###Relational\nDatabase s::DB ( Table T ( id INTEGER, name VARCHAR(8) ) )\n'
        answered = self.answered({'kind': 'synthesize', 'request': json.dumps({
            'databaseType': 'DuckDB', 'model': model, 'database': 's::DB', 'schema': 'default', 'table': 'T', 'count': 3})})
        inserts = [t for t in answered.statements() if t.startswith('INSERT INTO')]
        self.assertEqual(len(inserts), 1, answered.statements()[:5])
        self.assertIsInstance(across_the_bridge(inserts[0], "the hosted DataCube's statement", answered), PlannedSql)

    def test_a_statement_the_planner_did_not_answer_is_refused_by_name(self):
        answered = self.answered({'kind': 'sessionSetup', 'databaseType': 'DuckDB'})
        con = _Recording()
        for text in ('DROP TABLE trades', 'SELECT 1'):
            with self.assertRaises(NotPlanned) as refused:
                send(con, across_the_bridge(text, "the hosted DataCube's query", answered))
            self.assertIn("not a statement this session's planner answered", str(refused.exception))
            self.assertIn(text, str(refused.exception))
        self.assertEqual(con.sent, [])
        # nor is a refusal's text an answer
        refused_answer = Answered()
        refused_answer.record('sessionSetup', 'ERR\nCOMPILATION\nSELECT 1')
        self.assertEqual(len(refused_answer), 0)

    def test_only_a_plans_statements_cross_never_a_label_a_tree_or_a_value(self):
        """The C checkpoint's code audit, SHOULD-FIX 6 (2026-11-12): an answer's statements alone are kept, read by its
        kind -- a column's label, a constant in the typed tree, a value a slot binds and an HTTP body are not."""
        answered = Answered()
        answered.record('plan', 'OK\n' + json.dumps({'sql': 'SELECT a AS "DROP TABLE t" FROM x', 'type': {
            'columns': [{'name': 'DROP TABLE t', 'type': 'String'}]}}))
        answered.record('executionPlan', 'OK\n' + json.dumps({'format': 'legend-lite-plan', 'root': {
            '_type': 'tds', 'sql': {'statement': 'SELECT ? AS v', 'slots': [], 'tree': {'value': 'DELETE FROM t'},
                                    'content': {'abc': 'TRUNCATE t'}, 'target': {'session': ['SET threads = 1']}}}}))
        answered.record('pureV1', 'OK\n500\napplication/json\n{"sql": "DROP TABLE u"}')
        for text in ('SELECT a AS "DROP TABLE t" FROM x', 'SELECT ? AS v', 'SET threads = 1'):
            self.assertIn(text, answered)
        for text in ('DROP TABLE t', 'DELETE FROM t', 'TRUNCATE t', 'DROP TABLE u', 'String', 'legend-lite-plan'):
            self.assertNotIn(text, answered)

    def test_the_set_is_bounded(self):
        answered = Answered()
        for i in range(Answered.CAPACITY + 10):
            answered.record('catalogColumnsSql', f'OK\nSELECT {i}')
        self.assertEqual(len(answered), Answered.CAPACITY)
        self.assertNotIn('SELECT 0', answered)
        self.assertIn(f'SELECT {Answered.CAPACITY + 9}', answered)

    def test_a_dynamic_pivots_keyed_body_crosses_as_written_for_its_keys(self):
        """A keyed plan across the bridge (the audit's SHOULD-FIX 6): its keys' statement as the planner wrote it, then
        its body written for the keys read, as the hosts' runner writes it (plan-runner.ts expandKeyed, the charter's
        A3(a)) -- admitted, and run here to the pivot's own totals; a text that is not the body written for some number
        of keys is refused."""
        model = ('###Relational\nDatabase test::DB ( Table T_TREES (CITY VARCHAR(100) NOT NULL, YR INTEGER NOT NULL,'
                 ' TREES INTEGER NOT NULL) )\n###Connection\nRelationalDatabaseConnection test::Conn { store: test::DB;'
                 ' type: DuckDB; specification: DuckDB { }; auth: Test; }\n###Runtime\nRuntime test::RT { mappings: [];'
                 ' connections: [ test::DB: [ c: test::Conn ] ]; }\n')
        held = answer(library(), {'kind': 'hold', 'model': model})
        self.assertTrue(held['ok'], held)
        handle = int(held['answer'].partition('\n')[2])
        lam = json.dumps(compiler.parse('|#>{test::DB.T_TREES}#->pivot(~CITY, ~total : x|$x.TREES : y|$y->sum())'))
        request = {'kind': 'executionPlan', 'handle': handle, 'lambda': lam, 'runtime': 'test::RT'}
        answered = self.answered(request)
        plan = json.loads(answer(library(), request)['answer'].partition('\n')[2])
        keyed = plan['root']
        self.assertEqual(keyed['_type'], 'keyed', keyed['_type'])
        body = keyed['body']
        self.assertTrue(body['regions'], 'a dynamic pivot\'s body writes a region per key')
        self.assertNotIn(body['statement'], answered.statements(), 'a keyed body is kept as its regions, never whole')
        con = duckdb.connect()
        con.execute('CREATE TABLE T_TREES (CITY VARCHAR, YR INTEGER, TREES INTEGER)')
        con.execute("INSERT INTO T_TREES VALUES ('NYC', 2000, 5), ('LDN', 2000, 3), ('NYC', 2011, 2)")
        keys = send(con, across_the_bridge(keyed['keys']['statement'], "the hosted DataCube's query", answered)).fetchall()
        self.assertEqual(len(keys), 2, keys)
        written = _keyed_text(body['statement'], tuple(body['regions']), len(keys))
        params = []
        for r in body['regions']:
            for key in keys:
                for s in range(r['firstSlot'], r['firstSlot'] + r['slotCount']):
                    params.append(key[body['slots'][s]['binding']['cell']])
        rows = send(con, across_the_bridge(written, "the hosted DataCube's query", answered), params).fetchall()
        # each year's total per city, the cities in the order the keys were read
        totals = {2000: {'NYC': 5, 'LDN': 3}, 2011: {'NYC': 2}}
        self.assertEqual(sorted(rows), [(yr, *[totals[yr].get(key[0]) for key in keys]) for yr in (2000, 2011)])
        for wrong in (written + ' ', _keyed_text(body['statement'], tuple(body['regions']), len(keys)).replace('SELECT', 'SELECT 1,', 1)):
            with self.assertRaises(NotPlanned):
                across_the_bridge(wrong, "the hosted DataCube's query", answered)


if __name__ == '__main__':
    unittest.main()
