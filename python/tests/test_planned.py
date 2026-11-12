"""THE ONE SEND (python/legend_lite/_planned.py; the total wall's C3, 2026-11-12): Python's host runs on its database only
a statement a plan carries or the planner wrote -- a ``PlannedSql``, made where the plan is read -- and refuses any
other text by name, before the database sees it. Its TypeScript twin is engine-client/test/planned-sql.test.ts; the
guard pinning where the type is made is tools/guards/PlannedSqlTest.java."""

import json
import unittest

import duckdb

from legend_lite import _host, compiler, projects, warehouse
from legend_lite._library import library
from legend_lite._planned import NotPlanned, PlannedSql, across_the_bridge, all_from_plan, answered_texts, from_plan, send
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
        into: set[str] = set()
        for request in requests:
            said = answer(library(), request)
            self.assertTrue(said['ok'], said)
            answered_texts(said['answer'], into)
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
        inserts = [t for t in answered if t.startswith('INSERT INTO')]
        self.assertEqual(len(inserts), 1, sorted(answered)[:5])
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
        refused_answer: set[str] = set()
        answered_texts('ERR\nCOMPILATION\nSELECT 1', refused_answer)
        self.assertEqual(refused_answer, set())


if __name__ == '__main__':
    unittest.main()
