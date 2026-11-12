"""THE ONE SEND (python/legend_lite/_planned.py; the total wall's C3, 2026-11-12): Python's host runs on its database only
a statement a plan carries or the planner wrote -- a ``PlannedSql``, made where the plan is read -- and refuses any
other text by name, before the database sees it. Its TypeScript twin is engine-client/test/planned-sql.test.ts; the
guard pinning where the type is made is tools/guards/PlannedSqlTest.java."""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from legend_lite import _host, compiler, projects, warehouse
from legend_lite._planned import Database, NotPlanned, PlannedSql, all_from_plan, from_plan, send
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
        con = Database(duckdb.connect())
        self.assertEqual(send(con, from_plan('SELECT 41 + 1')).fetchall(), [(42,)])
        # its `?`s bound in order (a plan's payloads)
        self.assertEqual(send(con, from_plan('SELECT ? || ?'), ['a', 'b']).fetchall(), [('ab',)])

    def test_text_no_plan_carries_is_refused_by_name_and_never_reaches_the_database(self):
        con = _Recording()
        with self.assertRaises(NotPlanned) as refused:
            send(Database(con), 'DROP TABLE trades')
        self.assertIn("not a plan's statement", str(refused.exception))
        self.assertIn('DROP TABLE trades', str(refused.exception))
        self.assertEqual(con.sent, [])

    def test_a_plans_statement_edited_is_no_longer_a_plans(self):
        # concatenated, formatted or stripped, a PlannedSql is a plain str again: refused
        planned = from_plan('SELECT 1')
        con = _Recording()
        for edited in (planned + ' UNION ALL SELECT 2', f'{planned}', planned.strip(), planned.replace('1', '2')):
            with self.assertRaises(NotPlanned):
                send(Database(con), edited)
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
        self.assertEqual(_host.count(Database(con), {'op': 'count', 'from': {'relation': _host.name('t')}}), 1)

    def test_the_hosts_database_offers_no_other_way_to_run_text_or_load_data(self):
        """The C checkpoint's second audit (2026-11-12): DuckDB's own ways to run text or load data are not reachable
        through the Database a module holds, and the one send refuses a raw connection."""
        db = Database(duckdb.connect())
        for way in ('execute', 'executemany', 'sql', 'query', 'from_query', 'from_arrow', 'from_df', 'read_csv',
                    'read_parquet', 'read_json', 'table', 'view', 'install_extension', 'load_extension'):
            self.assertFalse(hasattr(db, way), f'a Database offers {way}')
        result = send(db, from_plan('SELECT 1'))
        for way in ('execute', 'sql', 'query', 'cursor', 'from_arrow'):
            self.assertFalse(hasattr(result, way), f'a statement\'s answer offers {way}')
        with self.assertRaises(NotPlanned) as refused:
            send(duckdb.connect(), from_plan('SELECT 1'))
        self.assertIn("runs on the host's Database", str(refused.exception))
        with self.assertRaises(NotPlanned):
            db.import_stream(None, None, 't; DROP TABLE x')

    def test_a_reads_send_refuses_text_no_plan_carries(self):
        con = _Recording()
        with self.assertRaises(NotPlanned):
            execute_redacted(Database(con), 'SELECT 1', ())
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


class HostRunnersTest(unittest.TestCase):
    """WHAT DATACUBE'S HOSTED CODE ASKS, PLANNED AND RUN BY PYTHON (the #6 bridge, the user's go of 2026-11-12): a load's
    file and bound steps and a write's program, run by Python's runners (projects.py) on plans Python's planner made --
    the tab's runner's rules and words (plan-runner.ts). Until then DataCube's code ran them in V8 and handed Python each
    statement, admitted by the set its planner had answered (Answered, across_the_bridge: deleted)."""

    def setUp(self):
        self.raw = duckdb.connect()
        self.db = Database(self.raw)
        self.dir = Path(self.enterContext(tempfile.TemporaryDirectory()))

    def file(self, name, text):
        path = self.dir / name
        path.write_text(text, encoding='utf-8')
        data = path.read_bytes()
        return projects.HostFile(name, hashlib.sha256(data).hexdigest(), len(data), str(path))

    def test_a_files_load_reads_the_file_the_plan_names_its_path_bound_never_written(self):
        held = self.file('upload_t.csv', 'id,name\n1,a\n2,b\n')
        plan = compiler.file_load({'databaseType': 'DuckDB', 'file': {'name': held.name, 'digest': held.digest,
                                                                      'size': held.size, 'format': 'csv'},
                                   'into': {'table': 't'}})
        self.assertNotIn(held.at, json.dumps(plan), 'the path is bound, never in the plan')
        projects.run_host_load(plan, self.db, files=(held,))
        self.assertEqual(self.raw.execute('SELECT id, name FROM t ORDER BY id').fetchall(), [(1, 'a'), (2, 'b')])

    def test_a_file_the_load_does_not_hold_is_refused_by_name_and_nothing_is_read(self):
        held = self.file('upload_u.csv', 'id\n1\n')
        plan = compiler.file_load({'databaseType': 'DuckDB', 'file': {'name': held.name, 'digest': held.digest,
                                                                      'size': held.size, 'format': 'csv'},
                                   'into': {'table': 'u'}})
        other = projects.HostFile(held.name, '0' * 64, held.size, held.at)
        with self.assertRaises(projects.LoadRefused) as refused:
            projects.run_host_load(plan, self.db, files=(other,))
        self.assertEqual(str(refused.exception), f"the file '{held.name}' ({held.size} bytes of digest {held.digest}) is "
                                                 'not one this load holds')
        with self.assertRaisesRegex(projects.LoadRefused, "this run was given no host's files"):
            projects.run_host_load(plan, self.db)
        self.assertEqual(self.raw.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'u'").fetchone(),
                         (0,))

    def test_a_bound_step_binds_what_the_host_holds_by_name_and_keeps_a_connections_setting(self):
        plan = compiler.remote_mount({'databaseType': 'DuckDB', 'view': 'r', 'format': 'parquet',
                                      'location': {'bearer': 'datacube_url_r', 'shown': 'https://b.example/r.parquet'}})
        sentinel = 'https://b.example/r.parquet?sig=SENTINEL-held'
        self.assertNotIn('SENTINEL', json.dumps(plan))
        sent = []

        class Recording(Database):
            def _run(self, sql, params):
                sent.append((str(sql), tuple(params or ())))
                return super()._run(sql, params) if str(sql).startswith('SET VARIABLE') else None

        ran = projects.run_host_load(plan, Recording(self.raw), secrets={'datacube_url_r': {'URL': sentinel}})
        bound = [p for sql, p in sent if sql.startswith('SET VARIABLE')]
        self.assertEqual(bound, [(sentinel,)], 'the bearer URL bound, as the host holds it')
        self.assertTrue(all('SENTINEL' not in sql for sql, _ in sent))
        self.assertEqual([s['binds'] for s in ran.settings], [[{'_type': 'held', 'name': 'datacube_url_r', 'field': 'URL'}]])
        self.assertIsInstance(ran.settings[0]['sql'], PlannedSql)
        with self.assertRaisesRegex(projects.LoadRefused, "'datacube_url_r' is not a secret this host holds"):
            projects.run_host_load(plan, Recording(self.raw), secrets={})

    MODEL = ('###Relational\nDatabase a::DB ( Table SRC ( ID INTEGER, NAME VARCHAR(8) ) Table DST ( ID INTEGER, '
             'NAME VARCHAR(8) ) )\n###Connection\nRelationalDatabaseConnection a::C { store: a::DB; type: DuckDB; '
             'specification: DuckDB { }; auth: Test; }\n###Runtime\nRuntime a::RT { mappings: []; connections: '
             '[ a::DB: [ c: a::C ] ]; }\n')

    def test_a_writes_program_runs_in_its_callers_transaction_answering_the_databases_count(self):
        self.raw.execute("CREATE TABLE SRC (ID INTEGER, NAME VARCHAR); INSERT INTO SRC VALUES (1, 'a'), (2, 'b')")
        self.raw.execute('CREATE TABLE DST (ID INTEGER, NAME VARCHAR)')
        plan = compiler.execution_plan(self.MODEL, compiler.parse('|#>{a::DB.SRC}#->write(#>{a::DB.DST}#)'), 'a::RT')

        def run(sql, params):
            result = send(self.db, sql, params)
            return result.fetchall() if result.description is not None else []

        self.raw.execute('BEGIN')
        self.assertEqual(projects.run_plan(plan, run, 'DuckDB'), '[{"value":2}]')
        self.raw.execute('ROLLBACK')
        self.assertEqual(self.raw.execute('SELECT count(*) FROM DST').fetchone(), (0,), "the caller's transaction held it")
        with self.assertRaisesRegex(projects.PlanRefused, 'a plan of a DuckDB database is not run on this Postgres one'):
            projects.run_plan(plan, run, 'Postgres')

    def test_a_keyed_plan_is_a_querys_never_a_writes(self):
        # a dynamic pivot's keyed plan is read by the host's plan-and-run, never handed to a write's runner
        model = ('###Relational\nDatabase t::DB ( Table T ( CITY VARCHAR(10) NOT NULL, YR INTEGER NOT NULL, N INTEGER '
                 'NOT NULL ) )\n###Connection\nRelationalDatabaseConnection t::C { store: t::DB; type: DuckDB; '
                 'specification: DuckDB { }; auth: Test; }\n###Runtime\nRuntime t::RT { mappings: []; connections: '
                 '[ t::DB: [ c: t::C ] ]; }\n')
        plan = compiler.execution_plan(model, compiler.parse('|#>{t::DB.T}#->pivot(~CITY, ~n : x|$x.N : y|$y->sum())'),
                                       't::RT')
        self.assertEqual(plan['root']['_type'], 'keyed')
        with self.assertRaisesRegex(projects.PlanRefused, "a plan's root is a sequence, a typed result or a constant, not a keyed"):
            projects.run_plan(plan, lambda sql, params: [], 'DuckDB')


if __name__ == '__main__':
    unittest.main()
