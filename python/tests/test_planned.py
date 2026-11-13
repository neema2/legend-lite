"""THE ONE SEND (python/legend_lite/_planned.py; the total wall's C3, 2026-11-12): Python's host runs on its database only
a statement a plan carries or the planner wrote -- a ``PlannedSql``, made where the plan is read -- and refuses any
other text by name, before the database sees it. Its TypeScript twin is engine-client/test/planned-sql.test.ts; the
guard pinning where the type is made is tools/guards/PlannedSqlTest.java."""

import json
import unittest

import duckdb

from legend_lite import _host, compiler, projects, warehouse
from legend_lite._library import library
from legend_lite._planned import (Answered, Database, NotPlanned, PlannedSql, _keyed_text, across_the_bridge, all_from_plan,
                                  from_plan, plan_from_answer, send)
from legend_lite._planner import answer
from legend_lite._runframe import current_run, in_run, planning
from legend_lite.compiler import LegendError
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
            # a plan the test wrote, typed as the planner's answer is (the re-audit's M1: a runner takes no other)
            projects.run_load(plan_from_answer(json.dumps(plan)), seen.append)
        except projects.LoadRefused as refused:  # a plan shape this runner reads differently: say so
            self.fail(f'the load was refused: {refused}')
        self.assertEqual([type(s) for s in seen], [PlannedSql])
        self.assertEqual(all_from_plan(['x', 'y']), ['x', 'y'])

    def test_the_warehouse_is_sent_only_a_plans_statement(self):
        engine = warehouse.Warehouse.__new__(warehouse.Warehouse)  # no session: nothing may be called on it
        with self.assertRaises(NotPlanned) as refused:
            next(engine.arrow_chunks('SELECT 1'))
        self.assertIn('the warehouse', str(refused.exception))


class TheReAuditsSendTest(unittest.TestCase):
    """The re-audit's M2 and S4 (2026-11-12): the Database's own send requires a plan's statement, as the one send does;
    and only a PlannedSql itself is one -- a str subclass claiming the class is refused."""

    def test_the_databases_own_send_requires_a_plans_statement(self):
        recording = _Recording()
        with self.assertRaises(NotPlanned):
            Database(recording)._send_planned('DROP TABLE t', None)
        self.assertEqual(recording.sent, [], "nothing reached the database")

    def test_a_string_claiming_to_be_planned_is_not(self):
        class Spoof(str):
            @property
            def __class__(self):
                return PlannedSql

        spoof = Spoof('DROP TABLE t')
        self.assertIsInstance(spoof, PlannedSql, 'isinstance is fooled: the check must not be isinstance')
        with self.assertRaises(NotPlanned):
            send(Database(_Recording()), spoof)


class APlanThePlannerAnsweredTest(unittest.TestCase):
    """The re-audit's M1 (2026-11-12): the runners take a plan the planner answered (PlanText) and no other -- a plan a
    host wrote, as a dict or as text, is refused by name before anything runs."""

    def test_a_hand_written_plan_runs_nothing(self):
        written = {'format': 'legend-lite-plan', 'version': projects.PLAN_VERSION,
                   'root': {'_type': 'load', 'identity': 'L', 'cadence': 'ONCE', 'connection': 'c',
                            'steps': [{'_type': 'statement', 'sql': 'DELETE FROM trades'}]}}
        for plan in (written, json.dumps(written)):
            sent = []
            with self.assertRaisesRegex(NotPlanned, 'not a plan the planner answered'):
                projects.run_load(plan, sent.append)
            with self.assertRaisesRegex(NotPlanned, 'not a plan the planner answered'):
                projects.run_host_load(plan, Database(_Recording()))
            self.assertEqual(sent, [])

    def test_the_compiler_answers_plan_text(self):
        plan = compiler.frame_load({'databaseType': 'DuckDB', 'held': 'f', 'as': 'x', 'live': True, 'relation': 'f'})
        self.assertIs(type(plan), projects.PlanText)


class _Witness:
    """A duckdb-python connection that records every text it is handed and every other act asked of it -- what the
    database itself received, not what a runner said it sent (the unfiltered witness: the C final audit's H8 and H13,
    2026-11-12). The one Database holds it as it holds duckdb's own."""

    def __init__(self, con):
        self._con, self.received = con, []

    def execute(self, sql, params=None):
        self.received.append(str(sql))
        return self._con.execute(sql) if params is None else self._con.execute(sql, params)

    def __getattr__(self, name):
        self.received.append(f'<{name}>')
        return getattr(self._con, name)


def _statements_of(node, content=None):
    """Every statement a plan's JSON carries for its steps, in the order written: a step's ``sql``, a list of them, a
    statements step's blob read by its digest (the TypeScript twin: plan-runner.test.ts statementsOf)."""
    if isinstance(node, list):
        return [s for n in node for s in _statements_of(n, content)]
    if not isinstance(node, dict):
        return []
    blobs = node['content'] if isinstance(node.get('content'), dict) else (content or {})
    if node.get('_type') == 'statements':
        return json.loads(blobs[node['ref']['digest']])
    out = []
    for key, value in node.items():
        if key in ('tree', 'content', 'regions', 'target'):
            continue
        if key in ('statement', 'sql') and isinstance(value, str):
            out.append(value)
        elif isinstance(value, list) and value and all(isinstance(v, str) for v in value):
            out.extend(value)
        else:
            out.extend(_statements_of(value, blobs))
    return out


class WitnessTest(unittest.TestCase):
    """THE UNFILTERED WITNESS (the C final audit's H8 and H13, 2026-11-12): everything Python's DuckDB receives during a
    plan's run is the plan's -- its statements in order, and no other text or act."""

    def test_a_load_hands_the_database_its_plans_statements_alone(self):
        import os
        fixture = json.loads(open(os.environ['LEGEND_LITE_RUN_LOAD_FIXTURE'], encoding='utf-8').read())
        project = compiler.project_data(fixture['model'], fixture['databaseType'])
        witness = _Witness(duckdb.connect())
        db = Database(witness)
        for sql in [*project['open'], *project['use']]:
            send(db, sql)
        witness.received.clear()
        projects.run_load(project['load'], lambda statement: send(db, statement))
        self.assertGreater(len(witness.received), 1)
        self.assertEqual(witness.received, _statements_of(json.loads(project['load'])['root']))

    def test_a_stream_hands_the_database_its_statements_and_its_import_alone(self):
        import pyarrow as pa
        import pyarrow.ipc

        rows = pa.table({'id': pa.array([1, 2, 3], pa.int32())})
        sink = pa.BufferOutputStream()
        with pyarrow.ipc.new_stream(sink, rows.schema) as w:
            w.write_table(rows)

        class Plane:
            def arrow_chunks(self, sql, on_receipt=None):
                on_receipt({'statementId': 's1', 'serverRows': 3, 'as': 'rita', 'where': 'a stand-in'})
                yield sink.getvalue().to_pybytes()

        plan = compiler.snap_stream({'databaseType': 'DuckDB', 'plane': 'warehouse', **_pull(), 'table': 'witnessed'})
        step = json.loads(plan)['root']['steps'][0]
        witness = _Witness(duckdb.connect())
        projects.run_host_load(plan, Database(witness), planes={'warehouse': Plane()})
        self.assertEqual(witness.received, [*step['transaction']['begin'], *step['prepare'], '<from_arrow>', step['count'],
                                            *step['transaction']['commit']])

    def test_a_streams_failed_rollback_is_said_with_its_failure(self):
        # G7 (the final audit's item 4, 2026-11-12): a rollback that failed too was suppressed, the failure said alone
        class Plane:
            def arrow_chunks(self, sql, on_receipt=None):
                raise ConnectionResetError('the connection was reset')
                yield b''

        class NoRollback(_Witness):
            def execute(self, sql, params=None):
                if str(sql).upper().startswith('ROLLBACK'):
                    raise RuntimeError('the rollback was refused')
                return super().execute(sql, params)

        plan = compiler.snap_stream({'databaseType': 'DuckDB', 'plane': 'warehouse', **_pull(), 'table': 'unrolled'})
        with self.assertRaises(projects.LoadRefused) as refused:
            projects.run_host_load(plan, Database(NoRollback(duckdb.connect())), planes={'warehouse': Plane()})
        self.assertEqual(str(refused.exception), 'the connection was reset -- and rolling its transaction back failed: '
                                                 'the rollback was refused')
        self.assertIsInstance(refused.exception.__cause__, ConnectionResetError)

    def test_a_statement_missing_what_the_planner_always_writes_is_refused_by_name(self):
        # G7 (the final audit's item 4, 2026-11-12): `slots or []` ran a statement missing its slots as one binding
        # nothing, and `content or {}` / `get('digest', '')` looked a payload up by no digest
        with self.assertRaises(compiler.PlanParamsRefused) as refused:
            compiler.statement_params({'statement': 'SELECT 1'})
        self.assertEqual(str(refused.exception), "this plan's statement does not carry its slots")
        payload = {'parameter': 'p', 'binding': {'_type': 'payload', 'digest': 'ab'}}
        for statement in ({'slots': [payload]}, {'slots': [{**payload, 'binding': {'_type': 'payload'}}], 'content': {}}):
            with self.assertRaises(compiler.PlanParamsRefused) as refused:
                compiler.statement_params(statement)
            self.assertEqual(str(refused.exception), "'p' binds a payload its statement does not carry")


# A SNAPSHOT'S PULL (the user's ruling of 2026-11-12, "that is a real off platform problem"): a stream's request carries
# the model, its runtime and the query, which the planner plans in the call; never the statement the plane runs
_PULL_MODEL = """###Relational
Database s::DB ( Table t ( id INTEGER, desk VARCHAR(8), qty DOUBLE ) )

###Connection
RelationalDatabaseConnection s::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }

###Runtime
Runtime s::RT { mappings: []; connections: [ s::DB: [ c: s::Conn ] ]; }
"""


def _pull() -> dict:
    """A grid's base query as a snapshot's request carries it: its model, its runtime and its lambda's JSON."""
    return {'model': _PULL_MODEL, 'runtime': 's::RT', 'lambda': json.dumps(compiler.parse('|#>{s::DB.t}#->select(~[id, desk, qty])'))}


class ASnapshotsCopyTest(unittest.TestCase):
    """A SNAPSHOT'S LOCAL COPY (the final audit's item 1(b), 2026-11-12): the native library's snap_copy answers the pull
    and a table with ONE plan of writes -- the table there before dropped, the table made from the pull -- in its
    target's transaction, run by projects.run_write; never the pull's statement handed back to the planner."""

    def setUp(self):
        self.con = duckdb.connect()
        self.con.execute("CREATE TABLE t (id INTEGER, desk VARCHAR, qty DOUBLE)")
        self.con.execute("INSERT INTO t VALUES (1, 'FX', 1.5), (2, 'EQ', 2.5)")

    def test_the_database_is_handed_exactly_the_plans_statements_in_its_transaction(self):
        plan = compiler.snap_copy({'databaseType': 'DuckDB', **_pull(), 'table': 'copied'})
        root = json.loads(plan)['root']
        self.assertEqual([s['_type'] for s in root['steps']], ['effect', 'effect'])
        target = root['steps'][0]['sql']['target']
        witness = _Witness(self.con)
        projects.run_write(plan, Database(witness))
        self.assertEqual(witness.received, [*target['session'], *target['transaction']['begin'],
                                            *[s['sql']['statement'] for s in root['steps']],
                                            *target['transaction']['commit']])
        self.assertEqual(self.con.execute('SELECT * FROM copied ORDER BY id').fetchall(),
                         [(1, 'FX', 1.5), (2, 'EQ', 2.5)])

    def test_a_copy_that_fails_after_its_drop_rolls_back_and_the_table_before_is_as_it_was(self):
        self.con.execute("CREATE TABLE kept AS SELECT 9 AS id, 'z' AS desk, CAST(9.5 AS DOUBLE) AS qty")
        plan = compiler.snap_copy({'databaseType': 'DuckDB', **_pull(), 'table': 'kept'})

        class Refusing(_Witness):
            def execute(self, sql, params=None):
                if str(sql).startswith('CREATE TABLE'):
                    raise duckdb.Error('the copy was refused')
                return super().execute(sql, params)

        witness = Refusing(self.con)
        with self.assertRaisesRegex(duckdb.Error, 'the copy was refused'):
            projects.run_write(plan, Database(witness))
        self.assertEqual(witness.received[-1], 'ROLLBACK')
        self.assertEqual(self.con.execute('SELECT * FROM kept').fetchall(), [(9, 'z', 9.5)])

    def test_any_other_plan_and_a_request_carrying_a_statement_are_refused_by_name(self):
        with self.assertRaises(LegendError) as refused:
            compiler.snap_copy({'databaseType': 'DuckDB', **_pull(), 'sql': 'SELECT 1', 'table': 't'})
        self.assertIn("a snapshot's copy has no key sql", str(refused.exception))
        stream = compiler.snap_stream({'databaseType': 'DuckDB', 'plane': 'warehouse', **_pull(), 'table': 'x'})
        with self.assertRaises(projects.LoadRefused) as refused:
            projects.run_write(stream, Database(self.con))
        self.assertIn('a plan of writes alone is effects of one statement each, not a load', str(refused.exception))


class TheAuthoredDoorIsNotPythonsTest(unittest.TestCase):
    """2026-11-12 (the orchestrator's ruling): a person's own statement is Studio's SQL playground's alone. Python's
    planner protocol refuses the kind by name, the native library exports no lite_authored, and the headless check
    still agrees with the bundle, which names the kind for the tab."""

    def test_the_kind_is_refused_by_name_and_the_library_exports_none(self):
        from legend_lite import _headless, _planner
        said = answer(library(), {'id': 7, 'kind': 'authored', 'request': '{"databaseType": "DuckDB", "text": "DROP TABLE t"}'})
        self.assertEqual((said['id'], said['ok']), (7, False))
        self.assertRegex(said['error'], "Studio's SQL playground alone")
        self.assertNotIn('authored', _planner.KINDS)
        self.assertFalse(library().exports('lite_authored'), 'the native library exports no authored door')
        _headless.check_interface([*_planner.KINDS, 'authored'], library())
        with self.assertRaises(_headless.InterfaceMismatch):
            _headless.check_interface([*_planner.KINDS], library())


class KeyedExpansionFixtureTest(unittest.TestCase):
    """THE KEYED EXPANSION'S ONE FIXTURE (the C final audit's H8, 2026-11-12; engine-client/test/fixtures/
    keyed-expansion.json): the text Python's bridge holds a keyed statement to (_keyed_text) is the text Java's
    PlanRunner.expand and the tab's expandKeyed write, case by case -- its text alone, the keys' cells being bound."""

    def test_each_case_is_written_as_the_other_runners_write_it(self):
        import os
        cases = json.loads(open(os.environ['LEGEND_LITE_KEYED_FIXTURE'], encoding='utf-8').read())['cases']
        self.assertGreater(len(cases), 3)
        for case in cases:
            with self.subTest(case['name']):
                body = case['body']
                written = _keyed_text(body['statement'], tuple(body['regions']), len(case['keys']))
                self.assertEqual(written, case.get('statement'), 'refused: no text' if 'refused' in case else '')


class HostOperationsTextTest(unittest.TestCase):
    """A HOST OPERATION TAKES NO SQL (the final audit's item 1, 2026-11-12; until then the C final audit's H2 let one carry
    a PlannedSql the planner had answered): a source query, a select list, a replacement -- each refused by name by the
    platform, even a planner's own statement; a copy's conversions are the platform's, written from its columns' catalog
    (``convert``)."""

    def test_an_operation_carrying_sql_is_refused_by_name_even_the_planners_own(self):
        for op in ({'op': 'count', 'from': {'query': from_plan('SELECT 1 AS a')}},
                   {'op': 'relation', 'kind': 'table', 'target': {'name': 't'}, 'selectList': '*', 'from': {'relation': {'name': 's'}}},
                   {'op': 'relation', 'kind': 'table', 'target': {'name': 't'}, 'from': {'relation': {'name': 's'}},
                    'replacing': [{'column': 'a', 'sql': 'a'}]}):
            with self.subTest(op=op), self.assertRaises(LegendError) as refused:
                compiler.host_operations('DuckDB', [op])
            self.assertTrue('a query is planned from its pull' in str(refused.exception)
                            or 'a host operation takes no SQL' in str(refused.exception), str(refused.exception))

    def test_a_copys_conversions_are_written_from_its_columns_catalog(self):
        model = compiler.table_model({'table': 't', 'convertible': True, 'databaseType': 'DuckDB', 'columns': [
            {'name': 'big', 'dataType': 'UBIGINT', 'logicalType': 'UBIGINT'}]})
        self.assertNotIn('copySelectList', model)
        ((copy,),) = compiler.host_operations('DuckDB', [{'op': 'relation', 'kind': 'table', 'target': {'name': 't'},
                                                         'replace': True, 'from': {'relation': {'name': 't'}},
                                                         'convert': [{'name': 'big', 'dataType': 'UBIGINT',
                                                                      'logicalType': 'UBIGINT'}]}])
        self.assertIn('CAST("big" AS DECIMAL(20,0))', copy)


class _RunWitness:
    """A duckdb-python connection that records the run each text it is handed arrived inside (the witness)."""

    def __init__(self, con):
        self._con, self.runs = con, []

    def execute(self, sql, params=None):
        self.runs.append(current_run())
        return self._con.execute(sql) if params is None else self._con.execute(sql, params)

    def __getattr__(self, name):
        return getattr(self._con, name)


class RunFrameTest(unittest.TestCase):
    """THE RUN FRAME (_runframe.py, the next PR's item 2, "separation made foolproof": the standing witness, 2026-11-13):
    a planner entry -- the native library's every entry but its services -- asked inside a run of its own flow is
    refused by name; outside one, after one and on another thread while one runs, it answers. The runners' sends are
    each inside a run."""

    def test_a_planner_entry_is_refused_inside_a_run_and_answers_outside_one(self):
        planning('plan')
        with in_run("a test's run"):
            self.assertEqual(current_run(), "a test's run")
            with self.assertRaisesRegex(RuntimeError, r"the planner was asked 'plan' inside a run \(a test's run\)"):
                planning('plan')
            with self.assertRaisesRegex(RuntimeError, r"the planner was asked 'lite_session_setup' inside a run"):
                library().call('lite_session_setup', 'DuckDB')
            # a service plans nothing: the site's types answer inside a run
            self.assertTrue(library().call('lite_site_types').startswith('OK'))
        self.assertIsNone(current_run(), 'the run ended with its block')
        self.assertTrue(library().call('lite_session_setup', 'DuckDB').startswith('OK'))

    def test_the_frame_is_the_flows_another_thread_plans_while_a_run_is_open(self):
        import threading
        seen = []
        with in_run("a test's run"):
            other = threading.Thread(target=lambda: seen.append((current_run(), compiler.session_setup('DuckDB') != [])))
            other.start()
            other.join()
        self.assertEqual(seen, [(None, True)])

    def test_no_crossing_is_known_and_none_is_exempt(self):
        # shrink-only, and empty since 2026-11-13: the read's re-pin, its one entry, went with the user's ruling (1b)
        from legend_lite._runframe import EXEMPTIONS, exempt
        self.assertEqual(EXEMPTIONS, frozenset())
        with in_run("a test's run"):
            for crossing in ("a read's re-pin after its statement failed", 'a crossing of my own'):
                with self.assertRaisesRegex(ValueError, 'no known crossing is named'):
                    with exempt(crossing):
                        pass
            with self.assertRaisesRegex(RuntimeError, 'inside a run'):
                planning('plan')

    def test_each_runner_sends_inside_its_run(self):
        import os
        fixture = json.loads(open(os.environ['LEGEND_LITE_RUN_LOAD_FIXTURE'], encoding='utf-8').read())
        project = compiler.project_data(fixture['model'], fixture['databaseType'])
        seen = []
        projects.run_load(project['load'], lambda statement: seen.append(current_run()))
        self.assertTrue(seen and set(seen) == {'a load'}, seen)
        witness = _RunWitness(duckdb.connect())
        db = Database(witness)
        _host.run(db, {'op': 'schema', 'name': 'witnessed'})
        _host.ask(db, {'op': 'platform'})
        self.assertEqual(witness.runs, ["a host's operations", "a host's question"])
        # a run's planner entry, refused inside it: a load whose execute asks the planner
        with self.assertRaisesRegex(RuntimeError, r"asked 'lite_session_setup' inside a run \(a load\)"):
            projects.run_load(project['load'], lambda statement: compiler.session_setup('DuckDB'))


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
                send(Database(con), across_the_bridge(text, "the hosted DataCube's query", answered))
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

    def test_a_stream_steps_remote_statement_and_an_updates_count_are_not_this_databases(self):
        """The C final audit's H3 (2026-11-12): a snapshot's stream step carries the REMOTE plane's statement (its
        ``sql``) and an update step names the value a later step binds (its ``count``): neither is a statement this
        database runs, so neither is admitted -- the stream's own local statements are."""
        plan = compiler.snap_stream({'databaseType': 'DuckDB', 'plane': 'warehouse', **_pull(), 'table': 'snap_t'})
        answered = Answered()
        answered.record('snapStream', 'OK\n' + plan)
        stream = json.loads(plan)['root']['steps'][0]
        self.assertEqual(stream['_type'], 'stream')
        sent = stream['sql']
        self.assertNotIn(str(sent), answered, "the remote plane's statement is never this database's")
        for local in [*stream['prepare'], stream['count'], *stream['transaction']['begin']]:
            self.assertIn(local, answered, local)
        answered.record('executionPlan', 'OK\n' + json.dumps({'root': {'_type': 'sequence', 'steps': [
            {'_type': 'update', 'count': 'written', 'sql': {'statement': 'INSERT INTO t SELECT 1', 'slots': []}}]}}))
        self.assertIn('INSERT INTO t SELECT 1', answered)
        self.assertNotIn('written', answered, "an update's count names a value, never a statement")

    def test_a_snapshots_stream_is_asked_by_its_pull_never_its_statement(self):
        # the user's ruling of 2026-11-12: the request carries the pull, which the planner plans in the call -- its
        # stream's statement is the planner's own plan of it, and a request carrying a statement is refused by name
        plan = compiler.snap_stream({'databaseType': 'DuckDB', 'plane': 'warehouse', **_pull(), 'table': 'snap_t'})
        pull = _pull()
        planned = compiler.plan(pull['model'], json.loads(pull['lambda']), pull['runtime'])
        self.assertEqual(json.loads(plan)['root']['steps'][0]['sql'], str(planned.sql))
        for given in ({'sql': 'DROP TABLE t'}, {**_pull(), 'sql': str(planned.sql)}):
            with self.assertRaisesRegex(LegendError, "a snapshot's stream has no key sql"):
                compiler.snap_stream({'databaseType': 'DuckDB', 'plane': 'warehouse', **given, 'table': 'snap_t'})

    def test_an_answer_bigger_than_the_bound_is_kept_whole(self):
        """The audit's NOTE: an answer of more than CAPACITY statements evicted its own first ones before any was sent;
        now the newest answer is kept whole, and the next one trims the set back to its bound."""
        answered = Answered()
        many = [f'SELECT {i}' for i in range(Answered.CAPACITY + 10)]
        answered.record('hostOperations', 'OK\n' + json.dumps([many]))
        self.assertEqual(len(answered), Answered.CAPACITY + 10)
        self.assertIn('SELECT 0', answered)
        answered.record('catalogColumnsSql', 'OK\nSELECT -1')
        self.assertEqual(len(answered), Answered.CAPACITY)
        self.assertIn('SELECT -1', answered)

    def test_a_host_operation_carrying_sql_is_refused_by_name_even_an_answered_statement(self):
        """The final audit's item 1 (2026-11-12; until then the re-audit's S3 admitted a text this session's planner had
        answered): a host operation takes no SQL -- a source query, a select list, a replacement -- whatever it is."""
        answered = Answered()
        ops = lambda op: {'kind': 'hostOperations', 'databaseType': 'DuckDB', 'operations': json.dumps([op])}
        answered.record('plan', 'OK\n' + json.dumps({'sql': 'SELECT * FROM t'}))
        for op, key in (({'op': 'count', 'from': {'query': 'SELECT * FROM t'}}, 'query'),
                        ({'op': 'relation', 'kind': 'table', 'target': {'name': 't'}, 'selectList': '*',
                          'from': {'relation': {'name': 't'}}}, 'selectList'),
                        ({'op': 'relation', 'kind': 'table', 'target': {'name': 't'}, 'from': {'relation': {'name': 's'}},
                          'replacing': [{'column': 'a', 'sql': 'DROP TABLE t'}]}, 'replacing')):
            self.assertIn(f'takes no SQL, and its {key} is SQL', answered.refusal(ops(op)))
        self.assertIsNone(answered.refusal(ops({'op': 'count', 'from': {'relation': {'name': 't'}}})))
        self.assertIsNone(answered.refusal({'kind': 'plan', 'query': 'anything'}), 'another kind carries no host text')

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
        raw = duckdb.connect()
        raw.execute('CREATE TABLE T_TREES (CITY VARCHAR, YR INTEGER, TREES INTEGER)')  # the test's own fixture
        raw.execute("INSERT INTO T_TREES VALUES ('NYC', 2000, 5), ('LDN', 2000, 3), ('NYC', 2011, 2)")
        con = Database(raw)
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
