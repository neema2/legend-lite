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
from legend_lite._library import library
from legend_lite._planned import (Database, NotPlanned, PlannedSql, all_from_plan, from_plan, host_operation_refusal,
                                  plan_from_answer, send)
from legend_lite._planner import answer
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


class TheBridgesRequestsTest(unittest.TestCase):
    """WHAT DATACUBE'S HOSTED CODE MAY ASK (the final audit's item 1 and the user's ruling of 2026-11-12, kept on the bridge
    on intent by the DataCube stream's merge, 2026-11-13): a snapshot's stream by its pull, never a statement; a host
    operation with no SQL in it."""

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

    def test_a_host_operation_carrying_sql_is_refused_by_name(self):
        """The final audit's item 1 (2026-11-12): a host operation takes no SQL -- a source query, a select list, a
        replacement -- whatever it is; refused at the bridge's planner door (_planned.py host_operation_refusal)."""
        ops = lambda op: {'kind': 'hostOperations', 'databaseType': 'DuckDB', 'operations': json.dumps([op])}
        for op, key in (({'op': 'count', 'from': {'query': 'SELECT * FROM t'}}, 'query'),
                        ({'op': 'relation', 'kind': 'table', 'target': {'name': 't'}, 'selectList': '*',
                          'from': {'relation': {'name': 't'}}}, 'selectList'),
                        ({'op': 'relation', 'kind': 'table', 'target': {'name': 't'}, 'from': {'relation': {'name': 's'}},
                          'replacing': [{'column': 'a', 'sql': 'DROP TABLE t'}]}, 'replacing')):
            self.assertIn(f'takes no SQL, and its {key} is SQL', host_operation_refusal(ops(op)))
        self.assertIsNone(host_operation_refusal(ops({'op': 'count', 'from': {'relation': {'name': 't'}}})))
        self.assertIsNone(host_operation_refusal({'kind': 'plan', 'query': 'anything'}), 'another kind carries no host text')


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

    def test_a_writes_program_runs_in_its_own_transaction_unless_joined(self):
        # the DataCube stream's merge (2026-11-13): a plan of writes carries its target's transaction (version 50) and
        # runs in it -- committed where it completes; a failure rolls all of it back
        self.raw.execute("CREATE TABLE SRC (ID INTEGER, NAME VARCHAR); INSERT INTO SRC VALUES (1, 'a'), (2, 'b')")
        self.raw.execute('CREATE TABLE DST (ID INTEGER NOT NULL, NAME VARCHAR)')
        plan = compiler.execution_plan(self.MODEL, compiler.parse('|#>{a::DB.SRC}#->write(#>{a::DB.DST}#)'), 'a::RT')
        sent = []

        def run(sql, params):
            sent.append(str(sql))
            result = send(self.db, sql, params)
            return result.fetchall() if result.description is not None else []

        self.assertEqual(projects.run_plan(plan, run, 'DuckDB'), '[{"value":2}]')
        self.assertEqual(self.raw.execute('SELECT count(*) FROM DST').fetchone(), (2,), 'committed')
        self.assertTrue(any(s.upper().startswith('BEGIN') for s in sent), sent)
        self.raw.execute('INSERT INTO SRC VALUES (NULL, NULL)')
        self.raw.execute('ALTER TABLE SRC ALTER COLUMN ID DROP NOT NULL')
        with self.assertRaises(Exception):
            projects.run_plan(plan, run, 'DuckDB')
        self.assertEqual(self.raw.execute('SELECT count(*) FROM DST').fetchone(), (2,), 'rolled back, all of it')

    def test_a_writes_program_runs_in_its_callers_transaction_answering_the_databases_count(self):
        self.raw.execute("CREATE TABLE SRC (ID INTEGER, NAME VARCHAR); INSERT INTO SRC VALUES (1, 'a'), (2, 'b')")
        self.raw.execute('CREATE TABLE DST (ID INTEGER, NAME VARCHAR)')
        plan = compiler.execution_plan(self.MODEL, compiler.parse('|#>{a::DB.SRC}#->write(#>{a::DB.DST}#)'), 'a::RT')

        def run(sql, params):
            result = send(self.db, sql, params)
            return result.fetchall() if result.description is not None else []

        self.raw.execute('BEGIN')
        self.assertEqual(projects.run_plan(plan, run, 'DuckDB', joined=True), '[{"value":2}]')
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
        self.assertEqual(json.loads(plan)['root']['_type'], 'keyed')
        with self.assertRaisesRegex(projects.PlanRefused, "a keyed root \\(a dynamic pivot's\\) is not run here"):
            projects.run_plan(plan, lambda sql, params: [], 'DuckDB')


if __name__ == '__main__':
    unittest.main()
