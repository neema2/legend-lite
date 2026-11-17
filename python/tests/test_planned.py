"""THE ONE SEND (python/legend_lite/_planned.py; the total wall's C3, 2026-11-12): Python's host runs on its database only
a statement a plan carries or the planner wrote -- a ``PlannedSql``, made where the plan is read -- and refuses any
other text by name, before the database sees it. Its TypeScript twin is engine-client/test/planned-sql.test.ts; the
guard pinning where the type is made is tools/guards/PlannedSqlTest.java."""

import json
import unittest

import duckdb

from legend_lite import _host, compiler, projects, warehouse
from legend_lite._library import library
from legend_lite._planned import (Answered, Database, NotPlanned, PlannedSql, Unframed, _keyed_text, across_the_bridge,
                                  all_from_plan, from_plan, plan_from_answer, send)
from legend_lite._planner import answer
from legend_lite._runframe import current_run, in_run, planning, runs
from legend_lite.compiler import LegendError
from legend_lite._reads import fetched_redacted


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
        with in_run("a test's run"):
            self.assertEqual(send(con, from_plan('SELECT 41 + 1')).fetchall(), [(42,)])
            # its `?`s bound in order (a plan's payloads)
            self.assertEqual(send(con, from_plan('SELECT ? || ?'), ['a', 'b']).fetchall(), [('ab',)])

    def test_a_plans_statement_sent_outside_a_run_is_refused_and_never_reaches_the_database(self):
        # C5-P (2026-11-15): a frame wraps every send -- the run side's own functions set it -- so a flow that sends is one
        # the planner refuses; a plan's statement sent in no run is refused by name, and nothing reaches the database
        con = _Recording()
        with self.assertRaisesRegex(Unframed, r"a plan's statement is sent only inside a run"):
            send(Database(con), from_plan('SELECT 1'))
        self.assertEqual(con.sent, [])
        # a text no plan carries is refused as that first, in a run or not
        with self.assertRaises(NotPlanned):
            send(Database(con), 'SELECT 1')
        with in_run("a test's run"):
            send(Database(con), from_plan('SELECT 1'))
        self.assertEqual(con.sent, ['SELECT 1'])

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
        with in_run("a test's run"):
            result = send(db, from_plan('SELECT 1'))
        for way in ('execute', 'sql', 'query', 'cursor', 'from_arrow'):
            self.assertFalse(hasattr(result, way), f'a statement\'s answer offers {way}')
        with self.assertRaises(NotPlanned) as refused:
            send(duckdb.connect(), from_plan('SELECT 1'))
        self.assertIn("runs on the host's Database", str(refused.exception))
        with self.assertRaises(NotPlanned):
            db.import_stream(None, None, 't; DROP TABLE x')

    def test_the_send_only_redaction_is_gone(self):
        # the C5 follow-up's check (2026-11-15): execute_redacted redacted the send alone, the gap fetched_redacted closed;
        # it is gone, its re-export with it, so nothing redacts half
        from legend_lite import _reads, frames
        self.assertFalse(hasattr(_reads, 'execute_redacted'))
        self.assertFalse(hasattr(frames, 'execute_redacted'))

    def test_a_reads_send_refuses_text_no_plan_carries(self):
        con = _Recording()
        with self.assertRaises(NotPlanned):
            fetched_redacted(Database(con), 'SELECT 1', (), (), lambda result: result)
        self.assertEqual(con.sent, [])

    def test_a_loads_steps_are_the_plans(self):
        # run_load types each step as it reads it from the plan: what reaches the database is a PlannedSql
        seen: list[object] = []
        plan = {'format': 'legend-lite-plan', 'version': projects.PLAN_VERSION, 'raises': {},
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

    def test_the_warehouse_is_sent_a_plans_statement_only_inside_a_run(self):
        # C5-P (the audit's item 2, 2026-11-15): a plane's send is framed by construction, as the one send is -- however
        # the method is reached (a name bound to it, getattr), outside a run it is refused by name before anything is
        # submitted; nothing may be called on this one, so a submit would fail otherwise
        engine = warehouse.Warehouse.__new__(warehouse.Warehouse)
        sends = (engine.arrow_chunks, getattr(engine, 'arrow_chunks'))
        for reached in sends:
            with self.assertRaisesRegex(Unframed, r"sent to the warehouse only inside a run"):
                next(reached(from_plan('SELECT 1')))


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
        with in_run("a test's run"):
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

    def test_a_runs_frame_ends_with_its_call_whether_it_returns_or_raises(self):
        # the frame `runs` marks is the call's own: after the call -- returned or raised -- the flow is in no run, and
        # the planner answers again (the orchestrator's condition for the direct set/reset, 2026-11-13)
        @runs("a test's run")
        def returns():
            self.assertEqual(current_run(), "a test's run")
            return 'answered'

        @runs("a test's run")
        def raises():
            self.assertEqual(current_run(), "a test's run")
            raise KeyError('the run failed')

        self.assertEqual(returns(), 'answered')
        self.assertIsNone(current_run(), 'the run ended with its call')
        with self.assertRaisesRegex(KeyError, 'the run failed'):
            raises()
        self.assertIsNone(current_run(), 'the run ended with its call, which raised')
        planning('plan')

    def test_a_run_inside_a_run_names_itself_and_hands_back_the_outer(self):
        @runs('the inner run')
        def inner(fail):
            self.assertEqual(current_run(), 'the inner run')
            if fail:
                raise KeyError('the inner run failed')

        with in_run('the outer run'):
            inner(False)
            self.assertEqual(current_run(), 'the outer run')
            with self.assertRaisesRegex(KeyError, 'the inner run failed'):
                inner(True)
            self.assertEqual(current_run(), 'the outer run', 'the outer run holds after the inner one raised')
            with self.assertRaisesRegex(RuntimeError, r"asked 'plan' inside a run \(the outer run\)"):
                planning('plan')
        self.assertIsNone(current_run())

    def test_each_python_run_sends_inside_its_own_run(self):
        # Reading.run, Frames.run, run_write and run_host_load (the C4 audit's M3, 2026-11-13: no test asserted these
        # four, so a run taken off one passed; the run wall pinned them by name too, until C5-P deleted it for
        # //python:run_closure_test, which holds every send of the run side framed): each statement each sends is sent
        # inside the run it names
        from unittest import mock
        import pyarrow as pa
        from legend_lite import frames as frames_module
        con = duckdb.connect()
        con.execute("CREATE TABLE t (id INTEGER, desk VARCHAR, qty DOUBLE)")
        con.execute("INSERT INTO t VALUES (1, 'FX', 1.5)")
        witness = _RunWitness(con)
        projects.run_write(compiler.snap_copy({'databaseType': 'DuckDB', **_pull(), 'table': 'copied'}), Database(witness))
        self.assertEqual(set(witness.runs), {'a plan of writes'})
        witness = _RunWitness(duckdb.connect())
        load = compiler.frame_load({'databaseType': 'DuckDB', 'held': 'h', 'as': '__legend_lite_h', 'live': True,
                                    'schema': 'main', 'relation': 'h'})
        projects.run_host_load(load, Database(witness), values=lambda held: pa.table({'x': [1]}))
        self.assertTrue(witness.runs and set(witness.runs) == {"a host's load"}, witness.runs)
        frames = frames_module.Frames()
        table = frames.register('trades', pa.table({'id': [1, 2], 'desk': ['FX', 'EQ']}))
        seen = []
        # the run side's send (C5-P, 2026-11-15: frames.py's reads moved to _reads.py, frames.py now plans and hands the
        # run its statements)
        from legend_lite import _reads
        real = _reads.send

        def recording(*args, **kwargs):
            seen.append(current_run())
            return real(*args, **kwargs)

        with mock.patch.object(_reads, 'send', recording):
            table.execute('->select(~[desk])')
            self.assertTrue(seen and set(seen) == {'a plan'}, seen)
            seen.clear()
            plan = table.plan('->select(~[desk])')
            with frames.reading() as read:
                # the read begun in its own run (C5-P: its session's setup and its pin, framed since)
                self.assertTrue(seen and set(seen) == {'a read begun'}, seen)
                seen.clear()
                read.run(plan)
                self.assertTrue(seen and set(seen) == {'a read'}, seen)

    def test_a_planner_entry_asked_through_the_pool_inside_a_run_is_refused(self):
        # the C4 audit's M4, 2026-11-13: the pool's thread ran a call in its own, empty context, so a run's frame did not
        # reach it and a planner entry asked through it inside a run answered; the call now runs in a copy of the
        # caller's context. A service and a held model let go stay exempt, as on the caller's own thread.
        from legend_lite._pool import _on_compiler_threads
        held = compiler.HeldModel(_PULL_MODEL)
        with in_run("a test's run"):
            with self.assertRaisesRegex(RuntimeError, r"asked 'lite_session_setup' inside a run \(a test's run\)"):
                _on_compiler_threads(lambda: library().call('lite_session_setup', 'DuckDB'))
            self.assertTrue(_on_compiler_threads(lambda: library().call('lite_site_types')).startswith('OK'))
            _on_compiler_threads(held.close)
        self.assertTrue(_on_compiler_threads(lambda: library().call('lite_session_setup', 'DuckDB')).startswith('OK'))

    def test_no_crossing_is_known_and_none_is_exempt(self):
        # shrink-only: the read's re-pin went with the user's ruling (1b), 2026-11-13; a gone session's finalizer (named
        # 2026-11-14, the final audit of 9af16fbe58's P6) went with the user's ruling (b) that day, the C4 final fix
        # round's D2: it queues its session, drained outside any run. The mechanism stays, refusing every name
        from legend_lite._runframe import EXEMPTIONS, exempt
        self.assertEqual(EXEMPTIONS, frozenset())
        with in_run("a test's run"):
            for crossing in ("a read's re-pin after its statement failed",
                             "a gone session's frames let go (its finalizer may land inside a run)",
                             'a crossing of my own'):
                with self.assertRaisesRegex(ValueError, 'no known crossing is named'):
                    with exempt(crossing):
                        pass
            with self.assertRaisesRegex(RuntimeError, 'inside a run'):
                planning('plan')

    def _gone_session(self, connection):
        """A marimo session over ``connection``, a frame registered, held as marimo's sessions are (its key)."""
        import pyarrow as pa
        from legend_lite import datacube
        from legend_lite.frames import Frames
        session = datacube._Session()
        session.frames = Frames(connection)
        session.frames.register('gone_t', pa.table({'a': [1, 2]}))
        key = id(session)
        with datacube._marimo_lock:
            datacube._marimo_sessions[key] = session
        self.addCleanup(lambda: datacube._marimo_sessions.pop(key, None))
        return session, key

    def test_a_gone_sessions_finalizer_sends_nothing_inside_a_run_and_its_frames_go_after_it(self):
        # the user's ruling (b) of 2026-11-14, the C4 final fix round's D2: garbage collection runs datacube._release (a
        # weakref.finalize) wherever it fires, a run's flow included -- it queues the session and sends nothing there;
        # the next call that finds its session (_current), outside any run, lets the session's frames go
        from legend_lite import datacube
        witness = _RunWitness(duckdb.connect())
        session, key = self._gone_session(witness)
        sent = len(witness.runs)
        with in_run("a test's run"):
            datacube._release(key, session)
            self.assertEqual(current_run(), "a test's run")
            # a call that finds its session inside a run leaves the queue for the next
            datacube._current()
        self.assertEqual(len(witness.runs), sent, 'the finalizer sent nothing inside the run')
        self.assertIn('gone_t', session.frames._tables)
        self.assertIn(key, datacube._marimo_sessions)
        datacube._current()
        self.assertEqual(session.frames._tables, {})
        self.assertNotIn(key, datacube._marimo_sessions)
        self.assertNotIn((key, session), datacube._gone)
        self.assertGreater(len(witness.runs), sent)
        self.assertNotIn("a test's run", witness.runs[sent:])

    def test_no_drop_of_a_gone_session_interleaves_with_a_runs_statements(self):
        # D2: a run's statements on one database, the finalizer of another session firing between them: every statement
        # the gone session's frames send comes after the run's last, each inside its own host operations' run
        from legend_lite import datacube
        log = []

        class Logged(_RunWitness):
            def __init__(self, con, who):
                super().__init__(con)
                self.who = who

            def execute(self, sql, params=None):
                log.append((self.who, current_run(), str(sql)))
                return super().execute(sql, params)

        session, key = self._gone_session(Logged(duckdb.connect(), 'gone'))
        running = Database(Logged(duckdb.connect(), 'run'))
        del log[:]
        with in_run("a test's run"):
            send(running, PlannedSql('SELECT 1'))
            datacube._release(key, session)
            send(running, PlannedSql('SELECT 2'))
        datacube._current()
        whose = [who for who, _, _ in log]
        self.assertEqual(whose[:2], ['run', 'run'], log)
        gone = log[2:]
        self.assertTrue(gone and all(who == 'gone' for who, _, _ in gone), log)
        self.assertTrue(any('DROP' in sql.upper() for _, _, sql in gone), log)
        self.assertEqual({run for _, run, _ in gone}, {"a host's operations"}, log)

    def test_a_gone_sessions_cleanup_error_is_reported_never_raised_and_the_queue_goes_on(self):
        # the C4 second small round's item 2b, 2026-11-14: a gone session's cleanup error is no caller's failure -- it
        # is reported through sys.unraisablehook (as a finalizer's error was, before the finalizer only queued), the
        # call that drained the queue raises nothing, and the next queued session is let go all the same
        import sys
        from unittest import mock
        from legend_lite import datacube
        first, first_key = self._gone_session(duckdb.connect())
        second, second_key = self._gone_session(duckdb.connect())
        reported = []
        with mock.patch.object(first.frames, 'close', side_effect=duckdb.IOException('the drop failed')), \
                mock.patch.object(sys, 'unraisablehook', reported.append):
            datacube._release(first_key, first)
            datacube._release(second_key, second)
            datacube._current()
        self.assertEqual(len(reported), 1, reported)
        self.assertIn("a gone marimo session's frames was not let go: the drop failed", str(reported[0].exc_value))
        self.assertIsInstance(reported[0].exc_value.__cause__, duckdb.IOException)
        self.assertNotIn(first_key, datacube._marimo_sessions)
        self.assertNotIn(second_key, datacube._marimo_sessions)
        self.assertEqual(second.frames._tables, {})
        self.assertEqual(len(datacube._gone), 0)

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


class C5SplitTest(unittest.TestCase):
    """C5-P (2026-11-15): Python's plan/exec split -- the warehouse's sends framed, and what a read's run sends planned
    once per Frames, depending on nothing that changes after it is made."""

    def test_the_two_keys_carry_their_own_names_alone(self):
        # A1 (C5-P's last round, 2026-11-15): the service key, the run side's, carries the warehouse's wire alone -- a
        # planner entry, or another service, asked of it is refused by name before the library is called; the planning
        # key refuses the wire, the service key's
        from legend_lite import _services
        from legend_lite._loader import SERVICE_KEY_NAMES
        self.assertEqual(SERVICE_KEY_NAMES, {'lite_warehouse_call', 'lite_warehouse_read'})
        for name in ('lite_session_setup', 'lite_plan_json_held', 'lite_site_types', 'lite_release_model'):
            with self.assertRaisesRegex(ValueError, f"{name} is not the service key's"):
                _services.call(name, 'DuckDB')
        self.assertIn(_services.call('lite_warehouse_call', 'submit', '{}').split('\n', 1)[0], ('OK', 'ERR'))
        for name in sorted(SERVICE_KEY_NAMES):
            with self.assertRaisesRegex(ValueError, f"{name} is the warehouse's wire, the run side's service key's"):
                library().call(name, 'x', 'y')

    def test_the_loader_itself_refuses_a_planner_entry_inside_a_run(self):
        # C5-P's audit M1 (2026-11-15): the planning check is the loader's, so no path past the keys skips it -- the
        # loader reached directly, inside a run, refuses a planner's entry by name; a service passes
        from legend_lite._loader import loaded
        with in_run("a test's run"):
            with self.assertRaisesRegex(RuntimeError, r"asked 'lite_session_setup' inside a run \(a test's run\)"):
                loaded().call('lite_session_setup', 'DuckDB')
            self.assertTrue(loaded().call('lite_site_types').startswith('OK'))
        self.assertTrue(loaded().call('lite_session_setup', 'DuckDB').startswith('OK'))

    def test_a_streams_landing_and_every_warehouse_fetch_refuse_outside_a_run(self):
        # C5-P's audit S3 (2026-11-15): a stream's landing is a run act, as a transaction's bounds are; and every call of
        # the warehouse's SQL API -- a statement's later chunks too, fetched by _reach -- refuses outside a run
        import pyarrow as pa
        reader = pa.RecordBatchReader.from_batches(pa.schema([('x', pa.int64())]), iter([]))
        with self.assertRaisesRegex(Unframed, 'a stream landed only inside a run'):
            Database(duckdb.connect()).import_stream(reader, None, 't')
        with self.assertRaisesRegex(Unframed, "a call of the warehouse's SQL API is made only inside a run"):
            warehouse._reach('http://wh.invalid', {'body': None, 'path': '/x', 'method': 'GET', 'headers': {}})

    def test_the_pool_carries_the_callers_run_for_every_caller(self):
        # C5-P's audit S4 (2026-11-15): the compiler's pool runs every call it is handed in a copy of the caller's
        # context -- a caller submitting to it directly too, not only _on_compiler_threads
        from legend_lite._pool import _workers
        with in_run("a test's run"):
            self.assertEqual(_workers().submit(current_run).result(), "a test's run")
        self.assertIsNone(_workers().submit(current_run).result())

    def test_the_warehouses_every_call_is_a_run(self):
        # A2 (2026-11-15): a sign-in, a listing, a statement's poll or close -- every call of the SQL API -- is a send,
        # made in a run ("the warehouse's SQL API"), whoever asks it
        from unittest import mock
        seen = []

        def reached(base_url, call):
            seen.append(current_run())
            raise warehouse.WarehouseError('a stand-in: nothing is reached')

        with mock.patch.object(warehouse, '_reach', reached):
            for asked in (lambda: warehouse.sign_in('http://wh.invalid', 'rita', 'pw'),
                          lambda: warehouse.sign_in_with_key('http://wh.invalid', 'k'),
                          lambda: warehouse.list_objects(warehouse.Session('http://wh.invalid', 't', 'rita', ''))):
                with self.assertRaises(warehouse.WarehouseError):
                    asked()
        self.assertEqual(seen, ["the warehouse's SQL API"] * 3)
        self.assertIsNone(current_run())

    def test_a_transactions_bounds_are_run_acts(self):
        # A3 (2026-11-15): begin, commit and rollback on the host's database, each refused by name outside a run, as the
        # one send is; inside one each runs
        db = Database(duckdb.connect())
        for act, said in ((db.begin, 'a transaction begun'), (db.commit, 'a transaction committed'),
                          (db.rollback, 'a transaction rolled back')):
            with self.assertRaisesRegex(Unframed, f'{said} only inside a run'):
                act()
        with in_run("a test's run"):
            db.begin()
            db.rollback()

    def test_a_streams_every_pull_is_in_its_run_on_duckdbs_own_thread(self):
        # A4 (2026-11-15): DuckDB pulls a stream's batches on a thread of its own (measured: duckdb 1.5.5, every pull on
        # a foreign thread), where a run's frame is not set; each pull -- the plane's next chunk -- runs in the run's
        # context, so the plane's every chunk is fetched in the run
        import threading
        import pyarrow as pa
        import pyarrow.ipc

        rows = pa.table({'id': pa.array([1, 2, 3], pa.int32())})
        sink = pa.BufferOutputStream()
        with pyarrow.ipc.new_stream(sink, rows.schema) as w:
            w.write_table(rows)
        pulls = []

        class Plane:
            def arrow_chunks(self, sql, on_receipt=None):
                on_receipt({'statementId': 's1', 'serverRows': 9, 'as': 'rita', 'where': 'a stand-in'})
                for _ in range(3):
                    pulls.append((current_run(), threading.current_thread() is threading.main_thread()))
                    yield sink.getvalue().to_pybytes()

        plan = compiler.snap_stream({'databaseType': 'DuckDB', 'plane': 'warehouse', **_pull(), 'table': 'pulled'})
        projects.run_host_load(plan, Database(duckdb.connect()), planes={'warehouse': Plane()})
        self.assertEqual(len(pulls), 3)
        self.assertTrue(any(not on_main for _, on_main in pulls), f'DuckDB pulled every chunk on the caller: {pulls}')
        self.assertEqual({run for run, _ in pulls}, {"a host's load"}, pulls)

    def test_a_remote_planes_read_is_a_run_and_its_bindings_calls_see_it_across_the_pool(self):
        # the warehouse's statement is sent, and its answer read, inside a run (engine.py sent it in none until C5-P);
        # the binding's library calls run on the compiler's pool (warehouse.set_library_thread, as the engine sets it),
        # which copies the caller's context: the frame reaches them, and they are services, which the witness passes
        import pyarrow as pa
        import pyarrow.ipc
        from legend_lite import _reads, _services
        from legend_lite._pool import _on_compiler_threads

        rows = pa.table({'n': pa.array([7], pa.int64())})
        sink = pa.BufferOutputStream()
        with pyarrow.ipc.new_stream(sink, rows.schema) as w:
            w.write_table(rows)
        seen = []

        class Plane:
            def arrow_chunks(self, sql, on_receipt=None):
                self.sql = sql
                seen.append(current_run())
                seen.append(warehouse._on_library(current_run))
                said = warehouse._on_library(lambda: _services.call('lite_warehouse_call', 'submit', '{}'))
                seen.append(said.split('\n', 1)[0])
                if on_receipt is not None:
                    on_receipt({'statementId': 's'})
                yield sink.getvalue().to_pybytes()

        before = warehouse._on_library
        warehouse.set_library_thread(_on_compiler_threads)
        self.addCleanup(warehouse.set_library_thread, before)
        plane = Plane()
        self.assertEqual(_reads.read_plane(plane, from_plan('SELECT 7 AS n')).column('n').to_pylist(), [7])
        self.assertEqual(seen[:2], ["a remote plane's read"] * 2)
        self.assertIn(seen[2], ('OK', 'ERR'))
        seen.clear()
        heard = []
        typed = _reads.run_on_plane(plane, compiler.Plan(from_plan('SELECT 7 AS n'), (compiler.Column('n', 'Integer', {}),)),
                                    heard.append)
        self.assertEqual(typed.column('n').to_pylist(), [7])
        self.assertEqual(heard, [{'statementId': 's'}])
        self.assertEqual(seen[:2], ["a remote plane's read"] * 2)
        self.assertIsNone(current_run())

    def test_a_planner_entry_reached_through_a_plane_inside_its_run_is_refused_by_name(self):
        # the build rule proves :run imports no planner; a plane is handed in, so what it does is the witness's to see:
        # a planner entry it asks inside the run -- on the caller's thread or through the compiler's pool, as the
        # warehouse's binding runs -- is refused by name (the orchestrator's ruling, 2026-11-15)
        from legend_lite import _reads
        from legend_lite._pool import _on_compiler_threads

        class Planning:
            def __init__(self, ask):
                self.ask = ask

            def arrow_chunks(self, sql, on_receipt=None):
                self.ask()
                yield b''

        for ask in (lambda: library().call('lite_session_setup', 'DuckDB'),
                    lambda: _on_compiler_threads(lambda: compiler.session_setup('DuckDB'))):
            with self.assertRaisesRegex(RuntimeError, r"asked 'lite_session_setup' inside a run \(a remote plane's read\)"):
                _reads.read_plane(Planning(ask), from_plan('SELECT 1'))
        self.assertIsNone(current_run())

    def test_a_reads_setup_and_pin_are_the_same_after_a_register_and_a_settings_change(self):
        # the session's setup and the pin are planned once, when the Frames is made: they depend on the database's type
        # and a constant name alone -- a read after a frame registered and the connection's own settings changed sends
        # exactly the statements a read sent before (its frames' attachments are data, attached, never statements)
        from unittest import mock
        import pyarrow as pa
        from legend_lite import _reads
        from legend_lite.frames import Frames

        con = duckdb.connect()
        frames = Frames(con)
        sent = []
        real = _reads.send

        def recording(target, statement, params=None):
            if current_run() == 'a read begun':
                sent.append((str(statement), params))
            return real(target, statement, params)

        with mock.patch.object(_reads, 'send', recording):
            with frames.reading():
                pass
            before = list(sent)
            sent.clear()
            frames.register('later', pa.table({'x': [1, 2]}))
            con.execute("SET TimeZone = 'America/New_York'")  # the person's own connection, theirs to set
            con.execute("SET threads = 2")
            with frames.reading():
                pass
        self.assertTrue(before, 'a read sends its setup and its pin')
        self.assertEqual(sent, before)


class FollowupTest(unittest.TestCase):
    """The C5 follow-up (2026-11-15): a read's rollback said, never swallowed; a stream's transaction rolled back whatever
    fails after its begin."""

    def test_a_reads_rollback_that_fails_is_said(self):
        # a rollback fails only when no transaction is open on the read's cursor any longer (measured, duckdb 1.5.5): one
        # ended by a statement of the read's own here. The read ended well: the rollback's failure is raised
        import pyarrow as pa
        from legend_lite.frames import Frames
        frames = Frames()
        frames.register('t', pa.table({'x': [1]}))
        with self.assertRaisesRegex(duckdb.TransactionException, 'cannot rollback - no transaction is active'):
            with frames.reading() as read:
                read.execute(PlannedSql('COMMIT'))  # the test's own statement: the read's transaction ended inside it
        # the read failed: its failure stays the one raised, the rollback's noted on it
        with self.assertRaises(KeyError) as failed:
            with frames.reading() as read:
                read.execute(PlannedSql('COMMIT'))
                raise KeyError('the read failed')
        self.assertEqual(failed.exception.args, ('the read failed',))
        self.assertTrue(any("rolling the read's transaction back failed: TransactionException" in n
                            for n in getattr(failed.exception, '__notes__', [])), failed.exception)
        # a read that ended well, its transaction open: rolled back, nothing said
        with frames.reading() as read:
            self.assertEqual(read.run(frames['t'].plan('->select(~[x])')).num_rows, 1)

    def test_a_failure_at_the_fetch_says_no_held_value(self):
        # the C5 follow-up's audit item 1 (2026-11-15): DuckDB fails at the fetch as well as at the statement (a remote file
        # read as its rows stream), and a presigned link's expiry there repeats its URL; run_plan, run_arrow, run_rows_json
        # and count_of take every held value out of either failure's words
        from legend_lite import _reads
        token = 'https://bucket.example/f.parquet?X-Amz-Signature=sig-0123456789abcdef'

        class Failing:
            description = [('n',)]

            def __getattr__(self, name):
                def fetch(*_a):
                    raise duckdb.IOException(f'HTTP Error: 403 (Forbidden) reading {token}')
                return fetch

        class Connection:
            def execute(self, sql, params=None):
                return Failing()

        db = Database(Connection())
        plan = compiler.Plan(from_plan('SELECT 1 AS n'), (compiler.Column('n', 'Integer', {}),))
        for run in (lambda: _reads.run_plan(db, plan, (token,)), lambda: _reads.run_arrow(db, plan.sql, (token,)),
                    lambda: _reads.run_rows_json(db, plan.sql, (token,)), lambda: _reads.count_of(db, plan, (token,))):
            with self.assertRaises(duckdb.IOException) as failed:
                run()
            self.assertIn('HTTP Error: 403', str(failed.exception))
            self.assertNotIn('sig-0123456789abcdef', str(failed.exception))
            self.assertNotIn('bucket.example', str(failed.exception))

    def test_a_stream_whose_plane_fails_at_once_is_rolled_back(self):
        # a plane failing as its statement is asked for (no generator: at once) failed before the stream's try, and left
        # the transaction its begin opened; it is rolled back now, and the plane's failure raised
        class Plane:
            def arrow_chunks(self, sql, on_receipt=None):
                raise ConnectionResetError('the plane refused at once')

        plan = compiler.snap_stream({'databaseType': 'DuckDB', 'plane': 'warehouse', **_pull(), 'table': 'never'})
        step = json.loads(plan)['root']['steps'][0]
        witness = _Witness(duckdb.connect())
        with self.assertRaisesRegex(ConnectionResetError, 'the plane refused at once'):
            projects.run_host_load(plan, Database(witness), planes={'warehouse': Plane()})
        self.assertEqual(witness.received, [*step['transaction']['begin'], *step['transaction']['rollback']])


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
        answered.record('plan', 'OK\n' + json.dumps({'sql': 'SELECT a AS "DROP TABLE t" FROM x', 'raises': {}, 'type': {
            'columns': [{'name': 'DROP TABLE t', 'type': 'String'}]}}))
        answered.record('executionPlan', 'OK\n' + json.dumps({'format': 'legend-lite-plan', 'raises': {}, 'root': {
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
        answered.record('executionPlan', 'OK\n' + json.dumps({'raises': {}, 'root': {'_type': 'sequence', 'steps': [
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
        answered.record('plan', 'OK\n' + json.dumps({'sql': 'SELECT * FROM t', 'raises': {}}))
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
        # the hosted runner's sends, by the run side's own functions (C5-P: _headless's query and statement)
        from legend_lite._reads import run_bridged_query, run_bridged_statement
        keys = [tuple(r.values()) for r in run_bridged_query(
            con, None, across_the_bridge(keyed['keys']['statement'], "the hosted DataCube's query", answered)).to_pylist()]
        self.assertEqual(len(keys), 2, keys)
        written = _keyed_text(body['statement'], tuple(body['regions']), len(keys))
        params = []
        for r in body['regions']:
            for key in keys:
                for s in range(r['firstSlot'], r['firstSlot'] + r['slotCount']):
                    params.append(key[body['slots'][s]['binding']['cell']])
        rows = [tuple(r.values()) for r in run_bridged_statement(
            con, across_the_bridge(written, "the hosted DataCube's query", answered), params).to_pylist()]
        # each year's total per city, the cities in the order the keys were read
        totals = {2000: {'NYC': 5, 'LDN': 3}, 2011: {'NYC': 2}}
        self.assertEqual(sorted(rows), [(yr, *[totals[yr].get(key[0]) for key in keys]) for yr in (2000, 2011)])
        for wrong in (written + ' ', _keyed_text(body['statement'], tuple(body['regions']), len(keys)).replace('SELECT', 'SELECT 1,', 1)):
            with self.assertRaises(NotPlanned):
                across_the_bridge(wrong, "the hosted DataCube's query", answered)

class ARaiseIsWorded(unittest.TestCase):
    """A product raise in lite's words wherever it fires (#104's M2(b), 2026-11-15): the envelope read as Java's runner
    and the tab's read it, one shared fixture; a raise DuckDB fires at a FETCH worded as one at the send."""

    def test_each_envelope_reads_as_javas_runner_and_the_tabs_read_it(self):
        import os

        from legend_lite._raised import declare_statement_sites, raised_kind, raised_words
        fixture = json.loads(open(os.environ['LEGEND_LITE_RAISE_FIXTURE'], encoding='utf-8').read())
        # the sites are the statement's (PlanJson v56; phase D's Phase 2): declared first, as a plan read does
        declare_statement_sites(fixture['statement'], fixture['sites'], 'the fixture')
        cases = fixture['cases']
        self.assertGreater(len(cases), 5)
        for case in cases:
            with self.subTest(case['name']):
                if case.get('refused'):
                    with self.assertRaisesRegex(ValueError, 'which the statement does not declare'):
                        raised_words(case['message'], fixture['statement'])
                    continue
                said = raised_words(case['message'], fixture['statement'])
                self.assertEqual(case['message'] if said is None else said, case['words'])
                self.assertEqual(raised_kind(case['message'], fixture['statement']), case.get('kind'))

    def test_a_token_its_statement_does_not_declare_is_refused_by_name(self):
        from legend_lite._raised import raised_words
        with self.assertRaisesRegex(ValueError, 'LG#1, which the statement does not declare'):
            raised_words('\x1f\x1cLG#1\x1f', 'SELECT a statement no plan declared')

    def test_a_raise_fired_at_a_fetch_is_worded(self):
        from legend_lite._planned import Result
        from legend_lite._raised import declare_statement_sites

        # what a plan that divides declares for its statement (PlanJson v56's sites)
        declare_statement_sites('SELECT 1 / 0', [{'n': 1, 'kind': 'DIVIDE_BY_ZERO', 'where': None,
                                                  'message': 'division by zero', 'upstream': None}], 'a plan')

        class Failing:
            def fetchall(self):
                raise RuntimeError('Invalid Input Error: Attempting to execute an unsuccessful or closed pending query'
                                   ' result\nError: Invalid Input Error: \x1f\x1cLG#1\x1f')

        with self.assertRaises(RuntimeError) as raised:
            Result(Failing(), 'SELECT 1 / 0').fetchall()
        self.assertEqual(str(raised.exception), 'division by zero')

if __name__ == '__main__':
    unittest.main()


class BindAtRun(unittest.TestCase):
    """A plan's parameters bound at RUN (the lite API spec's §4, 2026-11-17; Java's PlanParameters.check and the TS host's
    checkParameters, one rule): each declaration read strictly; every value checked before anything is sent, every
    problem named at once, the missing first; each value converted by its declared Pure type; an Integer within the
    64-bit range on every host, a Python bool no Integer."""

    @staticmethod
    def p(name, type_, lower=1, upper=1, enum=()):
        from legend_lite._plans import Declared
        return Declared(name, type_, lower, upper, tuple(enum))

    def test_a_declaration_is_read_strictly(self):
        from legend_lite._plans import PlanParametersRefused, declared
        ok = {'name': 'd', 'type': 'String', 'multiplicity': {'lower': 0}, 'enumValues': []}
        self.assertEqual(declared(ok), self.p('d', 'String', 0, None))
        for bad in ({**ok, 'extra': 1}, {**ok, 'multiplicity': {'lower': 0, 'upper': None}}, {**ok, 'enumValues': [1]},
                    {**ok, 'multiplicity': {'lower': True}}, {'name': 'd'}, 'd'):
            with self.subTest(bad=bad), self.assertRaisesRegex(
                    PlanParametersRefused, "a plan parameter declared without its name, type, multiplicity and enumeration's values"):
                declared(bad)

    def test_every_problem_is_named_at_once_the_missing_first(self):
        from legend_lite._params import ParametersRefused, check
        declared = [self.p('a', 'Integer'), self.p('b', 'String', 0, None), self.p('c', 'Integer'), self.p('x', 'Boolean')]
        with self.assertRaises(ParametersRefused) as refused:
            check(declared, {'b': 'one value', 'c': [1, 2], 'x': 'yes', 'unknown': 1})
        self.assertEqual(str(refused.exception),
                         "no value for the parameter(s) a:Integer[1]; values their parameters do not take: "
                         "[parameter 'b' (String[*]): takes a list, given one value; "
                         "parameter 'c' (Integer[1]): takes one value, given a list of 2; "
                         "parameter 'x' (Boolean[1]): given \"yes\" (string): a Boolean is a boolean]")
        with self.assertRaises(ParametersRefused) as refused:
            check([self.p('n', 'Integer', 1, 2)], {'n': [1, 2, 3]})
        self.assertEqual(str(refused.exception),
                         "values their parameters do not take: [parameter 'n' (Integer[1..2]): takes 1..2 values, given 3]")

    def test_each_type_takes_its_values(self):
        from legend_lite._params import Many, NoValue, One, check
        cases = [
            ('Integer', 2 ** 63 - 1, 2 ** 63 - 1), ('Integer', -(2 ** 63), -(2 ** 63)), ('Float', 1.5, 1.5), ('Float', 2, 2.0),
            ('Decimal', '-12.30', '-12.30'), ('Number', 3, 3), ('Number', 0.5, 0.5), ('Number', '1.25', '1.25'),
            ('Boolean', False, False), ('String', '', ''), ('StrictDate', '2026-11-17', '2026-11-17'),
            ('DateTime', '2026-11-17T09:30Z', '2026-11-17 09:30:00.000000000'),
            ('DateTime', '2026-11-17 09:30:01.5', '2026-11-17 09:30:01.500000000'),
            ('Date', '2026-11-17', '2026-11-17'), ('Date', '2026-11-17T01:02:03.123456789', '2026-11-17 01:02:03.123456789'),
        ]
        for type_, given, bound in cases:
            with self.subTest(type=type_, given=given):
                self.assertEqual(check([self.p('v', type_)], {'v': given}), {'v': One(bound)})
        self.assertEqual(check([self.p('c', 'p::Colour', enum=('RED', 'BLUE'))], {'c': 'BLUE'}), {'c': One('BLUE')})
        self.assertEqual(check([self.p('o', 'String', 0, 1), self.p('l', 'Integer', 0, None)], {'l': []}),
                         {'o': NoValue(), 'l': NoValue()})
        self.assertEqual(check([self.p('l', 'Integer', 1, None)], {'l': [1, 2]}), {'l': Many((1, 2))})

    def test_each_type_refuses_what_it_does_not_take(self):
        from legend_lite._params import ParametersRefused, check
        cases = [
            ('Integer', True, 'given true (boolean): an Integer is a whole number within the 64-bit range'),
            ('Integer', 2 ** 63, f'given {2 ** 63} (number): an Integer is a whole number within the 64-bit range'),
            ('Integer', 1.0, 'given 1.0 (number): an Integer is a whole number within the 64-bit range'),
            ('Number', 2 ** 63, f'given {2 ** 63} (number): an Integer is a whole number within the 64-bit range'),
            ('Float', float('nan'), 'nan is not finite: no SQL literal stands for it'),
            ('Decimal', '1e3', 'given "1e3" (string): a Decimal is its plain digits as text'),
            ('StrictDate', '0000-01-01', '0000-01-01 is outside the years 1 to 9999: no date text a database reads stands for it'),
            ('DateTime', '2026-11-17', 'given "2026-11-17" (string): a DateTime is its ISO 8601 text in UTC, '
                                       'YYYY-MM-DDTHH:MM[:SS[.fffffffff]]'),
            ('p::Unknown', 1, 'a p::Unknown value is not bound by a plan'),
        ]
        for type_, given, why in cases:
            with self.subTest(type=type_, given=given), self.assertRaises(ParametersRefused) as refused:
                check([self.p('v', type_)], {'v': given})
            self.assertEqual(str(refused.exception),
                             f"values their parameters do not take: [parameter 'v' ({type_}[1]): {why}]")
        with self.assertRaises(ParametersRefused) as refused:
            check([self.p('c', 'p::Colour', enum=('RED',))], {'c': 'BLUE'})
        self.assertIn('"BLUE" is not a value of p::Colour [RED]', str(refused.exception))
        with self.assertRaises(ParametersRefused) as refused:
            check([self.p('l', 'Integer', 0, None)], {'l': [1, None]})
        self.assertIn('a list holds no null', str(refused.exception))

    def test_a_plans_slots_bind_payloads_and_parameters_in_order(self):
        import hashlib
        from legend_lite._params import bind
        from legend_lite._plans import Plan, PlanParamsRefused, plan_binding
        payload = '[1,2]'
        digest = hashlib.sha256(payload.encode()).hexdigest()
        plan = {'parameters': [{'name': 'd', 'type': 'String', 'multiplicity': {'lower': 0, 'upper': 1}, 'enumValues': []}],
                'root': {'_type': 'result', 'sql': {'statement': 'S', 'content': {digest: payload}, 'slots': [
                    {'parameter': 'd', 'binding': {'_type': 'one', 'nullType': 'VARCHAR'}},
                    {'parameter': 'p', 'binding': {'_type': 'payload', 'digest': digest}},
                    {'parameter': 'd', 'binding': {'_type': 'one', 'asText': {}}}]}}}
        payloads, parameters, slots = plan_binding(plan)
        bound = Plan(PlannedSql.__new__(PlannedSql), (), payloads, parameters, slots)
        self.assertEqual(bind(bound, {'d': 'x'}), ('x', payload, 'x'))
        self.assertEqual(bind(bound, {}), (None, payload, None), "an optional given none binds a typed null")
        # a one slot of no declared parameter, and a dynamic pivot that takes parameters, refused by name
        undeclared = {**plan, 'parameters': []}
        with self.assertRaisesRegex(PlanParamsRefused, "'d' binds a value the run supplies"):
            plan_binding(undeclared)
        with self.assertRaisesRegex(PlanParamsRefused, 'a dynamic pivot that takes parameters is not run here'):
            plan_binding({**plan, 'root': {'_type': 'keyed'}})
