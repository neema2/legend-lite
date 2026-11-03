"""THE ARROW HANDOFF (docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md R3, its exit): a host's rows handed to the core as the
Arrow its database answered give the answers the same rows give handed over as JSON text -- legend-engine's JSON byte
for byte, and the Arrow answer value for value -- over the conformance suite's corpus (//conformance), each case planned
and run as Python's engine plans and runs it (its project database, its DuckDB). The handoff is the core's
(``compiler.execute_json_answer_arrow``: a pointer and a length, read where pyarrow holds it).
"""

import datetime
import io
import json
import os
import re
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc

import legend_lite as ll
from legend_lite import _json, compiler
from legend_lite.engine import Engine

RUNTIME = 'conf::RT'
TRACE = re.compile(r'executionTraceID\\" : \\"[0-9a-f-]{36}')


def corpus(name: str) -> str:
    return Path(os.environ[name]).read_text(encoding='utf-8')


def cases() -> list[tuple[str, str]]:
    out = []
    for line in corpus('LEGEND_LITE_CONFORMANCE_QUERIES').splitlines():
        if line and not line.startswith('#'):
            name, query = line.split('\t', 1)
            out.append((name, query))
    return out


def arrow_bytes(table: pa.Table) -> pa.Buffer:
    sink = pa.BufferOutputStream()
    with pyarrow.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


def arrow_answer(body: bytes) -> pa.Table:
    """An Arrow answer (one zstd frame of an IPC stream), its schema's trace masked in its metadata."""
    with pa.CompressedInputStream(pa.BufferReader(body), 'zstd') as stream:
        table = pyarrow.ipc.open_stream(stream.read()).read_all()
    metadata = {k: TRACE.sub('executionTraceID', v.decode('utf-8')) for k, v in (table.schema.metadata or {}).items()}
    return table.replace_schema_metadata(metadata)


def body_of(model: str, query: str, runtime: str) -> str:
    # the library's own JSON: a parsed tree keeps its numbers exact (Decimal)
    return _json.dumps({
        'clientVersion': 'vX_X_X', 'context': {'_type': 'BaseExecutionContext'},
        'function': ll.parse('|' + query + '->from(' + runtime + ')'),
        'model': {'_type': 'text', 'code': model},
    })


def planned_frame(engine: Engine, frames: ll.Frames, table) -> str:
    """A frame's query planned as the engine's JSON execute plans it: the plan record's text."""
    with frames.serving() as models:
        answer = engine.planner.execute_json_plan(body_of(table.model, table.accessor + '->sort(~ID->ascending())',
                                                          table.runtime), list(models.values()))
    assert answer.status == 200, answer.body
    return answer.body


class ArrowHandoff(unittest.TestCase):

    def test_every_case_answers_alike_from_arrow_and_from_text(self):
        model = corpus('LEGEND_LITE_CONFORMANCE_MODEL')
        frames = ll.Frames()
        engine = Engine(frames)
        compared, keyed, refused = 0, 0, []
        try:
            for name, query in cases():
                body = body_of(model, query, RUNTIME)
                with frames.serving() as models, engine._project_of(body, models) as (served, connection):
                    planned = engine.planner.execute_json_plan(body, served)
                    if planned.status != 200:
                        refused.append(name)   # a refusal at planning: no rows
                        continue
                    if compiler.keyed_statement(planned.body) is not None:
                        keyed += 1   # a pivot whose columns its data names: its own run (D18 D), not one statement's
                        continue
                    statement = compiler.answer_statement(planned.body)
                    try:
                        cursor = connection.execute(statement)
                    except Exception:
                        refused.append(name)   # the database's refusal: no rows
                        continue
                    rows = compiler.host_rows([d[0] for d in cursor.description], cursor.fetchall())
                    arrow = arrow_bytes(connection.execute(statement).to_arrow_table())
                with self.subTest(case=name):
                    text = compiler.execute_json_answer(planned.body, rows)
                    from_arrow = compiler.execute_json_answer_arrow(planned.body, arrow)
                    # a case with rows answers them: the two handoffs alike, and an answer (never two like refusals)
                    self.assertEqual((200, 200), (text.status, from_arrow.status), from_arrow.body[:300])
                    self.assertEqual(text.content_type, from_arrow.content_type)
                    self.assertEqual(TRACE.sub('executionTraceID', text.body),
                                     TRACE.sub('executionTraceID', from_arrow.body))
                    folded = _answer_bytes(compiler.library().call('lite_execute_arrow_answer', planned.body, rows))
                    # the bytes as Python's bytes too (the library reads either where it lies)
                    status, content_type, arrow_body = compiler.execute_arrow_answer_arrow(planned.body,
                                                                                         arrow.to_pybytes())
                    self.assertEqual((200, folded[0], folded[1]), (status, status, content_type))
                    expected, actual = arrow_answer(folded[2]), arrow_answer(arrow_body)
                    self.assertEqual(expected.schema.metadata, actual.schema.metadata)
                    self.assertTrue(expected.equals(actual, check_metadata=True), name)
                    compared += 1
        finally:
            frames.close()
        # every case of the corpus with rows compared, none skipped silently: the refusals are its err cases, and one
        # keyed pivot
        self.assertEqual(sorted(refused), sorted(n for n, _ in cases() if n.startswith('err')))
        self.assertEqual(1, keyed)
        self.assertEqual(len(cases()) - len(refused) - keyed, compared)

    def test_a_real_is_its_own_digits_from_arrow_as_on_the_jvm(self):
        # a FLOAT (DuckDB's four-byte REAL): the JVM's session reads Float.toString's digits (0.1), and so does the
        # Arrow handoff; the JSON text handoff hands the widened double (0.10000000149011612) -- one of the differences
        # this handoff makes, each toward the JVM's answer (the note's §7)
        frames = ll.Frames()
        try:
            table = frames.register('reals', pa.table({'ID': pa.array([1, 2], pa.int64()),
                                                       'R': pa.array([0.1, None], pa.float32())}))
            plan = planned_frame(Engine(frames), frames, table)
            statement = compiler.answer_statement(plan)
            cursor = frames.connection.execute(statement)
            rows = compiler.host_rows([d[0] for d in cursor.description], cursor.fetchall())
            arrow = arrow_bytes(frames.connection.execute(statement).to_arrow_table())
            self.assertIn('"R":0.10000000149011612', rows)
            text = compiler.execute_json_answer(plan, rows).body
            from_arrow = compiler.execute_json_answer_arrow(plan, arrow).body
            self.assertIn('0.10000000149011612', text)
            self.assertNotIn('0.10000000149011612', from_arrow)
            self.assertIn('0.1', from_arrow)
        finally:
            frames.close()

    def test_extreme_days_and_instants_are_the_databases_from_arrow(self):
        # an infinite DATE and TIMESTAMP and a BC date: the Arrow handoff answers the values DuckDB holds, as the JVM's
        # JDBC session reads them; the text handoff answered what duckdb-python's objects could hold (an infinity as
        # 9999-12-31) or refused (a BC date) -- R3's audit, the note's §7
        frames = ll.Frames()
        try:
            table = frames.register('days', pa.table({
                'ID': pa.array([1], pa.int64()), 'D': pa.array([datetime.date(2024, 1, 1)], pa.date32()),
                'TS': pa.array([datetime.datetime(2024, 1, 1)], pa.timestamp('us'))}))
            plan = planned_frame(Engine(frames), frames, table)
            query = ("SELECT 1::BIGINT AS ID, 'infinity'::DATE AS D, 'infinity'::TIMESTAMP AS TS UNION ALL "
                     "SELECT 2, '-infinity'::DATE, '-infinity'::TIMESTAMP UNION ALL "
                     "SELECT 3, '0054-05-07 (BC)'::DATE, '0054-05-07 (BC) 01:02:03'::TIMESTAMP ORDER BY ID")
            answer = compiler.execute_json_answer_arrow(plan, arrow_bytes(frames.connection.execute(query)
                                                                          .to_arrow_table()))
            self.assertEqual(200, answer.status, answer.body[:300])
            self.assertEqual([row['values'] for row in json.loads(answer.body)['result']['rows']], [
                [1, '5881580-07-11', '294247-01-10T04:00:54.775807000+0000'],
                [2, '-5877641-06-24', '-290308-12-21T19:59:05.224193000+0000'],
                [3, '-53-05-07', '-53-05-07T01:02:03.000000000+0000']])
            # the text handoff (measured 2026-11-03): the BC date's text duckdb-python spells is no date the core reads
            cursor = frames.connection.execute(query)
            text = compiler.execute_json_answer(plan, compiler.host_rows([d[0] for d in cursor.description],
                                                                         cursor.fetchall()))
            self.assertEqual(500, text.status)
            self.assertIn("'0054-05-07T(BC)'", text.body)
        finally:
            frames.close()

    def test_a_stream_that_is_not_the_plans_is_refused_by_name(self):
        frames = ll.Frames()
        try:
            table = frames.register('one', pa.table({'ID': pa.array([1], pa.int64())}))
            plan = planned_frame(Engine(frames), frames, table)
            wrong = arrow_bytes(pa.table({'A': [1], 'B': [2]}))
            answer = compiler.execute_json_answer_arrow(plan, wrong)
            self.assertEqual(500, answer.status)
            self.assertIn('answers 2 columns', answer.body)
            with self.assertRaises(TypeError):
                compiler.execute_json_answer_arrow(plan, 'not bytes')
        finally:
            frames.close()


def _answer_bytes(folded: str) -> tuple[int, str, bytes]:
    """The base64-folded Arrow answer (``Folded.http``): its status, media type and body bytes."""
    import base64
    status, content_type, body = compiler._answer(folded).split('\n', 2)
    if content_type.endswith(';base64'):
        return int(status), content_type[:-len(';base64')], base64.b64decode(body)
    return int(status), content_type, body.encode('utf-8')


if __name__ == '__main__':
    unittest.main()
