"""THE HOST EXECUTOR'S BENCHMARK, Python's (docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md, H0: the baseline; manual, never in
a gate): a frame of the five columns Java's benchmark reads (//core:seam_benchmark: an integer id, a text, a
DECIMAL(18,2), a timestamp, a nullable integer) queried at 10 to 1,000,000 rows, on four paths per size:

    duckdb   -- the planned SQL run on the frames' connection, its Arrow table and nothing else: the ceiling here;
    frames   -- Table.execute, the dataframe API: planned, run, typed by the plan (a pyarrow.Table);
    arrow    -- the engine's execute in upstream's Arrow format (``serializationFormat=ARROW_IPC``), as DataCube asks;
    json     -- the engine's execute in legend-engine's JSON: the rows fetched as Python objects, written as JSON text
                (compiler.host_rows) and spelled by the native planner (execute_json_answer).

Each size: one cold call, then calls timed one by one until BENCH_MEASURE_MS has passed (after BENCH_WARM_MS of warm-up);
the median and p90 printed, with a digest of each answer so a change is checked to answer the same bytes. Run:

    bazel test //python:executor_bench --test_output=streamed
    (--test_env=BENCH_SIZES=10,1000 --test_env=BENCH_WARM_MS=500 --test_env=BENCH_MEASURE_MS=1000)
"""

import hashlib
import json
import os
import time
import unittest

import duckdb
import pyarrow as pa
import pyarrow.ipc

import legend_lite as ll
from legend_lite.engine import Engine

SIZES = [int(s) for s in os.environ.get('BENCH_SIZES', '10,100,1000,10000,100000,1000000').split(',')]
WARM_MS = float(os.environ.get('BENCH_WARM_MS', '1000'))
MEASURE_MS = float(os.environ.get('BENCH_MEASURE_MS', '2000'))
EXECUTE = '/api/pure/v1/execution/execute'


def bench_table(rows: int) -> pa.Table:
    """The five columns, made by DuckDB on a connection of its own (no Python object per cell)."""
    with duckdb.connect() as made:
        made.execute("SET TimeZone = 'UTC'")
        return made.execute(f"""SELECT i AS id, 'name' || CAST(i AS VARCHAR) AS name,
            CAST(CAST(i AS DECIMAL(18,2)) / 7 AS DECIMAL(18,2)) AS amount,
            TIMESTAMP '2024-01-01 00:00:00' + INTERVAL (i % 86400) SECOND AS ts,
            CASE WHEN i % 10 = 0 THEN NULL ELSE CAST(i % 1000 AS INTEGER) END AS bucket
            FROM range({rows}) t(i)""").to_arrow_table()


def timed(work):
    work()
    warm_until = time.perf_counter() + WARM_MS / 1000
    while time.perf_counter() < warm_until:
        work()
    times = []
    until = time.perf_counter() + MEASURE_MS / 1000
    while time.perf_counter() < until or len(times) < 3:
        t0 = time.perf_counter()
        work()
        times.append((time.perf_counter() - t0) * 1000)
    times.sort()
    return times[len(times) // 2], times[int(len(times) * 0.9)], len(times)


def digest_table(table: pa.Table) -> str:
    sink = pa.BufferOutputStream()
    with pyarrow.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()[:16]


def ms(x: float) -> str:
    return f'{x:.3f}' if x < 10 else f'{x:.1f}'


class ExecutorBench(unittest.TestCase):

    def test_frames_and_engine_10_to_1m_rows(self):
        frames = ll.Frames()
        table = frames.register('bench', bench_table(max(SIZES)))
        engine = Engine(frames)
        print(f'[bench] python; sizes {SIZES}; warm {WARM_MS:.0f} ms, measure {MEASURE_MS:.0f} ms', flush=True)
        for rows in SIZES:
            steps = f'->select(~[id, name, amount, ts, bucket])->sort(~id->ascending())->limit({rows})'
            plan = table.plan(steps)
            tree = ll.parse('|' + table.accessor + steps + '->from(' + table.runtime + ')')
            body = json.dumps({
                'clientVersion': 'vX_X_X', 'function': tree, 'model': {'_type': 'text', 'code': table.model},
                'context': {'_type': 'BaseExecutionContext', 'queryTimeOutInSeconds': 60, 'enableConstraints': True},
            })
            answers = {}

            def raw():
                answers['duckdb'] = frames.connection.execute(plan.sql).to_arrow_table()

            def dataframe():
                answers['frames'] = table.execute(steps)

            def arrow():
                answers['arrow'] = engine.answer('POST', EXECUTE, 'serializationFormat=ARROW_IPC', body)

            def rows_json():
                answers['json'] = engine.answer('POST', EXECUTE, '', body)

            results = {name: timed(work) for name, work in
                       (('duckdb', raw), ('frames', dataframe), ('arrow', arrow), ('json', rows_json))}
            self.assertEqual(answers['duckdb'].num_rows, rows)
            self.assertEqual(answers['frames'].num_rows, rows)
            for name in ('arrow', 'json'):
                self.assertEqual(answers[name].status, 200, answers[name].body[:300])
            digests = {
                'duckdb': digest_table(answers['duckdb']), 'frames': digest_table(answers['frames']),
                'arrow': hashlib.sha256(answers['arrow'].body).hexdigest()[:16],
                'json': hashlib.sha256(answers['json'].body).hexdigest()[:16],
            }
            line = ' | '.join(f'{name} {ms(m)} ms (p90 {ms(p)}, {n} runs, {digests[name]})'
                              for name, (m, p, n) in results.items())
            print(f'[bench] rows={rows} | {line}', flush=True)
        frames.close()


if __name__ == '__main__':
    unittest.main()
