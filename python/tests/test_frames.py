"""Frames as Legend tables (legend_lite.frames): answers checked against pandas computing the same; Live reads
the frame as it is at each query, Snapped as it was copied; pandas, polars and Arrow answer alike; the model
writer's conversions hold exact values; and what a frame cannot be is refused."""

import datetime
import unittest
from decimal import Decimal

import pandas as pd
import polars as pl
import pyarrow as pa

import legend_lite as ll

GROUP = "->filter(x|$x.qty > 5)->groupBy(~[desk], ~[q: x|$x.qty : y|$y->sum()])->sort(~desk->ascending())"


def trades():
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'desk': ['FX', 'EQ', 'FX', 'RATES', 'EQ'],
        'qty': [10.5, 20.0, 30.25, 1.0, 7.5],
    })


def by_desk(df):
    """What GROUP computes, computed by pandas."""
    kept = df[df.qty > 5]
    return [{'desk': d, 'q': q} for d, q in kept.groupby('desk')['qty'].sum().sort_index().items()]


class Answers(unittest.TestCase):
    def test_a_query_answers_as_pandas_computes_it(self):
        df = trades()
        table = ll.Frames().register('trades', df)
        self.assertEqual(table.execute(GROUP).to_pylist(), by_desk(df))

    def test_the_compiler_types_the_frame(self):
        table = ll.Frames().register('trades', trades())
        self.assertEqual([(c.name, c.type) for c in table.columns], [('id', 'Integer'), ('desk', 'String'), ('qty', 'Float')])
        self.assertEqual(table.accessor, '#>{trades::DB.trades}#')
        self.assertIn('type: DuckDB;', table.model)

    def test_a_query_is_text_a_tree_or_a_step_on_the_table(self):
        table = ll.Frames().register('trades', trades())
        step = table.execute("->filter(x|$x.desk == 'EQ')->select(~[id])").to_pylist()
        text = table.execute("|#>{trades::DB.trades}#->filter(x|$x.desk == 'EQ')->select(~[id])").to_pylist()
        tree = table.execute(ll.parse("|#>{trades::DB.trades}#->filter(x|$x.desk == 'EQ')->select(~[id])")).to_pylist()
        self.assertEqual(step, [{'id': 2}, {'id': 5}])
        self.assertEqual(text, step)
        self.assertEqual(tree, step)

    def test_pandas_polars_and_arrow_answer_alike(self):
        df = trades()
        frames = ll.Frames()
        answers = [frames.register(name, frame).execute(GROUP).to_pylist()
                   for name, frame in (('a', df), ('b', pl.from_pandas(df)), ('c', pa.Table.from_pandas(df)))]
        self.assertEqual(answers, [by_desk(df)] * 3)

    def test_a_name_that_needs_quoting_in_sql_reads_as_written(self):
        df = pd.DataFrame({'total pnl': [1.5, 2.5], 'select': ['a', 'b']})
        table = ll.Frames().register('t', df)
        self.assertEqual(table.execute("->filter(x|$x.select == 'b')").to_pylist(), [{'total pnl': 2.5, 'select': 'b'}])


class LiveAndSnapped(unittest.TestCase):
    def test_live_reads_the_frame_as_it_is_at_each_query(self):
        df = trades()
        table = ll.Frames().register('trades', df)
        before = table.execute(GROUP).to_pylist()
        df.loc[0, 'qty'] = 1000.0
        self.assertNotEqual(table.execute(GROUP).to_pylist(), before)
        self.assertEqual(table.execute(GROUP).to_pylist(), by_desk(df))

    def test_snapped_keeps_the_frame_as_it_was_copied(self):
        df = trades()
        table = ll.Frames().register('trades', df, mode='snapped')
        copied = by_desk(df)
        df.loc[0, 'qty'] = 1000.0
        self.assertEqual(table.execute(GROUP).to_pylist(), copied)

    def test_a_live_frame_that_gains_a_column_gets_its_model_written_again(self):
        df = trades()
        table = ll.Frames().register('trades', df)
        df['fee'] = [1, 2, 3, 4, 5]
        self.assertEqual(table.execute('->select(~[id, fee])->filter(x|$x.fee > 4)').to_pylist(), [{'id': 5, 'fee': 5}])
        self.assertEqual(table.columns[-1].name, 'fee')

    def test_a_function_frame_follows_the_frame_it_returns(self):
        holder = {'df': trades()}
        table = ll.Frames().register('trades', lambda: holder['df'])
        holder['df'] = trades().iloc[:2]
        self.assertEqual(table.execute('->select(~[id])').to_pylist(), [{'id': 1}, {'id': 2}])


class Names(unittest.TestCase):
    def test_registering_a_name_again_replaces_its_table_in_either_mode(self):
        frames = ll.Frames()
        live = frames.register('trades', trades())
        snapped = frames.register('trades', trades().iloc[:1], mode='snapped')
        self.assertEqual(snapped.execute('->select(~[id])').to_pylist(), [{'id': 1}])
        again = frames.register('trades', trades())
        self.assertEqual(len(again.execute('->select(~[id])')), 5)
        for old in (live, snapped):
            with self.assertRaises(ValueError):
                old.execute('->select(~[id])')

    def test_names_are_one_name_whatever_their_case_as_in_duckdb(self):
        frames = ll.Frames()
        frames.register('Trades', trades())
        replaced = frames.register('trades', trades().iloc[:2])
        self.assertIs(frames['TRADES'], replaced)
        self.assertEqual(len(replaced.execute('->select(~[id])')), 2)

    def test_a_table_frames_did_not_make_is_never_replaced(self):
        frames = ll.Frames()
        frames.connection.execute('CREATE TABLE ledger (id INTEGER)')
        with self.assertRaises(ValueError):
            frames.register('Ledger', trades())
        self.assertEqual(frames.connection.execute('SELECT count(*) FROM ledger').fetchone(), (0,))

    def test_a_frame_that_cannot_be_served_leaves_its_name_free(self):
        # a Live column of nothing but nulls (no type a Pure column can have), a frame of no columns: refused, and
        # nothing of either left behind
        for frame, mode in ((pd.DataFrame({'id': [1, 2], 'nothing': [None, None]}), 'live'),
                            (pd.DataFrame(), 'live'), (pd.DataFrame(), 'snapped')):
            frames = ll.Frames()
            with self.assertRaises(Exception):
                frames.register('trades', frame, mode=mode)
            self.assertNotIn('trades', frames)
            self.assertEqual(len(frames.register('trades', trades(), mode=mode).execute('->select(~[id])')), 5)

    def test_unregister_and_close_take_the_tables_out(self):
        frames = ll.Frames()
        one = frames.register('one', trades())
        frames.register('two', trades(), mode='snapped')
        frames.unregister('one')
        with self.assertRaises(ValueError):
            one.execute('->select(~[id])')
        frames.close()
        left = frames.connection.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name IN ('one', 'two')").fetchone()
        views = frames.connection.execute("SELECT count(*) FROM duckdb_views() WHERE view_name IN ('one', 'two')").fetchone()
        self.assertEqual((left, views), ((0,), (0,)))

    def test_a_pure_literal_cannot_name_a_frame(self):
        with self.assertRaises(ValueError):
            ll.Frames().register('true', trades())


class Session(unittest.TestCase):
    def test_the_session_answers_in_utc_on_any_machine(self):
        frames = ll.Frames()
        self.assertEqual(frames.connection.execute("SELECT current_setting('TimeZone')").fetchone(), ('UTC',))

    def test_a_given_connection_is_set_the_same_way(self):
        import duckdb
        con = duckdb.connect()
        con.execute("SET TimeZone='Pacific/Kiritimati'")
        ll.Frames(con)
        self.assertEqual(con.execute("SELECT current_setting('TimeZone')").fetchone(), ('UTC',))


class Conversions(unittest.TestCase):
    """A column the model declares by a conversion (the writer's own: copySelectList) holds its value exactly."""

    def test_an_unsigned_64_bit_integer_is_an_exact_decimal(self):
        frame = pa.table({'u': pa.array([2 ** 64 - 1, 1], pa.uint64())})
        for mode in ('live', 'snapped'):
            table = ll.Frames().register('t', frame, mode=mode)
            self.assertEqual([(c.name, c.type) for c in table.columns], [('u', 'Decimal')])
            self.assertEqual(table.execute('->select(~[u])').to_pylist(), [{'u': Decimal(2 ** 64 - 1)}, {'u': Decimal(1)}])

    def test_a_zoned_timestamp_is_its_utc_instant(self):
        at = pd.Timestamp('2026-10-08 09:30', tz='America/New_York')
        for mode in ('live', 'snapped'):
            table = ll.Frames().register('t', pd.DataFrame({'at': [at]}), mode=mode)
            self.assertEqual([(c.name, c.type) for c in table.columns], [('at', 'DateTime')])
            self.assertEqual(table.execute('->select(~[at])').to_pylist(), [{'at': datetime.datetime(2026, 10, 8, 13, 30)}])


class Refusals(unittest.TestCase):
    def test_a_name_is_a_plain_identifier(self):
        with self.assertRaises(ValueError):
            ll.Frames().register('my trades', trades())

    def test_a_mode_is_live_or_snapped(self):
        with self.assertRaises(ValueError):
            ll.Frames().register('t', trades(), mode='linked')

    def test_a_stream_cannot_be_live(self):
        stream = pa.RecordBatchReader.from_batches(pa.schema([('a', pa.int64())]), [])
        with self.assertRaises(ValueError):
            ll.Frames().register('t', stream)

    def test_a_query_the_compiler_refuses_says_why(self):
        table = ll.Frames().register('trades', trades())
        with self.assertRaises(ll.LegendError) as e:
            table.execute('->filter(x|$x.nope == 1)')
        self.assertIn("'nope'", e.exception.message)


if __name__ == '__main__':
    unittest.main()
