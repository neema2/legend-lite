"""Frames as Legend tables (legend_lite.frames): answers checked against pandas computing the same; a frame is read
as it is at each query, a stream as it was read once; pandas, polars and Arrow answer alike; the model writer's
conversions hold exact values; an answer is typed by its plan; and what a frame cannot be is refused."""

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


def streamed(frame):
    """``frame`` as a stream: read once, so registered as a table of its own (the other way a frame is held)."""
    table = frame if isinstance(frame, pa.Table) else pa.Table.from_pandas(frame, preserve_index=False)
    return pa.RecordBatchReader.from_batches(table.schema, table.to_batches())


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

    def test_a_frame_holds_its_model_while_registered(self):
        frames = ll.Frames()
        table = frames.register('held_trades', pd.DataFrame({'desk': ['A', 'B'], 'qty': [1, 2]}))
        held = table.held
        self.assertIsNotNone(held)
        self.assertEqual(table.execute('->select(~[desk])').num_rows, 2)
        # registered again with other columns: a new model held, the one before let go
        again = frames.register('held_trades', pd.DataFrame({'desk': ['A'], 'qty': [1], 'note': ['x']}))
        with self.assertRaisesRegex(ValueError, 'was let go'):
            held.plan(again._tree('->select(~[desk])'), again.runtime)
        frames.unregister('held_trades')
        with self.assertRaisesRegex(ValueError, 'was let go'):
            again.held.plan(again._tree('->select(~[desk])'), again.runtime)
        frames.close()

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


class ReadAtEachQuery(unittest.TestCase):
    def test_a_frame_is_read_as_it_is_at_each_query(self):
        df = trades()
        table = ll.Frames().register('trades', df)
        before = table.execute(GROUP).to_pylist()
        df.loc[0, 'qty'] = 1000.0
        self.assertNotEqual(table.execute(GROUP).to_pylist(), before)
        self.assertEqual(table.execute(GROUP).to_pylist(), by_desk(df))

    def test_a_stream_is_read_once_into_a_table_of_its_own(self):
        # it can be read only once: its rows copied when it is registered, and never read again
        df = trades()
        table = ll.Frames().register('trades', streamed(df))
        self.assertFalse(table.live)
        copied = by_desk(df)
        df.loc[0, 'qty'] = 1000.0
        self.assertEqual(table.execute(GROUP).to_pylist(), copied)
        self.assertEqual(table.execute(GROUP).to_pylist(), copied, 'asked again, the stream is not read again')

    def test_a_function_making_a_stream_is_read_at_each_query(self):
        holder = {'df': trades()}
        table = ll.Frames().register('trades', lambda: streamed(holder['df']))
        self.assertTrue(table.live)
        holder['df'] = trades().iloc[:2]
        self.assertEqual(table.execute('->select(~[id])').to_pylist(), [{'id': 1}, {'id': 2}])

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
    def test_registering_a_name_again_replaces_its_table_a_view_or_a_streams_table(self):
        frames = ll.Frames()
        live = frames.register('trades', trades())
        copied = frames.register('trades', streamed(trades().iloc[:1]))
        self.assertEqual(copied.execute('->select(~[id])').to_pylist(), [{'id': 1}])
        again = frames.register('trades', trades())
        self.assertEqual(len(again.execute('->select(~[id])')), 5)
        for old in (live, copied):
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
        # a column of nothing but nulls (no type a Pure column can have), a frame of no columns, a stream of none:
        # refused, and nothing of either left behind
        for frame in (pd.DataFrame({'id': [1, 2], 'nothing': [None, None]}), pd.DataFrame(), streamed(pa.table({}))):
            frames = ll.Frames()
            with self.assertRaises(Exception):
                frames.register('trades', frame)
            self.assertNotIn('trades', frames)
            self.assertEqual(len(frames.register('trades', trades()).execute('->select(~[id])')), 5)

    def test_unregister_and_close_take_the_tables_out(self):
        frames = ll.Frames()
        one = frames.register('one', trades())
        frames.register('two', streamed(trades()))
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

    def test_a_name_is_refused_as_a_literal_only_as_pure_spells_one(self):
        # Pure's literals are `true` and `false`, as spelled: `True` is an identifier, for a page's file's table as for
        # a frame (frames.py read the file's name lowercased, and refused it)
        frames = ll.Frames()
        table = frames.register('True', trades())
        # SQL's TRUE is a keyword in any case: the frame's table is quoted where a query reads it
        self.assertEqual(table.execute("->filter(x|$x.desk == 'EQ')->select(~[id])").to_pylist(), [{'id': 2}, {'id': 5}])
        with frames.reading_file('False') as name:
            self.assertEqual(name, 'False')
        with self.assertRaises(ValueError), frames.reading_file('false'):
            pass

    def test_a_name_pure_takes_and_sql_reserves_reads_its_rows_a_view_or_a_table(self):
        # `order` is a Pure identifier (ModelChanges.isIdentifier) and a SQL keyword: the frame's view or a stream's
        # table is quoted wherever a query reads it
        for make in (trades, lambda: streamed(trades())):
            with self.subTest(stream=make is not trades):
                frames = ll.Frames()
                table = frames.register('order', make())
                self.assertEqual(table.accessor, '#>{order::DB.order}#')
                self.assertEqual(table.execute("->filter(x|$x.desk == 'EQ')->select(~[id])").to_pylist(),
                                 [{'id': 2}, {'id': 5}])
                frames.close()


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
        for given in (frame, streamed(frame)):
            table = ll.Frames().register('t', given)
            self.assertEqual([(c.name, c.type) for c in table.columns], [('u', 'Decimal')])
            self.assertEqual(table.execute('->select(~[u])').to_pylist(), [{'u': Decimal(2 ** 64 - 1)}, {'u': Decimal(1)}])

    def test_a_zoned_timestamp_is_its_utc_instant(self):
        at = pd.Timestamp('2026-10-08 09:30', tz='America/New_York')
        for given in (pd.DataFrame({'at': [at]}), streamed(pd.DataFrame({'at': [at]}))):
            table = ll.Frames().register('t', given)
            self.assertEqual([(c.name, c.type) for c in table.columns], [('at', 'DateTime')])
            self.assertEqual(table.execute('->select(~[at])').to_pylist(), [{'at': datetime.datetime(2026, 10, 8, 13, 30)}])


class TypedByPlan(unittest.TestCase):
    """An answer typed by its plan (frames.typed_by_plan; docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §15.3a), as the
    tab's executor types its own: each column the compiler's type, each value exact."""

    def test_each_column_carries_the_compilers_type_and_its_value_exact(self):
        frame = pa.table({
            'i': pa.array([1, None], pa.int32()),
            'f': pa.array([1.5, None]),
            'd': pa.array([Decimal('300.75'), None], pa.decimal128(10, 2)),
            's': pa.array(['a', None]),
            'b': pa.array([True, None]),
            'day': pa.array([datetime.date(2024, 1, 2), None]),
            'at': pa.array([datetime.datetime(2024, 1, 2, 3, 4, 5, 123456), None], pa.timestamp('us')),
            'v': pa.array(['{"a": [1, 2.50]}', None], pa.json_()),
        })
        table = ll.Frames().register('t', frame)
        rows = table.execute('->select()')
        self.assertEqual({f.name: f.metadata[b'legend.type'].decode() for f in rows.schema},
                         {'i': 'Integer', 'f': 'Float', 'd': 'Decimal', 's': 'String', 'b': 'Boolean', 'day': 'StrictDate',
                          'at': 'DateTime', 'v': 'meta::pure::metamodel::variant::Variant'})
        self.assertEqual(rows.schema.field('i').type, pa.int64())
        # a Decimal exact, a DateTime as stored (UTC) to the microsecond, a Variant its JSON text
        self.assertEqual(rows.to_pylist()[0], {
            'i': 1, 'f': 1.5, 'd': Decimal('300.75'), 's': 'a', 'b': True, 'day': datetime.date(2024, 1, 2),
            'at': datetime.datetime(2024, 1, 2, 3, 4, 5, 123456), 'v': '{"a": [1, 2.50]}'})
        self.assertEqual(set(rows.to_pylist()[1].values()), {None})

    def test_a_variant_stored_as_a_struct_is_its_json_whatever_the_shape_of_the_query(self):
        # a nested value (a STRUCT in DuckDB) is a Variant: its JSON text under a bare limit, a filter and a sort as
        # under a projection -- the planner's (47349d82a), never a dict one shape and text the other
        frame = pa.table({'id': [1, 2], 'customer': pa.array([{'name': 'A', 'tier': 2}, None])})
        table = ll.Frames().register('t', frame)
        for steps in ('->limit(5)', '->filter(x|$x.id > 0)', '->sort(~id->ascending())', '->select()'):
            with self.subTest(steps=steps):
                rows = table.execute(steps)
                self.assertEqual(rows.schema.field('customer').metadata[b'legend.type'],
                                 b'meta::pure::metamodel::variant::Variant')
                self.assertEqual(rows.to_pylist()[0]['customer'], '{"name":"A","tier":2}')

    def test_a_sum_of_integers_is_an_integer_and_past_64_bits_exact(self):
        # DuckDB answers a SUM of integers as an exact decimal(38, 0): an Integer, so an int64; past 64 bits, the
        # database's exact decimal, never wrapped
        frame = pa.table({'k': ['a', 'a'], 'n': pa.array([2 ** 63 - 1, 2 ** 63 - 1]), 'm': pa.array([1, None], pa.int32())})
        rows = ll.Frames().register('t', frame).execute('->groupBy(~[k], ~[s: x|$x.n : y|$y->sum(), t: x|$x.m : y|$y->sum()])')
        self.assertEqual(rows.schema.field('t').type, pa.int64())
        self.assertEqual(rows.schema.field('s').metadata[b'legend.type'], b'Integer')
        self.assertEqual(rows.to_pylist(), [{'k': 'a', 's': 2 * (2 ** 63 - 1), 't': 1}])

    def test_a_column_the_plan_does_not_type_is_refused(self):
        from legend_lite.frames import typed_by_plan
        plan = ll.Plan('SELECT 1 AS a, 2 AS b', (ll.Column('a', 'Integer', {}),))
        with self.assertRaises(RuntimeError) as e:
            typed_by_plan(pa.table({'a': [1], 'b': [2]}), plan)
        self.assertIn("does not type the result column 'b'", str(e.exception))

    def test_a_precise_integer_is_an_integer(self):
        # a column named by a precise primitive (the lattice knows them: _type_facts) is the plain primitive it refines:
        # an Int or a BigInt is an Integer by legend-lite's lattice (_types, generated), so an int64, and its name kept
        from legend_lite.frames import typed_by_plan
        plan = ll.Plan('SELECT a, b', (ll.Column('a', 'meta::pure::precisePrimitives::Int', {}),
                                       ll.Column('b', 'meta::pure::precisePrimitives::BigInt', {})))
        rows = typed_by_plan(pa.table({'a': pa.array([1], pa.int32()), 'b': pa.array([2], pa.int16())}), plan)
        self.assertEqual([f.type for f in rows.schema], [pa.int64(), pa.int64()])
        self.assertEqual(rows.schema.field('a').metadata[b'legend.type'], b'meta::pure::precisePrimitives::Int')

    def test_a_column_typed_by_a_name_legend_lite_does_not_read_is_refused(self):
        # never read as a guess: a column of the wrong type converts wrongly without a word (types.ts typeFact)
        from legend_lite.frames import typed_by_plan
        plan = ll.Plan('SELECT a', (ll.Column('a', 'my::Side', {}),))
        with self.assertRaises(RuntimeError) as e:
            typed_by_plan(pa.table({'a': ['BUY']}), plan)
        self.assertIn("the compiler typed a column 'my::Side', which legend-lite does not read", str(e.exception))

    def test_pandas_keeps_an_integer_and_a_boolean_with_nulls(self):
        from legend_lite.frames import to_pandas
        frame = pa.table({'i': pa.array([1, None]), 'b': pa.array([True, None]), 'd': pa.array([Decimal('0.10'), None])})
        out = to_pandas(ll.Frames().register('t', frame).execute('->select()'))
        self.assertEqual((str(out['i'].dtype), str(out['b'].dtype)), ('Int64', 'boolean'))
        self.assertEqual((out['i'][0], out['b'][0], out['d'][0]), (1, True, Decimal('0.10')))
        # an integer, never 1.0 (numpy's float64 is a float)
        self.assertNotIsInstance(out['i'][0], float)
        self.assertTrue(pd.isna(out['i'][1]) and pd.isna(out['b'][1]))


class TypeNamesAreReadHere(unittest.TestCase):
    """A column's type is read in one place (legend_lite/_types.py, over the generated _type_facts.py), as DataCube's
    guardrails test holds engine-client's types.ts the one: no other module of the package spells a type's name to
    decide what it is (docs/PYTHON_ASKS_DATACUBE_2026_10_15.md, item 4) -- it asks _types, which reads either compiler's
    name and refuses an unknown one. It scans the modules this lane holds: the compiler's and the datacube extra's
    (notebook.py, the notebook extra's widget, is not among them; it reads no type). It sees string literals: on the
    lanes' Python (3.12) an f-string is tokens of its own, so a type's name in an f-string's text is not seen."""

    def test_no_other_module_spells_a_type_name(self):
        import ast
        import io
        import pathlib
        import tokenize
        import legend_lite
        from legend_lite._type_facts import TYPE_FACTS
        names = set(TYPE_FACTS) | {plain for plain, _ in TYPE_FACTS.values()}
        package = pathlib.Path(legend_lite.__file__).parent
        modules = sorted(f for f in package.glob('*.py') if f.name not in ('_types.py', '_type_facts.py'))
        self.assertIn('page.py', [f.name for f in modules], 'the scan found the package')
        spelled = []
        for module in modules:
            for token in tokenize.generate_tokens(io.StringIO(module.read_text('utf-8')).readline):
                if token.type == tokenize.STRING and ast.literal_eval(token.string) in names:
                    spelled.append(f'{module.name}:{token.start[0]} {token.string}')
        self.assertEqual(spelled, [], 'a type is asked of legend_lite._types, never spelled')


class ModelFor(unittest.TestCase):
    """The model a page's query may be planned over (Frames.model_for; docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B3, the
    user's option (c)): one Python holds for what it serves, or a page's own whose every table Python serves."""

    def setUp(self):
        self.frames = ll.Frames()
        self.table = self.frames.register('trades', trades())

    def project(self, n=0):
        """A page's own model over the frame's table, its Database under a package of its own."""
        return self.table.model.replace('trades::DB', f'proj{n}::store::DB')

    def test_a_frames_own_and_a_page_model_over_it_are_planned(self):
        self.assertIs(self.frames.model_for(self.table.model), self.table.held)
        held = self.frames.model_for(self.project())
        self.assertIsNotNone(held)
        self.assertIs(self.frames.model_for(self.project()), held, 'held while recent, the one model')
        self.assertIn('FROM trades', held.plan(ll.parse('|#>{proj0::store::DB.trades}#->select(~[desk])'), self.table.runtime).sql)

    def test_a_table_in_another_schema_or_a_model_that_does_not_compile_is_refused(self):
        # the frame's table by name, but in a schema of another name: not the table Python serves
        other = '###Relational\nDatabase proj::DB\n(\n  Schema other\n  (\n    Table trades\n    (\n      id BIGINT\n    )\n  )\n)\n'
        # each reads as a model (so the refusal is the rule's, never a parse's)
        schemas = [sch['name'] for e in ll.model_elements(other) if e.get('_type') == 'relational' for sch in e['schemas']]
        self.assertEqual(schemas, ['other'])
        self.assertIsNone(self.frames.model_for(other))
        broken = self.project() + '\n###Pure\nClass proj0::A\n{\n  x: proj0::Nope[1];\n}\n'
        self.assertTrue(ll.model_elements(broken))
        self.assertIsNone(self.frames.model_for(broken))

    def test_only_the_most_recent_are_held_each_let_go_of_as_it_is_evicted(self):
        first = self.frames.model_for(self.project(0))
        for n in range(1, 9):
            self.frames.model_for(self.project(n))
        self.assertEqual(len(self.frames._admitted), 8)
        self.assertNotIn(self.project(0), self.frames._admitted)
        with self.assertRaises(ValueError):
            first.handle  # let go of (closed)
        # asked again, held again
        self.assertIsNotNone(self.frames.model_for(self.project(0)))


class Refusals(unittest.TestCase):
    def test_a_name_is_a_plain_identifier(self):
        with self.assertRaises(ValueError):
            ll.Frames().register('my trades', trades())

    def test_a_query_the_compiler_refuses_says_why(self):
        table = ll.Frames().register('trades', trades())
        with self.assertRaises(ll.LegendError) as e:
            table.execute('->filter(x|$x.nope == 1)')
        self.assertIn("'nope'", e.exception.message)


if __name__ == '__main__':
    unittest.main()
