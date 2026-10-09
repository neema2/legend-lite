"""legend_lite's bindings: trees in and out, exact numbers, typing and planning, refusals as
LegendError, several threads at once, and every answer freed."""

import os
import threading
import unittest
from decimal import Decimal
from pathlib import Path

import legend_lite as ll
from legend_lite import _json
from legend_lite._library import library

MODEL = Path(os.environ['LEGEND_LITE_CORPUS_MODEL']).read_text()
GROUP = "|#>{trades::DB.TRADES}#->groupBy(~[desk], ~[q: x|$x.notional : y|$y->sum()])->sort(~desk->ascending())"


class Trees(unittest.TestCase):
    def test_text_that_is_not_ascii_comes_back_intact(self):
        # in as UTF-8, out as the tree's own string (the printer writes such characters as \u escapes)
        tree = ll.parse("|'café ✓ 日本 \U0001F600'")
        self.assertEqual(tree['body'][0]['value'], 'café ✓ 日本 \U0001F600')
        self.assertEqual(ll.parse(ll.print_tree(tree)), tree)

    def test_a_nul_in_text_is_refused_before_the_call(self):
        with self.assertRaises(ValueError):
            ll.parse("|'a\x00b'")

    def test_text_to_tree_and_back(self):
        tree = ll.parse(GROUP)
        self.assertEqual(tree['_type'], 'lambda')
        self.assertEqual(ll.parse(ll.print_tree(tree)), tree, 'printing and parsing again is the same tree')
        self.assertNotIn('\n', ll.print_tree(tree, 'STANDARD'))

    def test_numbers_stay_exact(self):
        tree = ll.parse('|[1.50D, 9007199254740993, 0.1]')
        values = [v['value'] for v in tree['body'][0]['values']]
        self.assertEqual(values, [Decimal('1.50'), 9007199254740993, Decimal('0.1')])
        self.assertEqual(ll.print_tree(tree, 'STANDARD'), '|[1.50D, 9007199254740993, 0.1]')
        self.assertEqual(_json.dumps({'d': Decimal('12.30'), 'i': 2 ** 70}), '{"d":12.30,"i":1180591620717411303424}')

    def test_a_refusal_says_what_and_where(self):
        with self.assertRaises(ll.LegendError) as e:
            ll.parse('|1 +')
        self.assertEqual('PARSER', e.exception.kind)
        self.assertIn('[1:5]', e.exception.message)


class Compiling(unittest.TestCase):
    def test_types_a_query_compile_only(self):
        cols = ll.relation_type(MODEL, ll.parse(GROUP))
        self.assertEqual([(c.name, c.type) for c in cols], [('desk', 'String'), ('q', 'Float')])

    def test_plans_a_tree_and_text_alike(self):
        from_tree = ll.plan(MODEL, ll.parse(GROUP), 'trades::RT')
        from_text = ll.plan_text(MODEL, GROUP.lstrip('|'), 'trades::RT')
        self.assertEqual(from_tree, from_text)
        self.assertIn('GROUP BY', from_tree.sql)
        self.assertEqual([c.name for c in from_tree.columns], ['desk', 'q'])

    def test_an_unknown_column_is_refused(self):
        # typing refuses it, as legend-engine does (the erased TDS row reads only through its
        # accessors: docs/GATES.md, "The erased TDS row is read only through its accessors"), and so
        # does planning
        bad = ll.parse("|#>{trades::DB.TRADES}#->filter(x|$x.nope == 1)")
        with self.assertRaises(ll.LegendError) as e:
            ll.relation_type(MODEL, bad)
        self.assertIn("'nope'", e.exception.message)
        with self.assertRaises(ll.LegendError) as e:
            ll.plan(MODEL, bad, 'trades::RT')
        self.assertIn("'nope'", e.exception.message)
        with self.assertRaises(ll.LegendError) as e:
            ll.relation_type(MODEL, ll.parse("|#>{trades::DB.TRADES}#->select(~[nope])"))
        self.assertIn('nope', e.exception.message)

    def test_writes_a_tables_whole_model_from_its_catalog(self):
        m = ll.table_model({'table': 'orders', 'pkg': 'shop', 'convertible': True, 'databaseType': 'DuckDB',
                            'columns': [{'name': 'id', 'dataType': 'BIGINT', 'logicalType': 'BIGINT', 'notNull': True},
                                        {'name': 'big', 'dataType': 'UBIGINT', 'logicalType': 'UBIGINT'}]})
        self.assertEqual(m['runtime'], 'shop::RT')
        self.assertIn('Database shop::DB', m['model'])
        self.assertIn('RelationalDatabaseConnection shop::Conn', m['model'])
        self.assertEqual(m['copySelectList'], '* REPLACE (CAST("big" AS DECIMAL(20,0)) AS "big")')
        self.assertEqual([(c.name, c.type) for c in ll.relation_type(m['model'], {'_type': 'lambda', 'body': [m['source']], 'parameters': []})],
                         [('id', 'Integer'), ('big', 'Decimal')])
        self.assertNotIn('snapRuntime', m)

    def test_the_whole_model_is_exactly_what_datacube_writes(self):
        # DataCube's infer.ts wrote this text by hand; the compiler's boundary writes it now, for both
        m = ll.table_model({'table': 'orders', 'pkg': 'shop', 'convertible': True, 'databaseType': 'DuckDB',
                            'snapDatabaseType': 'DuckDB',
                            'columns': [{'name': 'id', 'dataType': 'BIGINT', 'logicalType': 'BIGINT', 'notNull': True},
                                        {'name': 'open', 'dataType': 'BOOLEAN', 'logicalType': 'BOOLEAN'}]})
        connection = lambda conn, rt: (
            '###Connection\nRelationalDatabaseConnection shop::' + conn + '\n{\n    type: DuckDB;\n'
            '    specification: DuckDB { };\n    auth: Test;\n}\n\n###Runtime\nRuntime shop::' + rt + '\n{\n'
            '    mappings: [];\n    connections:\n    [\n        shop::DB: [ c1: shop::' + conn + ' ]\n    ];\n}\n')
        database = ('###Relational\nDatabase shop::DB\n(\n    Table orders\n    (\n        id BIGINT NOT NULL,\n'
                    '        open BIT\n    )\n)\n')
        self.assertEqual(m['model'], database + '\n' + connection('Conn', 'RT') + '\n' + connection('SnapConn', 'SnapRT'))
        self.assertEqual((m['runtime'], m['snapRuntime'], m['accessor']), ('shop::RT', 'shop::SnapRT', '#>{shop::DB.orders}#'))
        self.assertEqual((m['conversions'], m['copySelectList'], m['excluded'], m['bitColumns']), ([], '*', [], ['open']))

    def test_a_duckdb_session_answers_in_utc(self):
        self.assertEqual(ll.session_setup('DuckDB'), ["SET TimeZone='UTC'"])

    def test_the_compiler_needs_no_package_beyond_pythons_own(self):
        # this suite runs with nothing but the bindings on the path: a star import brings no duckdb
        names = {}
        exec('from legend_lite import *', names)
        self.assertIn('plan', names)
        self.assertNotIn('Frames', names)

    def test_the_catalog_question_names_its_table_as_literals(self):
        sql = ll.catalog_columns_sql('my s', "o'{table}brien")
        self.assertIn("c.schema_name = 'my s'", sql)
        self.assertIn("c.table_name = 'o''{table}brien'", sql)

    def test_reads_a_model_into_its_elements(self):
        kinds = {e['_type'] for e in ll.model_elements(MODEL)}
        self.assertIn('relational', kinds)

    def test_writes_a_database_from_a_tables_catalog(self):
        db = ll.database_from_catalog({'path': 'local::DB', 'table': 'positions', 'convertible': True,
                                       'databaseType': 'DuckDB', 'columns': [
            {'name': 'desk', 'dataType': 'VARCHAR', 'logicalType': 'VARCHAR'},
            {'name': 'qty', 'dataType': 'INTEGER', 'logicalType': 'INTEGER'}]})
        self.assertIn('Table positions', db['text'])
        self.assertEqual(db['excluded'], [])


class Hosting(unittest.TestCase):
    def test_several_threads_at_once(self):
        want = ll.plan(MODEL, ll.parse(GROUP), 'trades::RT')
        wrong = []

        def work():
            for _ in range(20):
                if ll.plan(MODEL, ll.parse(GROUP), 'trades::RT') != want:
                    wrong.append(1)
        ts = [threading.Thread(target=work) for _ in range(8)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(wrong, [])

    def test_frees_every_answer(self):
        lib = library()
        before = lib.unfreed()
        tree = ll.parse(GROUP)
        for _ in range(50):
            ll.plan(MODEL, tree, 'trades::RT')
            ll.relation_type(MODEL, tree)
            ll.print_tree(tree)
        with self.assertRaises(ll.LegendError):
            ll.parse('|$x.')
        self.assertEqual(lib.unfreed(), before, 'an answer was not freed')
        # the count is live: an answer taken and not yet freed shows in it
        p = lib._lib.lite_session_setup(lib._thread(), b'DuckDB')
        self.assertEqual(lib.unfreed(), before + 1)
        lib._lib.lite_free(lib._thread(), p)
        self.assertEqual(lib.unfreed(), before)


if __name__ == '__main__':
    unittest.main()
