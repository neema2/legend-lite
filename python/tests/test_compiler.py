"""legend_lite's bindings: trees in and out, exact numbers, typing and planning, refusals as
LegendError, several threads at once, and every answer freed."""

import json
import os
import threading
import unittest
from decimal import Decimal
from pathlib import Path

import legend_lite as ll
from legend_lite import _json
from legend_lite._library import library
from legend_lite.compiler import _answer

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
        # laid out as json.dumps lays it out, the numbers still exact
        value = {'a': [1, {'b': 'é', 'c': []}, {}], 'd': None, 'e': [True, 0.5]}
        self.assertEqual(_json.dumps(value, indent=2), json.dumps(value, indent=2, ensure_ascii=False))
        self.assertEqual(_json.dumps({'d': Decimal('12.30')}, indent=2), '{\n  "d": 12.30\n}')

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

    def test_every_refusal_has_its_kind_by_one_rule(self):
        """A refusal's kind is legend-engine's errorType when the text is refused, else the status legend-lite's server
        answers -- for a call that asks pure/v1 and for the compiler's own calls alike (the protocol program's leg 6,
        docs/PROTOCOL_PROGRAM_2026_10_05.md)."""
        for call in (lambda: ll.parse('|1 +'), lambda: ll.plan_text(MODEL, '1 +', 'trades::RT')):
            with self.assertRaises(ll.LegendError) as e:
                call()
            self.assertEqual(e.exception.kind, 'PARSER')
        bad = ll.parse("|#>{trades::DB.TRADES}#->filter(x|$x.nope == 1)")
        for call in (lambda: ll.relation_type(MODEL, bad), lambda: ll.plan(MODEL, bad, 'trades::RT')):
            with self.assertRaises(ll.LegendError) as e:
                call()
            self.assertEqual(e.exception.kind, 'COMPILATION')
            self.assertIn("'nope'", e.exception.message)
        with self.assertRaises(ll.LegendError) as e:
            ll.table_model({'table': 'orders'})
        self.assertEqual(e.exception.kind, '500')
        self.assertEqual(e.exception.message, 'IllegalArgumentException: Missing required field: databaseType')

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

    def test_writes_one_model_of_several_tables_each_read_as_its_own_model_reads_it(self):
        # docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17.3: a join's inputs, read in one place
        orders = {'table': 'orders', 'pkg': 'shop', 'convertible': True,
                  'columns': [{'name': 'id', 'dataType': 'BIGINT', 'logicalType': 'BIGINT', 'notNull': True},
                              {'name': 'cust', 'dataType': 'INTEGER', 'logicalType': 'INTEGER'}]}
        customers = {'table': 'customers', 'pkg': 'shop', 'convertible': True,
                     'columns': [{'name': 'id', 'dataType': 'INTEGER', 'logicalType': 'INTEGER', 'notNull': True},
                                 {'name': 'region', 'dataType': 'VARCHAR', 'logicalType': 'VARCHAR'}]}
        m = ll.table_model({'databaseType': 'DuckDB', 'tables': [orders, customers]})
        self.assertEqual(m['runtime'], 'host::RT')
        self.assertEqual(m['model'].count('Database shop::DB'), 1)
        self.assertEqual([t['accessor'] for t in m['tables']], ['#>{shop::DB.orders}#', '#>{shop::DB.customers}#'])
        # each table's relation is the one its own model reads
        alone = ll.table_model({**customers, 'databaseType': 'DuckDB'})
        self.assertEqual(m['tables'][1]['source'], alone['source'])
        joined = ll.parse("|#>{shop::DB.orders}#->join(#>{shop::DB.customers}#->rename(~id, ~c_id), "
                          "meta::pure::functions::relation::JoinKind.LEFT, {a, b | $a.cust == $b.c_id})")
        self.assertEqual([c.name for c in ll.relation_type(m['model'], joined)], ['id', 'cust', 'c_id', 'region'])
        self.assertIn('LEFT OUTER JOIN', ll.plan(m['model'], joined, m['runtime']).sql)
        with self.assertRaises(ll.LegendError) as e:
            ll.table_model({'databaseType': 'DuckDB', 'tables': [customers, customers]})
        self.assertIn("'customers' is given twice", e.exception.message)

    def test_the_whole_model_is_exactly_what_datacube_writes(self):
        # DataCube's infer.ts wrote this text by hand; the compiler's boundary writes it now, for both
        m = ll.table_model({'table': 'orders', 'pkg': 'shop', 'convertible': True, 'databaseType': 'DuckDB',
                            'columns': [{'name': 'id', 'dataType': 'BIGINT', 'logicalType': 'BIGINT', 'notNull': True},
                                        {'name': 'open', 'dataType': 'BOOLEAN', 'logicalType': 'BOOLEAN'}]})
        connection = lambda conn, rt: (
            '###Connection\nRelationalDatabaseConnection shop::' + conn + '\n{\n    type: DuckDB;\n'
            '    specification: DuckDB { };\n    auth: Test;\n}\n\n###Runtime\nRuntime shop::' + rt + '\n{\n'
            '    mappings: [];\n    connections:\n    [\n        shop::DB: [ c1: shop::' + conn + ' ]\n    ];\n}\n')
        database = ('###Relational\nDatabase shop::DB\n(\n    Table orders\n    (\n        id BIGINT NOT NULL,\n'
                    '        open BIT\n    )\n)\n')
        self.assertEqual(m['model'], database + '\n' + connection('Conn', 'RT'))
        self.assertEqual((m['runtime'], m['accessor']), ('shop::RT', '#>{shop::DB.orders}#'))
        self.assertEqual((m['conversions'], m['copySelectList'], m['excluded'], m['bitColumns']), ([], '*', [], ['open']))

    def test_a_held_model_is_compiled_once_and_plans_as_a_model_given_each_time(self):
        # docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §9: the same answers, the model compiled once
        with ll.HeldModel(MODEL) as held:
            self.assertEqual(held.plan(ll.parse(GROUP), 'trades::RT'), ll.plan(MODEL, ll.parse(GROUP), 'trades::RT'))
            tree = ll.parse("|#>{trades::DB.TRADES}#->select(~[desk])")
            self.assertEqual(held.relation_type(tree), ll.relation_type(MODEL, tree))
            with ll.HeldModel(MODEL) as again:
                self.assertNotEqual(again._handle, held._handle, 'each hold its own handle')
                self.assertEqual(again.plan(ll.parse(GROUP), 'trades::RT'), held.plan(ll.parse(GROUP), 'trades::RT'))
            # the other holder let go: this one still holds it
            self.assertEqual(held.plan(ll.parse(GROUP), 'trades::RT').sql, ll.plan(MODEL, ll.parse(GROUP), 'trades::RT').sql)
        with self.assertRaisesRegex(ValueError, 'was let go'):
            held.plan(ll.parse(GROUP), 'trades::RT')
        held.close()

    def test_a_held_model_answers_a_datacube_planners_worker_as_the_tab_does(self):
        # the headless host answers DataCube's `relationType` and `testData` with these, over the same Boundary functions
        # as the tab's relationTypeHeldOrError and testDataSqlOrError (docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md §3)
        lib = library()
        handle = _answer(lib.call('lite_hold_model', MODEL))
        try:
            query = '|#>{trades::DB.TRADES}#->select(~[desk, year])'
            self.assertEqual(lib.call('lite_relation_type_text_held', handle, query),
                             lib.call('lite_relation_type_held', handle, _json.dumps(ll.parse(query))),
                             'typed from the text as from its tree')
            refused = lib.call('lite_relation_type_text_held', handle, '|#>{trades::DB.NOPE}#')
            self.assertTrue(refused.startswith('ERR\n'), refused)
        finally:
            lib.call('lite_release_model', handle)
        refused = lib.call('lite_plan_text_held', handle, GROUP, 'trades::RT')
        self.assertTrue(refused.startswith('ERR\n'), f'a handle let go is refused: {refused[:120]}')

    def test_a_models_test_data_comes_back_as_duckdb_statements_per_table(self):
        tables = [{'schema': 'default', 'table': 'TRADES', 'csv': 'region,desk,year\nEMEA,Rates,2024\n'}]
        seeds = json.loads(_answer(library().call('lite_test_data_sql', MODEL, 'trades::DB', json.dumps(tables))))
        self.assertEqual(len(seeds), 1)
        self.assertEqual(seeds[0]['table'], 'TRADES')
        self.assertTrue(any(s.lower().startswith('create table trades') for s in seeds[0]['sql']), seeds[0]['sql'])
        self.assertIn('region', [c['name'].lower() for c in seeds[0]['columns']])
        refused = library().call('lite_test_data_sql', MODEL, 'trades::DB', json.dumps([{'schema': 'default', 'table': 'NOPE', 'csv': 'a\n'}]))
        self.assertTrue(refused.startswith('ERR\n') and 'declares no table' in refused, refused)

    def test_a_model_that_does_not_compile_is_refused_when_held(self):
        with self.assertRaises(ll.LegendError):
            ll.HeldModel('###Pure\nClass bad::C { a: Nope[1]; }')

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


class ProjectDatabase(unittest.TestCase):
    """A project's own database (C5): the native library answers as the tab's planner does -- its name from the
    identity, DuckDB's statements, the project's rows as the server seeds them; another database refused by name.
    Run on Python's DuckDB by the engine's tests (they have duckdb; these bindings need no other package)."""

    PARTY = """###Relational
Database demo::party::PartyDB
(
  Schema PARTY
  (
    Table PARTY (ID INTEGER PRIMARY KEY, NAME VARCHAR(200))
  )
)

###Data
Data demo::party::PartyData
{
  Relational
  #{
    PARTY.PARTY:
      'ID,NAME\\n1,Meridian Capital\\n';
  }#
}
"""

    def test_duckdbs_statements(self):
        from legend_lite.compiler import project_data
        p = project_data(self.PARTY, 'DuckDB')
        self.assertRegex(p['name'], r'^__project_[0-9a-f]{16}$')
        self.assertEqual(p['open'], [f"ATTACH ':memory:' AS \"{p['name']}\""])
        self.assertEqual(p['use'], [f'USE "{p["name"]}"'])
        self.assertEqual(p['close'], [f'DETACH DATABASE IF EXISTS "{p["name"]}"'])
        self.assertTrue(any('Meridian Capital' in s for s in p['fill']), p['fill'])
        # named by what it holds: the same rows the same name, an edited row another
        self.assertEqual(project_data(self.PARTY + '\n', 'DuckDB')['name'], p['name'])
        self.assertNotEqual(project_data(self.PARTY.replace('Meridian', 'Kestrel'), 'DuckDB')['name'], p['name'])

    def test_another_database_refused_by_name(self):
        from legend_lite.compiler import project_data
        with self.assertRaisesRegex(ll.LegendError, "a project's own database on Postgres is not built"):
            project_data(self.PARTY, 'Postgres')


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
