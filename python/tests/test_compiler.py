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

    def test_a_nul_in_text_travels_in_the_requests_json(self):
        # the lite API's request is JSON (the spec's §5, 2026-11-17), which carries a NUL escaped: no C string ends at it
        tree = ll.parse("|'a\x00b'")
        self.assertEqual(tree['body'][0]['value'], 'a\x00b')

    def test_text_to_tree_and_back(self):
        tree = ll.parse(GROUP)
        self.assertEqual(tree['_type'], 'lambda')
        self.assertEqual(ll.parse(ll.print_tree(tree)), tree, 'printing and parsing again is the same tree')
        self.assertNotIn('\n', ll.print_tree(tree, 'standard'))

    def test_numbers_stay_exact(self):
        tree = ll.parse('|[1.50D, 9007199254740993, 0.1]')
        values = [v['value'] for v in tree['body'][0]['values']]
        self.assertEqual(values, [Decimal('1.50'), 9007199254740993, Decimal('0.1')])
        self.assertEqual(ll.print_tree(tree, 'standard'), '|[1.50D, 9007199254740993, 0.1]')
        self.assertEqual(_json.dumps({'d': Decimal('12.30'), 'i': 2 ** 70}), '{"d":12.30,"i":1180591620717411303424}')
        # laid out as json.dumps lays it out, the numbers still exact
        value = {'a': [1, {'b': 'é', 'c': []}, {}], 'd': None, 'e': [True, 0.5]}
        self.assertEqual(_json.dumps(value, indent=2), json.dumps(value, indent=2, ensure_ascii=False))
        self.assertEqual(_json.dumps({'d': Decimal('12.30')}, indent=2), '{\n  "d": 12.30\n}')

    def test_a_refusal_says_what_and_where(self):
        with self.assertRaises(ll.LegendError) as e:
            ll.parse('|1 +')
        self.assertEqual(ll.compiler.error_kind('PARSE'), e.exception.kind)
        # where: the position, its own field (the spec's §3)
        self.assertEqual('[1:5]', e.exception.where)


class LitesOwnEntries(unittest.TestCase):
    """The edge program's H4 (2026-11-17): parse, print, a query's types and a model's elements are the PRODUCT's --
    four native entries over Boundary's own operations, refused with lite's own kinds -- never legend-engine's pure/v1
    routes (the edge's lite_pure_v1 entry): with that entry refused, each still answers."""

    MODEL = """###Relational
Database s::DB ( Table t ( id INTEGER, desk VARCHAR(10) ) )
###Connection
RelationalDatabaseConnection s::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }
###Runtime
Runtime s::RT { mappings: []; connections: [ s::DB: [ c: s::Conn ] ]; }
"""

    def test_each_answers_with_the_edges_entry_refused(self):
        from unittest import mock
        from legend_lite import compiler
        real = library().call

        def no_edge(name, *args):
            if name == 'lite_pure_v1':
                raise AssertionError("legend-engine's pure/v1 entry asked for a product operation")
            return real(name, *args)

        with mock.patch.object(type(library()), 'call', lambda self, name, *args: no_edge(name, *args)):
            tree = compiler.parse('|#>{s::DB.t}#->select(~[desk])')
            self.assertEqual(compiler.parse(compiler.print_tree(tree, 'standard')), tree)
            self.assertEqual([c.name for c in compiler.relation_type(self.MODEL, tree)], ['desk'])
            named = {f"{e['package']}::{e['name']}" for e in compiler.model_elements(self.MODEL)}
            self.assertLessEqual({'s::DB', 's::Conn', 's::RT'}, named)
            # refused with lite's own kinds
            with self.assertRaises(ll.LegendError) as unparsed:
                compiler.parse('|1 +')
            self.assertEqual(unparsed.exception.kind, compiler.error_kind('PARSE'))
            with self.assertRaises(ll.LegendError) as untyped:
                compiler.relation_type(self.MODEL, compiler.parse('|#>{s::DB.t}#->select(~[nope])'))
            self.assertEqual(untyped.exception.kind, compiler.error_kind('COMPILE'))


class ThePlannerKinds(unittest.TestCase):
    """The planner protocol's lite kinds, answered by Python's native entries (lite API v1, the host train, 2026-11-17):
    each operation's kind (a/b -> aB) its native entry (a/b -> lite_a_b), the operation's request JSON carried whole;
    the held model's two over the binding's cache."""

    MODEL = LitesOwnEntries.MODEL

    def ask(self, request):
        from legend_lite import _planner
        said = _planner.answer(library(), request)
        self.assertTrue(said['ok'], said)
        return said['answer']

    def test_each_kind_is_answered(self):
        tree = ll.parse('|#>{s::DB.t}#->select(~[desk])')
        ask = lambda kind, request: self.ask({'kind': kind, 'request': _json.dumps(request)})
        self.assertEqual(ask('modelCheck', {'model': self.MODEL}), 'OK\n{"errors":[]}')
        self.assertTrue(ask('queryPrint', {'query': tree, 'style': 'standard'}).startswith('OK\n{"text":"|#>{s::DB.t}#'))
        self.assertIn('"desk"', ask('queryType', {'model': self.MODEL, 'query': tree}))
        self.assertTrue(ask('queryParse', {'text': '|1'}).startswith('OK\n{"query":'))
        self.assertTrue(ask('modelElements', {'model': self.MODEL}).startswith('OK\n{"elements":'))
        self.assertTrue(ask('queryPlan', {'model': self.MODEL, 'query': tree, 'runtime': 's::RT'}).startswith('OK\n{"plan":'))
        # a model's change and a #TS item: answered by their Boundary members, refused in lite's error shape where wrong
        self.assertTrue(ask('modelCreateDataSpace', {}).startswith('ERR\n{"kind":'))
        handle = _answer(self.ask({'kind': 'hold', 'model': self.MODEL}))
        try:
            held = lambda kind, request: self.ask({'kind': kind, 'handle': int(handle), 'request': _json.dumps(request)})
            self.assertIn('"desk"', held('queryTypeHeld', {'query': tree}))
            self.assertTrue(held('queryPlanHeld', {'query': tree, 'runtime': 's::RT'}).startswith('OK\n{"plan":'))
        finally:
            self.ask({'kind': 'release', 'handle': int(handle)})


class OneFoldedReader(unittest.TestCase):
    """H4's P3 (the user's CP-13, 2026-11-19): the native library's answers are read by ONE reader (_folded), the planner's
    and the run side's alike, and a host's folded refusal written by its one writer -- until then three readers read the
    layout retired on 2026-11-17 (ERR, a kind, a message), so a refusal surfaced as its raw JSON text."""

    def test_an_answer_a_refusal_and_an_http_answer_are_read_once(self):
        from legend_lite import _folded
        self.assertEqual(_folded.unfold('OK\n{"a":1}'), '{"a":1}')
        with self.assertRaises(ll.LegendError) as refused:
            _folded.unfold('ERR\n{"kind":"data","message":"division by zero","where":"[1:2]","raise":"divideByZero"}')
        e = refused.exception
        self.assertEqual((e.kind, e.message, e.where, e.raise_), ('data', 'division by zero', '[1:2]', 'divideByZero'))
        self.assertEqual(_folded.unfold_http('OK\n404\napplication/json\n{"kind":"notFound"}'),
                         (404, 'application/json', '{"kind":"notFound"}'))
        # the retired layout is no refusal of lite's: said as the library's own failure, never read by a guess
        for retired in ('ERR\nINTERNAL\nboom', 'NOPE', 'OK\nnot-a-status\ntype\nbody'):
            with self.subTest(retired=retired), self.assertRaises(OSError):
                _folded.unfold_http(retired)
        # the one writer's refusal reads back as itself
        written = _folded.refusal(ll.LegendError('no', 'request', None, None))
        self.assertEqual(written, 'ERR\n{"kind":"request","message":"no"}')
        with self.assertRaises(ll.LegendError) as back:
            _folded.unfold(written)
        self.assertEqual((back.exception.kind, back.exception.message), ('request', 'no'))

    def test_a_stores_refusal_is_said_in_its_words_never_its_raw_json(self):
        from legend_lite import _home
        with self.assertRaises(OSError) as failed:
            _home._answered('ERR\n{"kind":"internal","message":"IllegalStateException: the folder is gone"}')
        self.assertEqual(str(failed.exception), 'internal: IllegalStateException: the folder is gone')
        # the stores' own fold: OK, its status and its body (SavedStores, ModelHome)
        self.assertEqual(_home._answered('OK\n204\n'), (204, ''))
        self.assertEqual(_home._answered('OK\n404\n{"kind":"query_not_found"}'), (404, '{"kind":"query_not_found"}'))


class TheProductsKinds(unittest.TestCase):
    """The lite API spec's §3 (2026-11-17): a refusal's kind is the product's (LiteV1Api.RefusalKind), read from the
    native library (lite_refusal_kinds), never a spelling of Python's own -- so a rename in the product needs no edit
    here; and a refusal carries lite's error shape (kind, message, where, raise_)."""

    def test_the_kinds_are_the_products_and_a_refusal_says_one(self):
        from legend_lite import compiler
        kinds = compiler.refusal_kinds()
        self.assertLessEqual({'PARSE', 'COMPILE'}, set(kinds))
        with self.assertRaises(ll.LegendError) as refused:
            ll.parse('|1 +')
        self.assertEqual(refused.exception.kind, kinds['PARSE'])
        self.assertTrue(compiler.refuses_the_text(refused.exception))
        # a parse refusal says where (its position), and raised nothing (no raise_: the spec's §3)
        self.assertEqual((refused.exception.where, refused.exception.raise_), ('[1:5]', None))

    def test_a_refusal_is_answered_with_its_kinds_status_the_products(self):
        # H4's P2 (the user's CP-13, 2026-11-19): the status is the product's (LiteV1Api.Kind, through
        # lite_refusal_kinds), never a rule of Python's -- whose 422/500/else-400 answered a notFound with 400
        from legend_lite import compiler
        status, said = compiler.refused_route(ll.LegendError('no such query', compiler.error_kind('NOT_FOUND')))
        self.assertEqual((status, json.loads(said)), (404, {'kind': 'notFound', 'message': 'no such query'}))
        self.assertEqual({compiler.refusal_status(wire) for wire in compiler.refusal_kinds().values()} - {400, 404, 422, 500},
                         set())
        with self.assertRaises(ll.LegendError) as unknown:
            compiler.refusal_status('guessed')
        self.assertEqual(unknown.exception.kind, compiler.error_kind('INTERNAL'))


# the suite's operations a host serves on its own routes, which test_engine's HostRoutesConformance reads
HOST_ROUTES = frozenset({'query/run', 'queries/get', 'queries/dataCube/get'})


class LiteApiConformance(unittest.TestCase):
    """LITE API V1'S CONFORMANCE SUITE, THE PYTHON BINDING (docs/records/lite-api-v1-2026-11-17.md §5, 2026-11-17): each
    case of engine-client/test/fixtures/lite-api-cases.json asked of Python's native entries (``compiler.operation``,
    the spec's one rule: operation a/b -> lite_a_b) and its answer held to the case's -- exact (``answer``), or each key
    it names equal and each array item it names matched by some item (``answerIncludes``) -- as the browser's exports
    (core's LiteApiConformanceTest) and the server's routes are by their own readers, so the three cannot drift."""

    @staticmethod
    def includes(expected, actual):
        if isinstance(expected, dict):
            return isinstance(actual, dict) and all(k in actual and LiteApiConformance.includes(v, actual[k])
                                                    for k, v in expected.items())
        if isinstance(expected, list):
            return isinstance(actual, list) and all(any(LiteApiConformance.includes(e, a) for a in actual)
                                                    for e in expected)
        return expected == actual and type(expected) is type(actual)

    def test_pythons_entries_answer_each_case(self):
        from legend_lite import compiler
        fixture = _json.loads(Path(os.environ['LEGEND_LITE_API_CASES']).read_text(encoding='utf-8'))
        cases = fixture['cases']
        self.assertTrue(cases, 'the suite holds cases')
        shapes = {'string': str, 'number': (int, float), 'boolean': bool, 'object': dict, 'array': list}
        asked = 0
        for case in cases:
            bindings = case.get('bindings')
            if bindings is not None:
                unknown = set(bindings) - {'browser', 'server', 'python'}
                self.assertFalse(unknown, f"{case['name']}: a binding the suite does not know: {sorted(unknown)}")
                if 'python' not in bindings:
                    continue
            if case['op'] in HOST_ROUTES:
                # a host's own route -- query/run over its own DuckDB, the saved queries' store over its folders: Python's
                # server answers it, read by test_engine's HostRoutesConformance (this lane holds neither), never
                # skipped there
                continue
            asked += 1
            with self.subTest(case['name']):
                if 'refusal' in case:
                    # §3: the kind is the contract, the words the product's
                    with self.assertRaises(compiler.LegendError) as refused:
                        compiler.operation(case['op'], case['request'])
                    self.assertEqual(refused.exception.kind, case['refusal']['kind'])
                    continue
                answer = compiler.operation(case['op'], case['request'])
                if 'answer' in case:
                    self.assertEqual(answer, case['answer'])
                elif 'answerShape' in case:
                    self.assertEqual(set(answer), set(case['answerShape']))
                    for key, kind in case['answerShape'].items():
                        self.assertIsInstance(answer[key], shapes[kind], key)
                        self.assertNotIsInstance(answer[key], bool if kind == 'number' else ())
                else:
                    self.assertTrue(self.includes(case['answerIncludes'], answer), _json.dumps(answer)[:2000])
        self.assertTrue(asked, 'the suite holds cases Python answers')

    def test_an_integer_is_exact_on_the_python_binding(self):
        # ruled 7: an Integer an exact JSON number, 2^63-1 kept on all three bindings
        self.assertEqual(ll.parse('|9223372036854775807')['body'][0]['value'], 2 ** 63 - 1)


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
            self.assertEqual(e.exception.kind, ll.compiler.error_kind('PARSE'))
        bad = ll.parse("|#>{trades::DB.TRADES}#->filter(x|$x.nope == 1)")
        for call in (lambda: ll.relation_type(MODEL, bad), lambda: ll.plan(MODEL, bad, 'trades::RT')):
            with self.assertRaises(ll.LegendError) as e:
                call()
            self.assertEqual(e.exception.kind, ll.compiler.error_kind('COMPILE'))
            self.assertIn("'nope'", e.exception.message)
        with self.assertRaises(ll.LegendError) as e:
            ll.table_model({'table': 'orders'})
        self.assertEqual(e.exception.kind, ll.compiler.error_kind('INTERNAL'))
        self.assertEqual(e.exception.message, 'IllegalArgumentException: Missing required field: databaseType')

    def test_writes_a_tables_whole_model_from_its_catalog(self):
        m = ll.table_model({'table': 'orders', 'pkg': 'shop', 'convertible': True, 'databaseType': 'DuckDB',
                            'columns': [{'name': 'id', 'dataType': 'BIGINT', 'logicalType': 'BIGINT', 'notNull': True},
                                        {'name': 'big', 'dataType': 'UBIGINT', 'logicalType': 'UBIGINT'}]})
        self.assertEqual(m['runtime'], 'shop::RT')
        self.assertIn('Database shop::DB', m['model'])
        self.assertIn('RelationalDatabaseConnection shop::Conn', m['model'])
        # a copy's conversions are the platform's, from its columns' catalog (convert): the model carries no select list
        self.assertEqual([c['column'] for c in m['conversions']], ['big'])
        self.assertNotIn('copySelectList', m)
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
        self.assertEqual((m['conversions'], m['excluded']), ([], []))
        # legend-engine's TinyInt for a BIT column is the legend-engine client's alone (the edge program's H10,
        # 2026-11-17): the product's table model names none
        self.assertNotIn('bitColumns', m)

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

    def test_the_planner_protocols_hold_over_a_library_holds_the_model_over_it(self):
        # the protocol's hold with its library (A5, 2026-11-06): answered as planner-answer.ts answers it, by
        # lite_hold_model_over, the library a layer of its own -- never the model alone, which cannot see the library
        from legend_lite._planner import answer
        lib = library()
        over = '###Pure\nfunction lib::twice(x: Number[1]): Number[1] { $x * 2 }\n'
        held = answer(lib, {'id': 1, 'kind': 'hold', 'model': MODEL, 'library': over})
        self.assertTrue(held['ok'], held)
        handle = _answer(held['answer'])
        try:
            query = ll.parse('|#>{trades::DB.TRADES}#->extend(~twice: x|lib::twice($x.id))->select(~[desk, twice])')
            typed = lib.call('lite_query_type_held', handle, _json.dumps({'query': query}))
            self.assertFalse(typed.startswith('ERR\n'), typed)
            self.assertIn('twice', typed)
        finally:
            lib.call('lite_release_model', handle)
        alone = answer(lib, {'id': 2, 'kind': 'hold', 'model': MODEL})
        handle = _answer(alone['answer'])
        try:
            refused = lib.call('lite_query_type_held', handle, _json.dumps(
                {'query': ll.parse('|#>{trades::DB.TRADES}#->extend(~twice: x|lib::twice($x.id))')}))
            self.assertTrue(refused.startswith('ERR\n'), 'the model alone has no lib::twice')
        finally:
            lib.call('lite_release_model', handle)

    def test_a_held_model_answers_a_datacube_planners_worker_as_the_tab_does(self):
        # the headless host answers DataCube's `relationType` and `fileLoad` with these, over the same Boundary functions
        # as the tab's relationTypeHeldOrError and fileLoadOrError (docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md §3)
        lib = library()
        handle = _answer(lib.call('lite_hold_model', MODEL))
        try:
            query = ll.parse('|#>{trades::DB.TRADES}#->select(~[desk, year])')
            self.assertEqual(lib.call('lite_query_type_held', handle, _json.dumps({'query': query})),
                             lib.call('lite_query_type', _json.dumps({'model': MODEL, 'query': query})),
                             'typed over the model held as over the model given')
            refused = lib.call('lite_query_type_held', handle, _json.dumps({'query': ll.parse('|#>{trades::DB.NOPE}#')}))
            self.assertTrue(refused.startswith('ERR\n'), refused)
        finally:
            lib.call('lite_release_model', handle)
        refused = lib.call('lite_query_plan_held', handle, _json.dumps({'query': ll.parse(GROUP), 'runtime': 'trades::RT'}))
        self.assertTrue(refused.startswith('ERR\n'), f'a handle let go is refused: {refused[:120]}')

    def test_a_hosts_file_comes_back_as_a_planned_load(self):
        # B3 (2026-11-11): a person's file put into a declared table is a plan -- one HOST_SESSION load, the table made
        # as declared, the file a reference (its name, digest and size) the host binds, never a path in a statement
        digest = '0' * 64
        request = {'databaseType': 'DuckDB', 'file': {'name': 'upload-1.csv', 'digest': digest, 'size': 12, 'format': 'csv'},
                   'into': {'model': MODEL, 'database': 'trades::DB', 'schema': 'default', 'table': 'TRADES'}}
        plan = json.loads(_answer(library().call('lite_file_load', json.dumps(request))))
        load = plan['root']
        self.assertEqual((load['_type'], load['cadence']), ('load', 'HOST_SESSION'))
        self.assertTrue(any(s.get('_type') == 'statement' and s['sql'].lower().startswith('create table trades')
                            for s in load['steps']), load['steps'])
        file = load['steps'][-1]
        self.assertEqual(file['ref'], {'namespace': 'HOST', 'name': 'upload-1.csv', 'digest': digest, 'size': 12,
                                       'format': {'_type': 'csv', 'delimiter': ',', 'quote': '"'}})
        self.assertTrue(all('?' in s and 'upload-1' not in s for s in file['delivery']['statements']), file)
        request['into']['table'] = 'NOPE'
        refused = library().call('lite_file_load', json.dumps(request))
        self.assertTrue(refused.startswith('ERR\n') and 'declares not' in refused, refused)

    def test_a_remote_files_mount_comes_back_as_a_planned_load_naming_what_the_host_holds(self):
        # B3's Q3 (2026-11-11, the user's ruling: "Yes do option 2"): a remote file's mount is a plan -- one HOST_SESSION
        # load, its secret's fields and its bearer URL bound from the host's store BY NAME, never a value in the plan; the
        # request is names and fields alone, any other key refused
        request = {'databaseType': 'DuckDB', 'view': 'trades', 'format': 'parquet',
                   'location': {'bearer': 'datacube_url_trades', 'shown': 'https://b.s3.amazonaws.com/trades.parquet'},
                   'credentials': {'name': 'datacube_s3_trades', 'fields': ['KEY_ID', 'SECRET']}}
        plan = json.loads(_answer(library().call('lite_remote_mount', json.dumps(request))))
        load = plan['root']
        self.assertEqual((load['_type'], load['cadence']), ('load', 'HOST_SESSION'))
        bound = [s for s in load['steps'] if s['_type'] == 'bound']
        self.assertEqual([(s['reach'], s['binds']) for s in bound], [
            ('DATABASE', [{'_type': 'held', 'name': 'datacube_s3_trades', 'field': 'KEY_ID'},
                          {'_type': 'held', 'name': 'datacube_s3_trades', 'field': 'SECRET'},
                          {'_type': 'value', 'text': 'https://b.s3.amazonaws.com/trades.parquet'}]),
            ('CONNECTION', [{'_type': 'held', 'name': 'datacube_url_trades', 'field': 'URL'}])])
        self.assertIn('getvariable', load['steps'][-1]['sql'])
        request['credentials']['secret'] = 'SENTINEL'
        refused = library().call('lite_remote_mount', json.dumps(request))
        self.assertTrue(refused.startswith('ERR\n') and 'has no key secret' in refused and 'SENTINEL' not in refused, refused)

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
        # its rows: the plan's ONCE load, its statements a blob the host checks against its digest (B3, 2026-11-11)
        load = json.loads(p['load'])['root']
        self.assertEqual((load['_type'], load['cadence']), ('load', 'ONCE'))
        self.assertTrue(any('Meridian Capital' in text for text in load['content'].values()), load)
        self.assertNotIn('fill', p)
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
        # the loader's own functions, reached past the keys (C5-P: the library's loader is _loader.py since A1)
        from legend_lite._loader import loaded
        raw = loaded()
        p = raw._lib.lite_session_setup(raw._thread(), b'DuckDB')
        self.assertEqual(lib.unfreed(), before + 1)
        raw._lib.lite_free(raw._thread(), p)
        self.assertEqual(lib.unfreed(), before)


if __name__ == '__main__':
    unittest.main()
