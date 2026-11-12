"""THE PLANNED-SQL GUARD, its Python half, read by Python's own parser (the total wall's C3; 2026-11-12, the C checkpoint's
code audit, SHOULD-FIX 2: until then a regular expression in tools/guards/PlannedSqlTest.java, which a module alias, a
qualified call -- `_planned.from_plan(...)` -- a subclass, `type(s)(...)` or a send on another receiver passed). Python's
host sends its database only a PlannedSql, through the one send (python/legend_lite/_planned.py), which refuses any other
text by name; this pins, over every module of the package, where the type is made and where a statement is sent:

- `_planned` is imported one way, `from ._planned import <name>`, never aliased, never as a module; each module's names
  are pinned (IMPORTS). A mint (from_plan, all_from_plan) is imported where a plan or the planner's answer is read.
- A PlannedSql is constructed in _planned.py alone: no call of the class, no subclass, no `type(x)(...)`, no
  `x.__class__(...)`, no `__new__`.
- A send -- `.execute`, `.executemany`, `.sql`, `.query`, `.from_query` on ANY receiver -- is _planned.send's, or pinned
  by its module and receiver (SENDS).
- The bridge's crossings (`across_the_bridge`), counted by module (BRIDGE).
- (The C final audit's H4, 2026-11-12) each mint's uses, counted by module (MINT_REFERENCES, as the TypeScript guard's
  MINT_REFERENCES); duckdb imported as itself alone (`import duckdb as d`, `from duckdb import connect` refused); a send
  method reached and not called (`run = con.execute`, `getattr(con, 'execute')`) pinned or refused; and a value attached
  or a stream imported (`register`, `unregister`, `import_stream`) pinned by module and receiver (ATTACHES).
- (The audit's H1) the authored door -- a person's own statement planned as the planner's (Studio's SQL playground) --
  named by module, exactly (AUTHORED): Python's planner answers the kind; nothing in Python asks it.

Its TypeScript half is tools/guards/PlannedSqlTest.java. Each list moves only with a dated justification naming the task
(AGENTS.md, guard files), and shrinks."""

import ast
import os
import unittest
from pathlib import Path

from python.runfiles import runfiles

FILES = runfiles.Create()

# Each module's names from _planned. 2026-11-12 (C3; the audit's SHOULD-FIX 2): the mints are the compiler's readers
# (compiler.py), the load runners (projects.py) and execute's plan half (engine.py); the rest take the type, the one
# send, the bridge's check or the type's check (require).
IMPORTS = {
    # 2026-11-12 (the second audit): the host's Database (connect, Database, Result) where a database is made or held
    '_headless.py': {'Answered', 'Database', 'across_the_bridge', 'connect', 'send'},
    '_host.py': {'PlannedSql', 'send'},
    # 2026-11-12 (the C final audit's H2): require, a host operation's planner text and a snapshot's stream checked
    'compiler.py': {'PlannedSql', 'all_from_plan', 'from_plan', 'require'},
    'engine.py': {'PlannedSql', 'across_the_bridge', 'connect', 'from_plan', 'send'},
    'frames.py': {'Database', 'PlannedSql', 'Result', 'connect', 'send'},
    'projects.py': {'PlannedSql', 'all_from_plan', 'from_plan', 'send'},
    'warehouse.py': {'PlannedSql', 'require'},
}

# Each send by module, method and receiver. 2026-11-12: the one send (_planned.Database's run), and a frame's read
# whose own method runs the plan's statement through it (frames.py, Reading's `self.execute`).
SENDS = {
    # 2026-11-12 (the second audit): the Database's own -- the one send's run and a planned stream step's Arrow import
    ('_planned.py', 'execute', 'self._con'),
    ('_planned.py', 'from_arrow', 'self._con'),
    # 2026-11-12 (H13): the same import's table made, the relation API's send counted with it
    ('_planned.py', 'create', 'self._con.from_arrow(reader)'),
    ('frames.py', 'execute', 'self'),
    # 2026-11-12 (the second audit): the hosted DataCube's query in a read of the frames, by the read's own send
    # (frames.Reading.execute: the one send on its cursor), the read being no Database of its own
    ('_headless.py', 'execute', 'self._over'),
}

# The bridge's crossings by module. 2026-11-12 (C3): the hosted DataCube's query and statement (_headless.py), and a
# remote file's settings its plan runner kept (engine.py).
BRIDGE = {'_headless.py': 2, 'engine.py': 1}

# (TYPE_CALLS -- frames.py's `type(failure)(out)`, a database error rebuilt of its own type: GONE 2026-11-12, the
# re-audit's S4. Outside _planned.py a value's type is named (`type(x).__name__`) or compared (`type(x) is T`), never
# called, bound or handed on, and `__class__` is never reached: `T = type(p); T(text)` made a PlannedSql unseen.)

# DuckDB's ways to run text or load data, on any receiver (2026-11-12, the second audit: from_arrow(...).create landed a
# planned stream step outside the one send): each pinned by module and receiver (SENDS), or refused
SEND_METHODS = {'execute', 'executemany', 'sql', 'query', 'from_query', 'from_arrow', 'from_df', 'from_parquet',
                'from_csv_auto', 'read_csv', 'read_parquet', 'read_json', 'install_extension', 'load_extension',
                # 2026-11-12 (the C final audit's H13): the relation API's sends -- a relation made into a table or a
                # view, inserted, written out
                'create', 'create_view', 'insert_into', 'insert', 'to_table', 'to_view', 'write_csv', 'write_parquet',
                'to_csv', 'to_parquet'}
MINT_NAMES = {'from_plan', 'all_from_plan'}

# Each mint's uses by module (2026-11-12, the C final audit's H4, measured: an import is pinned by IMPORTS, a new call
# of a mint in a pinned module was not): the compiler's readers (its plans, catalog, host operations, table model's copy
# select list -- H2's), the load runners, execute's plan half. A use added or gone moves its count, dated.
MINT_REFERENCES = {'compiler.py': 6, 'engine.py': 2, 'projects.py': 9}

# A send method's name reached and not called, by module, name and receiver (H4, measured 2026-11-12): each a field of a
# plan (its sql), a URL (its query) or a view (page.py's), none a method handed on. A method handed on -- `run =
# con.execute` -- is a send the census would not see as one, so any other is red.
REFERENCES = {
    ('_headless.py', 'query', 'parts'),
    ('engine.py', 'query', 'url'),
    ('engine.py', 'sql', 'plan'),
    ('frames.py', 'sql', 'plan'),
    ('page.py', 'query', 'self'),
    ('page.py', 'sql', 'self'),
    ('page.py', 'sql', "compiler.read_planned(said['plan'])"),
}

# A value attached by name, or a stream imported, by module, method and receiver (H4, measured 2026-11-12): the
# Database's own (its connection), the load runner's attach and stream (projects.py), a frame's attach (frames.py); and
# methods of the same names that attach nothing to a database -- the hosted DataCube's frames and session (datacube.py,
# page.py), the interpreter's exit and IPython's events.
ATTACH_METHODS = {'register', 'unregister', 'import_stream'}
ATTACHES = {
    ('_planned.py', 'register', 'self._con'),
    ('_planned.py', 'unregister', 'self._con'),
    ('projects.py', 'register', 'connection'),
    ('projects.py', 'unregister', 'connection'),
    ('projects.py', 'import_stream', 'connection'),
    ('frames.py', 'register', 'target'),
    ('frames.py', 'unregister', 'con'),
    ('datacube.py', 'register', 'self.frames'),
    ('datacube.py', 'unregister', 'self.frames'),
    ('datacube.py', 'register', 'atexit'),
    ('datacube.py', 'register', 'shell.events'),
    ('page.py', 'register', 'session'),
    ('page.py', 'register', 'page._session'),
    ('page.py', 'unregister', 'session'),
}

# ONE CHOKE POINT PER TRANSPORT (the C final audit's H13, 2026-11-12; the user: "how do we stop this from actually being
# able to be added without loud alarm bells?"). DuckDB: the one connection is made and sent to in _planned.py alone (its
# Database: duckdb.connect, self._con; SENDS) -- outside it the duckdb module is named for its errors, a connection's
# type and its version alone, never for a function that runs on its default connection (duckdb.sql, duckdb.execute,
# duckdb.append, duckdb.read_csv, ...). HTTP: a SQL API's statement ('submit') is sent by warehouse.py alone
# (Warehouse.arrow_chunks: required a PlannedSql), and each module that opens a network client is pinned.
DUCKDB_NAMES = {'Error', 'TransactionException', 'DuckDBPyConnection', '__version__'}
SUBMITS = {'warehouse.py': 1}
NETWORK = {
    # the SQL API's client (the one): its HTTP, its errors
    'warehouse.py': {'http.client', 'urllib.error', 'urllib.request'},
    # the hosted DataCube's fetch, as a browser's (the bundle's own sends: its warehouse's statement is warehouse.ts's)
    '_headless.py': {'urllib.error', 'urllib.request'},
    # the engine's own server (it serves; it sends nothing)
    'engine.py': {'http.server', 'socket'},
}
_NETWORK_ROOTS = {'urllib', 'http', 'requests', 'urllib3', 'socket', 'httpx', 'aiohttp', 'ssl', 'websocket', 'websockets'}

# THE AUTHORED DOOR by module (the C final audit's H1, 2026-11-12, measured): a person's statement planned as the
# planner's is Studio's SQL playground's alone (tools/guards/PlannedSqlTest.java AUTHORED); Python's planner answers the
# request kind ('authored' -> the native library's 'lite_authored') and its library declares the export, and no module
# asks it (DataCube's python-host.ts refuses the kind unasked). Its spellings -- the kind's text, the export's, a name or
# an attribute named authored -- each counted.
AUTHORED_NAMES = {'authored', 'lite_authored'}
AUTHORED = {'_planner.py': 2, '_library.py': 1}


def _named_or_compared(call: ast.Call, parents: dict) -> bool:
    """Whether a one-argument ``type(...)`` is only named (``.__name__``, ``.__qualname__``) or compared (``is``, ``==``)."""
    up = parents.get(id(call))
    return (isinstance(up, ast.Attribute) and up.attr in ('__name__', '__qualname__')) or \
        (isinstance(up, ast.Compare) and all(isinstance(o, (ast.Is, ast.IsNot, ast.Eq, ast.NotEq)) for o in up.ops))


def census(name: str, source: str) -> dict:
    """One module, read: the names it imports from _planned, each form the guard refuses, its sends and its crossings."""
    tree = ast.parse(source)
    imported: set[str] = set()
    refused: list[str] = []
    sends: set[tuple[str, str, str]] = set()
    references: set[tuple[str, str, str]] = set()
    attaches: set[tuple[str, str, str]] = set()
    crossings = mints = authored = submits = 0
    network: set[str] = set()
    own = name == '_planned.py'
    called = {id(n.func) for n in ast.walk(tree) if isinstance(n, ast.Call)}
    parents = {id(c): p for p in ast.walk(tree) for c in ast.iter_child_nodes(p)}
    for node in ast.walk(tree):
        # S4 (the re-audit, 2026-11-12): a class reached through a value, called or not
        if isinstance(node, ast.Attribute) and node.attr == '__class__' and not own:
            refused.append(f'line {node.lineno}: {ast.unparse(node)[:60]!r}: a class reached through a value')
        # M2 (the re-audit, 2026-11-12): the Database's own run, called or handed on, is _planned.send's alone
        if isinstance(node, ast.Attribute) and node.attr == '_send_planned' and not own:
            refused.append(f'line {node.lineno}: {ast.unparse(node)[:60]!r}: the Database\'s own run is _planned.send\'s'
                           ' alone (send, which requires a PlannedSql)')
        # H4 and H1, on every node: a mint's use, a send method reached, an attach, the authored door's spellings
        if isinstance(node, ast.Name) and node.id in MINT_NAMES and not own:
            mints += 1
        if isinstance(node, ast.Attribute) and node.attr in SEND_METHODS and id(node) not in called:
            references.add((name, node.attr, ast.unparse(node.value)))
        if isinstance(node, ast.Attribute) and node.attr in ATTACH_METHODS:
            attaches.add((name, node.attr, ast.unparse(node.value)))
        if isinstance(node, ast.Constant) and node.value in AUTHORED_NAMES \
                or isinstance(node, ast.Name) and node.id in AUTHORED_NAMES \
                or isinstance(node, ast.Attribute) and node.attr in AUTHORED_NAMES \
                or isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in AUTHORED_NAMES:
            authored += 1
        # H13: the duckdb module's own names outside the one Database; a network client; the SQL API's submit
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == 'duckdb' \
                and not (own and node.attr == 'connect') and node.attr not in DUCKDB_NAMES:
            refused.append(f'line {node.lineno}: duckdb.{node.attr}: the duckdb module is sent to through the one Database'
                           ' (_planned.py), never on its default connection')
        if isinstance(node, ast.Import):
            network |= {a.name for a in node.names if a.name.split('.')[0] in _NETWORK_ROOTS and a.name != 'urllib.parse'}
        if isinstance(node, ast.ImportFrom) and (node.module or '').split('.')[0] in _NETWORK_ROOTS \
                and node.module != 'urllib.parse':
            network.add(node.module)
        if isinstance(node, ast.Constant) and node.value == 'submit':
            submits += 1
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == 'duckdb' and alias.asname is not None:
                    refused.append(f'line {node.lineno}: {ast.unparse(node)!r}: duckdb is imported as itself')
        if isinstance(node, ast.ImportFrom) and (node.module or '').split('.')[0] == 'duckdb':
            refused.append(f'line {node.lineno}: {ast.unparse(node)!r}: duckdb\'s names are reached through duckdb')
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == 'getattr' \
                and any(isinstance(a, ast.Constant) and a.value in SEND_METHODS | ATTACH_METHODS for a in node.args):
            refused.append(f'line {node.lineno}: {ast.unparse(node)[:60]!r}: a send reached by its name\'s text')
        if isinstance(node, ast.ImportFrom):
            planned = (node.module or '').split('.')[-1] == '_planned'
            if planned and not (node.level == 1 and node.module == '_planned'):
                refused.append(f'line {node.lineno}: _planned imported as {ast.unparse(node)!r}, not `from ._planned import`')
            for alias in node.names:
                if alias.name == '_planned':
                    refused.append(f'line {node.lineno}: _planned imported as a module ({ast.unparse(node)!r})')
                elif planned:
                    if alias.name == '*' or alias.asname is not None:
                        refused.append(f'line {node.lineno}: {ast.unparse(node)!r}: a name of _planned is imported as itself')
                    else:
                        imported.add(alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split('.')[-1] == '_planned':
                    refused.append(f'line {node.lineno}: _planned imported as a module ({ast.unparse(node)!r})')
        elif isinstance(node, ast.Attribute) and node.attr == '_con' and not own:
            refused.append(f'line {node.lineno}: {ast.unparse(node)}: the Database\'s own connection reached around it')
        elif isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == '_planned':
            # a module's attribute (engine.py's own function named _planned is no module, and is never reached into)
            refused.append(f'line {node.lineno}: _planned.{node.attr}: _planned reached as a module')
        elif isinstance(node, ast.ClassDef) and not own:
            for base in node.bases:
                if isinstance(base, ast.Name) and base.id == 'PlannedSql' or 'PlannedSql' in ast.unparse(base):
                    refused.append(f'line {node.lineno}: class {node.name} subclasses PlannedSql')
        elif isinstance(node, ast.Attribute) and not own and isinstance(node.value, ast.Name) \
                and node.value.id == 'PlannedSql':
            refused.append(f'line {node.lineno}: PlannedSql.{node.attr}: the type is used as itself, never reached into')
        elif isinstance(node, ast.Call):
            f = node.func
            if not own and isinstance(f, ast.Name) and f.id == 'PlannedSql':
                refused.append(f'line {node.lineno}: PlannedSql constructed outside _planned.py')
            if not own and isinstance(f, ast.Name) and f.id == 'type' and len(node.args) == 1 and not _named_or_compared(node, parents):
                refused.append(f'line {node.lineno}: {ast.unparse(parents.get(id(node), node))[:60]!r}: a value\'s type is'
                               ' named or compared outside _planned.py, never called, bound or handed on')

            if not own and isinstance(f, ast.Attribute) and f.attr == '__new__':
                refused.append(f'line {node.lineno}: {ast.unparse(node)[:60]!r}: a class reached through a value')
            if not own and isinstance(f, ast.Attribute) and f.attr == 'connect' and isinstance(f.value, ast.Name) \
                    and f.value.id == 'duckdb':
                refused.append(f'line {node.lineno}: duckdb.connect(): a database is the host\'s Database (_planned.connect)')
            if isinstance(f, ast.Attribute) and f.attr in SEND_METHODS:
                sends.add((name, f.attr, ast.unparse(f.value)))
            if isinstance(f, ast.Name) and f.id == 'across_the_bridge':
                crossings += 1
    return {'imported': imported, 'refused': refused, 'sends': sends, 'crossings': crossings, 'mints': mints,
            'references': references, 'attaches': attaches, 'authored': authored, 'submits': submits,
            'network': network}


class PlannedGuardTest(unittest.TestCase):

    def test_the_census_sees_every_form(self):
        """Each form the audit named, refused: measured, so a census that stops seeing one is red."""
        for source in [
            'from . import _planned\n_planned.from_plan(x)',
            'from ._planned import from_plan as f',
            'from ._planned import *',
            'import legend_lite._planned',
            'from legend_lite._planned import from_plan',
            'class Mine(PlannedSql):\n    pass',
            'type(planned)(text)',
            'planned.__class__(text)',
            'str.__new__(PlannedSql, text)',
            'PlannedSql("SELECT 1")',
            'duckdb.connect()',
            'frames.connection._con.execute(q)',
        ]:
            self.assertTrue(census('m.py', source)['refused'], f'the census does not refuse: {source!r}')
        self.assertEqual({'from_plan', 'send'}, census('m.py', 'from ._planned import from_plan, send')['imported'])
        self.assertEqual({('m.py', 'sql', 'con'), ('m.py', 'from_query', 'duckdb')},
                         census('m.py', 'con.sql(q)\nduckdb.from_query(q)')['sends'])
        self.assertEqual({('m.py', 'query', 'self._db')}, census('m.py', 'self._db.query(q)')['sends'])
        self.assertEqual({('m.py', 'from_arrow', 'con'), ('m.py', 'create', 'con.from_arrow(r)'), ('m.py', 'read_csv', 'duckdb')},
                         census('m.py', 'con.from_arrow(r).create(t)\nduckdb.read_csv(p)')['sends'])
        # a docstring's example is prose: no node of the tree
        self.assertEqual(set(), census('m.py', '"""trades.execute(q)"""')['sends'])

    def test_the_census_sees_the_final_audits_forms(self):
        """The C final audit's H4 and H1 (2026-11-12): each form planted, and seen."""
        for source in ['import duckdb as d\nd.connect()', 'from duckdb import connect\nconnect()',
                       'from duckdb import sql', "getattr(con, 'execute')(q)", "getattr(con, 'register')"]:
            self.assertTrue(census('m.py', source)['refused'], f'the census does not refuse: {source!r}')
        self.assertEqual(3, census('m.py', 'from ._planned import from_plan, all_from_plan\n'
                                           'from_plan(a)\nall_from_plan(b)\nf = from_plan')['mints'])
        self.assertEqual({('m.py', 'execute', 'con')}, census('m.py', 'run = con.execute\nrun(q)')['references'])
        self.assertEqual(set(), census('m.py', 'con.execute(q)')['references'], 'a call is a send, not a reference')
        self.assertEqual({('m.py', 'register', 'db'), ('m.py', 'import_stream', 'db')},
                         census('m.py', 'db.register(n, v)\nload = db.import_stream')['attaches'])
        for source in ["planner.ask({'kind': 'authored', 'request': r})", "lib.call('lite_authored', r)",
                       'client.authored(r)', 'def authored(r):\n    pass']:
            self.assertEqual(1, census('m.py', source)['authored'], f'the census does not see: {source!r}')

    def test_the_census_sees_the_re_audits_forms(self):
        """S4 and M2 (the re-audit, 2026-11-12): each form planted, and refused; a type named or compared is not."""
        for source in ['T = type(p)\nT(text)', 'type(p)(text)', 'make(type(p), text)', 'p.__class__', 'k = p.__class__',
                       'db._send_planned(text, None)', 'run = db._send_planned']:
            self.assertTrue(census('m.py', source)['refused'], f'the census does not refuse: {source!r}')
        for source in ["f'{type(e).__name__}: {e}'", 'type(x) is int', 'type(a) == type(b)']:
            self.assertEqual([], census('m.py', source)['refused'], source)

    def test_the_census_sees_every_transports_forms(self):
        """H13 (2026-11-12): each way past the one choke point, planted, and seen."""
        for source in ["duckdb.sql('DROP TABLE t')", 'duckdb.execute(q)', 'duckdb.append(t, df)', "duckdb.read_csv('x')",
                       'con = duckdb.default_connection']:
            self.assertTrue(census('m.py', source)['refused'], f'the census does not refuse: {source!r}')
        self.assertEqual([], census('m.py', 'try:\n    pass\nexcept duckdb.Error as e:\n    pass')['refused'])
        self.assertEqual({('m.py', 'create', 'con.from_arrow(r)'), ('m.py', 'from_arrow', 'con')},
                         census('m.py', 'con.from_arrow(r).create(t)')['sends'])
        self.assertEqual({('m.py', 'insert_into', 'rel')}, census('m.py', "rel.insert_into('t')")['sends'])
        self.assertEqual({'urllib.request', 'requests'},
                         census('m.py', 'import urllib.request\nimport urllib.parse\nfrom requests import post')['network'])
        self.assertEqual(1, census('m.py', "self._call('submit', {'sql': q})")['submits'])

    def test_pythons_host_makes_and_sends_a_planned_statement_only_where_pinned(self):
        package = Path(FILES.Rlocation(os.environ['PLANNED_PACKAGE'])).parent
        modules = sorted(package.glob('*.py'))
        self.assertGreater(len(modules), 10, f'{package} holds {len(modules)} modules: the guard is not looking')
        imports: dict[str, set[str]] = {}
        refused: list[str] = []
        sends: set[tuple[str, str, str]] = set()
        bridge: dict[str, int] = {}
        mints: dict[str, int] = {}
        references: set[tuple[str, str, str]] = set()
        attaches: set[tuple[str, str, str]] = set()
        authored: dict[str, int] = {}
        submits: dict[str, int] = {}
        network: dict[str, set[str]] = {}
        for m in modules:
            c = census(m.name, m.read_text(encoding='utf-8'))
            refused += [f'{m.name}: {r}' for r in c['refused']]
            if c['imported']:
                imports[m.name] = c['imported']
            sends |= c['sends']
            if c['crossings'] and m.name != '_planned.py':
                bridge[m.name] = c['crossings']
            if c['mints']:
                mints[m.name] = c['mints']
            references |= c['references']
            attaches |= c['attaches']
            if c['authored']:
                authored[m.name] = c['authored']
            if c['submits']:
                submits[m.name] = c['submits']
            if c['network']:
                network[m.name] = c['network']
        self.assertEqual([], refused, 'a PlannedSql is made by _planned.py alone, and _planned is imported by name')
        self.assertEqual(IMPORTS, imports, 'each module\'s names from _planned are pinned (IMPORTS): a mint where a plan'
                                          ' or the planner\'s answer is read, the one send, the bridge\'s check')
        self.assertEqual({m for m, names in IMPORTS.items() if names & MINT_NAMES},
                         {'compiler.py', 'projects.py', 'engine.py'}, 'the mints, by module')
        self.assertEqual(SENDS, sends, 'every statement Python\'s host sends goes through _planned.send, which refuses'
                                       ' any text no plan carries (SENDS)')
        self.assertEqual(BRIDGE, bridge, 'the statements the hosted DataCube hands across are checked at the named'
                                         ' crossings alone (BRIDGE)')
        self.assertEqual(MINT_REFERENCES, mints, 'each mint\'s uses, by module (MINT_REFERENCES): a new one is a new'
                                                 ' place a text is typed as a plan\'s')
        self.assertEqual(REFERENCES, references, 'a send method reached and not called is a field of a plan, a URL or'
                                                 ' a view (REFERENCES), never a method handed on')
        self.assertEqual(ATTACHES, attaches, 'a value attached or a stream imported, by module and receiver (ATTACHES)')
        self.assertEqual(AUTHORED, authored, 'the authored door, by module (AUTHORED): Python answers the kind, and'
                                             ' asks it nowhere')
        self.assertEqual(SUBMITS, submits, 'ONE CHOKE POINT (H13): a SQL API\'s statement is submitted by warehouse.py'
                                           ' alone, a PlannedSql (SUBMITS)')
        self.assertEqual(NETWORK, network, 'ONE CHOKE POINT (H13): each module that opens a network client is pinned'
                                           ' (NETWORK): a new one could carry a statement to a SQL API')


if __name__ == '__main__':
    unittest.main()
