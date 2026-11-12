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
    'compiler.py': {'PlannedSql', 'all_from_plan', 'from_plan'},
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
    ('frames.py', 'execute', 'self'),
    # 2026-11-12 (the second audit): the hosted DataCube's query in a read of the frames, by the read's own send
    # (frames.Reading.execute: the one send on its cursor), the read being no Database of its own
    ('_headless.py', 'execute', 'self._over'),
}

# The bridge's crossings by module. 2026-11-12 (C3): the hosted DataCube's query and statement (_headless.py), and a
# remote file's settings its plan runner kept (engine.py).
BRIDGE = {'_headless.py': 2, 'engine.py': 1}

# Each call of a value's own type, by module and text: none makes a PlannedSql. 2026-11-12: a database error rebuilt of
# its own type with a held value taken out of its words (frames.py redacted), never a statement.
TYPE_CALLS = {('frames.py', 'type(failure)(out)')}

# DuckDB's ways to run text or load data, on any receiver (2026-11-12, the second audit: from_arrow(...).create landed a
# planned stream step outside the one send): each pinned by module and receiver (SENDS), or refused
SEND_METHODS = {'execute', 'executemany', 'sql', 'query', 'from_query', 'from_arrow', 'from_df', 'from_parquet',
                'from_csv_auto', 'read_csv', 'read_parquet', 'read_json', 'install_extension', 'load_extension'}
MINT_NAMES = {'from_plan', 'all_from_plan'}


def census(name: str, source: str) -> dict:
    """One module, read: the names it imports from _planned, each form the guard refuses, its sends and its crossings."""
    tree = ast.parse(source)
    imported: set[str] = set()
    refused: list[str] = []
    sends: set[tuple[str, str, str]] = set()
    crossings = 0
    own = name == '_planned.py'
    for node in ast.walk(tree):
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
            if not own and isinstance(f, ast.Call) and isinstance(f.func, ast.Name) and f.func.id == 'type' \
                    and (name, ast.unparse(node)) not in TYPE_CALLS:
                refused.append(f'line {node.lineno}: {ast.unparse(node)[:60]!r}: a value\'s own type called')
            if not own and isinstance(f, ast.Attribute) and f.attr in ('__class__', '__new__'):
                refused.append(f'line {node.lineno}: {ast.unparse(node)[:60]!r}: a class reached through a value')
            if not own and isinstance(f, ast.Attribute) and f.attr == 'connect' and isinstance(f.value, ast.Name) \
                    and f.value.id == 'duckdb':
                refused.append(f'line {node.lineno}: duckdb.connect(): a database is the host\'s Database (_planned.connect)')
            if isinstance(f, ast.Attribute) and f.attr in SEND_METHODS:
                sends.add((name, f.attr, ast.unparse(f.value)))
            if isinstance(f, ast.Name) and f.id == 'across_the_bridge':
                crossings += 1
    return {'imported': imported, 'refused': refused, 'sends': sends, 'crossings': crossings}


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
        self.assertEqual({('m.py', 'from_arrow', 'con'), ('m.py', 'read_csv', 'duckdb')},
                         census('m.py', 'con.from_arrow(r).create(t)\nduckdb.read_csv(p)')['sends'])
        # a docstring's example is prose: no node of the tree
        self.assertEqual(set(), census('m.py', '"""trades.execute(q)"""')['sends'])

    def test_pythons_host_makes_and_sends_a_planned_statement_only_where_pinned(self):
        package = Path(FILES.Rlocation(os.environ['PLANNED_PACKAGE'])).parent
        modules = sorted(package.glob('*.py'))
        self.assertGreater(len(modules), 10, f'{package} holds {len(modules)} modules: the guard is not looking')
        imports: dict[str, set[str]] = {}
        refused: list[str] = []
        sends: set[tuple[str, str, str]] = set()
        bridge: dict[str, int] = {}
        for m in modules:
            c = census(m.name, m.read_text(encoding='utf-8'))
            refused += [f'{m.name}: {r}' for r in c['refused']]
            if c['imported']:
                imports[m.name] = c['imported']
            sends |= c['sends']
            if c['crossings'] and m.name != '_planned.py':
                bridge[m.name] = c['crossings']
        self.assertEqual([], refused, 'a PlannedSql is made by _planned.py alone, and _planned is imported by name')
        self.assertEqual(IMPORTS, imports, 'each module\'s names from _planned are pinned (IMPORTS): a mint where a plan'
                                          ' or the planner\'s answer is read, the one send, the bridge\'s check')
        self.assertEqual({m for m, names in IMPORTS.items() if names & MINT_NAMES},
                         {'compiler.py', 'projects.py', 'engine.py'}, 'the mints, by module')
        self.assertEqual(SENDS, sends, 'every statement Python\'s host sends goes through _planned.send, which refuses'
                                       ' any text no plan carries (SENDS)')
        self.assertEqual(BRIDGE, bridge, 'the statements the hosted DataCube hands across are checked at the named'
                                         ' crossings alone (BRIDGE)')


if __name__ == '__main__':
    unittest.main()
