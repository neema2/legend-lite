"""THE RUN SIDE'S CLOSURE, Python's plan/exec split (C5-P, 2026-11-15; the user: "let's lock this down"). It replaces the run
wall (python/tests/test_run_wall.py, interim scaffolding from the user's ruling M2 of 2026-11-13 to C5), as //core:exec's
pinned deps replace a reading of Java's code: Python's modules are SPLIT, so the rule is a plain dependency rule.

- THE RUN SIDE IMPORTS NO PLANNER, BY THE BUILD: this test's only package deps are //python:run and //python:seam, so a
  run module that imports a planner module -- or any module outside those two libraries -- does not load here
  (``test_every_run_module_loads_with_the_run_library_alone``).
- AND BY ITS SOURCES, every import read, a function's own included (one the import above would not run): a run module
  imports only run and seam modules; a seam module only seam modules; the planner's modules (PLANNER) import no run
  module and take from _planned no name that sends; the one send is imported by run modules alone, so every statement
  Python sends is sent by the run side.
- A FRAME WRAPS EVERY SEND, at run time: _planned.send refuses a statement sent outside a run (``Unframed``), and each
  run function of the run side sends inside its own frame (``test_the_one_send_is_framed``).

Each list moves only with a dated justification naming the task (AGENTS.md, guard files)."""

import ast
import os
import unittest
from pathlib import Path

from python.runfiles import runfiles

FILES = runfiles.Create()

# THE SEAM (//python:seam): the one text and its one send, the run frame, the plan's own records and readers, the exact
# JSON -- what both sides depend on, as Java's records are. 2026-11-15 (C5-P).
SEAM = frozenset({'_planned', '_runframe', '_plans', '_json'})

# THE RUN SIDE (//python:run): the hosts' runners, a read of the database, the type facts a plan's rows are typed by.
# 2026-11-15 (C5-P).
RUN = frozenset({'_runs', '_reads', '_types', '_type_facts'})

# THE PLANNER'S OWN MODULES: the native library and its protocol, the compiler's bindings, the model home and the
# compiler's threads. They send nothing. 2026-11-15 (C5-P): the run wall's PLANNER_MODULES named the doors that plan
# too (engine, datacube, page, notebook, _headless), which hand the run side its statements now, and are held below to
# send nothing themselves (the one send imported by run modules alone).
PLANNER = frozenset({'_library', '_planner', 'compiler', '_home', '_pool'})

# What of _planned sends, or holds a way to: never a planner module's. 2026-11-15 (C5-P).
SENDING_NAMES = frozenset({'send', 'connect', 'Database', 'Result', 'require'})


def package_sources() -> dict[str, str]:
    """Every module of the package by name, as shipped."""
    package = Path(FILES.Rlocation(os.environ['PLANNED_PACKAGE'])).parent
    return {p.stem: p.read_text(encoding='utf-8') for p in sorted(package.glob('*.py'))}


def imports(source: str) -> tuple[set[str], set[str]]:
    """What a module imports of the package, wherever in it (a function's own import too): the modules, and the names it
    takes from _planned. Every form: `from . import x`, `from .x import a`, `from legend_lite import x`, `from
    legend_lite.x import a`, `import legend_lite.x`; a form this cannot read (a star of the package, importlib) refused."""
    modules: set[str] = set()
    planned: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom):
            if node.level >= 1 and node.level != 1:
                raise AssertionError(f'line {node.lineno}: an import beyond the package ({ast.unparse(node)})')
            if (node.module or '').split('.')[0] == 'importlib':
                raise AssertionError(f'line {node.lineno}: importlib, which this rule cannot read')
            own = node.level == 1 or node.module == 'legend_lite' or (node.module or '').startswith('legend_lite.')
            if not own:
                continue
            base = node.module if node.level == 1 else (node.module or '').removeprefix('legend_lite').lstrip('.')
            if not base:
                modules.update(a.name for a in node.names)
                if any(a.name == '*' for a in node.names):
                    raise AssertionError(f'line {node.lineno}: a star of the package ({ast.unparse(node)})')
            else:
                modules.add(base.split('.')[0])
                if base == '_planned':
                    planned.update(a.name for a in node.names)
        elif isinstance(node, ast.Import):
            for a in node.names:
                # importlib.util's find_spec alone (_extra.py: whether a package is installed), which imports nothing
                if (a.name == 'importlib' or a.name.startswith('importlib.')) and (a.name, a.asname) != (
                        'importlib.util', None):
                    raise AssertionError(f'line {node.lineno}: importlib, which this rule cannot read')
                if a.name.startswith('legend_lite.'):
                    modules.add(a.name.split('.')[1])
        elif isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == 'importlib' and \
                node.attr != 'util':
            raise AssertionError(f'line {node.lineno}: importlib.{node.attr}, which this rule cannot read')
        elif isinstance(node, ast.Attribute) and ast.unparse(node.value) == 'importlib.util' and node.attr != 'find_spec':
            raise AssertionError(f'line {node.lineno}: importlib.util.{node.attr}, which this rule cannot read')
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == '__import__':
            raise AssertionError(f'line {node.lineno}: __import__, which this rule cannot read')
    return modules, planned


def broken(sources: dict[str, str]) -> list[str]:
    """Every import that breaks the split, in words."""
    out: list[str] = []
    for name, source in sorted(sources.items()):
        try:
            modules, planned = imports(source)
        except AssertionError as refused:
            out.append(f'{name}: {refused}')
            continue
        if name in RUN and not modules <= RUN | SEAM:
            out.append(f'{name} (run) imports {sorted(modules - RUN - SEAM)}: the run side imports the run side and the'
                       ' seam alone')
        if name in SEAM and not modules <= SEAM:
            out.append(f'{name} (seam) imports {sorted(modules - SEAM)}: the seam imports the seam alone')
        if name in PLANNER and modules & RUN:
            out.append(f'{name} (planner) imports {sorted(modules & RUN)}: the planner imports no run module')
        if name in PLANNER and planned & SENDING_NAMES:
            out.append(f'{name} (planner) takes {sorted(planned & SENDING_NAMES)} from _planned: the planner sends'
                       ' nothing')
        if name not in RUN and 'send' in planned:
            out.append(f'{name} imports the one send: only the run side sends (each door hands it its statements)')
    return out


class RunClosureTest(unittest.TestCase):

    def test_every_run_module_loads_with_the_run_library_alone(self):
        # this test's deps are //python:run (and :seam through it): a run module importing anything else fails here
        import importlib
        for name in sorted(RUN | SEAM):
            with self.subTest(module=name):
                importlib.import_module(f'legend_lite.{name}')

    def test_the_packages_modules_keep_the_split(self):
        sources = package_sources()
        self.assertTrue(RUN | SEAM | PLANNER <= set(sources), 'every module named here is the package\'s')
        self.assertEqual([], broken(sources))

    def test_the_rule_sees_every_break(self):
        # each a violator the rule must name, red on revert of its own check
        cases = {
            ('_runs', 'from . import compiler'): 'the run side imports',
            ('_reads', 'def f():\n    from .frames import Frames'): 'the run side imports',
            ('_reads', 'import legend_lite.engine'): 'the run side imports',
            ('_reads', 'from legend_lite import _library'): 'the run side imports',
            ('_reads', 'from legend_lite.compiler import plan'): 'the run side imports',
            ('_planned', 'from . import _reads'): 'the seam imports',
            ('compiler', 'from ._runs import run_load'): 'the planner imports no run module',
            ('compiler', 'from ._planned import PlannedSql, send'): 'the planner sends nothing',
            ('_library', 'from ._planned import connect'): 'the planner sends nothing',
            ('frames', 'from ._planned import send'): 'only the run side sends',
            ('engine', 'def f():\n    from legend_lite._planned import send'): 'only the run side sends',
            ('_reads', 'import importlib'): 'importlib',
            ('_reads', 'from importlib import import_module'): 'importlib',
            ('_reads', 'import importlib.util\nimportlib.import_module("legend_lite.compiler")'): 'importlib',
            ('_reads', 'import importlib.util as u'): 'importlib',
            ('_reads', '__import__("legend_lite.compiler")'): '__import__',
            ('_runs', 'from . import *'): 'a star of the package',
        }
        for (name, source), said in cases.items():
            with self.subTest(module=name, source=source):
                found = broken({name: source})
                self.assertTrue(found and said in found[0], found)
        # and the forms that keep it
        self.assertEqual([], broken({'_runs': 'from ._planned import send\nfrom . import _plans', 'frames': 'from ._reads'
                                     ' import run_plan\nfrom . import compiler', 'compiler': 'from ._planned import'
                                     ' PlannedSql, from_plan\nfrom ._plans import Plan'}))

    def test_the_one_send_is_framed(self):
        from legend_lite import _reads, _runs
        from legend_lite._planned import Unframed, connect, from_plan, send
        from legend_lite._plans import Column, Plan
        from legend_lite._runframe import current_run, in_run

        db = connect()
        one = from_plan('SELECT 1 AS n')
        # outside any run the one send refuses a plan's statement by name, and runs nothing
        with self.assertRaisesRegex(Unframed, 'sent only inside a run'):
            send(db, one)
        # inside one it runs
        with in_run('a test'):
            self.assertEqual([(1,)], send(db, one).fetchall())
        self.assertIsNone(current_run())
        # each run function sends inside its own frame, the caller in none: the one send above would refuse it else
        _runs.run_statements(db, [from_plan('CREATE TABLE t AS SELECT 1 AS n')])
        self.assertEqual([(1,)], _runs.ask(db, from_plan('SELECT count(*) FROM t')))
        self.assertEqual(1, _reads.count_of(db, Plan(from_plan('SELECT count(*) FROM t'), ())))
        self.assertEqual(1, _reads.run_arrow(db, one, ()).num_rows)
        self.assertEqual('[{"n":1}]', _reads.run_rows_json(db, one, ()))
        typed = _reads.run_plan(db, Plan(one, (Column('n', 'Integer', {}),)))
        self.assertEqual([1], typed.column('n').to_pylist())
        self.assertIsNone(current_run())


if __name__ == '__main__':
    unittest.main()
