"""THE RUN SIDE'S CLOSURE, Python's plan/exec split (C5-P, 2026-11-15; the user: "let's lock this down"). It replaces the run
wall (python/tests/test_run_wall.py, interim scaffolding from the user's ruling M2 of 2026-11-13 to C5), as //core:exec's
pinned deps replace a reading of Java's code: Python's modules are SPLIT, so the rule is a plain dependency rule.

- THE RUN SIDE IMPORTS NO PLANNER, BY THE BUILD: this test's only package deps are //python:run and //python:seam, so a
  run module that imports a planner module -- or any module outside those two libraries -- does not load here
  (``test_every_run_module_loads_with_the_run_library_alone``). Its sources are read from a copy outside the import path.
- AND BY ITS SOURCES, every import read, a function's own included (one the import above would not run). The three
  sets are the BUILD's own (SEAM_SRCS, RUN_SRCS, COMPILER_SRCS, LOADER_SRCS, handed in as names): a run module imports
  only run and seam modules, and never the package itself (its __init__ imports the compiler); a seam module only seam
  modules; a module of :compiler imports no run module and takes from _planned no name that sends; the one send is
  imported by run modules alone; a plane's send is called by run modules alone. THE NATIVE LIBRARY BY TWO KEYS (A1):
  its loader is imported by the planning key (_library) and the service key (_services, the warehouse's wire alone)
  and no other module, so the run side's closure reaches the loader through the service key alone and the planning key
  never. A run function takes and answers no planner (A5); a transaction's bounds are the run side's (A3).
- A RUN OPENS ONLY ON THE RUN SIDE, SPELLED ONE WAY: `runs`, `in_run` and `exempt` are imported from _runframe by run
  modules alone, by their names, and used only as `@runs(...)` on a function and `with in_run(...)` / `with
  exempt(...)` -- so no door opens a run in which it could plan.
- EVERY SEND OF THE RUN SIDE IS FRAMED, read: each call of the one send, of a sending method (a read's execute, a plane's
  arrow_chunks, a stream's import) or of a sending helper sits in an `@runs` function or a `with in_run(...)` block, or
  in a sending helper (SENDING_HELPERS) whose every call is so.
- AND AT RUN TIME: _planned.send refuses a statement sent outside a run (``Unframed``), as the warehouse's send does;
  each run function sends inside its own frame (``test_the_one_send_is_framed``).
- WHAT THIS CANNOT READ IS REFUSED: importlib under any name but the `importlib.util.find_spec` chain, `__import__` and
  `import_module` by any reference, a star of the package, an import beyond it.

Each list moves only with a dated justification naming the task (AGENTS.md, guard files)."""

import ast
import os
import unittest
from pathlib import Path

from python.runfiles import runfiles

FILES = runfiles.Create()


def _modules(variable: str) -> frozenset[str]:
    """A library's modules by name, as the BUILD lists its srcs (handed in by name, never as files)."""
    return frozenset(Path(p).stem for p in os.environ[variable].split())


# THE SEAM (//python:seam), THE RUN SIDE (//python:run), THE PLANNER (//python:compiler): the BUILD's own lists, so a
# module moved between libraries moves its rule with it (the C5-P audit's item 3, 2026-11-15: a hand list named 5 of
# :compiler's 12).
SEAM = _modules('SEAM_SRCS')
RUN = _modules('RUN_SRCS')
PLANNER = _modules('COMPILER_SRCS')
# THE NATIVE LIBRARY'S LOADER (//python:loader), imported by its two keys alone: the planning key (_library, the
# planner's) and the service key (_services, the run side's, the warehouse's wire alone). 2026-11-15 (C5-P, A1).
LOADER = _modules('LOADER_SRCS')
KEYS = {'_library': 'the planning key', '_services': 'the service key'}

# What of _planned sends, or holds a way to: never a planner module's. 2026-11-15 (C5-P).
SENDING_NAMES = frozenset({'send', 'connect', 'Database', 'Result', 'require'})

# (PLANNER_SENDERS -- the warehouse's client, :compiler's carve-out: GONE 2026-11-15 with C5-P's A1, the client a run
# module over the service key.)

# A remote plane's send (the warehouse's SQL API: Warehouse.arrow_chunks, framed by its own check too): called by run
# modules alone. 2026-11-15 (C5-P).
PLANE_SENDS = frozenset({'arrow_chunks'})

# The methods that send, called on any receiver in a run module: a read's own send (Reading.execute), a plane's, a
# stream's landing (the Database's Arrow import). 2026-11-15 (C5-P, the audit's item 4); and a transaction's bounds,
# run acts (C5-P's A3), called by run modules alone.
TRANSACTION_METHODS = frozenset({'begin', 'commit', 'rollback'})
SEND_METHODS = frozenset({'execute', 'arrow_chunks', 'import_stream'}) | TRANSACTION_METHODS

# The calls that reach the warehouse's SQL API over HTTP (warehouse._urlopen, its one place a request goes out).
# 2026-11-15 (C5-P, A2).
SEND_NAMES = frozenset({'send', '_urlopen'})

# THE SENDING HELPERS: run-module functions that send outside a frame of their own, each called only where a frame
# holds -- in a run, or in another helper. 2026-11-15 (C5-P, the audit's item 4): a statement run with a failure's held
# values taken out (_reads.execute_redacted, public as frames.execute_redacted: called by the runs alone), and a load's
# stream step (_runs._stream, run_host_load's); (C5-P's A2) the warehouse's HTTP (_reach: its _exchange's, a run, and
# its statement's chunks') and a statement's chunks (Warehouse.arrow_chunks, a generator, which refuses outside a run
# itself: Unframed).
SENDING_HELPERS = frozenset({'execute_redacted', '_stream', '_reach', 'arrow_chunks'})

# A PLANNER'S TYPES, which no run function takes or answers (the run wall's L2, restored by C5-P's A5, 2026-11-15): a held
# model, the planning key, the loader; and a callable answering a plan or a planner's text (a planner handed in under
# another name).
PLANNER_TYPES = frozenset({'HeldModel', 'Library', 'Loaded', 'PlannerModule'})
PLANNER_MODULE_NAMES = frozenset({'compiler', '_library', '_loader', '_planner'})
PLANNED_ANSWERS = frozenset({'PlannedSql', 'PlanText', 'Plan'})

# What opens a run (_runframe), spelled one way and on the run side alone. 2026-11-15 (C5-P, the audit's item 6).
OPENERS = frozenset({'runs', 'in_run', 'exempt'})


def package_sources() -> dict[str, str]:
    """Every module of the package by name, as shipped."""
    package = Path(FILES.Rlocation(os.environ['PLANNED_PACKAGE'])).parent
    return {p.stem: p.read_text(encoding='utf-8') for p in sorted(package.glob('*.py'))}


def _parents(tree: ast.AST) -> dict[int, ast.AST]:
    return {id(c): p for p in ast.walk(tree) for c in ast.iter_child_nodes(p)}


def unreadable(tree: ast.AST) -> list[str]:
    """What this rule cannot read, each refused (the run wall's L4, restored by the C5-P audit's item 5): importlib under
    any bound name but the `importlib.util.find_spec` chain, any reference to `__import__` or `import_module` -- a name,
    an attribute, a string a getattr would take."""
    out: list[str] = []
    parents = _parents(tree)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                if (a.name == 'importlib' or a.name.startswith('importlib.')) and (a.name, a.asname) != (
                        'importlib.util', None):
                    out.append(f'line {node.lineno}: {ast.unparse(node)}: importlib, which this rule cannot read')
        elif isinstance(node, ast.ImportFrom) and (node.module or '').split('.')[0] == 'importlib':
            out.append(f'line {node.lineno}: {ast.unparse(node)}: importlib, which this rule cannot read')
        elif isinstance(node, ast.Name) and node.id == 'importlib':
            up = parents.get(id(node))
            top = parents.get(id(up)) if up is not None else None
            if not (isinstance(up, ast.Attribute) and up.attr == 'util' and isinstance(top, ast.Attribute)
                    and top.attr == 'find_spec'):
                out.append(f'line {node.lineno}: importlib reached as {ast.unparse(top or up or node)!r}: only'
                           ' importlib.util.find_spec, which imports nothing')
        elif isinstance(node, ast.Name) and node.id in ('__import__', 'import_module'):
            out.append(f'line {node.lineno}: {node.id}, which this rule cannot read')
        elif isinstance(node, ast.Attribute) and node.attr in ('__import__', 'import_module'):
            out.append(f'line {node.lineno}: {ast.unparse(node)}, which this rule cannot read')
        elif isinstance(node, ast.Constant) and node.value in ('__import__', 'import_module'):
            out.append(f'line {node.lineno}: {node.value!r}, which this rule cannot read')
    return out


def imports(tree: ast.AST) -> tuple[set[str], set[str], set[str], bool]:
    """What a module imports of the package, wherever in it (a function's own import too): the modules, the names it
    takes from _planned and from _runframe, and whether it imports the package itself (`import legend_lite`, `import
    legend_lite as x`, `from legend_lite import *`: its __init__, which imports the compiler)."""
    modules: set[str] = set()
    planned: set[str] = set()
    frame: set[str] = set()
    package = False
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.level > 1:
                raise AssertionError(f'line {node.lineno}: an import beyond the package ({ast.unparse(node)})')
            own = node.level == 1 or node.module == 'legend_lite' or (node.module or '').startswith('legend_lite.')
            if not own:
                continue
            base = node.module if node.level == 1 else (node.module or '').removeprefix('legend_lite').lstrip('.')
            if not base:
                if any(a.name == '*' for a in node.names):
                    raise AssertionError(f'line {node.lineno}: a star of the package ({ast.unparse(node)})')
                modules.update(a.name for a in node.names)
                if '_runframe' in {a.name for a in node.names}:
                    frame.add('*module*')
            else:
                modules.add(base.split('.')[0])
                if base == '_planned':
                    planned.update(a.name for a in node.names)
                if base == '_runframe':
                    for a in node.names:
                        frame.add(a.name if a.asname in (None, a.name) else f'{a.name} as {a.asname}')
        elif isinstance(node, ast.Import):
            for a in node.names:
                if a.name == 'legend_lite':
                    package = True
                elif a.name.startswith('legend_lite.'):
                    modules.add(a.name.split('.')[1])
                    if a.asname is None:
                        package = True
                    if a.name.split('.')[1] == '_runframe':
                        frame.add('*module*')
    return modules, planned, frame, package


def misspelt_runs(tree: ast.AST) -> list[str]:
    """Each use of an opener that is not `@runs(...)` on a function or `with in_run(...)` / `with exempt(...)`."""
    out: list[str] = []
    parents = _parents(tree)
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Name) and node.id in OPENERS):
            continue
        call = parents.get(id(node))
        holder = parents.get(id(call)) if call is not None else None
        ok = isinstance(call, ast.Call) and call.func is node and (
            (node.id == 'runs' and isinstance(holder, (ast.FunctionDef, ast.AsyncFunctionDef))
             and call in holder.decorator_list)
            or (node.id != 'runs' and isinstance(holder, ast.withitem) and holder.context_expr is call))
        if not ok:
            shown = ast.unparse(holder if isinstance(holder, ast.AST) and not isinstance(
                holder, (ast.Module, ast.FunctionDef, ast.ClassDef)) else (call or node))
            out.append(f'line {node.lineno}: {shown[:60]!r}: a run is spelled `@runs(...)` on a function or `with'
                       f' {node.id}(...)` alone' if node.id != 'runs' else
                       f'line {node.lineno}: {shown[:60]!r}: a run is spelled `@runs(...)` on a function alone')
    return out


def _sends_in(node: ast.Call) -> str | None:
    """The send a call is, or None: the one send, the warehouse's HTTP, a sending helper, a sending method on any
    receiver."""
    f = node.func
    if isinstance(f, ast.Name) and (f.id in SEND_NAMES or f.id in SENDING_HELPERS):
        return f.id
    if isinstance(f, ast.Attribute) and f.attr in SEND_METHODS:
        return f'.{f.attr}'
    return None


def unframed_sends(tree: ast.AST) -> tuple[list[str], set[str]]:
    """Each send in a run module that no frame holds -- not in an `@runs` function, a `with in_run(...)` block, or a
    sending helper -- in words; and the sending helpers this module defines that send."""
    out: list[str] = []
    helpers: set[str] = set()

    def visit(node: ast.AST, function: str | None, framed: bool) -> None:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            marked = any(isinstance(d, ast.Call) and isinstance(d.func, ast.Name) and d.func.id == 'runs'
                         for d in node.decorator_list)
            for d in node.decorator_list:
                visit(d, function, framed)
            for child in node.body:
                visit(child, node.name, framed or marked)
            return
        if isinstance(node, ast.With):
            opened = any(isinstance(i.context_expr, ast.Call) and isinstance(i.context_expr.func, ast.Name)
                         and i.context_expr.func.id in ('in_run',) for i in node.items)
            for i in node.items:
                visit(i, function, framed)
            for child in node.body:
                visit(child, function, framed or opened)
            return
        if isinstance(node, ast.Call):
            sent = _sends_in(node)
            if sent is not None and not framed:
                if function in SENDING_HELPERS:
                    helpers.add(function)
                else:
                    out.append(f'line {node.lineno}: {sent} in {function or "the module"}, in no run: a send of the run'
                               ' side is in an `@runs` function, a `with in_run(...)` block, or a sending helper')
        for child in ast.iter_child_nodes(node):
            visit(child, function, framed)

    visit(tree, None, False)
    return out, helpers


def _annotation_names(annotation: ast.AST) -> list[ast.AST]:
    """An annotation's nodes, a quoted one parsed (``from __future__ import annotations`` writes them as text too)."""
    if isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):
        try:
            annotation = ast.parse(annotation.value, mode='eval').body
        except SyntaxError:
            return []
    return list(ast.walk(annotation))


def planner_signatures(tree: ast.AST) -> list[str]:
    """Each run function that takes or answers a planner (A5): a parameter or a return annotated with a planner's type,
    a planner module's member, or a callable answering a plan or a planner's text."""
    out: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        a = node.args
        annotated = [(p.arg, p.annotation) for p in [*a.posonlyargs, *a.args, *a.kwonlyargs,
                                                     *([a.vararg] if a.vararg else []), *([a.kwarg] if a.kwarg else [])]
                     if p.annotation is not None]
        if node.returns is not None:
            annotated.append(('its answer', node.returns))
        for what, annotation in annotated:
            nodes = _annotation_names(annotation)
            named = {n.id for n in nodes if isinstance(n, ast.Name)} | {n.attr for n in nodes if isinstance(n, ast.Attribute)}
            owners = {n.value.id for n in nodes if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name)}
            callables = [n for n in nodes if isinstance(n, ast.Subscript) and (
                (isinstance(n.value, ast.Name) and n.value.id == 'Callable')
                or (isinstance(n.value, ast.Attribute) and n.value.attr == 'Callable'))]
            answers = set()
            for c in callables:
                parts = c.slice.elts if isinstance(c.slice, ast.Tuple) else [c.slice]
                for n in ast.walk(parts[-1]):
                    if isinstance(n, ast.Name):
                        answers.add(n.id)
                    elif isinstance(n, ast.Attribute):
                        answers.add(n.attr)
            hit = sorted((named & PLANNER_TYPES) | {f'{o}.' for o in owners & PLANNER_MODULE_NAMES}
                         | {f'a callable answering {x}' for x in answers & PLANNED_ANSWERS})
            if hit:
                out.append(f'line {node.lineno}: {node.name} ({what}) {hit}: a run takes and answers no planner')
    return out


def broken(sources: dict[str, str]) -> list[str]:
    """Every break of the split, in words."""
    out: list[str] = []
    helpers: set[str] = set()
    for name, source in sorted(sources.items()):
        tree = ast.parse(source)
        out += [f'{name}: {r}' for r in unreadable(tree)]
        try:
            modules, planned, frame, package = imports(tree)
        except AssertionError as refused:
            out.append(f'{name}: {refused}')
            continue
        if name in RUN | SEAM and package:
            out.append(f'{name} imports the package itself, whose __init__ imports the compiler: a run or seam module'
                       ' imports its own modules by name')
        if name in RUN and not modules <= RUN | SEAM | LOADER:
            out.append(f'{name} (run) imports {sorted(modules - RUN - SEAM - LOADER)}: the run side imports the run side'
                       ' and the seam alone (the loader by its service key alone)')
        if modules & LOADER and name not in KEYS:
            out.append(f'{name} imports the native library\'s loader: its two keys alone do (_library, _services)')
        if name in LOADER and modules:
            out.append(f'{name} (the loader) imports {sorted(modules)}: it imports nothing of the package')
        if name in RUN | SEAM:
            for node in ast.walk(tree):
                if isinstance(node, ast.Attribute) and node.attr == 'modules' and isinstance(node.value, ast.Name) \
                        and node.value.id == 'sys':
                    out.append(f'{name}: line {node.lineno}: sys.modules, a late import this rule cannot read')
        if name in RUN:
            out += [f'{name}: {r}' for r in planner_signatures(tree)]
        if name in SEAM and not modules <= SEAM:
            out.append(f'{name} (seam) imports {sorted(modules - SEAM)}: the seam imports the seam alone')
        if name in PLANNER and modules & RUN:
            out.append(f'{name} (planner) imports {sorted(modules & RUN)}: the planner imports no run module')
        if name in PLANNER and planned & SENDING_NAMES:
            out.append(f'{name} (planner) takes {sorted(planned & SENDING_NAMES)} from _planned: the planner sends'
                       ' nothing')
        if name not in RUN and 'send' in planned:
            out.append(f'{name} imports the one send: only the run side sends (each door hands it its statements)')
        if name not in RUN | {'_runframe'} and frame & (OPENERS | {'*module*'}):
            out.append(f'{name} imports {sorted(frame & (OPENERS | {"*module*"}))} of _runframe: a run is opened on the'
                       ' run side alone, so no door opens one in which it could plan')
        renamed = sorted(f for f in frame if ' as ' in f and f.split(' as ')[0] in OPENERS)
        if renamed:
            out.append(f'{name} imports {renamed}: an opener is imported by its own name')
        if name in RUN | {'_runframe'}:
            out += [f'{name}: {r}' for r in misspelt_runs(tree)]
        if name not in RUN:
            for node in ast.walk(tree):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in PLANE_SENDS:
                    out.append(f'{name}: line {node.lineno}: {ast.unparse(node.func)}: a plane\'s send is the run'
                               ' side\'s (_reads.read_plane, run_on_plane; _runs\'s stream)')
                if name not in SEAM and isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and \
                        node.func.attr in TRANSACTION_METHODS:
                    out.append(f'{name}: line {node.lineno}: {ast.unparse(node.func)}: a transaction\'s bound is a run'
                               ' act, the run side\'s alone')
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in SENDING_HELPERS:
                    out.append(f'{name}: line {node.lineno}: {node.func.id}(...): a sending helper is called by the run'
                               ' side alone, in a run')
        if name in RUN:
            unframed, found = unframed_sends(tree)
            out += [f'{name}: {u}' for u in unframed]
            helpers |= found
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
        self.assertTrue(RUN and SEAM and PLANNER and RUN | SEAM | PLANNER <= set(sources),
                        'every module the BUILD names is the package\'s')
        self.assertTrue(LOADER and LOADER <= set(sources))
        libraries = [RUN, SEAM, PLANNER, LOADER]
        self.assertEqual(sum(map(len, libraries)), len(set().union(*libraries)), 'each module in one library')
        self.assertEqual({'_library'}, set(KEYS) & PLANNER)
        self.assertEqual({'_services'}, set(KEYS) & RUN)
        self.assertEqual([], broken(sources))
        # each sending helper named sends, in a run module (no stale entry)
        found = set()
        for name in RUN:
            found |= unframed_sends(ast.parse(sources[name]))[1]
        self.assertEqual(SENDING_HELPERS, found)
        # the run side's closure: the planning key never, the loader through the service key alone (A1)
        graph = {name: imports(ast.parse(text))[0] & set(sources) for name, text in sources.items()}
        self.assertTrue(graph['_services'] <= LOADER | SEAM, graph['_services'])
        for start in sorted(RUN - {'_services'}):
            seen, todo = {start}, [start]
            while todo:
                for nxt in graph[todo.pop()] - seen - {'_services'}:
                    seen.add(nxt)
                    todo.append(nxt)
            self.assertFalse(seen & ({'_library'} | LOADER), f'{start} reaches {sorted(seen & ({"_library"} | LOADER))}')

    def test_the_rule_sees_every_break(self):
        # each a violator the rule must name, red on revert of its own check
        run, door, planner = sorted(RUN)[0], 'engine', 'compiler'
        cases = {
            # the build's own rule, read: a run or seam module imports its own side alone
            (run, 'from . import compiler'): 'the run side imports',
            (run, 'def f():\n    from .frames import Frames'): 'the run side imports',
            (run, 'import legend_lite.engine'): 'the run side imports',
            (run, 'from legend_lite import _library'): 'the run side imports',
            (run, 'from legend_lite.compiler import plan'): 'the run side imports',
            ('_planned', 'from . import _reads'): 'the seam imports',
            # the package itself, whose __init__ imports the compiler (the audit's item 1)
            (run, 'import legend_lite'): 'imports the package itself',
            (run, 'import legend_lite as ll\nll.compiler.plan'): 'imports the package itself',
            ('_plans', 'def f():\n    import legend_lite'): 'imports the package itself',
            # every module of :compiler, by the BUILD's list (the audit's item 3)
            (planner, 'from ._runs import run_load'): 'the planner imports no run module',
            ('_home', 'from ._reads import run_plan'): 'the planner imports no run module',
            ('timeseries', 'def f():\n    from . import _runs'): 'the planner imports no run module',
            (planner, 'from ._planned import PlannedSql, send'): 'the planner sends nothing',
            ('_library', 'from ._planned import connect'): 'the planner sends nothing',
            ('_pool', 'from ._planned import Database'): 'the planner sends nothing',
            # the native library by its two keys (A1)
            ('warehouse', 'from ._library import library'): 'the run side imports',
            ('_reads', 'from ._loader import loaded'): "the native library's loader: its two keys alone",
            ('compiler', 'from ._loader import loaded'): "the native library's loader: its two keys alone",
            ('_loader', 'from ._runframe import planning'): 'it imports nothing of the package',
            ('_reads', 'import sys\nsys.modules["legend_lite.compiler"]'): 'sys.modules',
            # a run takes and answers no planner (A5), quoted or not
            ('_runs', 'def f(held: HeldModel): pass'): 'a run takes and answers no planner',
            ('_runs', 'def f(lib: "Library | None" = None): pass'): 'a run takes and answers no planner',
            ('_reads', 'def f(make: Callable[[str], PlannedSql]): pass'): 'a run takes and answers no planner',
            ('_reads', 'def f(make: "Callable[..., PlanText]"): pass'): 'a run takes and answers no planner',
            ('_runs', 'def f() -> compiler.HeldModel: pass'): 'a run takes and answers no planner',
            ('_runs', 'def f(*, p: Optional[Callable[[], Plan]]): pass'): 'a run takes and answers no planner',
            # a transaction's bounds the run side's (A3)
            ('frames', 'cursor.rollback()'): "a transaction's bound is a run act",
            ('_reads', 'def f(cursor):\n    cursor.commit()'): 'in no run',
            ('frames', 'from ._planned import send'): 'only the run side sends',
            (door, 'def f():\n    from legend_lite._planned import send'): 'only the run side sends',
            # a plane's send and a sending helper, the run side's alone
            (door, 'rows = table.warehouse.arrow_chunks(plan.sql)'): "a plane's send is the run side's",
            ('frames', 'execute_redacted(con, sql, ())'): 'a sending helper is called by the run side alone',
            # what this cannot read (the audit's item 5)
            (run, 'import importlib'): 'importlib',
            (run, 'from importlib import import_module'): 'importlib',
            (run, 'import importlib.util as u'): 'importlib',
            (door, 'import importlib.util\nimportlib.import_module("legend_lite.compiler")'): 'importlib',
            (door, 'import importlib.util\nm = importlib\nm.util.find_spec("x")'): 'importlib',
            (door, 'import importlib.util\ngetattr(importlib, "util")'): 'importlib',
            (door, '__import__("legend_lite.compiler")'): '__import__',
            (door, 'f = __import__\nf("legend_lite.compiler")'): '__import__',
            (door, 'import builtins\nbuiltins.__import__("legend_lite.compiler")'): '__import__',
            (door, 'getattr(builtins, "__import__")("x")'): '__import__',
            (run, 'from . import *'): 'a star of the package',
            (run, 'from .. import x'): 'beyond the package',
            # a run opened on the run side alone, spelled one way (the audit's item 6)
            (door, 'from ._runframe import in_run'): 'a run is opened on the run side alone',
            (door, 'from ._runframe import runs\n@runs("x")\ndef f(): pass'): 'a run is opened on the run side alone',
            (door, 'from . import _runframe\nwith _runframe.in_run("x"): pass'): 'a run is opened on the run side alone',
            ('_runs', 'from ._runframe import in_run as opened'): 'an opener is imported by its own name',
            ('_runs', 'from ._runframe import runs\ndef f(): pass\nf = runs("x")(f)'): 'a run is spelled',
            ('_runs', 'from ._runframe import runs\nframed = runs("x")'): 'a run is spelled',
            ('_reads', 'from ._runframe import in_run\nExitStack().enter_context(in_run("x"))'): 'a run is spelled',
            # every send of the run side framed (the audit's item 4)
            ('_runs', 'def f(db, sql):\n    send(db, sql)'): 'in no run',
            ('_reads', 'def f(read, sql):\n    return read.execute(sql)'): 'in no run',
            ('_reads', 'def f(plane, sql):\n    return plane.arrow_chunks(sql)'): 'in no run',
            ('_runs', 'def f(db, sql):\n    execute_redacted(db, sql, ())'): 'in no run',
            ('_runs', 'def f(db, r):\n    db.import_stream(r, None, "t")'): 'in no run',
            ('_runs', 'def f(db, sql):\n    with other(): send(db, sql)'): 'in no run',
        }
        for (name, source), said in cases.items():
            with self.subTest(module=name, source=source):
                found = broken({name: source})
                self.assertTrue(found and any(said in f for f in found), found)
        # and the forms that keep it
        self.assertEqual([], broken({
            '_runs': 'from ._planned import send\nfrom . import _plans\nfrom ._runframe import in_run, runs\n'
                     '@runs("x")\ndef f(db, sql):\n    send(db, sql)\n'
                     'def g(db, sql):\n    with in_run("y"):\n        send(db, sql)\n'
                     'def _stream(db, sql):\n    send(db, sql)\n'
                     '@runs("z")\ndef h(db, sql):\n    _stream(db, sql)',
            'frames': 'from ._reads import run_plan\nfrom . import compiler',
            'compiler': 'from ._planned import PlannedSql, from_plan\nfrom ._plans import Plan',
            'warehouse': 'from ._planned import PlannedSql, Unframed, require\nfrom ._runframe import current_run\n'
                         'import importlib.util\nimportlib.util.find_spec("duckdb")'}))

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
        _runs.run_statements(db, [from_plan('CREATE TABLE t AS SELECT 1 AS n')], 'a test')
        self.assertEqual([(1,)], _runs.ask(db, from_plan('SELECT count(*) FROM t'), 'a test'))
        self.assertEqual(1, _reads.count_of(db, Plan(from_plan('SELECT count(*) FROM t'), ())))
        self.assertEqual(1, _reads.run_arrow(db, one, ()).num_rows)
        self.assertEqual('[{"n":1}]', _reads.run_rows_json(db, one, ()))
        typed = _reads.run_plan(db, Plan(one, (Column('n', 'Integer', {}),)), ())
        self.assertEqual([1], typed.column('n').to_pylist())
        self.assertIsNone(current_run())


if __name__ == '__main__':
    unittest.main()
