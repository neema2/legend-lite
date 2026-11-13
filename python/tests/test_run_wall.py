"""THE RUN WALL, Python's half (the user's ruling M2, 2026-11-13: "a compile-time wall on both hosts"; its TypeScript half
is datacube/test/run-wall.test.ts). Read by Python's own parser, over every module of the package. At run time the run
frame (_runframe.py) refuses a planner entry asked inside a run; this holds, before anything runs, what can be stated:

- THE SEND SEAM IMPORTS NO PLANNER: `_planned` (the one send, the plan readers, the type) and `_runframe` import, through
  the package's own modules, no module of the planner's (PLANNER_MODULES).
- A RUN CALLS NO PLANNER: each run -- a function marked `@runs(...)`, the body of a `with in_run(...)` -- and every
  function of its module it reaches by name (a module function, a method of its class through `self`) calls no planner
  entry: no `library()`, no `_planner`, and of `compiler` only the readers COMPILER_READERS names (types, errors and the
  plan's own data, each held here to call no library itself).
- A RUN TAKES NO PLANNER: no run's parameter is annotated with a planner's type (a held model, the library) or with a
  callable that answers a PlannedSql (a planner handed in under another name).
- THE RUNS ARE PINNED by module and name (RUNS): a run unmarked, or a new one, is seen.

Python modules plan and run side by side (frames.py plans a table's query and runs a read), so the wall is drawn around
the runs, not around modules; Python's modules are not split by this guard (a product change, not a test's).

ITS LIMIT, said plainly: Python is dynamic. A planner reached through a value handed in at run time -- an untyped
closure, a callable from a dict, an object whose method plans -- is not seen here; a call through a name this module
does not define (another module's function a run calls) is followed only as far as the readers' check. The run frame is
the backstop at run time. Each list moves only with a dated justification naming the task, and shrinks."""

import ast
import os
import unittest
from pathlib import Path

from python.runfiles import runfiles

FILES = runfiles.Create()

# The planner's modules: the native library and its protocol, the compiler's bindings, and the doors that plan (the
# engine's routes, DataCube's host, the page, the notebook, the model home). 2026-11-13 (M2).
PLANNER_MODULES = frozenset({'_library', '_planner', 'compiler', '_home', '_headless', 'engine', 'datacube', 'page',
                             'notebook'})

# The run side's own modules: the one send and the frame. 2026-11-13 (M2).
RUN_MODULES = frozenset({'_planned', '_runframe'})

# What a run may take from compiler: its types, its error, and the plan's own data read with no library call (each held
# below to call none). 2026-11-13 (M2).
COMPILER_READERS = frozenset({'Plan', 'Column', 'LegendError', 'plan_params', 'statement_params'})

# The runs, by module: each `@runs` function (by its qualified name) and each `with in_run(...)` block (by its function
# and its run's name). 2026-11-13 (M2): Reading.run and Frames.run (frames.py), the three load runners (projects.py),
# a host's operations and question (_host.py).
RUNS = {
    'frames.py': {'Reading.run', 'Frames.run'},
    'projects.py': {'run_load', 'run_write', 'run_host_load'},
    '_host.py': {"run: with in_run(\"a host's operations\")", "ask: with in_run(\"a host's question\")"},
}


def package_sources() -> dict[str, str]:
    """Every module of the package by file name, as shipped."""
    package = Path(FILES.Rlocation(os.environ['PLANNED_PACKAGE'])).parent
    return {p.name: p.read_text(encoding='utf-8') for p in sorted(package.glob('*.py'))}


def imports_of(tree: ast.Module) -> set[str]:
    """The package's own modules a module imports, `import type`-free Python: `from .x import ...`, `from . import x`,
    `from legend_lite.x import ...`, `import legend_lite.x`."""
    out: set[str] = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom):
            if n.level >= 1:
                if n.module:
                    out.add(n.module.split('.')[0])
                else:
                    out.update(a.name for a in n.names)
            elif n.module and n.module.startswith('legend_lite.'):
                out.add(n.module.split('.')[1])
        elif isinstance(n, ast.Import):
            out.update(a.name.split('.')[1] for a in n.names if a.name.startswith('legend_lite.'))
    return out


def planner_reached(sources: dict[str, str], start: str) -> list[str]:
    """The path from `start` to a planner module through the package's imports, or [] when there is none."""
    trees = {name[:-3]: ast.parse(text) for name, text in sources.items()}
    seen = {start}
    stack = [[start]]
    while stack:
        path = stack.pop()
        for m in sorted(imports_of(trees[path[-1]]) if path[-1] in trees else ()):
            if m in PLANNER_MODULES:
                return path + [m]
            if m not in seen:
                seen.add(m)
                stack.append(path + [m])
    return []


def _decorated_run(fn: ast.AST) -> bool:
    return isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)) and any(
        isinstance(d, ast.Call) and isinstance(d.func, ast.Name) and d.func.id == 'runs' for d in fn.decorator_list)


def runs_of(tree: ast.Module) -> dict[str, list[ast.AST]]:
    """A module's runs: each `@runs` function by its qualified name (its body), each `with in_run(...)` block by its
    function and its run's name (the block's body)."""
    out: dict[str, list[ast.AST]] = {}

    def visit(node: ast.AST, owner: str) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.ClassDef):
                visit(child, child.name + '.')
            elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                name = owner + child.name
                if _decorated_run(child):
                    out[name] = child.body
                for w in ast.walk(child):
                    if isinstance(w, (ast.With, ast.AsyncWith)):
                        for item in w.items:
                            call = item.context_expr
                            if isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id == 'in_run':
                                label = ast.unparse(call.args[0]) if call.args else ''
                                out[f'{child.name}: with in_run({label})'] = w.body
                visit(child, name + '.')

    visit(tree, '')
    return out


def _definitions(tree: ast.Module) -> dict[str, ast.AST]:
    """A module's functions by name, and each class's methods as `Class.method`."""
    out: dict[str, ast.AST] = {}
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
            out[n.name] = n
        elif isinstance(n, ast.ClassDef):
            for m in n.body:
                if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    out[f'{n.name}.{m.name}'] = m
    return out


def _planner_names(tree: ast.Module) -> dict[str, str]:
    """The local names a module binds to a planner module or one of its names: `compiler` (from `from . import
    compiler`), and `x` from `from .compiler import x` -- each to `<module>` or `<module>.<name>`."""
    out: dict[str, str] = {}
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.level >= 1:
            if n.module is None:
                for a in n.names:
                    if a.name in PLANNER_MODULES:
                        out[a.asname or a.name] = a.name
            elif n.module.split('.')[0] in PLANNER_MODULES:
                for a in n.names:
                    out[a.asname or a.name] = f'{n.module}.{a.name}'
    return out


def run_crossings(source: str) -> list[str]:
    """Each planner call a run of `source` reaches -- in its body, or in a function of its module it calls by name, or
    through `self` -- named by its run and where it is."""
    tree = ast.parse(source)
    defs = _definitions(tree)
    planner = _planner_names(tree)
    out: list[str] = []
    for run, body in runs_of(tree).items():
        cls = run.split('.')[0] + '.' if '.' in run.split(':')[0] else ''
        seen: set[str] = set()
        work: list[tuple[str, list[ast.AST]]] = [(run, body)]
        while work:
            where, nodes = work.pop()
            for stmt in nodes:
                for n in ast.walk(stmt):
                    if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name):
                        bound = planner.get(n.value.id)
                        if bound is not None and not (bound == 'compiler' and n.attr in COMPILER_READERS):
                            out.append(f'{run}: {n.value.id}.{n.attr} (a planner\'s, in {where}, line {n.lineno})')
                    elif isinstance(n, ast.Name) and n.id in planner and isinstance(n.ctx, ast.Load):
                        bound = planner[n.id]
                        if bound.split('.')[0] != 'compiler' or bound.split('.')[-1] not in COMPILER_READERS:
                            if bound != 'compiler':
                                out.append(f'{run}: {n.id} (the planner\'s {bound}, in {where}, line {n.lineno})')
                    if isinstance(n, ast.Call):
                        f = n.func
                        name = None
                        if isinstance(f, ast.Name):
                            name = f.id if f.id in defs else None
                            if f.id == 'library':
                                out.append(f'{run}: library() (in {where}, line {n.lineno})')
                        elif isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name) and f.value.id == 'self':
                            name = cls + f.attr if (cls + f.attr) in defs else None
                        if name is not None and name not in seen:
                            seen.add(name)
                            work.append((name, defs[name].body))
    return sorted(set(out))


def _annotation_crossing(annotation: ast.AST | None) -> str | None:
    if annotation is None:
        return None
    text = ast.unparse(annotation)
    for planner_type in ('HeldModel', 'Library', 'PlannerModule'):
        if planner_type in text:
            return f'a {planner_type}'
    if isinstance(annotation, ast.Subscript) and ast.unparse(annotation.value).endswith('Callable'):
        returns = annotation.slice.elts[-1] if isinstance(annotation.slice, ast.Tuple) else None
        if returns is not None and 'PlannedSql' in ast.unparse(returns):
            return 'a callable that answers a PlannedSql'
    return None


def signature_crossings(source: str) -> list[str]:
    """Each run's parameter annotated with a planner's type, or a callable answering a PlannedSql."""
    tree = ast.parse(source)
    out: list[str] = []
    for owner in [tree] + [c for c in tree.body if isinstance(c, ast.ClassDef)]:
        for fn in owner.body:
            if _decorated_run(fn):
                args = fn.args
                for a in args.posonlyargs + args.args + args.kwonlyargs + [x for x in (args.vararg, args.kwarg) if x]:
                    why = _annotation_crossing(a.annotation)
                    if why is not None:
                        out.append(f'{fn.name}({a.arg}): {why}')
    return out


class RunWallTest(unittest.TestCase):

    def setUp(self) -> None:
        self.sources = package_sources()

    def test_the_send_seam_imports_no_planner(self):
        for module in sorted(RUN_MODULES):
            self.assertIn(module + '.py', self.sources)
            self.assertEqual(planner_reached(self.sources, module), [], f'{module} reaches the planner')

    def test_no_run_calls_a_planner(self):
        found = {name: run_crossings(text) for name, text in self.sources.items()}
        self.assertEqual({k: v for k, v in found.items() if v}, {})

    def test_no_run_takes_a_planner(self):
        found = {name: signature_crossings(text) for name, text in self.sources.items()}
        self.assertEqual({k: v for k, v in found.items() if v}, {})

    def test_the_runs_are_pinned(self):
        found = {name: set(runs_of(ast.parse(text))) for name, text in self.sources.items()}
        self.assertEqual({k: v for k, v in found.items() if v}, RUNS)

    def test_the_readers_a_run_may_take_from_compiler_call_no_library(self):
        tree = ast.parse(self.sources['compiler.py'])
        defs = {n.name: n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}
        for reader in sorted(COMPILER_READERS):
            self.assertIn(reader, defs, f'compiler.{reader} is not defined there')
            calls = {ast.unparse(n.func) for n in ast.walk(defs[reader]) if isinstance(n, ast.Call)}
            self.assertFalse({c for c in calls if c == 'library' or c.startswith(('library(', '_planner', 'library().'))},
                             f'compiler.{reader} calls the library: {sorted(calls)}')

    # ---- the guard held to planted violators

    def test_a_planted_run_that_plans_is_named(self):
        planted = '''
from . import compiler
from ._runframe import runs
from ._library import library

def helper(m):
    return compiler.plan(m, {}, 'r')

class Reader:
    def ask(self):
        return library().call('lite_plan_json_held', 'h', '{}', 'r')

    @runs('a planted read')
    def run(self, plan: compiler.Plan):
        compiler.plan_params(plan)
        helper('m')
        return self.ask()

@runs('a planted load')
def load(held: compiler.HeldModel, make: 'Callable[[str], PlannedSql]') -> None:
    compiler.session_setup('DuckDB')
'''
        found = run_crossings(planted)
        self.assertTrue(any('Reader.run: compiler.plan (' in f and 'in helper' in f for f in found), found)
        self.assertTrue(any('Reader.run: library() (in Reader.ask' in f for f in found), found)
        self.assertTrue(any('load: compiler.session_setup' in f for f in found), found)
        self.assertFalse(any('plan_params' in f or 'compiler.Plan' in f for f in found), found)
        signatures = signature_crossings(planted.replace("'Callable[[str], PlannedSql]'", 'Callable[[str], PlannedSql]'))
        self.assertEqual(signatures, ['load(held): a HeldModel', 'load(make): a callable that answers a PlannedSql'])

    def test_a_planted_in_run_block_that_plans_is_named(self):
        planted = '''
from . import compiler
from ._runframe import in_run

def run(connection, op):
    planned = compiler.host_operations('DuckDB', [op])
    with in_run('a planted run'):
        compiler.host_operations('DuckDB', [op])
'''
        self.assertEqual([f.split(' (')[0] for f in run_crossings(planted)],
                         ["run: with in_run('a planted run'): compiler.host_operations"])

    def test_a_planted_seam_that_imports_the_planner_is_named(self):
        sources = dict(self.sources)
        sources['_planned.py'] += '\nfrom ._helper import x\n'
        sources['_helper.py'] = 'from . import compiler\nx = 1\n'
        self.assertEqual(planner_reached(sources, '_planned'), ['_planned', '_helper', 'compiler'])


if __name__ == '__main__':
    unittest.main()
