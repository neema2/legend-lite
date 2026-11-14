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
  callable that answers a PlannedSql or a PlanText (a planner handed in under another name) -- in a union (`X | None`,
  `Optional[...]`), in a quoted annotation, or through a module's type alias.
- THE RUNS ARE PINNED by module and name (RUNS): a run unmarked, or a new one, is seen.
- EVERY SEND IS IN A RUN, OR NAMED (the follow-up audit's G2, 2026-11-14: until then only the marked runs were read):
  each call of the one send (`_planned.send`) is a send site, and so is each call of a SENDING HELPER -- a function that
  sends a statement or a plan it is handed (a parameter annotated PlannedSql or PlanText), outside a run of its own.
  A send site inside a run is framed; every other is an UNFRAMED SENDER, named by module and name with its reason
  (UNFRAMED_SENDERS): shrink-only, each C5's to frame.

Each name is read through what the module imported it as: a relative import (`from . import compiler`, `from .compiler
import plan`), an absolute one (`from legend_lite import compiler`, `from legend_lite.compiler import plan`, `import
legend_lite.compiler as c`), an alias (`from ._runframe import runs as marks`) or a module's attribute
(`@_runframe.runs(...)`, `with _runframe.in_run(...)`).

Python modules plan and run side by side (frames.py plans a table's query and runs a read), so the wall is drawn around
the runs and the sends, not around modules; Python's modules are not split by this guard (a product change, not a
test's).

ITS LIMIT, said plainly: Python is dynamic, and this reads names, not values. A planner reached through a value handed in
at run time -- an untyped closure, a callable from a dict, an object whose method plans, a name rebound after its import
-- is not seen; a call through a name this module does not define is followed only as far as the readers' check; a
sending helper is seen by its parameter's annotation, so one that takes its statement untyped is a send site its callers
do not inherit; a method called on anything but `self` is not resolved (a read's `over.execute(...)` is the read's,
seen at its own send). The run frame is the backstop at run time. Each list moves only with a dated justification naming
the task, and shrinks."""

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

# A planner's types, by name: a held model, the library, the planner's module. 2026-11-13 (M2).
PLANNER_TYPES = ('HeldModel', 'Library', 'PlannerModule')

# The planner's answers: a statement and a plan (_planned.py), each made only where a planner's answer is read.
PLANNED = ('PlannedSql', 'PlanText')

# The runs, by module: each `@runs` function (by its qualified name) and each `with in_run(...)` block (by its function
# and its run's name). 2026-11-13 (M2): Reading.run and Frames.run (frames.py), the three load runners (projects.py),
# a host's operations and question (_host.py).
RUNS = {
    'frames.py': {'Reading.run', 'Frames.run'},
    'projects.py': {'run_load', 'run_write', 'run_host_load'},
    '_host.py': {"run: with in_run(\"a host's operations\")", "ask: with in_run(\"a host's question\")"},
}

# The sending helpers: each sends a statement or a plan it is handed, outside a run of its own, so its callers' calls
# are send sites. 2026-11-14 (the follow-up audit's G2, measured).
SENDING_HELPERS = {'frames.execute_redacted', 'frames.Reading.execute'}

# THE UNFRAMED SENDERS: each function that sends -- the one send, or a sending helper -- outside a run, by module and
# name, with its reason. Shrink-only: each C5's to frame (its send in a run) or to split (its planning out of it).
# 2026-11-14 (the follow-up audit's G2, measured; notebook._EngineWidget._answer's `self.send` is the widget's message
# to its page, not the database's send, and is not one).
UNFRAMED_SENDERS = {
    '_headless.Headless._query': "the hosted DataCube's query, sent across the bridge outside a run (C5)",
    '_headless.Headless._statement': "the hosted DataCube's statement, sent across the bridge outside a run (C5)",
    'engine.Engine._execute': 'pure/v1 execute: plans, then sends the plan through execute_redacted (C5)',
    'engine.Engine._execute_json': "the plan's JSON execute: plans, then sends through execute_redacted (C5)",
    'engine.Engine._project_plan_and_run': "a project's plan and run: plans, then sends on the project's database (C5)",
    'engine.Engine._served_snap': "a snapshot's count: plans, then sends on the frames' connection (C5)",
    'frames.Table._load': "a frame's load: the catalog's question planned and sent (C5)",
    'frames.Frames.__init__': "the frames' setup: the planner's session setup sent on the connection (C5)",
    'frames.Frames.streaming_file': "a streamed file's session: the session setup sent on its cursor (C5)",
    'frames.Frames.reading': "a read's session: its setup and the kept files' settings sent on its cursor (C5)",
    'projects._stream': "a project's stream: its statements sent as the plan is read (C5)",
    'projects.ProjectDatabases._make': "a project's database: its statements and its load's sends (C5)",
}


def package_sources() -> dict[str, str]:
    """Every module of the package by file name, as shipped."""
    package = Path(FILES.Rlocation(os.environ['PLANNED_PACKAGE'])).parent
    return {p.name: p.read_text(encoding='utf-8') for p in sorted(package.glob('*.py'))}


def bindings(tree: ast.Module) -> dict[str, str]:
    """What each name a module imports means, as `<module>` or `<module>.<name>` of the package: `from . import x`,
    `from .x import a`, `from legend_lite import x`, `from legend_lite.x import a`, `import legend_lite.x as y` -- each
    under its alias -- and `import legend_lite.x` (the package's own name, read through its attributes: '')."""
    out: dict[str, str] = {}
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom):
            if n.level >= 1:
                base = n.module.split('.')[0] if n.module else None
            elif n.module == 'legend_lite':
                base = None
            elif n.module and n.module.startswith('legend_lite.'):
                base = n.module.split('.')[1]
            else:
                continue
            for a in n.names:
                out[a.asname or a.name] = a.name if base is None else f'{base}.{a.name}'
        elif isinstance(n, ast.Import):
            for a in n.names:
                if a.name.startswith('legend_lite.'):
                    if a.asname:
                        out[a.asname] = a.name.split('.')[1]
                    else:
                        out['legend_lite'] = ''
                elif a.name == 'legend_lite':
                    out[a.asname or 'legend_lite'] = ''
    return out


def resolve(node: ast.AST, bound: dict[str, str]) -> str | None:
    """What a name or an attribute chain names in the package (`compiler.plan`, `_runframe.runs`), or None."""
    if isinstance(node, ast.Name):
        return bound.get(node.id)
    if isinstance(node, ast.Attribute):
        base = resolve(node.value, bound)
        if base is None:
            return None
        return node.attr if base == '' else f'{base}.{node.attr}'
    return None


def imports_of(tree: ast.Module) -> set[str]:
    """The package's own modules a module imports: each binding's module."""
    out: set[str] = set()
    for target in bindings(tree).values():
        if target:
            out.add(target.split('.')[0])
    for n in ast.walk(tree):
        # `from . import x` binds x, itself a module; `import legend_lite.x` reaches x without binding it
        if isinstance(n, ast.Import):
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


def _is_runs(decorator: ast.AST, bound: dict[str, str]) -> bool:
    """Whether a decorator marks a run: `@runs(...)` however imported or aliased, `@_runframe.runs(...)`."""
    return isinstance(decorator, ast.Call) and resolve(decorator.func, bound) == '_runframe.runs'


def _is_in_run(item: ast.withitem, bound: dict[str, str]) -> bool:
    call = item.context_expr
    return isinstance(call, ast.Call) and resolve(call.func, bound) == '_runframe.in_run'


def _decorated_run(fn: ast.AST, bound: dict[str, str]) -> bool:
    return isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)) and any(
        _is_runs(d, bound) for d in fn.decorator_list)


def runs_of(tree: ast.Module) -> dict[str, list[ast.AST]]:
    """A module's runs: each `@runs` function by its qualified name (its body), each `with in_run(...)` block by its
    function and its run's name (the block's body)."""
    bound = bindings(tree)
    out: dict[str, list[ast.AST]] = {}

    def visit(node: ast.AST, owner: str) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.ClassDef):
                visit(child, child.name + '.')
            elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                name = owner + child.name
                if _decorated_run(child, bound):
                    out[name] = child.body
                for w in ast.walk(child):
                    if isinstance(w, (ast.With, ast.AsyncWith)):
                        for item in w.items:
                            if _is_in_run(item, bound):
                                call = item.context_expr
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


def _planner_use(node: ast.AST, bound: dict[str, str]) -> str | None:
    """The planner entry a load names (`compiler.plan`, `library`, `_planner.call`), but a compiler reader; or None."""
    if not isinstance(node, (ast.Name, ast.Attribute)) or not isinstance(getattr(node, 'ctx', None), ast.Load):
        return None
    target = resolve(node, bound)
    if target is None or target.split('.')[0] not in PLANNER_MODULES:
        return None
    if target == 'compiler' or (target.startswith('compiler.') and target.split('.')[1] in COMPILER_READERS):
        return None
    return target


def run_crossings(source: str) -> list[str]:
    """Each planner call a run of `source` reaches -- in its body, or in a function of its module it calls by name, or
    through `self` -- named by its run and where it is."""
    tree = ast.parse(source)
    bound = bindings(tree)
    defs = _definitions(tree)
    out: list[str] = []
    for run, body in runs_of(tree).items():
        cls = run.split('.')[0] + '.' if '.' in run.split(':')[0] else ''
        seen: set[str] = set()
        work: list[tuple[str, list[ast.AST]]] = [(run, body)]
        while work:
            where, nodes = work.pop()
            for stmt in nodes:
                # an attribute chain is named once, at its outermost load (`compiler.plan`, not `compiler` too)
                bases = {id(a.value) for a in ast.walk(stmt) if isinstance(a, ast.Attribute)}
                for n in ast.walk(stmt):
                    used = _planner_use(n, bound)
                    if used is not None and id(n) not in bases:
                        shown = ast.unparse(n)
                        out.append(f"{run}: {shown}{'()' if used.endswith('library') else ''} (the planner's {used},"
                                   f' in {where}, line {n.lineno})')
                    if isinstance(n, ast.Call):
                        f = n.func
                        name = None
                        if isinstance(f, ast.Name):
                            name = f.id if f.id in defs else None
                        elif isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name) and f.value.id == 'self':
                            name = cls + f.attr if (cls + f.attr) in defs else None
                        if name is not None and name not in seen:
                            seen.add(name)
                            work.append((name, defs[name].body))
    return sorted(set(out))


def _parsed(annotation: ast.AST) -> ast.AST:
    """An annotation with each quoted part parsed (`'Callable[[str], PlannedSql]'`, `Optional['HeldModel']`)."""
    if isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):
        try:
            return _parsed(ast.parse(annotation.value, mode='eval').body)
        except SyntaxError:
            return annotation
    for field, value in ast.iter_fields(annotation):
        if isinstance(value, ast.AST):
            setattr(annotation, field, _parsed(value))
        elif isinstance(value, list):
            setattr(annotation, field, [_parsed(v) if isinstance(v, ast.AST) else v for v in value])
    return annotation


def _answers_planned(annotation: ast.AST) -> bool:
    """Whether a callable anywhere in `annotation` answers a statement or a plan (`Callable[..., PlannedSql]`, in a
    union, an Optional, a list)."""
    for n in ast.walk(annotation):
        if isinstance(n, ast.Subscript) and ast.unparse(n.value).split('.')[-1] == 'Callable':
            returns = n.slice.elts[-1] if isinstance(n.slice, ast.Tuple) and n.slice.elts else None
            if returns is not None and any(p in ast.unparse(returns) for p in PLANNED):
                return True
    return False


def planner_aliases(tree: ast.Module) -> set[str]:
    """A module's type aliases that are a planner's: `X = Callable[..., PlannedSql]`, `X: TypeAlias = ...`, `type X =
    ...` whose value answers a statement or a plan, or names a planner's type (or another such alias)."""
    declared: dict[str, ast.AST] = {}
    for n in tree.body:
        if isinstance(n, ast.Assign) and len(n.targets) == 1 and isinstance(n.targets[0], ast.Name):
            declared[n.targets[0].id] = n.value
        elif isinstance(n, ast.AnnAssign) and isinstance(n.target, ast.Name) and n.value is not None:
            declared[n.target.id] = n.value
        elif hasattr(ast, 'TypeAlias') and isinstance(n, ast.TypeAlias):
            declared[n.name.id] = n.value
    out: set[str] = set()
    grew = True
    while grew:
        grew = False
        for name, value in declared.items():
            if name not in out and _annotation_crossing(value, out) is not None:
                out.add(name)
                grew = True
    return out


def _annotation_crossing(annotation: ast.AST | None, aliases: set[str] = frozenset()) -> str | None:
    if annotation is None:
        return None
    parsed = _parsed(annotation)
    if isinstance(parsed, ast.Constant):
        # a string that is no type (a module's text constant): nothing a run could be annotated with
        return None
    for n in ast.walk(parsed):
        named = n.id if isinstance(n, ast.Name) else n.attr if isinstance(n, ast.Attribute) else None
        if named in PLANNER_TYPES:
            return f'a {named}'
    for n in ast.walk(parsed):
        if isinstance(n, ast.Name) and n.id in aliases:
            return f'a {n.id}, a planner\'s alias'
    if _answers_planned(parsed):
        return 'a callable that answers a PlannedSql or a PlanText'
    return None


def signature_crossings(source: str) -> list[str]:
    """Each run's parameter annotated with a planner's type, an alias of one, or a callable answering a statement or a
    plan."""
    tree = ast.parse(source)
    bound = bindings(tree)
    aliases = planner_aliases(tree)
    out: list[str] = []
    for owner in [tree] + [c for c in tree.body if isinstance(c, ast.ClassDef)]:
        for fn in owner.body:
            if _decorated_run(fn, bound):
                args = fn.args
                for a in args.posonlyargs + args.args + args.kwonlyargs + [x for x in (args.vararg, args.kwarg) if x]:
                    why = _annotation_crossing(a.annotation, aliases)
                    if why is not None:
                        out.append(f'{fn.name}({a.arg}): {why}')
    return out


def _functions(tree: ast.Module) -> list[tuple[str, ast.AST, bool]]:
    """Every function of a module, nested ones too, by its qualified name, with whether it is inside a run of its own
    (marked `@runs`, or nested in one)."""
    bound = bindings(tree)
    out: list[tuple[str, ast.AST, bool]] = []

    def visit(node: ast.AST, owner: str, framed: bool) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.ClassDef):
                visit(child, owner + child.name + '.', framed)
            elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                inner = framed or _decorated_run(child, bound)
                out.append((owner + child.name, child, inner))
                visit(child, owner + child.name + '.', inner)
            else:
                visit(child, owner, framed)

    visit(tree, '', False)
    return out


def _own_calls(fn: ast.AST, bound: dict[str, str]) -> list[tuple[ast.Call, bool]]:
    """Each call in `fn`'s own body (a nested function's are its own), with whether it is inside a `with in_run(...)`."""
    out: list[tuple[ast.Call, bool]] = []

    def visit(node: ast.AST, in_block: bool) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            inside = in_block or (isinstance(child, (ast.With, ast.AsyncWith))
                                  and any(_is_in_run(i, bound) for i in child.items))
            if isinstance(child, ast.Call):
                out.append((child, in_block))
            visit(child, inside)

    visit(fn, False)
    return out


def _takes_planned(fn: ast.AST) -> bool:
    """Whether a function is handed a statement or a plan: a parameter annotated PlannedSql or PlanText."""
    args = fn.args
    return any(a.annotation is not None and any(p in ast.unparse(_parsed(a.annotation)) for p in PLANNED)
               for a in args.posonlyargs + args.args + args.kwonlyargs)


def send_sites(sources: dict[str, str]) -> tuple[dict[str, bool], set[str]]:
    """Every function that sends -- calls `_planned.send`, or a sending helper -- by `<module>.<qualified name>`, with
    whether every such call of it is framed (inside a run); and the sending helpers (each sends what it is handed,
    outside a run of its own), computed to a fixed point."""
    modules = {name[:-3]: ast.parse(text) for name, text in sources.items() if name != '_planned.py'}
    helpers: set[str] = set()
    while True:
        sites: dict[str, bool] = {}
        found: set[str] = set()
        for module, tree in modules.items():
            bound = bindings(tree)
            local = {q for q, _, _ in _functions(tree)}
            for qualified, fn, marked in _functions(tree):
                cls = qualified.rsplit('.', 1)[0] + '.' if '.' in qualified else ''
                for call, in_block in _own_calls(fn, bound):
                    f = call.func
                    target = resolve(f, bound)
                    if target is None and isinstance(f, ast.Name) and f.id in local:
                        target = f'{module}.{f.id}'
                    if (target is None and isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name)
                            and f.value.id == 'self' and f'{cls}{f.attr}' in local):
                        target = f'{module}.{cls}{f.attr}'
                    if target != '_planned.send' and target not in helpers:
                        continue
                    key = f'{module}.{qualified}'
                    framed = marked or in_block
                    sites[key] = sites.get(key, True) and framed
                    if not framed and _takes_planned(fn):
                        found.add(key)
        if found <= helpers:
            return sites, helpers
        helpers |= found


def unframed_senders(sources: dict[str, str]) -> set[str]:
    """Each function with a send outside a run, but a sending helper (whose callers' sends are the send sites)."""
    sites, helpers = send_sites(sources)
    return {k for k, framed in sites.items() if not framed and k not in helpers}


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

    def test_the_sends_are_computed(self):
        sites, helpers = send_sites(self.sources)
        # the computation is looking: the runs' own sends are framed, the helpers found by what they are handed
        self.assertTrue(sites.get('projects.run_write'), sites)
        self.assertTrue(sites.get('_host.run'), sites)
        self.assertEqual(helpers, SENDING_HELPERS)

    def test_every_send_is_in_a_run_or_a_named_unframed_sender(self):
        # shrink-only: a sender framed (or split) leaves the list, and a new one is named
        self.assertEqual(unframed_senders(self.sources), set(UNFRAMED_SENDERS))

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
        self.assertTrue(any('Reader.run: library() (' in f and 'in Reader.ask' in f for f in found), found)
        self.assertTrue(any('load: compiler.session_setup' in f for f in found), found)
        self.assertFalse(any('plan_params' in f or 'compiler.Plan' in f for f in found), found)
        # the quoted annotation read as written (until 2026-11-14 the test unquoted it first, hiding that the guard
        # did not see it: the follow-up audit's G2)
        self.assertEqual(signature_crossings(planted),
                         ['load(held): a HeldModel', 'load(make): a callable that answers a PlannedSql or a PlanText'])

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

    def test_absolute_imports_aliases_and_module_attributes_are_read(self):
        planted = '''
import legend_lite.compiler as planning
import legend_lite._library
from legend_lite import compiler
from legend_lite.compiler import session_setup as setup
from legend_lite import _runframe
from ._runframe import runs as marks

@marks('an aliased run')
def first(plan):
    compiler.plan('m', {}, 'r')
    setup('DuckDB')

@_runframe.runs('a run by its module')
def second(plan):
    planning.host_operations('DuckDB', [])
    legend_lite._library.library()

def third(connection):
    with _runframe.in_run('a block by its module'):
        compiler.compile_model('m')
'''
        self.assertEqual(set(runs_of(ast.parse(planted))),
                         {'first', 'second', "third: with in_run('a block by its module')"})
        found = [f.split(' (')[0] for f in run_crossings(planted)]
        self.assertEqual(found, ['first: compiler.plan', 'first: setup', 'second: legend_lite._library.library()',
                                 'second: planning.host_operations',
                                 "third: with in_run('a block by its module'): compiler.compile_model"])

    def test_unions_quotes_and_aliases_in_a_runs_signature_are_read(self):
        planted = '''
from typing import Callable, Optional, TypeAlias
from ._runframe import runs

Maker = Callable[[str], PlannedSql]
Loader: TypeAlias = 'Callable[[], PlanText]'
Makers = list[Maker]

@runs('a planted run')
def run(a: Callable[[str], PlannedSql] | None, b: 'HeldModel | None', c: Optional['Callable[[], PlanText]'],
        d: Maker, e: Loader, f: Makers, g: Callable[[PlannedSql], None], h: PlannedSql, i: 'list[PlanText]') -> None:
    pass
'''
        # a statement or a plan TAKEN (g, h, i) is the run's own: not named
        self.assertEqual(signature_crossings(planted), [
            'run(a): a callable that answers a PlannedSql or a PlanText',
            'run(b): a HeldModel',
            'run(c): a callable that answers a PlannedSql or a PlanText',
            "run(d): a Maker, a planner's alias",
            "run(e): a Loader, a planner's alias",
            "run(f): a Makers, a planner's alias",
        ])

    def test_a_planted_unframed_send_is_named_and_a_framed_one_is_not(self):
        sources = dict(self.sources)
        sources['planted.py'] = '''
from legend_lite._planned import send as deliver, PlannedSql
from . import _runframe
from .frames import execute_redacted

def unframed(connection, sql):
    deliver(connection, sql)

def framed(connection, sql):
    with _runframe.in_run('a planted run'):
        deliver(connection, sql)

@_runframe.runs('a planted run')
def marked(connection, sql):
    deliver(connection, sql)

def forwarder(connection, sql: PlannedSql):
    deliver(connection, sql)

def through_the_forwarder(connection):
    forwarder(connection, None)

def through_a_helper(connection):
    execute_redacted(connection, None, ())

class Holder:
    def own(self, connection):
        self.forward(connection)

    def forward(self, connection, sql: 'PlannedSql | None' = None):
        deliver(connection, sql)
'''
        found = unframed_senders(sources)
        self.assertEqual(found - set(UNFRAMED_SENDERS),
                         {'planted.unframed', 'planted.through_the_forwarder', 'planted.through_a_helper',
                          'planted.Holder.own'})
        self.assertEqual(send_sites(sources)[1] - SENDING_HELPERS, {'planted.forwarder', 'planted.Holder.forward'})

    def test_a_planted_seam_that_imports_the_planner_is_named(self):
        sources = dict(self.sources)
        sources['_planned.py'] += '\nfrom ._helper import x\n'
        sources['_helper.py'] = 'from . import compiler\nx = 1\n'
        self.assertEqual(planner_reached(sources, '_planned'), ['_planned', '_helper', 'compiler'])
        # an absolute import reaches it too
        sources['_helper.py'] = 'from legend_lite.compiler import plan\nx = 1\n'
        self.assertEqual(planner_reached(sources, '_planned'), ['_planned', '_helper', 'compiler'])
        sources['_helper.py'] = 'import legend_lite.compiler\nx = 1\n'
        self.assertEqual(planner_reached(sources, '_planned'), ['_planned', '_helper', 'compiler'])


if __name__ == '__main__':
    unittest.main()
