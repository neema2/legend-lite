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
SEAM = _modules("SEAM_SRCS")
RUN = _modules("RUN_SRCS")
PLANNER = _modules("COMPILER_SRCS")
# THE NATIVE LIBRARY'S LOADER (//python:loader), imported by its two keys alone: the planning key (_library, the
# planner's) and the service key (_services, the run side's, the warehouse's wire alone). 2026-11-15 (C5-P, A1).
LOADER = _modules("LOADER_SRCS")
KEYS = {"_library": "the planning key", "_services": "the service key"}

# What of _planned sends, or holds a way to: never a planner module's. 2026-11-15 (C5-P).
SENDING_NAMES = frozenset({"send", "connect", "Database", "Result", "require"})

# (PLANNER_SENDERS -- the warehouse's client, :compiler's carve-out: GONE 2026-11-15 with C5-P's A1, the client a run
# module over the service key.)

# A remote plane's send (the warehouse's SQL API: Warehouse.arrow_chunks, framed by its own check too): called by run
# modules alone. 2026-11-15 (C5-P).
PLANE_SENDS = frozenset({"arrow_chunks"})

# The methods that send, called on any receiver in a run module: a read's own send (Reading.execute), a plane's, a
# stream's landing (the Database's Arrow import). 2026-11-15 (C5-P, the audit's item 4); and a transaction's bounds,
# run acts (C5-P's A3), called by run modules alone.
TRANSACTION_METHODS = frozenset({"begin", "commit", "rollback"})
# 2026-11-15 (C5-P's final round, item 4): an HTTP request opened (`opener.open(...)`, `urllib.request.urlopen(...)`),
# the warehouse's or any run module's.
SEND_METHODS = frozenset({"execute", "arrow_chunks", "import_stream", "open", "urlopen"}) | TRANSACTION_METHODS

# The calls that reach the warehouse's SQL API over HTTP (warehouse._urlopen, its one place a request goes out).
# 2026-11-15 (C5-P, A2).
SEND_NAMES = frozenset({"send", "_urlopen", "urlopen"})

# THE SENDING HELPERS: run-module functions that send outside a frame of their own, each called only where a frame
# holds -- in a run, or in another helper. 2026-11-15 (C5-P, the audit's item 4): a load's stream step (_runs._stream,
# run_host_load's); (C5-P's A2) the warehouse's HTTP (_reach: its _exchange's, a run, and its statement's chunks') and a
# statement's chunks (Warehouse.arrow_chunks, a generator, which refuses outside a run itself: Unframed).
# 2026-11-15: execute_redacted was one (the audit's item 4), then a run of its own (the audit's M2), and is gone (the C5
# follow-up's check): _reads.fetched_redacted redacts the statement and its fetch, a run of its own, no helper.
# 2026-11-15 (C5-P's final round, item 4): _urlopen, the warehouse's one place a request goes out, refusing outside a
# run itself.
SENDING_HELPERS = frozenset({"_stream", "_reach", "arrow_chunks", "_urlopen"})

# What a module outside the run side takes from NO module, by name, alias, attribute or star: the one send, an opener
# of a run, a sending helper (C5-P's audit M2, 2026-11-15: until then read only as taken from _planned and _runframe,
# and the run modules bind them too).
RUN_ONLY = frozenset({"send"}) | frozenset({"runs", "in_run", "exempt"}) | SENDING_HELPERS

# THE STAR IMPORTS, by module and source: default-deny, as the run wall's STAR_IMPORTS (C5-P's audit M2, 2026-11-15):
# gs-quant's words, generated, in the timeseries builder.
STAR_IMPORTS = {"timeseries": {"_ts_words"}}

# THE LOADER'S NAMES, referred to by the loader and its two keys alone (C5-P's audit M1, 2026-11-15: a run module took
# `loaded` from the service key, which binds it, and called any name).
LOADER_NAMES = frozenset({"loaded", "Loaded", "_loader"})

# A planner module imports no door, at any level (C5-P's audit M2, 2026-11-15), but these, each by its reason and
# pinned to the names it takes (C5-P's final round, item 3, 2026-11-15: until then a carve-out admitted any name): the
# package's facade, whose lazy names ARE the doors -- Frames, Engine, show, DataCube, Page, and DataCube's module, whose
# query and studio it reads by name -- and the timeseries builder, which hands its query to DataCube's session
# (datacube._session) when asked to show it. ('', m) is the module m itself, taken by `from . import m`.
PLANNER_DOORS = {
    "__init__": {
        ("frames", "Frames"),
        ("engine", "Engine"),
        ("datacube", "show"),
        ("", "datacube"),
        ("notebook", "DataCube"),
        ("page", "Page"),
    },
    "timeseries": {("datacube", "_session")},
}

# A PLANNER'S TYPES, which no run function takes or answers (the run wall's L2, restored by C5-P's A5, 2026-11-15): a held
# model, the planning key, the loader; and a callable answering a plan or a planner's text (a planner handed in under
# another name).
PLANNER_TYPES = frozenset({"HeldModel", "Library", "Loaded", "PlannerModule"})
PLANNER_MODULE_NAMES = frozenset({"compiler", "_library", "_loader", "_planner"})
PLANNED_ANSWERS = frozenset({"PlannedSql", "PlanText", "Plan"})

# What opens a run (_runframe), spelled one way and on the run side alone. 2026-11-15 (C5-P, the audit's item 6).
OPENERS = frozenset({"runs", "in_run", "exempt"})


def package_sources() -> dict[str, str]:
    """Every module of the package by name, as shipped."""
    package = Path(FILES.Rlocation(os.environ["PLANNED_PACKAGE"])).parent
    return {p.stem: p.read_text(encoding="utf-8") for p in sorted(package.glob("*.py"))}


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
                if (a.name == "importlib" or a.name.startswith("importlib.")) and (a.name, a.asname) != (
                    "importlib.util",
                    None,
                ):
                    out.append(f"line {node.lineno}: {ast.unparse(node)}: importlib, which this rule cannot read")
        elif isinstance(node, ast.ImportFrom) and (node.module or "").split(".")[0] == "importlib":
            out.append(f"line {node.lineno}: {ast.unparse(node)}: importlib, which this rule cannot read")
        elif isinstance(node, ast.Name) and node.id == "importlib":
            up = parents.get(id(node))
            top = parents.get(id(up)) if up is not None else None
            if not (
                isinstance(up, ast.Attribute)
                and up.attr == "util"
                and isinstance(top, ast.Attribute)
                and top.attr == "find_spec"
            ):
                out.append(
                    f"line {node.lineno}: importlib reached as {ast.unparse(top or up or node)!r}: only"
                    " importlib.util.find_spec, which imports nothing"
                )
        elif isinstance(node, ast.Name) and node.id in ("__import__", "import_module"):
            out.append(f"line {node.lineno}: {node.id}, which this rule cannot read")
        elif isinstance(node, ast.Attribute) and node.attr in ("__import__", "import_module"):
            out.append(f"line {node.lineno}: {ast.unparse(node)}, which this rule cannot read")
        elif isinstance(node, ast.Constant) and node.value in ("__import__", "import_module"):
            out.append(f"line {node.lineno}: {node.value!r}, which this rule cannot read")
    return out


def imported(tree: ast.AST) -> tuple[list[tuple[str, str, str]], dict[str, set[str]], dict[str, str], set[str]]:
    """Every name a module takes from the package's modules, ``(module, name, bound as)``; every star it imports, by
    module (the package's or not); and the names it binds to a package module (``from . import x``, ``import
    legend_lite.x as y``), whose attributes are the module's names."""
    taken: list[tuple[str, str, str]] = []
    stars: dict[str, set[str]] = {}
    bound: dict[str, str] = {}
    packages: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            own = node.level == 1 or node.module == "legend_lite" or (node.module or "").startswith("legend_lite.")
            base = (
                (node.module if node.level == 1 else (node.module or "").removeprefix("legend_lite").lstrip("."))
                if own
                else None
            )
            for a in node.names:
                if a.name == "*":
                    stars.setdefault((base or "").split(".")[0] if own else (node.module or ""), set()).add(node.lineno)
                elif own and base:
                    taken.append((base.split(".")[0], a.name, a.asname or a.name))
                elif own:
                    bound[a.asname or a.name] = a.name
        elif isinstance(node, ast.Import):
            for a in node.names:
                if a.name.startswith("legend_lite.") and a.asname:
                    bound[a.asname] = a.name.split(".")[1]
                elif a.name == "legend_lite" or a.name.startswith("legend_lite."):
                    # `import legend_lite [as ll]`, and `import legend_lite.x`, which binds the package's own name
                    # (C5-P's final round, item 2: read as `legend_lite.x.<name>`, a read off module x)
                    packages.add(a.asname or "legend_lite")
    return taken, stars, bound, packages


def read_off(node: ast.AST, bound: dict[str, str], packages: set[str]) -> tuple[str, str] | None:
    """The package module and the name an attribute reads off it, or None: `x.name` with x bound to a module of the
    package, or `legend_lite.x.name` / `ll.x.name` through the package itself (C5-P's final round, item 2)."""
    if not isinstance(node, ast.Attribute):
        return None
    if isinstance(node.value, ast.Name) and node.value.id in bound:
        return bound[node.value.id], node.attr
    if (
        isinstance(node.value, ast.Attribute)
        and isinstance(node.value.value, ast.Name)
        and node.value.value.id in packages
    ):
        return node.value.attr, node.attr
    return None


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
                raise AssertionError(f"line {node.lineno}: an import beyond the package ({ast.unparse(node)})")
            own = node.level == 1 or node.module == "legend_lite" or (node.module or "").startswith("legend_lite.")
            if not own:
                continue
            base = node.module if node.level == 1 else (node.module or "").removeprefix("legend_lite").lstrip(".")
            if not base:
                if any(a.name == "*" for a in node.names):
                    raise AssertionError(f"line {node.lineno}: a star of the package ({ast.unparse(node)})")
                modules.update(a.name for a in node.names)
                if "_runframe" in {a.name for a in node.names}:
                    frame.add("*module*")
            else:
                modules.add(base.split(".")[0])
                if base == "_planned":
                    planned.update(a.name for a in node.names)
                if base == "_runframe":
                    for a in node.names:
                        frame.add(a.name if a.asname in (None, a.name) else f"{a.name} as {a.asname}")
        elif isinstance(node, ast.Import):
            for a in node.names:
                if a.name == "legend_lite":
                    package = True
                elif a.name.startswith("legend_lite."):
                    modules.add(a.name.split(".")[1])
                    if a.asname is None:
                        package = True
                    if a.name.split(".")[1] == "_runframe":
                        frame.add("*module*")
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
        ok = (
            isinstance(call, ast.Call)
            and call.func is node
            and (
                (
                    node.id == "runs"
                    and isinstance(holder, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and call in holder.decorator_list
                )
                or (node.id != "runs" and isinstance(holder, ast.withitem) and holder.context_expr is call)
            )
        )
        if not ok:
            shown = ast.unparse(
                holder
                if isinstance(holder, ast.AST) and not isinstance(holder, (ast.Module, ast.FunctionDef, ast.ClassDef))
                else (call or node)
            )
            out.append(
                f"line {node.lineno}: {shown[:60]!r}: a run is spelled `@runs(...)` on a function or `with"
                f" {node.id}(...)` alone"
                if node.id != "runs"
                else f"line {node.lineno}: {shown[:60]!r}: a run is spelled `@runs(...)` on a function alone"
            )
    return out


def _sends_in(node: ast.Call) -> str | None:
    """The send a call is, or None: the one send, the warehouse's HTTP, a sending helper, a sending method on any
    receiver."""
    f = node.func
    if isinstance(f, ast.Name) and (f.id in SEND_NAMES or f.id in SENDING_HELPERS):
        return f.id
    if isinstance(f, ast.Attribute) and f.attr in SEND_METHODS:
        return f".{f.attr}"
    return None


def unframed_sends(tree: ast.AST) -> tuple[list[str], set[str]]:
    """Each send in a run module that no frame holds -- not in an `@runs` function, a `with in_run(...)` block, or a
    sending helper -- in words; and the sending helpers this module defines that send."""
    out: list[str] = []
    helpers: set[str] = set()

    def visit(node: ast.AST, function: str | None, framed: bool) -> None:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            marked = any(
                isinstance(d, ast.Call) and isinstance(d.func, ast.Name) and d.func.id == "runs"
                for d in node.decorator_list
            )
            for d in node.decorator_list:
                visit(d, function, framed)
            for child in node.body:
                visit(child, node.name, framed or marked)
            return
        if isinstance(node, ast.With):
            opened = any(
                isinstance(i.context_expr, ast.Call)
                and isinstance(i.context_expr.func, ast.Name)
                and i.context_expr.func.id in ("in_run",)
                for i in node.items
            )
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
                    out.append(
                        f"line {node.lineno}: {sent} in {function or 'the module'}, in no run: a send of the run"
                        " side is in an `@runs` function, a `with in_run(...)` block, or a sending helper"
                    )
        for child in ast.iter_child_nodes(node):
            visit(child, function, framed)

    visit(tree, None, False)
    return out, helpers


def _annotation_names(annotation: ast.AST) -> list[ast.AST]:
    """An annotation's nodes, a quoted one parsed (``from __future__ import annotations`` writes them as text too); one
    that does not parse is refused, never skipped (G7; C5-P's audit S1)."""
    if isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):
        try:
            annotation = ast.parse(annotation.value, mode="eval").body
        except SyntaxError:
            raise AssertionError(f"an annotation this rule cannot read: {annotation.value!r}") from None
    return list(ast.walk(annotation))


class _Planners:
    """What names a planner in one module (A5; widened by C5-P's audit S1, 2026-11-15): a planner's type, a planner
    module's member, a callable answering a plan or a planner's text -- read through the names an import renamed
    (``PlanText as P``, ``Callable as C``), through a type alias (``Maker = Callable[[], PlanText]``), and through a
    class (a Protocol) whose ``__call__`` answers one."""

    def __init__(self, tree: ast.AST) -> None:
        self.renamed = {
            a.asname: a.name
            for n in ast.walk(tree)
            if isinstance(n, (ast.Import, ast.ImportFrom))
            for a in n.names
            if a.asname
        }
        self.aliases: set[str] = set()
        self.answers: set[str] = set(PLANNED_ANSWERS)
        while True:
            before = (len(self.aliases), len(self.answers))
            for node in ast.walk(tree):
                if isinstance(node, (ast.Assign, ast.AnnAssign)) or type(node).__name__ == "TypeAlias":
                    targets = (
                        node.targets
                        if isinstance(node, ast.Assign)
                        else [node.target if not type(node).__name__ == "TypeAlias" else node.name]
                    )
                    value = node.value
                    if value is None:
                        continue
                    names = [t.id for t in targets if isinstance(t, ast.Name)]
                    if self.hit(value):
                        self.aliases.update(names)
                    if self._name(value) in self.answers:
                        self.answers.update(names)
                elif isinstance(node, ast.ClassDef):
                    for item in node.body:
                        if (
                            isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
                            and item.name == "__call__"
                            and item.returns is not None
                            and (self.hit(item.returns) or self._names_in(item.returns) & self.answers)
                        ):
                            self.aliases.add(node.name)
            if (len(self.aliases), len(self.answers)) == before:
                return

    def _name(self, node: ast.AST) -> str | None:
        if isinstance(node, ast.Name):
            return self.renamed.get(node.id, node.id)
        if isinstance(node, ast.Attribute):
            return node.attr
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            try:
                return self._name(ast.parse(node.value, mode="eval").body)
            except SyntaxError:
                raise AssertionError(f"an annotation this rule cannot read: {node.value!r}") from None
        return None

    def _names_in(self, node: ast.AST) -> set[str]:
        return {x for x in (self._name(n) for n in _annotation_names(node)) if x}

    def hit(self, annotation: ast.AST) -> list[str]:
        nodes = _annotation_names(annotation)
        found: set[str] = set()
        for n in nodes:
            name = self._name(n) if isinstance(n, (ast.Name, ast.Attribute)) else None
            if name in PLANNER_TYPES or (isinstance(n, ast.Name) and name in self.aliases):
                found.add(name)
            if (
                isinstance(n, ast.Attribute)
                and isinstance(n.value, ast.Name)
                and self.renamed.get(n.value.id, n.value.id) in PLANNER_MODULE_NAMES
            ):
                found.add(f"{n.value.id}.")
            if isinstance(n, ast.Subscript) and self._name(n.value) == "Callable":
                parts = n.slice.elts if isinstance(n.slice, ast.Tuple) else [n.slice]
                found |= {f"a callable answering {x}" for x in self._names_in(parts[-1]) & self.answers}
        return sorted(found)


def planner_signatures(tree: ast.AST) -> list[str]:
    """Each run function that takes or answers a planner (A5, S1): a parameter or a return annotated with one."""
    out: list[str] = []
    try:
        planners = _Planners(tree)
    except AssertionError as refused:
        return [str(refused)]
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        a = node.args
        annotated = [
            (p.arg, p.annotation)
            for p in [
                *a.posonlyargs,
                *a.args,
                *a.kwonlyargs,
                *([a.vararg] if a.vararg else []),
                *([a.kwarg] if a.kwarg else []),
            ]
            if p.annotation is not None
        ]
        if node.returns is not None:
            annotated.append(("its answer", node.returns))
        for what, annotation in annotated:
            try:
                hit = planners.hit(annotation)
            except AssertionError as refused:
                out.append(f"line {node.lineno}: {node.name} ({what}): {refused}")
                continue
            if hit:
                out.append(f"line {node.lineno}: {node.name} ({what}) {hit}: a run takes and answers no planner")
    return out


def late_imports(tree: ast.AST) -> list[str]:
    """The late-import forms a run or seam module may not use (C5-P's audit S2, 2026-11-15; exactly these: the rules catch
    drift, not a determined adversary): sys.modules under any name, pkgutil's resolve_name, exec and eval, and a
    find_spec answer used beyond its `is None`."""
    out: list[str] = []
    parents = _parents(tree)
    systems = {"sys"} | {
        a.asname for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names if a.name == "sys" and a.asname
    }
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "sys" and any(a.name == "modules" for a in node.names):
            out.append(f"line {node.lineno}: {ast.unparse(node)}: sys.modules, a late import")
        elif (
            isinstance(node, ast.Attribute)
            and node.attr == "modules"
            and isinstance(node.value, ast.Name)
            and node.value.id in systems
        ):
            out.append(f"line {node.lineno}: {ast.unparse(node)}: sys.modules, a late import")
        elif (
            (isinstance(node, ast.Name) and node.id in ("resolve_name", "exec", "eval"))
            or (isinstance(node, ast.Attribute) and node.attr == "resolve_name")
            or (isinstance(node, ast.ImportFrom) and any(a.name == "resolve_name" for a in node.names))
        ):
            out.append(f"line {node.lineno}: {ast.unparse(node)[:40]}: a late import or a text run")
        elif isinstance(node, ast.Call) and (
            (isinstance(node.func, ast.Attribute) and node.func.attr == "find_spec")
            or (isinstance(node.func, ast.Name) and node.func.id == "find_spec")
        ):
            up = parents.get(id(node))
            if not (
                isinstance(up, ast.Compare)
                and up.left is node
                and len(up.ops) == 1
                and isinstance(up.ops[0], (ast.Is, ast.IsNot))
                and isinstance(up.comparators[0], ast.Constant)
                and up.comparators[0].value is None
            ):
                out.append(f"line {node.lineno}: {ast.unparse(node)[:40]}: a find_spec answer used beyond `is None`")
    return out


def broken(sources: dict[str, str]) -> list[str]:
    """Every break of the split, in words."""
    out: list[str] = []
    helpers: set[str] = set()
    for name, source in sorted(sources.items()):
        tree = ast.parse(source)
        out += [f"{name}: {r}" for r in unreadable(tree)]
        try:
            modules, planned, frame, package = imports(tree)
        except AssertionError as refused:
            out.append(f"{name}: {refused}")
            continue
        if name in (RUN | SEAM | PLANNER) - {"__init__"} and package:
            out.append(
                f"{name} imports the package itself, whose __init__ imports the compiler and reaches the doors: a"
                " run, seam or planner module imports its own modules by name (C5-P's final round, item 1)"
            )
        if "edge" in modules:
            # THE EDGE'S LAYER RULE (the edge program's H5 groundwork, 2026-11-17, the orchestrator's D-a): every module
            # read here is the product's (the package's top level; the edge is its own subpackage, legend_lite/edge), so
            # none imports the edge -- by any spelling, a function's own import too; the edge composes the product
            # through its own door (legend_lite.edge.engine), never the reverse
            out.append(
                f"{name} imports the edge (legend_lite.edge): the product never imports it -- the edge depends on"
                " the product, one way, and composes it at its own door"
            )
        if name in RUN and not modules <= RUN | SEAM | LOADER:
            out.append(
                f"{name} (run) imports {sorted(modules - RUN - SEAM - LOADER)}: the run side imports the run side"
                " and the seam alone (the loader by its service key alone)"
            )
        if modules & LOADER and name not in KEYS:
            out.append(f"{name} imports the native library's loader: its two keys alone do (_library, _services)")
        if name in LOADER and not modules <= SEAM:
            out.append(
                f"{name} (the loader) imports {sorted(modules - SEAM)}: it imports the seam alone (the run frame)"
            )
        taken, stars, bound, packages = imported(tree)
        if name not in LOADER | set(KEYS):
            # the loader's names, wherever an import binds them: taken from any module of the package (a key binds
            # `loaded`), or read off a module of it (`_services.loaded`)
            reached = sorted(
                {f"{what} from {source}" for source, what, _ in taken if what in LOADER_NAMES}
                | {
                    ast.unparse(n)
                    for n in ast.walk(tree)
                    if (read_off(n, bound, packages) or ("", ""))[1] in LOADER_NAMES
                }
            )
            if reached:
                out.append(
                    f"{name} reaches {reached}: the native library's loader is referred to by its two keys alone"
                    " (_library, _services)"
                )
        for source, lines in sorted(stars.items()):
            if source not in STAR_IMPORTS.get(name, set()):
                out.append(
                    f"{name}: line {min(lines)}: a star of {source or 'the package'}: a star import is one"
                    " STAR_IMPORTS lists"
                )
        if name not in RUN:
            for source, what, _ in taken:
                if what in RUN_ONLY and not (source == "_runframe" and name == "_runframe"):
                    out.append(
                        f"{name} takes {what} from {source}: the one send, a run's opener and a sending helper"
                        " are the run side's alone"
                    )
            for node in ast.walk(tree):
                if (read_off(node, bound, packages) or ("", ""))[1] in RUN_ONLY:
                    out.append(
                        f"{name}: line {node.lineno}: {ast.unparse(node)}: the one send, a run's opener and a"
                        " sending helper are the run side's alone"
                    )
        if name in PLANNER:
            door_modules = modules - RUN - SEAM - PLANNER - LOADER
            reached = {(source, what) for source, what, _ in taken if source in door_modules} | {
                ("", module) for module in bound.values() if module in door_modules
            }
            doors = sorted(
                f"{what} from {source}" if source else source + what
                for source, what in reached - PLANNER_DOORS.get(name, set())
            )
            if doors:
                out.append(f"{name} (planner) imports {doors}: a planner module imports no door")
        if name in RUN | SEAM:
            out += [f"{name}: {r}" for r in late_imports(tree)]
        if name in RUN:
            out += [f"{name}: {r}" for r in planner_signatures(tree)]
        if name in SEAM and not modules <= SEAM:
            out.append(f"{name} (seam) imports {sorted(modules - SEAM)}: the seam imports the seam alone")
        if name in PLANNER and modules & RUN:
            out.append(f"{name} (planner) imports {sorted(modules & RUN)}: the planner imports no run module")
        if name in PLANNER and planned & SENDING_NAMES:
            out.append(
                f"{name} (planner) takes {sorted(planned & SENDING_NAMES)} from _planned: the planner sends nothing"
            )
        if name not in RUN and "send" in planned:
            out.append(f"{name} imports the one send: only the run side sends (each door hands it its statements)")
        if name not in RUN | {"_runframe"} and frame & (OPENERS | {"*module*"}):
            out.append(
                f"{name} imports {sorted(frame & (OPENERS | {'*module*'}))} of _runframe: a run is opened on the"
                " run side alone, so no door opens one in which it could plan"
            )
        renamed = sorted(f for f in frame if " as " in f and f.split(" as ")[0] in OPENERS)
        if renamed:
            out.append(f"{name} imports {renamed}: an opener is imported by its own name")
        if name in RUN | {"_runframe"}:
            out += [f"{name}: {r}" for r in misspelt_runs(tree)]
        if name not in RUN:
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr in PLANE_SENDS
                ):
                    out.append(
                        f"{name}: line {node.lineno}: {ast.unparse(node.func)}: a plane's send is the run"
                        " side's (_reads.read_plane, run_on_plane; _runs's stream)"
                    )
                if (
                    name not in SEAM
                    and isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr in TRANSACTION_METHODS
                ):
                    out.append(
                        f"{name}: line {node.lineno}: {ast.unparse(node.func)}: a transaction's bound is a run"
                        " act, the run side's alone"
                    )
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in SENDING_HELPERS:
                    out.append(
                        f"{name}: line {node.lineno}: {node.func.id}(...): a sending helper is called by the run"
                        " side alone, in a run"
                    )
        if name in RUN:
            unframed, found = unframed_sends(tree)
            out += [f"{name}: {u}" for u in unframed]
            helpers |= found
    return out


class RunClosureTest(unittest.TestCase):
    def test_every_run_module_loads_with_the_run_library_alone(self):
        # this test's deps are //python:run (and :seam through it): a run module importing anything else fails here
        import importlib

        for name in sorted(RUN | SEAM):
            with self.subTest(module=name):
                importlib.import_module(f"legend_lite.{name}")

    def test_the_packages_modules_keep_the_split(self):
        sources = package_sources()
        self.assertTrue(
            RUN and SEAM and PLANNER and RUN | SEAM | PLANNER <= set(sources),
            "every module the BUILD names is the package's",
        )
        self.assertTrue(LOADER and LOADER <= set(sources))
        libraries = [RUN, SEAM, PLANNER, LOADER]
        self.assertEqual(sum(map(len, libraries)), len(set().union(*libraries)), "each module in one library")
        self.assertEqual({"_library"}, set(KEYS) & PLANNER)
        self.assertEqual({"_services"}, set(KEYS) & RUN)
        self.assertEqual([], broken(sources))
        # each sending helper named sends, in a run module (no stale entry)
        found = set()
        for name in RUN:
            found |= unframed_sends(ast.parse(sources[name]))[1]
        self.assertEqual(SENDING_HELPERS, found)
        # the run side's closure: the planning key never, the loader through the service key alone (A1)
        graph = {name: imports(ast.parse(text))[0] & set(sources) for name, text in sources.items()}
        self.assertTrue(graph["_services"] <= LOADER | SEAM, graph["_services"])
        for start in sorted(RUN - {"_services"}):
            seen, todo = {start}, [start]
            while todo:
                for nxt in graph[todo.pop()] - seen - {"_services"}:
                    seen.add(nxt)
                    todo.append(nxt)
            self.assertFalse(
                seen & ({"_library"} | LOADER), f"{start} reaches {sorted(seen & ({'_library'} | LOADER))}"
            )

    def test_the_rule_sees_every_break(self):
        # each a violator the rule must name, red on revert of its own check
        run, door, planner = sorted(RUN)[0], "engine", "compiler"
        cases = {
            # the build's own rule, read: a run or seam module imports its own side alone
            (run, "from . import compiler"): "the run side imports",
            (run, "def f():\n    from .frames import Frames"): "the run side imports",
            (run, "import legend_lite.engine"): "the run side imports",
            (run, "from legend_lite import _library"): "the run side imports",
            (run, "from legend_lite.compiler import plan"): "the run side imports",
            ("_planned", "from . import _reads"): "the seam imports",
            # the package itself, whose __init__ imports the compiler (the audit's item 1)
            (run, "import legend_lite"): "imports the package itself",
            (run, "import legend_lite as ll\nll.compiler.plan"): "imports the package itself",
            ("_plans", "def f():\n    import legend_lite"): "imports the package itself",
            # every module of :compiler, by the BUILD's list (the audit's item 3)
            (planner, "from ._runs import run_load"): "the planner imports no run module",
            ("_home", "from ._reads import run_plan"): "the planner imports no run module",
            ("timeseries", "def f():\n    from . import _runs"): "the planner imports no run module",
            (planner, "from ._planned import PlannedSql, send"): "the planner sends nothing",
            ("_library", "from ._planned import connect"): "the planner sends nothing",
            ("_pool", "from ._planned import Database"): "the planner sends nothing",
            # the edge's layer rule (H5's groundwork, 2026-11-17): no product module imports the edge, by any spelling
            ("engine", "from .edge import engine"): "imports the edge",
            ("datacube", "from . import edge"): "imports the edge",
            ("page", "def f():\n    import legend_lite.edge"): "imports the edge",
            ("__init__", "from legend_lite.edge import EngineRoutes"): "imports the edge",
            ("_edge_seam", "from legend_lite import edge"): "imports the edge",
            # the native library by its two keys (A1)
            ("warehouse", "from ._library import library"): "the run side imports",
            ("_reads", "from ._loader import loaded"): "the native library's loader: its two keys alone",
            ("compiler", "from ._loader import loaded"): "the native library's loader: its two keys alone",
            ("_loader", "from ._library import library"): "it imports the seam alone",
            ("_reads", 'import sys\nsys.modules["legend_lite.compiler"]'): "sys.modules",
            # a run takes and answers no planner (A5), quoted or not
            ("_runs", "def f(held: HeldModel): pass"): "a run takes and answers no planner",
            ("_runs", 'def f(lib: "Library | None" = None): pass'): "a run takes and answers no planner",
            ("_reads", "def f(make: Callable[[str], PlannedSql]): pass"): "a run takes and answers no planner",
            ("_reads", 'def f(make: "Callable[..., PlanText]"): pass'): "a run takes and answers no planner",
            ("_runs", "def f() -> compiler.HeldModel: pass"): "a run takes and answers no planner",
            ("_runs", "def f(*, p: Optional[Callable[[], Plan]]): pass"): "a run takes and answers no planner",
            # a transaction's bounds the run side's (A3)
            ("frames", "cursor.rollback()"): "a transaction's bound is a run act",
            ("_reads", "def f(cursor):\n    cursor.commit()"): "in no run",
            ("frames", "from ._planned import send"): "only the run side sends",
            (door, "def f():\n    from legend_lite._planned import send"): "only the run side sends",
            # a plane's send and a sending helper, the run side's alone
            (door, "rows = table.warehouse.arrow_chunks(plan.sql)"): "a plane's send is the run side's",
            ("frames", "_stream(root, step, con, None)"): "a sending helper is called by the run side alone",
            # what this cannot read (the audit's item 5)
            (run, "import importlib"): "importlib",
            (run, "from importlib import import_module"): "importlib",
            (run, "import importlib.util as u"): "importlib",
            (door, 'import importlib.util\nimportlib.import_module("legend_lite.compiler")'): "importlib",
            (door, 'import importlib.util\nm = importlib\nm.util.find_spec("x")'): "importlib",
            (door, 'import importlib.util\ngetattr(importlib, "util")'): "importlib",
            (door, '__import__("legend_lite.compiler")'): "__import__",
            (door, 'f = __import__\nf("legend_lite.compiler")'): "__import__",
            (door, 'import builtins\nbuiltins.__import__("legend_lite.compiler")'): "__import__",
            (door, 'getattr(builtins, "__import__")("x")'): "__import__",
            (run, "from . import *"): "a star of the package",
            (run, "from .. import x"): "beyond the package",
            # a run opened on the run side alone, spelled one way (the audit's item 6)
            (door, "from ._runframe import in_run"): "a run is opened on the run side alone",
            (door, 'from ._runframe import runs\n@runs("x")\ndef f(): pass'): "a run is opened on the run side alone",
            (
                door,
                'from . import _runframe\nwith _runframe.in_run("x"): pass',
            ): "a run is opened on the run side alone",
            ("_runs", "from ._runframe import in_run as opened"): "an opener is imported by its own name",
            ("_runs", 'from ._runframe import runs\ndef f(): pass\nf = runs("x")(f)'): "a run is spelled",
            ("_runs", 'from ._runframe import runs\nframed = runs("x")'): "a run is spelled",
            ("_reads", 'from ._runframe import in_run\nExitStack().enter_context(in_run("x"))'): "a run is spelled",
            # every send of the run side framed (the audit's item 4)
            ("_runs", "def f(db, sql):\n    send(db, sql)"): "in no run",
            ("_reads", "def f(read, sql):\n    return read.execute(sql)"): "in no run",
            ("_reads", "def f(plane, sql):\n    return plane.arrow_chunks(sql)"): "in no run",
            ("_runs", "def f(db, sql):\n    _stream(db, sql, None, None)"): "in no run",
            # the loader past its keys (C5-P's audit M1)
            (
                "_reads",
                'from ._services import loaded\nloaded().call("lite_session_setup", "DuckDB")',
            ): "loader is referred",
            ("_runs", 'from . import _services\n_services.loaded().call("x")'): "loader is referred",
            ("frames", "from ._library import loaded as l"): "loader is referred",
            ("engine", "from . import _loader"): "the native library's loader",
            ("_reads", "from ._services import Loaded"): "loader is referred",
            # the run side's names taken by a door, by any form (C5-P's audit M2)
            ("frames", "from ._runs import in_run, send"): "the run side's alone",
            ("engine", "from ._reads import runs as r"): "the run side's alone",
            ("engine", 'from . import _runs\nwith _runs.in_run("x"): pass'): "the run side's alone",
            ("engine", "from ._runframe import *"): "a star import is one STAR_IMPORTS lists",
            ("frames", "from ._runs import *"): "a star import is one STAR_IMPORTS lists",
            ("engine", "from os.path import *"): "a star import is one STAR_IMPORTS lists",
            ("timeseries", "from ._ts_terms import *"): "a star import is one STAR_IMPORTS lists",
            # a planner module imports no door, at any level (C5-P's audit M2)
            ("compiler", "def f():\n    from .frames import run_plan"): "a planner module imports no door",
            ("_home", "from . import engine"): "a planner module imports no door",
            ("timeseries", "from .frames import Frames"): "a planner module imports no door",
            # the final round's four forms (C5-P, 2026-11-15)
            (
                "compiler",
                "def f(con, plan):\n    import legend_lite\n    return legend_lite.Frames(con).run(plan)",
            ): "imports the package itself",
            (
                "_home",
                "import legend_lite as ll\nll.datacube._session().frames.run(plan)",
            ): "imports the package itself",
            (
                "engine",
                'import legend_lite._runs\nwith legend_lite._runs.in_run("x"):\n    legend_lite._runs.send(db, s)',
            ): "the run side's alone",
            ("frames", "import legend_lite._services\nlegend_lite._services.loaded()"): "loader is referred",
            ("__init__", "def run(con, q):\n    from .frames import run_plan"): "a planner module imports no door",
            ("timeseries", "from .datacube import Frames"): "a planner module imports no door",
            ("warehouse", "def f(req):\n    _OPENER.open(req)"): "in no run",
            ("warehouse", "import urllib.request\ndef f(req):\n    urllib.request.urlopen(req)"): "in no run",
            ("_reads", "def f(req):\n    urlopen(req)"): "in no run",
            # a run takes and answers no planner, through an alias, a Protocol, a rename (C5-P's audit S1)
            (
                "_reads",
                "Maker = Callable[[], PlanText]\ndef f(make: Maker): pass",
            ): "a run takes and answers no planner",
            (
                "_reads",
                'class Maker(Protocol):\n    def __call__(self) -> PlanText: ...\ndef f(make: "Maker"): pass',
            ): "a run takes and answers no planner",
            (
                "_runs",
                "from ._planned import PlanText as P\nfrom typing import Callable as C\ndef f(m: C[[], P]): pass",
            ): "a run takes and answers no planner",
            ("_runs", 'def f(m: "Callable[[, ]"): pass'): "an annotation this rule cannot read",
            # the late imports a run or seam module may not use, exactly these (C5-P's audit S2)
            ("_reads", "from sys import modules"): "sys.modules",
            ("_runs", 'import sys as s\ns.modules["x"]'): "sys.modules",
            ("_plans", 'import pkgutil\npkgutil.resolve_name("legend_lite.compiler:plan")'): "a late import",
            ("_reads", 'exec("from . import compiler")'): "a text run",
            ("_runs", 'eval("1")'): "a text run",
            ("_reads", 'import importlib.util\nspec = importlib.util.find_spec("x")'): "beyond `is None`",
            ("_runs", 'def f(db, r):\n    db.import_stream(r, None, "t")'): "in no run",
            ("_runs", "def f(db, sql):\n    with other(): send(db, sql)"): "in no run",
        }
        for (name, source), said in cases.items():
            with self.subTest(module=name, source=source):
                found = broken({name: source})
                self.assertTrue(found and any(said in f for f in found), found)
        # and the forms that keep it
        self.assertEqual(
            [],
            broken(
                {
                    "_runs": "from ._planned import send\nfrom . import _plans\nfrom ._runframe import in_run, runs\n"
                    '@runs("x")\ndef f(db, sql):\n    send(db, sql)\n'
                    'def g(db, sql):\n    with in_run("y"):\n        send(db, sql)\n'
                    "def _stream(db, sql):\n    send(db, sql)\n"
                    '@runs("z")\ndef h(db, sql):\n    _stream(db, sql)',
                    "frames": "from ._reads import run_plan\nfrom . import compiler",
                    "compiler": "from ._planned import PlannedSql, from_plan\nfrom ._plans import Plan",
                    "warehouse": "from ._planned import PlannedSql, Unframed, require\nfrom ._runframe import current_run\n"
                    'import importlib.util\nimportlib.util.find_spec("duckdb") is None',
                }
            ),
        )

    def test_the_one_send_is_framed(self):
        from legend_lite import _reads, _runs
        from legend_lite._planned import Unframed, connect, from_plan, send
        from legend_lite._plans import Column, Plan
        from legend_lite._runframe import current_run, in_run

        db = connect()
        one = from_plan("SELECT 1 AS n")
        # outside any run the one send refuses a plan's statement by name, and runs nothing
        with self.assertRaisesRegex(Unframed, "sent only inside a run"):
            send(db, one)
        # inside one it runs
        with in_run("a test"):
            self.assertEqual([(1,)], send(db, one).fetchall())
        self.assertIsNone(current_run())
        # each run function sends inside its own frame, the caller in none: the one send above would refuse it else
        _runs.run_statements(db, [from_plan("CREATE TABLE t AS SELECT 1 AS n")], "a test")
        self.assertEqual([(1,)], _runs.ask(db, from_plan("SELECT count(*) FROM t"), "a test"))
        self.assertEqual(1, _reads.count_of(db, Plan(from_plan("SELECT count(*) FROM t"), ()), ()))
        self.assertEqual(1, _reads.run_arrow(db, one, ()).num_rows)
        self.assertEqual('[{"n":1}]', _reads.run_rows_json(db, one, ()))
        typed = _reads.run_plan(db, Plan(one, (Column("n", "Integer", {}),)), ())
        self.assertEqual([1], typed.column("n").to_pylist())
        self.assertIsNone(current_run())


if __name__ == "__main__":
    unittest.main()
