"""A PROJECT'S OWN DATABASE on Python's DuckDB (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C5), the twin of the tab's
(engine-client/src/project-data.ts): where a project version's Data rows live when Query or Studio, served by Python,
runs one of its queries, or a saved query opens on Python's page -- a database of their own inside Python's DuckDB, its
schemas as the model declares them, never beside a person's frames. What it is, and every statement that makes and
points at it, is the native library's (``compiler.project_data``, the platform's ProjectData in Java, by this
database's type), named by what it holds (the model's Databases and Data rows); Python spells none. Its rows are the
plan's: the ONCE load a server's project plan carries (``load``), run here (``run_load``), its statements a blob checked
against its digest (B3, 2026-11-11: until then the fill's statements as text, run outside any plan, census M14). Made
the first time a query over the model asks, on a cursor of its own, and kept for the process's life (the user,
2026-10-13: no strategy for letting one go; legend-lite's server keeps its own so too). A model with no Data rows has
none."""

from __future__ import annotations

import contextlib
import hashlib
import json
import re
import threading
from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager, nullcontext
from dataclasses import dataclass, field
from typing import Any

from . import compiler
from ._planned import PlannedSql, all_from_plan, from_plan, send

# the kinds the compiler names a model's failure to read as a model by (planner.Folded, PureV1Api.refusal): such a
# model is no project's, and is refused where a model is; any other refusal of a project's database (a Data element
# filling a table no Database declares) is said as it is
_NOT_A_MODEL = frozenset({'PARSER', 'COMPILATION'})


# the plan format this host runs: the planner's (PlanJson.VERSION), as the tab's runner's is (plan-runner.ts PLAN_VERSION,
# held equal by PlanVersionsAgreeTest); a stale one here refuses every fresh plan, which the shared conformance fixture
# runs (test_engine) -- so the two cannot drift apart unseen
PLAN_VERSION = 48


class LoadRefused(ValueError):
    """A plan's load this host does not run, whose data is not the data it was planned against, or whose statement the
    database refused (its words, the data named): refused by name."""


def _load_of(plan: Any) -> dict[str, Any]:
    """``plan``'s root load, refused by name unless it is a legend-lite plan of the version this host runs with a load at
    its root."""
    if not isinstance(plan, dict) or plan.get('format') != 'legend-lite-plan' or plan.get('version') != PLAN_VERSION:
        # the tab's runner's words (plan-runner.ts read), the shared fixture's (the tenets audit's LOW 1, 2026-11-11)
        said = {'format': plan.get('format'), 'version': plan.get('version')} if isinstance(plan, dict) else {}
        raise LoadRefused(f'not a legend-lite execution plan of version {PLAN_VERSION}: '
                          f"{json.dumps(said, separators=(',', ':'))}")
    root = plan.get('root')
    if not isinstance(root, dict) or root.get('_type') != 'load':
        raise LoadRefused('a load\'s plan is a legend-lite plan with a load at its root')
    return root


def run_load(plan: Any, execute: Callable[[PlannedSql], Any]) -> None:
    """A plan whose root is the ONCE load a project's database is made with (the native library's ``load``: the platform's
    LoadPlanner.projectData, the load a server's project plan carries), run by ``execute`` on the database Python made
    for it: its steps in order -- a statement as it is; a content blob of statements, its bytes checked against the
    digest and size its reference names, each statement as written, a refusal naming its data; a file's load refused by
    name (no store a host's plan names). The rule is engine-client's plan-runner.ts ``runLoad`` (the tab's), the same plan
    format (B3, 2026-11-11)."""
    root = _load_of(plan)
    if root.get('cadence') != 'ONCE':
        raise LoadRefused(f"a load's plan made with a database is ONCE, not {root.get('cadence')}")
    content = root.get('content') or {}
    for step in root['steps']:
        kind = step.get('_type')
        if kind == 'statement':
            execute(from_plan(step['sql']))
        elif kind == 'statements':
            ref, of = step['ref'], step['of']
            text = content.get(ref['digest'])
            if text is None:
                raise LoadRefused(f"the load {root['identity']} reads the content {ref['digest']}, which its plan does not carry")
            data = text.encode('utf-8')
            digest = hashlib.sha256(data).hexdigest()
            if digest != ref['digest'] or len(data) != ref['size']:
                raise LoadRefused(f'{of} is not the data it was planned against: {len(data)} bytes of digest {digest}, '
                                  f"where the plan names {ref['size']} bytes of digest {ref['digest']}")
            statements = json.loads(text)
            if not isinstance(statements, list) or not statements or not all(isinstance(x, str) for x in statements):
                raise LoadRefused(f'{of}: a statements blob is a non-empty JSON array of strings')
            # the blob's statements, the plan's own (its content, checked against the digest it names above)
            for sql in all_from_plan(statements):
                try:
                    execute(sql)
                except Exception as refused:
                    # the database's own words, the data named (as the tab's runner says them)
                    raise LoadRefused(f'{of}: {refused}') from refused
        elif kind == 'file':
            raise LoadRefused(f"the file '{step['ref']['name']}' is a load this host does not run: a host's files are no "
                              'store a plan names yet')
        else:
            raise LoadRefused(f"a load's step is a statement, a statements blob or a file, not a {kind}")


# a name the database's Arrow import is given as it is written (duckdb-python's relation ``create``): a plain one alone,
# so this host spells no quoting
_PLAIN = re.compile(r'[A-Za-z_][A-Za-z0-9_]*\Z')


@dataclass(frozen=True)
class Landed:
    """What a stream step landed: its table, the rows the database counted there, and the plane's receipt."""

    plane: str
    schema: str | None
    table: str
    rows: int
    receipt: dict[str, Any]


@dataclass(frozen=True)
class HostLoad:
    """What a host's load left: each value it attached for the connection, ``(held, as)`` -- kept, and attached again on
    every cursor the host opens (``Frames.reading``) -- what each stream landed, and each setting of the connection a
    bound step made (``{"sql", "binds"}``: its statement and what it binds by name, never a value -- a bearer URL's
    session variable, which the host replays on every cursor it opens: B3's Q3)."""

    attached: tuple[tuple[str, str], ...]
    landed: tuple[Landed, ...]
    settings: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True)
class HostFile:
    """A file this host holds, as a load's plan names it (its name, its bytes' size and lower-case hex SHA-256), and
    where its database reads it (``at``: bound to the statement's ``?``, never written into one)."""

    name: str
    digest: str
    size: int
    at: str


def run_host_load(plan: Any, connection: Any, *, values: Callable[[str], Any] | None = None,
                  planes: Mapping[str, Any] | None = None, files: tuple[HostFile, ...] | None = None,
                  secrets: Mapping[str, Mapping[str, str]] | None = None) -> HostLoad:
    """A HOST_SESSION load run on ``connection`` (duckdb-python's) -- the user's ruling of 2026-11-11, "This is the right
    plan": a statement as it is; a value this host holds ATTACHED BY NAME (``values``: a Python frame by its name; one it
    does not hold refused by name), kept for the connection (CONNECTION: answered in ``attached``, which the host attaches
    again on every cursor it opens) or for the load alone (DATABASE: let go when the load ends); a remote plane's answer
    STREAMED into a table (``planes``: the remote planes this host holds by name, each with ``arrow_chunks``) in one
    transaction, kept only when the stream completed and the table holds the rows the plane counted; a FILE this host
    holds read by its database (``files``: the one the plan names by name, size and digest, its path bound to each
    reading statement's ``?``); and a statement BOUND from what this host holds by name (``secrets``: a remote file's
    keys, a bearer URL), a setting it makes on the connection answered in ``settings`` (the #6 bridge, 2026-11-12: until
    then DataCube's code ran a page's file and a remote mount through the tab's runner and handed Python the statements).
    A load that fails leaves nothing attached. Never a row, a value's bytes, a path or a key in the plan: each is named.
    The rule is engine-client's plan-runner.ts ``runLoad`` (the tab's), the same plan format and the same words."""
    root = _load_of(plan)
    if root.get('cadence') != 'HOST_SESSION':
        raise LoadRefused(f"a host's load is HOST_SESSION, not {root.get('cadence')}")
    attached: list[tuple[str, str]] = []
    once: list[str] = []
    landed: list[Landed] = []
    settings: list[dict[str, Any]] = []
    ok = False
    try:
        for step in root['steps']:
            kind = step.get('_type')
            if kind == 'statement':
                send(connection, from_plan(step['sql']))
            elif kind == 'file':
                _file(step, connection, files)
            elif kind == 'bound':
                sql = from_plan(step['sql'])
                send(connection, sql, bound_values(step['binds'], secrets, f"the load {root['identity']}'s bound statement"))
                if step.get('reach') == 'CONNECTION':
                    settings.append({'sql': sql, 'binds': list(step['binds'])})
                elif step.get('reach') != 'DATABASE':
                    raise LoadRefused(f"a bound statement's reach is DATABASE or CONNECTION, not {step.get('reach')}")
            elif kind == 'attach':
                held, as_, reach = step['held'], step['as'], step['reach']
                if reach not in ('CONNECTION', 'DATABASE'):
                    raise LoadRefused(f'an attached value\'s reach is CONNECTION or DATABASE, not {reach}')
                value = None if values is None else values(held)
                if value is None:
                    raise LoadRefused(f"'{held}' is not a value this host holds: nothing is attached as {as_}")
                connection.register(as_, value)
                if reach == 'CONNECTION':
                    attached.append((held, as_))
                else:
                    once.append(as_)
            elif kind == 'stream':
                landed.append(_stream(root, step, connection, planes))
            else:
                raise LoadRefused(f"a host's load step is a statement, a file, a bound statement, an attached value or a "
                                  f'stream, not a {kind}')
        ok = True
    finally:
        # a value attached for the load alone is let go when it ends; a load that failed leaves nothing attached
        for as_ in [*once, *([] if ok else [a for _, a in attached])]:
            connection.unregister(as_)
    return HostLoad(tuple(attached), tuple(landed), tuple(settings))


def _file(step: dict[str, Any], connection: Any, files: tuple[HostFile, ...] | None) -> None:
    """One file step (PlanJson's ``file``): the file the plan names, one this host holds, read by its database -- each
    reading statement run with the file's path bound to its ``?``, then its ``after`` statements whatever happened. The
    tab's runner's rule and words (plan-runner.ts ``deliver``)."""
    ref, delivery = step['ref'], step['delivery']
    if ref.get('namespace') != 'HOST':
        raise LoadRefused(f"the file '{ref['name']}' ({ref.get('namespace')}) is not a host's own: this host reads its own "
                          'files alone')
    if files is None:
        raise LoadRefused(f"the file '{ref['name']}' is a host's own, and this run was given no host's files")
    if delivery.get('_type') != 'reads':
        raise LoadRefused(f"a file's delivery is its database's reading of it, not {delivery.get('_type')}")
    at = located(files, ref)
    try:
        for statement in all_from_plan(delivery['statements']):
            try:
                send(connection, statement, (at,))
            except Exception as refused:
                # the database's own words, the file named (its reader names the bound path)
                raise LoadRefused(f"the file '{ref['name']}': {refused}") from refused
    finally:
        for statement in all_from_plan(delivery['after']):
            send(connection, statement)


def located(files: tuple[HostFile, ...], ref: Mapping[str, Any]) -> str:
    """Where this host's database reads the file ``ref`` names: the one held under its name, its size and digest the
    plan's -- else refused by name (plan-runner.ts ``heldFiles``, the same words)."""
    for f in files:
        if (f.name, f.digest, f.size) == (ref['name'], ref['digest'], ref['size']):
            return f.at
    raise LoadRefused(f"the file '{ref['name']}' ({ref['size']} bytes of digest {ref['digest']}) is not one this load holds")


def bound_values(binds: list[dict[str, Any]], secrets: Mapping[str, Mapping[str, str]] | None, what: str) -> tuple[Any, ...]:
    """``binds`` as the values a statement's ``?``s take: the plan's own text, or what ``secrets`` holds under the name and
    field; a bind of another kind, or a held one with no store, refused by name (connection-settings.ts ``boundValues``
    and the host's store, ``SecretStore``: the same words)."""
    out: list[Any] = []
    for b in binds:
        if b.get('_type') == 'value':
            out.append(b['text'])
            continue
        if b.get('_type') != 'held':
            raise LoadRefused(f"{what}'s bind is the plan's value or a held one, not {b.get('_type')}")
        if secrets is None:
            raise LoadRefused(f"{what} binds what a host holds, and this run was given no host's store")
        held = secrets.get(b['name'])
        if held is None:
            raise LoadRefused(f"'{b['name']}' is not a secret this host holds")
        if b['field'] not in held:
            raise LoadRefused(f"the secret '{b['name']}' holds no {b['field']}")
        out.append(held[b['field']])
    return tuple(out)


def _stream(root: dict[str, Any], step: dict[str, Any], connection: Any, planes: Mapping[str, Any] | None) -> Landed:
    """One stream step (PlanJson's ``stream``): in ONE transaction, its statements the plan's -- its ``prepare`` (the
    schema made, the table there before dropped), the plane's answer read into the table by the database's own Arrow
    import (its chunks one Arrow stream, read once, never held whole), then the database's ``count`` of what landed --
    committed only when the stream completed (the plane's done step heard, every chunk it names read) and the count is
    the plane's own for its answer; else rolled back, nothing left, and refused by name. No hash (the orchestrator,
    2026-11-12): a snap has no content known at planning; a stream cut short is the count's and the done step's to tell."""
    import pyarrow as pa
    import pyarrow.ipc

    schema, table = step['schema'], step['table']
    into = table if schema is None else f'{schema}.{table}'
    if planes is None:
        raise LoadRefused(f"the load {root['identity']} streams the remote plane '{step['plane']}' into {into}, and this "
                          "host streams no plane's rows into its database")
    plane = planes.get(step['plane'])
    if plane is None:
        raise LoadRefused(f"'{step['plane']}' is not a remote plane this host holds: nothing is streamed into {into}")
    if not all(_PLAIN.match(n) for n in (table, *(() if schema is None else (schema,)))):
        raise LoadRefused(f"a stream lands in a table this host names plainly (letters, digits, '_'), not {into}")
    receipt: dict[str, Any] = {}
    for sql in all_from_plan(step['transaction']['begin']):
        send(connection, sql)
    chunks = iter(plane.arrow_chunks(from_plan(step['sql']), receipt.update))
    try:
        for sql in all_from_plan(step['prepare']):
            send(connection, sql)
        first = next(chunks, None)
        if first is None:
            raise LoadRefused(f"no data came back from '{step['plane']}' to load into {into}, not even its columns")
        head = pyarrow.ipc.open_stream(first)

        def batches() -> Iterator[Any]:
            yield from head
            for chunk in chunks:
                yield from pyarrow.ipc.open_stream(chunk)

        # the database's own Arrow import: the chunks read as one stream as the table is made, no statement spelled
        connection.import_stream(pa.RecordBatchReader.from_batches(head.schema, batches()), schema, table)
        if not receipt:
            raise LoadRefused(f"the stream from '{step['plane']}' ended before its plane said it was done: nothing of it "
                              f'is kept in {into}')
        expected = receipt.get('serverRows')
        if expected is None:
            raise LoadRefused(f"'{step['plane']}' did not count its answer, so a stream cut short could not be told: "
                              f'nothing of it is kept in {into}')
        counted = int(send(connection, from_plan(step['count'])).fetchone()[0])
        if counted != expected:
            raise LoadRefused(f"the stream from '{step['plane']}' landed {counted} rows in {into}, where its plane counted "
                              f'{expected}: nothing of it is kept')
        for sql in all_from_plan(step['transaction']['commit']):
            send(connection, sql)
        return Landed(step['plane'], schema, table, counted, dict(receipt))
    except BaseException:
        # the database's own undo: the table made in the transaction goes with it, one there before is as it was
        for sql in all_from_plan(step['transaction']['rollback']):
            with contextlib.suppress(Exception):
                send(connection, sql)
        raise
    finally:
        # a read that ended early lets its plane free the statement (the warehouse's: closed or cancelled)
        close = getattr(chunks, 'close', None)
        if close is not None:
            close()


class PlanRefused(ValueError):
    """A plan this host's runner does not run: refused by name, before any of it runs."""


def run_plan(plan: Any, run: Callable[[PlannedSql, tuple[Any, ...]], list[tuple[Any, ...]]], database_type: str) -> str:
    """A WRITE'S PROGRAM, run in the transaction its caller has begun (engine-client's plan-runner.ts ``runPlan`` with
    ``joined``, its rules and words; the #6 bridge, 2026-11-12: a table output's write, which DataCube's code had planned
    and handed Python step by step, planned and run here): its steps in order on ``run`` (one statement and its bound
    values, its rows) -- an update's count bound where a later step reads it, a data: payload's text from the statement's
    content checked against its digest -- and its answer, its typed result's rows as lite's JSON rows. Nothing is begun or
    ended here: the caller's transaction holds the whole write. Refused by name: another plan's format or version, a
    parameter, a keyed (dynamic pivot) root, a constant answer, steps on several databases or on one of another type, a
    load, a raw grid's relation, a per-key region, a trace stamp, a slot the run would supply."""
    if not isinstance(plan, dict) or plan.get('format') != 'legend-lite-plan' or plan.get('version') != PLAN_VERSION:
        said = {'format': plan.get('format'), 'version': plan.get('version')} if isinstance(plan, dict) else {}
        raise PlanRefused(f'not a legend-lite execution plan of version {PLAN_VERSION}: '
                          f"{json.dumps(said, separators=(',', ':'))}")
    if not isinstance(plan.get('parameters'), list) or plan['parameters']:
        raise PlanRefused('a plan that takes parameters is not run here: its program binds none')
    root = plan.get('root')
    if not isinstance(root, dict):
        raise PlanRefused('a plan with no root')
    kind = root.get('_type')
    steps = root['steps'] if kind == 'sequence' else [root] if kind in ('tdsResult', 'constant') else None
    if steps is None:
        raise PlanRefused(f"a plan's root is a sequence, a typed result or a constant, not a {kind}")
    results = [s for s in steps if s.get('_type') in ('tdsResult', 'constant')]
    if len(results) != 1:
        raise PlanRefused(f'a plan answers with one typed result, not {len(results)}')
    if results[0].get('_type') == 'constant':
        raise PlanRefused("a constant answer is a query's, not a write's: a write answers what the database counted")
    sqls = [_step_sql(s) for s in steps]
    target = json.dumps(sqls[0]['target'], sort_keys=True)
    if any(json.dumps(s['target'], sort_keys=True) != target for s in sqls):
        raise PlanRefused("a plan's steps run on one database: these name several")
    t = sqls[0]['target']
    database = t.get('database') or {}
    named = (database.get('connection') or {}).get('databaseType', database.get('databaseType'))
    if named != database_type:
        raise PlanRefused(f'a plan of a {named} database is not run on this {database_type} one')
    if not isinstance(t.get('loads'), list):
        raise PlanRefused('a plan whose target names no loads list is not of this version')
    if t['loads']:
        raise PlanRefused("a plan that loads data into its database first runs on a database the host made for it and "
                          "established (establish): not on the host's own")
    for s in steps:
        if s.get('_type') == 'tdsResult' and s.get('shape') == 'RELATION' and not s.get('columns'):
            raise PlanRefused("a relation of the columns its result reports (a raw grid's) is not run here: the planner "
                              'runs it')
    for s in sqls:
        if s.get('regions'):
            raise PlanRefused("a statement of per-key regions (a dynamic pivot's) is not run here: a write's has none")
        if s.get('stamp') is not None:
            raise PlanRefused(f"a statement binding its own execution-trace stamp ('{s['stamp']}') is not run here: a "
                              "host's session sends no trace comment")
        for slot in s.get('slots') or []:
            binding = slot['binding']
            if binding.get('_type') == 'payload':
                continue
            if binding.get('_type') != 'one':
                what = {'key': "a key's cell", 'source': 'a file source', 'payload': 'a data: payload'}.get(
                    binding.get('_type'), 'a list')
                raise PlanRefused(f"'{slot['parameter']}' binds {what}, which is not run here")
            if binding.get('asText') is not None:
                raise PlanRefused(f"'{slot['parameter']}' is a date bound as its text, which is not run here")
    # the target's session statements: this host's own database is set up by nothing else first
    for statement in all_from_plan(t.get('session') or []):
        run(statement, ())
    values: dict[str, Any] = {}

    def bound(s: dict[str, Any]) -> tuple[Any, ...]:
        out: list[Any] = []
        for slot in s.get('slots') or []:
            if slot['binding'].get('_type') == 'payload':
                out.append(compiler.statement_params({'content': s.get('content'), 'slots': [slot]})[0])
            elif slot['parameter'] not in values:
                raise PlanRefused(f"'{slot['parameter']}' is bound before any step gives it")
            else:
                out.append(values[slot['parameter']])
        return tuple(out)

    answer = ''
    for step, s in zip(steps, sqls):
        rows = run(from_plan(s['statement']), bound(s))
        if step['_type'] == 'update':
            if step.get('count') is not None:
                values[step['count']] = _count_of(rows)
        elif step['_type'] == 'tdsResult':
            answer = _rows_of(step['columns'], rows)
    return answer


def _step_sql(s: dict[str, Any]) -> dict[str, Any]:
    if s.get('_type') in ('update', 'check', 'tdsResult'):
        return s['sql']
    raise PlanRefused('a sequence inside a sequence' if s.get('_type') == 'sequence'
                      else f"a plan's step is an update, a check or a typed result, not a {s.get('_type')}")


def _count_of(rows: list[tuple[Any, ...]]) -> int:
    v = rows[0][0] if rows and rows[0] else None
    if isinstance(v, bool) or not isinstance(v, int):
        raise ValueError(f'the database answered a change with no count: {json.dumps(list(rows[0]) if rows else None)}')
    return v


def _rows_of(columns: list[dict[str, Any]], rows: list[tuple[Any, ...]]) -> str:
    """Rows as lite's JSON rows, one object per row keyed by the plan's column names, as plan-runner.ts ``rowsOf`` writes
    them: an integer, a text or a boolean cell; any other refused by name (the planner writes it)."""
    def cell(c: dict[str, Any], v: Any) -> str:
        if v is None:
            return 'null'
        if isinstance(v, bool):
            return 'true' if v else 'false'
        if isinstance(v, str):
            return json.dumps(v, ensure_ascii=False)
        if isinstance(v, int) and abs(v) <= 2 ** 53 - 1:
            return str(v)
        raise PlanRefused(f"the host's runner writes an integer, a text or a boolean cell, and '{c['name']}' "
                          f"({c.get('type')}) holds {type(v).__name__}: the planner writes it (executeAnswer)")
    return '[' + ','.join('{' + ','.join(f"{json.dumps(c['name'], ensure_ascii=False)}:{cell(c, r[i])}"
                                         for i, c in enumerate(columns)) + '}' for r in rows) + ']'


@dataclass(frozen=True)
class Project:
    """A project's database in use: a cursor pointed at it, and its model held."""

    cursor: Any
    held: compiler.HeldModel


@dataclass
class _Open:
    cursor: Any
    # the project's model, held: what its queries are planned against (a page's planner requests, plan-and-run.json)
    held: compiler.HeldModel
    # one use at a time: a DuckDB connection is not safe from several threads at once (the audit of C5)
    lock: threading.Lock = field(default_factory=threading.Lock)


class ProjectDatabases:
    """The project databases on one DuckDB connection (an engine's Frames'): ``using`` a model's, made the first time."""

    def __init__(self, connection: Any, database_type: str = 'DuckDB',
                 guard: Callable[[], AbstractContextManager[Any]] = nullcontext) -> None:
        """``guard``: what every use of ``connection`` from another thread holds (an engine's Frames' lock): a database
        is made under it, never beside a query on the frames."""
        self._connection = connection
        self._guard = guard
        self._type = database_type
        self._open: dict[str, _Open] = {}
        self._lock = threading.RLock()

    @property
    def names(self) -> list[str]:
        """The project databases this process holds."""
        with self._lock:
            return list(self._open)

    @contextmanager
    def using(self, model: str) -> Iterator[Project | None]:
        """``model``'s project database (named by what it holds) -- a cursor on it and the model held -- made and filled
        the first time, this thread's alone until the block ends; None when the model has no Data rows, or does not
        read as a model (refused where a model is)."""
        try:
            statements = compiler.project_data(model, self._type)
        except compiler.LegendError as refused:
            if refused.kind not in _NOT_A_MODEL:
                raise
            statements = None
        if statements is None or statements.get('load') is None:
            yield None
            return
        with self._guard(), self._lock:
            opened = self._open.get(statements['name'])
            if opened is None:
                opened = self._make(model, statements)
                self._open[statements['name']] = opened
        with opened.lock:
            yield Project(opened.cursor, opened.held)

    def _make(self, model: str, statements: dict[str, Any]) -> _Open:
        held = compiler.HeldModel(model)
        cursor = None
        try:
            for sql in statements['open']:
                send(self._connection, sql)
            cursor = self._connection.cursor()
            # a cursor's session is its own: set up as every connection Python plans for is (frames.py)
            for sql in [*compiler.session_setup(self._type), *statements['use']]:
                send(cursor, sql)
            # its rows: the plan's load (B3)
            run_load(statements['load'], lambda sql: send(cursor, sql))
        except Exception:
            # a half-made database left behind would be found again by its name: drop it, and let its model go
            if cursor is not None:
                cursor.close()
            for sql in statements['close']:
                send(self._connection, sql)
            held.close()
            raise
        return _Open(cursor, held)
