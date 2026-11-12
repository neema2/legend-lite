"""THE ONE TEXT PYTHON'S HOST SENDS ITS DATABASE (the total wall's C3, 2026-11-12, the user's: "the hosts' separation
proven structurally"), the twin of engine-client's planned-sql.ts: a statement a PLAN carries, or one the PLANNER wrote
as its answer (a host operation's, a session's setup, a catalog's read, a project database's statements, a load's steps).

``PlannedSql`` is a ``str`` made in ONE place each, where a plan or a planner's answer is READ (``from_plan``'s callers:
compiler.py's readers, projects.py's load runners, engine.py's execute plan); ``send`` -- every statement Python's host
runs on its database goes through it -- refuses any other text by name (``NotPlanned``). A ``str`` edited after it was
read (concatenated, stripped, formatted) is a plain ``str`` again, so an edited plan statement is refused too.

Two kinds of text reach ``send`` that no reader here made, each by name:
- DataCube's own code, hosted in V8 (``_headless``): the statements its plan runner and planner client hand to Python's
  database cross the bridge as text. They are ``PlannedSql`` on that side (engine-client's planned-sql.ts: DataCube's
  TypeScript sends nothing else), and are checked again here at the crossing alone, ``across_the_bridge``: each must be
  a statement the session's planner answered (:class:`Answered`, recorded as the headless engine answers each request), or
  it is refused by name (the orchestrator's ruling of 2026-11-12). The guard (python/tests/test_planned_guard.py) pins
  the crossing's callers.
- Nothing else: Python spells no SQL of its own.
"""

from __future__ import annotations

import json
import re
from collections import OrderedDict
from collections.abc import Container, Iterable, Sequence
from typing import Any


class PlannedSql(str):
    """A statement a plan carries or the planner wrote: the only text ``send`` runs."""

    __slots__ = ()


class NotPlanned(TypeError):
    """A text sent to a database that no plan carries: refused, never run."""


def from_plan(text: str) -> PlannedSql:
    """``text`` as the plan or the planner's answer it was READ from carries it: called by the plan readers alone (the
    guard, tools/guards/PlannedSqlTest.java, holds it so). The text is never edited, only typed."""
    if not isinstance(text, str):
        raise NotPlanned(f"a plan's statement is text, not {type(text).__name__}")
    return text if isinstance(text, PlannedSql) else PlannedSql(text)


def all_from_plan(texts: Iterable[str]) -> list[PlannedSql]:
    """Each of ``texts``, as :func:`from_plan` types one."""
    return [from_plan(t) for t in texts]


def across_the_bridge(text: str, what: str, answered: Container[str]) -> PlannedSql:
    """A statement DataCube's hosted code hands Python's database (``what`` names the call): a PlannedSql on its side
    (planned-sql.ts), text on the bridge -- typed again here only if the session's planner answered it (``answered``:
    the statements its answers carried, :class:`Answered`); any other text is refused by name, never sent. Its callers
    are pinned by the guard (python/tests/test_planned_guard.py, BRIDGE)."""
    if not isinstance(text, str):
        raise NotPlanned(f"{what}: a statement is text, not {type(text).__name__}")
    if text not in answered:
        shown = text if len(text) <= 120 else text[:117] + '...'
        raise NotPlanned(f"{what}: not a statement this session's planner answered, so not sent: {shown!r} (a host sends "
                         'only what a plan carries or the planner wrote: _planned.py)')
    return PlannedSql(text)


class Answered:
    """THE STATEMENTS A SESSION'S PLANNER ANSWERED: what DataCube's hosted code may hand Python's database
    (:func:`across_the_bridge`; 2026-11-12, the C checkpoint's code audit SHOULD-FIX 6: until then every string of every
    answer, an HTTP body of any status, a label and a folded constant among them, kept for ever). Kept from an answer
    that succeeded (its ``OK`` tag) and read by its request's kind -- only the fields its TypeScript reader types as a
    plan's statement (datacube/src/lite-planner.ts, each ``fromPlan``): a text plan's ``sql``; a catalog's read; a
    session's setup; a host's operations; a project database's statements and its load; and, of a plan, each statement
    its steps carry -- a statement's text, a load step's, a delivery's, a session's, a stream's, the read-back's, a
    load's statements blobs -- never its typed tree, a label or a value its slots bind. A statement of per-key regions (a
    dynamic pivot's body) is kept as its regions: the text sent is that body written for some number of keys, exactly
    as the runner writes it (plan-runner.ts ``expandKeyed``, the charter's A3(a)), never a text the planner wrote whole.
    Bounded: the most recent :data:`CAPACITY` statements and :data:`KEYED_CAPACITY` keyed bodies, each refreshed when
    read; an older one is asked of the planner again, as the query that needs it is -- and never fewer than one answer
    carries (the C final audit's NOTE, 2026-11-12: an answer of more than CAPACITY statements evicted its own first ones
    before any was sent), so the newest answer is always kept whole. Not kept, being no statement this database runs (the
    audit's H3): a stream step's ``sql``, the REMOTE plane's statement, which this database never runs; an update step's
    ``count``, the name of a value a later step binds."""

    CAPACITY = 4096
    KEYED_CAPACITY = 256
    # an answer whose statements DataCube's code hands the database, by the request's kind
    PLANS = frozenset({'executionPlan', 'planJson', 'planModel', 'fileLoad', 'remoteMount', 'snapStream', 'synthesize'})
    # a plan's fields that hold a statement or a list of them (PlanJson's spellings)
    _STATEMENT_FIELDS = frozenset({'statement', 'sql', 'statements', 'after', 'session', 'read', 'reset', 'prepare',
                                   'count', 'begin', 'commit', 'rollback'})

    # per step kind, the fields that hold no statement of this database's (H3)
    _NOT_HERE = {'stream': frozenset({'sql'}), 'update': frozenset({'count'})}

    def __init__(self) -> None:
        self._texts: OrderedDict[str, None] = OrderedDict()
        self._keyed: OrderedDict[str, tuple[dict, ...]] = OrderedDict()
        # what the answer being recorded carries: never evicted by its own size (the NOTE)
        self._this: set[str] = set()
        self._this_keyed: set[str] = set()

    def record(self, kind: str, answer: str) -> None:
        """The statements the planner's ``answer`` to a request of ``kind`` carries, kept; a refusal carries none."""
        tag, _, payload = answer.partition('\n')
        if tag != 'OK':
            return
        self._this, self._this_keyed = set(), set()
        try:
            self._record(kind, payload)
        finally:
            # the oldest let go, never below what this answer carries: an answer is kept whole (each of its statements
            # moved to the newest end as it was kept)
            while len(self._texts) > max(self.CAPACITY, len(self._this)):
                self._texts.popitem(last=False)
            while len(self._keyed) > max(self.KEYED_CAPACITY, len(self._this_keyed)):
                self._keyed.popitem(last=False)

    def refusal(self, asked: dict) -> str | None:
        """Why a request the hosted code makes of the planner is refused, or None (the re-audit's S3, 2026-11-12): a host
        operation's own texts -- its source ``query``, its ``selectList``, a ``replacing`` expression -- are pasted by the
        planner into the statements it answers, so each must be a statement this session's planner answered already
        (a plan's statement, a table model's copy select list); any other is refused by name, the planner never asked."""
        if asked.get('kind') != 'hostOperations':
            return None
        operations = asked.get('operations')
        for op in json.loads(operations) if isinstance(operations, str) else []:
            source = op.get('from') if isinstance(op, dict) else None
            texts = [source['query']] if isinstance(source, dict) and 'query' in source else []
            texts += [op['selectList']] if isinstance(op, dict) and 'selectList' in op else []
            texts += [r.get('sql') for r in op.get('replacing') or []] if isinstance(op, dict) else []
            for text in texts:
                if text not in self:
                    shown = text if isinstance(text, str) and len(text) <= 120 else str(text)[:117] + '...'
                    return (f"a host operation's ({op.get('op')}) text is not a statement this session's planner answered, so"
                            f' the planner is not asked to write it into one: {shown!r} (_planned.py Answered.refusal)')
        return None

    def _record(self, kind: str, payload: str) -> None:
        if kind == 'catalogColumnsSql':
            self._add(payload)
        elif kind == 'tableModel':
            # the planner's copy select list, which a host operation's selectList carries back (S3)
            model = json.loads(payload)
            if model.get('copySelectList') is not None:
                self._add(model['copySelectList'])
        elif kind == 'sessionSetup':
            self._all(json.loads(payload))
        elif kind == 'hostOperations':
            for statements in json.loads(payload):
                self._all(statements)
        elif kind == 'projectData':
            database = json.loads(payload)
            for step in ('open', 'use', 'close'):
                self._all(database[step])
            if database.get('load') is not None:
                self._plan(database['load'])
        elif kind == 'plan':
            text = json.loads(payload)
            self._add(text['sql'])
            if text.get('plan') is not None:
                self._plan(text['plan'])
        elif kind in self.PLANS:
            self._plan(json.loads(payload))

    def __contains__(self, text: object) -> bool:
        if not isinstance(text, str):
            return False
        if text in self._texts:
            self._texts.move_to_end(text)
            return True
        for body, regions in self._keyed.items():
            if _written_for_keys(body, regions, text):
                self._keyed.move_to_end(body)
                return True
        return False

    def __len__(self) -> int:
        return len(self._texts) + len(self._keyed)

    def statements(self) -> tuple[str, ...]:
        """The statements kept whole, oldest first (a keyed body is kept as its regions, not among them)."""
        return tuple(self._texts)

    def _add(self, text: object) -> None:
        if not isinstance(text, str):
            raise ValueError(f"a planner's statement is text, not {type(text).__name__}")
        self._this.add(text)
        self._texts[text] = None
        self._texts.move_to_end(text)

    def _all(self, texts: object) -> None:
        if not isinstance(texts, list):
            raise ValueError(f"a planner's statements are a list, not {type(texts).__name__}")
        for text in texts:
            self._add(text)

    def _plan(self, node: object) -> None:
        """Each statement a plan's JSON carries (PlanJson's fields), its typed tree and its slots' values passed over."""
        if isinstance(node, list):
            for item in node:
                self._plan(item)
            return
        if not isinstance(node, dict):
            return
        regions = node.get('regions')
        if isinstance(node.get('statement'), str) and regions:
            self._this_keyed.add(node['statement'])
            self._keyed[node['statement']] = tuple(regions)
            self._keyed.move_to_end(node['statement'])
        if 'steps' in node and isinstance(node.get('content'), dict):
            # a load's bundle: each statements blob its steps read, by digest (DataRef.Statements: a JSON list)
            for blob in node['content'].values():
                self._all(json.loads(blob))
        not_here = self._NOT_HERE.get(node.get('_type'), frozenset())
        for key, value in node.items():
            if key in ('tree', 'content', 'regions') or key in not_here:
                continue
            if key in self._STATEMENT_FIELDS and isinstance(value, str):
                if not (key == 'statement' and regions):
                    self._add(value)
            elif key in self._STATEMENT_FIELDS and isinstance(value, list) and all(isinstance(v, str) for v in value):
                self._all(value)
            else:
                self._plan(value)


def _written_for_keys(body: str, regions: tuple[dict, ...], text: str) -> bool:
    """Whether ``text`` is the keyed ``body`` written for some number of keys, as the runner writes it (plan-runner.ts
    ``expandKeyed``: the text outside the regions as it is; each region once per key, ``lead`` before the first copy,
    ``separator`` between copies, the key's position at each counter; ``empty`` for no key). The keys' cells are bound
    values, never written: the text depends on how many keys there are alone."""
    n = 0
    while True:
        written = _keyed_text(body, regions, n)
        if written is not None and written == text:
            return True
        if n > 0 and (written is None or len(written) > len(text) or written == _keyed_text(body, regions, n - 1)):
            return False
        if n > len(text):
            return False
        n += 1


def _keyed_text(body: str, regions: tuple[dict, ...], n: int) -> str | None:
    out = []
    at = 0
    for r in regions:
        out.append(body[at:r['start']])
        if n == 0:
            if r.get('empty') is None:
                return None
            out.append(r['empty'])
        else:
            out.append(r['lead'])
            for k in range(n):
                if k > 0:
                    out.append(r['separator'])
                cursor = r['start']
                for counter in r['counters']:
                    out.append(body[cursor:counter] + str(k))
                    cursor = counter
                out.append(body[cursor:r['end']])
        at = r['end']
    out.append(body[at:])
    return ''.join(out)


def require(statement: Any, where: str) -> PlannedSql:
    """``statement`` if it is a plan's; else refused by name, ``where`` saying what it was about to be sent to. The type
    exactly (2026-11-12, the re-audit's S4: an isinstance check passed a str subclass whose ``__class__`` claimed to be
    PlannedSql)."""
    if type(statement) is PlannedSql:
        return statement
    shown = statement if isinstance(statement, str) else type(statement).__name__
    if isinstance(shown, str) and len(shown) > 120:
        shown = shown[:117] + '...'
    raise NotPlanned(f"not a plan's statement, so not sent to {where}: {shown!r} (a host sends only what a plan "
                     'carries or the planner wrote: _planned.py)')


_PLAIN_NAME = re.compile(r'[A-Za-z_][A-Za-z0-9_]*')


class Result:
    """A statement's answer as the host reads it: its rows, its Arrow table, its columns' description -- never the
    connection DuckDB answers with, which runs any text (``execute`` returns it)."""

    __slots__ = ('_answer',)

    def __init__(self, answer: Any) -> None:
        self._answer = answer

    @property
    def description(self) -> Any:
        return self._answer.description

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._answer.fetchall()

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._answer.fetchone()

    def fetchmany(self, size: int = 1) -> list[tuple[Any, ...]]:
        return self._answer.fetchmany(size)

    def to_arrow_table(self) -> Any:
        return self._answer.to_arrow_table()

    def fetch_arrow_table(self) -> Any:
        return self._answer.fetch_arrow_table()

    def fetch_record_batch(self, rows_per_batch: int = 1_000_000) -> Any:
        return self._answer.fetch_record_batch(rows_per_batch)

    def df(self) -> Any:
        return self._answer.df()


class Database:
    """THE HOST'S DATABASE, as Python's modules hold it (2026-11-12, the C checkpoint's second audit: DuckDB's own ways to
    run text or load data -- ``sql``, ``query``, ``read_csv``, ``from_arrow(...).create`` -- passed both the one send and
    the guard, one live in a planned stream step). A duckdb-python connection, or a cursor of one, behind :func:`send`
    and the few acts that are no statement: a session of its own (``cursor``), a transaction's bounds, a value attached
    by name (``register``, ``unregister``), a planned stream step's own Arrow import (``import_stream``), ``close``.
    Nothing else of DuckDB's is reachable through it, so a module holds no way to send what no plan carries. Made by
    :func:`connect`, or over a connection a person handed the host (Frames(connection=...): theirs to keep)."""

    __slots__ = ('_con',)

    def __init__(self, connection: Any) -> None:
        if isinstance(connection, (Database, Result)):
            raise TypeError('a Database holds a duckdb-python connection, not another wrapper')
        self._con = connection

    def cursor(self) -> Database:
        """A session of its own over the same database (a cursor's settings are its own)."""
        return Database(self._con.cursor())

    def begin(self) -> None:
        self._con.begin()

    def commit(self) -> None:
        self._con.commit()

    def rollback(self) -> None:
        self._con.rollback()

    def close(self) -> None:
        self._con.close()

    def register(self, name: str, value: Any) -> None:
        """A value the host holds (a frame, an Arrow table) attached by ``name``, as a plan's attach step names it."""
        self._con.register(name, value)

    def unregister(self, name: str) -> None:
        self._con.unregister(name)

    def import_stream(self, reader: Any, schema: str | None, table: str) -> None:
        """A planned stream step's landing (PlanJson ``stream``): ``reader``'s Arrow batches made into the step's table
        by the database's own Arrow import -- no statement spelled, the names the plan's, each plain or refused."""
        for name in (table, *(() if schema is None else (schema,))):
            if not _PLAIN_NAME.fullmatch(name):
                raise NotPlanned(f"a stream lands in a table named plainly (letters, digits, '_'), not {name!r}")
        self._con.from_arrow(reader).create(table if schema is None else f'{schema}.{table}')

    def _send_planned(self, sql: PlannedSql, params: Sequence[Any] | None) -> Result:
        # the one send's own check, here too (the re-audit's M2, 2026-11-12: this, `_run` until then, sent whatever it
        # was handed, and the guard refused no call of it -- it refuses any reference to it outside this module now)
        sql = require(sql, 'the database')
        return Result(self._con.execute(sql, list(params)) if params else self._con.execute(sql))


def connect(path: str | None = None) -> Database:
    """A database of the host's own (duckdb-python's, in memory unless ``path``), held as a :class:`Database`."""
    import duckdb

    return Database(duckdb.connect() if path is None else duckdb.connect(path))


def send(target: Database, statement: Any, params: Sequence[Any] | None = None) -> Result:
    """THE ONE SEND: ``statement`` run on ``target`` (the host's :class:`Database`, or a cursor of it), its ``?``s bound
    to ``params`` in order when there are any -- only if it is a plan's (:func:`require`)."""
    if not isinstance(target, Database):
        raise NotPlanned(f"the one send runs on the host's Database, not on a {type(target).__name__}")
    sql = require(statement, 'the database')
    return target._send_planned(sql, params)
