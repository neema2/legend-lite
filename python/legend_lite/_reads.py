"""A READ OF PYTHON'S DATABASE, the run side (C5-P, 2026-11-15: the plan/exec split on Python, as //core:exec is Java's):
a plan's statement sent and its answer read -- its rows typed by the plan (``typed_by_plan``), a failure's words cleared
of every value the host holds (``fetched_redacted``), one read of the frames as it saw the database (``Reading``) -- and
the runs Python's doors hand the planner's statements to (``run_plan``, ``run_arrow``, ``run_rows_json``, ``count_of``,
``begin_read``, the hosted DataCube's ``run_bridged_query`` and ``run_bridged_statement``). Each is a run (``@runs``):
it is handed what the planner answered and plans nothing, and this module imports no planner module
(//python:run_closure_test; its Bazel library, //python:run, has no planner in its deps). Until C5-P its reads were
frames.py's, which names them still, and its runs were sent where they were planned."""

from __future__ import annotations

import copy
from collections.abc import Callable, Iterable, Sequence
from typing import Any
from urllib.parse import unquote_plus

import duckdb
import pyarrow as pa
import pyarrow.ipc

from ._params import bind
from ._planned import Database, PlannedSql, Result, send
from ._plans import Plan, host_rows
from ._runframe import current_run, in_run, runs
from ._types import UnknownType, is_integer

# the field metadata that carries a result column's type, as the compiler typed it
TYPE_KEY = b'legend.type'


class UntypedColumn(RuntimeError):
    """A result column its plan does not type -- the plan and the database disagree about the query's shape -- or types
    by a name legend-lite does not read (``_types``)."""


def _integer(column: pa.ChunkedArray) -> pa.ChunkedArray:
    """An Integer column as int64, from any integer the database answers with (a SUM's exact decimal(38, 0)
    included); one with a value past 64 bits as the database stored it, exact."""
    kind = column.type
    if pa.types.is_int64(kind):
        return column
    if pa.types.is_integer(kind) or pa.types.is_null(kind) or (pa.types.is_decimal(kind) and kind.scale == 0):
        try:
            # a safe cast: one past 64 bits refuses it, never wrapped, and the exact value is kept
            return column.cast(pa.int64())
        except pa.ArrowInvalid:
            return column
    return column


def typed_by_plan(rows: pa.Table, plan: Plan) -> pa.Table:
    """A planned query's rows, typed by its plan: each column the compiler's type (``TYPE_KEY``), its values exact (an
    Integer an int64, by either compiler's name for one: a server's ``precisePrimitives::BigInt`` too). A result column
    the plan does not type is refused: the two disagree about the query's shape, a bug to surface, never a gap to fill
    (engine-client's ``typedByPlan``); so is one typed by a name legend-lite's lattice does not know (``_types``)."""
    types = {c.name: c.type for c in plan.columns}
    fields, columns = [], []
    for field, column in zip(rows.schema, rows.columns):
        typed = types.get(field.name)
        if typed is None:
            raise UntypedColumn(f"the plan does not type the result column '{field.name}': {plan.sql}")
        try:
            integer = is_integer(typed)
        except UnknownType as unknown:
            raise UntypedColumn(f'{unknown}: {plan.sql}') from None
        if integer:
            column = _integer(column)
        fields.append(pa.field(field.name, column.type, field.nullable, {**(field.metadata or {}), TYPE_KEY: typed}))
        columns.append(column)
    return pa.Table.from_arrays(columns, schema=pa.schema(fields, rows.schema.metadata))


def to_pandas(rows: pa.Table) -> Any:
    """Typed rows as a pandas DataFrame, each value kept as it is: an integer an integer and a boolean a boolean, nulls
    and all (pandas' nullable ``Int64`` and ``boolean``, where its own would make them floats and objects), a Decimal
    a ``Decimal``, a StrictDate a ``date``."""
    import pandas as pd
    nullable = {pa.int64(): pd.Int64Dtype(), pa.bool_(): pd.BooleanDtype()}
    return rows.to_pandas(types_mapper=nullable.get)


# the shortest query-string parameter value redacted on its own (engine-client connection-settings.ts, the same rule): a
# shorter one (``X-Amz-Expires=3600``'s) is no token, and taking it out would take out the message's own words
_SHORTEST_HELD_PARAMETER = 8


def held_texts(value: str) -> list[str]:
    """THE TEXTS A HELD VALUE MAY COME BACK AS (the B checkpoint's tenets audit, M2, 2026-11-11; engine-client
    connection-settings.ts ``heldTexts``): a database repeats a value it was bound in its own error -- an expired
    presigned link's read names the whole URL -- and may quote its query string, or one parameter, alone, encoded or
    not. So a held URL is taken out as itself, its query string and each parameter's value (raw and decoded), the
    longest first."""
    out = {value}
    query = value.find('?')
    if query >= 0:
        fragment = value.find('#', query)
        string = value[query + 1:] if fragment < 0 else value[query + 1:fragment]
        out.add(string)
        for part in string.split('&'):
            raw = part.partition('=')[2] if '=' in part else part
            for v in (raw, unquote_plus(raw)):
                if len(v) >= _SHORTEST_HELD_PARAMETER:
                    out.add(v)
    return sorted((t for t in out if t), key=len, reverse=True)


def redact_held(text: str, values: Iterable[str]) -> str:
    """``text`` with every held value in ``values`` (each as ``held_texts`` takes it apart) written ``***``."""
    for value in values:
        for t in held_texts(value):
            text = text.replace(t, '***')
    return text


def redacted(failure: BaseException, held: Iterable[str]) -> BaseException | None:
    """``failure`` again, of its type, its words with every held value taken out -- or None where none was in them. The
    caller raises it outside its ``except``, so the database's own error, which still holds the value, is not chained
    to it."""
    said = str(failure)
    out = redact_held(said, held)
    if out == said:
        return None
    # of its type, as raised, by a copy: no traceback, cause or context of the original's, its attributes cleared, its
    # words the redacted ones (2026-11-12, the re-audit's S4: no value's own type is called outside _planned.py)
    redacted = copy.copy(failure)
    redacted.__dict__.clear()
    redacted.args = (out,)
    return redacted


def fetched_redacted(connection: Any, sql: PlannedSql, held: Iterable[str], params: Sequence[Any],
                     fetch: Callable[[Result], Any]) -> Any:
    """``sql`` run on ``connection`` AND its answer fetched (``fetch``), one failure's words for both with every held
    value taken out (``redacted``): DuckDB fails at the fetch as well as at the statement (a remote file read as its
    rows stream, a value that does not convert past the first rows), and a presigned link's expiry there repeats its
    URL (the C5 follow-up's audit item 1, 2026-11-15: until then the fetch was outside the redaction; its check removed
    execute_redacted, which redacted the send alone). A run of its own -- the caller's, named as it is, or one of its
    own outside any."""
    failure: BaseException | None = None
    try:
        with in_run(current_run() or 'a statement redacted'):
            return fetch(send(connection, sql, params))
    except duckdb.Error as e:
        failure = redacted(e, held)
        if failure is None:
            raise
    raise failure


# the failures DuckDB raises BEFORE a statement runs -- a table not there, a name not bound, text that does not parse, a
# file the database may not touch: they leave the transaction standing, so they never end a read, and DuckDB still
# raises them after a read has ended (measured on the pinned DuckDB, 2026-11-14: after an abort a ParserException, a
# CatalogException, a BinderException and a PermissionException are each raised as themselves -- a PermissionException
# first leaves the transaction standing, its next statement runs -- as are an invalid input and a file that cannot be
# read, which Reading._failed reads apart; a statement that runs says the transaction is aborted)
_REFUSED_BEFORE_RUNNING = (duckdb.ParserException, duckdb.CatalogException, duckdb.BinderException,
                           duckdb.PermissionException)

# the failures that abort a standing transaction, yet that DuckDB still raises as themselves after an abort (measured on
# the pinned DuckDB, 2026-11-14): an invalid input (a statement given fewer values than its `?`s) and a file that cannot
# be read (an IOException at bind -- a local or remote file not there, read_parquet's or read_csv's or a view's over
# one; an HTTPException is one): each is kept only where nothing is kept yet -- the read's end when it came first, and
# never a replacement for the failure that ended the read (the final audit of 9af16fbe58's P7; the C4 final fix round's
# B1, 2026-11-14)
_ABORTS_UNLESS_ABORTED = (duckdb.InvalidInputException, duckdb.IOException)


def _words(said: str, kind: str) -> str:
    """A failure's words: its first line that says something, or its class where none does (an empty message, or one
    that is only space and line breaks)."""
    for line in said.splitlines():
        if line.strip():
            return line.strip()
    return kind


class Reading:
    """WHAT ONE READ SEES (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D15): the frames' database as it was when the read
    began -- a cursor of its own, its session set as the planner's SQL expects, each frame read at each query registered
    on it as the frame was then, in a transaction pinned as it began -- and what was served then (``hosted``, the frames'
    and files' ``models``). Whatever is registered, read in, snapped or dropped meanwhile, on any thread, a query on it
    reads the same rows, and no lock is held while it is used: DataCube's code, asked a call by Python, reads through it
    and asks what it asks of Python's engine over it, on its own thread, waiting on no thread. One thread uses it at a
    time (its read's: the caller, then DataCube's thread while the call runs)."""

    def __init__(self, cursor: Database, hosted: dict[str, Any], models: dict[str, str],
                 held: tuple[str, ...] = ()) -> None:
        self.cursor = cursor
        self.hosted = hosted
        self.models = models
        # the values the remote files' settings bind (a bearer URL), taken out of a failure's words (M2)
        self.held_values = held
        # the failure that aborted this read's transaction, in its words (``execute``): the read's end
        self.ended_by: str | None = None
        # the read's latest failure that RAN -- at a statement or at its fetch -- in its words: the one that aborted the
        # transaction, when DuckDB next says it is aborted (one refused before it ran never replaces it)
        self._last_failure: str | None = None

    @runs('a read')
    def execute(self, sql: PlannedSql, params: tuple[str, ...] = ()) -> Result:
        """A statement run as this read sees the database. One that fails at run time (a value that does not convert, an
        overflow) aborts DuckDB's transaction, and the read ENDS there: the call shows that failure, and its later
        queries do not run -- DuckDB refuses each, said in words naming the failure that ended the read -- and the next
        call opens a fresh read (the user's ruling of 2026-11-13, (1b)). Until then the read began and pinned another
        transaction (F36), which planned the pin's statement inside the read's run (G4: a run plans nothing) and moved
        the read, unsaid, to the database as it was then (G7: no silent snapshot switch). One refused before it ran (a
        table not there) leaves the read as it was: DuckDB's transaction stands. A failure's words name no value the
        engine holds (an expired presigned link's read repeats its URL: M2)."""
        failure: BaseException | None = None
        try:
            # a plan's own values (its data: payloads: compiler.plan_params, M3) bound to its `?`s
            return send(self.cursor, sql, params)
        except duckdb.TransactionException as e:
            # DuckDB says the read's transaction is aborted: by the read's latest failure that ran -- at a statement or
            # at its fetch -- never one refused before it ran (a table not there, text that does not parse), which left
            # the transaction standing, before the abort or after it (the C4 audit's M4, 2026-11-13; the follow-up
            # audit's G3, 2026-11-14: until then a refusal after the abort replaced the failure that ended the read)
            if self.ended_by is None:
                self.ended_by = self._last_failure
            if self.ended_by is None:
                failure = redacted(e, self.held_values)
                if failure is None:
                    raise
            else:
                # the database's own error class, so a caller that answers a database's refusal answers this one (the
                # plan-and-run's 422, never a 500)
                failure = duckdb.TransactionException(
                    f"this read ended at the failure that aborted its transaction ({self.ended_by}): DuckDB aborts"
                    " the read's transaction there, so the call's later queries do not run; the next call reads the"
                    ' database afresh')
        except duckdb.Error as e:
            failure = self._failed(e)
            if failure is None:
                raise
        raise failure

    def _failed(self, e: duckdb.Error) -> BaseException | None:
        """``e`` kept as the read's latest failure where it RAN (one refused before it ran leaves the transaction
        standing, and never replaces the one that aborted it); the failure to raise, its held values taken out (None:
        ``e`` names none)."""
        failure = redacted(e, self.held_values)
        # an invalid input or a file that cannot be read aborts a standing transaction, yet DuckDB raises it as itself
        # after an abort too: it is kept only where nothing is kept yet (_ABORTS_UNLESS_ABORTED)
        if not isinstance(e, _REFUSED_BEFORE_RUNNING) and not (
                isinstance(e, _ABORTS_UNLESS_ABORTED) and self._last_failure is not None):
            self._last_failure = _words(str(failure) if failure is not None else str(e), type(e).__name__)
        return failure

    def fetched(self, result: Result) -> pa.Table:
        """``result``'s rows as this read fetches them: a failure AT THE FETCH (DuckDB converts as it streams, so a
        value that does not convert past the first rows it hands over fails here, not at the statement: measured at
        3M rows, 2026-11-14) is the read's failure as one at the statement is -- it aborts the transaction, and the
        read ends there, named by it (the follow-up audit's G3: until then a fetch's failure went unrecorded). Named by
        what DuckDB raises: DuckDB 1.5.5 now and then answers a worker's conversion failure as its interrupt, a race of
        its own threads (measured 2026-11-14 with no legend-lite code: about 1 in 20 on a cursor or in a transaction),
        and the read is named by that."""
        try:
            return result.to_arrow_table()
        except duckdb.TransactionException:
            raise
        except duckdb.Error as e:
            failure = self._failed(e)
            if failure is None:
                raise
            raise failure from None

    @runs('a read')
    def run(self, plan: Plan, values: dict[str, Any] | None = None) -> pa.Table:
        """A plan's SQL run as this read sees the database, its rows typed by the plan (``typed_by_plan``): its declared
        parameters bound at this run from ``values`` (``_params.bind``, checked before anything is sent: the lite API
        spec's §4, 2026-11-17)."""
        return typed_by_plan(self.fetched(self.execute(plan.sql, bind(plan, values))), plan)


@runs('a plan')
def run_plan(target: Database, plan: Plan, held: Iterable[str], values: dict[str, Any] | None = None) -> pa.Table:
    """PYTHON'S EXECUTOR: a plan's SQL run on ``target``, its rows typed by the plan (``typed_by_plan``), a failure's words
    with every value in ``held`` taken out, at the statement and at its fetch (``fetched_redacted``); its declared
    parameters bound at this run from ``values`` (``_params.bind``: the lite API spec's §4). Only SQL a planner wrote for
    this database comes here."""
    return typed_by_plan(fetched_redacted(target, plan.sql, held, bind(plan, values), Result.to_arrow_table), plan)


@runs('an execute')
def run_arrow(target: Database, sql: PlannedSql, held: Iterable[str], params: tuple[str, ...] = ()) -> pa.Table:
    """An execute's statement run on ``target`` (its ``?``s bound to ``params``), its rows as Arrow, a failure's words
    with every value in ``held`` taken out."""
    return fetched_redacted(target, sql, held, params, Result.to_arrow_table)


@runs('an execute')
def run_rows_json(target: Database, sql: PlannedSql, held: Iterable[str], params: tuple[str, ...] = ()) -> str:
    """An execute's typed statement run on ``target``, its rows handed back as the driver read them (``host_rows``: what
    the compiler decodes by the plan's columns), a failure's words with every value in ``held`` taken out."""
    return fetched_redacted(target, sql, held, params,
                            lambda cursor: host_rows([d[0] for d in cursor.description], cursor.fetchall()))


@runs("a snap's count")
def count_of(target: Database, plan: Plan, held: Iterable[str]) -> int:
    """A counting plan's number (its one row's one column), run on ``target`` with its own values bound, a failure's
    words with every value in ``held`` taken out (the C5 follow-up's audit item 1)."""
    return fetched_redacted(target, plan.sql, held, list(plan.params), lambda result: int(result.fetchone()[0]))


@runs('a read begun')
def begin_read(cursor: Database, setup: Sequence[PlannedSql],
               settings: Iterable[tuple[PlannedSql, Sequence[Any]]],
               attachments: Iterable[tuple[str, Any]], pin: PlannedSql) -> None:
    """A read's own session begun on ``cursor`` (``Frames.reading``): the planner's session setup; each remote file's
    settings of the connection with the values they bind (B3's Q3: a bearer URL's session variable is one
    connection's); each frame's value attached by the name its plan holds it under (a cursor is a connection of its
    own); then a transaction begun and pinned by the planner's catalog question (DuckDB begins one on a database when
    it is first read), so the read sees the database as it is now."""
    for statement in setup:
        send(cursor, statement)
    for statement, params in settings:
        send(cursor, statement, params)
    for as_, value in attachments:
        cursor.register(as_, value)
    cursor.begin()
    send(cursor, pin).fetchall()


@runs('a read ended')
def end_read(cursor: Database, failing: BaseException | None) -> None:
    """A read's transaction let go -- rolled back when it began (``Database.began``), as nothing it ran is kept -- and
    its cursor closed (``Frames.reading``). A rollback that fails is said, never swallowed (G7; the C5 follow-up,
    2026-11-15: until then every duckdb.Error of the rollback was suppressed): raised when the read itself ended well;
    when the read is ``failing``, that failure stays the one raised -- a read is named by the failure that ended it
    (C4) -- and the rollback's is noted on it. DuckDB rolls back an aborted transaction without complaint (measured
    2026-11-15, duckdb 1.5.5): a rollback fails only when no transaction is open on the cursor any longer (ended by a
    statement of the read's own) or the connection is gone."""
    try:
        if cursor.began:
            try:
                cursor.rollback()
            except duckdb.Error as refused:
                if failing is None:
                    raise
                failing.add_note(f"and rolling the read's transaction back failed: {type(refused).__name__}: {refused}")
    finally:
        cursor.close()


@runs("the hosted DataCube's query")
def run_bridged_query(db: Database, over: Reading | None, statement: PlannedSql) -> pa.Table:
    """The hosted DataCube's query (checked at the bridge first: ``_planned.across_the_bridge``) run on ``db`` -- or, a
    call made in a read of the frames, through it (``Reading.execute``: its cursor, its failures' words redacted) -- its
    rows as Arrow (none: an empty table)."""
    result = send(db, statement) if over is None else over.execute(statement)
    if result.description is None:
        return pa.table({})
    return result.to_arrow_table() if over is None else over.fetched(result)


@runs("the hosted DataCube's statement")
def run_bridged_statement(db: Database, statement: PlannedSql, params: Sequence[Any]) -> pa.Table:
    """The hosted DataCube's statement (checked at the bridge first) run on ``db`` with its values bound -- never in a
    read of the frames -- its rows as Arrow (none: an empty table)."""
    result = send(db, statement, params)
    return pa.table({}) if result.description is None else result.to_arrow_table()


def arrow_of(chunks: Iterable[bytes]) -> pa.Table:
    """A result's Arrow IPC streams, one per chunk, as one table: the first chunk's schema (every chunk carries it;
    the warehouse's DuckDB writes one at least, its schema alone for no rows). engine.py's ``_arrow_of`` until C5-P."""
    tables = [pyarrow.ipc.open_stream(c).read_all() for c in chunks]
    if not tables:
        raise pa.ArrowInvalid('the warehouse answered no chunk, not even its columns')
    return pa.concat_tables(tables) if len(tables) > 1 else tables[0]


@runs("a remote plane's read")
def read_plane(plane: Any, sql: PlannedSql, params: Sequence[Any] = ()) -> pa.Table:
    """A plan's statement sent to a remote plane (the warehouse's SQL API: ``plane.arrow_chunks``, which takes a
    PlannedSql and the values its ``?``s bind, positional, bound at run), its answer read as one Arrow table (C5-P,
    2026-11-15: until then sent in no run). The plane is handed in: this module imports no plane's client. Its binding's
    calls are the native library's services, framed here on whatever thread the binding runs them (the compiler's pool
    copies the caller's context)."""
    return arrow_of(plane.arrow_chunks(sql, None, tuple(params)))


@runs("a remote plane's read")
def run_on_plane(plane: Any, plan: Plan, on_receipt: Callable[[dict[str, Any]], None] | None = None,
                 values: dict[str, Any] | None = None) -> pa.Table:
    """A plan run on a remote plane (``read_plane``), its rows typed by the plan; ``on_receipt`` hears what the plane
    issued for it (its statement and its count). Its statement's values bound as a local run binds them (``bind``: its
    payloads, and each declared parameter's checked value), carried to the plane positionally (the host train,
    2026-11-17)."""
    return typed_by_plan(arrow_of(plane.arrow_chunks(plan.sql, on_receipt, bind(plan, values))), plan)
