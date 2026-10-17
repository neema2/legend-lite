"""Dataframes as Legend tables: a frame registered in duckdb-python, its model written by the compiler, and
queries the compiler plans run by DuckDB, answered as Arrow.

    frames = Frames()
    trades = frames.register('trades', df)              # Live: every query reads df as it is then
    trades.execute("->filter(x|$x.desk == 'FX')")       # a pyarrow.Table, typed by the plan

A frame is anything Arrow reads: a pandas or polars DataFrame, a pyarrow Table, any object that exports
Arrow -- or a function returning one, so a Live table follows a frame you rebuild. It is handed to DuckDB
AS ARROW: duckdb-python's own pandas reader is about ten times slower on text columns, and reading a
pandas frame through it once does not see later changes (pandas copies on write).

LIVE (the default) re-reads the frame at every query, so the answer is the frame's as it is then; if its
columns changed, the model is written again. SNAPPED copies it into DuckDB once: faster to query, and it
never changes. Either way the frame is an ordinary DuckDB table or view named as registered, so the
compiler reads its catalog and writes its model exactly as it does for a DataCube table
(``table_model``): one writer.

AN ANSWER IS TYPED BY ITS PLAN (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §15.3a), as the tab's executor types its
own (engine-client's ``typedByPlan``): each column carries the compiler's type (its field's ``legend.type``), a column
the plan does not type is refused, and each value is exact, as the database stored it -- an Integer an int64 (a sum
DuckDB answers as an exact decimal included), keeping that exact decimal only past 64 bits, never wrong; a Decimal
exact; a DateTime as stored, in UTC (the session's zone), to the microsecond; a Variant its JSON text, whatever the
shape of the query (the planner answers a stored STRUCT or LIST as JSON, §15.3a). ``to_pandas`` keeps them so: an
Integer or a Boolean with nulls stays an integer or a boolean (pandas' nullable types), never a float or an object.
"""

from __future__ import annotations

import contextlib
import re
import sys
import threading
from typing import Any, Callable, Iterator

from ._extra import require_datacube

# DataCube's: the datacube extra's packages, or one sentence saying to install it (_extra.py)
require_datacube()

import duckdb
import pyarrow as pa

from . import _host, compiler
from ._types import UnknownType, is_integer

LIVE = 'live'
SNAPPED = 'snapped'

# a frame's name is its DuckDB table and its Legend package: one plain identifier
_NAME = re.compile(r'[A-Za-z_][A-Za-z0-9_]*\Z')
# the plain identifiers Pure reads as literals, so no package can be named by them
_LITERALS = ('true', 'false')
# the names Frames gives what it registers for a Live frame (its Arrow source): never a page's file's table's
_OWN = '__legend_lite_'

# the catalog question's columns, in its order, as the model writer names them (compiler.catalog_columns_sql)
_CATALOG_FIELDS = ('name', 'dataType', 'logicalType', 'precision', 'scale', 'notNull')

# the field metadata that carries a result column's type, as the compiler typed it
TYPE_KEY = b'legend.type'

# how many models a page sent Python holds at once (Frames.model_for): a page's project, as its revisions move
_ADMITTED = 8


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


def typed_by_plan(rows: pa.Table, plan: compiler.Plan) -> pa.Table:
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


def _arrow(frame: Any) -> pa.Table:
    """A frame as an Arrow table. pandas through its own converter, without its index (``reset_index()``
    keeps one as a column); pandas is not imported here -- a pandas frame means it already is."""
    pandas = sys.modules.get('pandas')
    if pandas is not None and isinstance(frame, pandas.DataFrame):
        return pa.Table.from_pandas(frame, preserve_index=False)
    if isinstance(frame, pa.Table):
        return frame
    if isinstance(frame, pa.RecordBatchReader):
        return frame.read_all()
    return pa.table(frame)


class Table:
    """A registered frame: its Legend model and the queries over it. Unusable once its name is registered
    again or unregistered."""

    def __init__(self, frames: Frames, name: str, read: Callable[[], Any], mode: str, given: Any = None) -> None:
        self._frames = frames
        self.name = name
        self.mode = mode
        self._read = read
        # the frame (or function) it was registered with, as given: what a page's grid asks for by name is it
        self.given = given
        self._registered = f'{_OWN}{name}'
        # its table or view, in the database's own main schema: named for the host's operations (D5)
        self._target = _host.name(name, schema='main')
        self._schema: pa.Schema | None = None
        self._closed = False
        self.model = ''
        self.runtime = ''
        self.accessor = ''
        self.source: dict[str, Any] = {}
        self.excluded: tuple[str, ...] = ()
        self.columns: tuple[compiler.Column, ...] = ()
        # its model, compiled once and held while the table is registered (each plan then pays for its query alone)
        self.held: compiler.HeldModel | None = None

    def _load(self, arrow: pa.Table) -> None:
        """Puts the frame in DuckDB as the table it is named, reads its catalog, and writes its model."""
        con = self._frames.connection
        con.register(self._registered, arrow)
        kind = 'view' if self.mode == LIVE else 'table'
        # a view whose columns changed is dropped, not replaced: the copy below may change its kind of select
        _host.run(con, {'op': 'drop', 'kind': kind, 'target': self._target},
                  {'op': 'relation', 'kind': kind, 'target': self._target,
                   'from': {'relation': _host.name(self._registered)}})
        if self.mode == SNAPPED:
            con.unregister(self._registered)
        rows = con.execute(compiler.catalog_columns_sql('main', self.name)).fetchall()
        written = compiler.table_model({
            'table': self.name,
            'pkg': self.name,
            # a frame is ours to convert: the view applies the conversions as it reads, the copy as it is made
            'convertible': True,
            'databaseType': 'DuckDB',
            'columns': [dict(zip(_CATALOG_FIELDS, row)) for row in rows],
        })
        if written['conversions']:
            source = _host.name(self._registered) if self.mode == LIVE else self._target
            _host.run(con, {'op': 'relation', 'kind': kind, 'target': self._target, 'replace': True,
                            'selectList': written['copySelectList'], 'from': {'relation': source}})
        self.model = written['model']
        self.runtime = written['runtime']
        self.source = written['source']
        self.accessor = written['accessor']
        self.excluded = tuple(written['excluded'])
        # the model as it is now held; the one before (columns changed) let go
        held, before = compiler.HeldModel(self.model), self.held
        self.held = held
        if before is not None:
            before.close()
        self.columns = self.relation_type({'_type': 'lambda', 'body': [self.source], 'parameters': []})
        self._schema = arrow.schema

    def _drop(self) -> None:
        """Takes the frame out of DuckDB, and makes this handle unusable."""
        con = self._frames.connection
        _host.run(con, {'op': 'drop', 'kind': 'view' if self.mode == LIVE else 'table', 'target': self._target})
        if self.mode == LIVE:
            con.unregister(self._registered)
        if self.held is not None:
            self.held.close()
        self._closed = True

    def _refresh(self) -> None:
        """Live: the frame as it is now. Its rows are read again; if its columns changed, so is its model."""
        arrow = _arrow(self._read())
        if arrow.schema == self._schema:
            self._frames.connection.register(self._registered, arrow)
        else:
            self._load(arrow)

    def _tree(self, query: str | dict[str, Any]) -> dict[str, Any]:
        if isinstance(query, dict):
            return query
        # "->filter(...)": a query that starts at this table
        return compiler.parse('|' + self.accessor + query if query.lstrip().startswith('->') else query)

    def _model(self) -> compiler.HeldModel:
        if self.held is None:
            raise ValueError(f'the frame {self.name!r} has no model yet')
        return self.held

    def relation_type(self, tree: dict[str, Any]) -> tuple[compiler.Column, ...]:
        """The columns a query over this table returns, as the compiler types them, against its model held."""
        return self._model().relation_type(tree)

    def _ready(self) -> None:
        if self._closed:
            raise ValueError(f'the frame {self.name!r} was registered again or unregistered: use the new table')
        # held still (Frames.serving), it was read as the query began: read once per query, a function frame called once
        if self.mode == LIVE and not self._frames._still:
            self._refresh()

    def plan(self, query: str | dict[str, Any]) -> compiler.Plan:
        """The plan for a query over this table (Pure text, a tree, or ``->...`` applied to it), by Python's own
        compiler, in this process."""
        with self._frames._lock:
            self._ready()
            return self._model().plan(self._tree(query), self.runtime)

    def execute(self, query: str | dict[str, Any]) -> pa.Table:
        """Runs a query over this table (as ``plan`` takes it) and answers as an Arrow table, typed by its plan."""
        with self._frames._lock:
            self._ready()
            return self._frames.run(self._model().plan(self._tree(query), self.runtime))


class Frames:
    """Frames in one duckdb-python database, each a Legend table the compiler plans queries over.

    The connection's session is set as the planner's SQL expects (the dialect's own setup: UTC), whether
    Frames opened it or was given it. Frames creates and drops only its own tables and views (a Live one
    reads an object registered on the connection, so in a database file it lasts no longer than the
    session: ``close()`` drops them). Use the connection through Frames' methods while other threads do;
    they hold one lock."""

    def __init__(self, connection: duckdb.DuckDBPyConnection | None = None) -> None:
        self.connection = connection if connection is not None else duckdb.connect()
        # a database Frames made itself, which legend-lite may set up for its own reading (an engine's remote files),
        # where the person's own is left as they keep it
        self.owns_connection = connection is None
        for statement in compiler.session_setup('DuckDB'):
            self.connection.execute(statement)
        self._lock = threading.RLock()
        # how deep in ``serving`` the thread holding the lock is: inside, the frames are held still, each Live one read
        self._still = 0
        # DuckDB matches names without regard to case, quoted or not: so does this
        self._tables: dict[str, Table] = {}
        # the tables a page's files were read into on this connection by DataCube's own code (the engine's file.json),
        # and the views a page's remote files are read through (remote.json; docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md
        # §4.3), each by its name: its name as written, its model, whether it is a view, and its model held (planned
        # against while it is kept, as a frame's is: §15.3)
        self._files: dict[str, tuple[str, str, bool, compiler.HeldModel]] = {}
        # the models a page sent that read only what Python serves (model_for), each held with the tables it declares
        # (checked again at each use: a table no longer served is no longer read), while recent: a page's project, most
        # recent last
        self._admitted: dict[str, tuple[compiler.HeldModel, frozenset[str]]] = {}

    def register(self, name: str, frame: Any, mode: str = LIVE) -> Table:
        """Registers a frame (or a function returning one) as the table ``name``, Live or Snapped, and
        writes its model. Registering a name again replaces its table, and the old handle stops working."""
        if not _NAME.match(name):
            raise ValueError(f"a frame's name is a plain identifier (letters, digits, '_'), not {name!r}")
        if name in _LITERALS:
            raise ValueError(f"a frame cannot be named {name!r}: Pure reads it as a literal, so no package can be")
        if mode not in (LIVE, SNAPPED):
            raise ValueError(f"mode is {LIVE!r} or {SNAPPED!r}, not {mode!r}")
        read: Callable[[], Any] = frame if callable(frame) else (lambda: frame)
        first = read()
        if mode == LIVE and isinstance(first, pa.RecordBatchReader):
            raise ValueError('a stream can be read only once, so it cannot be Live: register it snapped')
        arrow = _arrow(first)
        with self._lock:
            key = name.lower()
            # the old table stays under its name until the new one takes its place: a reader asking for the name
            # meanwhile (a tab asking its version, which takes no lock) finds it all along, never a frame gone
            old = self._tables.get(key)
            if key in self._files:
                raise ValueError(f'a page\'s file was read into a table named {self._files[key][0]!r}: '
                                 'a frame takes another name')
            if old is not None:
                old._drop()
            elif self._exists(name):
                raise ValueError(f'the database already has a table or view named {name!r}, and Frames did not make it')
            table = Table(self, name, read, mode, frame)
            try:
                table._load(arrow)
            except BaseException:
                # the name serves nothing (its old table dropped): what the load made of it so far taken out again,
                # so the name is free for a frame that can be served
                self._tables.pop(key, None)
                table._drop()
                raise
            self._tables[key] = table
            return table

    def run(self, plan: compiler.Plan) -> pa.Table:
        """PYTHON'S EXECUTOR: a plan's SQL run on this database, its rows typed by the plan (``typed_by_plan``) --
        a table's query, a grid's rows, and a page's grid planned and run in Python (the engine's plan-and-run.json).
        Only SQL a planner wrote for this database comes here."""
        with self._lock:
            return typed_by_plan(self.connection.execute(plan.sql).to_arrow_table(), plan)

    @contextlib.contextmanager
    def serving(self) -> Iterator[dict[str, str]]:
        """The frames held still for one query that another host plans and runs (the engine): the lock taken,
        each Live table read as its frame is now (its model written again if its columns changed), and their
        models given by name -- the only models such a query may be over. Run its SQL on ``connection`` inside."""
        with self._lock:
            for table in self._tables.values():
                if table.mode == LIVE:
                    table._ready()
            self._still += 1
            try:
                yield {**{table.name: table.model for table in self._tables.values()},
                       **{name: model for name, model, _view, _held in self._files.values()}}
            finally:
                self._still -= 1

    def held_for(self, model: str) -> compiler.HeldModel | None:
        """The model held for the frame or page file whose model is exactly this text -- what a page's plan is made
        against (the engine's plan-and-run.json, plan.json) -- or None: no model this engine serves. Asked inside
        ``serving``, the frames held still."""
        with self._lock:
            for table in self._tables.values():
                if table.model == model and table.held is not None:
                    return table.held
            for _name, kept, _view, held in self._files.values():
                if kept == model:
                    return held
        return None

    def model_for(self, model: str) -> compiler.HeldModel | None:
        """The model a page's query may be planned over (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B3; the user's ruling,
        option (c)): one held for a frame or a page's file (``held_for``), or a model the page sent -- its project's, say
        -- every table of whose Databases is one Python serves, so the SQL planned over it reads only what Python showed
        the page, never another table of this database (a person's own, an attached database's). Held while recent;
        None when it declares a table Python does not serve, or does not read as a model."""
        with self._lock:
            held = self.held_for(model)
            if held is not None:
                return held
            served = set(self._tables) | set(self._files)
            if model in self._admitted:
                held, tables = self._admitted.pop(model)
                self._admitted[model] = (held, tables)
                return held if tables <= served else None
            try:
                elements = compiler.model_elements(model)
            except compiler.LegendError:
                return None
            tables: set[str] = set()
            for element in elements:
                for schema in element.get('schemas', []) if element.get('_type') == 'relational' else []:
                    if schema.get('name') not in ('default', 'main') and schema.get('tables'):
                        return None
                    tables |= {str(t.get('name', '')).lower() for t in schema.get('tables', [])}
            if not tables <= served:
                return None
            try:
                held = compiler.HeldModel(model)
            except compiler.LegendError:
                # it reads as a model, but does not compile: no model to plan over
                return None
            self._admitted[model] = (held, frozenset(tables))
            while len(self._admitted) > _ADMITTED:
                self._admitted.pop(next(iter(self._admitted)))[0].close()
            return held

    @contextlib.contextmanager
    def reading_file(self, asked: str, view: bool = False) -> Iterator[str]:
        """The frames held still while DataCube's own code reads a page's file into a table of its own on this
        connection (the engine's file.json). Yields the table's name: the one the page asked for when it is free, else
        the next free one (``<name>_2``, ...) -- free meaning no frame's, no other file's (another page's, say), no
        table or view Frames did not make in any schema, and never Frames' own ``__legend_lite_`` names -- so no page
        reads into, or later drops, a table it did not make. ``view``: a remote file, read through a view of that name.
        Inside, the model is kept with ``keep_file``; a read that fails leaves nothing under the name."""
        if not _NAME.match(asked) or asked in _LITERALS:
            raise ValueError(f"a file's table is named by a plain identifier (letters, digits, '_'), not {asked!r}")
        if asked.lower().startswith(_OWN):
            raise ValueError(f'a name beginning {_OWN} is legend-lite\'s own, not a file\'s: {asked!r}')
        with self._lock:
            name, n = asked, 2
            while self._taken(name):
                name, n = f'{asked}_{n}', n + 1
            try:
                yield name
            except BaseException:
                # what the read made of the name taken out again: the name was free, so nothing else is under it
                dropped = self._files.pop(name.lower(), None)
                if dropped is not None:
                    dropped[3].close()
                _host.run(self.connection, {'op': 'drop', 'kind': 'view' if view else 'table',
                                            'target': _host.name(name, schema='main')})
                raise

    def keep_file(self, name: str, model: str, view: bool = False) -> None:
        """The table a page's file was read into, or the view a remote file is read through (``reading_file``'s name),
        and the model DataCube's code wrote for it: served beside the frames' from now on."""
        held = compiler.HeldModel(model)
        with self._lock:
            before = self._files.get(name.lower())
            self._files[name.lower()] = (name, model, view, held)
        if before is not None:
            before[3].close()

    def file_table(self, name: str) -> str | None:
        """The table a page's file was read into, by the name a page gives it, as Frames named it -- or None when no
        file's table has that name (a page drops no other; a remote file's view is not let go of this way)."""
        if not _NAME.match(name):
            return None
        with self._lock:
            kept = self._files.get(name.lower())
            return None if kept is None or kept[2] else kept[0]

    def forget_file(self, name: str) -> None:
        """A page's file let go of, its table dropped (by DataCube's code, inside ``holding``): its model served no more."""
        with self._lock:
            gone = self._files.pop(name.lower(), None)
        if gone is not None:
            gone[3].close()

    @contextlib.contextmanager
    def holding(self) -> Iterator[None]:
        """The frames held still while another thread's code uses the connection (DataCube's, dropping a file's table)."""
        with self._lock:
            yield

    def _taken(self, name: str) -> bool:
        """Whether a file's table may not take ``name``: a frame's, a file's, or any table or view in the database."""
        key = name.lower()
        if key in self._tables or key in self._files:
            return True
        # this database's own and its temporary ones, which a bare name reads: a project's database attached beside it
        # (C5) holds none of the names a file may take
        return _host.count(self.connection, {'op': 'nameTaken', 'name': name}) > 0

    def taken(self, name: str) -> bool:
        """Whether ``name`` is a frame's or a page's file's table: a frame of that name cannot be registered."""
        with self._lock:
            return name.lower() in self._tables or name.lower() in self._files

    def _exists(self, name: str) -> bool:
        return _host.count(self.connection, {'op': 'relationExists', 'name': _host.name(name, schema='main')}) > 0

    def unregister(self, name: str) -> None:
        """Takes a frame's table out of the database; its handle stops working."""
        with self._lock:
            self._tables.pop(name.lower())._drop()

    def close(self) -> None:
        """Takes every frame's table, and every page's file's, out of the database (the connection itself stays open)."""
        with self._lock:
            for table in self._tables.values():
                table._drop()
            self._tables.clear()
            files, self._files = list(self._files.values()), {}
            try:
                for name, _model, view, _held in files:
                    _host.run(self.connection, {'op': 'drop', 'kind': 'view' if view else 'table',
                                                'target': _host.name(name, schema='main')})
            finally:
                # every model let go, a drop that failed or not, and every model a page sent
                for _name, _model, _view, held in files:
                    held.close()
                admitted, self._admitted = list(self._admitted.values()), {}
                for held, _tables in admitted:
                    held.close()

    def __getitem__(self, name: str) -> Table:
        return self._tables[name.lower()]

    def __contains__(self, name: str) -> bool:
        return name.lower() in self._tables
