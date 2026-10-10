"""Dataframes as Legend tables: a frame registered in duckdb-python, its model written by the compiler, and
queries the compiler plans run by DuckDB, answered as Arrow.

    frames = Frames()
    trades = frames.register('trades', df)              # Live: every query reads df as it is then
    trades.execute("->filter(x|$x.desk == 'FX')")       # a pyarrow.Table

A frame is anything Arrow reads: a pandas or polars DataFrame, a pyarrow Table, any object that exports
Arrow -- or a function returning one, so a Live table follows a frame you rebuild. It is handed to DuckDB
AS ARROW: duckdb-python's own pandas reader is about ten times slower on text columns, and reading a
pandas frame through it once does not see later changes (pandas copies on write).

LIVE (the default) re-reads the frame at every query, so the answer is the frame's as it is then; if its
columns changed, the model is written again. SNAPPED copies it into DuckDB once: faster to query, and it
never changes. Either way the frame is an ordinary DuckDB table or view named as registered, so the
compiler reads its catalog and writes its model exactly as it does for a DataCube table
(``table_model``): one writer.
"""

from __future__ import annotations

import contextlib
import re
import sys
import threading
from typing import Any, Callable, Iterator

import duckdb
import pyarrow as pa

from . import compiler

LIVE = 'live'
SNAPPED = 'snapped'

# a frame's name is its DuckDB table and its Legend package: one plain identifier
_NAME = re.compile(r'[A-Za-z_][A-Za-z0-9_]*\Z')
# the plain identifiers Pure reads as literals, so no package can be named by them
_LITERALS = ('true', 'false')

# the catalog question's columns, in its order, as the model writer names them (compiler.catalog_columns_sql)
_CATALOG_FIELDS = ('name', 'dataType', 'logicalType', 'precision', 'scale', 'notNull')


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


def _quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


class Table:
    """A registered frame: its Legend model and the queries over it. Unusable once its name is registered
    again or unregistered."""

    def __init__(self, frames: Frames, name: str, read: Callable[[], Any], mode: str) -> None:
        self._frames = frames
        self.name = name
        self.mode = mode
        self._read = read
        self._registered = f'__legend_lite_{name}'
        self._target = 'main.' + _quoted(name)
        self._schema: pa.Schema | None = None
        self._closed = False
        self.model = ''
        self.runtime = ''
        self.accessor = ''
        self.source: dict[str, Any] = {}
        self.excluded: tuple[str, ...] = ()
        self.columns: tuple[compiler.Column, ...] = ()

    @property
    def reader(self) -> Callable[[], Any]:
        """What it reads its frame from: the function given, or the frame itself (a Snapped one's, as it was given)."""
        return self._read

    def _load(self, arrow: pa.Table) -> None:
        """Puts the frame in DuckDB as the table it is named, reads its catalog, and writes its model."""
        con = self._frames.connection
        con.register(self._registered, arrow)
        kind = 'VIEW' if self.mode == LIVE else 'TABLE'
        # a view whose columns changed is dropped, not replaced: the copy below may change its kind of select
        con.execute(f'DROP {kind} IF EXISTS {self._target}')
        con.execute(f'CREATE {kind} {self._target} AS SELECT * FROM {self._registered}')
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
            select = written['copySelectList']
            source = self._registered if self.mode == LIVE else self._target
            con.execute(f'CREATE OR REPLACE {kind} {self._target} AS SELECT {select} FROM {source}')
        self.model = written['model']
        self.runtime = written['runtime']
        self.source = written['source']
        self.accessor = written['accessor']
        self.excluded = tuple(written['excluded'])
        self.columns = compiler.relation_type(self.model, {'_type': 'lambda', 'body': [self.source], 'parameters': []})
        self._schema = arrow.schema

    def _drop(self) -> None:
        """Takes the frame out of DuckDB, and makes this handle unusable."""
        con = self._frames.connection
        con.execute(f"DROP {'VIEW' if self.mode == LIVE else 'TABLE'} IF EXISTS {self._target}")
        if self.mode == LIVE:
            con.unregister(self._registered)
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

    def _ready(self) -> None:
        if self._closed:
            raise ValueError(f'the frame {self.name!r} was registered again or unregistered: use the new table')
        if self.mode == LIVE:
            self._refresh()

    def plan(self, query: str | dict[str, Any]) -> compiler.Plan:
        """The compiler's plan for a query over this table: Pure text, a tree, or ``->...`` applied to it."""
        with self._frames._lock:
            self._ready()
            return compiler.plan(self.model, self._tree(query), self.runtime)

    def execute(self, query: str | dict[str, Any]) -> pa.Table:
        """Runs a query over this table (as ``plan`` takes it) and answers as an Arrow table."""
        with self._frames._lock:
            self._ready()
            planned = compiler.plan(self.model, self._tree(query), self.runtime)
            return self._frames.connection.execute(planned.sql).to_arrow_table()


class Frames:
    """Frames in one duckdb-python database, each a Legend table the compiler plans queries over.

    The connection's session is set as the planner's SQL expects (the dialect's own setup: UTC), whether
    Frames opened it or was given it. Frames creates and drops only its own tables and views (a Live one
    reads an object registered on the connection, so in a database file it lasts no longer than the
    session: ``close()`` drops them). Use the connection through Frames' methods while other threads do;
    they hold one lock."""

    def __init__(self, connection: duckdb.DuckDBPyConnection | None = None) -> None:
        self.connection = connection if connection is not None else duckdb.connect()
        for statement in compiler.session_setup('DuckDB'):
            self.connection.execute(statement)
        self._lock = threading.RLock()
        # DuckDB matches names without regard to case, quoted or not: so does this
        self._tables: dict[str, Table] = {}

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
            if old is not None:
                old._drop()
            elif self._exists(name):
                raise ValueError(f'the database already has a table or view named {name!r}, and Frames did not make it')
            table = Table(self, name, read, mode)
            try:
                table._load(arrow)
            except BaseException:
                # its old table dropped, the name serves nothing: said so, not left serving a table that is gone
                self._tables.pop(key, None)
                raise
            self._tables[key] = table
            return table

    @contextlib.contextmanager
    def serving(self) -> Iterator[dict[str, str]]:
        """The frames held still for one query that another host plans and runs (the engine): the lock taken,
        each Live table read as its frame is now (its model written again if its columns changed), and their
        models given by name -- the only models such a query may be over. Run its SQL on ``connection`` inside."""
        with self._lock:
            for table in self._tables.values():
                if table.mode == LIVE:
                    table._ready()
            yield {table.name: table.model for table in self._tables.values()}

    def _exists(self, name: str) -> bool:
        found = self.connection.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = current_database() AND schema_name = 'main'"
            " AND lower(table_name) = lower(?)", [name]).fetchone()[0]
        found += self.connection.execute(
            "SELECT count(*) FROM duckdb_views() WHERE database_name = current_database() AND schema_name = 'main'"
            " AND NOT internal AND lower(view_name) = lower(?)", [name]).fetchone()[0]
        return found > 0

    def unregister(self, name: str) -> None:
        """Takes a frame's table out of the database; its handle stops working."""
        with self._lock:
            self._tables.pop(name.lower())._drop()

    def close(self) -> None:
        """Takes every frame's table out of the database (the connection itself stays open)."""
        with self._lock:
            for table in self._tables.values():
                table._drop()
            self._tables.clear()

    def __getitem__(self, name: str) -> Table:
        return self._tables[name.lower()]

    def __contains__(self, name: str) -> bool:
        return name.lower() in self._tables
