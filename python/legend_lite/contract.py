"""DATACUBE'S ENGINE CONTRACT, ANSWERED BY PYTHON (docs/DATACUBE_ENGINE_CONTRACT_2026_11_12.md, ratified by the user
2026-11-13; engine-client's engine-contract.ts is its other half): ONE intent API in, typed answers out. DataCube sends
Pure and choices; Python's planner plans, its runner runs, its database answers. No SQL, plan, path, connection or
secret crosses (R3, R4, R5); a call this engine does not do is refused BY NAME (R6, G7).

One implementation behind every transport: engine.py routes ``/contract/v1/<call>`` here, from the V8 bridge (headless
calls.ts ``engine``: DataCube's code run in this process, over the read its call was made in) and, at step 5, from the
page over HTTP.

Wire: each call's body is JSON. A small answer is JSON (200). ``query`` answers Arrow IPC (200), its contract metadata in
the schema's (``legend.contract``: the execution id, the schema as the compiler typed it, the explain text, the receipt)
-- never the plan. A failure is JSON ``{"error": {kind, call, message, executionId?, what?, engine?, where?}}`` with a
4xx status, its kind the contract's (``compile``, ``run``, ``refused``)."""

from __future__ import annotations

import base64
import hashlib
import json
import threading
import uuid
from pathlib import Path
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
import pyarrow.ipc

from . import compiler
from ._types import UnknownType, plain_type

# the contract's version, as capabilities() states it (engine-contract.ts ENGINE_CONTRACT_VERSION)
VERSION = 1

# the path every call is routed under, its call's name after it
PATH = '/contract/v1/'

# the metadata key the query answer's contract facts ride under (never the plan: R4)
META = b'legend.contract'

ENGINE_NAME = 'Python'


@dataclass(frozen=True)
class Said:
    """A contract answer, as HTTP would carry it: its status, media type and body."""

    status: int
    content_type: str
    body: bytes


class ContractError(Exception):
    """A contract failure: its kind, the call, its words, and what else its kind carries."""

    def __init__(self, kind: str, call: str, message: str, **extra: Any) -> None:
        super().__init__(message)
        self.kind = kind
        self.call = call
        self.message = message
        self.extra = {k: v for k, v in extra.items() if v is not None}

    def said(self) -> Said:
        status = {'compile': 422, 'run': 422, 'refused': 403}.get(self.kind, 500)
        error = {'kind': self.kind, 'call': self.call, 'message': self.message, **self.extra}
        return Said(status, 'application/json', json.dumps({'error': error}).encode('utf-8'))


def refused(call: str, what: str, why: str, **extra: Any) -> ContractError:
    """A refusal by name: ``what`` was asked of ``call``, and why this engine does not do it."""
    return ContractError('refused', call, f'{ENGINE_NAME} refuses {what}: {why}', what=what, engine=ENGINE_NAME, **extra)


# what this engine does, by the contract's names (engine-contract.ts Capabilities): step 1 (2026-11-13), the reading
# calls, typeOf and query; step 2, write; step 3, sources behind handles, describe, drop and bindSecret.
CAPABILITIES: dict[str, Any] = {
    'contract': VERSION,
    'engine': {'name': ENGINE_NAME, 'databaseType': 'DuckDB'},
    'calls': {'typeOf': True, 'query': True, 'cancel': True, 'describe': True, 'drop': True,
              'parse': True, 'print': True, 'elements': True, 'bindSecret': True, 'modelOf': True,
              # step 3 (2026-11-13): sources, behind handles this engine issues
              'addSource': {'sources': ['upload', 'held', 'url', 'remote', 'query'], 'how': ['live', 'snap']},
              # step 2 (2026-11-13): DataCube's five table-output modes (Q10), one call, the engine's transaction
              'write': {'modes': ['create', 'overwrite', 'append', 'deleteAppend', 'merge']}},
}

# A write's modes as the contract names them (engine-contract.ts WriteMode), and merge's keys
_WRITE_MODES = frozenset({'create', 'overwrite', 'append', 'deleteAppend'})


# The kinds of element a model DataCube sends may not hold (R3a (a)): the engine owns connections and runtimes. Read off
# the PARSED model (its protocol JSON's ``_type``), never its text.
OWNED_KINDS = frozenset({'connection', 'runtime'})

# the parser's own index of a text's sections, which no model holds
_SECTION_INDEX = 'sectionIndex'


@dataclass(frozen=True)
class OwnRuntime:
    """A runtime this engine owns (R3a (b)): its name, and the Connection and Runtime elements it adds before planning --
    as Pure text, and as the whole model text it was written in beside the source's (so a model it handed DataCube plans
    as the very model it was served as)."""

    name: str
    text: str


@dataclass(frozen=True)
class _Source:
    """A source a session added, behind the handle it issued: the table (or view) it is in, the model the engine wrote
    over it (whole, and as DataCube is handed it), its runtime by name, and the relation that reads it."""

    table: str
    view: bool
    model: str
    handed: str
    runtime: str
    relation: Any


@dataclass
class _Session:
    """A session: the sources it added, by the handles it issued, and the secrets bound to it by name (Q13) -- both
    dropped when it closes."""

    handles: dict[str, _Source] = field(default_factory=dict)
    secrets: dict[str, dict[str, Any]] = field(default_factory=dict)
    # the runtimes of the models modelOf wrote for it, let go when it closes
    together: list[tuple[str, str]] = field(default_factory=list)


# a secret's fields, as the hosts' runner binds them (host-operations.ts SecretField)
SECRET_FIELDS = frozenset({'KEY_ID', 'SECRET', 'SESSION_TOKEN', 'REGION', 'ENDPOINT', 'URL_STYLE', 'USE_SSL', 'URL'})

# a file's format, by its name's ending
_FORMATS = {'.csv': 'csv', '.parquet': 'parquet', '.json': 'json'}


class EngineSide:
    """WHAT THE DOOR ASKS OF THE ENGINE IT IS PART OF (engine.py's): a plan request planned and run over a read (its
    plan-and-run); whether a table is one of its own (a frame's, a file's, a copy's); a table's columns as its catalog
    says them; and a write's plan run in the plan's own transaction, in no read."""

    def plan_and_run(self, body: str, over: Any) -> Any:
        raise NotImplementedError

    def owns(self, table: str) -> bool:
        raise NotImplementedError

    def catalog(self, schema: str | None, table: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    def write(self, plan: Any, execution: str) -> str:
        """``plan`` run in its own transaction, any stamp it reads bound to ``execution``, its door's id."""
        raise NotImplementedError

    def add_file(self, path: str, name: str, digest: str, size: int, file_format: str) -> dict[str, Any]:
        """A file this engine holds read into a table of its own (a planned fileLoad): its model, runtime, source and
        table."""
        raise NotImplementedError

    def mount(self, asked: str, request: dict[str, Any], secrets: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
        """A remote file mounted as a view (a planned remoteMount), what it binds by name bound from ``secrets``: its
        model, runtime, source and view."""
        raise NotImplementedError

    def snap(self, model: str, runtime: str, query: Any) -> dict[str, Any]:
        """A query's rows copied into a table of its own (a planned snapCopy): its model, runtime, source and table, and
        its rows' count (``rowCount``, Q26: the pull's size(), planned and run where the rows are)."""
        raise NotImplementedError

    def drop(self, table: str, view: bool) -> None:
        """A source's table or view dropped, its model served no more."""
        raise NotImplementedError

    def spool(self, name: str, data: bytes) -> str:
        """A person's upload kept as a file this engine holds: its path, never handed out."""
        raise NotImplementedError

    def project_model(self, group: str, artifact: str, version: str) -> str | None:
        """A project's version as one model text, resolved by this engine's own model home (Q19): its files and its
        dependency closure's -- or None, no such version."""
        raise NotImplementedError

    def revision_model(self, project: str, workspace: str, revision: str) -> str | None:
        """A project's workspace at a revision as one model text, resolved by this engine's own model home's SDLC (Q23):
        its files joined by the project-text rule -- or None, no such project, workspace or revision."""
        raise NotImplementedError


class ContractDoor:
    """THE CONTRACT'S DOOR ON PYTHON: each call answered by name; a query's run given its execution id HERE, once per
    plan run (Q7: the door mints, exec binds, planning holds none; ``ArchitectureTest.theServersMintsAreItsHostDoorsAlone``
    the Java twin), carried in its answer and in every failure of it."""

    def __init__(self, side: EngineSide) -> None:
        # engine.py's side of it (EngineSide): its plan-and-run, its own tables, its catalog, its write runner
        self._side = side
        self._plan_and_run = side.plan_and_run
        self._sessions: dict[str, _Session] = {}
        self._lock = threading.Lock()
        # the runtimes this engine owns, by name (R3a (b)); and each model it handed DataCube, by its text and runtime,
        # to the whole model it was written as (c)
        self._runtimes: dict[str, OwnRuntime] = {}
        self._served: dict[tuple[str, str], str] = {}
        # the files this engine holds, by the handles it issued for them (hold)
        self._held: dict[str, str] = {}
        # the libraries checked, by their digest (Q17)
        self._libraries: set[str] = set()

    # ---- R3a: the engine owns its runtimes

    def served(self, model: str, runtime: str) -> tuple[str, str]:
        """A model this engine wrote (a frame's, a file's: the planner's ``table_model``) as DataCube is handed it (R3a
        (c)): its Connection and Runtime kept here as the engine's own, the rest answered as Pure text (printed from its
        parsed elements), with the runtime by its name. ``(model, runtime)``.

        The engine's own part is the planner's text's ``###Connection`` and ``###Runtime`` sections, verified on their
        parsed elements to be exactly the model's connections and runtimes (the platform's printer prints no DuckDB
        connection, so it is never re-printed): this splits the ENGINE's own text, never one DataCube sent."""
        elements = [e for e in compiler.lite_elements(model) if e.get('_type') != _SECTION_INDEX]
        owned = sorted(_path(e) for e in elements if e.get('_type') in OWNED_KINDS)
        if runtime not in owned:
            raise ValueError(f"the model declares no runtime '{runtime}' for this engine to own")
        own = _sections(model, ('Connection', 'Runtime'))
        mine = sorted(_path(e) for e in compiler.lite_elements(own) if e.get('_type') != _SECTION_INDEX)
        if mine != owned:
            raise ValueError(f"the model's Connection and Runtime sections hold {mine}, its elements {owned}")
        handed = compiler.model_text([e for e in elements if e.get('_type') not in OWNED_KINDS])
        with self._lock:
            self._runtimes[runtime] = OwnRuntime(runtime, own)
            self._served[(handed, runtime)] = model
        return handed, runtime

    def _model_for(self, ask: Mapping[str, Any], call: str) -> str:
        """The model a QueryAsk plans over (R3a): refused, by name, where its parsed elements hold a Connection or a
        Runtime, or its runtime is none this engine owns; else the model this engine served as that text, or the text
        with the engine's own runtime added."""
        try:
            elements = compiler.lite_elements(ask['model'])
        except compiler.LegendError as failure:
            raise ContractError('compile', call, failure.message) from None
        owned = sorted({f"a {e.get('_type')} '{_path(e)}'" for e in elements if e.get('_type') in OWNED_KINDS})
        if owned:
            raise refused(call, f'a model declaring {owned[0]}', 'a model DataCube sends holds classes, stores and mappings'
                          ' only: the engine owns its connections and runtimes (R3a)')
        project = (ask.get('how') or {}).get('project')
        if project is not None:
            return self._with_library(self._project_model(ask, project, elements, call), ask, call)
        with self._lock:
            runtime = self._runtimes.get(ask['runtime'])
            whole = self._served.get((ask['model'], ask['runtime']))
        if runtime is None:
            raise refused(call, f"the runtime '{ask['runtime']}'", 'it is no runtime this engine owns')
        return self._with_library(whole if whole is not None else f"{ask['model']}\n{runtime.text}", ask, call)

    def _with_library(self, model: str, ask: Mapping[str, Any], call: str) -> str:
        library = ask.get('library')
        if library is None:
            return model
        if not isinstance(library, str):
            raise refused(call, 'a library that is not text', "a library is Pure text, its functions and enumerations alone")
        self._library_checked(library, call)
        return f'{model}\n{library}'

    def _project_text(self, project: Mapping[str, str], call: str) -> str:
        """A project's model text by its coordinates: a version through Depot (Q19), a workspace's revision through SDLC
        (Q23) -- refused by name where the model home holds none."""
        if _version(project):
            coords = f"{project['groupId']}:{project['artifactId']}:{project['versionId']}"
            resolve = lambda: self._side.project_model(project['groupId'], project['artifactId'], project['versionId'])
            none = 'its model home holds no such version'
        else:
            coords = f"{project['project']}/{project['workspace']}@{project['revision']}"
            resolve = lambda: self._side.revision_model(project['project'], project['workspace'], project['revision'])
            none = 'its model home holds no such project, workspace or revision'
        try:
            text = resolve()
        except Exception as failure:
            raise refused(call, f"the project {coords}", f'its model home did not resolve it: {failure}') from None
        if text is None:
            raise refused(call, f"the project {coords}", none)
        return text

    def _project_model(self, ask: Mapping[str, Any], project: Mapping[str, str], added: list[dict[str, Any]],
                       call: str) -> str:
        """A project's model, resolved by its coordinates (Q19), with what DataCube adds after it: the query's runtime
        one of the project's (the engine owns it by its path), its mapping the project's or DataCube's own."""
        text = self._project_text(project, call)
        try:
            elements = compiler.lite_elements(text)
        except compiler.LegendError as failure:
            raise ContractError('compile', call, failure.message) from None
        runtimes = {_path(e) for e in elements if e.get('_type') == 'runtime'}
        if ask['runtime'] not in runtimes:
            raise refused(call, f"the runtime '{ask['runtime']}'", 'it is no runtime of the project')
        mapping = (ask.get('how') or {}).get('mapping')
        mappings = {_path(e) for e in [*elements, *added] if e.get('_type') == 'mapping'}
        if mapping is not None and mapping not in mappings:
            raise refused(call, f"the mapping '{mapping}'", 'it is no mapping of the project, nor one the query adds')
        return f"{text}\n{ask['model']}" if ask['model'].strip() else text

    def _library_checked(self, library: str, call: str) -> None:
        """DataCube's Pure library (Q17), on its PARSED elements: functions and enumerations alone (Q27), read in lite's dialect, held by its digest once checked."""
        digest = hashlib.sha256(library.encode('utf-8')).hexdigest()
        with self._lock:
            if digest in self._libraries:
                return
        try:
            elements = compiler.lite_elements(library)
        except compiler.LegendError as failure:
            raise ContractError('compile', call, failure.message) from None
        other = sorted({f"a {e.get('_type')} '{_path(e)}'" for e in elements if e.get('_type') not in _LIBRARY_KINDS})
        if other:
            raise refused(call, f'a library declaring {other[0]}', 'a library holds functions and enumerations alone (Q17)')
        with self._lock:
            self._libraries.add(digest)

    # ---- the door

    def answer(self, call: str, body: str, over: Any = None) -> Said:
        """``call`` (the path's last part) answered over ``body`` (JSON) -- ``over``, the read of the frames it was made
        in (D15), where a call reads the database. Never raises: a failure is a ContractError's JSON, every secret its
        session holds taken out of its words (Q13: nothing echoes a secret, the database's own words included)."""
        asked: Any = {}
        try:
            asked = json.loads(body) if body else {}
            if not isinstance(asked, dict):
                raise refused(call if call in _CALLS else 'capabilities', 'a call whose body is not a JSON object',
                              'each call is asked as JSON')
            handler = _CALLS.get(call)
            if handler is None:
                raise refused('capabilities', f"the call '{call}'", 'the contract names no such call')
            return handler(self, asked, over)
        except ContractError as failure:
            return self._redacted(failure, asked).said()

    def _redacted(self, failure: ContractError, asked: Any) -> ContractError:
        name = asked.get('session') if isinstance(asked, dict) else None
        with self._lock:
            session = self._sessions.get(name) if isinstance(name, str) else None
            values = sorted({str(v) for s in (session.secrets.values() if session else ()) for v in s.values()
                             if isinstance(v, str) and v}, key=len, reverse=True)
        if not values:
            return failure

        def clean(text: str) -> str:
            for v in values:
                text = text.replace(v, '<redacted>')
            return text
        return ContractError(failure.kind, failure.call, clean(failure.message),
                             **{k: clean(v) if isinstance(v, str) else v for k, v in failure.extra.items()})

    # ---- the files this engine holds (Q12)

    def hold(self, path: str) -> dict[str, str]:
        """A file this engine holds, given a handle DataCube's code may name it by (``{held: handle}``): the handle
        only this engine resolves -- never a path handed out."""
        handle = f'held-{uuid.uuid4().hex}'
        with self._lock:
            self._held[handle] = str(path)
        return {'handle': handle}

    # ---- sessions

    def _open(self, asked: Mapping[str, Any], over: Any) -> Said:
        session = f'session-{uuid.uuid4().hex}'
        with self._lock:
            self._sessions[session] = _Session()
        return _json({'session': session})

    def _close(self, asked: Mapping[str, Any], over: Any) -> Said:
        """A session closed: what it made dropped -- each source's table or view -- and its secrets let go of."""
        with self._lock:
            session = self._sessions.pop(_session_of(asked, 'close'), None)
        if session is not None:
            session.secrets.clear()
            with self._lock:
                for handed, runtime in session.together:
                    self._served.pop((handed, runtime), None)
                    self._runtimes.pop(runtime, None)
            for source in session.handles.values():
                self._forget(source)
            session.handles.clear()
        return _json({})

    def _forget(self, source: _Source) -> None:
        self._side.drop(source.table, source.view)
        with self._lock:
            self._served.pop((source.handed, source.runtime), None)
            self._runtimes.pop(source.runtime, None)

    def _session(self, asked: Mapping[str, Any], call: str) -> _Session:
        name = _session_of(asked, call)
        with self._lock:
            session = self._sessions.get(name)
        if session is None:
            raise refused(call, f"session '{name}'", 'it is no session this engine opened, or it was closed')
        return session

    # ---- the calls

    def _capabilities(self, asked: Mapping[str, Any], over: Any) -> Said:
        return _json(CAPABILITIES)

    def _cancel(self, asked: Mapping[str, Any], over: Any) -> Said:
        # best effort: a call on Python runs to its end once begun (the contract allows it); an unknown request is no error
        self._session(asked, 'cancel')
        return _json({})

    def _parse(self, asked: Mapping[str, Any], over: Any) -> Said:
        self._session(asked, 'parse')
        text = _text(asked, 'text', 'parse')
        return _json({'lambda': _compiled('parse', lambda: compiler.parse(text))})

    def _print(self, asked: Mapping[str, Any], over: Any) -> Said:
        self._session(asked, 'print')
        style = asked.get('style', 'PRETTY')
        if style not in ('PRETTY', 'COMPACT'):
            raise refused('print', f"the style '{style}'", 'a query is printed PRETTY or COMPACT')
        query = asked.get('query')
        return _json({'text': _compiled('print', lambda: compiler.print_tree(query, 'PRETTY' if style == 'PRETTY' else 'STANDARD'))})

    def _elements(self, asked: Mapping[str, Any], over: Any) -> Said:
        """A model's elements -- its text's, or a project's resolved by its coordinates (Q19), its connections and
        runtimes left out (the engine's)."""
        self._session(asked, 'elements')
        project = asked.get('project')
        if project is None:
            model = _text(asked, 'model', 'elements')
            return _json({'elements': _compiled('elements', lambda: compiler.model_elements(model))})
        if not _coordinates(project):
            raise refused('elements', 'a project that is not its coordinates', "a project is a version's groupId, "
                          "artifactId and versionId, or a workspace's project, workspace and revision")
        text = self._project_text(project, 'elements')
        elements = _compiled('elements', lambda: compiler.model_elements(text))
        return _json({'elements': [e for e in elements if e.get('_type') not in OWNED_KINDS]})

    def _type_of(self, asked: Mapping[str, Any], over: Any) -> Said:
        self._session(asked, 'typeOf')
        ask = _query_ask(asked, 'typeOf')
        model = self._model_for(ask, 'typeOf')
        columns = _compiled('typeOf', lambda: compiler.relation_type(model, ask['query']))
        return _json({'columns': [_column(c, 'typeOf') for c in columns]})

    def _query(self, asked: Mapping[str, Any], over: Any) -> Said:
        self._session(asked, 'query')
        ask = _query_ask(asked, 'query')
        model = self._model_for(ask, 'query')
        # THE MINT: one execution id for this plan run (Q7), its answer's and every failure's -- a UUID's canonical text,
        # the shape an execution's trace comment carries (Java's ExecutionPlan.traceComment)
        execution = str(uuid.uuid4())
        request = {'lambda': json.dumps(ask['query']), 'model': model, 'runtime': ask['runtime'],
                   # a version's rows are its project's own Data (C5); a workspace revision's read this engine's tables
                   **({'project': True} if _version((ask.get('how') or {}).get('project') or {}) else {})}
        said = self._plan_and_run(json.dumps(request), over)
        if said.content_type == 'application/vnd.apache.arrow.stream' and said.status == 200:
            return _rows(said.body, execution)
        if said.content_type == 'application/json':
            answered = json.loads(said.body)
            try:
                plan = compiler.read_planned(answered['plan'])
            except compiler.LegendError as failure:
                raise ContractError('compile', 'query', failure.message, executionId=execution) from None
            raise ContractError('run', 'query', answered.get('error', 'the database refused it'), executionId=execution,
                                explain=str(plan.sql))
        raise ContractError('run', 'query', said.body.decode('utf-8', 'replace'), executionId=execution)


def _write(door: ContractDoor, asked: Mapping[str, Any], over: Any) -> Said:
    """WRITE, ONE CALL (step 2; the contract's R2): a box's rows into a table of this engine's database, by one of
    DataCube's five modes -- planned as ONE plan (Boundary.outputPlan: the table made where the mode makes it, then the
    write) and run in the plan's own transaction. The box's tables are DERIVED here from its model's Databases, never
    sent (Q11 (a)); a target that is one of this engine's own tables is refused by name (Q11 (b)); a write made in a read
    of the frames is refused (a read sees the database as it was). Its execution id minted here, once per plan run."""
    door._session(asked, 'write')
    w = asked.get('ask')
    if not isinstance(w, dict) or set(w) != {'box', 'target', 'mode'}:
        raise refused('write', 'a write that is not a box, a target and a mode', 'write is asked a WriteAsk')
    if over is not None:
        raise refused('write', 'a write made in a read of the frames', 'a read sees the database as it was when it '
                      'began: a write is made in no read')
    box = _query_ask({'ask': w['box']}, 'write')
    model = door._model_for(box, 'write')
    target = w['target'].get('table') if isinstance(w['target'], dict) else None
    if not isinstance(target, str) or not target:
        raise refused('write', 'a write with no target table', "its target is a table's name")
    mode, keys = _mode_of(w['mode'])
    if door._side.owns(target):
        raise refused('write', f"the table '{target}'", "it is one of this engine's own tables (a frame's, a file's, a "
                      "copy's): a write names another")
    tables = [{**t, 'convertible': True, 'columns': door._side.catalog(t.get('schema'), t['table'])}
              for t in _tables_of(model)]
    present = door._side.catalog(None, target)
    request = {'databaseType': 'DuckDB', 'tables': tables,
               'output': {'name': target, 'mode': mode, **({'keys': keys} if mode == 'merge' else {})},
               'rows': json.dumps(box['query']),
               # a table there already is named by its columns; none, the planner refuses the write by name
               **({'target': present} if present and mode not in ('create', 'overwrite') else {})}
    # THE MINT: one execution id for this plan run (Q7), its answer's and every failure's; bound where the plan reads
    # its stamp (step 3b)
    execution = str(uuid.uuid4())
    try:
        plan = compiler.output_plan(request)
    except compiler.LegendError as failure:
        raise ContractError('compile', 'write', failure.message, executionId=execution) from None
    try:
        answer = door._side.write(plan, execution)
    except ContractError:
        raise
    except Exception as failure:
        raise ContractError('run', 'write', str(failure), executionId=execution) from None
    rows = json.loads(answer)
    count = rows[0].get('value') if isinstance(rows, list) and len(rows) == 1 and isinstance(rows[0], dict) else None
    if not isinstance(count, int) or isinstance(count, bool):
        raise ContractError('run', 'write', f'the write answered no count: {answer}', executionId=execution)
    return _json({'rowsWritten': count, 'executionId': execution})


def _mode_of(mode: Any) -> tuple[str, list[str]]:
    if isinstance(mode, str) and mode in _WRITE_MODES:
        return mode, []
    keys = mode.get('merge', {}).get('keys') if isinstance(mode, dict) and set(mode) == {'merge'} \
        and isinstance(mode['merge'], dict) else None
    if isinstance(keys, list) and keys and all(isinstance(k, str) for k in keys):
        return 'merge', keys
    raise refused('write', f'the mode {json.dumps(mode)}', 'a write is create, overwrite, append, deleteAppend, or merge by '
                  'its keys')


def _tables_of(model: str) -> list[dict[str, Any]]:
    """The tables a model's Databases declare, each with its Database's package and its schema (none for the default),
    read off its PARSED elements (Q11 (a))."""
    out: list[dict[str, Any]] = []
    for e in compiler.lite_elements(model):
        if e.get('_type') != 'relational':
            continue
        for schema in e.get('schemas') or []:
            for t in schema.get('tables') or []:
                out.append({'table': t['name'], 'pkg': e.get('package') or 'local',
                            **({'schema': schema['name']} if schema.get('name') not in (None, 'default') else {})})
    return out


def _bind_secret(door: ContractDoor, asked: Mapping[str, Any], over: Any) -> Said:
    """A secret held under its name for this session (Q13): WRITE-ONLY -- it answers nothing and nothing ever echoes it
    back; its fields as the hosts' runner binds them; dropped when the session closes. A plan names it alone."""
    session = door._session(asked, 'bindSecret')
    name, fields = asked.get('name'), asked.get('fields')
    if not isinstance(name, str) or not name.replace('_', '').isalnum():
        raise refused('bindSecret', 'a secret with no plain name', "a secret is named by letters, digits and '_'")
    if not isinstance(fields, dict) or not fields:
        raise refused('bindSecret', f"the secret '{name}' with no field", 'a secret is held as its fields')
    for k, v in fields.items():
        if k not in SECRET_FIELDS or not isinstance(v, (str, bool)):
            # the field's name alone, never its value
            raise refused('bindSecret', f"the field '{k}' of the secret '{name}'",
                          f"a secret's fields are {', '.join(sorted(SECRET_FIELDS))}, each text or a boolean")
    session.secrets[name] = dict(fields)
    return Said(204, 'application/json', b'')


def _add_source(door: ContractDoor, asked: Mapping[str, Any], over: Any) -> Said:
    """A SOURCE ADDED (step 3): an upload, a file this engine holds by its handle, a URL, a remote file by its secret's
    name, or a query's rows -- planned and run here (a fileLoad, a remoteMount, a snapCopy), the model over it written
    by this engine (Q3) and handed DataCube as R3a has it, behind a handle this session issued. Its execution id minted
    once for the plan run; in no read of the frames."""
    session = door._session(asked, 'addSource')
    source, how = asked.get('source'), asked.get('how')
    if over is not None:
        raise refused('addSource', 'a source added in a read of the frames', 'a read sees the database as it was when '
                      'it began: a source is added in no read')
    if not isinstance(source, dict) or len(source) != 1 or how not in ('live', 'snap'):
        raise refused('addSource', 'a source that is not one SourceAsk and how', 'addSource is asked a source and how')
    (kind, given), = source.items()
    execution = str(uuid.uuid4())
    digest = size = None
    try:
        if kind in ('upload', 'held'):
            if how != 'snap':
                raise refused('addSource', f"a file added '{how}'", "a file is read into a table of this engine's own "
                              '(snap)')
            if kind == 'upload':
                name, data = given.get('name'), given.get('bytes')
                if not isinstance(name, str) or not isinstance(data, str):
                    raise refused('addSource', 'an upload with no name or bytes', 'an upload is its name and bytes')
                raw = base64.b64decode(data)
                path = door._side.spool(name, raw)
            else:
                handle = given.get('handle') if isinstance(given, dict) else None
                with door._lock:
                    path = door._held.get(handle) if isinstance(handle, str) else None
                if path is None:
                    raise refused('addSource', f"the handle {json.dumps(handle)}", 'it is no file this engine holds')
                raw = Path(path).read_bytes()
                name = Path(path).name
            file_format = _FORMATS.get(Path(name).suffix.lower())
            if file_format is None:
                raise refused('addSource', f"the file '{name}'", f"a file is read as {', '.join(sorted(_FORMATS))}")
            digest, size = hashlib.sha256(raw).hexdigest(), len(raw)
            made = door._side.add_file(path, name, digest, size, file_format)
        elif kind in ('url', 'remote'):
            if how != 'live':
                raise refused('addSource', f"a remote file added '{how}'", "a remote file is mounted live; its rows are "
                              "snapped as a query's ({query})")
            made = door._side.mount(*_mount_of(session, kind, given))
        elif kind == 'query':
            if how != 'snap':
                raise refused('addSource', f"a query's rows added '{how}'", "a query's rows are added as a snap alone")
            pull = _query_ask({'ask': given}, 'addSource')
            made = door._side.snap(door._model_for(pull, 'addSource'), pull['runtime'], pull['query'])
        else:
            raise refused('addSource', f"a source of kind '{kind}'", 'a source is an upload, a held file, a url, a '
                          'remote file or a query')
    except ContractError:
        raise
    except compiler.LegendError as failure:
        raise ContractError('compile', 'addSource', failure.message, executionId=execution) from None
    except Exception as failure:
        raise ContractError('run', 'addSource', f'{type(failure).__name__}: {failure}', executionId=execution) from None
    handed, runtime = door.served(made['model'], made['runtime'])
    handle = f'h-{uuid.uuid4().hex}'
    session.handles[handle] = _Source(made['table'], kind in ('url', 'remote'), made['model'], handed, runtime,
                                      made['source'])
    return _json({'handle': {'handle': handle}, 'name': made['table'], 'model': handed, 'runtime': runtime,
                  'relation': made['source'], 'executionId': execution,
                  **({'digest': digest, 'size': size} if digest is not None else {}),
                  # a query's rows copied: their count, the engine's own (Q26); no other kind's -- never a guessed 0
                  **({'rowCount': made['rowCount']} if kind == 'query' else {})})


def _mount_of(session: _Session, kind: str, given: Any) -> tuple[str, dict[str, Any], dict[str, dict[str, Any]]]:
    """A URL's or a remote file's mount request, and the secrets it binds by name (Q13: the plan names them alone)."""
    if kind == 'url':
        url = given if isinstance(given, str) else None
        if url is None:
            raise refused('addSource', 'a url that is not text', 'a url source is its address')
        return 'remote', {'location': {'url': url}, 'format': _remote_format(url)}, {}
    if not isinstance(given, dict) or set(given) != {'kind', 'ref', 'secret'} or given['kind'] not in ('bearer', 's3'):
        raise refused('addSource', 'a remote source that is not its kind, ref and secret',
                      "a remote source is a bearer URL or an s3 object, and its secret's name")
    secret = session.secrets.get(given['secret'])
    if secret is None:
        raise refused('addSource', f"the secret '{given['secret']}'", 'no secret of that name is bound to this session')
    ref = given['ref']
    if given['kind'] == 'bearer':
        if set(secret) != {'URL'}:
            raise refused('addSource', f"the secret '{given['secret']}' for a bearer URL", 'a bearer URL is held as its '
                          'field URL alone')
        request = {'location': {'bearer': given['secret'], 'shown': ref}, 'format': _remote_format(ref)}
    else:
        request = {'location': {'url': ref}, 'format': _remote_format(ref),
                   'credentials': {'name': given['secret'], 'fields': sorted(secret)}}
    return 'remote', request, {given['secret']: secret}


def _remote_format(url: str) -> str:
    path = url.partition('?')[0].lower()
    for ending, fmt in (('.csv', 'csv'), ('.parquet', 'parquet')):
        if path.endswith(ending):
            return fmt
    raise refused('addSource', f"the remote file {url.partition('?')[0]!r}", 'a remote file is read as csv or parquet')


def _source_of(door: ContractDoor, asked: Mapping[str, Any], call: str) -> tuple[_Session, str, _Source]:
    session = door._session(asked, call)
    given = asked.get('handle')
    handle = given.get('handle') if isinstance(given, dict) else None
    source = session.handles.get(handle) if isinstance(handle, str) else None
    if source is None:
        raise refused(call, f'the handle {json.dumps(handle)}', 'it is no handle this session issued, or it was dropped')
    return session, handle, source


def _model_of(door: ContractDoor, asked: Mapping[str, Any], over: Any) -> Said:
    """ONE MODEL OVER SEVERAL SOURCES (Q15; compile-only): the tables behind ``handles``, each its session's, written
    by this engine as one model with one runtime it owns -- each relation in the order given. A handle not this
    session's, or a source given twice, refused by name; never split."""
    session = door._session(asked, 'modelOf')
    handles = asked.get('handles')
    if not isinstance(handles, list) or not handles:
        raise refused('modelOf', 'a modelOf of no handle', 'modelOf is asked the handles of the sources read together')
    sources = []
    for h in handles:
        handle = h.get('handle') if isinstance(h, dict) else None
        source = session.handles.get(handle) if isinstance(handle, str) else None
        if source is None:
            raise refused('modelOf', f'the handle {json.dumps(handle)}', 'it is no handle this session issued, or it was '
                          'dropped')
        if source in sources:
            raise refused('modelOf', f'the handle {json.dumps(handle)} given twice', 'each source is read once')
        sources.append(source)
    tables = [{'table': s.table, 'pkg': s.table, 'convertible': not s.view,
               'columns': door._side.catalog(None, s.table)} for s in sources]
    # a connection of its own name: so the runtime is this model's alone
    connection = f'together_{uuid.uuid4().hex[:12]}'
    written = _compiled('modelOf', lambda: compiler.table_model({'databaseType': 'DuckDB', 'connection': connection,
                                                                 'tables': tables}))
    handed, runtime = door.served(written['model'], written['runtime'])
    session.together.append((handed, runtime))
    return _json({'model': handed, 'runtime': runtime, 'relations': [t['source'] for t in written['tables']]})


def _describe(door: ContractDoor, asked: Mapping[str, Any], over: Any) -> Said:
    """A source's columns, as the compiler types the relation that reads it (compile-only)."""
    _, _, source = _source_of(door, asked, 'describe')
    query = {'_type': 'lambda', 'parameters': [], 'body': [source.relation]}
    columns = _compiled('describe', lambda: compiler.relation_type(source.model, query))
    return _json({'columns': [_column(c, 'describe') for c in columns]})


def _drop(door: ContractDoor, asked: Mapping[str, Any], over: Any) -> Said:
    """A source dropped: its table or view, its model served no more, its handle refused from now on."""
    session, handle, source = _source_of(door, asked, 'drop')
    if over is not None:
        raise refused('drop', 'a source dropped in a read of the frames', 'a source is dropped in no read')
    del session.handles[handle]
    try:
        door._forget(source)
    except Exception as failure:
        raise ContractError('run', 'drop', f'{type(failure).__name__}: {failure}') from None
    return _json({})


_CALLS: dict[str, Callable[[ContractDoor, Mapping[str, Any], Any], Said]] = {
    'capabilities': ContractDoor._capabilities, 'open': ContractDoor._open, 'close': ContractDoor._close,
    'cancel': ContractDoor._cancel, 'parse': ContractDoor._parse, 'print': ContractDoor._print,
    'elements': ContractDoor._elements, 'typeOf': ContractDoor._type_of, 'query': ContractDoor._query,
    'write': _write, 'bindSecret': _bind_secret, 'addSource': _add_source, 'describe': _describe, 'drop': _drop,
    'modelOf': _model_of,
}




def _sections(text: str, names: tuple[str, ...]) -> str:
    """The sections of a model's text (each from its ``###Name`` line to the next) named ``names``, in order."""
    out: list[str] = []
    keep = False
    for line in text.splitlines(keepends=True):
        if line.startswith('###'):
            keep = line[3:].strip() in names
        if keep:
            out.append(line)
    return ''.join(out)


def _path(element: Mapping[str, Any]) -> str:
    return f"{element.get('package')}::{element.get('name')}" if element.get('package') else str(element.get('name'))


def _json(value: Any) -> Said:
    return Said(200, 'application/json', json.dumps(value).encode('utf-8'))


def _session_of(asked: Mapping[str, Any], call: str) -> str:
    session = asked.get('session')
    if not isinstance(session, str):
        raise refused(call, 'a call with no session', 'each call but capabilities and open names its session')
    return session


def _text(asked: Mapping[str, Any], key: str, call: str) -> str:
    value = asked.get(key)
    if not isinstance(value, str):
        raise refused(call, f"a call with no '{key}'", f"{call} is asked its {key} as text")
    return value


def _compiled(call: str, work: Callable[[], Any]) -> Any:
    try:
        return work()
    except compiler.LegendError as failure:
        raise ContractError('compile', call, failure.message) from None


_QUERY_KEYS = frozenset({'model', 'runtime', 'query', 'parameters', 'how', 'library'})
# a project's coordinates (Q19, Q23): a version's by Depot, or a workspace's revision by SDLC
_VERSION_KEYS = frozenset({'groupId', 'artifactId', 'versionId'})
_REVISION_KEYS = frozenset({'project', 'workspace', 'revision'})


def _coordinates(project: Any) -> bool:
    """Whether ``project`` is a project's coordinates: a version's or a workspace revision's, each part named text."""
    return isinstance(project, dict) and set(project) in (_VERSION_KEYS, _REVISION_KEYS) \
        and all(isinstance(v, str) and v for v in project.values())


def _version(project: Mapping[str, str]) -> bool:
    """Whether coordinates name a version (its rows the project's own Data, C5) rather than a workspace's revision."""
    return set(project) == _VERSION_KEYS

# a library's elements (Q17, Q27: the orchestrator, 2026-11-13): its functions and its enumerations, nothing else
_LIBRARY_KINDS = frozenset({'function', 'Enumeration', 'sectionIndex'})


def _query_ask(asked: Mapping[str, Any], call: str) -> dict[str, Any]:
    """A QueryAsk (engine-contract.ts), checked: its model as text, its runtime by name, its query as a lambda's protocol
    JSON; anything else asked of it refused by name."""
    ask = asked.get('ask')
    if not isinstance(ask, dict):
        raise refused(call, 'a query with no ask', f'{call} is asked a QueryAsk')
    extra = sorted(set(ask) - _QUERY_KEYS)
    if extra:
        raise refused(call, f"a QueryAsk's '{extra[0]}'", 'a QueryAsk is a model, a runtime, a query, its parameters and how')
    if not isinstance(ask.get('model'), str) or not isinstance(ask.get('runtime'), str):
        raise refused(call, 'a QueryAsk with no model or runtime', 'its model is Pure text and its runtime a name')
    query = ask.get('query')
    if not isinstance(query, dict) or query.get('_type') != 'lambda':
        raise refused(call, 'a QueryAsk whose query is no lambda', "its query is a lambda's protocol JSON")
    if ask.get('parameters'):
        raise refused(call, "a query's parameters", 'this engine plans a query of no parameter (step 1)')
    how = ask.get('how') or {}
    project = how.get('project') if isinstance(how, dict) else None
    if not isinstance(how, dict) or set(how) - {'project', 'mapping'} or not isinstance(how.get('mapping', ''), str) \
            or (project is not None and not _coordinates(project)):
        raise refused(call, f"how a query reads its model ({json.dumps(how)})", "this engine reads a model's own tables, "
                      "through a mapping, over a project by its coordinates (a version's groupId, artifactId and "
                      "versionId, or a workspace's project, workspace and revision)")
    mapping = how.get('mapping')
    if mapping is None:
        return ask
    # through a mapping: the query as Query sends it, `->from(mapping, runtime)` outermost (lite-planner.ts overMapping)
    body = query.get('body') or []
    if len(body) != 1:
        raise refused(call, 'a query through a mapping of more than one expression', 'it reads `->from(mapping, runtime)`'
                      ' over one expression')
    ptr = lambda path: {'_type': 'packageableElementPtr', 'fullPath': path}  # noqa: E731
    return {**ask, 'query': {**query, 'body': [{'_type': 'func', 'function': 'from',
                                                 'parameters': [body[0], ptr(mapping), ptr(ask['runtime'])]}]}}


def _column(c: compiler.Column, call: str) -> dict[str, Any]:
    try:
        return {'name': c.name, 'type': plain_type(c.type)}
    except UnknownType as failure:
        raise ContractError('compile', call, str(failure)) from None


def _rows(arrow: bytes, execution: str) -> Said:
    """Plan-and-run's rows answered as the contract's: the same Arrow, the plan taken out of its metadata (R4) and the
    contract's facts put in -- the execution id, the schema as the compiler typed it, the explain text, the receipt."""
    table = pyarrow.ipc.open_stream(pa.py_buffer(arrow)).read_all()
    meta = dict(table.schema.metadata or {})
    planned = meta.pop(b'legend.plan', None)
    if planned is None:
        raise ContractError('run', 'query', 'the rows came without the plan they are of', executionId=execution)
    plan = compiler.read_planned(planned.decode('utf-8'))
    receipt = meta.pop(b'legend.receipt', None)
    facts = {'executionId': execution, 'schema': [_column(c, 'query') for c in plan.columns], 'explain': str(plan.sql),
             **({'receipt': json.loads(receipt)} if receipt is not None else {})}
    table = table.replace_schema_metadata({**meta, META: json.dumps(facts).encode('utf-8')})
    sink = pa.BufferOutputStream()
    with pyarrow.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return Said(200, 'application/vnd.apache.arrow.stream', sink.getvalue().to_pybytes())
