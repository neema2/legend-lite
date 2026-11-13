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

import json
import threading
import uuid
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


# what this engine does, by the contract's names (engine-contract.ts Capabilities). Step 1 (2026-11-13): the reading
# calls, typeOf and query; describe, drop, addSource and write follow with steps 2 and 3, refused by name until then.
CAPABILITIES: dict[str, Any] = {
    'contract': VERSION,
    'engine': {'name': ENGINE_NAME, 'databaseType': 'DuckDB'},
    'calls': {'typeOf': True, 'query': True, 'cancel': True, 'describe': False, 'drop': False,
              'parse': True, 'print': True, 'elements': True},
}


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


@dataclass
class _Session:
    """A session: the handles it issued (none until step 3), dropped when it closes."""

    handles: set[str] = field(default_factory=set)


class ContractDoor:
    """THE CONTRACT'S DOOR ON PYTHON: each call answered by name; a query's run given its execution id HERE, once per
    plan run (Q7: the door mints, exec binds, planning holds none; ``ArchitectureTest.theServersMintsAreItsHostDoorsAlone``
    the Java twin), carried in its answer and in every failure of it."""

    def __init__(self, plan_and_run: Callable[[str, Any], Any]) -> None:
        # engine.py's plan-and-run: a plan request in, its Answer (Arrow rows, or JSON: the planner's refusal, or why
        # the database refused its SQL) out
        self._plan_and_run = plan_and_run
        self._sessions: dict[str, _Session] = {}
        self._lock = threading.Lock()
        # the runtimes this engine owns, by name (R3a (b)); and each model it handed DataCube, by its text and runtime,
        # to the whole model it was written as (c)
        self._runtimes: dict[str, OwnRuntime] = {}
        self._served: dict[tuple[str, str], str] = {}

    # ---- R3a: the engine owns its runtimes

    def served(self, model: str, runtime: str) -> tuple[str, str]:
        """A model this engine wrote (a frame's, a file's: the planner's ``table_model``) as DataCube is handed it (R3a
        (c)): its Connection and Runtime kept here as the engine's own, the rest answered as Pure text (printed from its
        parsed elements), with the runtime by its name. ``(model, runtime)``.

        The engine's own part is the planner's text's ``###Connection`` and ``###Runtime`` sections, verified on their
        parsed elements to be exactly the model's connections and runtimes (the platform's printer prints no DuckDB
        connection, so it is never re-printed): this splits the ENGINE's own text, never one DataCube sent."""
        elements = [e for e in compiler.model_elements(model) if e.get('_type') != _SECTION_INDEX]
        owned = sorted(_path(e) for e in elements if e.get('_type') in OWNED_KINDS)
        if runtime not in owned:
            raise ValueError(f"the model declares no runtime '{runtime}' for this engine to own")
        own = _sections(model, ('Connection', 'Runtime'))
        mine = sorted(_path(e) for e in compiler.model_elements(own) if e.get('_type') != _SECTION_INDEX)
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
            elements = compiler.model_elements(ask['model'])
        except compiler.LegendError as failure:
            raise ContractError('compile', call, failure.message) from None
        owned = sorted({f"a {e.get('_type')} '{_path(e)}'" for e in elements if e.get('_type') in OWNED_KINDS})
        if owned:
            raise refused(call, f'a model declaring {owned[0]}', 'a model DataCube sends holds classes, stores and mappings'
                          ' only: the engine owns its connections and runtimes (R3a)')
        with self._lock:
            runtime = self._runtimes.get(ask['runtime'])
            whole = self._served.get((ask['model'], ask['runtime']))
        if runtime is None:
            raise refused(call, f"the runtime '{ask['runtime']}'", 'it is no runtime this engine owns')
        return whole if whole is not None else f"{ask['model']}\n{runtime.text}"

    # ---- the door

    def answer(self, call: str, body: str, over: Any = None) -> Said:
        """``call`` (the path's last part) answered over ``body`` (JSON) -- ``over``, the read of the frames it was made
        in (D15), where a call reads the database. Never raises: a failure is a ContractError's JSON."""
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
            return failure.said()

    # ---- sessions

    def _open(self, asked: Mapping[str, Any], over: Any) -> Said:
        session = f'session-{uuid.uuid4().hex}'
        with self._lock:
            self._sessions[session] = _Session()
        return _json({'session': session})

    def _close(self, asked: Mapping[str, Any], over: Any) -> Said:
        with self._lock:
            self._sessions.pop(_session_of(asked, 'close'), None)
        return _json({})

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
        self._session(asked, 'elements')
        model = _text(asked, 'model', 'elements')
        return _json({'elements': _compiled('elements', lambda: compiler.model_elements(model))})

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
        # THE MINT: one execution id for this plan run (Q7), its answer's and every failure's
        execution = uuid.uuid4().hex
        request = {'lambda': json.dumps(ask['query']), 'model': model, 'runtime': ask['runtime'],
                   **({'project': True} if ask.get('how', {}).get('project') is True else {})}
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


_CALLS: dict[str, Callable[[ContractDoor, Mapping[str, Any], Any], Said]] = {
    'capabilities': ContractDoor._capabilities, 'open': ContractDoor._open, 'close': ContractDoor._close,
    'cancel': ContractDoor._cancel, 'parse': ContractDoor._parse, 'print': ContractDoor._print,
    'elements': ContractDoor._elements, 'typeOf': ContractDoor._type_of, 'query': ContractDoor._query,
}



def _not_offered(call: str) -> Callable[[ContractDoor, Mapping[str, Any], Any], Said]:
    """A call the contract names that this engine does not do yet: refused by name (its capabilities say so)."""
    def refuse(door: ContractDoor, asked: Mapping[str, Any], over: Any) -> Said:
        raise refused(call, call, 'its capabilities do not list it')
    return refuse


# step 1 (2026-11-13): these follow with steps 2 and 3
for _call in ('addSource', 'describe', 'drop', 'write'):
    _CALLS[_call] = _not_offered(_call)


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


_QUERY_KEYS = frozenset({'model', 'runtime', 'query', 'parameters', 'how'})


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
    if not isinstance(how, dict) or set(how) - {'project'} or how.get('project') not in (None, True):
        raise refused(call, f"how a query reads its model ({json.dumps(how)})", "this engine reads a model's own tables, "
                      "or a project's own rows")
    return ask


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
