"""THE WAREHOUSE, AS PYTHON SPEAKS IT (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C3): a driver of the one binding --
warehouse/.../sqlapi/SqlApiCalls.java, in the native library (``lite_warehouse_call``, ``lite_warehouse_read``) -- the
twin of the tab's (engine-client/src/warehouse.ts): the same functions, which make the very calls the binding names and
read every answer through it. This module only performs them: the HTTP (``urllib``: no new dependency), the clock, and
the Arrow bytes, which stay here. One recorded corpus holds both drivers to the same calls
(warehouse/src/test/resources/sqlapi-corpus.json; python/tests/test_warehouse.py).

The person running Python signs in as themselves (the user, 2026-10-12): by password, or by the launch key a
single-user warehouse printed. The token is kept here and refreshed as it nears its end, as the tab's is.
"""

from __future__ import annotations

import http.client
import json
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any
from urllib.parse import urlsplit

from ._library import library
from ._planned import PlannedSql, require

# how long one call may take on the wire: a poll waits up to 10 s on the server (the binding's), a chunk is read whole
_WIRE_TIMEOUT_S = 120


class WarehouseError(RuntimeError):
    """The warehouse refused a call: the API's own error (its code and message), or why it could not be reached."""

    def __init__(self, message: str, code: str | None = None) -> None:
        super().__init__(message)
        self.code = code


class SessionExpired(WarehouseError):
    """The warehouse refused the token: expired, or issued by a warehouse that has since restarted."""

    def __init__(self, base_url: str, principal: str) -> None:
        super().__init__(f'the warehouse session for {principal} has expired — sign in again', 'AUTH_INVALID')
        self.base_url = base_url
        self.principal = principal


@dataclass(frozen=True)
class Session:
    """A signed-in session: where the warehouse is, the bearer token, who the server says it is, and when it ends."""

    base_url: str
    token: str
    principal: str
    expires_at: str


# where a call of the library runs: here, unless a host whose callers are many threads (the engine's connections)
# hands in its own pool -- a thread that calls the library is attached to it for good, so a host keeps the calls on a
# few (set_library_thread); the HTTP never runs there
_on_library: Callable[[Callable[[], str]], str] = lambda call: call()  # noqa: E731


def set_library_thread(run: Callable[[Callable[[], str]], str]) -> None:
    """Every call of the binding run by ``run`` (the engine's compiler pool), never on the caller's own thread."""
    global _on_library
    _on_library = run


def _binding(entry: str, *args: str) -> dict[str, Any] | list[Any]:
    tag, _, rest = _on_library(lambda: library().call(entry, *args)).partition('\n')
    if tag != 'OK':
        raise RuntimeError(f'the native library could not answer {entry}: {rest}')
    return json.loads(rest)


def _call(op: str, args: dict[str, Any]) -> dict[str, Any]:
    """The call the binding names; a refusal of its arguments is this driver's bug, raised."""
    said = _binding('lite_warehouse_call', op, json.dumps(args))
    if not isinstance(said, dict) or 'error' in said:
        raise RuntimeError(f'the warehouse binding refused {op}: {said}')
    return said


def _read(op: str, status: int, body: str) -> Any:
    # a C string ends at its first NUL: an answer carrying one is read with it replaced, never cut short
    return _binding('lite_warehouse_read', op, str(status), body.replace('\0', '\ufffd'))


def _refused(said: Any) -> bool:
    """A refusal: an error and no step (a statement's failed step carries its error too, and is an answer)."""
    return isinstance(said, dict) and 'error' in said and 'step' not in said


def _words(said: dict[str, Any]) -> str:
    return f"{said['error']['code']}: {said['error']['message']}"


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """A redirect is not followed: urllib's own would carry the bearer token to wherever it points (another host,
    plain http), which a browser's fetch never does. Answered as the redirect it is, and so refused."""

    def redirect_request(self, *_: Any) -> None:
        return None


_OPENER = urllib.request.build_opener(_NoRedirect)


def _urlopen(request: urllib.request.Request) -> Any:
    """The one place a request goes out (a test answers in its stead)."""
    return _OPENER.open(request, timeout=_WIRE_TIMEOUT_S)


def _reach(base_url: str, call: dict[str, Any]) -> tuple[int, bytes]:
    """The call made, and its answer's status and bytes; a refused connection said as what it is."""
    body = call['body']
    request = urllib.request.Request(base_url.rstrip('/') + call['path'], method=call['method'],
                                     data=body.encode('utf-8') if body is not None else None, headers=call['headers'])
    try:
        with _urlopen(request) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as answered:
        with answered:
            return answered.code, answered.read()
    except (urllib.error.URLError, http.client.HTTPException, OSError) as unreachable:
        reason = getattr(unreachable, 'reason', unreachable)
        raise WarehouseError(f'cannot reach the warehouse at {base_url} — {reason}') from unreachable


def _exchange(base_url: str, op: str, args: dict[str, Any]) -> Any:
    status, body = _reach(base_url, _call(op, args))
    return _read(op, status, body.decode('utf-8', errors='replace'))


def _remote(base_url: str) -> str:
    """A warehouse is reached over HTTP(S) only: a remote address by its scheme (R4's rule for what Python reads)."""
    if urlsplit(base_url).scheme not in ('http', 'https') or not urlsplit(base_url).netloc:
        raise WarehouseError(f'a warehouse is an http:// or https:// address, not {base_url!r}')
    return base_url


def sign_in(base_url: str, user: str, password: str) -> Session:
    """Sign in by the warehouse's own user list: the password sent once and not kept, only the token."""
    t = _exchange(_remote(base_url), 'login', {'user': user, 'password': password})
    if _refused(t):
        raise WarehouseError(f'sign-in failed — {_words(t)}', t['error']['code'])
    return Session(base_url, t['token'], t['principal'], t['expiresAt'])


def sign_in_with_key(base_url: str, key: str) -> Session:
    """The single-user warehouse's sign-in: the launch key it printed, for a token like a password's."""
    t = _exchange(_remote(base_url), 'loginKey', {'key': key})
    if _refused(t):
        raise WarehouseError(f'sign-in failed — {_words(t)}', t['error']['code'])
    return Session(base_url, t['token'], t['principal'], t['expiresAt'])


def list_objects(session: Session) -> list[dict[str, Any]]:
    """What the user may read in every catalog: each table or view with its columns (structured, as the catalog
    reads them), its catalog, and that catalog's database type, as a Pure connection names it."""
    objects = _exchange(session.base_url, 'objects', {'token': session.token})
    if _refused(objects):
        raise WarehouseError(f'could not list the tables — {_words(objects)}', objects['error']['code'])
    return objects


def connect(base_url: str, user: str, password: str) -> tuple[Session, list[dict[str, Any]]]:
    """Sign in AND list what the user may read, as one step: the session comes only with its own tables (P2-334)."""
    session = sign_in(base_url, user, password)
    return session, list_objects(session)


def _seconds_left(expires_at: str, now: float) -> float | None:
    try:
        return datetime.fromisoformat(expires_at.replace('Z', '+00:00')).timestamp() - now
    except ValueError:
        return None


class Warehouse:
    """One catalog of the warehouse, read as the signed-in user: statements run there, their rows the server's Arrow
    chunks. The token is refreshed in its last fifth (``POST /sql/v1/token/refresh``), at the next call, so nobody is
    asked for a password mid-work; the server stops refreshing at its session limit, and the next call then says the
    session expired."""

    def __init__(self, session: Session, catalog: str = 'main', clock: Callable[[], float] = time.time) -> None:
        self._session = session
        self._catalog = catalog
        self._clock = clock
        self._issued = clock()
        self._lock = threading.Lock()

    @property
    def session(self) -> Session:
        """The session as it is now (its token refreshed as it neared its end)."""
        return self._session

    @property
    def principal(self) -> str:
        return self._session.principal

    @property
    def base_url(self) -> str:
        return self._session.base_url

    @property
    def expires_at(self) -> str:
        return self._session.expires_at

    def renew(self, session: Session) -> None:
        """A fresh token for the SAME user at the same warehouse; anyone else's session is refused."""
        if session.principal != self._session.principal or session.base_url.rstrip('/') != self._session.base_url.rstrip('/'):
            raise WarehouseError(f'this warehouse is read as {self._session.principal}; signed in as {session.principal}, '
                                 f'open a table to work as {session.principal}')
        with self._lock:
            self._session = session
            self._issued = self._clock()

    def sign_in_again(self, password: str) -> None:
        """Sign in again as this user at this warehouse, and go on with the new token."""
        self.renew(sign_in(self._session.base_url, self._session.principal, password))

    def refresh_token(self) -> None:
        """Swap the token for a fresh one now: same user, a new expiry."""
        t = self._call('refresh', {}, refresh=False)
        self.renew(replace(self._session, token=t['token'], principal=t['principal'], expires_at=t['expiresAt']))

    def objects(self) -> list[dict[str, Any]]:
        """What this user may read in every catalog (``list_objects``), the token refreshed first when it nears its end;
        a token refused is an expired session."""
        return self._call('objects', {})

    def check(self, statement_id: str) -> str:
        """Whether a statement is on the warehouse's record for this user (its history: the caller's own only)."""
        history = self._call('history', {'limit': 1000})
        who = self._session.principal
        found = next((h for h in history if h['statementId'] == statement_id), None)
        if found is None:
            return f"Not on the warehouse's record for {who}: no statement {statement_id} in its last {len(history)}."
        rows = found.get('rowCount')
        said = (f"On the warehouse's record for {who}: statement {statement_id}, {found['state']}, "
                f"{'its rows uncounted' if rows is None else f'{rows:,} rows'}, submitted {found['submittedAt']}")
        return said + (f", finished {found['finishedAt']}" if found.get('finishedAt') else '') + '.'

    def arrow_chunks(self, sql: PlannedSql, on_receipt: Callable[[dict[str, Any]], None] | None = None) -> Iterator[bytes]:
        """The result of ``sql`` as the server wrote it: one Arrow IPC stream per chunk, in order. The statement is
        closed once every chunk is read, so the server frees it; ``on_receipt`` hears what the server issued for it
        (its id and its count) before the first chunk. Only a plan's statement is sent (_planned.py)."""
        step = self._call('submit', {'sql': require(sql, 'the warehouse'), 'catalog': self._catalog})
        statement = step.get('statementId')
        # where a read that ends early leaves its statement (the caller stops, or a call fails): still running,
        # cancelled; done, its result closed; read to the end (or failed there), nothing -- the tab's rule
        ending: str | None = 'cancel'
        try:
            while step['step'] == 'poll':
                step = self._call('poll', {'statementId': step['statementId']})
            if step['step'] == 'failed':
                ending = None
                raise WarehouseError(f"{step['error']['code']}: {step['error']['message']}", step['error']['code'])
            ending = 'closeStatement'
            if on_receipt is not None:
                receipt = {'statementId': step['statementId'], 'as': self._session.principal,
                           'where': f'the warehouse at {urlsplit(self._session.base_url).netloc}'}
                on_receipt({**receipt, **({'serverRows': step['rowCount']} if 'rowCount' in step else {})})
            for index in range(step['chunkCount']):
                # a chunk's bytes are this driver's: only a failed chunk's answer is the binding's to read
                status, body = _reach(self._session.base_url, _call('fetchChunk', {
                    'statementId': step['statementId'], 'index': index, 'token': self._session.token}))
                if status != 200:
                    self._raise(_read('fetchChunk', status, body.decode('utf-8', errors='replace')))
                yield body
        finally:
            # the server may free the result now, not at expiry, and stop a statement still running; what it answers
            # changes nothing here (the rows were read, or the read has failed)
            if ending is not None and statement is not None:
                try:
                    self._call(ending, {'statementId': statement})
                except WarehouseError:
                    pass

    def _raise(self, said: Any) -> None:
        if not _refused(said):
            return
        if said.get('expired'):
            raise SessionExpired(self._session.base_url, self._session.principal)
        raise WarehouseError(_words(said), said['error']['code'])

    def _call(self, op: str, args: dict[str, Any], refresh: bool = True) -> Any:
        if refresh:
            self._refresh_if_ending()
        said = _exchange(self._session.base_url, op, {**args, 'token': self._session.token})
        self._raise(said)
        return said

    def _refresh_if_ending(self) -> None:
        """The token's last fifth reached: refreshed first; not refreshed (the session's limit), the call says so."""
        now = self._clock()
        left = _seconds_left(self._session.expires_at, now)
        if left is None or left <= 0:
            return
        life = left + (now - self._issued)
        if left < life * 0.2:
            try:
                self.refresh_token()
            except WarehouseError:
                # not refreshed: the call itself says what the warehouse thinks of the token
                pass
