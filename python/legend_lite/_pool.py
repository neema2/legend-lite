"""The threads that call the compiler: ONE fixed pool for every caller in the process, as the native library asks of a
host (each thread that calls it stays attached to its isolate), so opening engine after engine in a notebook attaches
no new threads. On Python's standard library alone: the compiler-only install (``pip install legend-lite``) calls the
native library through it too -- a model home's calls, the saved pages and queries (_home.py) -- and
DataCube's engine (engine.py) through the same pool. A connection is read and written on a thread of its own, which
never calls the compiler: an idle connection holds no compiler thread."""

from __future__ import annotations

import queue
import threading
from collections.abc import Callable
from concurrent.futures import Future
from typing import Any

_WORKERS = 4


class _Pool:
    """The compiler's threads: daemon threads taking calls from one queue. Not concurrent.futures' pool: Python shuts
    that down before its atexit handlers run, and a plain script's end waits in one (show(): the cubes stay open until
    Ctrl-C), so every call the engine made there would be refused (the audit of show(), 2026-10-08)."""

    def __init__(self, size: int) -> None:
        self._calls: queue.SimpleQueue[tuple[Future[Any], Any, tuple[Any, ...]]] = queue.SimpleQueue()
        for i in range(size):
            threading.Thread(target=self._serve, name=f'legend-lite-compiler-{i}', daemon=True).start()

    def submit(self, call: Any, *args: Any) -> Future[Any]:
        future: Future[Any] = Future()
        self._calls.put((future, call, args))
        return future

    def _serve(self) -> None:
        while True:
            future, call, args = self._calls.get()
            if not future.set_running_or_notify_cancel():
                continue
            try:
                future.set_result(call(*args))
            except BaseException as failure:  # noqa: BLE001 -- handed to the caller, which answers it
                future.set_exception(failure)


_pool: _Pool | None = None
_pool_lock = threading.Lock()


# the threads besides the pool's that call the library, each for its life: DataCube's engine's (_headless.py), which
# answers its planner's requests and what its calls ask of Python
_library = threading.local()


def library_thread() -> None:
    """This thread calls the native library for its life, as the pool's do (DataCube's engine's thread: a fixed
    thread, attached once): a library call it makes runs in place, never waiting on the pool -- whose threads may all
    be waiting on this one (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D15)."""
    _library.is_one = True


def _on_compiler_threads(call: Callable[[], Any]) -> Any:
    """A library call run on the compiler's threads: in place when the caller is one (a pool thread waiting on the pool
    would wait on itself; DataCube's engine's thread, ``library_thread``), else on the pool."""
    if threading.current_thread().name.startswith('legend-lite-compiler-') or getattr(_library, 'is_one', False):
        return call()
    return _workers().submit(call).result()


def _workers() -> _Pool:
    global _pool
    with _pool_lock:
        if _pool is None:
            _pool = _Pool(_WORKERS)
        return _pool
