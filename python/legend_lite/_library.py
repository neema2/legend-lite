"""The native library (//native:compiler) and the GraalVM rules for calling it.

One ISOLATE per process, created on first use. Every OS thread that calls in must be ATTACHED
to it and pass its own isolate thread: GraalVM requires it, and sharing one handle between
threads crashes the process (measured, 2026-10-02). So each thread attaches itself once, lazily,
and stays attached: a thread that exits leaves a few kilobytes behind, so a host that starts a
thread per request should call from a pool instead.

Not across ``os.fork``: the isolate's process is multi-threaded, so a forked child may deadlock.
Start child processes with multiprocessing's ``spawn`` (or ``forkserver``) method; each child
loads its own isolate.
"""

from __future__ import annotations

import ctypes
import os
import sys
import threading
from pathlib import Path
from typing import Any

_NAMES = {'darwin': 'libcompiler.dylib', 'linux': 'libcompiler.so', 'win32': 'libcompiler.dll'}

# each entry point and how many C strings it takes after its isolate thread
_ARITY = {
    'lite_plan_json': 3, 'lite_plan_text': 3, 'lite_hold_model': 1, 'lite_release_model': 1, 'lite_plan_json_held': 3,
    'lite_relation_type_held': 2, 'lite_execution_plan_json_held': 3, 'lite_plan_text_held': 3, 'lite_relation_type_text_held': 2, 'lite_test_data_sql': 3,
    'lite_database_from_catalog': 1, 'lite_table_model': 1, 'lite_catalog_columns_sql': 2, 'lite_session_setup': 1,
    # a project's own database (C5)
    'lite_project_data': 2,
    # a host's named operations (D5)
    'lite_host_operations': 2,
    'lite_pure_v1': 3, 'lite_execute_plan': 2, 'lite_pure_v1_refusal': 1,
    # the planner protocol's execute halves for a host's own database, and its warm (C2)
    'lite_execute_json_plan': 2, 'lite_execute_json_answer': 2, 'lite_warm_model': 1,
    # the answer in upstream's Arrow format from the host's rows, folded as base64 (track A, 2026-10-29): wired to
    # the route when the user rules on the schema change (the decisions log D56)
    'lite_execute_arrow_answer': 2,
    # the site's media types, the one table every host serving a site answers with (C4)
    'lite_site_types': 0,
    # the warehouse's SQL API, as the one binding speaks it (C3)
    'lite_warehouse_call': 2, 'lite_warehouse_read': 3,
    # Python's model home (native/.../ModelHome.java): a repository's folder, then a request's method, target and body
    'lite_sdlc_handle': 4, 'lite_depot_handle': 4, 'lite_saved_queries': 6, 'lite_saved_cubes': 6,
}


def _find() -> Path:
    """The library: LEGEND_LITE_LIBRARY, else the one shipped inside this package."""
    explicit = os.environ.get('LEGEND_LITE_LIBRARY')
    if explicit:
        return Path(explicit)
    name = _NAMES.get(sys.platform)
    if name is None:
        raise OSError(f'legend-lite has no native library for {sys.platform} yet')
    return Path(__file__).with_name('_native') / name


class Library:
    """The loaded library: its functions, the process's isolate, and each thread's attachment."""

    def __init__(self, path: Path) -> None:
        if not path.is_file():
            raise OSError(f"legend-lite's native library is not at {path} (set LEGEND_LITE_LIBRARY)")
        self.path = path
        self._lib = ctypes.CDLL(str(path))
        self._isolate = ctypes.c_void_p()
        self._local = threading.local()
        lib = self._lib
        lib.graal_create_isolate.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(ctypes.c_void_p)]
        lib.graal_attach_thread.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
        lib.lite_free.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
        lib.lite_free.restype = None
        lib.lite_unfreed.argtypes = [ctypes.c_void_p]
        lib.lite_unfreed.restype = ctypes.c_int64
        # each entry point made ready on its first call: a library without one fails that call (and DataCube's
        # start-up check names it), not everything else
        self._ready: dict[str, Any] = {}
        first = ctypes.c_void_p()
        if lib.graal_create_isolate(None, ctypes.byref(self._isolate), ctypes.byref(first)) != 0:
            raise OSError(f'could not start the compiler from {path}')
        self._local.thread = first

    def _thread(self) -> ctypes.c_void_p:
        t = getattr(self._local, 'thread', None)
        if t is None:
            t = ctypes.c_void_p()
            if self._lib.graal_attach_thread(self._isolate, ctypes.byref(t)) != 0:
                raise OSError('could not attach this thread to the compiler')
            self._local.thread = t
        return t

    def call(self, name: str, *args: str) -> str:
        """One entry point; its answer text (``OK\\n...`` or ``ERR\\n...``), the native string freed."""
        for a in args:
            if '\0' in a:
                # a C string ends at its first NUL: the compiler would silently read less than was given
                raise ValueError(f'{name}: an argument contains a NUL character')
        t = self._thread()
        p = self._function(name)(t, *[a.encode('utf-8') for a in args])
        if not p:
            raise OSError(f'{name} returned no answer (the library could not allocate one)')
        try:
            return ctypes.string_at(p).decode('utf-8')
        finally:
            self._lib.lite_free(t, p)

    def _function(self, name: str) -> Any:
        f = self._ready.get(name)
        if f is None:
            arity = _ARITY.get(name)
            if arity is None:
                raise ValueError(f'{name} is not one of the native library\'s entry points')
            f = getattr(self._lib, name)
            f.argtypes = [ctypes.c_void_p] + [ctypes.c_char_p] * arity
            f.restype = ctypes.c_void_p
            self._ready[name] = f
        return f

    def exports(self, name: str) -> bool:
        """Whether the library exports a function of that name: its own symbol table asked, so no list is kept by hand."""
        try:
            getattr(self._lib, name)
        except AttributeError:
            return False
        return True

    def unfreed(self) -> int:
        """The answers the library has returned that were not yet freed (its own count): 0 between calls."""
        return self._lib.lite_unfreed(self._thread())


_loaded: Library | None = None
_lock = threading.Lock()


def library() -> Library:
    """The process's one library, loaded on first use."""
    global _loaded
    if _loaded is None:
        with _lock:
            if _loaded is None:
                _loaded = Library(_find())
    return _loaded
