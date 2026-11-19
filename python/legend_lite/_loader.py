"""THE NATIVE LIBRARY'S LOADER (//native:compiler; C5-P, 2026-11-15: split out of _library.py, the orchestrator's A1), and
the GraalVM rules for calling it. It is no key: it calls by name for the two keys alone -- the PLANNING key
(``_library``, the planner's: every entry but the warehouse's wire, each planner entry refused inside a run) and the
SERVICE key (``_services``, the run side's: the warehouse's wire calls alone). No other module imports it
(//python:run_closure_test). One library, one isolate per process: the keys share it.

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

from ._runframe import planning

_NAMES = {'darwin': 'libcompiler.dylib', 'linux': 'libcompiler.so', 'win32': 'libcompiler.dll'}

# each entry point and how many C strings it takes after its isolate thread
_ARITY = {
    'lite_hold_model': 1, 'lite_hold_model_over': 2, 'lite_release_model': 1,
    # query/plan and query/type over a held model: the binding's cache (the lite API spec's §7a, 2026-11-17)
    'lite_query_plan_held': 2, 'lite_query_type_held': 2,
    'lite_file_load': 1, 'lite_remote_mount': 1,
    # a snapshot's stream and a frame's attach, as plans (the user's ruling of 2026-11-11)
    'lite_snap_stream': 1, 'lite_snap_copy': 1, 'lite_output_plan': 1, 'lite_synthesize': 1, 'lite_frame_load': 1,
    'lite_database_from_catalog': 1, 'lite_table_model': 1, 'lite_catalog_columns_sql': 2, 'lite_session_setup': 1,
    # a project's own database (C5)
    'lite_project_data': 2,
    # a host's named operations (D5)
    'lite_host_operations': 2,
    # LITE'S OWN API, v1: one entry per operation, its request JSON in (the lite API spec's §5, §7, 2026-11-17)
    'lite_model_check': 1, 'lite_model_elements': 1, 'lite_query_parse': 1, 'lite_query_print': 1, 'lite_query_type': 1,
    'lite_query_plan': 1, 'lite_model_create_class': 1, 'lite_model_create_association': 1,
    'lite_model_create_data_space': 1, 'lite_ts_item': 1, 'lite_ts_statement': 1,
    # lite's routes as the server answers them, and query/run's request as it reads it (H4's P4, 2026-11-19)
    'lite_route': 4, 'lite_query_run_request': 1,
    # query/run's answer from this host's rows, the server's writer's (H4's P5, 2026-11-19)
    'lite_query_run_json': 2, 'lite_query_run_arrow_facts': 1,
    # the planner's warm (C2)
    'lite_warm_model': 1,
    # the site's media types, the one table every host serving a site answers with (C4)
    'lite_site_types': 0,
    # lite's refusal kinds by name (the lite API spec's §3, 2026-11-17)
    'lite_refusal_kinds': 0,
    # the warehouse's SQL API, as the one binding speaks it (C3)
    'lite_warehouse_call': 2, 'lite_warehouse_read': 3,
    # Python's model home (native/.../ModelHome.java): a repository's folder, then a request's method, target and body
    'lite_sdlc_handle': 4, 'lite_depot_handle': 4,
    # the product's saved queries and pages at lite's mount, in lite's words (the edge program's H6, 2026-11-17)
    'lite_store_queries': 6, 'lite_store_cubes': 6,
}

# THE ENGINE LIBRARY'S ENTRIES (//native:engine, libcompiler_engine; K2, the user's "product root, edge opt-in",
# 2026-11-18): legend-engine's routes -- pure/v1, execute's halves, the refusal in its envelope, its saved-query store --
# loaded by the edge's key alone (legend_lite/edge/_engine.py), never the product's. The product library exports none
# of them (//python:engine_test's EdgeOff).
ENGINE_ARITY = {
    'lite_pure_v1': 3, 'lite_execute_plan': 2, 'lite_pure_v1_refusal': 1,
    'lite_execute_json_plan': 2, 'lite_execute_json_answer': 2, 'lite_execute_arrow_answer': 2,
    # legend-engine's saved queries and pages, at its mount in its words (the edge's SavedQueriesHome)
    'lite_saved_queries': 6, 'lite_saved_cubes': 6,
}
# the engine library's entries that plan nothing: legend-engine's store
ENGINE_NOT_PLANNING = frozenset({'lite_saved_queries', 'lite_saved_cubes'})


# The library's entries that plan nothing: its services (the site's types, the warehouse's binding, the model home's
# stores) and a held model let go -- which a finalizer may do at any point, a run's included. Every other entry is the
# planner's, refused inside a run of its flow (_runframe.py, the witness, 2026-11-13), by Loaded.call itself (C5-P's
# audit M1, 2026-11-15). _library.py writes the same set (_NOT_PLANNING, which PlannerSurfaceTest reads), held equal
# to this one when it loads.
# (lite_store_queries, lite_store_cubes: the product's saved queries and pages, at lite's mount in lite's words --
# the edge program's H6, 2026-11-17)
# (lite_saved_queries, lite_saved_cubes: legend-engine's store, the engine library's since K2, 2026-11-18)
NOT_PLANNING = frozenset({
    'lite_site_types', 'lite_warehouse_call', 'lite_warehouse_read', 'lite_sdlc_handle', 'lite_depot_handle',
    'lite_release_model', 'lite_store_queries', 'lite_store_cubes',
})

# THE SERVICE KEY'S NAMES (_services.py): the warehouse's SQL API, as the one binding speaks it -- the run side's, and
# no other key's. 2026-11-15 (C5-P, A1).
SERVICE_KEY_NAMES = frozenset({'lite_warehouse_call', 'lite_warehouse_read'})


def _find() -> Path:
    """The library: LEGEND_LITE_LIBRARY, else the one shipped inside this package."""
    explicit = os.environ.get('LEGEND_LITE_LIBRARY')
    if explicit:
        return Path(explicit)
    name = _NAMES.get(sys.platform)
    if name is None:
        raise OSError(f'legend-lite has no native library for {sys.platform} yet')
    return Path(__file__).with_name('_native') / name


class Loaded:
    """The loaded library: its functions, the process's isolate, and each thread's attachment. Called by the keys alone."""

    def __init__(self, path: Path, arity: dict[str, int] | None = None, not_planning: frozenset[str] | None = None,
                 setting: str = 'LEGEND_LITE_LIBRARY') -> None:
        if not path.is_file():
            raise OSError(f"legend-lite's native library is not at {path} (set {setting})")
        self.path = path
        # its entries and those that plan nothing: the product library's, unless given (the engine library's: K2)
        self._arity = _ARITY if arity is None else arity
        self._not_planning = NOT_PLANNING if not_planning is None else not_planning
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
        """One entry point; its answer text (``OK\\n...`` or ``ERR\\n...``), the native string freed -- a key's to ask
        (each key checks what it may carry first). A planner's entry is refused inside a run of its flow HERE, whoever
        asks (C5-P's audit M1, 2026-11-15: until then the planning key alone checked, and a module reaching the loader
        past the keys skipped it): every name but the library's services and a held model let go (``NOT_PLANNING``)."""
        if name not in self._not_planning:
            planning(name)
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
            arity = self._arity.get(name)
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


_loaded: Loaded | None = None
_lock = threading.Lock()


def loaded() -> Loaded:
    """The process's one library, loaded on first use: the keys' alone."""
    global _loaded
    if _loaded is None:
        with _lock:
            if _loaded is None:
                _loaded = Loaded(_find())
    return _loaded


# THE ENGINE LIBRARY (K2, the user's "product root, edge opt-in", 2026-11-18): legend-engine's entry points, a library
# of their own (//native:engine), its own isolate -- loaded on first use by the edge's key alone
# (legend_lite/edge/_engine.py), never by the product. Found at LEGEND_LITE_ENGINE_LIBRARY, else in the
# legend-lite-engine package (the [engine] extra: `pip install legend-lite[engine]`); neither, refused by name.
_ENGINE_NAMES = {'darwin': 'libcompiler_engine.dylib', 'linux': 'libcompiler_engine.so', 'win32': 'libcompiler_engine.dll'}


class EngineMissing(OSError):
    """legend-engine's routes asked of a Python without its engine library: the [engine] extra not installed."""


def _find_engine() -> Path:
    """The engine library: LEGEND_LITE_ENGINE_LIBRARY, else the one the legend-lite-engine package ships."""
    explicit = os.environ.get('LEGEND_LITE_ENGINE_LIBRARY')
    if explicit:
        return Path(explicit)
    name = _ENGINE_NAMES.get(sys.platform)
    try:
        import legend_lite_engine
    except ImportError:
        raise EngineMissing('legend-engine needs the legend-lite[engine] extra: pip install legend-lite[engine]') from None
    if name is None:
        raise EngineMissing(f'legend-lite has no engine library for {sys.platform} yet')
    return Path(legend_lite_engine.__file__).with_name('_native') / name


_engine: Loaded | None = None


def engine_loaded() -> Loaded:
    """The process's engine library, loaded on first use: the edge's key's alone."""
    global _engine
    if _engine is None:
        with _lock:
            if _engine is None:
                _engine = Loaded(_find_engine(), ENGINE_ARITY, ENGINE_NOT_PLANNING, 'LEGEND_LITE_ENGINE_LIBRARY')
    return _engine
