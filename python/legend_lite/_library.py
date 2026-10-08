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

_NAMES = {'darwin': 'libcompiler.dylib', 'linux': 'libcompiler.so'}


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
        for name, arity in (('lite_plan_json', 3), ('lite_plan_text', 3), ('lite_relation_type_json', 2),
                            ('lite_lambda_json', 1), ('lite_compose', 2), ('lite_model_json', 1),
                            ('lite_database_from_catalog', 1), ('lite_table_model', 1),
                            ('lite_catalog_columns_sql', 2), ('lite_session_setup', 1)):
            f = getattr(lib, name)
            f.argtypes = [ctypes.c_void_p] + [ctypes.c_char_p] * arity
            f.restype = ctypes.c_void_p
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
        p = getattr(self._lib, name)(t, *[a.encode('utf-8') for a in args])
        if not p:
            raise OSError(f'{name} returned no answer (the library could not allocate one)')
        try:
            return ctypes.string_at(p).decode('utf-8')
        finally:
            self._lib.lite_free(t, p)

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
