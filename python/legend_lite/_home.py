"""Where legend-lite keeps this user's things on this machine (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §6, Q1): the
Python analogue of the browser's per-site storage -- a folder of this user's, shared by every notebook and script --
and, in it, the saved pages DataCube's Save and Open… keep (§5, step 2d).

``home()`` says where it is: the folder ``home(path)`` pointed this process at, else ``LEGEND_LITE_HOME`` (a folder for
every process started with it: a test's own), else the operating system's per-user data folder (macOS
``~/Library/Application Support/legend-lite``, Linux ``$XDG_DATA_HOME/legend-lite`` or ``~/.local/share/legend-lite``,
Windows ``%LOCALAPPDATA%\\legend-lite``). Its folders are made private to this user (on POSIX: only its owner reads
them), as the browser's storage is the person's own.

The saved pages are kept as files, one each (``home()/pages``, the layout the page's own records were kept in before
B6), and the saved queries one file each (``home()/queries``): both stores answered by legend-lite's own rules in the
native library -- the server's, and the page's compiled to WebAssembly (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B5,
B6) -- each call under a lock several Python processes share (two notebooks at once).

The page's own settings (the grids' defaults, kept between visits: the browser keeps them per site) are kept here too,
one file (``home()/settings/datacube.json``): every notebook and run of this user's shows them, where the browser's
storage would lose them with each run's new address (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B2).

And the model home (B3): the SDLC's projects, a git repository (``home()/projects``) the native library's SDLC writes
as the model home's server writes its own, each call under the folder's lock."""

from __future__ import annotations

import contextlib
import json
import os
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

_chosen: Path | None = None
# who this user is to the stores kept here (the saved pages, the saved queries): the folder's one owner -- there is no
# sign-in -- as the browser's store is 'this browser''s
THIS_USER = 'this user'


def home(path: str | Path | None = None) -> Path:
    """This user's legend-lite folder (made when first used). Given a path, this process keeps its things there from
    now on (a folder beside a notebook, say) and says it."""
    global _chosen
    if path is not None:
        _chosen = Path(path).expanduser().resolve()
    if _chosen is not None:
        return _chosen
    explicit = os.environ.get('LEGEND_LITE_HOME')
    if explicit:
        return Path(explicit).expanduser().resolve()
    return _system_home()


def _system_home() -> Path:
    if sys.platform == 'darwin':
        return Path.home() / 'Library' / 'Application Support' / 'legend-lite'
    if sys.platform == 'win32':
        local = os.environ.get('LOCALAPPDATA')
        return (Path(local) if local else Path.home() / 'AppData' / 'Local') / 'legend-lite'
    data = os.environ.get('XDG_DATA_HOME')
    return (Path(data) if data else Path.home() / '.local' / 'share') / 'legend-lite'


class Settings:
    """DataCube's own settings, as the page keeps them between visits (``home()/settings/datacube.json``): one JSON
    object, the page's, never read here; written whole or not at all, under the folder's lock."""

    def __init__(self, folder: Path | None = None) -> None:
        self.folder = folder if folder is not None else home() / 'settings'

    def read(self) -> dict[str, Any] | None:
        """What the page last kept, or None: none kept, or a file that is not one (said on standard error)."""
        path = self.folder / 'datacube.json'
        with locked(self.folder):
            if not path.is_file():
                return None
            try:
                kept = json.loads(path.read_text(encoding='utf-8'))
            except (ValueError, OSError) as unread:
                print(f'legend-lite: {path} is not DataCube\'s settings ({unread}); they start afresh', file=sys.stderr)
                return None
        return kept if isinstance(kept, dict) else None

    def write(self, values: dict[str, Any]) -> None:
        """The page's settings, kept whole: to a file beside them, then moved over them."""
        if not isinstance(values, dict):
            raise ValueError("DataCube's settings are one object")
        with locked(self.folder):
            written = tempfile.NamedTemporaryFile('w', encoding='utf-8', dir=self.folder, suffix='.part', delete=False)
            try:
                with written:
                    json.dump(values, written, ensure_ascii=False)
                os.replace(written.name, self.folder / 'datacube.json')
            except BaseException:
                Path(written.name).unlink(missing_ok=True)
                raise


@contextlib.contextmanager
def locked(folder: Path) -> Iterator[None]:
    """A folder of this user's held for one call, across every Python process of this user (a lock file in it). The
    folder and the one it is in are made private to this user when this makes them."""
    folder.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    folder.mkdir(mode=0o700, exist_ok=True)
    with open(folder / '.lock', 'a+b') as lock:
        if sys.platform == 'win32':
            import msvcrt
            lock.seek(0)
            # blocks until the other process lets go (retried by msvcrt every second, for ten tries, then raises)
            msvcrt.locking(lock.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def model_home_folder() -> Path:
    """Where this user's model home keeps its projects: a git repository in their legend-lite folder, as it is now
    (the program's B3; Python's engine serves it, DataCube's model actions write into it through the host's fetch)."""
    return home() / 'projects'


def queries_folder() -> Path:
    """Where this user's saved queries are kept: one JSON file per query, every version of it, as legend-lite's server
    keeps them (the program's B5; Python's engine serves them at /api/pure/v1/query)."""
    return home() / 'queries'


def pages_folder() -> Path:
    """Where this user's saved pages are kept: one ``<id>.json`` file each, the page's row (its content the page
    document's exact JSON text), as they always were (the program's B6; Python's engine serves them at
    /api/pure/v1/query/dataCube, ``page.save()`` writes into them)."""
    return home() / 'pages'


def store_call(entry: str, folder: Path, *asked: str) -> tuple[int, str]:
    """One call of a store the native library answers over ``folder`` (``entry``: ``lite_saved_queries`` or
    ``lite_saved_cubes``, legend-lite's own rules), under the lock every Python process takes on it, the library's call
    on one of the compiler's threads: its status and its body. A failure of the library's raised (``OSError``)."""
    from ._library import library
    from ._pool import _on_compiler_threads
    with locked(folder):
        said = _on_compiler_threads(lambda: library().call(entry, str(folder), *asked))
    tag, _, rest = said.partition('\n')
    if tag != 'OK':
        raise OSError(rest.partition('\n')[2] or rest)
    status, _, answered = rest.partition('\n')
    return int(status), answered


def home_call(folder: Path, kind: str, method: str, path: str, body: str | None) -> tuple[int, str]:
    """One call of the model home in this process (``kind``: ``'sdlc'`` or ``'depot'``): the native library's SDLC or
    Depot over the git repository in ``folder``, the library's call on one of the compiler's threads -- the one way
    Python's engine (its ``/sdlc/api``, ``/depot/api``) and DataCube's code headless (its model actions' SDLC, through the
    host's fetch) reach it. The library makes each call under the repository's own lock, which every kind of process
    that opens it takes (the model home's server too; docs/ONE_PLATFORM_PROGRAM_2026_10_12.md G11); Python's own lock
    across its processes is taken first, on the caller's thread, so a call waiting on another Python process holds no
    compiler thread. Asked on a thread that calls the library itself (DataCube's engine's, its model actions' fetch), the
    call runs there, never waiting on the pool (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D15). A failure of the
    library's raised (``OSError``)."""
    from ._library import library
    from ._pool import _on_compiler_threads
    with locked(folder):
        said = _on_compiler_threads(lambda: library().call(f'lite_{kind}_handle', str(folder), method, path, body or ''))
    tag, _, rest = said.partition('\n')
    if tag != 'OK':
        raise OSError(rest.partition('\n')[2] or rest)
    status, _, answered = rest.partition('\n')
    return int(status), answered
