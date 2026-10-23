"""ONE KEEPER PER PAGE (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md): the one copy of each page a session's Python pages
hold, shown or not, and its version -- the host's storage, which never reads a page: what a page reads (its frames, its
copies, its ids) comes with each write from DataCube's own code (headless pageReads, pageWritten, pageRebase), and so
does every rule of how two editors' edits combine (page-rebase.ts).

THE HOST APPLIES THE COMMANDS (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S4): either editor -- Python's page, a tab or a
notebook widget open on it -- writes a page as the version it is at and the edits it made there (``settle``), never the
page itself. The keeper has DataCube apply them to the page as it is now (``combine``: headless pageRebase -- at the
writer's version there is nothing to combine; at one the page has moved on from they are made again on top) and keeps
what they make; edits that do not apply there are refused (``Refused``), nothing kept. So the page is what the commands
make, the two editors never hold two pages, and neither keeps a rule of its own for combining them. One write is a page
whole: a RESTORE (an undo or a redo, the plan's N2; a page loaded, N1), taken only at the version it was made at.

Each version keeps the edits that moved the page to it (a page's, or a grid's with its tile's id), or none -- a page
restored whole -- so a page open at an older version follows by the edits since, or opens the page again; and the ids
the page had held then (what a write made at it knew). A write that leaves the page as it was is no new version.

EACH COPY IS THE KEEPER'S ONCE A PAGE READS IT (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z17): a snap's copy is its
maker's until a page the keeper keeps reads it -- its maker lets it go when it never lands (``has_read``) -- and from then
on goes only when nothing reads it any more (``letting_go``): no page as it is, and no page open on one at a version that
reads it, or any version since. Each open page says the version it shows (``follow``), as it asks and writes; one that
says nothing for ``FOLLOWED_FOR`` seconds (its tab closed, its machine asleep) keeps nothing. So a page open somewhere
that has not followed yet still reads its copies, and follows by the commands since (the keeper's known limit 6).

AND SO IS EACH FILE (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md X3, D14): a page's file read into a table, by the hash of its
bytes, and a remote file mounted as a view, by its URL -- what DataCube says a page reads beside its copies (headless
pageReads: ``files``, ``remotes``) -- is kept by the same rule: its reader's until a kept page reads it, the keeper's from
then on, let go when nothing reads it any more (``HOLDS``: each kind kept alike, by its key).
"""

from __future__ import annotations

import copy
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

# how many versions back a page keeps the edits of: a page open on an older one opens the page again
STEPS_KEPT = 64
# how many times a write is made again on top of a page that keeps moving before it is refused (each makes it again on
# the page as it is then: only a writer that keeps losing the race to another meets it)
COMBINED_AT_MOST = 64
# how long a page open somewhere is taken to show the version it last said (seconds): a tab asks about once a second,
# and a hidden one about once a minute; one that says nothing for longer -- closed, asleep -- keeps no copy
FOLLOWED_FOR = 300.0
# what a page reads that its host holds a table for: a snap's copies by table, its files by their bytes' hash, its remote
# files by URL -- each kept while a page reads it (``letting_go``)
HOLDS = ('copies', 'files', 'remotes')


class Stale(Exception):
    """A write against a version that is no longer the page's: ``version`` is the page's now."""

    def __init__(self, version: int) -> None:
        super().__init__(f'the page is at version {version}')
        self.version = version


class Refused(Exception):
    """A write the page cannot take: edits that do not apply to the page as it is now (DataCube's words), or a restore
    made against a version it moved on from."""


@dataclass(frozen=True)
class Settled:
    """A write kept: the page's version now; whether the keeper made another page of it than the writer's -- its edits
    made again on top of a page that had moved on, or an id given out afresh (the writer then takes the page as the keeper
    has it); and each id they gave out that was given out afresh."""
    version: int
    combined: bool
    renamed: Mapping[str, str]


@dataclass
class _Kept:
    version: int
    doc: dict[str, Any]
    # what the page reads, as DataCube said with its last write: {frames, copies, files, remotes, ids}
    reads: dict[str, list[str]]
    # every id the page has held: none is given out again
    held: list[str]
    # the edits that moved the page to each version kept, or None: born or restored whole
    steps: dict[int, list[Any] | None] = field(default_factory=dict)
    # the ids the page had held at each version kept: what a write made at it knew (a page filter it made, told from one
    # another writer made since under the same id: H3)
    helds: dict[int, list[str]] = field(default_factory=dict)
    # what the page read at each version kept that its host holds (HOLDS: its copies, files, remote files): what a page
    # open at that version, or following past it, reads
    holds: dict[int, dict[str, list[str]]] = field(default_factory=dict)
    # the pages open on it, each by the id it asks by: the version it said it shows, and when (time.monotonic)
    followers: dict[str, tuple[int, float]] = field(default_factory=dict)
    # shown somewhere (a tab, a notebook widget): what an open page may read and write
    served: bool = False


# combine(page, held, edits, known): DataCube's pageRebase as the host asks it -- the page and the ids it has held now, a
# writer's edits, the ids the page had held at the version they were made at -- {page, held, edits, renamed, reads}, or
# {problems}
Combine = Callable[[dict[str, Any], list[str], list[Any], list[str]], dict[str, Any]]
# admit(reads): the host's own refusal of a page its edits make (what it reads: a frame the host does not serve), or None
Admit = Callable[[Mapping[str, list[str]]], str | None]


class PageKeeper:
    """The pages a session keeps, by their keys (each Python page's own: ``Page._key``)."""

    def __init__(self, on_write: Callable[[str, int], None] | None = None, combine: Combine | None = None) -> None:
        self._lock = threading.Lock()
        self._pages: dict[str, _Kept] = {}
        # every copy, file and remote file a page kept has read (HOLDS, each by its key): the keeper's to let go from then
        # on, never its maker's
        self._read: dict[str, set[str]] = {kind: set() for kind in HOLDS}
        # told each new version (its page's key, the version), outside the lock: where the page is shown follows it
        self.on_write = on_write
        # how a write's edits are applied to the page as it is (asked outside the lock: DataCube's, headless)
        self.combine = combine

    def keep(self, key: str, doc: dict[str, Any], reads: Mapping[str, list[str]], held: list[str]) -> int:
        """A page kept from now on (a new Python page), at version 1."""
        kept_reads = _reads(reads)
        with self._lock:
            ids = sorted(set(held))
            self._pages[key] = _Kept(1, copy.deepcopy(doc), kept_reads, ids, {1: None}, {1: ids}, {1: _holds(kept_reads)})
            self._have_read(kept_reads)
        return 1

    def forget(self, key: str) -> None:
        """A page no longer kept (its Python page collected)."""
        with self._lock:
            self._pages.pop(key, None)

    def current(self, key: str) -> tuple[int, dict[str, Any]] | None:
        """The page's version and its document -- the keeper's own: read it, never change it -- or None."""
        with self._lock:
            kept = self._pages.get(key)
            return None if kept is None else (kept.version, kept.doc)

    def now(self, key: str) -> tuple[int, dict[str, Any], list[str]] | None:
        """The page's version, its document -- the keeper's own: read it, never change it -- and the ids it holds, as
        DataCube said with its last write (pageReads), read together; or None."""
        with self._lock:
            kept = self._pages.get(key)
            return None if kept is None else (kept.version, kept.doc, list(kept.reads['ids']))

    def frames(self, key: str) -> list[str] | None:
        """The frames the page reads, as DataCube said with its last write, or None."""
        with self._lock:
            kept = self._pages.get(key)
            return None if kept is None else list(kept.reads['frames'])

    def held(self, key: str) -> list[str]:
        """Every id the page has held."""
        with self._lock:
            kept = self._pages.get(key)
            return [] if kept is None else list(kept.held)

    def write(self, key: str, base: int, doc: dict[str, Any], reads: Mapping[str, list[str]], edits: list[Any] | None,
              held: list[str]) -> int:
        """The page made ``doc`` (reading what ``reads`` says), moved there from version ``base`` by ``edits`` (None: whole):
        its new version -- or, the page no longer at ``base``, ``Stale``. A document the page has already is no new
        version. A page not kept is a ``KeyError``. What the keeper takes from its editors is ``settle``'s and
        ``restore``'s; this is theirs."""
        kept_doc = copy.deepcopy(doc)
        kept_edits = None if edits is None else copy.deepcopy(edits)
        kept_reads = _reads(reads)
        with self._lock:
            kept = self._pages[key]
            if kept.version != base:
                raise Stale(kept.version)
            kept.held = sorted(set(kept.held) | set(held) | set(kept_reads['ids']))
            if kept_doc == kept.doc and kept_reads == kept.reads:
                return kept.version
            kept.version += 1
            kept.doc = kept_doc
            kept.reads = kept_reads
            kept.steps[kept.version] = kept_edits
            kept.helds[kept.version] = list(kept.held)
            kept.holds[kept.version] = _holds(kept_reads)
            self._have_read(kept_reads)
            for old in [v for v in kept.steps if v <= kept.version - STEPS_KEPT]:
                del kept.steps[old]
                kept.helds.pop(old, None)
                kept.holds.pop(old, None)
            version = kept.version
        if self.on_write is not None:
            self.on_write(key, version)
        return version

    def settle(self, key: str, base: int, edits: list[Any], admit: Admit | None = None) -> Settled:
        """A WRITE, SETTLED (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S4, H1): ``edits``, made by a writer at version
        ``base``, applied by DataCube to the page as it is now (``combine``: headless pageRebase -- at ``base`` nothing to
        combine, past it made again on top) and what they make kept -- or ``Refused``: one does not apply there, in
        DataCube's words, or the host refuses the page they make (``admit``), nothing kept. The ids the page holds from
        then on are the ones the commands gave out (H3)."""
        if self.combine is None:
            raise Refused('this page keeps no rule to apply its edits by')
        for _ in range(COMBINED_AT_MOST):
            with self._lock:
                kept = self._pages[key]
                at, now, now_held = kept.version, kept.doc, list(kept.held)
                known = kept.helds.get(base) if base <= at else None
            if known is None:
                raise Refused('the page changed where it is open, further than it keeps, while this change was made: make it '
                              'again')
            said = self.combine(now, now_held, edits, known)
            if 'problems' in said:
                raise Refused('the page changed where it is open while this change was made: ' + '; '.join(said['problems'])
                              if at != base else '; '.join(said['problems']))
            if admit is not None:
                refused = admit(said['reads'])
                if refused is not None:
                    raise Refused(refused)
            try:
                version = self.write(key, at, said['page'], said['reads'], said['edits'], said['held'])
            except Stale:
                continue
            renamed = dict(said['renamed'])
            return Settled(version, at != base or bool(renamed), renamed)
        raise Refused('the page kept changing where it is open while this change was made: make it again')

    def restore(self, key: str, base: int, doc: dict[str, Any], reads: Mapping[str, list[str]], held: list[str]) -> int:
        """A PAGE WRITTEN WHOLE (the plan's N1, N2: a page loaded, an undo or a redo) -- the one write that is no commands --
        kept only at the version it was made at: its version from then on; else ``Refused``, nothing kept (the page
        moved on: the writer opens it as it is)."""
        try:
            return self.write(key, base, doc, reads, None, held)
        except Stale:
            raise Refused('the page changed where it is open while this was undone or redone: it is shown as it is now') from None

    def changes(self, key: str, since: int) -> tuple[int, list[Any] | None]:
        """What moved the page since version ``since``: its version now, and the edits since, in order -- or None, when
        a version since was moved by no command or is no longer kept (the page is then opened again)."""
        with self._lock:
            kept = self._pages[key]
            if since > kept.version:
                return kept.version, None
            moved = [kept.steps.get(v) for v in range(since + 1, kept.version + 1)]
            if any(m is None for m in moved):
                return kept.version, None
            return kept.version, [c for m in moved for c in m or []]

    def serve(self, key: str, served: bool) -> None:
        """The page shown somewhere, or shown nowhere any more (no page is open on it then)."""
        with self._lock:
            kept = self._pages.get(key)
            if kept is not None:
                kept.served = served
                if not served:
                    kept.followers.clear()

    def follow(self, key: str, follower: str, version: int | None) -> bool:
        """A page open on ``key`` -- ``follower``, the id it asks by -- says it shows ``version`` (None: it is opening or
        following one, and shows the version it said last, if any): the copies that version and every one since read
        stay while it does (``letting_go``), and for ``FOLLOWED_FOR`` seconds after it last said anything. Whether it
        moved from the version it said before (the copies only that one read may go now: a sweep)."""
        with self._lock:
            kept = self._pages.get(key)
            if kept is None:
                return False
            was = kept.followers.get(follower)
            if version is None:
                if was is not None:
                    kept.followers[follower] = (was[0], time.monotonic())
                return False
            kept.followers[follower] = (version, time.monotonic())
            return was is not None and was[0] != version

    def served(self, key: str) -> bool:
        with self._lock:
            kept = self._pages.get(key)
            return kept is not None and kept.served

    def _have_read(self, reads: Mapping[str, list[str]]) -> None:
        for kind in HOLDS:
            self._read[kind] |= set(reads[kind])

    def has_read(self, key: str, kind: str = 'copies') -> bool:
        """Whether a page kept has read ``key`` -- a copy by its table, a file by its bytes' hash, a remote file by its URL
        (``kind``, one of HOLDS): the keeper's to let go from then on (``letting_go``), never its maker's."""
        with self._lock:
            return key in self._read[kind]

    def letting_go(self, held: Iterable[str], kind: str = 'copies') -> list[str]:
        """Of ``held`` (the copies, files or remote files its host holds, ``kind``, by their keys), the ones nothing reads
        any more, to let go: read by a page kept once, and now by none -- neither by a page as it is, nor at a version a
        page open on it shows (as it last said within ``FOLLOWED_FOR`` seconds), nor at any version since, which that
        page follows by. A page collected reads none."""
        now = time.monotonic()
        with self._lock:
            read: set[str] = set()
            for kept in self._pages.values():
                kept.followers = {f: said for f, said in kept.followers.items() if now - said[1] <= FOLLOWED_FOR}
                oldest = min((v for v, _ in kept.followers.values()), default=kept.version)
                read.update(kept.reads[kind])
                for version, then in kept.holds.items():
                    if version >= oldest:
                        read.update(then[kind])
            return sorted(k for k in set(held) if k in self._read[kind] and k not in read)

    def gone(self, key: str, kind: str = 'copies') -> None:
        """``key`` let go of (a copy's table is never given again; a file read anew is the keeper's once a page reads it
        again)."""
        with self._lock:
            self._read[kind].discard(key)


def _reads(reads: Mapping[str, list[str]]) -> dict[str, list[str]]:
    """What a page reads, as kept: its frames, its copies, its files, its remote files and its ids, each a sorted list."""
    return {part: sorted(set(reads.get(part, []))) for part in ('frames', *HOLDS, 'ids')}


def _holds(reads: Mapping[str, list[str]]) -> dict[str, list[str]]:
    """What a page reads that its host holds a table for (HOLDS), as kept for a version."""
    return {kind: list(reads[kind]) for kind in HOLDS}
