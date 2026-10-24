"""ONE KEEPER PER PAGE (docs/DATACUBE_PAGE_KEEPER_2026_10_20.md): the one copy of each page a session's Python pages
hold, shown or not, and its version -- the host's storage, which never reads a page: what a page reads (its frames, its
copies, its ids) comes with each write from DataCube's own code (headless pageReads, pageRebase), and so
does every rule of how two editors' edits combine (page-rebase.ts).

THE HOST APPLIES THE COMMANDS (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S4): either editor -- Python's page, a tab or a
notebook widget open on it -- writes a page as the version it is at and the edits it made there (``settle``), never the
page itself. The keeper has DataCube apply them to the page as it is now (``combine``: headless pageRebase -- at the
writer's version there is nothing to combine; at one the page has moved on from they are made again on top) and keeps
what they make; edits that do not apply there are refused (``Refused``), nothing kept. So the page is what the commands
make, the two editors never hold two pages, and neither keeps a rule of its own for combining them. One write is a page
whole: a RESTORE (a page loaded, the plan's N1), taken only at the version it was made at. A write that leaves the page
as it was is no new version.

THE MIDDLE VERSION (docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md): every id an edit makes is its editor's own (a
tab's under its tag, page-ids.ts nextPageId), so the keeper renames nothing; a write it made another page of is answered
with that page, which the writer's screen is set to in one step; a page open on it follows by reading it whole; and each
tab's write carries its id (``writer``, ``seq``), taken once -- a write whose answer was lost is resolved by asking which
of its writes the keeper took (``snapshot``), never by sending it again blind.

EACH COPY IS THE KEEPER'S ONCE A PAGE READS IT (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z17): a snap's copy is its
maker's until a page the keeper keeps reads it -- its maker lets it go when it never lands (``has_read``) -- and from then
on goes only when nothing reads it any more (``letting_go``): no page as it is, and no page open on one at a version that
reads it, or any version since. Each open page says the version it shows (``follow``), as it asks and writes; one that
says nothing for ``FOLLOWED_FOR`` seconds (its tab closed, its machine asleep) keeps nothing. So a page open somewhere
that has not followed yet still reads its copies until it reads the page as it is (the keeper's known limit 6).

AND SO IS EACH FILE (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md X3, D14): a page's file read into a table, by the hash of its
bytes, and a remote file mounted as a view, by its URL -- what DataCube says a page reads beside its copies (headless
pageReads: ``files``, ``remotes``) -- is kept by the same rule: its reader's until a kept page reads it, the keeper's from
then on, let go when nothing reads it any more (``HOLDS``: each kind kept alike, by its key).
"""

from __future__ import annotations

import copy
import hashlib
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from . import _json

# how many versions back a page keeps what each read (its copies, files and remote files): a page open on an older one
# holds none of them past it (it follows by reading the page whole: docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md)
READS_KEPT = 64
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
    """A write kept: the page's version now; whether the keeper made another page of it than the writer's (its edits made
    again on top of a page that had moved on, or a write it had taken already); ``hash``, the page's at that version
    (``page_hash``), which the writer's own page must hash to (W17); and, combined, the page the keeper made and every id
    it has held -- read with the version, under one lock -- which the writer's screen is set to
    (docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md, the middle version)."""
    version: int
    combined: bool
    hash: str = ''
    page: dict[str, Any] | None = None
    held: list[str] | None = None


class _Taken(Exception):
    """A write its writer sent before, taken already: applied once."""


def page_hash(doc: Mapping[str, Any]) -> str:
    """The hash every editor checks a page by (W17): SHA-256 of the page's canonical JSON (_json.canonical: compact, keys
    in code point order, numbers their own digits), as DataCube's page-hash.ts computes it of the page it shows."""
    return hashlib.sha256(_json.canonical(doc).encode('utf-8')).hexdigest()


@dataclass
class _Kept:
    version: int
    doc: dict[str, Any]
    # what the page reads, as DataCube said with its last write: {frames, copies, files, remotes, ids}
    reads: dict[str, list[str]]
    # every id the page has held: none is given out again
    held: list[str]
    # what the page read at each version kept that its host holds (HOLDS: its copies, files, remote files): what a page
    # open at that version, or following past it, reads
    holds: dict[int, dict[str, list[str]]] = field(default_factory=dict)
    # the pages open on it, each by the id it asks by: the version it said it shows, and when (time.monotonic)
    followers: dict[str, tuple[int, float]] = field(default_factory=dict)
    # shown somewhere (a tab, a notebook widget): what an open page may read and write
    served: bool = False
    # the page's hash (page_hash), kept with each version: what an open page checks its own by (W17)
    hash: str = ''
    # the last write taken from each writer (a tab's id: its write's number), so one sent again is applied once
    taken: dict[str, int] = field(default_factory=dict)


# combine(page, held, edits): DataCube's pageRebase as the host asks it -- the page and the ids it has held now, a
# writer's edits -- {page, held, edits, reads}, or {problems}
Combine = Callable[[dict[str, Any], list[str], list[Any]], dict[str, Any]]
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
            self._pages[key] = _Kept(1, copy.deepcopy(doc), kept_reads, sorted(set(held)), {1: _holds(kept_reads)},
                                     hash=page_hash(doc))
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

    def hash_of(self, key: str) -> str:
        """The page's hash now (page_hash), or '' when the page is not kept."""
        with self._lock:
            kept = self._pages.get(key)
            return '' if kept is None else kept.hash

    def write(self, key: str, base: int, doc: dict[str, Any], reads: Mapping[str, list[str]], held: list[str]) -> int:
        """The page made ``doc`` (reading what ``reads`` says), at version ``base``: its new version -- or, the page no
        longer at ``base``, ``Stale``. A document the page has already is no new version. A page not kept is a
        ``KeyError``. What the keeper takes from its editors is ``settle``'s and ``restore``'s; this is theirs."""
        return self._write(key, base, doc, reads, held)[0]

    def _write(self, key: str, base: int, doc: dict[str, Any], reads: Mapping[str, list[str]], held: list[str],
               writer: str | None = None, seq: int = 0) -> tuple[int, str, dict[str, Any], list[str]]:
        """``write``, and the page at the version it answers -- its hash, its document (the keeper's own: read it, never
        change it) and every id it has held -- read with it, under the lock: another writer may move the page before the
        caller reads anything else (the D19 audit's 3). ``writer``'s write ``seq`` taken with it; one taken already is
        ``_Taken``, nothing written (two sends of one write, made again on top at once, keep it once)."""
        kept_doc = copy.deepcopy(doc)
        kept_reads = _reads(reads)
        with self._lock:
            kept = self._pages[key]
            if writer is not None and kept.taken.get(writer, 0) >= seq:
                raise _Taken()
            if kept.version != base:
                raise Stale(kept.version)
            kept.held = sorted(set(kept.held) | set(held) | set(kept_reads['ids']))
            if writer is not None:
                kept.taken[writer] = seq
            if kept_doc == kept.doc and kept_reads == kept.reads:
                return kept.version, kept.hash, kept.doc, list(kept.held)
            kept.version += 1
            kept.doc = kept_doc
            kept.hash = page_hash(kept_doc)
            kept.reads = kept_reads
            kept.holds[kept.version] = _holds(kept_reads)
            self._have_read(kept_reads)
            for old in [v for v in kept.holds if v <= kept.version - READS_KEPT]:
                del kept.holds[old]
            wrote = kept.version, kept.hash, kept.doc, list(kept.held)
        if self.on_write is not None:
            self.on_write(key, wrote[0])
        return wrote

    def settle(self, key: str, base: int, edits: list[Any], admit: Admit | None = None, writer: str | None = None,
               seq: int = 0) -> Settled:
        """A WRITE, SETTLED (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md S4, H1): ``edits``, made by a writer at version
        ``base``, applied by DataCube to the page as it is now (``combine``: headless pageRebase -- at ``base`` nothing to
        combine, past it made again on top) and what they make kept -- or ``Refused``: one does not apply there, in
        DataCube's words, or the host refuses the page they make (``admit``), nothing kept. Every id they make is their
        editor's own, so none is renamed (docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md). ``writer``'s write
        ``seq`` (a tab's) is taken once: sent again, it is answered with the page as it is now, which holds it."""
        if self.combine is None:
            raise Refused('this page keeps no rule to apply its edits by')
        for _ in range(COMBINED_AT_MOST):
            with self._lock:
                kept = self._pages[key]
                at, now, now_held = kept.version, kept.doc, list(kept.held)
                taken = writer is not None and kept.taken.get(writer, 0) >= seq
            if taken:
                return self._taken(key)
            if not 1 <= base <= at:
                raise Refused(f'the page is at version {at}, not {base}: it is shown as it is now')
            said = self.combine(now, now_held, edits)
            if 'problems' in said:
                raise Refused('the page changed where it is open while this change was made: ' + '; '.join(said['problems'])
                              if at != base else '; '.join(said['problems']))
            if admit is not None:
                refused = admit(said['reads'])
                if refused is not None:
                    raise Refused(refused)
            try:
                version, written_hash, doc, held = self._write(key, at, said['page'], said['reads'], said['held'], writer, seq)
            except Stale:
                continue
            except _Taken:
                return self._taken(key)
            # the page the writer's own version has -- never a later one another wrote meanwhile (the D19 audit's 3: the
            # page open on it checks its own against its hash)
            if at == base:
                return Settled(version, False, written_hash)
            return Settled(version, True, written_hash, doc, held)
        raise Refused('the page kept changing where it is open while this change was made: make it again')

    def _taken(self, key: str) -> Settled:
        """A write taken already, answered as one the keeper made another page of: the page as it is now, which holds it."""
        with self._lock:
            kept = self._pages[key]
            return Settled(kept.version, True, kept.hash, kept.doc, list(kept.held))

    def restore(self, key: str, base: int, doc: dict[str, Any], reads: Mapping[str, list[str]], held: list[str]) -> int:
        """A PAGE WRITTEN WHOLE (the plan's N1: a page loaded) -- the one write that is no commands -- kept only at the
        version it was made at: its version from then on; else ``Refused``, nothing kept (the page moved on: the writer
        opens it as it is)."""
        try:
            return self.write(key, base, doc, reads, held)
        except Stale:
            raise Refused('the page changed where it is open while this was loaded: it is shown as it is now') from None

    def snapshot(self, key: str, writer: str | None = None) -> tuple[int, dict[str, Any], list[str], str, int] | None:
        """The page whole, as a page open on it reads it: its version, its document (the keeper's own: read it, never
        change it), every id it has held, its hash, and the last write taken from ``writer`` (0: none, or no writer) --
        the five under one lock, so a write taken is in the page read with it -- or None."""
        with self._lock:
            kept = self._pages.get(key)
            if kept is None:
                return None
            return kept.version, kept.doc, list(kept.held), kept.hash, 0 if writer is None else kept.taken.get(writer, 0)

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
