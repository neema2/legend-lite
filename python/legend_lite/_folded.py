"""THE NATIVE LIBRARY'S ANSWERS, READ ONCE (H4's P3, the user's CP-13 of 2026-11-19): every entry answers in the one text
encoding both strings-only hosts speak (``planner.Folded``, written once in Java) -- ``OK`` and the answer, a refusal as
``ERR`` and lite's error ``{kind, message, where?, raise?}`` (the lite API spec's §3), an HTTP answer as ``OK``, its
status, its media type and its body, a store's answer as ``OK``, its status and its body -- and this module is Python's
one reader of it, and its one writer of a refusal.
Until then three readers still read the layout retired on 2026-11-17 (``ERR``, a kind, a message: ``_home``'s two and
the edge's store), so a refusal surfaced as its raw JSON text, and eight more read the tag by hand.

The seam's: the planner's modules and the run side's read it alike (the warehouse's wire too). A refusal is raised as
``LegendError``, lite's one error shape; whether its kind is one the product says is the compiler's to check
(``compiler._answer``), which asks the product its kinds.
"""

from __future__ import annotations

from typing import Any

from . import _json


class LegendError(Exception):
    """The compiler refused, in lite's one error shape (the lite API spec's §3, 2026-11-17): its ``kind`` -- LITE'S OWN,
    as the native library folds it (``LiteV1Api.Kind``; never a constant of Python's: ``compiler.refusal_kinds``) --
    its ``message`` in lite's words, and, where there is one, its ``where`` (a position or a site) and, for a run's
    raise, the raise's own kind (``raise_``)."""

    def __init__(self, message: str, kind: str, where: str | None = None, raise_: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.kind = kind
        self.where = where
        self.raise_ = raise_


def unfold(text: str) -> str:
    """An entry's answer (``OK`` and the answer), or its refusal raised (``ERR`` and lite's error: ``LegendError``). A
    refusal in no error shape of lite's, or an answer in neither form, is the library's own failure (``OSError``),
    said with what it answered -- never read as a refusal by a guess."""
    tag, _, rest = text.partition('\n')
    if tag == 'OK':
        return rest
    if tag == 'ERR':
        try:
            said = _json.loads(rest)
        except ValueError:
            said = None
        if not (isinstance(said, dict) and isinstance(said.get('kind'), str) and isinstance(said.get('message'), str)):
            raise OSError(f"the native library refused in no error shape lite's (§3): {rest[:200]!r}")
        raise LegendError(said['message'], said['kind'], said.get('where'), said.get('raise'))
    raise OSError(f'the native library answered something unrecognised: {text[:120]!r}')


def unfold_http(text: str) -> tuple[int, str, str]:
    """An entry's HTTP answer (``OK``, its status, its media type and its body: ``Folded.http``), its status, media type
    and body; a refusal in its place (the call failed outside the answer) raised as ``unfold`` raises it. A binary body
    folded as base64 (``Folded.BASE64``'s suffix on the media type) is the caller's to decode, under that media type."""
    rest = unfold(text)
    status, _, rest = rest.partition('\n')
    media, _, body = rest.partition('\n')
    if not (status.isascii() and status.isdigit() and media):
        raise OSError(f'the native library answered no HTTP answer: {text[:120]!r}')
    return int(status), media, body


def unfold_status(text: str) -> tuple[int, str]:
    """A store's or the model home's folded answer (``OK``, its status and its body: the stores' and the SDLC's entries,
    ``SavedStores``, ``ModelHome``), its status and body; a refusal in its place raised as ``unfold`` raises it."""
    rest = unfold(text)
    status, _, body = rest.partition('\n')
    if not (status.isascii() and status.isdigit()):
        raise OSError(f'the native library answered no status: {text[:120]!r}')
    return int(status), body


def refusal(error: LegendError) -> str:
    """``error`` folded as an entry folds a refusal (``ERR`` and lite's error): what a host answers in the planner's
    place, read by the same readers as the planner's own."""
    said: dict[str, Any] = {'kind': error.kind, 'message': error.message}
    if error.where is not None:
        said['where'] = error.where
    if error.raise_ is not None:
        said['raise'] = error.raise_
    return 'ERR\n' + _json.dumps(said)
