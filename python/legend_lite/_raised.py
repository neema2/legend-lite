# Copyright 2026 Legend Contributors
# SPDX-License-Identifier: Apache-2.0
"""A PRODUCT RAISE, WORDED (#104's M2(b), 2026-11-15; the user's ruling of 2026-11-10: "a product error is its kind plus
where it happened ... never the value"): a statement lite planned raises its kind and its where inside an envelope
(sql.RaiseKind, the dialect's raise), and Python words it as Java's runner does (exec.RaisedErrors, error.DataError): the
kind's sentence, then its where. The envelope's marks and the sentences are GENERATED from legend-lite
(``_type_facts.py``, error.ErrorKind), the same as the tab's (engine-client/src/raised.ts): one source, never copied."""

from __future__ import annotations

import copy

from ._type_facts import (RAISE_KIND_MARK, RAISE_POSITION_SEP, RAISE_QUOTE_MARK, RAISE_SENTINEL,
                          RAISED_KINDS)


def raised_words(message: str) -> str | None:
    """The words of the raise inside a database's ``message``, as Java's runner reads one (RaisedErrors.unwrapped, kindOf,
    whereOf): the text between the first two sentinels, a source position before its separator dropped, a held quote
    restored; a kind's raise its sentence, then ``: `` and its where; any other raised text (a user's ``fail`` message)
    as it is. None where ``message`` holds no raise: a database's own error, passed on as it is. A kind legend-lite does
    not name is refused by name (no default)."""
    first = message.find(RAISE_SENTINEL)
    last = -1 if first < 0 else message.find(RAISE_SENTINEL, first + 1)
    if first < 0 or last <= first:
        return None
    inner = message[first + 1:last].replace(RAISE_QUOTE_MARK, "'")
    sep = inner.find(RAISE_POSITION_SEP)
    if sep >= 0:
        inner = inner[sep + 1:]
    if not inner.startswith(RAISE_KIND_MARK):
        return inner
    end = inner.find(RAISE_KIND_MARK, 1)
    kind = inner[1:] if end < 0 else inner[1:end]
    sentence = RAISED_KINDS.get(kind)
    if sentence is None:
        raise ValueError(f"a raise of the kind '{kind}', which legend-lite does not name")
    return sentence if end < 0 else f'{sentence}: {inner[end + 1:]}'


def worded(failure: BaseException) -> BaseException | None:
    """``failure`` again, of its type, its words the raise's (``raised_words``) -- or None where it holds no raise. The
    caller raises it outside its ``except``, as ``_reads.redacted``'s copy is, so the envelope's text is not chained."""
    words = raised_words(str(failure))
    if words is None:
        return None
    out = copy.copy(failure)
    out.__dict__.clear()
    out.args = (words,)
    return out
