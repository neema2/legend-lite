# Copyright 2026 Legend Contributors
# SPDX-License-Identifier: Apache-2.0
"""A PRODUCT RAISE, WORDED (#104's M2(b), 2026-11-15; the user's ruling of 2026-11-10: "a product error is its kind plus
where it happened ... never the value"): a statement lite planned raises its kind and its where inside an envelope
(sql.RaiseKind, the dialect's raise), and Python words it as Java's runner does (exec.RaisedErrors, error.DataError): the
kind's sentence, then its where. The envelope's marks are GENERATED from legend-lite (``_type_facts.py``), the same as
the tab's (engine-client/src/raised.ts); the sentences are the plans' own (phase D's option (D), 2026-11-15): each plan
and text plan the planner answers carries ``raises``, every product raise its statements can send, by kind, to its
sentence (PlanJson.raises, from error.ErrorKind), declared here as each answer is read (:func:`declare_raises`) -- one
source, never copied; a kind no answer declared is refused by name."""

from __future__ import annotations

import copy

from ._type_facts import RAISE_KIND_MARK, RAISE_POSITION_SEP, RAISE_QUOTE_MARK, RAISE_SENTINEL

# each kind a plan this process read declared, to its sentence (the planner's: one per kind)
_DECLARED: dict[str, str] = {}


def declare_raises(raises: object, what: str) -> None:
    """The raises a planner's answer carries (``raises``: kind to sentence), declared; refused by name where the answer
    has none (a plan of PlanJson v53 always carries them), one has no sentence, or a kind already declared is worded
    otherwise -- as the tab's declared-raises.ts."""
    if not isinstance(raises, dict):
        raise ValueError(f"{what} carries no raises (PlanJson v53's ``raises``, each kind its sentence)")
    for kind, sentence in raises.items():
        if not isinstance(sentence, str):
            raise ValueError(f"{what}'s raise {kind} has no sentence")
        held = _DECLARED.get(kind)
        if held is not None and held != sentence:
            raise ValueError(f"{what} words the raise {kind} {sentence!r}, and an earlier plan worded it {held!r}")
        _DECLARED[kind] = sentence


def raised_words(message: str) -> str | None:
    """The words of the raise inside a database's ``message``, as Java's runner reads one (RaisedErrors.unwrapped, kindOf,
    whereOf): the text between the first two sentinels, a source position before its separator dropped, a held quote
    restored; a kind's raise its sentence, then ``: `` and its where; any other raised text (a user's ``fail`` message)
    as it is. None where ``message`` holds no raise: a database's own error, passed on as it is. A kind no plan read here
    declared is refused by name (no default)."""
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
    sentence = _DECLARED.get(kind)
    if sentence is None:
        raise ValueError(f"a raise of the kind '{kind}', which no plan read here declares")
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
