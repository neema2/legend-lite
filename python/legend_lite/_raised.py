# Copyright 2026 Legend Contributors
# SPDX-License-Identifier: Apache-2.0
"""A PRODUCT RAISE, WORDED (#104's M2(b), 2026-11-15; phase D's Phase 2, raises as SITES, 2026-11-16): a statement lite
planned raises a bare token ``LG#<n>`` inside an envelope (the dialect's raise), and Python reads it as Java's runner does
(exec.RaisedErrors): site ``n`` of the statement it sent, from the sites the plans read here declared for that text
(``sites`` of each statement, PlanJson v56) -- its kind, its where, and its words rendered at planning: LITE'S own
``message`` alone (the edge program's H9, 2026-11-17: until then a site's ``upstream`` words, legend-engine's, were
preferred where the plan carried them; those are the edge's to say). The envelope's marks are GENERATED from legend-lite (``_type_facts.py``), the
same as the tab's (engine-client/src/raised.ts); each answer's sites are declared here as it is read
(:func:`declare_sites`) -- one source, never copied; a token no answer declared for the statement is refused by name."""

from __future__ import annotations

import copy
import json
import re
from typing import Any, NamedTuple

from ._type_facts import RAISE_POSITION_SEP, RAISE_QUOTE_MARK, RAISE_SENTINEL, RAISE_TOKEN

_DECLARED: dict[str, list[dict[str, Any]]] = {}

_TOKEN = re.compile(re.escape(RAISE_TOKEN) + r'([0-9]{1,9})')


def _sites_of(value: object, what: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise ValueError(f"{what}'s raise sites are not a list")
    for i, site in enumerate(value):
        if not isinstance(site, dict) or site.get('n') != i + 1 or not isinstance(site.get('kind'), str) \
                or not isinstance(site.get('message'), str):
            raise ValueError(f"{what}'s raise site {i + 1} is not a site numbered in order")
    return value


def declare_statement_sites(text: str, sites: list[dict[str, Any]], what: str) -> None:
    """The raise sites ``sites`` of the statement ``text``, declared; refused by name where the text was declared
    otherwise -- as the tab's declared-raises.ts."""
    held = _DECLARED.get(text)
    if held is not None and json.dumps(held, sort_keys=True) != json.dumps(sites, sort_keys=True):
        raise ValueError(f'{what} declares the raise sites of a statement otherwise than before')
    _DECLARED[text] = sites


def declare_sites(answer: object, what: str) -> None:
    """Every statement's sites a planner's answer carries (a plan, a text plan), declared: wherever an object of it
    holds ``sites``, each statement text beside them (``statement``, ``sql``, ``run``, ``statements``). Refused by name
    where the answer carries no ``raises`` (a plan of PlanJson v53 or later always does)."""
    if not isinstance(answer, dict) or not isinstance(answer.get('raises'), dict):
        raise ValueError(f"{what} carries no raises (PlanJson's ``raises`` and each statement's ``sites``)")

    def walk(node: object) -> None:
        if isinstance(node, list):
            for item in node:
                walk(item)
            return
        if not isinstance(node, dict):
            return
        if 'sites' in node:
            sites = _sites_of(node['sites'], what)
            for key in ('statement', 'sql', 'run'):
                if isinstance(node.get(key), str):
                    declare_statement_sites(node[key], sites, what)
            for text in node.get('statements') or []:
                if isinstance(text, str):
                    declare_statement_sites(text, sites, what)
        for value in node.values():
            walk(value)

    walk(answer)


def _inner(message: str) -> str | None:
    """The raise inside a database's ``message``: the text between its first two sentinels, a source position before its
    separator dropped, a held quote restored -- None where ``message`` holds no raise (the database's own error)."""
    first = message.find(RAISE_SENTINEL)
    last = -1 if first < 0 else message.find(RAISE_SENTINEL, first + 1)
    if first < 0 or last <= first:
        return None
    inner = message[first + 1:last].replace(RAISE_QUOTE_MARK, "'")
    sep = inner.find(RAISE_POSITION_SEP)
    return inner[sep + 1:] if sep >= 0 else inner


def _site(inner: str, statement: str) -> dict[str, Any] | None:
    """The site of ``statement`` the raise ``inner`` names by its token, or None where it names none (a user's ``fail``
    message raised as it is). A token the statement does not declare is refused by name (no default)."""
    token = _TOKEN.fullmatch(inner)
    if token is None or int(token.group(1)) < 1:
        return None
    n = int(token.group(1))
    sites = _DECLARED.get(statement, [])
    if n > len(sites):
        declared = ', '.join(f"{RAISE_TOKEN}{s['n']} {s['kind']}" for s in sites) or 'none'
        raise ValueError(f'the database raised {inner}, which the statement does not declare (it declares {declared})')
    return sites[n - 1]


def raised_words(message: str, statement: str) -> str | None:
    """The words of the raise inside a database's ``message``, as Java's runner reads one (RaisedErrors.unwrapped): the
    text between the first two sentinels, a source position before its separator dropped, a held quote restored; a
    site's token its planned words, read through the sites declared for ``statement``; any other raised text (a user's
    ``fail`` message) as it is. None where ``message`` holds no raise: a database's own error, passed on as it is. A token
    the statement does not declare is refused by name (no default)."""
    inner = _inner(message)
    if inner is None:
        return None
    site = _site(inner, statement)
    return inner if site is None else site['message']


def raised_kind(message: str, statement: str) -> str | None:
    """The kind of the failure ``message`` tells, as Java's runner tells it (exec.RaisedErrors, error.DataError): a
    site's token its site's kind; lite's envelope with no site (a user's ``fail``) none; no envelope at all, the
    database's own failure, ``DATABASE_ERROR`` (phase D's Phase 2, item 4, 2026-11-16: its words the database's,
    verbatim, which :func:`worded` leaves as they are). A token the statement does not declare is refused by name."""
    inner = _inner(message)
    if inner is None:
        return 'DATABASE_ERROR'
    site = _site(inner, statement)
    return None if site is None else site['kind']


class Raised(NamedTuple):
    """A raise's facts, as Java's DataError carries them (the PR #118 follow-up, 2026-11-19, Q9): its site's ``kind``
    and ``where`` -- both None for a user's ``fail``, which names no site; ``where`` None where the site has none."""
    kind: str | None
    where: str | None


_RAISED = 'legend_raised'


def raised_of(failure: BaseException) -> Raised | None:
    """The raise ``failure`` carries (:func:`worded` marks the copy it makes), or None: a failure lite planned no raise
    for -- the database's own, in its own words."""
    held = failure.__dict__.get(_RAISED)
    return held if isinstance(held, Raised) else None


def wire_name(kind: str) -> str:
    """A raise kind as the wire spells it (the lite API spec's §3: lower camel, as every wire name is, and as Java's
    LiteV1Api spells it): ``DIVIDE_BY_ZERO`` as ``divideByZero``."""
    head, *rest = kind.lower().split('_')
    return head + ''.join(part[:1].upper() + part[1:] for part in rest)


def worded(failure: BaseException, statement: str) -> BaseException | None:
    """``failure`` of ``statement`` again, of its type, its words the raise's (``raised_words``) and its raise's facts on
    it (:func:`raised_of`) -- or None where it holds no raise. The caller raises it outside its ``except``, as
    ``_reads.redacted``'s copy is, so the envelope's text is not chained."""
    inner = _inner(str(failure))
    if inner is None:
        return None
    site = _site(inner, statement)
    out = copy.copy(failure)
    out.__dict__.clear()
    out.args = (inner if site is None else site['message'],)
    setattr(out, _RAISED, Raised(None, None) if site is None else Raised(site['kind'], site.get('where')))
    return out
