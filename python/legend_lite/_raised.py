# Copyright 2026 Legend Contributors
# SPDX-License-Identifier: Apache-2.0
"""A PRODUCT RAISE, WORDED (#104's M2(b), 2026-11-15; phase D's Phase 2, raises as SITES, 2026-11-16): a statement lite
planned raises a bare token ``LG#<n>`` inside an envelope (the dialect's raise), and Python reads it as Java's runner does
(exec.RaisedErrors): site ``n`` of the statement it sent, from the sites the plans read here declared for that text
(``sites`` of each statement, PlanJson v59) -- its kind and its operands, worded HERE in the one catalog's words
(``RAISE_FORMS``, generated from error.ErrorKind: the plan carries a site's facts and no words since stage B's B1,
2026-11-20; the catalog's words are upstream's wherever upstream has one, stage B's BE, 2026-11-21). The envelope's marks are GENERATED from legend-lite (``_type_facts.py``), the
same as the tab's (engine-client/src/raised.ts); each answer's sites are declared here as it is read
(:func:`declare_sites`) -- one source, never copied; a token no answer declared for the statement is refused by name."""

from __future__ import annotations

import copy
import json
import math
import re
from typing import Any

from ._type_facts import RAISE_FORMS, RAISE_POSITION_SEP, RAISE_QUOTE_MARK, RAISE_SENTINEL, RAISE_TOKEN

_DECLARED: dict[str, list[dict[str, Any]]] = {}

_TOKEN = re.compile(re.escape(RAISE_TOKEN) + r'([0-9]{1,9})')


def _sites_of(value: object, what: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise ValueError(f"{what}'s raise sites are not a list")
    for i, site in enumerate(value):
        if not isinstance(site, dict) or site.get('n') != i + 1 or not isinstance(site.get('kind'), str) \
                or 'message' in site:
            raise ValueError(f"{what}'s raise site {i + 1} is not a site numbered in order (facts, no words)")
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
    where the answer carries no ``raises``, the kinds its statements can send (a plan of PlanJson v59 always does)."""
    if not isinstance(answer, dict) or not isinstance(answer.get('raises'), list):
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


def raised_words(message: str, statement: str) -> str | None:
    """The words of the raise inside a database's ``message``, as Java's runner reads one (RaisedErrors.unwrapped): the
    text between the first two sentinels, a source position before its separator dropped, a held quote restored; a
    site's token its planned words, read through the sites declared for ``statement``; any other raised text (a user's
    ``fail`` message) as it is. None where ``message`` holds no raise: a database's own error, passed on as it is. A token
    the statement does not declare is refused by name (no default)."""
    first = message.find(RAISE_SENTINEL)
    last = -1 if first < 0 else message.find(RAISE_SENTINEL, first + 1)
    if first < 0 or last <= first:
        return None
    inner = message[first + 1:last].replace(RAISE_QUOTE_MARK, "'")
    sep = inner.find(RAISE_POSITION_SEP)
    if sep >= 0:
        inner = inner[sep + 1:]
    token = _TOKEN.fullmatch(inner)
    if token is None or int(token.group(1)) < 1:
        return inner
    n = int(token.group(1))
    sites = _DECLARED.get(statement, [])
    if n > len(sites):
        declared = ', '.join(f"{RAISE_TOKEN}{s['n']} {s['kind']}" for s in sites) or 'none'
        raise ValueError(f'the database raised {inner}, which the statement does not declare (it declares {declared})')
    site = sites[n - 1]
    return sentence(site)


def raised_site(message: str) -> int | None:
    """The number of the site whose token a database's ``message`` raises, read as :func:`raised_words` reads it; None
    where it raises none (a database's own error, a user's ``fail`` text)."""
    first = message.find(RAISE_SENTINEL)
    last = -1 if first < 0 else message.find(RAISE_SENTINEL, first + 1)
    if first < 0 or last <= first:
        return None
    inner = message[first + 1:last].replace(RAISE_QUOTE_MARK, "'")
    sep = inner.find(RAISE_POSITION_SEP)
    if sep >= 0:
        inner = inner[sep + 1:]
    token = _TOKEN.fullmatch(inner)
    return None if token is None or int(token.group(1)) < 1 else int(token.group(1))


def engine_words(failure: BaseException, site_words: object) -> str | None:
    """legend-pure's words of the raise ``failure`` is, at legend-engine's door: its site (the number :func:`worded`
    kept on it, or a token still in its words), worded by the edge's table beside the plan (``siteWords``: each site's
    ``n`` and ``words``, worded in Java from the site's record -- stage B's B1, 2026-11-20, the plan carrying no words).
    None where it is no raise of a site (a database's own error, a user's ``fail`` text). A site the table words none
    for is refused by name (no default)."""
    n = getattr(failure, 'raise_site', None)
    if n is None:
        n = raised_site(str(failure))
    if n is None:
        return None
    for entry in site_words if isinstance(site_words, list) else []:
        if isinstance(entry, dict) and entry.get('n') == n and isinstance(entry.get('words'), str):
            return entry['words']
    raise ValueError(f'the database raised {RAISE_TOKEN}{n}, which the answer words no site for')


_COMPUTED = '<computed>'
_PLACEHOLDER = re.compile(r'%(\d+)\$s')
_DATE_FROM_MINUTE = re.compile(r'T\d{2}:\d{2}')
_NUMBER = re.compile(r'^(-?)(\d+)(?:\.(\d+))?(?:[eE]([+-]?\d+))?$')


def sentence(site: dict[str, Any]) -> str:
    """A site's sentence, the one catalog's (error.ErrorKind's forms, generated: ``RAISE_FORMS``): the first form that
    takes the operands, each operand printed as the form's slot says, a value the database computed ``<computed>`` --
    what Java's runner says (result.RaiseWords). A kind or a count of operands no form says is refused by name."""
    kind = site['kind']
    forms = RAISE_FORMS.get(kind)
    if forms is None:
        raise ValueError(f'a raise of kind {kind}, which this host has no sentence for')
    ops = site.get('operands') or []

    def takes(form: dict[str, Any]) -> bool:
        if form['arity'] >= 0 and form['arity'] != len(ops):
            return False
        # a form takes the operands its sentence names, at least (result.RaiseWords refuses fewer by name, as here)
        if len(ops) < max([int(n) for n in _PLACEHOLDER.findall(form['template'])] or [0]):
            return False
        slot = form['emptySlot']
        return slot < 0 or (slot < len(ops) and ops[slot].get('string') == '')

    form = next((f for f in forms if takes(f)), None)
    if form is None:
        raise ValueError(f'no sentence of {kind} says {len(ops)} values')
    slots = form['slots']
    values = [_print(o, slots[i] if i < len(slots) else 'TEXT') for i, o in enumerate(ops)]
    return _PLACEHOLDER.sub(lambda m: values[int(m.group(1)) - 1], form['template'])


def _print(operand: dict[str, Any], slot: str) -> str:
    key, value = next(iter(operand.items()))
    if key == 'computed':
        return _COMPUTED
    if slot == 'DATE' and key == 'string':
        return f'{value}+0000' if _DATE_FROM_MINUTE.search(value) else value
    if slot == 'FLOAT' and key in ('float', 'integer', 'decimal'):
        return _carried(float(value))
    if slot == 'WHOLE' and key == 'float':
        # a Double's intValue, as legend-pure's date() validates a fractional second (Java's narrowing: toward zero,
        # saturated at the int range, NaN 0; the BE audit 3's S3)
        x = float(value)
        return '0' if math.isnan(x) else str(max(-2147483648, min(2147483647, int(x) if math.isfinite(x)
                                                              else (2147483647 if x > 0 else -2147483648))))
    if slot == 'WHOLE' and key == 'decimal':
        # a BigDecimal's intValue: its whole part's low 32 bits, two's complement
        w = int(_plain(str(value), True).split('.')[0])
        return str((w + 2 ** 31) % 2 ** 32 - 2 ** 31)
    return _text(key, value)


def _text(key: str, value: Any) -> str:
    """As the program wrote it: a text itself, an Integer's digits, a Float as Pure prints one, a Decimal's plain
    digits, a Boolean's word, a date's ISO text."""
    if key == 'float':
        return _carried(float(value))
    if key == 'decimal':
        return _plain(str(value), True)
    if key == 'boolean':
        return 'true' if value else 'false'
    return str(value)


def _carried(d: float) -> str:
    """A Float as Pure prints one (result.CellText.carriedFloat): its shortest digits laid out with a point always,
    never an exponent."""
    # Java's Double.toString for the values that have no digits (result.CellText.carriedFloat)
    if math.isnan(d):
        return 'NaN'
    if math.isinf(d):
        return 'Infinity' if d > 0 else '-Infinity'
    p = _plain(repr(d), False)
    return p if '.' in p else p + '.0'


def _plain(s: str, keep: bool) -> str:
    """``s``, a number's digits possibly with an exponent, laid out plainly: every digit kept where ``keep``
    (BigDecimal.toPlainString), trailing fraction zeros dropped otherwise."""
    m = _NUMBER.match(s)
    if m is None:
        return s
    sign, whole, frac, exp = m.group(1), m.group(2), m.group(3) or '', int(m.group(4) or '0')
    digits = whole + frac
    point = len(whole) + exp
    if point <= 0:
        digits = '0' * (1 - point) + digits
        point = 1
    elif point > len(digits):
        digits = digits + '0' * (point - len(digits))
    head = digits[:point].lstrip('0') or '0'
    tail = digits[point:]
    if not keep:
        tail = tail.rstrip('0')
    return sign + head + ('.' + tail if tail else '')


def raised_kind(message: str, statement: str) -> str | None:
    """The kind of the failure ``message`` tells, as Java's runner tells it (exec.RaisedErrors, error.DataError): a
    site's token its site's kind; lite's envelope with no site (a user's ``fail``) none; no envelope at all, the
    database's own failure, ``DATABASE_ERROR`` (phase D's Phase 2, item 4, 2026-11-16: its words the database's,
    verbatim, which :func:`worded` leaves as they are). A token the statement does not declare is refused by name."""
    first = message.find(RAISE_SENTINEL)
    last = -1 if first < 0 else message.find(RAISE_SENTINEL, first + 1)
    if first < 0 or last <= first:
        return 'DATABASE_ERROR'
    inner = message[first + 1:last].replace(RAISE_QUOTE_MARK, "'")
    sep = inner.find(RAISE_POSITION_SEP)
    if sep >= 0:
        inner = inner[sep + 1:]
    token = _TOKEN.fullmatch(inner)
    if token is None or int(token.group(1)) < 1:
        return None
    raised_words(message, statement)   # an undeclared token refused by name, as the words are
    return _DECLARED[statement][int(token.group(1)) - 1]['kind']


def worded(failure: BaseException, statement: str) -> BaseException | None:
    """``failure`` of ``statement`` again, of its type, its words the raise's (``raised_words``) -- or None where it holds
    no raise. The caller raises it outside its ``except``, as ``_reads.redacted``'s copy is, so the envelope's text is not
    chained."""
    words = raised_words(str(failure), statement)
    if words is None:
        return None
    out = copy.copy(failure)
    out.__dict__.clear()
    out.args = (words,)
    # the site it raised, kept for legend-engine's door, which words it its own way (engine_words; stage B's B1)
    site = raised_site(str(failure))
    if site is not None:
        out.raise_site = site
    return out
