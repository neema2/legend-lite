"""Protocol JSON with its numbers EXACT, as the compiler writes them.

A Pure decimal keeps its digits (``12.30`` is not ``12.3``) and an integer past 2**53 keeps
every digit: Python's ``int`` is exact already, and decimals are read as ``Decimal`` and written
back as their own text. The same rule as pure-protocol's exact JSON in TypeScript.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any


def loads(text: str) -> Any:
    """JSON text as Python values: objects as dicts (key order kept), decimals as ``Decimal``."""
    return json.loads(text, parse_float=Decimal)


def dumps(value: Any) -> str:
    """Python values as compact JSON text, ``Decimal`` written as its own digits."""
    out: list[str] = []
    _write(value, out)
    return ''.join(out)


def _write(v: Any, out: list[str]) -> None:
    if v is None:
        out.append('null')
    elif v is True:
        out.append('true')
    elif v is False:
        out.append('false')
    elif isinstance(v, Decimal):
        if not v.is_finite():
            raise ValueError(f'{v} is not a JSON number')
        out.append(str(v))
    elif isinstance(v, int):
        out.append(str(v))
    elif isinstance(v, float):
        if v != v or v in (float('inf'), float('-inf')):
            raise ValueError(f'{v} is not a JSON number')
        out.append(repr(v))
    elif isinstance(v, str):
        out.append(json.dumps(v, ensure_ascii=False))
    elif isinstance(v, dict):
        out.append('{')
        for i, (k, x) in enumerate(v.items()):
            if not isinstance(k, str):
                raise TypeError(f'a JSON object key must be a string, not {type(k).__name__}')
            if i:
                out.append(',')
            out.append(json.dumps(k, ensure_ascii=False))
            out.append(':')
            _write(x, out)
        out.append('}')
    elif isinstance(v, (list, tuple)):
        out.append('[')
        for i, x in enumerate(v):
            if i:
                out.append(',')
            _write(x, out)
        out.append(']')
    else:
        raise TypeError(f'{type(v).__name__} is not JSON')
