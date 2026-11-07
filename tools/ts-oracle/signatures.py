# Copyright 2026 Legend Contributors
# SPDX-License-Identifier: Apache-2.0
"""GS-QUANT'S SIGNATURES, READ FROM GS-QUANT ITSELF (docs/TS_DECISIONS_2026_11_07.md §4, the builders: "close if not
identical to gs-quant", 2026-11-07).

The builders (Python's DSL and TypeScript's, B3) spell each word as gs-quant does: its name, its parameters' names,
order and defaults, its types. They are generated from what this writes -- each word of core's catalog
(datacube/src/generated/ts-words.ts) that names gs-quant functions (its gsQuant tag), each function's signature as
inspect.signature reads it -- never from a memory of gs-quant. A word whose tag names no gs-quant function (DataCube's
own: rebase, drawdown, atr, ...) has none.

Run it with gs-quant installed (not one of the repository's locked requirements -- a manual check, as the oracle is);
its output is checked in, and a change of it is read before it is committed:

    python3 -m venv /tmp/gsq && /tmp/gsq/bin/pip install gs-quant==2.2.3
    /tmp/gsq/bin/python -I tools/ts-oracle/signatures.py datacube/src/generated/ts-words.ts \\
        > projects/timeseries/gs-quant-signatures.json
"""

import enum
import importlib
import inspect
import json
import re
import sys

import gs_quant
import gs_quant.timeseries as ts
from gs_quant.timeseries.helper import Window

VERSION = '2.2.3'


def catalog(path):
    """Each word of the generated catalog and the gs-quant functions its gsQuant tag names (`module.function`, one or
    several, comma-separated; a tag naming no function -- `DataCube (...)`, `none (...)` -- none)."""
    text = open(path, encoding='utf-8').read()
    words = {}
    for m in re.finditer(r"^  ([a-z_0-9]+): \{\n(.*?)^  \},$", text, re.M | re.S):
        tag = re.search(r"gsQuant: \[([^\]]*)\]", m.group(2))
        paths = []
        if tag is not None:
            for quoted in re.findall(r"'((?:[^'\\]|\\.)*)'", tag.group(1)):
                for part in quoted.split(','):
                    part = part.strip()
                    if re.fullmatch(r'[a-z_]+\.[a-z_0-9]+', part):
                        paths.append(part)
        words[m.group(1)] = paths
    return words


def resolve(path):
    module, _, name = path.rpartition('.')
    return getattr(importlib.import_module('gs_quant.timeseries.' + module), name)


def default_of(v):
    """A parameter's default as data: none, a value, a Window's size and ramp, an enumeration's member."""
    if v is inspect.Parameter.empty:
        return None
    if isinstance(v, Window):
        return {'window': [v.w if isinstance(v.w, (int, type(None))) else str(v.w),
                           v.r if isinstance(v.r, (int, type(None))) else str(v.r)]}
    if isinstance(v, enum.Enum):
        return {'enum': type(v).__name__, 'member': v.name}
    if v is None or isinstance(v, (bool, int, float, str)):
        return {'value': v}
    return {'repr': repr(v)}


def annotation(a):
    """A parameter's annotation as gs-quant writes it (its types' names, unqualified), none where it has none."""
    if a is inspect.Parameter.empty:
        return None
    text = a if isinstance(a, str) else getattr(a, '__name__', None) if isinstance(a, type) else str(a)
    return re.sub(r'\b(?:[a-z_][a-z0-9_]*\.)+([A-Za-z_][A-Za-z0-9_]*)', r'\1', text)


def enums_in(a):
    """The enumerations an annotation names (Returns, ThresholdType, ...), each once."""
    found = []
    stack = [a]
    while stack:
        t = stack.pop()
        if isinstance(t, type) and issubclass(t, enum.Enum):
            found.append(t)
        stack.extend(getattr(t, '__args__', ()) or ())
    return found


def main(path):
    if getattr(gs_quant, '__version__', VERSION) != VERSION:
        raise SystemExit(f'gs-quant {VERSION} is the one read (installed: {gs_quant.__version__})')
    out = {}
    enums = {}
    for word, paths in sorted(catalog(path).items()):
        if not paths:
            continue
        sigs = []
        for p in paths:
            params = []
            for q in inspect.signature(resolve(p)).parameters.values():
                params.append({'name': q.name, 'kind': q.kind.name, 'default': default_of(q.default),
                               'annotation': annotation(q.annotation)})
                for e in enums_in(q.annotation):
                    enums[e.__name__] = {m.name: m.value if isinstance(m.value, (str, int, float)) else str(m.value)
                                         for m in e}
            sigs.append({'function': p, 'params': params})
        out[word] = sigs
    json.dump({'gs_quant': VERSION, 'words': out, 'enums': enums}, sys.stdout, indent=1, sort_keys=True)
    sys.stdout.write('\n')


if __name__ == '__main__':
    main(sys.argv[1])
