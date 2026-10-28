"""
Emits the portable .pure artefacts: a ###Data element and the 12 services with testSuites.

Everything written here is plain Legend grammar. No part of it is specific to a runner, so
legend-engine executes it through TestableRunner and legend-lite can parse the identical
bytes — which is the portability claim, and parity.py checks it rather than asserting it.

Three details are load-bearing and were established from working examples in
legend-engine rather than guessed:

  * The connection key inside `data: [ connections: [ ... ] ]` is matched against the
    runtime's identifiedConnection **id** when the runtime uses the
    `connections: [ store: [ id: conn ] ]` shape. stress::RT does, and its id is
    `environment` — NOT the connection's element path `store::Conn`. Using the path
    silently seeds nothing and every test returns zero rows.
    (TestRuntimeBuilder.java)

  * `serializationFormat: PURE_TDSOBJECT;` produces `[{"col":val},...]`, which is the
    shape the oracle emits. The default (`DEFAULT`/`PURE`) instead produces
    `{"columns":[...],"rows":[{"values":[...]}]}`. Omitting the line is therefore a
    silent format mismatch, not an error. (TDSResult.getSerializer)

  * The classic `###Data` Relational form carries no store qualifier and no trailing
    semicolon after `}#`. The store-qualified form with `;` belongs to the newer
    `Relation #{...}#` grammar and does not parse here.

Also fixed here, because it is a real defect the old corpus could not see: all 12 services
declared `mapping: stress::RT`, pointing the mapping slot at a *Runtime*. Nothing
resolved that reference, so it survived. It becomes `stress::AllMapping`.
"""
from __future__ import annotations

import decimal
import re

import json

from model import Corpus
from query import Spec

DATA_ELEMENT = "stress::TestData"
EXTERNAL_DATA = "external::EntityData"
EXTERNAL_TABLES = {"EXT_LEGAL_ENTITY"}
# The hier:: feature domain has its own stores and therefore its own connection, so its
# tables cannot travel in the main ###Data element -- test data is keyed by CONNECTION.
# Derived from the model rather than listed, so a new hier table is routed correctly the
# moment it is declared instead of silently landing in the wrong element.
# Any store outside the main one needs its own ###Data element, so the main element has to
# know what to leave out.
#
# Derived from the table's DATABASE rather than from a list of store prefixes. The prefix
# list was a standing trap: a new store not on it had its tables emitted into the MAIN
# element, which is bound to store::DB, and the session then failed with `Table "LEG" not
# found in Schema "default" in Database(s) store::DB` -- an error naming the table rather
# than the routing. Every table now routes by where it was declared.
MAIN_STORE = "store::DB"
SIDE_STORES = ("hier::", "combo::")


def store_tables(c, prefix: str) -> set:
    return {n for n, t in c.tables.items()
            if getattr(t, "database", "").startswith(prefix) and n not in c.views}


def side_tables(c) -> set:
    """Tables belonging to a store the MAIN runtime does not connect.

    Follows the include closure rather than comparing one name. `store::DB` includes a
    linked project's store, and an included store's tables are created in the same physical
    database -- so treating them as a side store left them out of the data element entirely
    and every query over them failed with `Table with name CTN_BUCKET does not exist`, which
    reads as a missing table rather than as a table nobody seeded.
    """
    import model

    main = model.store_closure(c, MAIN_STORE)
    return {n for n, t in c.tables.items()
            if n not in c.views and getattr(t, "database", "") not in ({""} | main)}

CONNECTION_KEY = "environment"      # the identifiedConnection id in stress::RT
MAPPING = "stress::AllMapping"
RUNTIME = "stress::RT"
# A ###Data element keys rows by SCHEMA.TABLE. Hardcoding "default" was correct only
# while no table lived in a Schema block -- and a schema-qualified table seeded under
# `default.` is not seeded at all, so the mapping over it returns nothing.
SCHEMA = "default"


def _ident(name: str) -> str:
    """A relational identifier, re-quoted if it is not a bare one.

    The reader strips quotes so a quoted table keys the same as an unquoted one, which is
    right for lookups and wrong at EMIT: `default.spaced table:` is not parseable, so a
    seeded table with a quoted name produced a ###Data element that failed to parse and took
    the whole model down with it.
    """
    # SINGLE quotes, not double. A Database DDL takes `relationalIdentifier:
    # unquotedIdentifier | QUOTED_STRING` (double); a ###Data element takes `identifier:
    # unquotedIdentifier | STRING` (single). The same table is "spaced table" where it is
    # declared and 'spaced table' where it is seeded, and the declaration's form does not
    # parse in the data element.
    return name if re.fullmatch(r"[A-Za-z_]\w*", name) else f"'{name}'"


def _pure_str(s: str) -> str:
    """Escape for a single-quoted Pure string literal."""
    return s.replace("\\", "\\\\").replace("'", "\\'")


def _csv_cell(v) -> str:
    if v is None:
        return ""                     # an empty CSV field is NULL — proven, not assumed
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float):
        return repr(v)
    if isinstance(v, decimal.Decimal):
        return format(v, "f")         # never exponent notation: 1E-18 is not a CSV decimal
    s = str(v)
    # RFC4180 quoting. Verified against the engine before relying on it: a field wrapped in
    # double quotes carries an embedded comma correctly. Without this a JSON payload -- which
    # a Binding transformer needs -- could not travel in a ###Data element at all, and the
    # corpus guard rejected such values outright rather than emitting a broken row.
    if any(ch in s for ch in (",", '"', "\n")):
        return '"' + s.replace('"', '""') + '"'
    return s


def data_element(c: Corpus, tables: dict[str, list[dict]]) -> str:
    """One ###Data element holding every seeded table.

    Every column DECLARED on the table is emitted, not merely those the seed populates,
    so a column the seed omits is unambiguously NULL rather than absent-and-unspecified.
    """
    out = ["###Data", f"Data {DATA_ELEMENT}", "{", "  Relational", "  #{"]
    emitted = [(n, r) for n, r in tables.items()
               if n not in c.views and n not in EXTERNAL_TABLES
               and n not in side_tables(c)]
    for i, (name, rows) in enumerate(emitted):
        cols = list(c.tables[name].columns)
        lines = [",".join(cols)]
        lines += [",".join(_csv_cell(r.get(col)) for col in cols) for r in rows]
        out.append(f"    {_ident(c.tables[name].schema)}.{_ident(name)}:")
        body = [f"      '{_pure_str(l)}\\n'" for l in lines]
        out.append(" +\n".join(body) + ";")
        if i < len(emitted) - 1:
            out.append("")
    out += ["  }#", "}"]
    return "\n".join(out)


def _tests(spec: Spec, payload) -> str:
    """One test per execution key, one per parameter case, or a single unkeyed test.

    `payload` is the expected JSON as Pure string text, or for a spec with parameter cases a
    list of (test id, {parameter: value}, payload). A parameter a case leaves out is absent:
    the test binds nothing for it, which is how an optional parameter is given no value.
    """
    fmt = "" if spec.graph else "          serializationFormat: PURE_TDSOBJECT;\n"
    if spec.cases:
        tests = []
        for test_id, params, case_payload in payload:
            given = [f"            {n} = {_literal(params[n])}" for n, _t, _m in spec.params
                     if n in params]
            # NO trailing semicolon after the parameters' bracket, unlike serializationFormat:
            # `serviceTestParameters: PARAMETERS COLON BRACKET_OPEN ... BRACKET_CLOSE`
            # (73-service-surface.pure, where it was established).
            plines = ("          parameters:\n          [\n" + ",\n".join(given)
                      + "\n          ]\n") if given else ""
            tests.append(_one_test(test_id, plines + fmt, case_payload))
        return ",\n".join(tests)
    keys = [k for k, _, _ in spec.multi] or [None]
    out = []
    for k in keys:
        keyline = f"          keys:\n          [\n            '{k}'\n          ];\n" if k else ""
        out.append(_one_test(('expected_rows_' + k) if k else 'expected_rows', fmt + keyline,
                             payload))
    # Test elements are COMMA-separated. Without the comma the parser reports the
    # failure at `asserts` inside the FIRST test -- "Valid alternatives: [',', ']']" --
    # which points nowhere near the missing separator.
    return ",\n".join(out)


def _one_test(test_id: str, head: str, payload: str) -> str:
    return (f"        {test_id}:\n"
            f"        {{\n{head}          asserts:\n          [\n"
            f"            matchesOracle:\n              EqualToJson\n"
            f"              #{{\n                expected:\n"
            f"                  ExternalFormat\n                  #{{\n"
            f"                    contentType: 'application/json';\n"
            f"                    data: '{payload}';\n                  }}#;\n"
            f"              }}#\n          ]\n        }}")


def store_data_element(c: Corpus, tables: dict[str, list[dict]],
                       database: str, element: str, only=None) -> str:
    """`only` names the tables explicitly, for a store whose tables this reader cannot
    attribute to it. STORE SUBSTITUTION creates exactly that case: the substituted database
    redeclares the same table names with the same shapes, and table names are keyed globally
    here, so every such table is attributed to whichever database declared it first.
    """
    """A ###Data element for ONE side store.

    One element per STORE, not one for the domain. Test data is bound to a connection and a
    runtime connects a single store here, so an element carrying another store's tables is
    rejected at session setup with `Table "X" not found in Schema "default" in Database(s) Y`
    -- an error that names the table rather than the packaging, and so reads as a missing
    table rather than a misrouted one.
    """
    out = ["###Data", f"Data {element}", "{", "  Relational", "  #{"]
    names = sorted(only) if only is not None else sorted(
        n for n in side_tables(c)
        if tables.get(n) and getattr(c.tables[n], "database", "") == database)
    for i, name in enumerate(names):
        cols = list(c.tables[name].columns)
        lines = [",".join(cols)]
        lines += [",".join(_csv_cell(r.get(col)) for col in cols) for r in tables[name]]
        body = "\\n' +\n      '".join(lines)
        out.append(f"    {_ident(c.tables[name].schema)}.{_ident(name)}:")
        out.append(f"      '{body}\\n'" + (";" if i == len(names) - 1 else ";"))
    out += ["  }#", "}"]
    return "\n".join(out)


def external_data_element(c: Corpus, tables: dict[str, list[dict]]) -> str:
    """A SECOND ###Data element for the second store.

    Test data is keyed by CONNECTION, and the two stores have different connections, so
    one element cannot seed both — the tables of external::EntityDB have to travel
    separately or the cross-store side is simply empty.
    """
    out = ["###Data", f"Data {EXTERNAL_DATA}", "{", "  Relational", "  #{"]
    for name in sorted(EXTERNAL_TABLES):
        cols = list(c.tables[name].columns)
        lines = [",".join(cols)]
        lines += [",".join(_csv_cell(r.get(col)) for col in cols) for r in tables[name]]
        out.append(f"    {_ident(c.tables[name].schema)}.{_ident(name)}:")
        out.append(" +\n".join(f"      '{_pure_str(l)}\\n'" for l in lines) + ";")
    out += ["  }#", "}"]
    return "\n".join(out)


def _shorten(v):
    """Replace 4-byte column values with the shortest decimal that round-trips through them.

    json's C encoder calls float.__repr__ directly, so a float SUBCLASS cannot change how it
    serialises -- which is why this is a walk over the payload rather than a __repr__ on the
    value. The engine prints a FLOAT column as 16.1 and computes with 16.100000381469727;
    the value carries the second into arithmetic and this renders the first.
    """
    import flat

    if isinstance(v, dict):
        return {k: _shorten(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_shorten(x) for x in v]
    if isinstance(v, flat.F32):
        return float(repr(v))
    return v


# A Decimal in the payload travels as this marker and is written back as its digits: json writes
# a float through float.__repr__, which cannot carry 1.0000000000000001, and the engine's
# assertion compares numbers by value (1.0 is not 1.0000000000000001; 0.1 is 0.100000).
_DECIMAL = "\u0000decimal:"


def _decimal_text(d: decimal.Decimal) -> str:
    s = format(d, "f")
    # A fraction always, so the JSON number is a decimal like the engine's and never an integer.
    return s if "." in s else s + ".0"


def _payload(spec: Spec, rows: list[dict]) -> str:
    payload = rows if spec.graph is None else {"builder": {"_type": "json"}, "values": rows}
    text = json.dumps(_shorten(payload), separators=(",", ":"),
                      default=lambda o: _DECIMAL + _decimal_text(o)
                      if isinstance(o, decimal.Decimal) else _unserializable(o))
    text = re.sub(r'"\\u0000decimal:(-?[0-9.]+)"', r"\1", text)
    return _pure_str(text)


def _unserializable(o):
    raise TypeError(f"{type(o).__name__} {o!r} has no JSON form in an expected result")


def test_suite(spec: Spec, expected, note: str) -> str:
    """The testSuites block.

    A graph-fetch result is not a bare array: `serialize()` wraps it as
    `{"builder":{"_type":"json"},"values":[...]}`. Determined by running one and reading
    the actual, not assumed — an expectation written as a bare array fails with the two
    sides looking identical for the first 300 characters.
    """
    if spec.cases:
        payload = [(test_id, params, _payload(spec, rows)) for test_id, params, rows in expected]
    else:
        payload = _payload(spec, expected)
    return f"""  testSuites:
  [
    {spec.short}_suite:
    {{
      data:
      [
        connections:
        [
          {spec.connection or CONNECTION_KEY}:
            Reference
            #{{
              {spec.data_element or DATA_ELEMENT}
            }}#{_extra_connections(spec)}
        ]
      ]
      tests:
      [
        // {note}
{_tests(spec, payload)}
      ]
    }}
  ]"""


def _extra_connections(spec: Spec) -> str:
    """Additional (connection id, data element) pairs — a cross-store service needs one
    per store, since test data is bound to a connection and not to a runtime."""
    return "".join(f",\n          {cid}:\n            Reference\n            #{{\n"
                   f"              {elem}\n            }}#"
                   for cid, elem in spec.extra_data)


VAR = "x"


def _literal(v) -> str:
    import query

    if isinstance(v, bool):
        return "true" if v else "false"
    # Before the plain-string branch: DateArg IS a str, and quoting it would produce
    # `'2024-02-01'` where Pure wants `%2024-02-01`. The same for the other typed literals.
    if isinstance(v, query.DateArg):
        return f"%{v}"
    if isinstance(v, query.DateTimeArg):
        return f"%{v.replace(' ', 'T')}"
    if isinstance(v, query.StrictTimeArg):
        return f"%{v}"
    if isinstance(v, query.EnumArg):
        return f"{v.enum}.{v}"
    if isinstance(v, str):
        return f"'{_pure_str(v)}'"
    if isinstance(v, decimal.Decimal):
        return format(v, "f") + "D"
    if isinstance(v, query.Param):
        return f"${v.name}->toOne()" if v.to_one else f"${v.name}"
    if isinstance(v, query.PathArg):
        return f"${VAR}.{_path(v.path)}"
    if isinstance(v, list):
        return "[" + ", ".join(_literal(x) for x in v) + "]"
    return repr(v)


def _condition(f) -> str:
    """One filter conjunct as Pure text: a comparison, a bare boolean, a membership or absence
    test, or a tree of them (query.And, Or, Not, ParamEmpty)."""
    import query

    if isinstance(f, query.And):
        return "(" + " && ".join(_condition(x) for x in f.items) + ")"
    if isinstance(f, query.Or):
        return "(" + " || ".join(_condition(x) for x in f.items) + ")"
    if isinstance(f, query.Not):
        inner = _condition(f.item)
        return "!" + (inner if inner.startswith("(") else f"({inner})")
    if isinstance(f, query.ParamEmpty):
        return f"${f.name}->isEmpty()"
    path = f"${VAR}.{_path(f.path)}"
    # `is`/`not` are the BARE boolean forms -- `$x.flag` and `!$x.flag` -- with no
    # comparison. They exist because comparing a boolean to a boolean literal with `==`
    # is the one filter shape the engine cannot lower: `$x.isFinal == false` over a
    # derived Boolean reaches the database as SQL it rejects with "Parser Error: syntax
    # error at or near =", while the identical `!$x.isFinal` runs. See F50 and
    # scripts/corpus/probe_derived_filter.py.
    if f.op == "is":
        return path
    if f.op == "not":
        return f"!{path}"
    if f.op in ("isEmpty", "isNotEmpty"):
        return f"{path}->{f.op}()"
    if f.op == "in":
        return f"{path}->in({_literal(f.value)})"
    return f"({path} {f.op} {_literal(f.value)})"


def _date(d) -> str:
    """A milestoning date: `%latest`, a typed literal, a parameter, a path, or a plain ISO date."""
    import query

    if d == "latest":
        return "%latest"
    if isinstance(d, (query.DateArg, query.DateTimeArg, query.Param, query.PathArg)):
        return _literal(d)
    return "%" + d


def _as_of(spec: Spec) -> str:
    """A temporal root takes one date, or TWO for a bitemporal class: all(%P, %B)."""
    if spec.as_of is None:
        return ""
    dates = spec.as_of if isinstance(spec.as_of, list) else [spec.as_of]
    return ", ".join(_date(d) for d in dates)


def _root(spec: Spec) -> str:
    """The query's source: `Class.all(...)`, or every version, or the versions in a range."""
    v = spec.versions
    if v is None:
        return f"{spec.root}.all({_as_of(spec)})"
    if v.start is None:
        return f"{spec.root}.allVersions()"
    return f"{spec.root}->getAllVersionsInRange({_date(v.start)}, {_date(v.end)})"


def _step(step) -> str:
    """One step of a path: a property, or a milestoned one with its date(s) (query.Hop)."""
    import query

    if not isinstance(step, query.Hop):
        return step
    # Pure's grammar takes %latest in a property's arguments only when every one is %latest
    # (M3ParserGrammar's functionExpressionLatestMilestoningDateParameter), and one unparseable service
    # stops the whole corpus loading on the engine, so a mixed form is refused here.
    if step.dates and "latest" in step.dates and set(step.dates) != {"latest"}:
        raise ValueError(f"{step}: %latest beside another date is not Pure: {step.dates!r}")
    if step.versions == "all":
        return f"{step}AllVersions"
    name = f"{step}AllVersionsInRange" if step.versions == "range" else str(step)
    return name if step.dates is None else f"{name}({', '.join(_date(d) for d in step.dates)})"


def _path(path) -> str:
    return ".".join(_step(s) for s in path)


def _tree(node: dict, indent: str) -> str:
    parts = []
    for name, sub in node.items():
        if sub is None:
            parts.append(f"{indent}{_step(name)}")
        else:
            parts.append(f"{indent}{_step(name)}\n{indent}{{\n"
                         + _tree(sub, indent + "  ") + f"\n{indent}}}")
    return ",\n".join(parts)


def graph_text(spec: Spec) -> str:
    """`->graphFetch(#{...}#)->serialize(#{...}#)` — the tree is written twice, and the
    two must agree or the serializer emits a shape the fetch never populated."""
    tree = ("#{\n  " + spec.root + "\n  {\n" + _tree(spec.graph, "    ")
            + "\n  }\n}#")
    params = ", ".join(f"{n}: {t}[{m}]" for n, t, m in spec.params)
    return (f"    query: {{{params}|{_root(spec)}\n" if spec.params
            else f"    query: |{_root(spec)}\n") + (
            f"        ->graphFetch({tree})\n"
            f"        ->serialize({tree})" + ("};" if spec.params else ";"))


def query_text(spec: Spec) -> str:
    """Render the query from the parsed spec.

    The queries are regenerated rather than preserved because the originals were not
    valid Legend. Every one projected bare column names — `~[tradeId, instrName:t|...]` —
    against a Class source. A Class has no columns, so the bare form only means anything
    over a Relation. legend-engine rejects an all-bare array with a clean "Can't find a
    match for function 'project(Trade[*],ColSpecArray...)'", and crashes on the MIXED
    array with a raw ClassCastException out of processColSpecArray, which inspects only
    the first element to choose the array kind and then casts the rest to it.

    The canonical form is a FuncColSpec per column, `alias:x|$x.path`, which is what the
    bare names always meant here. Column aliases are unchanged, so the expectation JSON
    keys are identical.
    """
    params = ", ".join(f"{n}: {t}[{m}]" for n, t, m in spec.params)
    lines = [f"    query: {{{params}|{_root(spec)}" if spec.params
             else f"    query: |{_root(spec)}"]
    if spec.filters:
        conds = " && ".join(_condition(f) for f in spec.filters)
        lines.append(f"        ->filter({{{VAR}|{conds}}})")
    def expr(p):
        if p.func:
            inner = ", ".join(_literal(a) for a in p.args)
            return f"${VAR}->{p.func}({inner})"
        return (f"${VAR}.{_path(p.path)}"
                + (f"({', '.join(_literal(a) for a in p.args)})" if p.args else "")
                + (f"->{p.agg}()" if p.agg else ""))

    if spec.paradigm == "tds" and spec.group_by:
        # A legacy TDS groupBy operates on OBJECTS, straight off all() -- there is no
        # project() in front of it. Projecting first yields a TDS and the groupBy then
        # indexes past the end of it ("Index: 3 Size: 2"). This is also the only shape the
        # AggregationAware rewrite recognises.
        keys = ", ".join(f"{VAR}|${VAR}.{k}" for k in spec.group_by)
        aggs = ", ".join(f"agg({VAR}|${VAR}.{src}, y|$y->{fn}())"
                         for _, src, fn in spec.aggs)
        names = ", ".join(f"'{k}'" for k in spec.group_by)
        names += ", " + ", ".join(f"'{n}'" for n, _, _ in spec.aggs)
        lines.append(f"        ->groupBy(\n            [{keys}],\n"
                     f"            [{aggs}],\n            [{names}]\n        )")
    elif spec.paradigm == "tds":
        # Legacy TDS: the lambdas and the column names travel in two parallel lists.
        lams = [f"{VAR}|{expr(p)}" for p in spec.projections]
        names = [f"'{_pure_str(p.alias)}'" for p in spec.projections]
        lines.append("        ->project(")
        lines.append("            [")
        for i, l in enumerate(lams):
            lines.append(f"                {l}" + ("," if i < len(lams) - 1 else ""))
        lines.append("            ],")
        lines.append("            [" + ", ".join(names) + "]")
        lines.append("        )")
    else:
        cols = [f"{p.alias}:{VAR}|{expr(p)}" for p in spec.projections]
        lines.append("        ->project(~[")
        for i, col in enumerate(cols):
            lines.append(f"            {col}" + ("," if i < len(cols) - 1 else ""))
        lines.append("        ])")
    if spec.group_by and spec.paradigm != "tds":
        keys = ", ".join(spec.group_by)
        # The aggregate list is BRACKETED. The bare `~name: ... : ...` form parses only
        # with a SINGLE aggregate; with two it builds a colSpec whose function is null and
        # fails with "colSpec.function1 is null".
        parts = ",\n            ".join(
            f"{name}: {VAR}|${VAR}.{src} : agg|$agg->{fn}()"
            for name, src, fn in spec.aggs)
        lines.append(f"        ->groupBy(~[{keys}], ~[\n            {parts}\n        ])")
    if getattr(spec, "post_filters", None):
        # On the RELATION, so the operands are projected aliases and not model paths.
        conds = " && ".join(
            f"(${VAR}.{f.path[0]} {f.op} {_literal(f.value)})" for f in spec.post_filters)
        lines.append(f"        ->filter({{{VAR}|{conds}}})")
    if spec.sort:
        keys = spec.sort if isinstance(spec.sort, list) else [spec.sort]
        rendered = ", ".join(
            f"~{alias}->{'descending' if desc else 'ascending'}()" for alias, desc in keys)
        lines.append(f"        ->sort({rendered})" if len(keys) == 1
                     else f"        ->sort([{rendered}])")
    if spec.limit is not None:
        lines.append(f"        ->limit({spec.limit})")
    return "\n".join(lines) + ("};" if spec.params else ";")


def _multi_execution(spec: Spec) -> str:
    """`execution: Multi` — one query, several (mapping, runtime) bindings chosen by key.

    Expressing an invariance THIS way is stronger than two services: there is literally
    one query text, so the two runs cannot drift apart by editing.
    """
    body = [graph_text(spec) if spec.graph else query_text(spec),
            f"        key: '{spec.multi_key}';"]
    for key, mapping, runtime in spec.multi:
        body.append(f"        executions['{key}']:\n        {{\n"
                    f"            mapping: {mapping};\n"
                    f"            runtime: {runtime};\n        }}")
    return "\n".join(body)


def service(spec: Spec, expected, note: str) -> str:
    """`expected` is the rows, or for a spec with parameter cases (test id, params, rows) per
    case."""
    if spec.multi:
        exec_block = f"    execution: Multi\n    {{\n{_multi_execution(spec)}\n    }}"
    else:
        exec_block = (f"    execution: Single\n    {{\n"
                      f"{graph_text(spec) if spec.graph else query_text(spec)}\n"
                      f"        mapping: {spec.mapping or MAPPING};\n"
                      f"        runtime: {spec.runtime or RUNTIME};\n    }}")
    return f"""Service {spec.name}
{{
    pattern: '{spec.pattern}';
    documentation: '{_pure_str(spec.doc)}';
{exec_block}
{test_suite(spec, expected, note)}
}}"""

