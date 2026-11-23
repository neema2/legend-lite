"""THE PLAN'S RECORDS AND ITS OWN DATA'S READERS (C5-P, 2026-11-15: moved out of compiler.py, which re-exports each, so the
run side reads a plan without importing the planner -- Java's records, which //core:exec and the planner both depend on):
a planned query (``Plan``) and its columns (``Column``), the values a plan's statement binds that the plan carries
(``plan_params``, ``statement_params``), and a host's rows written as the compiler reads them back (``host_rows``). None
of it calls the native library: Python's standard library alone, as the compiler's bindings are."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

from . import _json
from ._planned import PlannedSql


@dataclass(frozen=True)
class Column:
    """A result column as the compiler typed it."""

    name: str
    type: str
    raw: dict[str, Any] = field(hash=False, compare=False)

    @property
    def sql_type(self) -> str | None:
        """Its SQL type, when the plan says it (a server's ``tdsColumns`` carry ``relationalType``: the one typing
        table's pair, the program's G9)."""
        found = self.raw.get("relationalType") if isinstance(self.raw, dict) else None
        return found if isinstance(found, str) else None


class PlanParametersRefused(ValueError):
    """A plan's parameter declaration not as the planner writes it: refused by name, nothing run."""


@dataclass(frozen=True)
class Declared:
    """A plan's parameter as PlanJson.parameterJson writes it: its name, Pure type, multiplicity (``upper`` None for
    none) and, for an enumeration, its values' names (the lite API spec's §4, 2026-11-17)."""

    name: str
    type: str
    lower: int
    upper: int | None
    enum_values: tuple[str, ...]

    @property
    def typed(self) -> str:
        """``Type[multiplicity]``, as the words name a parameter."""
        return f"{self.type}[{multiplicity(self.lower, self.upper)}]"


def multiplicity(lower: int, upper: int | None) -> str:
    """``[lower..upper]`` as Pure spells it: ``*``, ``1``, ``1..*``, ``0..1``."""
    if upper is None:
        return "*" if lower == 0 else f"{lower}..*"
    return str(lower) if lower == upper else f"{lower}..{upper}"


def _whole(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def declared(d: Any) -> Declared:
    """One declaration read strictly -- exactly ``{name, type, multiplicity: {lower, upper?}, enumValues}`` -- any other
    refused by name, never read loosely."""
    m = d.get("multiplicity") if isinstance(d, dict) else None
    enum_values = d.get("enumValues") if isinstance(d, dict) else None
    if not (
        isinstance(d, dict)
        and set(d) == {"name", "type", "multiplicity", "enumValues"}
        and isinstance(d["name"], str)
        and isinstance(d["type"], str)
        and isinstance(m, dict)
        and set(m) <= {"lower", "upper"}
        and _whole(m.get("lower"))
        and ("upper" not in m or _whole(m.get("upper")))
        and isinstance(enum_values, list)
        and all(isinstance(v, str) for v in enum_values)
    ):
        raise PlanParametersRefused(
            f"a plan parameter declared without its name, type, multiplicity and enumeration's values: {_json.dumps(d)}"
        )
    return Declared(d["name"], d["type"], m["lower"], m.get("upper"), tuple(enum_values))


@dataclass(frozen=True)
class Plan:
    """A query planned: the SQL to run, the compiler's type of what it returns, and the values its ``?``s bind, in
    order -- the plan's own data: payloads (``plan_params``: the tenets audit's M3); none for a text that runs alone.
    A plan that DECLARES parameters (``parameters``) binds them at RUN (the lite API spec's §4, 2026-11-17): its slots
    in placeholder order (``slots``), each a payload's text or a parameter's name, bound by the run side
    (``_params.bind``)."""

    sql: PlannedSql
    columns: tuple[Column, ...]
    params: tuple[str, ...] = ()
    parameters: tuple[Declared, ...] = ()
    slots: tuple[tuple[str, Any], ...] = ()


class PlanParamsRefused(ValueError):
    """A plan whose statement binds what a host running its text does not bind, or whose payload is not the data it was
    planned against: refused by name."""


def plan_binding(plan: Any) -> tuple[tuple[str, ...], tuple[Declared, ...], tuple[tuple[str, Any], ...]]:
    """A typed result's plan read for its run (the lite API spec's §4, 2026-11-17): its payloads (``plan_params``' rule),
    its declared parameters (``_params.declared``, each read strictly) and its slots in placeholder order -- a payload's
    text, or the declared parameter a ``one`` slot binds. A ``one`` slot of no declared parameter, and any other slot
    (a key's cell, a file's path, a list), refused by name; a dynamic pivot that takes parameters is not run here.
    ``plan``: the execution plan (PlanJson, parsed), or None for a text that runs alone."""
    if plan is None:
        return (), (), ()
    if not isinstance(plan, dict):
        raise PlanParamsRefused(f"a plan is a JSON object, not {plan!r}")
    listed = plan.get("parameters") or []
    if not isinstance(listed, list):
        raise PlanParamsRefused(f"a plan's parameters are a list, not {listed!r}")
    declarations = tuple(declared(d) for d in listed)
    root = plan.get("root")
    if declarations and isinstance(root, dict) and root.get("_type") == "keyed":
        raise PlanParamsRefused("a dynamic pivot that takes parameters is not run here")
    if not declarations:
        payloads = plan_params(plan)
        return payloads, (), tuple(("payload", p) for p in payloads)
    statement = root.get("sql") if isinstance(root, dict) else None
    if not isinstance(statement, dict):
        raise PlanParamsRefused(
            f"a text plan's statement is its root's (a typed result's), not a "
            f"{root.get('_type') if isinstance(root, dict) else root!r}'s"
        )
    names = {d.name for d in declarations}
    payloads = statement_params(statement, names)
    slots: list[tuple[str, Any]] = []
    texts = iter(payloads)
    for slot in statement["slots"]:
        if slot["binding"].get("_type") == "payload":
            slots.append(("payload", next(texts)))
        else:
            slots.append(("one", slot["parameter"]))
    return payloads, declarations, tuple(slots)


def plan_params(plan: Any) -> tuple[str, ...]:
    """THE VALUES A PLAN'S STATEMENT BINDS THAT THE PLAN CARRIES (the tenets audit's M3, 2026-11-11; engine-client
    plan-params.ts, the same rule): each data: payload -- the plan's own value, one JSON array -- read from the answering
    statement's content by the digest its slot names, its bytes checked against that SHA-256, once, here where the host
    reads the plan; any other slot (a parameter the run supplies, a key's cell, a file's path, a list) refused by name.
    ``plan``: the execution plan (PlanJson, parsed), or None for a text that runs alone."""
    if plan is None:
        return ()
    root = plan.get("root") if isinstance(plan, dict) else None
    statement = root.get("sql") if isinstance(root, dict) else None
    if not isinstance(statement, dict):
        raise PlanParamsRefused(
            f"a text plan's statement is its root's (a typed result's), not a "
            f"{root.get('_type') if isinstance(root, dict) else root!r}'s"
        )
    return statement_params(statement)


def statement_params(
    statement: dict[str, Any], declared_names: frozenset[str] | set[str] = frozenset()
) -> tuple[str, ...]:
    """The payloads ``statement``'s slots bind (``plan_params``' rule), in its placeholders' order. What the planner always
    writes is required, never defaulted (G7; the final audit, 2026-11-12, plan-params.ts's twin): a statement with no
    slots, and a payload its statement does not carry (no digest, no content), each refused by name. A ``one`` slot of a
    parameter in ``declared_names`` is the run's to bind (``plan_binding``), its ``asText`` too; any other refused."""
    if "slots" not in statement:
        raise PlanParamsRefused("this plan's statement does not carry its slots")
    content = statement.get("content")
    out = []
    for slot in statement["slots"]:
        kind = slot["binding"].get("_type")
        if kind == "one" and slot["parameter"] in declared_names:
            continue
        if kind != "payload":
            what = {"key": "a key's cell", "source": "a file source's path", "array": "a list"}.get(
                kind, "a value the run supplies"
            )
            raise PlanParamsRefused(
                f"'{slot['parameter']}' binds {what}, which a host running the plan's text does not bind"
            )
        digest = slot["binding"].get("digest")
        if digest is None or content is None:
            raise PlanParamsRefused(f"'{slot['parameter']}' binds a payload its statement does not carry")
        text = content.get(digest)
        if text is None:
            raise PlanParamsRefused(
                f"the plan's statement binds the payload {digest}, which its content does not carry"
            )
        actual = hashlib.sha256(text.encode("utf-8")).hexdigest()
        if actual != digest:
            raise PlanParamsRefused(
                f"the data: source's payload is not the data it was planned against: its digest is "
                f"{actual}, where the plan names {digest}"
            )
        out.append(text)
    return tuple(out)


def host_rows(names: list[str], rows: list[tuple]) -> str:
    """A result's rows as the compiler reads a host's rows back (``PureV1Api.jsonAnswer``): a JSON array of objects,
    each a column's name and its cell exactly as the driver read it -- an integer's digits, a float's shortest digits
    (a non-finite one as its name in a string), a decimal, a date, a time and a date-time as their text, a text, a
    boolean, null, a structure as its JSON with the same rules. The plan's columns say how each reads."""
    return (
        "["
        + ",".join("{" + ",".join(_json.dumps(n) + ":" + _cell(v) for n, v in zip(names, row)) + "}" for row in rows)
        + "]"
    )


def _cell(v) -> str:
    import datetime
    import decimal
    import math
    import uuid

    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if math.isfinite(v):
            return repr(v)
        return _json.dumps("NaN" if math.isnan(v) else ("Infinity" if v > 0 else "-Infinity"))
    if isinstance(v, decimal.Decimal):
        return _json.dumps(str(v))
    if isinstance(v, datetime.datetime):
        # a zoned value at UTC, its offset dropped: the plan's session is UTC (Boundary.sessionSetup)
        if v.tzinfo is not None:
            v = v.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return _json.dumps(v.isoformat())
    if isinstance(v, (datetime.date, datetime.time)):
        return _json.dumps(v.isoformat())
    if isinstance(v, str):
        return _json.dumps(v)
    if isinstance(v, uuid.UUID):
        return _json.dumps(str(v))
    if isinstance(v, (list, tuple)):
        return "[" + ",".join(_cell(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{" + ",".join(_json.dumps(str(k)) + ":" + _cell(x) for k, x in v.items()) + "}"
    raise TypeError(f"a cell of {type(v).__name__} has no form a host hands its rows back in")
