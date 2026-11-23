"""A PLAN'S PARAMETERS, BOUND AT RUN (the lite API spec's §4, 2026-11-17; the orchestrator's Q2): a query's parameters are
declared in its plan and bound when it runs, never pasted into its text. Java's ``exec.PlanParameters.check`` and the
TypeScript host's ``checkParameters`` (engine-client/src/plan-runner.ts) are the same rule; this is Python's: every value
checked against its declaration before any statement is sent, every problem named at once (the missing first), each
value converted for its slot by its declared Pure type -- asked of ``_types``, the one reader of a type. An Integer takes
the 64-bit range on every host (Java's Long; a Python ``bool`` is no Integer). The shared cases beside engine-client's
raise-envelopes.json hold the three to one outcome. The RUN side's (a run binds; the planner only declares): the plan's
declarations are read by the seam (``_plans.declared``)."""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from typing import Any

from ._plans import Declared, Plan, PlanParamsRefused, multiplicity
from ._types import (
    UnknownType,
    is_boolean,
    is_date,
    is_date_time,
    is_decimal,
    is_float,
    is_integer,
    is_number,
    is_strict_date,
    is_string,
)

# the 64-bit range an Integer takes on every host (the orchestrator's ruling (a), 2026-11-17)
_LONG_MIN, _LONG_MAX = -(2**63), 2**63 - 1

_STRICT_DATE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
_DATE_TIME = re.compile(r"(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?Z?")
_DECIMAL = re.compile(r"-?\d+(\.\d+)?")


def _whole(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


class ParametersRefused(ValueError):
    """A run's parameter values refused -- one missing, or one its declared type does not take -- every problem named
    at once, nothing run: the spec's ``parameters`` kind (§3, ruled 6)."""


@dataclass(frozen=True)
class One:
    """A to-one parameter's value, as its slot binds it."""

    value: Any


@dataclass(frozen=True)
class Many:
    """A to-many parameter's values, as a list binds them."""

    values: tuple[Any, ...]


@dataclass(frozen=True)
class NoValue:
    """An optional parameter given no value: bound as a typed null (a list as an empty one)."""


Checked = One | Many | NoValue


class _Refused:
    __slots__ = ("why",)

    def __init__(self, why: str) -> None:
        self.why = why


def check(declarations: list[Declared] | tuple[Declared, ...], values: dict[str, Any] | None) -> dict[str, Checked]:
    """``values`` checked against ``declarations`` and converted for binding, by name: every declared parameter, its
    checked value. A value for no declared parameter is ignored; absent is None or an empty list -- missing where the
    parameter needs one, else a typed null. Every problem named at once, the missing first (``ParametersRefused``)."""
    given = values or {}
    missing: list[str] = []
    invalid: list[str] = []
    out: dict[str, Checked] = {}
    for p in declarations:
        value = given.get(p.name)
        if value is None or (isinstance(value, list) and not value):
            if p.lower > 0:
                missing.append(f"{p.name}:{p.typed}")
            else:
                out[p.name] = NoValue()
            continue
        problem = _checked(p, value, out)
        if problem is not None:
            invalid.append(f"parameter '{p.name}' ({p.typed}): {problem}")
    wrong = None if not invalid else f"values their parameters do not take: [{'; '.join(invalid)}]"
    if missing:
        raise ParametersRefused(
            f"no value for the parameter(s) {', '.join(missing)}" + ("" if wrong is None else f"; {wrong}")
        )
    if wrong is not None:
        raise ParametersRefused(wrong)
    return out


def _checked(p: Declared, value: Any, out: dict[str, Checked]) -> str | None:
    if p.upper == 1:
        if isinstance(value, list):
            return f"takes one value, given a list of {len(value)}"
        c = value_of(p, value)
        if isinstance(c, _Refused):
            return c.why
        out[p.name] = One(c)
        return None
    if not isinstance(value, list):
        return "takes a list, given one value"
    if len(value) < p.lower or (p.upper is not None and len(value) > p.upper):
        return f"takes {multiplicity(p.lower, p.upper)} values, given {len(value)}"
    converted = []
    for v in value:
        if v is None:
            return "a list holds no null"
        c = value_of(p, v)
        if isinstance(c, _Refused):
            return c.why
        converted.append(c)
    out[p.name] = Many(tuple(converted))
    return None


def _shown(v: Any) -> str:
    if isinstance(v, str):
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def _kind(v: Any) -> str:
    """A value's kind as the words name it, one spelling on every host (JSON's: string, number, boolean)."""
    if isinstance(v, bool):
        return "boolean"
    if isinstance(v, (int, float)):
        return "number"
    if isinstance(v, str):
        return "string"
    return type(v).__name__


def value_of(p: Declared, v: Any) -> Any:
    """``v`` (one value, not None) of ``p`` as its slot binds it, or why it is not one ``p`` takes (``_Refused``)."""

    def not_a(expected: str) -> _Refused:
        return _Refused(f"given {_shown(v)} ({_kind(v)}): {expected}")

    if p.enum_values:
        return (
            v
            if isinstance(v, str) and v in p.enum_values
            else _Refused(f"{_shown(v)} is not a value of {p.type} [{', '.join(p.enum_values)}]")
        )

    def integer() -> Any:
        if _whole(v) and _LONG_MIN <= v <= _LONG_MAX:
            return v
        return not_a("an Integer is a whole number within the 64-bit range")

    def floating() -> Any:
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            return not_a("a Float is a number")
        f = float(v)
        return f if math.isfinite(f) else _Refused(f"{_shown(v)} is not finite: no SQL literal stands for it")

    def decimal() -> Any:
        return v if isinstance(v, str) and _DECIMAL.fullmatch(v) else not_a("a Decimal is its plain digits as text")

    def strict_date() -> Any:
        d = _STRICT_DATE.fullmatch(v) if isinstance(v, str) else None
        if d is None:
            return None
        year = int(d.group(1))
        return (
            v
            if 1 <= year <= 9999
            else _Refused(f"{v} is outside the years 1 to 9999: no date text a database reads stands for it")
        )

    def date_time() -> Any:
        t = _DATE_TIME.fullmatch(v) if isinstance(v, str) else None
        if t is None:
            return None
        year = int(t.group(1))
        if not 1 <= year <= 9999:
            return _Refused(f"{v} is outside the years 1 to 9999: no date-time text a database reads stands for it")
        # the text its cast reads, every digit to the nanosecond, as Java's runner writes it (PlanRunner.dateTimeText)
        return (
            f"{t.group(1)}-{t.group(2)}-{t.group(3)} {t.group(4)}:{t.group(5)}:{t.group(6) or '00'}."
            f"{(t.group(7) or '').ljust(9, '0')}"
        )

    # the declared type asked of _types, the one reader of a type (never spelled here: test_frames' TypeNamesAreReadHere)
    try:
        if is_integer(p.type):
            return integer()
        if is_float(p.type):
            return floating()
        if is_decimal(p.type):
            return decimal()
        if is_number(p.type):
            # a whole number an Integer (its range the Integer's: never a float that lost its digits), another number a
            # Float, a text a Decimal's
            if _whole(v):
                return integer()
            return floating() if isinstance(v, float) else decimal()
        if is_boolean(p.type):
            return v if isinstance(v, bool) else not_a("a Boolean is a boolean")
        if is_string(p.type):
            return v if isinstance(v, str) else not_a("a String is a string")
        if is_strict_date(p.type):
            found = strict_date()
            return found if found is not None else not_a("a StrictDate is its text YYYY-MM-DD")
        if is_date_time(p.type):
            found = date_time()
            return (
                found
                if found is not None
                else not_a("a DateTime is its ISO 8601 text in UTC, YYYY-MM-DDTHH:MM[:SS[.fffffffff]]")
            )
        if is_date(p.type):
            found = strict_date()
            if found is None:
                found = date_time()
            return found if found is not None else not_a("a Date is a StrictDate's or a DateTime's text")
    except UnknownType:
        pass
    return _Refused(f"a {p.type} value is not bound by a plan")


def bind(plan: Plan, values: dict[str, Any] | None = None) -> tuple[Any, ...]:
    """The values ``plan``'s statement binds at this run, in placeholder order: a plan that declares no parameter binds
    its own payloads (a value for no declared parameter is ignored, as Java's and TypeScript's runners ignore one); one
    that declares some checks ``values`` against them first (``check``: every problem named at once, nothing run) and
    binds each slot -- its payload, or its parameter's checked value (None, a typed null, for an optional given none).
    A list in a one-value slot is refused by name."""
    if not plan.parameters:
        return plan.params
    checked = check(plan.parameters, values)
    out: list[Any] = []
    for kind, what in plan.slots:
        if kind == "payload":
            out.append(what)
            continue
        value = checked[what]
        if isinstance(value, Many):
            raise PlanParamsRefused(
                f"'{what}' binds one value, and its parameter takes a list: a list is bound by an"
                " array slot, which a host running the plan's text does not bind"
            )
        out.append(value.value if isinstance(value, One) else None)
    return tuple(out)
