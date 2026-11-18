# Copyright 2026 Legend Contributors
# SPDX-License-Identifier: Apache-2.0
"""PLANTED (census B2): a Python product module reading legend-engine's tdsColumns shape (M1), with a function only the
edge uses (M6), a name naming upstream (M2), a reproduction declared (M5), and a static import of the edge (M7)."""

from .edge import _planted_routes


def sql_type(raw: dict) -> str:
    # ENGINE GAP: legend-engine's server carries the column's SQL type beside its Pure type
    return raw.get('relationalType')


def engine_columns(plan: dict) -> list:
    return plan['rootExecutionNode']['resultType']['tdsColumns']


LEGEND_ENGINE_ROUTES = _planted_routes
