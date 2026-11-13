"""Pricing and risk execution engines: local Python ``expr.Expr`` evaluation vs. Pure / SQL / DuckDB basis execution."""

from __future__ import annotations

from .base import ExecutionEngine, MarketSession, TickResult
from .pure_basis import DuckDBBasisEngine, PureBasisEngine, PureBasisMarketSession
from .python_expr import PythonEngineExpr, PythonMarketSession


def resolve_engine(engine: str | ExecutionEngine | None = None) -> ExecutionEngine:
    """Resolves an engine instance or name (``'python'``, ``'pure'``, ``'sql'``, ``'duckdb'``, ``'pure_duckdb'``)."""
    if isinstance(engine, ExecutionEngine):
        return engine
    name = (engine or 'pure_duckdb').lower().strip()
    if name in ('python', 'py', 'python_expr'):
        return PythonEngineExpr()
    if name in ('pure', 'sql', 'duckdb', 'pure_duckdb', 'pure_basis', 'duckdb_basis'):
        return PureBasisEngine()
    raise ValueError(f"Unknown engine {engine!r}; expected 'python' or 'pure_duckdb'")


__all__ = [
    'ExecutionEngine',
    'MarketSession',
    'TickResult',
    'PythonEngineExpr',
    'PythonMarketSession',
    'PureBasisEngine',
    'PureBasisMarketSession',
    'DuckDBBasisEngine',
    'resolve_engine',
]

