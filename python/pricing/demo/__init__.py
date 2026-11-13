"""Demos, interactive workbench, scaling benchmark, and DataCube dashboard for ``pricing`` on ``legend_lite.scalar_parallel``."""

from __future__ import annotations

import importlib

__all__ = [
    'dashboard',
    'irs_frames',
    'irs_page',
]


def __getattr__(name: str):
    if name == 'dashboard':
        return importlib.import_module('.dashboard', __name__)
    if name in ('irs_frames', 'irs_page'):
        mod = importlib.import_module('.dashboard', __name__)
        return getattr(mod, name)
    raise AttributeError(f"module 'pricing.demo' has no attribute {name!r}")

