"""Generic computation in Python, run as Pure on any database legend-lite renders. Three levels, each on the one
before:

* ``expr``  -- THE COMPUTE TREE: Python arithmetic traced into a hash-consed DAG; evaluate, walk, differentiate exactly.
* ``basis`` -- THE INTERPOLATION BASIS: knots of a series, values interpolated piecewise-linearly between them, basis
  functions of those values (exp(-x t), x, 1 / (1 + x t)), and an expression's expansion over them and dynamic formula
  factors.
* ``basis_pure`` -- THE BASIS IN PURE: expanded measures compiled into a dynamically synthesized Pure model and
  relational schema, and the Pure queries that value them, planned for any database and run on DuckDB.

Nothing here knows of any domain: ``python/pricing`` builds yield curves, swaps, options and their risk on it.
"""

from . import basis, basis_pure, expr

__all__ = ['basis', 'basis_pure', 'expr']


