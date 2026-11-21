# Option E probed: one fused statement, each raise guarded by "the asserts before it passed"

Probed 2026-11-21 by the compatibility program's owner session, for the user's question on single-shot verdicts.

**The case.** Assert 0 fails. Assert 1 would raise (`toOne` over an empty relation). Upstream reports assert 0's failure.

**Two forms of the guard:**
- **naive:** the whole of assert 1 is wrapped in `CASE WHEN <assert 0 passed> THEN ... END`;
- **pushed:** the condition is added to the raise site's own guard:
  `CASE WHEN <assert 0 passed> AND COUNT(*) <> 1 THEN raise ELSE ... END`.

| Case | DuckDB 2.0.1 (`error()`) | H2 2.5.252 (`SIGNAL`) |
|---|---|---|
| naive | **raises** (the relation is computed before the `CASE` chooses) | right: `[0 false, 1 null]` |
| pushed | right: `[0 false, 1 null]` | right |
| pushed, `MATERIALIZED` frame | right | not run |
| pushed, a raise per row (`at(0)`) | right | not run |
| pushed, a constant raise | right | right |
| control: assert 0 passes, assert 1 raises | raises (right) | raises (right) |
| a database error lite did not write (a cast of `'abc'` to INTEGER) | **raises** whatever the guard | not run |

**Findings:**
- Only the pushed form works on both databases.
- The guard must reach every raise site under assert k, including any let frame defined after assert k.
- An error the database raises on its own, with no raise site of lite's, is not guarded. That case keeps the
  verdict right but can name the wrong failure, the same residual as option C.

`e_duckdb.py` holds the DuckDB statements; the H2 statements are the same, with `SIGNAL('45000', ...)` for `error()`.
