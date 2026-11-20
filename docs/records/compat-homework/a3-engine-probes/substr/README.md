# substr, and substring under CORRECT_SQL_SUBSTRING_INDEXING, asked of legend-engine (2026-11-20)

The compat owner's question of 2026-11-20: does the pure-meaning detector, and the `engine` rewrite, cover `substr` and
the corrected `substring` once A3 piece 2's review fix (S383's out-of-range guard on both) is in? Read, not assumed:
legend-engine's runner (4.145.0; H2 2.1.214, DuckDB v1.3.0) and lite were asked the same 17 requests
(`requests.tsv`), each over the trades seed's four desks (Credit 6, Equity 6, FX 2, Rates 5) or a literal.

## What legend-engine sends and answers

`processSubstr` (`pureToSQLQuery.pure:4654-4690`) sends the database's SUBSTRING over `start + 1` and `end - start`,
folding literal indexes (`correctSubstrIndices`), with no range check; `substring` under the flag takes the same path
(`processSubstring`, `:4660`). So out of range it answers the database's own value, cut short and per database
(`engine-h2.tsv`, `engine-duckdb.tsv`; the SQL in the `.jsonl` files):

| request | H2 | DuckDB |
|---|---|---|
| `substr(1, 3)`, FX past the end | 'X' | 'X' |
| `substr(5)`, FX past the end | '' | '' |
| `substr(2, 1)`, start after end | '' | 'r', 'q', 'X', 'a' (a negative length reads leftward) |
| `substr(-1, 2)`, negative start | 'Cre', 'Equ', 'FX', 'Rat' | 'Cr', 'Eq', 'FX', 'Ra' |
| in range (`substr(0, 2)`, `'Credit'->substr(1, 3)`) | Pure's | Pure's |
| `substring` under the flag | the same as `substr`, row for row | the same |

legend-pure raises in each out-of-range case (Java's substring).

## What lite answered

- `lite-before-fix-*.tsv`: on claude/compat-a3-engine 6b0d25d8f, before the fix. substr agreed with legend-engine on
  every row but DuckDB's negative start ('Cre': lite's SubstringClamp); the corrected substring already raised out of
  range at the door.
- `lite-*.tsv`: with claude/compat-a3-switch 97d4c78b2 merged (local only). Every out-of-range request raises
  ("Range [1, 3) out of bounds for length 2", ...), legend-pure's behaviour; in range lite equals legend-engine. warn and
  engine answered the same, since neither form was flagged or rewritten.

The lite runs used LiteBatteries with its error text appended to each refused row (a local debug change, never
pushed). The fix that follows (the detector flags the four forms; `engine` rewrites them to legend-engine's SQL; under
`engine` a literal-0 `substring` is rewritten too, the reviewer's M-a1) is guarded by `PureMeaningEngineTest` and by the
batteries' rows on a value shorter than its end (`col.s19`..`lit.c02`).
