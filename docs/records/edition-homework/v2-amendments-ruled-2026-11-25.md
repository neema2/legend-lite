# Amendments to design v2, to fold in after the second review lands (2026-11-25, late)

Ruled by the user, in order:
1. "one place for compiler rules and one place for presentation/format java rules would be an okay place to land":
   - meaning: only the native-call site calls the rule set;
   - format: only the door's presenter, chosen by the door, never by the edition;
   - folds call nothing (they stop folding cross-kind comparisons and value texts);
   - forArgument deleted.
2. Placement as a part the typer passes to the one call site. The user's insight: "engine is only for the database part and pure is for the upstream non-database part".
   - Parts: database part (row lambdas, mapping code, graph-fetch-pushed properties); legend-engine's own part (statements reading no store, in-memory M2M); legend-pure's part (the corpus harness).
   - The memo is keyed by (function, rule set, part).
   - A3 is deleted after a shadow comparison on every test (pure/v1 goldens, A3's tests, the corpus).
   - The corpus switch is deleted.
3. The model, confirmed: "yes so pure edition is really just everything pure, engine edition is really engine for database, pure for outside?"
   - The refinement the user accepted: "outside" is compiled legend-pure for the corpus harness (exactly Pure), and legend-engine's own Java for pure/v1's host-side code, which is Pure's meaning except a measured exception list (so far: `==` across number kinds is true, B2's m2m measurement). Each entry is asked of the engine runner and recorded.
   - "great nuance about what outside means".
4. "pure should be compiled legend-pure agreed": `pure` = compiled legend-pure (the PCT reference runtime, core-compiled manifest: 3 known failures); interpreted legend-pure's differences (e.g. 1D == 1) are not followed.

Earlier the same evening (already in v2 §1): engine mode on DuckDB only; lite may answer where engine refuses; the go for R0/R1; the referee kept; PMCD/plans after.

Measurement added: M7, a typed census of governed constructs in the corpus harness (now informational, since the part model handles the harness).
