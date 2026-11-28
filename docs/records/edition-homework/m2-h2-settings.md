# M2: legend-engine's H2 settings at the edge, measured (B2, 2026-11-25)

R1's second measurement (the user's go for R0 and R1, 2026-11-25, "lets go"; briefed by B3). Documentation only: both
probes were temporary edits, reverted; no pin, ledger or roster moved. Read under tenet 1 as amended the same day (the
product's database computes every value; compatibility's presentation is the edge's wherever it can be) and rule zero of
design v3 (a mode changes the SQL, or here the session, only where that changes what the database returns).

**What the edge sets today.** `edge/EngineDatabaseSettings` is the `DatabaseSettingsForm` of `PureText.UPSTREAM` (and of
`PURE_V1`, which delegates to it). It requires `MODE=LEGACY` and `DEFAULT_NULL_ORDERING=HIGH` on every H2 database an
edge plan targets (legend-engine 4.145.0's `H2Manager` / `H2Defaults`). The product requires H2's own defaults,
`MODE=REGULAR` and `DEFAULT_NULL_ORDERING=LOW` (`sql/dialect/ProductDatabaseSettings`). The plan states the settings as
data and the session verifies them (`H2Connector`, `JdbcStatements.verifySettings`).

**Two probes**, to tell null order from everything else `MODE=LEGACY` changes, both at `4b6f632bf`:
- **M2-a, both dropped:** `UPSTREAM`'s settings form answered the product's (`MODE=REGULAR`, nulls `LOW`).
- **M2-b, `MODE=LEGACY` alone dropped:** `MODE=REGULAR`, nulls `HIGH` kept.

A move in both is `MODE=LEGACY`'s. A move in M2-a alone is null order's.

**The lanes:** `//gates:local`, the edge's tests and their H2 integration lane, PCT channel A on H2, channel B on H2
(the Relation suite, and the twice-run lane), the H2 engine lane, `//pct:edge_net` (the edge contract, the pure/v1
golden net on H2, the edge cases), and the corpus on H2. The Postgres, `is_numeric` and datacube targets failed only as
root, as on every round, and passed on the unprivileged rerun (14 of 14, both probes).

## 1. The moves, per door

| Door (H2) | M2-a (both dropped) | M2-b (`MODE` alone) | So |
|---|---|---|---|
| PCT channel A (the Relation suite) | 0 | 0 | none |
| PCT channel B (Relation) | 0 | 0 | none |
| the corpus | 1 newly FAILS, none newly pass | the same 1 | `MODE=LEGACY` |
| pure/v1's golden net | 0 | 0 | none |
| the H2 engine lane | 0 | 0 | none |
| the edge's H2 integration lane | 0 | 0 | none |
| the edge's own tests | 2 assertions about the settings themselves | 0 | the settings' identity |
| lean SQL | 0 | plan JSON +2 characters in one family | the settings' spelling |

**The corpus, H2: `meta::relational::graphFetch::tests::isolation::testIsolationOfPropertyTargetFilter` newly fails**
under both probes (run alone, `//spec:corpus_one -- h2 host <fqn>`). Its store's filter compares a BOOLEAN column with an
integer: `WHERE "t1".active = 1`. Under `MODE=REGULAR`, H2 refuses that comparison: `DataError: Values of types
"BOOLEAN" and "INTEGER" are not comparable`. `MODE=LEGACY` accepts it. legend-engine answers the test (it passes on the
committed roster), so this move is AWAY from legend-engine, and it changes whether an error happens. Under rule zero it
is a meaning, which Java at the edge cannot fix afterwards.

**The edge's own tests (M2-a only).** Both assertions are about the settings, not about an answer:
- `EdgeH2SessionTest.anEdgePlanOnAProductSessionIsRefusedByName_onItsOwnSessionItRuns`: an edge plan sent to a product
  session is refused because the two sessions' settings differ. With equal settings it runs.
- `HookRulingTest.theTwoFormKindsAreAClosedListTheProductAnswersItsOwnForm`: "the edge's database has legend-engine's"
  settings.

**Lean SQL (M2-b only):** `query::filter::or` on H2, plan JSON 11,116 -> 11,118 characters. That is the settings'
spelled value in the plan's data (`REGULAR` in place of `LEGACY`), not SQL. Under M2-a, `LOW` in place of `HIGH` evens
it out.

## 2. Null order, separately

**`DEFAULT_NULL_ORDERING`: no move in any lane** (M2-a's moves minus M2-b's: only the two settings-identity assertions).
The H2 dialect writes a sort key's null placement explicitly wherever the plan does not state one (`NULLS LAST`
ascending, `NULLS FIRST` descending, `sql/dialect/H2.java:1181-1184`). So the plans measured here never rely on the
database's default null order. One spelling leaves it unstated (`H2.java:1234-1236`, a key whose null order is unset);
no lane reaches a case where that changes the rows.

## 3. Pre-existing, not M2's

`//pct:pct_channel_b_twice_h2_relation` (a `manual` lane, G10's twice-run guard) fails the same 4 cases on a clean tree
at `4b6f632bf`, with no probe:
- `project::testSimpleProject`, `testSimpleProjectList` and `testSimpleProjectWithEmpty`, and
  `composition::testFilterPostProject`;
- each with `DialectCapability: LIST_GET reached a dialect without a list encoding`.

The single-run lane (`//pct:pct_channel_b_h2_relation`) passes. Recorded for whoever owns that lane; nothing changed.

## 4. Summary

- **`MODE=LEGACY`:** one corpus test on H2 depends on it, a BOOLEAN compared with an integer in a store's filter. Dropping
  it moves that test away from legend-engine (a new error). It is a meaning that lives in the session, not a format.
- **`DEFAULT_NULL_ORDERING=HIGH`:** no measured effect, because the dialect writes null placement explicitly.
- Nothing newly errors besides that test. No PCT, pure/v1 or engine-lane move.
