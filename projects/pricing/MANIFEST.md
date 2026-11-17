# pricing

Layer 0, no dependencies. Rates curve knot data, normalized swap/FRA cashflow rows, and
interpolation-independent Pure pricing functions (`evalSegmentDf`, `curveDiscountFactors`, `cashflowPvs`).

## Exports

| element | kind | note |
| --- | --- | --- |
| `pricing::PrInterpMethod` | enum | `LOG_LINEAR_DF`, `LINEAR_ZERO`, `LINEAR_DF` |
| `pricing::PrCurveKnot` | class | one row per curve knot `(scenarioId, curveId, knotIdx)` |
| `pricing::PrCashflow` | class | one row per schedule-adjusted cashflow `(tradeId, legId, cfIdx)` |
| `pricing::evalSegmentDf` | function | `(t, t0, r0, t1, r1, method): Float[1]` — interpolates `LOG_LINEAR_DF`, `LINEAR_ZERO`, or `LINEAR_DF` |
| `pricing::curveDiscountFactors` | function | `(targets: Relation<(scenarioId, curveId, t)>[1]): Relation<(scenarioId, curveId, t, df)>[1]` — single-pass `LAG`/`LEAD` segment interpolation |
| `pricing::cashflowPvs` | function | `(): Relation<(scenarioId, tradeId, legId, cfIdx, instrumentType, currency, csa, pv)>[1]` — prices future (`payT > 0`) `PrCashflow` rows across scenarios |
| `pricing::Store` | store | tables `PR_CURVE_KNOT`, `PR_CASHFLOW` |
| `pricing::Mapping` | mapping | enum `PrInterpMethodCode`, sets `prCurveKnot`, `prCashflow` |
