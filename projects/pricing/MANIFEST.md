# pricing

Layer 0, no dependencies. Rates curve knot data, normalized swap/FRA cashflow rows, and
interpolation-independent Pure pricing functions (`isNan`, `evalSegmentDf`, `curveDiscountFactors`, `cashflowPvs`).
Times in this initial schema are pre-expanded `ACT/365F` year fractions (`knotT`, `payT`); real `StrictDate`/`DateTime`
columns, holiday calendars, and seasoned OIS fixings arrive in a follow-up PR.

## Exports

| element | kind | note |
| --- | --- | --- |
| `pricing::PrInterpMethod` | enum | `LOG_LINEAR_DF`, `LINEAR_ZERO`, `LINEAR_DF` |
| `pricing::PrCurveKnot` | class | one row per curve knot `(scenarioId, curveId, knotIdx)` |
| `pricing::PrCashflow` | class | one row per schedule-adjusted cashflow `(tradeId, legId, cfIdx)` |
| `pricing::isNan` | function | `(x: Float[1]): Boolean[1]` — portable SQL `NaN` predicate across IEEE and total-order dialects |
| `pricing::evalSegmentDf` | function | `(t, t0, r0, t1, r1, method): Float[1]` — interpolates `LOG_LINEAR_DF`, `LINEAR_ZERO`, or `LINEAR_DF`, clamping flat zero rate outside `[t0, t1]` (`P(0, t <= 0) = 1.0`) |
| `pricing::curveDiscountFactors` | function | `(targets: Relation<(scenarioId, curveId, t)>[1]): Relation<(scenarioId, curveId, t, df)>[1]` — single-pass `LAG`/`LEAD` segment interpolation with whole-curve validation |
| `pricing::cashflowPvs` | function | `(): Relation<(scenarioId, tradeId, legId, cfIdx, instrumentType, currency, csa, pv)>[1]` — prices future (`payT > 0`) `PrCashflow` rows across scenarios, refusing mid-period float coupons (`accrualStartT < 0`) |
| `pricing::Store` | store | tables `PR_CURVE_KNOT`, `PR_CASHFLOW` |
| `pricing::Mapping` | mapping | enum `PrInterpMethodCode`, sets `prCurveKnot`, `prCashflow` |
