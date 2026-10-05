# `dateDiff(..., HOURS)` from a start off the hour: 163 or 164

    sentAt    = %2024-01-15T14:30:00
    matchedAt = %2024-01-22T10:02:00
    $sentAt->dateDiff($matchedAt, DurationUnit.HOURS)

The elapsed time is 163 h 32 min. Two rules give two answers:

| rule | answer | who |
| --- | --- | --- |
| hour boundaries crossed (`date_diff('hour', a, b)`) | **164** | legend-engine's relational SQL; the corpus oracle (`scripts/corpus/oracle.py`), whose expectation the engine passes (`stress::MO2_Confirmations` is in no `ENGINE_QUARANTINE`) |
| truncated elapsed time | **163** | legend-lite (`Scalars.dateDiffExpr`: `(epoch_ms(b) - epoch_ms(a)) // 3600000`) |

legend-pure's own tests do not decide it: every HOURS, MINUTES and SECONDS case in
`platform/pure/essential/date/operation/dateDiff.pure` (4.145.0 / 5.99.0) starts ON a unit
boundary (`13:00:00`, `23:00:00`, `00:00:00`), where the two rules agree. legend-lite's comment
calls its rule "PCT-pinned"; the PCT cases pin neither.

Where it shows: `stress::MO2_Confirmations` (`hoursToMatch`, CNF-00005) fails in
`//core:stress_suites` (one of its counted failures) and disagrees in
`//core:corpus_differential_test` (LITE_QUARANTINE, F58).

Open: which rule is Legend's. The interpreted reference (legend-pure's Java native for
`dateDiff`) decides; a PCT case starting off the boundary would pin it for every engine.
