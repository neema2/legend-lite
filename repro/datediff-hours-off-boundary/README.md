# `dateDiff(..., HOURS)` from a start off the hour: 163, not 164

    sentAt    = %2024-01-15T14:30:00
    matchedAt = %2024-01-22T10:02:00
    $sentAt->dateDiff($matchedAt, DurationUnit.HOURS)

The elapsed time is 163 h 32 min. **Pure's answer is 163.** legend-pure's m4 `DateFunctions` documents it: the time
units (HOURS, MINUTES, SECONDS, ...) "measure elapsed time, dropping any remainder. This is not the same as counting
boundaries" (12:59:00 to 13:01:00 is zero HOURS), and `DateDiff` computes `ChronoUnit.HOURS.between` (legend-pure
5.99.0, the pinned release). Its PCT cases (`platform/pure/essential/date/operation/dateDiff.pure`) all start on a unit boundary, where the
two rules agree, so they do not show it.

| rule | answer | who |
| --- | --- | --- |
| truncated elapsed time | **163** | legend-pure (the reference); legend-lite (`Scalars.dateDiffExpr`); the corpus oracle since 2026-10-05 |
| hour boundaries crossed (SQL `DATEDIFF`) | 164 | legend-engine's relational SQL; the corpus oracle until 2026-10-05 |

Where it shows: `stress::MO2_Confirmations` and `stress::DSLocal_MiddleofficeConfirmation` (`hoursToMatch`),
`stress::REGX_All`, `stress::REGX_LiquidityCoverageReport` and `stress::DSLocal_RegulatorySubmission` (`ackHours`,
negative intervals included: elapsed time truncates toward zero). Their expectations moved when the oracle was fixed;
legend-engine's are in `ENGINE_QUARANTINE` (F58), to be confirmed by `run.py` when it runs again. Also recorded
earlier as a seed disagreement: `docs/plan-audit-2026-09-26/wrongrows/seed-disagreements-2026-09-30.md`.
