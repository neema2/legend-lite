# The timeseries time model: dates, moments, pairing and duplicates (2026-11-02)

**Ratified 2026-11-03** (the user: "Okay let's go", to every recommendation of the v1 roadmap). The working note, with its
worked examples and its comparison with kdb+, pandas, QuestDB and Polars, is the shared doc "Timeseries time model:
dates, moments, pairing and duplicates" (claude.ai/artifact/Sikk7P9eWmqroegXWftyLT). This file is its record in the
repository: the rules, the rulings, and what is built. The language spec's R2 and R6 carry the rules (D7).

## The model in five rules

1. **A point has a time.** A timestamp is a moment (2 January, 10:00); a date is a whole day (2 January).
2. **A line is one source's rows for one choice of instrument.** Every row is a point, even when several share a date
   (the 2026-10-30 ruling). `close` and `volume` read from the same rows are two columns of one line.
3. **A daily value sits somewhere in its day.** A close at its end, an open at its start, a fixing at its own time; the
   default is the end of the day, which never makes a value look known early. It matters only where a daily value
   meets moments.
4. **Changing grain is always explicit**: `SAMPLE BY` and how to take a period's rows (`last(close)`), rule R5.
5. **Two lines pair in one of two ways**: by the same period, or by the value prevailing at each point's time. On a
   calendar of days both exist today (R6, and `FILL prev`); for moments meeting days they answer different questions
   (end-of-day marking, or what the market knew), so neither may be a silent default.

## The rulings

| # | Decision | Ruled | Built |
|---|---|---|---|
| D1 | Duplicates allowed inside a line; one side with several rows pairs as a lookup; both sides with several refused | agreed, 2026-11-03 | 692016cb: `lines::meet`, `lines::onto`, the database refusing while the query runs |
| D2 | The check skipped where uniqueness is known (a declared key, `SAMPLE BY`); else the database checks | agreed | the check is built; **the skip is not** (measured cost: only queries whose lines meet, `beta_63` 25 -> 53 ms, `ratio_step` 36 -> 72 ms) |
| D3 | A daily value sits at the end of its day unless its source says otherwise | agreed | nothing reads it in v1 (prevailing pairing for moments is v1.1) |
| D4 | In v1 a line of moments meeting a line of days is refused, naming the fix | agreed | 50ebe0bd: `lines::meet` and `lines::union` forms by the periods' types |
| D5 | R7: words that keep their rows add columns to one table; each word's kind declared in the library | agreed | a00fc91b (one table per base), 5eb1ba92 (points carried), 485ba593 (the simple statistics); the others keep line forms (the program doc, A1) |
| D6 | Prevailing pairing for moments in v1.1 | agreed | -- |
| D7 | These rules amend R2 and R6 in the language spec | agreed | the spec's R6 |

## What the proofs found

- **Duplicates.** Before, two lines with several rows on a date were joined and multiplied silently (3 against 2 gave
  6); every read of a column built its own table, so even two items over one column multiplied (48 rows for 24
  points). Both fixed.
- **Moments meeting days.** Before, an expression or a word of both found no row, silently; two items were a type
  error with no fix named. Now refused by name, the fix (`SAMPLE BY 1d`, `last(...)`) answering right on DuckDB and
  SQLite (H2 has no `ARG_MAX`).
- **Points.** A word's answer is no point of its ramp's rows, and a point may have no value (a return's first): one
  column cannot say both, so a value that is not a point of every row carries its points as another column, the
  library's rules (`timeseries::rows::`) deciding what they are.
</content>
</invoke>
<invoke name="Bash">
<parameter name="command">cd /home/user/legend-lite && git add docs/TS_TIME_MODEL_2026_11_02.md && git commit -q -F - <<'EOF'
Docs: the timeseries time model, ratified 2026-11-03, its record in the repository

The five rules, the rulings D1 to D7 (all agreed, "Okay let's go"), what each built and where, and what the proofs
found. The working note stays the shared doc; the spec's R2 and R6 carry the rules.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01TvL7jAWxW8Ux7ygvg3UaE5
EOF
git push -u origin claude/timeseries-language 2>&1 | tail -1; git log --oneline -1