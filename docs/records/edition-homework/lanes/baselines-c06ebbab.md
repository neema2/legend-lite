# Lane baselines at `c06ebbab` (2026-11-25)

*Each lane's first round on the merged tree (step 1, B2's branch merged, the audit's fixes), kept as its history: later
rounds compare with these logs test by test. Each lane keeps its logs in its own scratchpad under `r1/c06ebbab/`.*

## SQLite and MongoDB (`session_0195rQEn8iDNDtGvFr9Db1jW`)

| Run | Essential | Grammar | Relation | Standard | Unclassified |
|---|---|---|---|---|---|
| SQLite channel A | 345/345 | 137/137 | 469/469 | 205/205 | 93/93 |
| SQLite channel B | PASS=336 ERROR=9 | PASS=134 ERROR=3 | PASS=458 ERROR=11 | PASS=205 | PASS=92 ERROR=1 |
| MongoDB channel A | 345/345 | 137/137 | 469/469 | 205/205 | 93/93 |

Every suite passed (channel B's errors are the ones its ledger expects). No "does not match its ledger" block.
Timings: setup 7m24s, build 1m11s, `pct_sqlite` 2m03s, `pct_channel_b_sqlite` 28s, `pct_mongodb` 2m17s. The docs'
commands worked as written.
