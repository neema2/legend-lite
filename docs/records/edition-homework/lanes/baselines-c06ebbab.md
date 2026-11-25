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

## Snowflake (`session_0195rQEixiryogCvRMhCTxvs`)

| Run | Essential | Grammar | Relation | Standard | Unclassified |
|---|---|---|---|---|---|
| Channel A | 345/345 | 137/137 | 469/469 | 205/205 | 93/93 |
| Channel B | PASS=338 ERROR=7 | PASS=133 ERROR=4 | PASS=469 | PASS=205 | PASS=93 |

All 10 suites pass; every channel B error is inside its ledger. Timings: setup 432 s, credentials under a second,
build 61 s, channel A 264 s, channel B 128 s: about 6.5 minutes of warehouse time for the round, not the 75 the older
records estimate.

### A finding for the user: the exact Float text on Snowflake is not what its javadoc says

B2's `SqlFn.FLOAT_FULL_TEXT` on Snowflake is `CAST(CAST(x AS DECFLOAT) AS VARCHAR)` (amendment 21; "unmeasured as this
node until Snowflake's lane runs"). The lane sent nine doubles through that exact expression (no test of the embedded
lanes runs on a cloud lane: the pin is `ProductRelationTextTest.aCsvsFloatCellIsTheDatabasesExactTextOfTheDouble`):

| Double | Snowflake's text | Reads back as the same double |
|---|---|---|
| 0.1 | `1.0000000000000000555111512312578270212e-1` | yes |
| 1.0E-7 | `9.9999999999999995474811182588625868561e-8` | yes |
| 123456789.12345679 | `123456789.12345679104328155517578125` | yes |
| 1.7976931348623157E308 | `1.797693134862315708145274237317043568e308` | yes |
| 4.9E-324 | `4.9406564584124654417656879286822137237e-324` | yes |
| 0.30000000000000004 | `3.0000000000000004440892098500626161695e-1` | yes |
| 100.0 | `100` | yes, with no `.0` |
| NaN | `NaN` | yes |
| Infinity | `inf` | only if `inf` is read as infinity |

Every value keeps its exact double, so nothing is lost. But the text is up to 38 digits of the double's exact binary
expansion, not 17 significant digits in exponent form as `Snowflake.floatFullText`'s javadoc says, and a `toCSV` cell
on Snowflake prints `0.1` as `1.0000000000000000555111512312578270212e-1`. Not ruled: whether that is acceptable as
"the database's own full-precision text", or the Snowflake arm should be another of its own expressions. It belongs
to the presentation work (the plan's step 8), with register S311's Snowflake note.

**What the lane found wrong or missing in the docs:** `docs/CLOUD_BACKENDS_PCT_2026_10_11.md` section 4 names the
key-password property two ways (`private_key_file_pwd` is the one that works) and does not list the secret's
`public_key` and `username`; the round's cost estimates are far above what was measured; on login the Snowflake
driver logs SEVERE 403s probing cloud metadata endpoints, which is noise.
