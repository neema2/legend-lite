# Audit PCT2-B — WRONG-ROWS / VACUOUS PASSES (range 64311065..HEAD, pct-native)

Scope: the seven commits fa9da3b2..8bd4aa98 (PURE_ORDERED order contract,
fromJson canonicalization, pct.only instrument, temporal TDS-cell
spelling, the DATE lattice parse-back, the red-gate heal, average-is-
always-Float). Method: hunk-by-hunk diff read + live probes (a disposable
JUnit probe class under `core/src/test/.../harness/`, real model +
`TestBody.run(..., PURE_ORDERED)` + DuckDB in-memory with
`SET TimeZone='UTC'`, deleted after) + `pct.only` runs of the affected
corpus tests + two full sweeps. Every probe verdict below is the printed
`TestBody.Outcome` of an actual run.

## Verdict

**No HIGH findings. The prior audit's two HIGH channels are closed and
pinned, and PURE_ORDERED verifiably reaches every compare shape probed.**
Two MEDIUM silent-value channels exist in the new code: (1) the TDS-cell
DateTime spelling truncates sub-millisecond precision — two DISTINCT
microsecond timestamps print identically in a `#TDS` grid, a wrong-PASS
channel with zero corpus witnesses today; (2) `fromJson` duplicate keys
diverge from real pure (DuckDB keeps both, Jackson keeps last). One LOW:
the average to-one guard is loud-but-wrong-bucket for non-literal to-one
collections. The sweep's 1031/75/78/15 and all per-suite numbers
reproduce byte-identically across two consecutive runs — no flappers —
and the spot-checked new PASSes (average family, adjust-by-micros,
least/max Date) are honest value-equalities, not spelling coincidences.

---

## Re-verification of the prior audit's HIGH fixes

- **pct-b H1 (order through the eval wrapper)** — pin exists:
  `core/src/test/java/com/legend/harness/TestBodyTest.java:88`
  (`evalPayloadSortIsAnOrderContract`). Independent re-probe:
  `assertEquals([2,1], {|[1,2]->sort()}->eval())` under BOTH policies →
  FAIL (`expected [2, 1], got [1, 2]`). Holds.
- **pct-b H2 (relation toString scalarization)** — pin exists:
  `TestBodyTest.java:105`
  (`relationToStringOutsideResultPositionIsLoudNotScalarized`).
  Independent re-probes: `'a' + $f->eval(|#TDS…#->toString())` and the
  2-arg `->toString(true)` both throw
  `IllegalStateException: no scalar lowering registered for resolved
  overload 'meta::pure::functions::relation::toString'` — loud, named,
  never a value. Holds.

## Findings

### F1 — TDS-cell DateTime spelling SILENTLY TRUNCATES sub-millisecond precision (MEDIUM, verified by probe)

`core/src/main/java/com/legend/StatementExecutor.java:291-305`:
`pureDateTime` formats every Timestamp/LocalDateTime cell with
`yyyy-MM-dd'T'HH:mm:ss.SSS` — a 3-digit fraction. DuckDB TIMESTAMP is
microsecond-precision and the wire keeps it (below), but the `#TDS`
toString grid drops digits 4-6 silently.

Probes:

- grid cell written `%2024-01-29T00:32:34.123456+0000` prints
  **`2024-01-29T00:32:34.123+0000`** — the written value is not the
  printed value.
- **collision**: two grids differing ONLY at micros (`.123456` vs
  `.123999`), both `->toString()`, compared equal →
  `Ran[verified=1, failures=[]]` — **PASS on unequal values**. A
  toString-pinned PCT assert cannot distinguish sub-ms-wrong results.
- control — the WIRE is faithful: the same two values compared as DATE
  lattice values (not strings) FAIL loud
  (`…10:00:00.123456 vs …10:00:00.123457`), scalar and mixed-collection
  both.

Witnesses today: **zero** — every sub-ms fraction in the corpus's grid
fixtures is zeros (`.000000000+0000`, over.pure/composition.pure), and
real pure's own grid print is 3-digit (`.000+0000` in the pinned
expectations), so `.SSS` matches pure whenever the value is ms-exact.
The channel is latent: any future grid test whose values differ at
micros wrong-passes its toString assert. Fix shape: format with
nano-aware precision (pad to the value's own precision like the wire
does) or at minimum fail loud when `ldt.getNano() % 1_000_000 != 0`.

### F2 — fromJson duplicate keys: keep-both vs real pure's last-wins (MEDIUM, legend-lite side verified by probe, pure side reasoned from source)

`JSON_PARSE` renders as DuckDB `json(...)`
(`core/src/main/java/com/legend/sql/dialect/AnsiSqlRenderer.java:443`,
`core/src/main/java/com/legend/lowering/Scalars.java:1494-1497`).

- Probe: `fromJson('{"a":1,"a":2}')->toString()` → **`{"a":1,"a":2}`**
  (DuckDB/yyjson preserves both members).
- Real pure parses with a DEFAULT-config Jackson
  `ObjectMapper().readTree(json)`
  (`legend-engine …/variant/VariantInstanceImpl.java:31,92`) — no
  `STRICT_DUPLICATE_DETECTION`, so the last duplicate WINS and
  `toString` prints `{"a":2}`.

A PCT test pinning pure's behavior would FAIL honestly (visible), but
any legend-lite consumer of `fromJson` gets a structurally different
variant than real pure — wrong value, no witness in the corpus
(fromJson.pure has no duplicate-key case). The rest of the JSON_PARSE
surface probed CLEAN: huge integers exact
(`9223372036854775808` and a 30-digit integer round-trip verbatim —
matches Jackson's BigInteger nodes), `0.1000` → `0.1` (matches Jackson
double parse), and the canonicalization the commit exists for is real
(the six former decorative-whitespace FAILs now PASS).

### F3 — average to-one guard: non-literal to-one collections die loud in SQL (LOW, verified by probe)

`core/src/main/java/com/legend/lowering/Scalars.java:1069-1083`: the
cast identity applies when `isToOne(arg) && !(arg instanceof
TypedCollection)`. The exclusion is LITERAL-shaped only.

- `let x = [7]; … $x->average()` → **7.0 PASS** (TestBody splices the
  let, the arg IS a TypedCollection → LIST_AVG; and `assertEquals(7, …)`
  FAILS — the result is genuinely Float, the commit's claim holds).
- `average(test::seven())` (to-one call) → CAST(scalar AS DOUBLE) →
  **7.0 PASS**. `test::sevens()->average()` ([3] call) → LIST_AVG →
  **8.0 PASS**.
- `[7]->map(x|$x*1)->average()` — to-one typed, NOT a TypedCollection,
  but lowers to an ARRAY → `CAST(INTEGER[] AS DOUBLE)` → **loud
  `SQLException: Conversion Error: Unimplemented type for cast
  (INTEGER[] -> DOUBLE)`** → ERROR bucket. Never a silent wrong value,
  but the guard books a platform gap for any to-one chain that yields a
  list at SQL level. The corpus's own bodies (`average([1])`,
  `average([5,1,2,8,3])`) are literal collections — all 10
  `testAverage*` PASS honest (pct.only verified, values 1.0/3.8-shaped).

### F4 — PURE_ORDERED threading: COMPLETE for every reachable compare (verified good)

The policy enters at the runner
(`pct-corpus/src/test/java/com/legend/pctcorpus/PctCorpusRunner.java:181-187`),
threads through `checkAssert` into every `eval`/`evalScalar` call, into
the golden loop (`TestBody.java:768`, `checkAssert(af2, order, …)` at
:804), and gates all three `new Eval(...)` sites via `ordered(order,…)`
(`TestBody.java:1094, 1113, 1135`; `ordered` at :1881-1883). `compare`'s
every multiset fallback is conditioned on `sortedChain`, which
PURE_ORDERED forces true.

Probes (adapter shape `let f = {g|$g->eval()};` exactly as the runner
builds it), reversed expectations under PURE_ORDERED:

- list, sorted chain → **FAIL**; list, sort-free chain (`concatenate`)
  → **FAIL** while the SQL_ORDER_POLICY control **PASSES** the same body
  (the leniency is confined to the corpus policy, by design).
- `#TDS` grids both sides → **FAIL** (control: SQL policy passes).
- `makeString(',')` reversed → **FAIL** (the split-multiset fallback is
  dead: `joinSep` is only recorded when `!ordered(…)`, never under
  PURE_ORDERED).
- correct-order sanity and `assertSameElements` reversed both **PASS**
  (sameElements stays a multiset — its own semantics, correct).

CSV variants could not be probed end-to-end and are REASONED: the PCT
corpus contains **no `toCSV()` call at all** (only the scalar
`toCSVString`), and the CSVJOIN/csvTail strip arms cannot fire on the
PCT path anyway (the assert's actual root is the `eval` wrapper, not
`replace`/`toCSV`, so the whole expression forwards and compares
string-exact — stricter than the arm). `assertTdsEquivalent` ignores the
policy but its `tdsEquivalent` is an ordered zip — already the
PURE_ORDERED semantics; the prior audit's F4 "too strict" note is now
the correct strictness. Residual (INFO): SQL_ORDER_POLICY callers
(engine corpus harness) keep the old leniency — unchanged, out of scope.

### F5 — DATE-lattice parse-back: honest at every probed edge (verified good)

`core/src/main/java/com/legend/exec/Executor.java:126-136`. The
parse-back triggers only for `rootType == DATE` String cells — Strings
under a Date root are only ever the engine's own print forms (no
user-string channel types as Date).

- mixed `[%2015-01-01, %2015-01-01T10:00:00+0000]` → kinds recovered
  (LocalDate + Timestamp), correct expectation PASSES, wrong day/wrong
  micros **FAIL loud** with correct rendered values.
- BC/negative-year: `-0500-01-01` is 11 chars → parse-back SKIPS, both
  sides stay symmetric strings → correct PASS, wrong-value **FAIL
  loud**. (BC values never reach `LocalDate.parse` — no era bug
  reachable.)
- partial date in a mixed Date collection (`[%2015, %…T10:00:00+0000]`)
  → loud DuckDB `Conversion Error: invalid timestamp field format:
  "2015"` before any compare → ERROR bucket, honest.
- minute-precision print form `…T10:30` parses (LocalDateTime.parse
  accepts it); micros survive (`.123456` ≠ `.123457` FAILS).
- midnight DateTime under the lattice keeps its KIND: expected
  `%2025-02-10` vs actual `min([...%2025-02-10T00:00:00+0000])` →
  **FAIL** (`2025-02-10 vs 2025-02-10 00:00:00.0`) and the
  DateTime-typed expectation **PASSES** — the pre-existing midnight
  narrowing (`Executor.java:137-141`, out of range) is bypassed by the
  print-form string channel, so no wrong-kind pass arrives via this
  diff. The healed engine pin (`DuckDBIntegrationTest.java:5526-5537`,
  Timestamp value equality) matches the probed behavior.
- pct.only spot-checks: `testLeast_Date`, `testMax_Date/DateArray`,
  `testAdjustByMicrosecondsBigNumber` all PASS by VALUE equality
  (`.4990000` == `.499` — same instant; the old ledger FAIL was a
  string-spelling mismatch, not a value divergence).

### F6 — compactJson: payload fidelity verified (verified good)

`core/src/main/java/com/legend/sql/dialect/DuckDb.java:31-56`
(`normalize` → `compactJson`), applied only to `SqlType.Scalar.JSON`
leaves (`Executor.java:286`).

- string payload with braces/brackets/spaces
  (`{ "k" : "a { b } [ c ] d" }`) → payload survives EXACTLY, outer
  whitespace stripped → PASS.
- escaped quotes inside the payload (`"say \"hi\" now"`) → survive; the
  escape scanner consumes `\x` pairs before testing `"` — a `\\` before
  the closing quote does not swallow it (probe PASSES).
- No whitespace outside strings is ever JSON-significant, so the strip
  cannot corrupt structure. Note `normalize` fires per-JSON-cell in
  grids too (unwrap leaves), consistent with the `filter`/`extend`
  variant-column tests that flipped to PASS.

### F7 — sweep reproducibility and ledger hygiene (verified good)

- Two consecutive full sweeps regenerate `docs/PCT_CORPUS.md`
  **byte-identical** to the committed file (the runner rewrites it every
  run — `PctCorpusRunner.java:93-94` — so an unchanged file IS the
  reproduction proof): total **1031 PASS / 75 FAIL / 78 ERROR /
  15 SHAPE**, per-suite 288/122/301/174/93/53. **No flappers.**
- The classifier now stores FULL detail and buckets only at print
  (`PctCorpusRunner.java:220-230` vs `:389-401`) — per-test ledger
  entries stay distinct even when the 300-char bucket cap would merge
  them; strictly better for flapper detection.
- `pct.only` (`PctCorpusRunner.java:132-147`) is a pure filter over the
  same `runTest` — it cannot change classification, only visibility.
- The `-9` plain-sort null-order divergence (Lowerer.java:1290-1300
  comments) is a DOCUMENTED engine-parity choice, booked as honest FAILs
  in the ledger, not suppressed.

### Cross-checks with no findings

- `TemporalEmission` extraction is verbatim (diff shows pure code
  motion; `enumName` widened to package-private only).
- The `Ran[verified=0]` → SHAPE classifier is unchanged from the prior
  audit's verified-good state; every probe in this audit that PASSED
  reported `verified=1`.
- The variant/to.pure assertError FAILs re-spelled by the
  `CAST(json(…))` SQL change remain FAILs (error-text drift only) — no
  bucket migration.
