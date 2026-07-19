# Audit PCT2-A — Overfit / Hardcoding, range 64311065..HEAD (PCT-native arc, 7 commits)

Auditor: PCT2-A (overfit/hardcoding). Range: `fa9da3b2..8bd4aa98` (OrderPolicy
PURE_ORDERED; fromJson→JSON_PARSE + DuckDb.normalize compaction; Scalars→
TemporalEmission split; pct.only + null-order ledger; pure temporal spelling in
TDS cells; DATE-lattice parse-back; average-is-Float cast + TypedCollection
guard). Diff read hunk by hunk. Ground truth: /Users/neemsandv/legend/legend-pure
(`m4/.../primitive/date/{DateFormat,DateFunctions,AbstractDateWithSubsecond}.java`)
and /Users/neemsandv/legend/legend-engine (`TestTDS.java`,
`VariantInstanceImpl.java`, `core_functions_standard/math/aggregator/average.pure`,
`core_functions_variant/functions/convert/fromJson.pure`,
`core_relational/.../sqlQueryToString/extensionDefaults.pure`,
`core_functions_relation/.../asOfJoin.pure`), plus empirical probes of the local
DuckDB (`json()` canonicalization, `strftime` year padding). Prior audit
docs/audit-pct-a-overfit.md consulted; its F1–F9 are not re-reported.

## Verdict

No HIGH. The range's two temporal moves are better grounded than they look:
`.SSS+0000` is EXACT for the relation-suite wire because real pure's TestTDS
parses every CSV datetime through `fromCalendar(…, Calendar.MILLISECOND)` —
`String.format("%03d")`, always three digits (F1) — and the DATE-lattice
parse-back round-trips lite's own strftime emission faithfully because DuckDB
pads `%Y` to four digits (F3). The residual channels are: one genuinely SILENT
wrong-value path — `DateTimeFormatter` `SSS` TRUNCATES sub-milli digits, so a
lite value wrong only at micro precision prints identically to a correct
3-digit pin (F1b); and two CONFIRMED-OVERFIT keyings — `compactJson` is a
whitespace-only canonicalizer standing in for pure's full Jackson parse-reprint
(number lexemes, unicode escapes, duplicate keys all preserved where pure
normalizes them; calibrated to the ASCII fixtures, fail-loud when it bites,
F2), and the average guard keys "lowers as an array" on the MIR node class
`TypedCollection` instead of the lowered SQL shape — the commit's own corpus
delta (testNullableSalaryMapping, testSubAggregationMultiLevel FAIL→ERROR)
proves to-one shapes escape the guard and die at `CAST(list AS DOUBLE)` (F4;
loud direction, wrong layer). JSON_PARSE≡Jackson on every probed edge except
duplicate keys (DuckDB preserves both, Jackson keeps last — inert today, F5).
PURE_ORDERED, the TemporalEmission split, the null-order ledger, and pct.only
audit clean or defensible (F6–F9).

**Counts: HIGH 0, MED 3 (F1, F2, F4), LOW 3 (F3, F5, F7), CLEAN 3 (F6, F8, F9).**

---

## F1 — MED — DEFENSIBLE core with one SILENT truncation channel: pureDateTime's 'yyyy-MM-dd'T'HH:mm:ss.SSS'+0000

`core/src/main/java/com/legend/StatementExecutor.java:292-300` (tdsCell temporal
arms), `:303-306` (pureDateTime).

Real mechanism, verified against BOTH print layers:
1. Pure's default DateTime print (`DateFormat.append`,
   `legend-pure-m4/.../date/DateFormat.java:354-384`) is precision-preserving:
   subsecond digits AS STORED (`.append('.').append(date.getSubsecond())`),
   NOTHING when `hasSubsecond()` is false, `+0000` only when `hasMinute()`,
   and the year via `append(date.getYear())` — an int append, UNPADDED
   (year 985 prints `985-01-12`).
2. But the channel these cells travel is the relation-suite TestTDS wire:
   `TestTDS.java:202-210` parses every CSV datetime to nanos and rebuilds via
   `DateFunctions.fromDate(new Date(value/1000000))` →
   `fromCalendar(…, Calendar.MILLISECOND)` → `String.format("%03d", millis)`
   (`DateFunctions.java:121-124`) — EVERY seed-derived DateTime cell in real
   pure carries EXACTLY three subsecond digits. That is why the asOfJoin seeds
   spell `2000-10-25T06:30:00Z` yet the pinned grids spell
   `06:30:00.000+0000` (engine `asOfJoin.pure:26` vs `:48`). For this channel
   `.SSS+0000` is the exact ground truth, not a pin accommodation. The 29
   FAIL→PASS moves in the range (over.pure RangeInterval family, asOfJoin,
   dayOfWeek_Relation) are value-true.

Divergences that remain:
1. **SILENT (the only wrong-value channel in the range)**: `SSS` in
   `DateTimeFormatter` TRUNCATES — a DuckDB TIMESTAMP carrying micros
   (`.499999`) prints `.499`. If lite's computation is wrong only below the
   millisecond, the cell prints identically to the correct pin and PASSES.
   Real pure cannot even produce sub-milli cells on the CSV channel
   (`adjustSubseconds` THROWS adding micros to a 3-digit value,
   `AbstractDateWithSubsecond.java:85-92`), so nothing on the reference side
   ever exposes it — a classic silent surface. Principled shape: strip
   trailing zeros beyond 3 digits loudly, or print micros when non-zero (a
   non-zero micro remainder is ipso facto a divergence worth FAILing).
2. Fail-visible: a computed second-precision cell (e.g. an extend spelling
   `%2000-01-01T00:00:00`) prints `.000` where real pure prints no fraction —
   string-exact pins catch it (FAIL, honest direction).
3. The `java.sql.Date` arm uses `LocalDate.toString` — zero-padded years
   (`0985-01-12`) and a `+` prefix above 9999, where pure prints `getYear()`
   raw. Unpinned; fail-visible when pinned.
4. `+0000` unconditional vs pure's hasMinute-gate — unreachable on this wire
   (a TIMESTAMP always has minutes); no surface.

## F2 — MED — CONFIRMED-OVERFIT (subset canonicalizer): compactJson strips whitespace where pure's Variant does a full parse-reprint

`core/src/main/java/com/legend/sql/dialect/DuckDb.java:30-59` (normalize +
compactJson); pins removed at `docs/PCT_CORPUS.md` (testVariantColumn_filterOnKey…,
testVariantColumn_keyExtraction, testFlatten_LateralJoin_Nested).

Real mechanism, verified: a pure Variant IS a Jackson tree —
`VariantInstanceImpl.newVariant` does `OBJECT_MAPPER.readTree(json)` and the
variant's string form is `node.toString()` (engine
`legend-engine-pure-runtime-java-extension-shared-functions-variant/.../VariantInstanceImpl.java:88-110`)
— Jackson's compact reprint. That normalizes MORE than whitespace:
- number lexemes: `1e2` → `100.0`, `1.250` → `1.25` (Double/BigInteger nodes);
- unicode escapes decoded: `"A"` → `"A"`; solidus `\/` → `/`;
- duplicate keys DEDUPED last-wins: `{"a":1,"a":2}` → `{"a":2}`.

compactJson preserves ALL of these — it is whitespace-outside-strings only
(the scanner itself is correct: escape-pair consumption, quote state, and
in-string whitespace verified by hand). It matches pure exactly on the pinned
fixtures because those fixtures are ASCII and already canonical modulo
whitespace — calibration to the corpus shapes, the definition of the overfit
lens. Two mitigations keep it MED not HIGH: (1) every divergence surfaces as a
string-exact FAIL (the pin is pure's canonical text; lite's under-canonical
text cannot coincidentally equal it), no false-PASS channel found; (2) the
fromJson path proper now rides `json()` (F5) which DOES the full reprint — but
that means the two variant channels (fromJson-parsed vs seed-CAST cell) now
canonicalize DIFFERENTLY, an internal inconsistency that will confuse exactly
the first non-ASCII fixture. Principled shape: route normalize through
`json(x)` in SQL (one canonicalizer, the engine-verified one) instead of a
second hand-rolled one in Java.

## F3 — LOW — DEFENSIBLE, holes are loud: the DATE-lattice parse-back guards

`core/src/main/java/com/legend/exec/Executor.java:126-138` (latticeKind string
arm); emission ground truth `core/src/main/java/com/legend/lowering/Scalars.java:2372-2385`
(encodeMixed STRICT_DATE/DATE_TIME identity strings), `:2419-2426`
(dateTimeFormatOf).

The strings this arm parses are LITE'S OWN wire spellings — `strftime(x,
'%Y-%m-%d')` and `strftime(x, '%Y-%m-%dT%H:%M:%S[.%f]') || '+0000'` — not
arbitrary pure prints, so the guards must be measured against that emission:
1. **Mainline round-trips exactly**: DuckDB pads `%Y` to 4 digits (probed:
   `strftime(DATE '0985-01-12', '%Y-%m-%d')` = `0985-01-12`), so
   `length==10 && charAt(4)=='-'` accepts every AD 4-digit StrictDate, and the
   `charAt(10)=='T'` arm accepts every second/subsecond DateTime.
   `LocalDateTime.parse` takes up to 9 fraction digits → `Timestamp` keeps
   them. The `.%f` (6-digit) identity print differs from pure's
   written-digit-count print, but the parse-back launders it into a VALUE
   compare, so nothing surfaces.
2. **3-digit negative year crashes at the wrong layer**: DuckDB spells a BC
   date `-499-01-01` (probed) — length 10 with `charAt(4)=='-'` → the
   LocalDate.parse arm → `DateTimeParseException` UNCAUGHT → test ERROR. Loud,
   but a parse-guard admitting a shape it cannot parse.
3. ≥5-char years (`-1500-01-12`, `10000-01-12`, `+`-prefixed) miss both guards
   → string passthrough → wireEquals String-vs-typed → honest FAIL.
4. Non-`+0000` zoned forms: lite never emits them on this wire (the `+0000`
   literal is concatenated in SQL); a hypothetical one falls into the
   `charAt(10)=='T'` arm and throws loudly.
No silent-PASS channel found. Nit: the NUMBER-arm comment eight lines up
(`Executor.java:112` — "DATE identities stay strings — the wire's date
convention") is now false and should have been updated in the same commit.
The `DuckDBIntegrationTest.java:5526-5536` pin move (string → Timestamp kind)
is the right contract pin for the change.

## F4 — MED — CONFIRMED-OVERFIT (wrong keying layer, loud surface): the average TypedCollection guard

`core/src/main/java/com/legend/lowering/Scalars.java:1068-1080` (the rule),
`:3228-3231` (isToOne); ground truth engine
`core_functions_standard/math/aggregator/average.pure:17-32` — every
average/mean overload (`Number[*]`, `Integer[*]`, `Float[*]`, the window form)
returns **Float[1]**; `testAverage_Integers` pins `average([1])` = `1.0`.

The Float-always rule itself is CORRECT and was a real bug fix (the identity
arm returned the Integer unchanged). The overfit is the guard's KEY: "a to-one
arg that lowers as an array" is decided by `instanceof TypedCollection` — a
MIR-node-class proxy for a SQL-shape property. The commit's own corpus delta
is the proof that the proxy under-covers: `testNullableSalaryMapping` and
`testSubAggregationMultiLevel` (relational corpus) carry to-one-typed
aggregation args that are NOT collection literals yet lower to arrays; the
cast fires → `CAST(INTEGER[] AS DOUBLE)` conversion error → FAIL→ERROR
(commit 8bd4aa98's message, "the loud direction"). Every future shape in the
same family — a let-bound singleton collection that doesn't splice, a nav-path
the mapping layer lowers to a list — lands on the same ERROR. Mitigations:
DuckDB cannot silently cast LIST→DOUBLE, so the mis-key can never produce a
wrong VALUE, only a loud ERROR; and `LIST_AVG` is DOUBLE-valued, so the
guarded branch is Float-true. Debts: (1) the decision belongs where the SQL
shape is known (key on the lowered `SqlExpr`/its type, or normalize the
to-one-collection lowering first); (2) the FAIL→ERROR pair is recorded ONLY in
the commit message — `docs/RELATIONAL_CORPUS.md:4531` still says FAIL for
testSubAggregationMultiLevel; the ledger the message cites was not regenerated.

## F5 — LOW — DEFENSIBLE: JSON_PARSE via DuckDB json() vs Jackson readTree

`core/src/main/java/com/legend/lowering/Scalars.java:1488-1494` (fromJson rule),
`core/src/main/java/com/legend/sql/SqlFn.java:69-73`,
`core/src/main/java/com/legend/sql/dialect/AnsiSqlRenderer.java:444`.

The move from a text-preserving `CAST(x AS JSON)` to `json(x)` is grounded:
the PCT pins (`fromJson.pure:33,43,53` — `'[1,2,3]'` from `'[ 1, 2 , 3 ]'`)
are Jackson-reprint outputs, and a parse is what pure does (F2 citation).
Empirical probe of the local DuckDB against Jackson semantics:
- MATCH: `1e2`→`100.0`, `1.250`→`1.25`, `1.0`→`1.0`, `A`→`A`, `\/`→`/`,
  raw UTF-8 passthrough, 23-digit integer preserved exactly.
- DIVERGE: duplicate keys — `json('{"a":1,"a":2}')` = `{"a":1,"a":2}`
  (preserved) where Jackson keeps last-wins `{"a":2}`; and `json('1e999')`
  raw-preserves `1e999` where Jackson collapses to a Double infinity.
Neither shape appears in any corpus fixture; both would surface as loud
string-mismatch FAILs against a real-pure pin. Inert today; worth one ledger
line since the divergence is invisible until a fixture uses it. (Side effect,
accepted: assertError message texts changed spelling from `CAST('…' AS JSON)`
to `json('…')` — those tests were already honest FAILs on message wording.)

## F6 — CLEAN (anti-overfit): OrderPolicy.PURE_ORDERED

`core/src/main/java/com/legend/harness/TestBody.java:225-252` (enum + overload),
`:1878-1883` (ordered), threading throughout;
`pct-corpus/src/test/java/com/legend/pctcorpus/PctCorpusRunner.java:181-187`.

Pure lists are ordered; the PCT expectations and the execution share that one
semantics, so forcing every compare ordered REMOVES the corpus-side multiset
leniency (the endsInSort shape walk) rather than adding any. Order divergences
that previously slid through as multiset-equal now FAIL honestly — the
anti-overfit direction. `assertSameElements` stays multiset (`compare(…,
false)`), matching real pure's order-insensitive contract. The relational
corpus keeps SQL_ORDER_POLICY via the delegating overload. One nit: the
7-arg overload silently defaults NEW callers to the lenient policy — an
explicit parameter at every call site would make the leniency a visible
choice.

## F7 — LOW — DEFENSIBLE (corpus-adjudicated, documented): plain-sort emits no NULLS clause

`core/src/main/java/com/legend/lowering/Lowerer.java:1293-1297`, `:1333-1337`
(comment-only hunks); commit 84eae148's reverted experiment.

Engine ground truth verified: the default `processOrderBy`
(`core_relational/.../sqlQueryToString/extensionDefaults.pure:495-501`) emits
`col [desc]` and NOTHING else — no NULLS FIRST/LAST — so clause-free plain
sort IS engine parity, and over()'s explicit window-null pin is correctly left
alone. The experiment record (-9 corpus / +4 PCT) is empirical grounding for
the trade, and the 4 PCT plain-sort-null FAILs stay honest. One caveat the
site comment elides: "engine parity" here is SQL-TEXT parity; the corpus
goldens pin H2's RESULT defaults while lite executes on DuckDB, so result
parity rides DuckDB's default null order coinciding with H2's on the pinned
nine — true today by measurement, guaranteed by nothing. If DuckDB's
`default_null_order` ever changes (it is a session setting), the nine flip
silently; pinning `SET default_null_order` alongside the connection's
`SET TimeZone='UTC'` would close that.

## F8 — CLEAN: the Scalars→TemporalEmission split is a verbatim move

`core/src/main/java/com/legend/lowering/TemporalEmission.java` (new, 141 lines)
vs the 123 lines deleted from `Scalars.java`. Both diff sides compared
method-by-method: intervalFn, diffPart, dateArg, dateDiffExpr, elapsed,
sundayIndex, backOneDay are textually identical (call-qualification aside);
`enumName` widened private→package-private, no behavior change. Registration
family extraction at the guardrail ceiling per the audit-20c seam; no
semantics moved.

## F9 — CLEAN: pct.only instrument + raw-detail storage

`pct-corpus/src/test/java/com/legend/pctcorpus/PctCorpusRunner.java:132-147`
(filter + full-detail print), `:166-168`/`:220-231` (details stored RAW),
`:389-403` (bucket() applied at render). `bucketCounts` (`:294-300`) still
buckets before counting, so the Top-ERROR/SHAPE tables group as before (the
PCT_CORPUS.md `\\n`→`\n` churn is the single-escape at render vs the old
double-escape at store — presentation only, counts unchanged). The substring
filter is a dev instrument outside the scoreboard contract. The
ENGINEERING_LOG grep-gate lesson (`docs/ENGINEERING_LOG.md:115-118`) honestly
records the 4a6de11c red-gate push and its heal (45bf4f9d).

---

Cross-cutting note: the range's dominant pattern is healthier than the prior
one — three of the seven commits move comparisons from STRING coincidence to
VALUE truth (parse-back kinds, Timestamp pin, ordered contracts), which
shrinks the silent surface. The two places it grew are both stand-ins for a
real canonicalizer/shape-check that exists one layer away (F2: `json()` in
SQL; F4: the lowered expr's SQL type) — the recurring lite shape of keying a
semantic decision on the artifact at hand instead of the owning layer.
