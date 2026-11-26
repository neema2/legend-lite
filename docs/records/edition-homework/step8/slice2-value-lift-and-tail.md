# Step 8, slice 2: a print's tail and a Float's last-step print are Java's (2026-11-26)

*Written by the step 8 session (`claude/edition-step8`). The proposal and its measured reach are
`print-tail-proposal.md`; the measurement that says what the next slice may delete is
`moves-if-float-text-used-further-is-lites.md`.*

## What it does

Tenet 1 as the user amended it on 2026-11-25: at a compatibility door, how a value is written for legend-pure is
presentation, done in Java from the typed value the database computed. And the user's words of 2026-11-26: "we saw
toCSV->replace() and we should do that as one call at the end to replace newline in csv, so maybe small set up
string/count functions chained should all be in java".

- **The tail** (`edge/PrintTail.java`, new). One closed, sealed set of functions of a text, each legend-pure's own
  Java for it: `replace` (in the rule since 2026-11-24), `length`, `trim`, `startsWith`, `endsWith`, `contains`,
  `indexOf` with two and with three arguments. Where the calls after a print the door lifts are all of this set
  with literal arguments, the door takes them off the program with the print and Java applies them to the text. The
  answer is a String, or the Integer or Boolean the last step answers.
- **A value's print** (`edge/PrintedRelations`, `edge/PureValueText.value`). At legend-pure's doors a Float's
  `toString` at a last step is lifted: the program that remains computes the Float, and Java prints it by
  legend-pure's rule (`DecimalFormat("0.0")`, up to 340 fraction digits, never an exponent). At legend-engine's door
  it is not lifted: there the database's cast is legend-engine's own answer.
- **The doors.** PCT channel A lifts a last statement (`PctPlanRoute`; a value only for legend-pure's audience).
  PCT channel B and the corpus pre-read a printed side and put a literal back (`edge/UpstreamPrints`), now of the
  tail's type. A relation's printed side is read wherever it stands outside a lambda, as since B3 step 2b. A
  Float's print is read only where it is an assert's own argument, with its tail: joined to a text, bound by a let,
  in a list or under a call outside the set it is no last step, and it is left to the database.
- **The product's road is not touched.** No file outside `com.legend.edge` changed in main sources.

**The user's ruling on the set (2026-11-26, about 13:57, relayed by the orchestrator):** put to them as these seven
members with the guard exception they need, because Java computes a length, an index or a true/false over text the
edge printed: "yes should add guard". The set is pinned by `PrintTailTest`, and `JavaEvalLedgerTest` names
`PrintTail.java` as the edge's one class that computes a value.

Left out of the set, each for a reason (legend-pure 5.99.0's Java read): `toUpper` and `toLower` (the JVM's default
locale), `ltrim` and `rtrim` (commons-lang's characters, not `trim`'s), `substring` (raises out of range), `split`
(a `StringTokenizer`; answers a list), `reverseString`, `size`, `toOne`, and `lpad`, `rpad`, `repeatString` (Pure
bodies in legend-engine). `+` with literal Strings was proposed and is not built: its typed shape was not read yet.
There is no count beyond `length` among legend-pure's or legend-engine's string functions.

## What it deletes

Nothing yet. The edge's SQL still writes legend-engine's cast for a Float's text. The deletion is the next slice
(`moves-if-float-text-used-further-is-lites.md`): it needs one row of legend-engine's table from the orchestrator
and the user's word on the register row.

## What was measured

- **No recorded answer FILE moved; two pinned PCT failures on H2 did, and this record missed them** (corrected
  2026-11-26 after the independent audit of the slice, its N1). The whole edge net passes unchanged
  (`//pct:edge_contract_test`: PCT's answers and outcomes on DuckDB, SQLite and H2, channel B's outcomes, the corpus's
  on both lanes, pure/v1's answers, the edge cases), and so do both corpus rosters, PCT's legend-engine lane on DuckDB
  and H2, and the SQL size pins of the product and of the corpus. **What moved:** on H2, at legend-pure's door,
  `testFloatToStringWithNegativeExponent` and `testFloatToStringWithPositiveExponent` were pinned as expected failures
  (`Test_LegendLite_H2_EssentialFunctions_PCT`): the edge's SQL wrote legend-engine's H2 form of a Float literal there
  (`1.3421E8`), the test expects legend-pure's (`134210000.0`), and the lift's Java print now writes legend-pure's,
  so both pass. That is the answer tenet 1 asks for, but it is a moved answer. **Why the measurement missed it:** the
  H2 essential suite at legend-pure's door was not a target on this slice's base (`//gates:pct_h2`'s five suites
  joined the gate with the next batch), so no run here held those pins. The orchestrator found it in the next
  batch's audit and removed the two pins there (`fd63bd84`). `//gates:pct_h2` is in this slice's checks since.
- **The lift does hold the last-step Float prints.** With the edge's SQL cast switched off on top of this slice (the
  probe of `moves-if-float-text-used-further-is-lites.md`), PCT's essential suite on DuckDB and SQLite, `pct_h2`,
  channel B's essential suite and all 33 PCT answer files are unchanged. Run twice: on the first draft and again
  after the review's narrowing.
- **A computed Float at a last step now prints legend-pure's text** where a database's cast chose an exponent:
  `(1.0E-5 * 1.0)->toString()` is `0.00001` at PCT's legend-pure door, where the cast wrote `1e-05`. No recorded
  case holds such a value (`PctPlanRoutePrintTest` now does).
- **The tail against lite's own SQL** (`PrintTailParityTest`, on DuckDB, H2 and SQLite): every member answers as
  lite's SQL does on the plain texts a printed table holds, but for `trim` of a text that begins or ends with a
  line break. legend-pure's `trim` is Java's `String.trim` and strips it; lite's SQL `trim` keeps it. That is a
  defect of the product's `trim` with no register row; the orchestrator has filed it for the user. The tail's
  `trim` is Java's, so it answers legend-pure's.

## The checks

- The fixed pre-push set, on the reviewed code: 110 targets pass (`//core:guardrails`, `guardrails_edge`, `census`,
  `//docs:all`, `core_edge_tests` on three lanes, `core_tests_integration`, the two SQL size baselines and the
  corpus's, `//spec:spec_tests`, both corpus lanes, PCT on DuckDB, H2 and SQLite, channel B on DuckDB and H2, the
  legend-engine lane's pins, the whole edge net). One failed and is fixed: the generated list of the classes that
  name each native (`native-claims.tsv`) gained `PrintTail` on eight string natives, regenerated.
- The local gate (`--lockfile_mode=error //gates:local`) on `d4071d5`: 555 of 571 pass. The 15 that fail are the 14
  that fail only because the container runs as root (the thirteen Postgres-backed core targets and
  `//datacube:verify_app_test`), and `//tools/guards:shrink_only_check`, which could not read the history in a shallow
  clone and passes once `origin/main` is fetched in full (45 pins in 19 files, none grown).

## The independent review (Opus, high effort, given the diff and the rules)

Two must-fix, both acted on:
1. **The first draft was wider than "last step".** The pre-read read a Float's print wherever it stood outside a
   lambda, so `(1.0E-5*1.0)->toString() + ';'` got legend-pure's text in a test body and not at channel A, against
   tenet 1's "the database writes lite's own text where the program goes on computing with it". Fixed: an assert's
   own argument alone.
2. **The set had not been ruled by the user.** It has been since (above).

Should-fix, all done: stale comments and three stale ledger entries; a refusal that named the wrong cause for an
empty value; two tests that did not prove what they said (a `startsWith` case that was true without the lift; a
literal scan that the golden satisfied). It checked each member against legend-pure 5.99.0's compiled and
interpreted Java, each exclusion's reason, the overloads the walk matches, a Float's edge values (zero of either
sign, NaN, the infinities, an integer or a decimal carrier) and that no value lift reaches legend-engine's
audience, and found them right.

One of its notes stands as a known limit, from before this slice: a pre-read runs before the body, so a side the
program would not have reached (the right of an `||` whose left is true) is still read. It applied to a relation's
printed side already; a Float's print is read only at an assert's own argument, which the assert always evaluates.

## The guards that moved, each with a dated note

- `JavaEvalLedgerTest`: `PrintTail.java` registered as the edge's one class that computes a value; three entries
  brought up to date (`PureRelationText` and its writer, `PureValueText`, `PrintedRelations`).
- `WriterCarriageGuardTest`: `PrintTail.java` scanned, with one registered operation (`indexOf`'s start read as an
  int, as legend-pure's Java reads it).
- `core/edge_tests.bzl`: the two new tests listed with the edge's.
- `native-claims.tsv`: regenerated.
- No pin, ceiling or size went up, and no recorded answer file was re-recorded. (Two H2 PCT pins of expected
  failures began to pass and were removed in the next batch: "What was measured", above.)

## The independent audit and its repair (2026-11-26)

An audit of this slice by a reviewer outside the session (`../audits/step8-slice2-independent-2026-11-26.md`, on
`claude/edit-ahead`) found no blocker and seven should-fix. The repair is `slice2-repair-2026-11-26.md`.
