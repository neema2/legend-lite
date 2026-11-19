# H11: legend-pure's relation text printed in Java (the spike for slice 1.3), 2026-11-19

**Status: IN PROGRESS. Written as it is found; §0 is filled last.**

- **Branch:** `claude/compat-relation-text-spike`, from `main` `b75d6c73a`. Scratch: never proposed for merge as is.
- **Session:** `session_0195rQEjVtMXfZdcaQdweUzf`; reports to the program's owner (`session_0195rQEoSpv1FxC3WndEk5ZH`).
- **The question:** slice 1.3 deletes `edge/RelationTextLowering.java` (791 lines, a relation's `toString`, `toCSV` and
  a TDS's `csv` composed in SQL) and writes the same text in Java after the run, from typed rows. No Java printer of a
  relation existed. 423 PCT tests assert legend-pure's `toString` of a relation. Does it work, byte for byte?
- **Nothing here moves a pin, a ceiling, a ledger, a roster, a golden or an allowlist.**

## 0. The answers on one page

(filled at the end)

## 1. What was read

| Source | Where | Head or pin |
|---|---|---|
| The plan's slices 1.1 and 1.3, rulings CP-5, CP-6, CP-14 | `docs/records/compat-program-plan-2026-11-19.md` on `claude/compat-program` | fetched 2026-11-19 ~12:20 UTC |
| The log's §1 and its entries on 0.4 (f) and H6 | `docs/records/compat-program-log-2026-11-19.md`, same branch | same |
| H6, the judges' homework | `docs/records/compat-homework/H6-judges.md` on `claude/compat-judges-homework` | same |
| The old design's §3.6 | `docs/records/presentation-program-design-2026-11-18.md` on `claude/presentation-design` | same |
| The SQL builder and its facet | `edge/RelationTextLowering.java`, `lowering/RelationTexts.java`, `lowering/Lowerer.java:4300-4318` | `main` `b75d6c73a` |
| The host judge's rendered arm | `testplan/AssertVerdicts.java` (`renderForm`, `renderedPlan`), `testplan/HostJudge.planRendered`, `testexec/HostJudgments.renderedOf` | `main` |
| The edge's assert arm and its run hook | `testplan/EngineTestText.java`, `testplan/EngineTextGates.java`, `testplan/EngineTextPlanning.java`, `edge/EdgeEngineTestText.java`, `edge/Appeals.java`, `testexecutionplan/HostJudgment.EdgeJudged` | `main` |
| legend-engine's `toString` of a relation and its cell rule | `core_functions_relation/relation/functions/toString.pure`, `s.pure` | legend-engine 4.145.0 (the pin; archive sha256 matched) |
| legend-pure's value of a TDS cell and its print | legend-engine's `TestTDS.java`, `TestTDSInterpreted.java`; legend-pure's `DateFunctions.java`, `FloatCoreInstance.java`, `SimpleFloatCoreInstance.java`, `SimpleDecimalCoreInstance.java`, interpreted `ToString.java` | legend-engine 4.145.0, legend-pure 5.99.0 (the pins; archive sha256 matched) |

## 2. legend-pure's rule, read at the pin

**The text is Pure code, not a native.** `meta::pure::functions::relation::toString` (`toString.pure:19-35`):

```
'#TDS\n' + '   ' + <names joined ','> + '\n' + <one line per row: '   ' + cells joined ','> joined '\n' + '\n' + '#'
```

- **A name** is trimmed, then quoted with `'` unless it already starts with `'` or matches `^[a-zA-Z0-9_]+$`.
- **A cell** is `s(value, columnType)` (`s.pure:23-38`):
  - an empty cell is `null`, or `'null'` (quoted) under a Variant column;
  - a Variant is quoted, with `\` and `'` escaped;
  - a String holding `{` or `[` is quoted the same way; any other String is itself;
  - anything else is its `toString()`.
- **No rows:** the rows segment is empty, so the text is `#TDS\n   names\n\n#`.

**A cell's `toString()` is the interpreter's print of the value the TDS holds** (`ToString.java`: a primitive's
`getName()`), and what a TDS holds is decided by legend-engine's `TestTDS`:

| Column type | What `TestTDS` holds | Its print | Read at |
|---|---|---|---|
| Integer | a `long` | digits | `TestTDS.java:180-198` |
| Float | a `Double`; a `long` cell is cast to double | `BigDecimal.valueOf(double)`, at least one decimal, trailing zeros dropped, `toPlainString()`: never an exponent (`0.00001`, `25000000.0`) | `TestTDS.java:235-258`, `TestTDSInterpreted.java:96`, `FloatCoreInstance.canonicalizeBigDecimal`, `SimpleFloatCoreInstance.getName` |
| Number | a `Double`, a `long` cast to double | as Float | `TestTDS.java:332-356` |
| Decimal | a `BigDecimal` (`valueOf(double)` or `valueOf(long)`); under `Numeric(p, s)` set to scale `s`, HALF_UP | `toPlainString()`, no `D` suffix | `TestTDS.java:260-295`, `SimpleDecimalCoreInstance.getName` |
| any Date type | nanoseconds as a `long`, read back as `DateFunctions.fromDate(new Date(ns / 1000000))` | **always exactly three subsecond digits, truncated**, then `+0000`: `fromDate` is `fromInstant(instant, 3)` | `TestTDS.java:201-212`, `DateFunctions.java:257-268, 314-317` |
| Boolean | a `Boolean` | `true` / `false` | `TestTDS.java:215-229` |
| String, an enumeration | the text | itself | `TestTDS.java:356-367` |
| Variant | a `Variant` of the text | its `toString()`, quoted by `s()` | `TestTDS.java:296-331` |

**Measured over PCT's goldens** (`grep` of the `'   ...` lines of the relation and standard function trees at the
pin): 530 date cells, every one `yyyy-MM-ddTHH:mm:ss.SSS+0000`; none is date-only and none has more or fewer than three
subsecond digits. So `TestTDS.java:209` is what PCT expects, as the old design's §3.6 said.

**One difference from today's SQL builder, found by this reading.** `RelationTextLowering.dateTimeTextTds` writes "at
least three fraction digits": a nanosecond cell prints all nine (`.123456789`). legend-pure prints `.123`. No PCT
golden has a finer fraction, so neither form is tested; §5 says what the Java printer does.

## 3. The wiring: the edge's existing hooks carry it (no new hook in the core)

(being built)
