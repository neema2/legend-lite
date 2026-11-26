# Step 8: what moves if a Float's text used further is lite's own (measured 2026-11-26)

*Measured by the step 8 session on `claude/edition-step8` at `da181cc` (the code is `9c751659`'s), for the
orchestrator, who takes the list to the user. Tenet 1 as the user amended it on 2026-11-25: at a compatibility door
a print at a last step is Java's, written with upstream's text from the typed value; a print used further is lite's
own text, written by the database.*

## The probe

Three edits at the edge, never committed (their source is `probe-float-text-used-further-is-lites.patch.txt`):

- `edge/PurePrintForms`: the pass no longer rewrites a Float's text (`FLOAT_TO_TEXT`: `toString`, `format`'s `%s`
  and `%f` of a Float) to legend-engine's in-query cast, so the product's own text is written, six decimals;
- `edge/PureText`: the twelve `floatToText` overrides write the product's text too;
- `edge/EdgePureOnlyTexts`: legend-pure's `%r` and `toRepresentation` of a Float is the product's text of a Float,
  where it wrote `FLOAT_SHORTEST_TEXT` (and an integer-held Float column its digits).

The probe changes every compatibility door at once and lifts nothing, so it shows **more** than would move: it
includes the last-step prints a lift will hand to Java, and the prints inside a legend-engine store query, which
are rows of legend-engine's table by the user's ruling of 2026-11-26. Every recorded answer file was then
re-recorded (`//pct:update_edge_contract`, `//spec:update_edge_corpus`, `//core:update_edge_pure_v1`,
`//core:update_edge_cases`, `//pct:update_engine_lane`) and the diff read line by line; `//pct:pct_duckdb`,
`//pct:pct_h2`, `//pct:pct_sqlite`, `//pct:pct_channel_b`, `//pct:pct_channel_b_h2`, `//spec:corpus_duckdb`,
`//spec:corpus_h2` and `//core:core_edge_tests` were run on it. The tree was restored after.

## Everything that moved under the probe

| # | Where | The test | The expression | Today | Under the probe | What it is |
|---|---|---|---|---|---|---|
| 1 | PCT channel A, DuckDB and SQLite | `string::tests::toString::testFloatToStringWithNegativeExponent` | `1.3421e-8->toString()` (four cases) | `0.000000013421`, passes | `0.0`, fails | a print at a last step |
| 2 | PCT channel B, DuckDB and SQLite | the same test | the same, a side of `assertEquals` | passes | `expected: "0.000000013421", actual: "0.0"` | a print at a side's root |
| 3 | PCT's legend-engine lane, DuckDB | the same test | the same, inside the store query | passes, as legend-engine does | fails | inside a store query at legend-engine's door |
| 4 | PCT's legend-engine lane, H2 | `string::tests::toString::testFloatToStringWithPositiveExponent` | `1.3421e8->toString()` | `1.3421E8`, fails as legend-engine's H2 does | `134210000.0`, passes | inside a store query at legend-engine's door |
| 5 | the corpus, DuckDB and H2 | `relational::validation::showcase::standalone::validateAllConstraints` and `validateAllConstraintsWithPostTDSOperation2` | a constraint's message, `'Quantity not in range: ' + $this.quantity->toString()`, over a Float property held in an INT column | `Quantity not in range: 1000000`, passes | `Quantity not in range: 1000000.0`, fails | inside a store query at legend-engine's door |

**Nothing else moved.** No line of pure/v1's answers or of the edge cases; no other PCT case on DuckDB, SQLite or
H2 on either channel (`//pct:pct_h2` and `//pct:pct_channel_b_h2` pass under the probe); no other corpus test on
either lane (the two rosters each gain exactly the two tests of row 5). The `%r` change alone moved nothing: every
recorded `%r` of a Float is of a literal whose six decimals are its whole text (`3.14`).

Four of core's edge unit tests fail under the probe, as they must: they pin the texts the probe replaces
(`AuditRound3Test.floatReprLargeBand`, three of `FloatTextTest`, and
`TypeInferenceIntegrationTest.testFloatToStringWithNegativeExponent`).

## What is left once the two rules are in place

- **Rows 1 and 2 do not move** once a Float's `toString` at a last step is lifted at legend-pure's doors: Java
  prints legend-pure's `0.000000013421` from the double. That is the value lift (the next slice); the probe has no
  lift in it. To be confirmed by running these two tests on the lift.
- **Rows 3, 4 and 5 do not move** once a Float's `toString` inside a store query at legend-engine's door is a row of
  legend-engine's table (the user's ruling of 2026-11-26, "agreed on float/date text for engine"). The row keeps
  what the edge's pass writes today. It is the orchestrator's to add (`edge/EngineTable`); what it must hold:
  - the call: `toString` of a Float, and the Float operand of `format`'s `%s` and `%f` (all three are the node
    `FLOAT_TO_TEXT` today);
  - the rule: the database's cast, `CAST(x AS VARCHAR)`, as legend-engine's `toString` dynaFunction writes it
    (legend-engine 4.145.0, `duckdbExtension.pure` and `h2Extension2_1_214.pure`, cited in
    `edge/PurePrintForms.java:107-110`);
  - a Float literal operand as legend-engine's literal processor writes it: legend-pure's print as an exact
    numeric, bare (DuckDB and the databases that keep the default processor), and on H2 that print cast to FLOAT
    (`edge/PurePrintForms.FloatLiteral`, `PRINT_BARE` and `PRINT_AS_FLOAT`);
  - where it is written today: `edge/PurePrintForms.floatOf` and the twelve `floatToText` overrides in
    `edge/PureText`;
  - the evidence: rows 3, 4 and 5 above.
- **So the list of test answers that move is empty**, on PCT (both channels; DuckDB, H2, SQLite), the corpus
  (DuckDB, H2), pure/v1 and the edge cases, provided the lift and the row are in place first.

## What does change, with no test on it

A difference from today that no recorded test holds, for the register:

- **At legend-pure's door, a Float's text used further** (`toString`, `format`'s `%s`, `%f` and `%r`,
  `toRepresentation`, where the program goes on computing with the text) is lite's own: six decimals, trailing
  zeros trimmed, a whole value keeping one. The three examples are worked from the two texts' rules, not run:
  - `(0.1 + 0.2)->toString()->length()`: 19 today on DuckDB (`0.30000000000000004`), 3 after (`0.3`).
  - `format('%r', [1.0/3.0])` joined to something: `0.3333333333333333` today, `0.333333` after.
  - `1.3421e-8->toString()->length()`: 14 today (`0.000000013421`), 3 after (`0.0`).
  With the tail (`edge/PrintTail`), a text followed only by `length`, `trim`, `replace`, `startsWith`, `endsWith`,
  `contains` or `indexOf` with literal arguments is still a last step, so the first and third examples keep
  legend-pure's answer; they change only where the text feeds something outside that set.
- Not measured: the server and cloud databases' PCT lanes.

## What it lets the product delete

With the lift and the row in place: `FLOAT_SHORTEST_TEXT`, `sql/dialect/FloatShortestText.java`, `FLOOR_RAW`, their
arms in twelve dialects, the hooks `spellsFloatShortestText` and `readsLongTextOnce`, Snowflake's own spelling and
BigQuery's override (slice 1's list, parked, plus `FLOOR_RAW`); and at the edge the Float half of `PurePrintForms`
with `FloatLiteral` and the twelve `floatToText` overrides, unless the row reuses them.
