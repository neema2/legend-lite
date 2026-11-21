# The one error model (stage B's BE), the design

Stacked on B1 (PR #130), before B3. Ruled by the user 2026-11-20 ~23:55 ("we need to do the 'one error' work first");
the model was proposed ~21:00, answering "what is the expert way to deal with errors holistically". The plan's section:
`compat-stage-b-plan-2026-11-20.md`, BE.

## 1. The rule

Every error lite raises is ONE record: a **code**, its **values**, its **position** and the **frames** it passed through.
No record holds prose: not the plan, not the test plan, not an exception's fields. Words are made at the last step, by
a **catalog** for the audience reading them. The precedent is rustc's diagnostics: a typed struct per diagnostic, a
slug into a per-locale catalog, the struct rendered at emission.

## 2. The parts

- **`error.ErrorCode`**: a sealed interface every kind implements: `CompileKind` (compile errors), `RefusalKind`
  ("not supported" refusals, new), `ErrorKind` (run-time errors, B1's), and the frames' `FrameKind`. A code has a stable
  name (its JSON) and lite's template (`%1$s` positional over the values).
- **`error.ErrorRecord(code, values, position, frames)`**: values are the facts the error is about, each a Pure spelling
  (a name, a type as Pure writes it, a count): data, not prose. `position` is the driver's `[line:col]`.
- **Frames** replace lite's wrapper text (B1's `prefix`/`suffix`): `IN_FUNCTION(fqn)`, `INLINED_VIA(chain)`,
  `CALL_ARGUMENT(function, index)`, ... Each renders through the catalog around the inner error.
- **`error.LegendError`**: implemented by every lite exception: `ErrorRecord record()`. Its `getMessage()` is
  `LiteCatalog.render(record)`: errors render themselves (the user's (d), ~20:30).
- **ONE catalog, its words legend-pure's** (the user's ruling of 2026-11-21 ~02:40, replacing Q6 of 2026-11-10 and
  Q3 = B's "compile errors stay lite's words"): a code's sentence is legend-pure's wherever legend-pure has one, read
  from the pinned sources. Lite's own words only where it must: an error legend-pure does not have (lite's refusals,
  a database's capability, a data load); a value only the database computed, not read back by the lean-SQL rule
  (legend-pure's sentence with `<computed>`, D1); legend-engine's wrapper (`Raised: ...`) at the engine's door
  alone. The lite and legend-pure audiences then read the same words; `PctPlanRoute.Audience.words` comes down to
  the wrapper.
- **The test plan**: a `Failure` carries the record, not a message (`Failure.message`, `Fact`, the wrapper texts go).
  The run rebuilds the exception from it, so its message is lite's sentence again and the edge's words are the same
  function of the same record.
- **The edge**: `PureMessages.compile(Throwable)` walks for one thing, a `LegendError`'s record. The six shapes it
  walks today (a raise site, missing and invalid parameters, a file too large, test data's facts, a planned compile
  failure) become codes of the one space.
- **Answers to clients** (lite's API, pure/v1, the hosts) carry the record and the sentence for that client's audience:
  an answer may hold words; a plan may not.

## 3. Scope, measured 2026-11-21 ~00:00

| errors | sites in core | today |
|---|---|---|
| compile errors (`TypeInferenceException` 269, `ModelException` 87, `MappingResolutionException` 69, `ParseException` 11, `SchemaInvariantException` 6) | 442 | about 30 kinded (20 `CompileKind`s) |
| "not supported" refusals (`NotImplementedException`) | 417 | none kinded |
| run-time errors (`DataError`, raise sites) | 8 + B1's | kinded (B1) |
| internal defects (`IllegalStateException` 997, `IllegalArgumentException` 420) | ~1,400 | developer text |

The test plan's closed list also carries Java's own exceptions (`NullPointerException`, `NumberFormatException`, ...)
as planned failures, with their text.

## 4. Where an error is raised (the user's rulings of 2026-11-21: "defects def fail loud", ~00:10; "D", ~03:25)

Decided by upstream's answer, stated once per code from the pinned sources, never by the exception's class:
- **legend-pure never raises it** (a lite refusal), **raises it at compile time**, or **lite's own defect**: refused
  at planning, naming it. A test body lite cannot fully plan -- one of these anywhere in it, an untaken `if` side
  included -- is refused whole, as a query is. Refusals never enter a test plan, so they need no codes here (later
  polish for API clients).
- **legend-pure raises it at run time**: a run-time code, raised at its turn (`assertError` catches it as upstream
  does), even where lite could find it at planning. The census (claude/compat-planned-failure-census, 410aa467)
  found 8 such errors lite files as model errors: the window frames, `timeBucket`'s checks, a variant output.
- So a test plan holds run-time errors alone, each a code. The cost: 2 tests that pass upstream fail on lite
  (`mapping::inheritance::relational::testGetAll` and its `union::` twin, DuckDB and H2: a refusal in an untaken `if`
  side), a register row; a failing test's reason may name the first refusal found in planning (about 5 corpus rows
  on DuckDB, 6 on H2). Measured before landing on the stress lanes, core's suites, the corpus, PCT and Postgres;
  anything beyond these comes back to the user first.
- **Mapping errors (the user's option 1, 2026-11-21 ~06:45; the D audit's finding 3):** D first kept every
  `MappingResolutionException` at its turn by its class, against the rule above. Of its 73 sites, ~14 are errors
  legend-engine's router raises at run; until those have codes in its words, every mapping error refuses the body at
  planning (register S393). The codes, and the rest of that research, are a follow-up:
  docs/records/compat-mapping-errors-followup-2026-11-21.md.

## 5. The order of work

1. **The foundation**: `ErrorCode`, `ErrorRecord`, frames, `LegendError`, the catalogs, the test plan's codec (test
   plan v35), the edge's one path; the existing kinds (`CompileKind`, `ErrorKind`, `MissingParameters`,
   `InvalidParameters`, `DataSize.TooLarge`, test data's facts) moved into the one space; B1's `Fact` and wrapper texts
   deleted.
2. **Refused at planning** (§4): compile errors, refusals and defects anywhere in a test body; the 8 run-time errors
   moved to their turn; each code states upstream's phase, and a guard refuses a code without one.
3. **The catalog in legend-pure's words** (§2): the 51 kinds of today first (about 113 test and ledger files name one
   of lite's sentences), then the compile sites kind by kind as each gets its code, each sentence read from the
   pinned sources; every pinned message that moves listed by name for the user.
4. **The guard**: no prose in the plan's or the test plan's JSON (a scan of every string field against the codes'
   and the values' shapes), no exception constructor taking a sentence; the 13 refusal-fragment sites (B1's audit (a))
   among the converted.
5. Re-pins listed by name for the user, an independent audit, the PR.

## 6. What does not change

- (Superseded 2026-11-21 ~02:40: lite's own sentences give way to legend-pure's wherever legend-pure has one, §2;
  each pinned message that moves is listed by name.)
- legend-pure's words at legend-engine's doors, where B1 gave them.
- The phases (`LegendCompileException.Phase`) and the exception classes: a class stays the family of its codes, so the
  catches that decide by type are unchanged.
