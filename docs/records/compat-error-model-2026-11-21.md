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
- **Catalogs:**
  - **lite's**: the codes' own templates;
  - **legend-pure's / legend-engine's** (`edge/PureMessages`): upstream's sentence for each code an upstream test or
    client reads; every other code is worded by lite's catalog at legend-engine's door, ONE ruled mapping (Q3 = B,
    ~16:40: compile errors stay lite's words), written as such, never a silent fallback.
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

## 4. Internal defects: the user's decision (asked 2026-11-21 ~00:00)

- **A.** A defect keeps its developer text, marked `INTERNAL`: the one prose a test plan may hold, a named exception
  the guard allows.
- **B.** A defect's record is `INTERNAL` and its class; its developer text goes to the log, never the plan.
- Recommended B where a measurement shows no test reads a defect's text; A for the cases that do. Measured first.

## 5. The order of work

1. **The foundation**: `ErrorCode`, `ErrorRecord`, frames, `LegendError`, the catalogs, the test plan's codec (test
   plan v35), the edge's one path; the existing kinds (`CompileKind`, `ErrorKind`, `MissingParameters`,
   `InvalidParameters`, `DataSize.TooLarge`, test data's facts) moved into the one space; B1's `Fact` and wrapper texts
   deleted.
2. **The sites**, one family at a time: `NotImplementedException`'s 417, then type, model, mapping, parse and schema
   errors. A literal message joined with values becomes a code and its values (scripted where mechanical, by hand
   where the message is built elsewhere); a site whose words a test pins keeps them exactly, or moves by name.
3. **The defects**, by the user's A or B.
4. **The guard**: no prose in the plan's or the test plan's JSON (a scan of every string field against the codes'
   and the values' shapes), no exception constructor taking a sentence; the 13 refusal-fragment sites (B1's audit (a))
   among the converted.
5. Re-pins listed by name for the user, an independent audit, the PR.

## 6. What does not change

- Lite's own sentences, where a test pins them, are kept word for word: a code's template is the sentence the site
  wrote.
- legend-pure's words at legend-engine's doors, where B1 gave them.
- The phases (`LegendCompileException.Phase`) and the exception classes: a class stays the family of its codes, so the
  catches that decide by type are unchanged.
