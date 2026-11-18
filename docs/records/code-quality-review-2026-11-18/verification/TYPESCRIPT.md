# TYPESCRIPT verification

## Verdict on the unit: agree (MIXED)
The core really is sound. The brands, the closed protocol unions, the role interfaces and the small doors all check out
(see the spot-checks below). The debt the reviewer lists is mostly real: the plan decoder works by casts,
`Boundary.java` is a grab bag, the planner clients are duplicated, and `runPlan` has a confused structure. Two findings
are overstated. TYPESCRIPT-2 is real, but its proposed fix does not work as stated and its premise is partly wrong.
TYPESCRIPT-8 mostly describes constraints, not choices. Neither correction moves the unit to SOLID, and nothing found
here makes it WEAK.

## Findings
| ID | Reviewer severity | Verdict | Severity I would set | Evidence checked / correction |
|----|----|----|----|----|
| TYPESCRIPT-1 | S1 | CONFIRMED-WITH-CORRECTION | S1 (borderline) | Catch-all members `\| { readonly _type: string }` at `plan-runner.ts:143`, `:161`; `JSON.parse(plan) as ReadPlan` at `:188` and again at `:530`; `PLAN_VERSION = 51` in Java/TS/Python; the 70-line changelog at `:63-131`. All as described. Corrections: about 29 assertions on code lines, not 31 (one grep hit is a doc comment at `:334`, and `:316` is an unrelated `Uint8Array` cast). There is no schema to generate from: `PlanJson.java` is a 1,423-line hand codec with tag enums, so generating types means building a schema or a reflective generator (large). The runner logic duplicated in TS and Python is forced, because each host runs on its own database; only the types, the version and the decoding could be generated. Python's version is held equal only by behaviour (the shared fixture), not by `PlanVersionsAgreeTest`. |
| TYPESCRIPT-2 | S1 | OVERSTATED | S2 | Sizes and duplications are right (757/375/87 lines; `withoutComments` and `withoutStrings` copied; the census at `app-runs.test.ts:183-210`; lizard CCN 32/26/23/19 reproduced). But: (a) the core "a layer alone" rule already uses the compiler on Bazel's declared closure (`layer-alone.test.ts:131-160`). Only the side rules are regex. (b) The proposed `tsc -b` cannot express the app-to-run rule, because project references are transitive (verified by experiment). (c) "Drop the census, it adds nothing" is wrong. (d) The repo has no ESLint, and TS 7.0.2 complicates typescript-eslint. (e) "1,650 lines protect 284 lines of doors" is a misleading ratio. Details below. |
| TYPESCRIPT-3 | S2 | CONFIRMED-WITH-CORRECTION | S2 | 1,004 lines and 0 imports. There are more FQNs than claimed: 246 `com.legend.*.Type` occurrences on code lines (214 lines), not 172; `java.util` 70 is right. Round trips at `:463`, `:468`, `:477`, `:484` and regex surgery at `:521-528` confirmed. `Boundary.intOrNull` (`:845-849`) truncates, and also turns a non-number into null without a word, while `catalogColumns` defaults `notNull` to false. `HostOperationsJson.intOrNull` (`:200-208`) is strict. Imports cost TeaVM nothing (`HostOperationsJson.java:6-10` uses them). Cost not mentioned: `PlannerSurfaceTest` reflects over `Boundary`'s public statics (`:819-841`) and regex-reads its method bodies (`:844`, `:1447-1460`), so a split also means rewriting that guard. |
| TYPESCRIPT-4 | S2 | CONFIRMED | S2 | `KEYS` (`HostOperationsJson.java:102-123`) and the switch arms (`:57-94`) list the keys twice. The tombstones are present: `insert`, `secret`, `SQL_KEYS`, and the `file`/`remote`/`query` refusals. Worse than described: `PlannerSurfaceTest.hostOperationKeys` (`:1472-1527`) depends on exact indentation (`ARM = "\\n {12}case \"(\\w+)\" ->"`, `"\n            default ->"`) and the literal text `return switch (op) {`. The StrictObj alternative needs no reflection or casts, so it is TeaVM-safe. Small. |
| TYPESCRIPT-5 | S2 | CONFIRMED-WITH-CORRECTION | S2 | Both clients send the same `RequestBody` (`lite-planner.ts:42`, `:210`; `wasm-grammar.ts:227-307` vs `lite-planner.ts:741-838`), one line per kind each; `request: string` at `planner-protocol.ts:63-81`. Corrections: 19 product files name `snapCopy`, not 23, and several of those only in comments. Part of the roughly 11 edit sites is forced: TeaVM needs one `@JSExport` per function (36 in `TabExports`) and GraalVM one `@CEntryPoint` per function (`nativelib/Compiler.java:253`), unless a single dispatching `ask(kind, json)` export is adopted. The avoidable part is the two TS clients and the untyped wire request. |
| TYPESCRIPT-6 | S2 | CONFIRMED | S2 | `read()` re-inlined, with a shadowing `const read` (`:530`); the target check copied (`:556-558` vs `:205-211`); a dead `'payload'` arm at `:577` after the `continue` at `:575`; `deliver`'s positional optionals and out-parameter (`:402-403`); `putFile` pairs `inLoadsTransaction` with `runLoad` and parses twice (`model-data.ts:147`). lizard: CCN 39, 89 lines. Also missed: `stream()` and `transacted` already disagree on commit failure (see Missed). |
| TYPESCRIPT-7 | S2 | CONFIRMED | S2 (low end) | `rowsOf` builds JSON by template (`:800-810`); `table-output.ts:72-81` parses it back for `[0].value`. No byte-parity need: every test `deepEqual`s the parsed value. The blast radius is small: there are only two product callers (`table-output.ts:72`, and `snap.ts:338`, which ignores the answer). |
| TYPESCRIPT-8 | S2 | OVERSTATED | S3 | The counts are right (6 `sending`, 15 `planning`, 24 `inRun`, 229 `asTest(`, 52 `inRun('a test'`). But the `sending()` sites already are the choke points: 5 private primitives of `DuckDbEngine` and the warehouse submit. `planning()` is already applied once per port in `wasm-grammar.ts:49`, `lite-planner.ts:213` and `planner-answer.ts:45`; only the HTTP clients do it per method. Being blind in the browser and in V8 is forced (no AsyncContext there), the gap is named in the header, and the witness is G4's mandated run-time proof. The `asTest` tax mostly comes from wrapping on every call (`asTest(engine).execute(...)`, e.g. `chart-queries.test.ts:175-461`): wrapping once per test fixes most of it. Nested frames are harmless. |
| TYPESCRIPT-9 | S2 | CONFIRMED | S2 | `engine as unknown as Partial<StreamStore>` plus `typeof store.exclusive !== 'function'` (`snap.ts:307-308`); `typeof host.snapStream/snapCopy` checks; `state`, `isSnapped`, `sourceFor`, `release`, `detach` and `reattach` written twice (`SnapManager` vs `HostSnaps`); three `SnapInfo` builds (`snap()`, `HostSnaps.snap`, `follow`). The copies are small (about 30 lines), but the cast-based capability check is a genuine type-design gap. |

## Details

**TYPESCRIPT-1.** The substance stands. Because of the catch-all member, `step._type === 'statements'` narrows only to
`'statements' | string`, so every arm casts: `:407`, `:409`, `:429`, `:448`, `:453`, `:456`, `:544`, `:553`, `:563-564`,
`:597`, `:604`, `:607`, `:769`. The interfaces are loose: `target: unknown` at `:58`, and the database type is read from
two shapes at `:757`. The format bumped 33 times in about two weeks (18 on 2026-10-29, 51 on 2026-11-12), and most
changelog lines record a no-op for this runner ("refused by name here"). That is the cost the reviewer names.

Three corrections:
- **Nothing to generate from yet.** "Generate from the one schema that already exists" overstates what is there.
  `PlanJson.java` (1,423 lines; the package is 5,146) is a hand codec (`o.put("_type", NodeTag.update.name())` ...), so
  generation means first making the wire shape declarative. That is large. TeaVM is no obstacle, because the generator
  would run on the JVM at build time, as `TypeFacts` does.
- **What generation would and would not remove.** The per-host runner logic (transactions, refusals, binding) is not
  removable: the tab runs on DuckDB-WASM and Python on duckdb-python. Generation removes the types, the decoding and the
  version constants, not "most of" `_runs.py`.
- **A cheaper first step exists.** Close the TS unions, write one validating `readPlan(text: PlanText): Plan` and
  generate only `PLAN_VERSION`. That is S2-sized and removes the casts, but not the cross-language drift. S1 is right
  only for the full fix.

**TYPESCRIPT-2.** What the reviewer gets right:
- About 860 lines of hand-lexing and regex analysis.
- `withoutComments` and `withoutStrings` copied, argument splitting written twice, a fourth import parser in
  `PlannedSqlTest.java:346-360`.
- A census keyed on receiver names (`RUN_CALL`, `app-runs.test.ts:217`).
- Self-imposed obfuscation: `layer-alone.test.ts:182` writes "written in pieces: the repository's import scanners read
  this file's text", and builds `import` as `['im','port'].join('')`. Overlapping scanners force guards to dodge each
  other.
- The scanners have real holes: `withoutComments` treats `'a//b'` as a comment, and `closing()` ignores escapes.

The claim and the alternative do not hold as written:
1. **The build's part is already done the build's way.** The main rule copies exactly the Bazel-declared closure into a
   temp directory and runs the pinned `tsc` (`layer-alone.test.ts:131-160`; TS2307 at `:179-199`). It is a hand-built
   equivalent of a sandboxed per-target typecheck, not regex. The regex rules are the ones no compiler or
   project-reference setup covers:
   - computed specifiers;
   - literal `new Worker(...)`, `new URL(..., import.meta.url)` and `importScripts` specifiers (tsc follows none of
     them: verified);
   - the run-signature rule.
2. **`tsc -b` with project references cannot express "an app reaches the doors but not the run layer below them".**
   References are transitive. In an experiment with TypeScript 6.0.2
   (`/tmp/claude-0/-home-user-legend-lite/e46cefdc-2e0e-59c4-80ed-3422bd6cbcd6/scratchpad/review/verify/tsref-exp`), an
   `app` project referencing only `doors` imported a `run` file and built clean. A `planner` project referencing only
   `records` got TS6307 on importing `run`, including a literal `import()`, but not on `new Worker(new URL(...))`. So
   the reviewer's premise "once the apps cannot import a run module, which the build enforces" is false under the
   proposed fix: the build would not enforce it, and `runReaches` (`app-runs.test.ts:151-182`) guards a real hole.
3. **"Drop the census, it adds nothing" is wrong.** TypeScript is structural, so an app calling `.run`/`.rows` on an
   engine a door hands it imports nothing of the run side (the file's own header, `app-runs.test.ts:19-24`). The fair
   criticism is that the census is textual: a checker-API rule on calls whose receiver type is
   `QueryEngine`/`HeldConnection` would be the robust form.
4. **Lint is not a drop-in.** The repo has no ESLint at all. It pins TypeScript 7.0.2, whose JS API is the "unstable
   sync" API (`brand-casts.ts:15-20`), while typescript-eslint's parser is built on the classic `typescript` API. Lint
   would need a second TypeScript in the lock, plus at least two custom rules (literal worker and URL specifiers, run
   signatures).
5. **The Bazel-native form is rules_ts, not `tsc -b` with directories.** That means `ts_project` per layer, whose
   sandbox holds only declared deps. It is not in `MODULE.bazel`, so adopting it is medium: TS 7 rules out rules_ts's
   worker, and Windows needs checking (unverified). `tsc -b` would duplicate the BUILD graph in tsconfig `references`,
   two sources of truth. Moving files into directories is unnecessary: `files` or `srcs` lists work.
6. **The ratio is misleading.** The 1,654 lines include `bundle-inputs.test.ts`, which the reviewer lists as a strength,
   and `TsLayeringTest.java` plus `ts-layers.txt`, which are deps pins and not regex. The guards cover the layering of
   all of engine-client (6,979 lines), DataCube's layers and three apps, not "284 lines of doors".

The right fix is medium: rewrite the regex rules on the pinned compiler's own API, as `brand-casts.ts` already does, with
one shared scanner, and possibly move to `ts_project` later. That is a guard refactor in three files, not a unit
redesign: S2.

**TYPESCRIPT-3.** The reviewer undercounted: 246 FQN occurrences on code lines, and 38 `public static`. Two further facts:
- `catalogColumns` (`:850-863`) defaults `notNull` with `getBoolOr("notNull", false)`, and `intOrNull` maps a non-number
  to null. Both are silent defaulting under invariant 4. The reviewer saw the truncation only.
- The surface guard is coupled to this file's shape (one class, public statics, `only(...)` text), so the expert split
  pays for a guard rewrite too. Typed request records would in turn make that guard simpler.

**TYPESCRIPT-5.** 19 product files, not 23. Generating `PlannerModule['exports']` from `TabExports` is feasible, as for
`TypeFacts`. A single generic `ask(kind, json)` export on both builds (TeaVM and native) would remove the per-kind Java
and Python sites outright, which the reviewer did not consider.

**TYPESCRIPT-8.** Real but small. The reviewer's three "apply once" fixes are already two-thirds in place:
- `sending()` sits in the connection primitives.
- `planning()` sits in the port `ask` for the WebAssembly clients.

The remaining per-method `planning()` calls are 9, in the two HTTP clients (`planning-client.ts:96-140`,
`legend/engine.ts:66-80`) plus 2 in `datacube/src/planner.ts`. Browser blindness cannot be fixed today. The test tax is
real but has a one-line remedy (`const e = asTest(engine)` once per test). That makes it a ceremony pattern: S3.

## S3 and strengths spot-checks
- **TYPESCRIPT-10 counts: exact.** engine-client/src is 6,979 lines; 2,170 are comment lines; there are 148 ISO dates.
- **TYPESCRIPT-12 counts: exact.** `e instanceof Error ? e.message : String(e)` appears 16 times in engine-client/src
  and 142 in datacube/src; `const looseJson` is defined 13 times.
- **Strength, the brands: holds.** `planned-sql.ts:23` and `:42` declare the brands, and `planned-sql.test.ts:23-42`
  carries nine `@ts-expect-error` negatives. One caveat: `fromPlan` is a bare cast, so the brand's guarantee rests on
  the guards that restrict where it is called.
- **Strength, the kind-list check: holds.** `planner-protocol.ts:111-112`
  `Same<RequestBody['kind'], RequestKind> = true` keeps the kind list and the union in step at compile time.
- **Framing inconsistency.** The reviewer praises `bundle-inputs.test.ts` and then counts its 118 lines in TYPESCRIPT-2's
  cost.

## Missed (if any)
- **The two transaction paths disagree on a failed COMMIT.** `transacted` (`plan-runner.ts:240-258`) sends the commit
  outside its `try`, so a failed COMMIT is never rolled back. `stream()` (`:357-395`) commits inside its `try`, so a
  failed commit is rolled back. Python's `run_write` (`_runs.py:144-160`) also commits inside its `try`. The hosts claim
  "the same rules", and already differ here. This strengthens TYPESCRIPT-6 (implement `stream` through one
  `transacted`) and TYPESCRIPT-1 (the cross-host copies drift).
- **`Boundary.catalogColumns` defaults silently** (`notNull` to false; a non-numeric precision or scale to null), while
  `HostOperationsJson`'s decoder of the same shape refuses (`:200-208`). This is an invariant-4 defect, not only a style
  one.
- **`PlannerSurfaceTest.hostOperationKeys` depends on exact indentation** (`:1465`, `:1474`). Reformatting the switch
  breaks or silently empties the guard.
