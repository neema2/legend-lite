# Audit: Phase 2, `beeb9b1c3` (build/phase2-tables)

"Upstream tables joined when the classes load: DynaFn and the engine handlers from upstream alone", on top of Phase 1
(`59070c2c2`). Independent, read-only audit, 2026-10-06. Worktree clean at HEAD; nothing edited, committed or pushed.
Scratch evidence is in `runs/homework/phase2/audit/` (gitignored).

## Verdict: ready after fixes

The design matches the plan (REBUILD_PROGRAM Phase 2; GENERATORS.md rows 6 and 7; UPSTREAM_ONLY_HOMEWORK section 2).
Behavior is unchanged: I re-ran the before/after comparison at HEAD and it is identical. Both generators read upstream
only (checked with aquery). Class initialization has no cycle. The guard edits are honest.

One blocker: the census of the 162 engine ids the platform does not declare is now recorded nowhere. The javadoc still
calls it "pinned". Phase 2 removed the record that the P2-16 decision relied on. The fix is small. There are also three
should-fix items (the IN_FLIGHT list on main, a test check that can never fail and that the commit message cites, and
stale comments) and a set of nits.

## What I verified (beyond reading the diff)

| Check | Result |
|---|---|
| The author's 641-line dump (`probe/DumpTables.java`, which loads EngineHandlers first), re-run at HEAD in three other load orders: `DynaFn.SQL_NULL.fqns()` first, `DynaFn.ABS.resolution()` first, the prelude first (`audit/probe2/AuditProbe.java`) | all three byte-identical to `before.txt` |
| Answers the dump does not print | `Dialect.values()`: the same 20 constants (COMPOSITE kept: 7 members use it). `Inference`/`Resolution` values: the same. `of()`/`valueOf()` round-trip for all 232 members. `liteFqn()`: the 8 SHIMs match the old rows; the other 224 throw. |
| Lite surface by AT_ group vs the old generator's by-name filter over `Pure.all()` | the same ids in the same order for all 4 names; `unmatchedSurface()` is empty |
| New `engine-handlers.tsv` vs the old file's engine rows (columns 1-2) | identical, same order (830 rows); `SEMANTICS_REGISTER.md:47`'s references to lines 583-597 still point at the `over` rows |
| `bazel aquery //spec:gen_dynafn` | inputs: `libdynafn_generator.jar`, the engine archive and the JDK; classpath is the generator jar only |
| `bazel aquery //spec:gen_engine_handlers` | inputs: `libengine_handlers_generator.jar`, `Handlers.java` and the JDK; no core jar, no committed core file |
| Each generator run twice from its jar | same bytes both times, and equal to the committed `DynaFn.java` / `engine-handlers.tsv` |
| `native-claims.tsv` | columns 1-5 byte-identical; only `also` moves (Q5) |
| Class-init timing (`probe/TimeInit.java`, 3 runs) | EngineHandlers 4.0 / 7.0 / 5.3 ms with the prelude loaded; DynaFn plus its decisions 2.3-2.9 ms. Consistent with the commit message. |
| Final gate log (`gate3.log`) | 290 pass, including `//core:guardrails`, `//core:update_generated_*`, `//spec:spec_tests` and the wasm differential lanes |

Not re-run: `--config=bazel10` analysis (the author's claim), and Linux/Windows determinism (reasoned below, not run).

## Findings

### Blocker

**B1. The undeclared-ids census lost its record, and its javadoc still says "pinned".**
- Where:
  - `core/src/main/java/com/legend/builtin/EngineHandlers.java:113`: `/** The engine's signature ids the platform declares nowhere (shrink-only, pinned). */`
  - `core/src/test/java/com/legend/builtin/EngineHandlersTest.java:77-86`
  - `engine-handlers.tsv`, whose FQN column is gone
- Evidence:
  - Until P2-16 (a) (`a2ce035eb`), the census had a dated hand pin: `assertEquals(162, EngineHandlers.undeclaredIds().size())`, history 169 → 168 → 162, each step with its reason.
  - P2-16 removed that pin because "the file is //core:update_generated's, so a bump or a declaration moves it, as a reviewed diff". The record moved into the drift-tested FQN column.
  - Phase 2 deletes that column. The new test only checks `undeclared.contains(id) || !fqnsOf(name).isEmpty()`, a weak disjunction.
  - `undeclaredIds()` is read only by that test (grep across the repo), and no report records the number.
- Effect: a declaration removed from `Pure.java` or the prelude now grows the census silently. It also silently shrinks a bare name's FQNs, and a PURE dynafunction's FQNs (which `DynaFn.java` used to record per row). Nothing turns red and no diff appears.
- Related: the IN_FLIGHT paragraph on main lists `SpecRatchets` among the files that "follow", but the commit does not touch it.
- Fix (the D9 form, which `dynafn.unsupported` already follows):
  - Add `engine.handlers.undeclared 162` to `spec/src/test/resources/com/legend/generators/ratchets.tsv` (`SpecRatchets.main`), compared by a spec test.
  - Add a dated shrink-only ceiling constant beside it: "2026-10-06, build rebuild Phase 2: the census left engine-handlers.tsv".
  - Optionally add a measured count of the PURE dynafunction declarations (`dynafn.pure.declarations`).
  - Then correct the javadoc.
  - If the user decides the census need not be recorded, say so with a dated note in the commit and drop "(shrink-only, pinned)".

### Should-fix

**S1. The IN_FLIGHT announcement on main does not list every core file the commit touches.**
- `origin/main:docs/IN_FLIGHT.md:49-55` names `DynaFn.java`, `DynaFnDecisions.java`, `EngineHandlers.java`, `engine-handlers.tsv`, `EngineHandlersTest`, `DynaFnRegistryTest`, `SpecRatchets` and `spec/`. It also says the users of DynaFn are not edited, which is true.
- Touched but not announced:
  - `core/src/main/java/com/legend/builtin/Pure.java`: a new public `liteSurfaceFunctions()`, in a heavily shared file
  - `core/BUILD.bazel`: a comment only, but IN_FLIGHT:63-64 says C1/C2 also touch it
  - `native-claims.tsv`: regenerated
  - `core/src/test/java/com/legend/ParkedWorkLedgerTest.java`
  - `core/src/test/java/com/legend/PlatformNamesGuardrailTest.java`
  - `docs/PARKED_WORK_LEDGER.md`
- Announced but not touched: `SpecRatchets`. It becomes accurate if B1 is fixed there.
- Fix: amend the paragraph on main before the PR.

**S2. A DynaFnRegistryTest check can never fail, and the commit message cites it as evidence.**
- `spec/src/test/java/com/legend/generators/DynaFnRegistryTest.java:71-75` compares `d.fqns()` with `EngineHandlers.fqnsOf(name)` whenever the latter is non-empty.
- In exactly that case, `DynaFnDecisions.Declarations` (`DynaFnDecisions.java:251-271`) sets `fqns = EngineHandlers.fqnsOf(name)`. So the comparison is always equal.
- The commit message says "DynaFnRegistryTest checks PURE declarations against the engine surface". The checks with real power are the non-empty check (:67) and the catalog-existence check (:76-80).
- Fix: replace it with a check that can fail. Suggested: the residue is used only where the engine surface is empty, i.e. `EngineHandlers.fqnsOf("sqlNull"/"sqlTrue"/"sqlFalse").isEmpty()`, and each of those members' `fqns()` equals `List.of(Pure.SQL_*.qualifiedName())`. If the engine ever registers one of them as a handler, the residue entry is dead and should go. Otherwise, delete the lines and reword the message.

**S3. Stale comments describe the old design.** All comment-only fixes:
- `spec/BUILD.bazel:331-332`: "each name with its signature ids and the platform's FQN". The file has no FQN column now.
- `core/src/main/java/com/legend/normalizer/RelOpTranslator.java:731-733`: "the registry row's FQNs (generated from the catalog)". They are now joined from the engine surface when DynaFnDecisions loads. Editing this file means adding it to IN_FLIGHT.
- `spec/BUILD.bazel:364`: "DynaFn.java's members". The file is now generated whole.
- `DynaFnRegistryTest.java:26-36` (pre-existing, but the commit edited this javadoc): it claims the test verifies every member and its dialects against the checkout. Nothing in it walks the checkout (`engineRoot()` at :53 is unused); the `//core:update_generated_*` diff test does that.
- `spec/src/gen/java/com/legend/generators/DynaFnGenerator.java:24`: "Checked by DynaFnRegistryTest". The members are checked by the diff test; the decisions by DynaFnRegistryTest.

### Nits

- **N1. The surface is declared twice, and a wrong pairing would pass the tests.**
  - `LITE_SURFACE` (`Pure.java:559-563`) and the keys of `liteSurfaceFunctions()` (`Pure.java:568-574`) are held equal by `EngineHandlersTest.java:94`.
  - A name paired with the wrong AT_ group passes every EngineHandlersTest assertion; `:47-48` checks only joinWithPrefix's package.
  - `:73-76` weakened the name check from equality to subset (`LITE_SURFACE.containsAll(onlyApi)`). No surface name collides with an engine name today (0 rows), so `assertEquals(Pure.LITE_SURFACE, onlyApi)` holds exactly.
  - Fix: add, per surface name, `idsOf(name)` equal to its group's ids, or a test-side check of each group FQN's local name. Later: derive `LITE_SURFACE` (and `Pure.Index`'s lite partition, `Pure.java:631-632`) from the id map, so the surface is declared once.
- **N2.** `EngineHandlers.java:48-51` re-computes the id of all 837 natives, but `Pure.nativeFunctionById` (`Pure.java:641`) is the same index (verified: they agree for 837/837). Only the prelude half needs its own map.
- **N3. The TSV parse could be stricter.**
  - `EngineHandlers.java:61` skips any line starting with `name\t`, so a future handler named `name` would be dropped silently. This is pre-existing, but the format is now fixed, so it can match the exact header `name\tid`.
  - `:60` uses `split("\n")`, and the id is now the last column, so a stray CR would corrupt every id. `.gitattributes` (`eol=lf`) protects the file, so this is defensive only; `lines()` handles both.
- **N4.** `DynaFnGenerator.java:61`: a registry file on an unrecognized path gets dialect `OTHER`. That now becomes an enum constant silently; before, `Dialect.OTHER` failed to compile against the hand enum. The bump's diff would show it, but throwing would keep it loud.
- **N5.** `DynaFnDecisions.java:43`: `Map.ofEntries` throws on a duplicated row when the class loads (ExceptionInInitializerError on the first `resolution()`), not as a test message. The agreed rule is "a broken decision fails a test rather than class load". Low risk: every test that touches DynaFn would fail loudly.
- **N6.** `DynaFnDecisions.java:239`: `RESIDUE` is used only by `Declarations`. Moving it into that holder would stop `DynaFn.resolution()` from initializing `Pure`; today `resolution()` → DynaFnDecisions init → Pure init. The class itself (`:25`) could be package-private.
- **N7.** `DynaFn.java:322-324` (the template): `liteFqn()` calls `fqns()`, which can trigger the engine-surface join and the prelude parse, before it checks `resolution()`. Check `resolution()` first.
- **N8.** `PlatformNamesGuardrailTest.java:90-97`: the "DynaFn.java left 2026-10-06" note sits between CoreFn's comment and the `"CoreFn.java"` entry, so it reads as a note about CoreFn. Move it below the entry.
- **N9. Commit-message precision.**
  - "So no code matches a function by its name text" is true of the surface join. The lite partition still matches by name text: `Pure.java:631-632` (`LITE_SURFACE.contains(bare)`) and `BareNames.java:78` (a `Lite.PKG` prefix test), both unchanged.
  - "Only its last column moves": 282 rows drop `DynaFn`, but 3 rows (sqlFalse, sqlNull, sqlTrue) replace it with `DynaFnDecisions`, which names `Pure.SQL_*`. That deserves a clause.
- **N10.** `docs/BUILD_REBUILD_DESIGN_2026_10_05.md:192` still puts `gen_engine_handlers` in group B ("//core:builtin plus //core:model"); it is now upstream-only (group A). `:195`: spec:ratchets' triggers now include `DynaFnDecisions.java`. Phase 1 kept this document current (`6466a9e80`). The plan's Phase 2 status line belongs on the plan branch.
- **N11 (pre-existing, informational).** `DynaFnGenerator.java:67` uses `Files.walk(engineRoot)`, which does not follow a symlinked root. Run against `$output_base/external/+http_archive+legend_engine_src` (a symlink into the repo contents cache), it fails with "no dynaFnToSql registration found". The failure is loud, and the sandboxed action and Windows CI's checks lane both work. `FOLLOW_LINKS` or `toRealPath()` would make it independent of the execution strategy.

## Answers to the six questions

**1. Equivalence.**
- No answer differs (see the table). Order-sensitive cases:
  - **A surface name equal to an engine name:** old and new both add the surface after the engine rows (old: lite rows at the end of the TSV; new: after the TSV loop, `EngineHandlers.java:77-87`). Each name's ids and FQNs are therefore engine first, then lite. No collision exists today.
  - **`undeclaredIds()` order:** TSV row order in both.
  - **`names()`:** `Map.copyOf` order is unspecified in both versions, so only the set matters.
  - **Load order:** the author's probe loads EngineHandlers before the prelude; my runs cover DynaFn first, `resolution()` first and the prelude first. All identical.
- AT_ group order vs `Pure.all()`:
  - The AT_ groups are "in constant order" (`Pure.java:2291`), and `ALL` is filled by `signature()` in declaration order (`Pure.java:369`, `:762`).
  - navigate's overloads are declared at `Pure.java:1253-1255` and grouped in that order at `:2329`. The other three names have one overload each.
  - The probe confirms equality for all four names.

**2. Class-init safety.**
- The initializers form a DAG:
  - `DynaFn` (its constants and `BY_NAME` only)
  - ← `DynaFnDecisions` (`BY_MEMBER` → DynaFn; `RESIDUE` → Pure)
  - `Declarations` → `EngineHandlers` → {`Pure`, `Prelude`}
  - `Pure` mentions DynaFn and EngineHandlers only in comments. `Prelude` and `SignatureMangle` live in `:parser`/`:model`, which cannot see `:builtin`.
- No cycle, so no recursion or init deadlock on the JVM, and none in TeaVM either.
- Product paths to EngineHandlers:
  - BareNames (`NameResolver.java:1717`, `ResolvedNames.java:29`, `FunctionCompiler.java:45`)
  - `DynaFn.fqns()`/`liteFqn()` (`RelOpTranslator.java:714`, `:737`; `GroupBySynthesis.java:245`)
  - All of them run inside a compile whose `Compiler.boot()` (`Compiler.java:278-279`) loads the prelude. No product path pays the prelude parse for EngineHandlers alone. Tests that touch it in isolation now do (about 100 ms).
- Both TeaVM suppliers embed `prelude.pure` and the TSV (`wasm/.../PreludeResources.java:22`, `sdlc-server/.../PageResources.java:15`), so EngineHandlers' new dependency on the prelude cannot meet a missing resource. The wasm lanes pass.

**3. Generators.**
- Upstream-only: confirmed by aquery (table above).
- The template is correct Java:
  - zero-dialect members (between, case, dayOfMWeek, not, reverse) are empty varargs;
  - many-dialect members are ordinary varargs;
  - `__MEMBERS__;` gives the last member its `;`.
- Determinism: TreeMap/TreeSet in String order and `Locale.ROOT`. Two runs gave the same bytes on macOS.
- Windows:
  - `dialectOf` normalizes `\`;
  - the output is LF everywhere (the text block is normalized to LF, and the joins use explicit `"\n"`);
  - `.gitattributes` sets `eol=lf` on the generated files;
  - the walk does not depend on file order.

**4. Guard changes.**
- PlatformNamesGuardrailTest: removing `DynaFn.java` from `CATALOG_FILES` tightens the guard. The file is now scanned, and it holds 0 FQN literals, as do DynaFnDecisions and EngineHandlers.
- PARK-3: the anchor's file list is exact (`ParkedWorkLedgerTest.java:85-104`), so the anchor stays red-capable:
  - Any second site turns it red. A TRANSLATED fix must name `DynaFn.TO_STRING` in RelOpTranslator, because `armsAreDerivedFromTheTranslatorSource` requires it.
  - Deleting the decision row also turns it red.
  - Changing the decision to a SHIM stays green, but it stayed green before too (the old row was unqualified). Coverage is equivalent, not looser.
- Both edits carry dated notes, as AGENTS.md requires.
- Stale sites: B1 and S3.

**5. `native-claims.tsv`.**
- Only the last column moves: columns 1-5 are byte-identical over 838 rows.
- 285 rows change:
  - 282 drop `DynaFn` (`DynaFn.java` no longer quotes the FQNs);
  - 3 change `DynaFn` → `DynaFnDecisions` (sqlFalse, sqlNull, sqlTrue: `RESIDUE` names `Pure.SQL_*`);
  - `objectReferenceIn`'s `also` becomes empty, but it is still claimed (`NativeFn.ObjectReference`).
- Every move follows from `ClaimsGenerator.also()`, which records a quoted FQN or a `Pure.<constant>` reference in a source file.

**6. What a strict reviewer would block on.** B1. Then S1-S3 and the nits above.
- No dead code, unused imports or unused members in the new or changed files (the unused imports in DynaFnRegistryTest are pre-existing).
- No compile warnings, and NullAway is on for `:builtin`.
- The commit-message overstatements are S2 and N9.
