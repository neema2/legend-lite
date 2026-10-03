# Spike S5: the stress-corpus generator as a hermetic Bazel action (2026-10-03)

**Plan items:** Phase 1.3 (the Python row: rules_python as a foundation) and Phase 2.1 (stress corpus: 92–98 from `build.py`; 59, 60 and 64 from `dense_mapping.py`, `dense_store.py` and `combos.py`).
**Base:** `main` at `23b441852`. **Branch:** `spike/s5-stress-generator`, two local commits (`d20afcaa6`, `efefb9bee`). Not pushed.
**Machine:** macOS arm64, Bazel 9.2.0, bazel_lib 3.7.2, host python 3.12.6. The shared machine ran at load 30–80 throughout, so wall times are inflated; CPU times are given where they exist.
Every Bazel command used `--local_resources=cpu=3 --jobs=3`.

## Decision

**GO.** All ten generated stress files come out of Bazel actions, byte-identical to `origin/main`, over declared inputs only, on a hermetic interpreter. Ten `write_source_files` diff tests pass. No committed stress file changed.

| # | Question | Answer |
|---|---|---|
| 1 | Hermetic interpreter plus a locked pip hub on Bazel 9.2? | **Yes: rules_python 2.3.4** (BCR's latest stable; 2.4.0 is still at rc2). It provides CPython **3.12.13** (`python.toolchain(python_version = "3.12")`) and one `pip.parse` hub `@pypi` over `tools/python/requirements_lock.txt`, which holds only **tzdata==2026.5** (IANA 2026e), hash-pinned. It works with Bazel 9.2 and the existing graph. rules_python 1.7.0 was already in the graph transitively; the direct dependency moves it to 2.3.4, and `bazel build --nobuild //...` still analyses all 398 targets cleanly (E6). |
| 2 | `build.py` as an action over declared inputs, 92–98 byte-identical, with a diff test? | **Yes.** `//scripts/corpus:gen_stress` (`py_binary` plus bazel_lib `run_binary`, darwin-sandbox) writes all 7 files byte-identical (E3). `//core:update_stress_corpus` is a `write_source_files` like `//core:update_generated`, and its 10 diff tests pass (E4). Input = output is broken in two ways. (a) The 12 hand-written queries move from `92-services.pure` to a committed `scripts/corpus/queries.pure`: a pure split, proven mechanically (E2). (b) Every reader of "the corpus" (`model.load`, `model.check`, `density.load`, `build._id_collisions`) now reads `model.stress_sources()`, which leaves out all ten generated files. With the generated files out of the model, the output is unchanged (E1). **Cost: about 380–455 s of CPU** on a single core (6 min 23 s wall at 99% CPU on the quietest run; 26 min wall inside Bazel at load 80). The old `--check` cost the same: 464 s user. |
| 3 | 59 and 60: stable seed list plus the view-root rule, to a byte-identical fixpoint? | **Yes, both byte-identical. Nothing remains.** `dense_mapping.SEED_CLASSES` holds the 60 classes of the committed file, in order (the order fixes the set ids). `dense_store.SEED_TABLES` holds `ACCOUNT, ACCOUNTING_RULE, ACCRUAL_ENTRY, ACCRUAL_SCHEDULE`. The hand fix d0367624b is now a generator rule: when `dense_NotNull` reads a table other than the view's root, emit `Filter dense_<RootFirstWord>NotNull(<root>.<pk> is not null)` with the explanatory comment, and point the view's `~filter` at it. With the seeds, every per-class body and every table pick is reproduced from today's model. A probe stress file whose table sorts first (`AAA_PROBE`) leaves 59 and 60 unchanged; the `origin/main` generator picks the probe table (E5). |
| 4 | 64 (combos)? | **Already a fixpoint.** `combos.build_source()` takes no corpus input and reproduces 64 exactly. It only needed a writer: `dense_build.py` writes 59, 60 and 64 (E3). |
| 5 | Host dependence, and determinism? | **None observed.** Same bytes in each of these cases: (a) Bazel: hermetic 3.12.13, `PYTHONHASHSEED=0`, `PYTHONTZPATH=""`, `LC_ALL=C.UTF-8`. (b) Host 3.12.6 with `TZ=Pacific/Kiritimati`, `LC_ALL=tr_TR.UTF-8`, `PYTHONHASHSEED=12345`, and the system zone database. (c) The plain host run (E1). (d) The dense generator under two more TZ, locale and hash-seed combinations (E7). `--action_env` cannot reach the action, because `run_binary` sets its env explicitly and does not use the default shell env. That is the point: no host variable can reach it. The `zoneinfo` blocker is **latent**: no committed service calls `convertTimeZone` (nor `today`/`now`), so no current output reads zone data. It stays pinned for the day one does. |

## Evidence

All commands ran in the spike worktree. Output is trimmed.

### E0. Baseline (pristine `origin/main` copy)

```
$ time python3 scripts/corpus/build.py --check
  93-testdata.pure        6134 lines  up to date
  92-services.pure        1164 lines  up to date
  94-fanout-services.pure 291278 lines  up to date
  ... (95-98 up to date)
463.83s user 3.15s system 82% cpu 9:28.75 total
```

`--check` passes, as reported. Its CPU cost is about 7.7 min, not "over 200 s"; the 200 s figure was presumably taken on an idle, faster run.

### E1. Generated files removed from the model: output unchanged

The patch adds `model.GENERATED` (the 7 build.py outputs), `model.DENSE_GENERATED` (59, 60, 64) and `model.stress_sources()`. The latter excludes `GENERATED` and any per-generator `EXCLUDE`, and orders files by name. The six `STRESS.glob` / `glob.glob` sites in the closure now call it. `query.load()` reads `model.QUERIES`. `ROOT` comes from `CORPUS_ROOT` when that is set; otherwise from `__file__`, as before.

```
$ CORPUS_ROOT=$PWD python3 scripts/corpus/build.py --out runs/s5/out1
$ for f in runs/s5/out1/*; do cmp $f core/src/test/resources/stress/$(basename $f) && echo IDENTICAL ...; done
IDENTICAL 92-services.pure   ... IDENTICAL 98-combination-execution.pure   (7/7)
455.07s user 2.98s system 81% cpu 9:19.41 total
```

So nothing in 92–98 depends on a generated file. The generated files sort last (`92-`…`98-`), so excluding them also cannot change any earlier file's inherited `###` section.

### E2. The 92 split is a pure move

`queries.pure` is `92-services.pure` with two changes: the 6-line GENERATED header is replaced by a 4-line source header, and each service's `  testSuites:` block (from that line up to the service's closing `}`) is removed. The script is `runs/s5/split92.py`. Mechanical proof, in both directions:

```
queries body == 92 minus testSuites: True   (92: 1164 lines, queries.pure: 706 lines, 12 services each)
generator(queries.pure) == committed 92-services.pure: IDENTICAL   (E1, E3)
$ git diff origin/main --name-only -- core/src/test/resources/stress | wc -l
0
```

### E3. The Bazel actions

```
$ bazel build //scripts/corpus:gen_dense
INFO: 13 processes: 11 internal, 2 darwin-sandbox.     Elapsed 41 s (incl. fetching rules_python, CPython, tzdata)
  bazel-bin/scripts/corpus/dense/{59-dense-mapping,60-dense-store,64-combinations}.pure   -> IDENTICAL x3
$ bazel build //scripts/corpus:gen_stress
INFO: Elapsed time: 1602.029s, Critical Path: 1590.68s    (load 30-80; one action)
  bazel-bin/scripts/corpus/stress/92..98                  -> IDENTICAL x7
runfiles: rules_python++python+python_3_12_aarch64-apple-darwin  (Python 3.12.13)
          rules_python++pip+pypi_312_tzdata_py2_none_any_b683bd1b (tzdata 2026e)
```

`gen_stress` takes `:gen_dense`'s **outputs** as its 59, 60 and 64 (`build.py --dense DIR` sets `model.DENSE_DIR`). A dense-generator change therefore reaches 92–98 within one `bazel run //core:update_stress_corpus`.

### E4. Diff tests

```
$ bazel test //core:update_stress_corpus_{0..9}_test
Executed 10 out of 10 tests: 10 tests pass.
```

### E5. 59 and 60: seeds plus the rule

Before (today's generators over today's corpus): 59 differs in 50 of 60 classes; 60 differs in every pick (`ACCESS_ENTITLEMENT`, `ACCOUNT_REGISTER` instead of `ACCOUNT`, `ACCRUAL_ENTRY` and the others), and it drops the d0367624b filter. After:

```
59 with seeds identical: True  (1447 / 1447 lines)
60 identical: True
64 identical: True
# stability probe: a temporary copy of the corpus plus 55-aaa-probe.pure declaring Table AAA_PROBE(ID PK, CURRENCY, STATUS, AMOUNT)
seeded generators:            IDENTICAL 59, IDENTICAL 60, IDENTICAL 64
origin/main dense_store:      picks AAA_PROBE: True   (the churn the seed list removes)
```

The dense generators load the model without all of `DENSE_GENERATED`. 59 maps the very classes it would read, so including it would make the generator read its own output.

### E6. The rules_python bump does not disturb the rest of the graph

```
$ bazel build --nobuild //...
INFO: Analyzed 398 targets (703 packages loaded, 28640 targets configured).
INFO: Build completed successfully
```

### E7. Host variation

```
TZ=Pacific/Kiritimati LC_ALL=tr_TR.UTF-8 PYTHONHASHSEED=12345 (host 3.12.6, system tz):
  382.00s user 0.72s system 99% cpu 6:22.83 total   -> IDENTICAL 92..98 (7/7)
dense_build.py, TZ=Asia/Kathmandu LC_ALL=tr_TR.UTF-8 PYTHONHASHSEED=1      -> IDENTICAL 59, 60, 64
dense_build.py, TZ=America/St_Johns LC_ALL=de_DE.UTF-8 PYTHONHASHSEED=987654 -> IDENTICAL 59, 60, 64
```

A second identical Bazel run would only hit the disk cache, so no re-execution was forced. The coordinator capped the machine at one generator process during a load emergency. The host run with three variables changed at once, against the Bazel run's bytes, stands in for "two runs".

## Recommended design

**File split.**
- `scripts/corpus/queries.pure`: the 12 hand-written services without test suites. This is the source; 92 is the output.
- Stress files are of two kinds: *sources*, which everything else in the directory is, and *generated*, the ten files listed once in `core/BUILD.bazel`'s `_STRESS_GENERATED` and again in `model.GENERATED` / `model.DENSE_GENERATED`. Fold those lists into one data file, with `LINKED_PROJECTS` (2.1(e)).

**Generator changes** (all prototyped):
- `model.stress_sources()` is the only way to read the corpus. `CORPUS_ROOT` replaces the `__file__`-relative root under Bazel, because `__file__.resolve()` follows the runfiles symlink back into the real checkout.
- `build.py --out DIR [--dense DIR]`.
- `dense_build.py --out DIR` writes 59, 60 and 64.
- `dense_mapping.SEED_CLASSES` and `dense_store.SEED_TABLES` replace "rank the whole corpus". A seed that stops qualifying fails the build instead of dropping out silently.
- The view-root rule lives in `dense_store`.

**BUILD targets** (prototyped):
- `//scripts/corpus:corpus`: a `py_library` of the 28 modules, `imports = ["."]`, `deps = ["@pypi//tzdata"]`.
- `:build` and `:dense_build`: `py_binary`.
- `:gen_dense` and `:gen_stress`: `run_binary`. Their env is `CORPUS_ROOT=.`, `PYTHONHASHSEED=0`, `PYTHONTZPATH=""` (so zoneinfo uses the pinned tzdata, never `/usr/share/zoneinfo`) and `LC_ALL=C.UTF-8`. Their srcs are `queries.pure`, `//core:stress_sources` and `//projects:srcs`.
- `//core:stress_sources`: a filegroup globbing the stress directory minus the ten generated files.
- `//core:update_stress_corpus`: `write_source_files` with the 10 diff tests. Add it to the root `//:update_generated`'s `additional_update_targets`; this was not done in the spike.

**rules_python setup** (MODULE.bazel, prototyped):
- `bazel_dep(rules_python, 2.3.4)`.
- `config.explicit_init_py(default = True)`, which silences the deprecated implicit-`__init__` warning.
- `python.toolchain(python_version = "3.12", is_default = True)`.
- `pip.parse(hub_name = "pypi", python_version = "3.12", requirements_lock = "//tools/python:requirements_lock.txt")`.
- pyarrow, duckdb and the rest join the same lock file when their scripts are wired (Phase 1.3).

**Retire:** `build.py --check` and the in-place write path. The diff test replaces them, and they are now the only path that still depends on the checkout. Turn `density`, `executed` and `stacking` `--gate` into `py_test`s on the same library (2.1(f), not done).

## Remaining gaps for 59, 60 and 64

None for byte-identity. Open judgement calls:
- **The filter name in the view-root rule** is `dense_<first word of root>NotNull`, derived from the single data point the hand fix gives (`dense_AccrualNotNull`). It is a plausible rule, not a stated one. If a seed change ever makes the root a table such as `ACCOUNT_REGISTER`, the name would be `dense_AccountNotNull`. Fine, but confirm the spelling.
- **The seeds are the committed picks** (59: 60 classes from b88fbe90f; 60: 4 tables from 6718aae22). The lists are explicit; growing them is a deliberate edit that the diff test will show.
- **The `60` header text** still says "the base model's 210 tables" and "every join … is single-column". Those comments are static text and are now stale facts. Changing them changes bytes, so it is a separate, deliberate edit.

## Risks

1. **Cost of an edit.** Any change to any stress source, the projects or the generator re-runs a single-threaded action of 380–455 s CPU, which took 26 min wall on this machine under load. Every `bazel test //...` after such an edit waits on it before the diff tests finish. Gate 10 does not depend on the action: it reads the committed files. Mitigations:
   - disk/remote cache;
   - tag the diff tests so the edit loop can skip them;
   - profile `build.py`. It looks like every model generator runs about three times: once in `generate()`, once in `spread.build(..., executed.base_specs(c))`, and once in `executed.report(c, executed.all_specs(c))`, which rebuilds base_specs and then spread. This was read from the code, not profiled; it is likely a 2–3x win.
2. **Unsandboxed execution (Windows, or `--spawn_strategy=local`)** exposes the whole stress directory through the execroot's symlink forest. The generated ten are still excluded by name, so the outputs stay correct. But a new, undeclared stress file would be read while the BUILD glob (which matches all `*.pure`) declares it anyway, so in practice the two agree. For strictness, pass the declared file list as arguments instead of globbing the directory.
3. **rules_python 1.7.0 → 2.3.4 for transitive users** (protobuf and others). Analysis passed; nothing was built or tested beyond the corpus targets and the 10 diff tests.
4. **The interpreter moves 3.12.6 → 3.12.13**: same bytes today. A future minor-version bump (3.13) should be treated as a generator change and re-verified by the diff test, which is exactly what the diff test is for.
5. **Latent clock and zone reads in `oracle.py`** (`today`, `now`, `firstDayOfThis*` use `datetime.now()`; `convertTimeZone` uses zoneinfo). No committed service uses them today. If one is ever added, the clock functions make the output depend on the date and no pin fixes that. Recommend making them raise `Unsupported` in the oracle.
6. **`.bazelignore` does not list `runs/`.** Scratch copies under `runs/` became Bazel packages and broke `//...` analysis until I deleted them. This hits anyone who copies a tree with BUILD files into `runs/`.

## Effort estimate

The spike took about half a day of agent time; most of it was waiting on 6–26 min generator runs. Productionising:
- one data file for the generated lists and `LINKED_PROJECTS`, read by Python, Java and the shell script (0.5 day);
- a narrower projects input (only the linked projects), the root `update_generated` wiring, retiring `--check` and the in-place write, and updating `docs/RUNNING_THE_CORPUS.md` and the headers' "Regenerate with" lines (0.5 day);
- `py_test` gates for density, executed and stacking (0.5 day);
- optionally, deduplicating the triple generator evaluation in `build.py` (0.5–1 day, with the diff test as the safety net).

**Total about 2–2.5 days**, with no unknowns left on the critical path.

## Open questions

1. Should the stress diff tests run in the default `bazel test //...`? They pull in a 6+ min action after any corpus edit. The alternative is a tag that CI selects explicitly.
2. Confirm the view-root filter naming rule (`dense_<RootFirstWord>NotNull`), or name the filter explicitly beside `SEED_TABLES`.
3. Should the seed lists live in the modules (as prototyped), or in committed data files next to `queries.pure`?
4. Should `queries.pure` live in `scripts/corpus/` (prototyped) or next to the corpus under a non-`*.pure` name? It must stay out of the stress directory's `*.pure` glob, which the Java stress suites also read.
5. Python 3.12 or 3.13 for the repository's one interpreter? 3.12 was chosen to match the host the files were generated on.
