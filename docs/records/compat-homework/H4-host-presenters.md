# H4: how three hosts present without drifting (2026-11-19)

The compatibility homework spike, item H4 (brief: `docs/records/compat-homework-2026-11-19.md` on
`origin/claude/refactor-code-quality-review-9fqhug`, §2). The question: lite has three hosts that answer requests,
the Java server, the TypeScript tab and Python. Each needs presenters for lite's own answers and for upstream's
(legend-engine's pure/v1 and API routes, legend-pure's print forms). How do they present without drifting?

Measured at `main` `b75d6c73` ("The edge program, stage 1", #118). No product code was changed. The measurement
scripts and their raw output are in `H4-evidence/` beside this file (`.txt` sources, as the probes elsewhere in `docs/`).

## 0. The answer in brief

- **Today: 12 presentation or envelope rules are each held in two or three languages.** At least 7 copies already
  behave differently (§3).
  - The fixtures that should catch drift compare parsed JSON, refusal kinds and selected cases. They never compare a
    host's bytes, and only one of the four readers checks a status exactly. So most of the 7 pass them today (§4).
- **Every host already runs Java.**
  - The tab runs it through three TeaVM WebAssembly modules.
  - Python runs it through two GraalVM native-image libraries. No JVM is needed, and the library is already in the
    product wheel.
  - Most of legend-engine's presenting is already one Java implementation that both hosts call after exec (§2).
- **Recommendation: A.** One presenter per audience, in Java:
  - LITE's in the product library;
  - ENGINE's and PURE's in the edge's.
  - Each host asks it through the runtime it already carries (the TeaVM modules; the native image).

  It should be built in its two cheap forms wherever they apply:
  - **C1:** present inside the Java call the host already makes, such as the store answering lite's shape itself. This
    costs nothing extra.
  - **C2:** Java writes an Arrow answer's frame and metadata; the host's Arrow library carries the database's own
    batches untouched.

  Rows cross into Java only where a host must write a JSON body from rows.
- **What A costs (measured, §5):**
  - **No new module or library and no new startup:** the presenters ride in what each host already loads. LITE's
    writers (`result.HostRows`, `LiteV1Planning.Answered`, `ArrowWriter`) are already inside the plan side both
    libraries compile.
  - **Python:** rows crossing into Java cost 2.8-5.0 µs per row. Python's own lite writer costs 2.4-3.4 µs per row, so
    the price is about the same.
  - **The tab:** 4.6-6.6 µs per row, against 0.14-0.24 µs for TypeScript writing the same envelope. That is 25-35
    times slower, but only about 5 ms per 1,000 rows. The tab writes no lite bytes today: DataCube reads typed rows.
- **B is the cheaper road for one rule today and the dearer one for all of them.**
  - B means per-language presenters held to shared fixtures and generated tables.
  - It ports every ENGINE and PURE presenter twice.
  - It holds only the inputs its fixtures name. HOSTS-10 and HOSTS-11 are exactly inputs outside the fixture.
  - It goes against the user's standing ruling of 2026-10-15 for Python and DataCube: "one rulebook for sure ... a
    table of shared cases is a test of the asking, never the glue between two copies"
    (`docs/PYTHON_ASKS_DATACUBE_2026_10_15.md`).
- **B's machinery stays where it is the only possibility: exec's own readers.** These are the plan reader, the
  parameter checks, the raise-token reader and the host-rows writer. Exec runs where the database is (G4), so these are
  per-language by construction. Their outputs must be records, never words; they are held by generated constants and
  by three-reader fixtures (§7, step 6).
- **The guard:**
  - (1) A host meaning lens. Census-b2's TS and Python token reader refuses presenter vocabulary in the TS and Python
    product outside generated files. It is shrink-only to 0, with plants.
  - (2) A cross-host byte-parity suite over each host's real path, Arrow included.
  - (3) The existing JVM/WASM/native differential pattern for every presenter export.

## 1. What must be presented where

| Audience | Server (Java) | Tab (TypeScript) | Python |
|---|---|---|---|
| **LITE** | lite API v1: JSON, Arrow, CSV, errors | lite API v1's planning operations: Java (TeaVM `Boundary`). `query/run` builds an object, no bytes (`engine-client/src/legend/browser-engine.ts:119-136`). Errors are read from Java's refusal. The store's lite route is TS (`query-store/src/wasm-server.ts`) | lite API v1 over HTTP (`engine.py`): planning is Java (native). `query/run` JSON and Arrow, `user`, the store's route and request refusals are Python |
| **ENGINE** | the edge (Java) | the engine module (Java, TeaVM, opt-in): `executeJsonAnswerOrError` after exec. It has no error half (design §2.5) | the engine library (Java, native, opt-in): `lite_execute_json_answer` after exec; Arrow is Java's metadata with pyarrow's bytes plus Python's zstd frame and header; `lite_pure_v1_refusal` |
| **PURE** | the edge (PCT's harness) | none today (design §3.8) | none today |

DataCube's screen and export formatting (`datacube/src/format.ts`, `export-render.ts`, `date-tools.ts`) is a
different rulebook. It is DataCube's own, written once in TypeScript, and Python asks it through V8. It is outside H4,
but it is the same pattern as A in the other direction: one implementation in its owner's language, asked by the other
hosts.

## 2. How each host runs Java today

| Host | Runtime | Artifacts (measured today, Linux x86-64, the repository's default build) | Interface |
|---|---|---|---|
| Server | the JVM | `//core:server` | direct |
| Tab | TeaVM, WebAssembly-GC | `//wasm:planner` (product, `planner.TabExports` over `planner.Boundary` over `//core:plan_side`): **8,976,670 B raw, 3,382,160 B gzip -9**. `//wasm:engine_planner` (the edge, opt-in, embedding the product again): **9,336,548 B / 3,530,280 B**. `//query-store:page` (the saved-query store): **418,159 B**. Each module also has a 34,487 B runtime JS | strings in, strings out, folded `OK\n...` / `ERR\n<json>` (`wasm/src/main/java/planner/Folded.java`); binary as base64 |
| Python | GraalVM native image, loaded by ctypes (`python/legend_lite/_loader.py`) | `//native:compiler` `libcompiler.so`: **83,889,712 B** (29.7 MB gzip), in the product wheel. `//native:engine` `libcompiler_engine.so`: **90,443,312 B** (31.9 MB gzip), in the `legend-lite-engine` wheel, a second isolate. About 40 C entry points | UTF-8 C strings, the same `Folded` encoding, `lite_free` |
| Python, also | V8 (`mini-racer`), the `datacube` and `notebook` extras | DataCube's headless bundle, about 1.3 MB (`python/BUILD.bazel:885-892`) | Python asks DataCube's TypeScript rules; "No WebAssembly in the Python process" is ruled (`docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md` §1, §10) |

- **Startup** (measured, §5):
  - The tab's module instantiates in 177-277 ms under Node 22, and its first plan takes 389-544 ms.
  - The native isolate is created in 3-5 ms, and its first plan takes 149 ms.
- **The tab's module has doubled since the last record.** It was 4.2 MB raw and 1.46 MB gzip on 2026-09-20
  (`wasm/README.md`); today it is 9.0 MB and 3.4 MB.
- **Already one Java implementation, asked by both other hosts:**
  - legend-engine's pure/v1 grammar, compile and generatePlan (`lite_pure_v1`, `pureV1OrError`);
  - execute's JSON answer over the host's rows: `result.HostRows`, reached by `lite_execute_json_answer` and
    `executeJsonAnswerOrError`;
  - legend-engine's store words (`edge.StoreWords`, through `lite_saved_queries` and `lite_saved_cubes`);
  - lite API v1's planning operations and lite's error kinds (Python asks `lite_refusal_kinds` at run time).
- **Generated from Java, diff-tested:**
  - the type facts (`engine-client/src/generated/lite-facts.ts`, `python/legend_lite/_type_facts.py`);
  - the raise envelope's marks (`lite-raise.ts`).

  Both come from `engine-client/tools/typefacts/TypeFacts.java`, checked by `//engine-client:update_generated` and
  `//python:update_generated`.

## 3. Today's copies (the count)

Each row is one rule that turns a typed result, a refusal or an error record into bytes, words or a status, and that
is implemented in more than one language. "Verified" means read or run in this spike. Rows marked (a) were found by
the audit (`docs/records/pr118-audit-2026-11-18/findings/HOSTS.md`) and re-read here.

| # | Rule | Java (the source) | TypeScript | Python | Behaves differently today |
|---|---|---|---|---|---|
| P1 | The store's refusal to lite's kind and status (HOSTS-9) (a) | `LegendHttpServer.java:244-314` (an adapter after the store) | `query-store/src/wasm-server.ts:66-89`; `datacube/src/cube-store.ts:197-219` | `engine.py:416-446` | **Yes.** Python's bytes (`json.dumps` default separators and `ensure_ascii`). DataCube keeps the store's status on `notFound` where the other three force 404 (`cube-store.ts:222`): the same bytes today only because `SavedCubes` refuses with 404 |
| P2 | lite's error kinds and their statuses | `LiteV1Api.java:289-314` (`Kind`, status per kind) | `engine-client/src/lite-error.ts:9` (the kind list by hand); the test's `STATUS` table (`engine-client/test/lite-api-server.test.ts:65-67`) | kinds asked at run time; statuses by hand: `compiler.py:164` (422/500/400, **no 404 arm**) and `engine.py:437-446` | Latent: Python's general rule would answer a `notFound` with 400 |
| P3 | The folded encoding (`OK\n`, `ERR\n<json>`, `OK\n<status>\n<type>\n<body>`) | `Folded.java:23-54`, the one writer | hand writers in `datacube/src/services/lite-routes.ts:18-26`, `lite-server.ts:38-49` and `python-host.ts:250-266`; 3 readers (`wasm-grammar.ts:63-75`, `datacube/src/lite-planner.ts:948-969`, which keeps the message only, and `wasm-edge.ts:58-67`) | a hand writer (`engine.py:491-504`); 2 readers plus 8 ad hoc `partition('\n')` readers | **Yes, verified.** 3 Python readers still read the layout retired on 2026-11-17 (`ERR\n<kind>\n<message>`): `_home.py:153-155` and `:174-176`, and `edge/_pure_v1.py:115-118`. A failure folded as JSON then surfaces as raw JSON text in an `OSError` (`_home`) or in a text/plain 500 (`_stored`). Neither host decodes the `;base64` suffix; its one producer has no product caller |
| P4 | Request-refusal words of the host-only lite routes (`query/run`'s closed request, `user`, an unknown path or method) | `LiteV1Api.java:455-471` (`read`: one key named, "(it holds ...)"); `LegendHttpServer.java:404-442` | `wasm-server.ts:119-136` | `engine.py:685-726`; `compiler.py:123-149` | **Yes, verified.** Python names every unknown key and drops "(it holds ...)"; the format and non-JSON words differ. An unknown lite path is 400 lite JSON on the server, 404 text/plain in Python and 404 `request` in the page |
| P5 | lite's `query/run` answer: JSON cells and Arrow metadata | `LiteV1Planning.java:320-371` (`Answered`, `CellText.pureJson`); `ArrowWriter.java:105-116` (`facts`) and `LiteV1Planning.ShapedArrow` | builds an object (`browser-engine.ts:119-136`), no bytes | `engine.py:232-284` (`_carried_float`, `_lite_cell`, `_lite_rows_json`); `engine.py:745` with `_reads.py:28, 73` | **Yes, verified.** Python's Arrow fields carry `legend.type` only. The server's also carry `legend.multiplicity`, and `legend.slot` and `legend.precision` where the plan has them |
| P6 | The host-rows wire (rows handed to Java's answer half) | `result/HostRows.java` (the reader) | `edge-engine.ts:85-106` (`hostRows`) | `_plans.py:182-223` (`host_rows`) | The text differs (`1` vs `1.0`, `1e-7` vs `1e-07`, zoned date-times). **Absorbed:** Java re-reads each cell by the plan's column, and both spellings gave identical answer bytes (`H4-evidence/rows_spelling_probe.py.txt`) |
| P7 | The raise readers (HOSTS-11) (a) | `exec/RaisedErrors.java:72`, `sql/RaiseKind.java:70-81` | `raised.ts` (68 lines) + `declared-raises.ts` (52) | `_raised.py` (128) | **Yes.** 2 inputs outside the fixture: the last vs the first separator, and leading zeros |
| P8 | The plan readers and `PLAN_VERSION` | `PlanJson.java` (1,625 lines), `exec/PlanRunner.java` (1,232) | `plan-runner.ts` (1,037; the version at `:301`, checked at `:359, :739`) | `_runs.py`, `_plans.py`, `_planned.py`, `_runframe.py` (1,124; the version at `_runs.py:27`, checked at `:40, :129`) | The version is guarded by `PlanVersionsAgreeTest` (a regex over the two constants); the reading is not |
| P9 | The parameter checks (HOSTS-10) (a) | `exec/PlanParameters.java` (214) | `plan-runner.ts:41-195`, `plan-params.ts` (68) | `_params.py` (235) | **Yes.** 3 divergences (non-ASCII digits, unsafe whole numbers, an `int` taken as a Float) |
| P10 | The Arrow metadata key names (`legend.shape`, `.type`, `.builder`, `.activities`, `.columns`, `.plan`, `.receipt`) | `ArrowWriter.java:108-114`, `LiteV1Planning.java:294`, `HostAnswers.java:77-80, 201-205`, `PureV1Writer.java:126-128` | readers: `arrow-ipc.ts:40, 43`, `engine-remote.ts:106, 125`, `python-host.ts:345, 349` | writers: `engine.py:154, 222-224, 745`, `_reads.py:28`; reader: `edge/_pure_v1.py:134` | Every key is a literal typed in each language; there is no shared or generated source. P5's difference is one consequence |
| P11 | legend-engine's answer pieces outside Java | `HostAnswers.java:60-96` (the whole Arrow answer, exported as `lite_execute_arrow_answer` and never called by Python); `PureV1Routes.java:89, 355`; `EdgeServer.java:69-80` | the request envelope `clientVersion: 'vX_X_X'` twice (`planning-client.ts:15-17`, `pure-v1-planning.ts:159-167`); three readers of `{message, errorType}` (one cuts to 400 characters) | `edge/_pure_v1.py:127-142` (the ARROW_IPC query parse, the columns check, the zstd frame), `:228` (`FormatNotSet`), `:171-173` (`currentUser`) | **Yes, verified.** `currentUser`: GET only and the local user in Python; any method and `"anonymous"` in Java. Possibly deliberate; no ruling found |
| P12 | Exact-number JSON (the byte layer under every presenter) | `json/.../Json.java` (refuses NaN) | `pure-protocol` `json.ts` + `exact.ts`; `values.ts:120-139` (`parseExact`); `page-hash.ts:15-29` (`canonicalJson`) | `_json.py` (90; accepts NaN; `canonical` sorts by code point, TS by UTF-16 unit) | Not held across languages by any fixture (the subagent's read; not re-run) |

- **Totals.** 12 rules, at least 25 implementations outside the one Java source, 7 rules that behave differently
  today (P1, P3, P4, P5, P7, P9, P11) and 2 latent differences (P1's DataCube status, P2's 404 arm).
- **Beside the table: wording a run's own failures.**
  - The design (§3.4) gives these to the presenter family: "the run's own failures".
  - Today the hosts word them in their own language:
    - 59 `new PlanRefusal(` sites in `plan-runner.ts`;
    - 48 refusals raised in Python's runner modules;
    - 118 `_said(<status>, <words>)` sites in `engine.py`. These are text/plain, on DataCube's private Python host
      routes, which the native protocol design (`docs/NATIVE_PROTOCOL_DESIGN_2026_11_11.md`) replaces.
  - Under rule 2 of the architecture (an error is its kind, its location and its operands, never words), each is a
    record waiting for a presenter.
- **The one store, four adapters.** P1 is the clearest case of the drift mechanism.
  - The store itself is one Java implementation, compiled for all three hosts: `//core:saved_queries` runs on the
    JVM, inside `libcompiler.so` and inside `//query-store:page`.
  - The copies are written after the Java call returns, so having one implementation was not enough. The
    presentation must happen inside the call (C1).

Commands:
- `grep -rn "query_not_found" ...` for P1.
- `wc -l` for the line counts.
- `python3 -I H4-evidence/rows_spelling_probe.py.txt <engine lib> <product lib> <model>` for P6.
- Reading `ArrowWriter.facts` against `_reads.py:73` for P5.
- The rest by reading the cited lines. A read-only subagent swept P3, P4, P6, P10, P11 and P12; every claim this
  record relies on was re-read here except P12's.

## 4. How drift is caught today

| Net | Readers | What it compares | What it misses |
|---|---|---|---|
| `engine-client/test/fixtures/lite-api-cases.json` (26 cases; 7 name the server, 6 name Python) | `core` `LiteApiConformanceTest` (the "browser" binding: the JVM `Boundary`, **not** the tab's TypeScript); `engine-client/test/lite-api-server.test.ts` (the server); `python/tests/test_compiler.py:168`, `test_engine.py:550` | **parsed** JSON. A refusal's **kind** only; its status is exact on the server (a hand-kept `STATUS` table) and `>= 400` in Python (`test_engine.py:567`) | bytes (so P1's Python bytes pass), words (by design: "the words are the product's"), Arrow, the tab's own TS paths, Python's data refusal (excluded: Q9) |
| `raise-envelopes.json` (11 cases) | Java `RaiseEnvelopeConformanceTest`, TS `plan-runner.test.ts:1040`, Python `test_planned.py:1132` | the words read from an envelope | P7's two inputs |
| `keyed-expansion.json` (4) | all three | expansion | — |
| `run-load-conformance.json` | TS, Python (no Java reader) | a load's checks | — |
| `query-store/test/conformance.ts` | the page module, the server, Python's engine | the store's answers | the lite adapters' bytes |
| DataCube's `parity.json`, `chart-rules.json`, `builder-python.json` | TS and Python | DataCube's rulebook, asked | — |
| Generated tables (`lite-facts.ts`, `lite-raise.ts`, `_type_facts.py`) | diff tests | the committed copy equals the generator's | — (this net is exact) |
| `PlanVersionsAgreeTest` | regex over two constants | the version number | the reading |
| Differentials: `//wasm:differential_test` (69 queries, JVM vs WASM), `//python:bindings_test` (native vs JVM), `wasm/engine-routes.json` (SHA-256 pins of the engine module's answers) | one source, two builds | bytes, for the planner's answers and the tab's ENGINE answers | everything a host writes itself |

**The pattern:** where the rule is ONE source compiled for each host (the differentials) or generated (the diff
tests), the net is exact and nothing drifts. Where the rule is a copy held by cases, the copies drift on the inputs
the cases do not name. That covers P1's bytes, P5's Arrow, P7, P9 and P3's stale readers, none of which any net sees.

## 5. Measurements

Machine: the cloud session's container (4 cores, Linux x86-64), Node 22.23.5, CPython 3.13.18. The native libraries
were built as the repository builds them by default: fastbuild, which `rules_graalvm` turns into `-Ob`. No `-c opt`
appears in `.bazelrc` or the workflows, but the wheel's release job was not checked. The first tab run overlapped the
native-image build; the numbers below are the rerun on the idle machine (`H4-evidence/tab_result.json`). Quote ratios,
not milliseconds.

**The proxy.** Each host's existing "Java presents rows the host read" path, legend-engine's TDS JSON answer half,
over 3 columns (String, String, Float), median of 5-31 runs after a warm-up. It is the same work a LITE answer export
over `HostRows` + `LiteV1Planning.Answered` would do.

| Rows | Tab, A: Java in WASM (incl. the host's JSON encode) | Tab, B: TS writes the envelope | Python, A: Java in the native image (incl. the host's encode) | Python, B as it is: `_lite_cell` + `_json.dumps` (pure Python) | Python, B at best: stdlib `json.dumps` (C) |
|---|---|---|---|---|---|
| 100 | 0.66 ms (6.6 µs/row) | 0.02 ms | 0.46 ms (4.6 µs/row) | 0.24 ms (2.4 µs/row) | 0.05 ms |
| 1,000 | 4.6 ms (4.6) | 0.14 ms | 2.8 ms (2.8) | 3.4 ms (3.4) | 0.5 ms |
| 10,000 | 53 ms (5.3) | 1.7 ms | 32 ms (3.2) | 26 ms (2.6) | 5.6 ms |
| 100,000 | 592 ms (5.9) | 24 ms | 500 ms (5.0) | 310 ms (3.1) | 72 ms |

Commands:
- `node --experimental-wasm-exnref H4-evidence/tab_answer_bench.mjs.txt <engine_planner dir> <planner dir>`;
- `python3 -I H4-evidence/native_answer_bench.py.txt <libcompiler_engine.so> <libcompiler.so> <model.pure>`;
- `python3 -I H4-evidence/py_lite_writer_bench.py.txt <repo>`.

The model is `MODEL` of `wasm/engine_routes.mjs`, saved as `model.pure` beside the scripts. The modules and libraries
come from `bazel build //wasm:planner //wasm:engine_planner //query-store:page //native:compiler //native:engine` (in a
cloud session, after `tools/cloud-distdir.sh` on the same targets).

**What the numbers say:**
- **Python: A costs what Python's B costs today** (0.8-1.9x, the same order), because Python's own writer is pure
  Python. Only a C-accelerated B would be 5-7x faster.
- **The tab: A is 25-35x a TypeScript writer, at about 5 µs/row absolute.** A grid's viewport of 1,000 rows or fewer
  costs 5 ms or less, and a 100,000-row export about 0.6 s. Today the tab pays this only for ENGINE answers, and has
  done so since the edge program. Its LITE answers are objects, not bytes.
- **Startup and size: A adds no module.**
  - LITE's writers are already in `//core:plan_side`, which both libraries compile: `:result` holds `HostRows` and
    `ArrowWriter`, and `:lite_v1` holds `LiteV1Planning.Answered` and `ShapedArrow` (`core/BUILD.bazel:91, 367-378,
    413-417`). A new export only makes them reachable for TeaVM's and native-image's closed-world compiles.
  - ENGINE's and PURE's writers are already in the edge's module and library. The edge on top of the product is
    +0.36 MB raw (+0.15 MB gzip) in the tab and +6.55 MB in Python. These deltas compare different entry closures,
    so they are an indication, not the presenters' own size.
- **The cost B would avoid is per-row work in the tab, about 5 µs a row on the tab's ENGINE path.** In exchange it
  would port about 9,300 lines.
  - That figure counts the 38 edge files whose names say they present (words, texts, writers, envelopes, answers,
    prints: `ls core/src/main/java/com/legend/edge | grep -E "Writer|Words|Text|Present|Answer|Envelope|Message|Print"`).
  - Some of those files are SQL text builders that rule 4 deletes, so the presenters' true share is to be measured
    in B1. Even a third of it is a large port, twice over.

## 6. The options

### A. One presenter per audience, in Java, asked by every host

- **How each host binds:**
  - **The tab:** LITE presenters are exported by `//wasm:planner`; ENGINE and PURE by `//wasm:engine_planner` (opt-in,
    K2 unchanged).
  - **Python:** LITE by `libcompiler.so`; ENGINE and PURE by `libcompiler_engine.so`.
- **Python's possible bindings, compared:**

  | Binding | Exists? | Verdict |
  |---|---|---|
  | GraalVM native image (ctypes) | yes, in the product wheel; the ENGINE answer half already uses it | **the binding.** 3-5 ms isolate start; strings today, a (pointer, length) byte entry where Arrow must cross |
  | A JVM subprocess (the server jar over stdio or HTTP) | the jar exists | no: needs a JRE, and the wheel "needs only Python's standard library" (ruled 2026-10-15) |
  | An in-process JVM (JPype, JNI) | no | no: needs a JVM, the same ruling |
  | TeaVM's WebAssembly in V8 (`mini-racer`) | V8 only in the `datacube` extra | no: "No WebAssembly in the Python process" (ruled 2026-10-11); and absent from the base wheel |
  | GraalPy, Jython | no | no: not CPython |

- **Cost to build (LITE):**
  - About 20 Java lines per export: `HostRows.stream(rows, columns, new Answered())`, or `ShapedArrow`, in
    `TabExports` and `nativelib.Compiler`, through one new `Boundary` member.
  - The two classes moved from package-private in `server` to an accessible spot in `:lite_v1`.
  - The Python and TS call sites, minus the deleted copies.
  - Each export gets the existing differential pattern.
- **ENGINE:** largely done. The error half (`executeErrorAnswerOrError`, `lite_execute_error_answer`) is already
  planned in the design's §3.8. Python's `_arrow_ipc` framing and header move into Java (C2).
- **Run-time cost:** §5.
  - No new startup or module.
  - Rows cross only where a host writes JSON from rows: Python's lite `query/run` JSON, and the ENGINE JSON answers
    in the tab and Python.
- **Drift it prevents:** every presenter rule (words, kinds, statuses, envelopes, metadata keys, cell text) exists
  once, and a host cannot drift on a rule it does not hold. P1, P2, P4, P5, P10 and P11 go by construction.
- **What it does not prevent:**
  - exec's own readers (P3's readers, P6-P9), which stay per language;
  - P12, the JSON layer each host still needs.
- **The guard:** §8.

### B. Per-language presenters, held to shared fixtures and generated tables

- **Cost to build:**
  - A generator per table: kinds, statuses, words, metadata keys, envelope keys. The `TypeFacts` pattern costs one
    Java program and one diff test per output.
  - Byte-level fixture cases for every presenter in every format, with three readers per suite.
  - The ENGINE and PURE presenters ported to TypeScript and Python (the 9,300-line estimate of §5, twice).
  - Then every later presenter change, three times, plus its cases.
- **Run-time cost:** the fastest in the tab, the same in Python as written today (§5).
- **Drift it prevents:**
  - **Tables:** exactly. A generated table cannot drift.
  - **Rules:** only on the inputs the fixtures name. HOSTS-10's three divergences, HOSTS-11's two and P5's Arrow
    metadata all sit outside today's cases. Every one of the 7 live divergences in §3 is a copy held by cases, or by
    nothing.
- **The guard:**
  - the diff tests;
  - fixture readers in each host;
  - a census that no presenter literal sits outside the generated files.
- **Against B:** it contradicts the user's 2026-10-15 rule, which the repository already follows for DataCube's
  rulebook. B's honest use is narrower: data tables (generated) and exec-side readers (unavoidable).

### C. What else the code base already does or suggests

- **C1: present inside the Java call the host already makes.**
  - **Examples:**
    - `SavedQueries` and `SavedCubes` answer lite's `{kind, message}` and status themselves (HOSTS-9's own
      "Instead", a `LITES_SHAPE` beside `LITES_WORDS`);
    - `Folded.failure` already does it for every planning refusal.
  - **Costs:** zero crossings and zero new exports, so it is A at no cost.
  - **Applies to:** records made in Java (store answers, planning refusals, model checks), and request validation if
    the host-only routes' closed requests are read by `LiteV1Api.read` through an export.
- **C2: Java writes the frame; the host's Arrow library carries the database's own batches.**
  - **How it works:** the schema and field metadata, and legend-engine's outer zstd frame where ENGINE needs it, come
    from Java (`ArrowWriter.facts`, `HostAnswers`' metadata); the host attaches them and never converts a value (G6;
    the native protocol design's "a host may only relabel ... or refuse by name").
  - **Today:** this is half of what Python does for ENGINE's Arrow (`lite_execute_plan` gives the metadata). Python
    still keeps the frame, the header and the columns check itself (P11).
  - **Costs:** zero per-row crossing.
  - **Applies to:** every Arrow answer, lite's and ENGINE's.
- **C3: one presenter in TypeScript,** asked by Python through V8 as DataCube's rules are.
  - **Rejected:** the server would need a JavaScript engine (GraalJS or a Node sidecar), and V8 is only in Python's
    `datacube` extra.
- **C4: generate presenter code in three languages** from a DSL or a transpiler (J2CL, JSweet).
  - **Rejected:** it is a second compiler to own, and no transpiler targets Python.
  - The data half of this idea is B's generated tables, which A keeps for exec.
- **C5: the server presents for every host.**
  - **Rejected:** the tab and Python run with no server.

## 7. Recommendation, and the first steps

**A, built as C1 and C2 wherever they apply. B's machinery stays only for exec's readers.** First steps, in order;
each is a small landing of its own.

1. **The pilot: HOSTS-9, R11(a).**
   - **Change:** `SavedQueries` and `SavedCubes` answer lite's shape at lite's mount (C1).
   - **Delete** the four adapters: `LegendHttpServer.java:244-314`, `engine.py:416-446`, `wasm-server.ts:66-89` and
     `cube-store.ts:197-219`. That is about 150 lines out and about 40 in.
   - **Its guard** is the pilot of §8 (2): the store cases of `lite-api-cases.json` asked of all three hosts' real
     routes, the status, content type and body compared byte for byte. Today's Python bytes then turn it red first,
     which shows the net works.
2. **Kinds, statuses and `PLAN_VERSION` from Java.**
   - **Generate them:** `TypeFacts` writes `lite-errors.ts` and `_lite_errors.py`, or `lite_refusal_kinds` answers
     the statuses too. `PLAN_VERSION` is generated in the same pass, as the design's §3.8 already plans.
   - **Delete** `LITE_ERROR_KINDS`'s hand list, `compiler.py:164`, `engine.py:437-446`'s numbers, the test's `STATUS`
     table and `PlanVersionsAgreeTest`'s regex.
3. **Python's lite `query/run` answer through Java.**
   - **JSON:** a native entry over `HostRows` and the server's `Answered`.
   - **Arrow:** C2 (Java's field and schema metadata, attached by pyarrow).
   - **Delete** `_carried_float`, `_lite_cell` and `_lite_rows_json` (about 60 lines). This also fixes P5's Arrow
     metadata.
   - **Measured cost:** about Python's own (§5).
4. **ENGINE's remaining host pieces:**
   - the error half (design §3.8);
   - Python's zstd frame, `FormatNotSet` and ARROW_IPC parse into Java (C2);
   - the `currentUser` difference ruled (P11);
   - the `clientVersion` request envelope read from one place in TypeScript.
5. **The guards (§8).**
6. **Exec's own readers, which stay per language (B, honestly used):**
   - one folded-answer reader per language, with P3's three stale Python readers fixed;
   - HOSTS-11's two inputs in `raise-envelopes.json`;
   - a parameter case file with refusals, read by all three (HOSTS-10);
   - a host-rows fixture read by Java and written by TS and Python (P6);
   - the hosts' run failures as records (a kind and operands), worded by the family. This is the design's §3.4, so
     the 59 + 48 sites shrink as B1's slices reach them.

## 8. The guard for A (what proves no host presents)

1. **A host meaning lens: no presenter vocabulary in the TS or Python product.**
   - **What it reads:** census-b2's `//tools/meaning:host_sources` (`HostSources`: LITERAL, NAME, PROPERTY and
     COMMENT tokens per file; on `origin/claude/census-b2`, not yet on `main`).
   - **What it refuses**, outside generated files and the one call site per export:
     - a store refusal kind;
     - a lite error kind's wire word;
     - a status number in a refusal;
     - a `legend.*` metadata key;
     - an envelope key (`builder`, `tdsBuilder`, `activities`, `FormatNotSet`).
   - **Its count:** pinned shrink-only from today's count (§3's sites) to 0. It ships with a planted TypeScript and a
     planted Python example that it must catch.
2. **A cross-host byte-parity suite: one fixture, each host's real path.** The three paths are:
   - the tab's TypeScript on Node with the modules and DuckDB;
   - Python's `Engine`;
   - the server.

   Each answer is compared as bytes: status, content type and body, Arrow included. The current readers' parsed-JSON,
   kind-only and `>= 400` checks become byte checks. The "browser" binding stops meaning the JVM `Boundary`.
3. **One differential per presenter export:** JVM vs WASM vs native, as `//wasm:differential_test` and
   `//python:bindings_test` already do for planning. One source compiled three ways is then shown to give one answer.
4. **Lanes.** Each of these runs in a per-push lane: HOSTS-1 found the hosts' proofs in no lane.

## 9. Risks and open points

- **The tab's per-row cost** (about 5 µs a row) matters only if the tab starts writing large lite bodies, such as a
  JSON export of 100,000 rows. C2 (Arrow, no crossing) is the answer there, not a TypeScript copy.
- **Two isolates or two modules with the edge on** (HOSTS-14) is unchanged by A. Merging them is K2's question, not
  H4's.
- **Arrow across strings.** Today a binary answer crosses as base64 (`Folded.BASE64`). Python's ctypes can take a
  (pointer, length) byte buffer instead; the tab can take a `Uint8Array` through TeaVM's JS interop. Neither was
  measured here, and C2 avoids the question for every Arrow answer.
- **Native timings were taken at `-Ob`** (the default fastbuild). Whether the release wheel is built `-c opt` was not
  checked; if it is, A's Python numbers improve.
- **The module has doubled since September** (§2). This is not caused by A, but it is worth a look in B1.
- **For the user:** whether Python's `currentUser` (the local user, GET only) is a deliberate host difference or P11's
  drift.
