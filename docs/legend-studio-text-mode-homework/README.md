# Studio's engine calls on legend-lite's engine module: the probes (2026-11-23)

These are the evidence for section 4 of [`../LEGEND_STUDIO_TEXT_MODE_PLAN_2026_11_23.md`](../LEGEND_STUDIO_TEXT_MODE_PLAN_2026_11_23.md).

| File | What it is |
|---|---|
| `shim.mjs` | A local HTTP server that answers legend-engine's `pure/v1` routes with legend-lite's engine module, in-process, so legend-studio's own tests can call it as they call an engine. The two protocol GET routes the module lacks (`getClassifierPathMap`, `getSubtypeInfo`; neema2/legend-lite#100) are passed through to a real engine, and counted. Answers CORS as an engine does |
| `probe.mjs` | Studio text mode's three routes, the module against a real engine: parse with positions on every Studio case, error answers, a compile with a dependency as Studio sends it, timings |
| `probe-output.txt` | `probe.mjs`'s output on 2026-11-23 |
| `sourceid.mjs` | Form mode's compile: a derived property's lambda carries Studio's `sourceId` (`model::A@derivedProperty@upper@u1`) and an error. Does the compile error return that `sourceId`, as form mode needs to place it? |
| `mixed.mjs` | Each one's JSON in the other: legend-lite's parse JSON compiled by the engine (local parse with compile on the server), and the engine's JSON composed by legend-lite, `PRETTY` and `STANDARD` (stored entities composed locally) |
| `mixed-output.txt` | `mixed.mjs`'s output on 2026-11-23 |

## Setup

- **legend-lite:** `0be02799d` (the fork's branch `claude/sync-neema-main-2026-11-23`: neema2/legend-lite#113's branch
  merged with `main` at `2942e9838`).
- **The module:** `bazel build //wasm:engine_planner_dir`, which gives `classes.wasm` (9.3 MB) and
  `wasm-gc-module-runtime.js`. It needs WebAssembly GC: Node 25 was used, and Node 20 refuses the module
  (`Unknown type code 0x5e, enable with --experimental-wasm-gc`).
- **legend-engine:** a local build reporting 4.152.1-SNAPSHOT, on port 6500.
- **legend-studio:** `d564dd86d` (finos `master`). The cases are
  `packages/legend-manual-tests/src/__tests__/roundtrip-grammar/cases` (135 files).

## How to run

The probe:

```bash
node probe.mjs <module dir> <legend-studio>/packages/legend-manual-tests/src/__tests__/roundtrip-grammar/cases http://localhost:6500
```

Studio's own suite against the module:

1. Start the shim:

   ```bash
   node shim.mjs <module dir> 6399 http://localhost:6500
   ```

2. In legend-studio, compile the extension packages the suite imports. The suite imports their built `lib`:

   ```bash
   yarn tsc -b packages/legend-extension-dsl-text/tsconfig.build.json packages/legend-extension-dsl-persistence/tsconfig.build.json packages/legend-extension-dsl-data-quality/tsconfig.build.json packages/legend-extension-store-graphql/tsconfig.build.json packages/legend-extension-store-service-store/tsconfig.build.json
   ```

3. Point the suite at the shim. The suite's engine URL is fixed: `ENGINE_TEST_SUPPORT_API_URL =
   'http://localhost:6300/api'`, read from `legend-graph`'s built `lib`
   (`lib/graph-manager/__test-utils__/EngineTestSupport.js`). For these runs that one line was changed temporarily to
   read `process.env.PROBE_ENGINE_URL`, and restored afterwards. Running the shim on port 6300, when nothing else uses
   it, needs no change.

4. Run the suite:

   ```bash
   TEST_GROUP=engine-roundtrip PROBE_ENGINE_URL=http://localhost:6399/api yarn jest packages/legend-manual-tests/src/__tests__/roundtrip-grammar/
   ```

   Run it again with `PROBE_ENGINE_URL=http://localhost:6500/api` for the engine.

## Results

**Studio's suite** (135 cases, each with and without Studio's extensions):

| Server | Passed | Failed |
|---|---|---|
| legend-engine 4.152.1-SNAPSHOT (local) | 266 of 270 | `STO_GraphQL-empty-store-and-plain-connection` and `STO_GraphQL-store-connection-mapping`, both modes: `'GraphQL' is not a known section parser` |
| legend-lite, through the shim | 265 of 270 | the same four, and `DSL_DataQuality-relation-comparison-advanced` with extensions (`PROTOCOL_ROUNDTRIP`). Studio's serializer writes `additionalColumnsToPersist: []` on its three `DataQualityRelationComparison`s, and legend-lite's JSON has no such field. The engine writes it too |

**The routes the shim served during legend-lite's run:**

| Route | Status | Calls |
|---|---|---|
| `grammar/grammarToJson/model` | 200 | 271 |
| `grammar/grammarToJson/model` | 400 | 12 |
| `grammar/jsonToGrammar/model` | 200 | 265 |
| `compilation/compile` | 200 | 265 |
| `protocol/pure/getClassifierPathMap` | passed to the engine | 280 |
| `protocol/pure/getSubtypeInfo` | passed to the engine | 280 |

**The probe:** see `probe-output.txt`.

- **A.** 124 of 133 cases give identical JSON with positions. One differs only in positions, and eight differ in
  content. Each element's own position matches in 929 of 929 elements.
- **B.** legend-lite's parse errors have no `sourceInformation`. Its compile errors give the whole element's span. S21
  is a plain 400.
- **C.** A compile with a dependency's elements appended to the project fails on legend-lite (`the model's section
  index lists no section for it`). The engine compiles it.
- **D.** Timings.

**`sourceid.mjs`:**

| | Error position | `sourceId` |
|---|---|---|
| Engine | the expression (line 4, columns 18 to 20) | `model::A@derivedProperty@upper@u1` |
| legend-lite | the whole class (lines 1 to 5) | empty |

**`mixed.mjs`:**

- **The engine compiling legend-lite's parse JSON:** 132 of 133 compile. On `CORE-function-test-variants`, the engine
  fails with a 500 (`ClassCastException`) on legend-lite's `minus(3)`.
- **legend-lite composing the engine's JSON:** 126 of 133 give the engine's text, `PRETTY` and `STANDARD` alike.
  - The three function-test cases fail with a 500: legend-lite can't read the engine's test-data shape.
  - The four connection cases write the engine's quoted `timeZone` as `'\'UTC\''`.

**What these runs do not show:**
- a browser;
- real projects;
- the deployment's engine version;
- whether section A's differences change Studio's hashes.
