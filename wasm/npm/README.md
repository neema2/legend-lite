# @legend-lite/planner-wasm

legend-lite's **engine module**, compiled to WebAssembly, for a host outside the legend-lite repository: legend-engine's
`pure/v1` routes answered in the page, by the compiler legend-lite is built on.

The name says planner because the module compiles legend-lite's planner. It is the engine module, not the product
module (`//wasm:planner`), which answers legend-lite's own operations and exports no `pure/v1` route.

| File | What it is |
|---|---|
| `classes.wasm` | The engine module (`//wasm:engine_planner`) |
| `wasm-gc-module-runtime.js` | The runtime that loads it. An ES module exporting `load` |
| `index.d.ts` | Types for `load` and for the one export a host calls |

## Use

```js
import { load } from '@legend-lite/planner-wasm';

const planner = await load(urlOfClassesWasm, {
  stackDeobfuscator: { enabled: false },
  installImports(imports) {
    imports.teavmConsole = { putcharStdout() {}, putcharStderr() {} };
  },
});
const answer = planner.exports.pureV1OrError('/api/pure/v1/execution/generatePlan', '', JSON.stringify(input));
```

`pureV1OrError` answers one route of legend-engine's `pure/v1` API. Its answer is one string:
`OK\n<status>\n<content type>\n<body>`, or `ERR\n<kind>\n<message>`.

## What a host must know

- **Load it in a worker.** Building the planner's boot layer is long synchronous work and would freeze a page.
- **Serve both files as files.** Do not pass `wasm-gc-module-runtime.js` through a bundler: it decides where it is
  running by `typeof process`, and it imports by a computed name.
- **The browser needs WebAssembly GC.** Under Node, version 25 runs it with no flag; 22 needs
  `--experimental-wasm-exnref`.

## Building it

```bash
bazel build //wasm:engine_planner_package
```

The package is the directory `bazel-bin/wasm/engine_planner_package`. It is not published.
