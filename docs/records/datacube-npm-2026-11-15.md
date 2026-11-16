# Browser-only DataCube as an npm dependency

The user authorized implementation on `make_packageable_codex`, 2026-11-15, with commits and pushes at useful
checkpoints. The consumer is another web application that imports DataCube and analyzes data entirely in its user's
browser. No Legend backend is required. Publication to npm and landing on main remain separate maintainer actions.

## Contract

- One public, framework-independent entry: `createBrowserDataCube({ host, assetsUrl, storageNamespace?, onStatus?,
  signal? })`. It mounts the existing full application, initially empty, and returns `openFile(File)` and async
  `dispose()`. Its public TypeScript declarations refer only to the public interfaces and browser platform types.
- The existing `LitePlanner`, compiled from the one Java compiler to WebAssembly, plans. The existing tab DuckDB
  door starts the database and its existing runner executes plans. This introduces no evaluator or execution path.
- Each mounted instance owns its database and planner workers. Several grids within that instance share them.
  Closing an instance releases workers, connections, listeners and browser-storage connections. Failed startup and
  cancellation also release acquired resources.
- Embedded use preserves the host's title and route and does not expose the standalone app's debug globals.
  Saved pages, saved queries, broadcasts and settings use the host's storage namespace.
- The library is ESM. Its JavaScript, declarations, styles, workers, planner WASM, DuckDB WASM and required
  extensions, and saved-store WASM come from one Bazel build. Consumers need no Java or Bazel installation.
- Runtime assets are served by the embedding application's own origin. A packaged CLI copies them into its static
  assets directory. Local-file analysis requires only those static requests; a remote file is read only when
  explicitly opened. Offline reloads are the host application's caching concern.

## Packaging

The source package remains private. `datacube/npm/package.json` is the release manifest, with the proposed npm
name `@legend-lite/datacube`. Its scope must be owned by the publishing maintainer. `//datacube:npm_package` creates
the installable directory; `//datacube:npm_archive` packs it with the pinned npm toolchain. Nothing publishes as a
side effect of a build. The package's README documents installation, lifecycle, browser requirements and release.

## Acceptance

The decisive check is `//datacube:npm_consumer_test`: install the real archive offline in an otherwise empty
directory, typecheck against the installed declarations, bundle the consumer, copy the installed assets, and serve
only that directory at a nested URL. In a real browser, open a local CSV, verify rows, filter, group and pivot,
save/open locally, dispose and remount. Reject unexpected requests; verify worker cleanup and preservation of the
host's title and URL. The existing architectural layer checks and JVM/WASM differentials still apply.

Focused resource-lifetime tests precede the integration check. The repository's local gate is the broader
regression check. Test results and any environment limitations are recorded with the checkpoint report, rather
than presumed from an internal demo or a successful bundle.

## Implementation and review notes

The archive is `bazel-bin/datacube/legend-lite-datacube.tgz`. The source manifests remain private; the release
manifest and installation/release instructions are in `datacube/npm/`. The public facade's declarations contain
only its interfaces and DOM types. The asset-copy command runs offline with the installed package alone.

The lifecycle review exercised failures as well as successful teardown. DuckDB-WASM's worker termination does
not settle its pending JavaScript requests, so the owning runtime explicitly rejects outstanding work. Disposal
also stops the planner, rejects public file operations, closes owned dialogs and touch handlers, and cancels
IndexedDB opens blocked by another tab. IndexedDB itself cannot cancel a queued open; any late connection is
closed when it arrives. Borrowed planners retain their existing graceful release path.

Source and save dialogs are fetched when opened. This keeps the existing standalone browser, engine and notebook
startup-size budgets intact while adding lifecycle ownership. No size ceiling was raised. One source guard was
corrected, with a dated explanation, to distinguish the platform's `AbortSignal` abort listener (available in Node)
from DOM listeners; its screen-import and DOM-global restrictions remain.

## Verification, 2026-11-15

Implementation checkpoint: `a6c31e0e0`, pushed to `fork/make_packageable_codex`. Its runtime-lifecycle checkpoints
are `231386080` and `87b2b099f`. No npm package was published.

- `bazel test --lockfile_mode=error //gates:local` passed on macOS arm64: 544 test targets passed and two were
  skipped by Bazel; 241 targets executed in the final run and the remaining successful results were cached.
  Invocation: `dad4debb-9eea-482f-a2c5-a6fa6e739ae6`. This includes the architectural guards, browser scenarios,
  JVM/WebAssembly differential checks, and the new consumer and lifecycle regressions.
- `//datacube:npm_consumer_test` passed with the actual archive installed offline in a separate application:
  strict public declarations and CSS imports, a nested static asset URL, CSV rows/filter/group/pivot, local
  save/open, malformed Parquet refusal, dispose/remount, held-file cancellation, startup cancellation during
  WASM and ICU downloads, and startup cancellation behind a blocked IndexedDB open. It verified no unexpected
  network requests, browser errors, or surviving workers, and no leaked late storage connection.
- The original startup bundle budgets passed unchanged. The focused dialog, touch, query-store, planner, and
  database-lifecycle checks passed. The test import manifest was regenerated with `//datacube:test_imports`.
- `git diff --check` passed before the implementation checkpoint.

The tested archive is `bazel-bin/datacube/legend-lite-datacube.tgz` (about 16 MB compressed). Its SHA-256 is
`c5cc0b2d0fd68406807c9635678fece4feb294d631f3763c5834437820681ae2`.
