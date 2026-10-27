# Running the build in a Claude Code cloud session

A cloud session runs in a container whose network goes through a proxy, as root, with a fixed disk allowance. Bazel
works there, but not out of the box. This is what a session hit and how it got through (2026-10-11 to 2026-10-14,
Bazel 9.2.0, a 4-core Linux x86 container).

## Maven Central answers Bazel's downloads with 404: `tools/cloud-maven-distdir.sh`

Seen 2026-10-15 after the Bazel repository cache was emptied to free the disk allowance: Bazel's downloader got
`404 Not Found` from `repo1.maven.org` for every jar the build pins, while `curl` of the same URL got the file. The
proxy answers the two clients differently. `tools/cloud-maven-distdir.sh <targets>` runs `bazel fetch --keep_going`,
reads the URLs Bazel could not download, fetches each with `curl` into the distdir (`~/.cache/legend-lite/distdir`,
which Bazel reads first and checks against the pinned sha256), and repeats until a fetch names none; four rounds for
the core test targets. Run it after `tools/cloud-distdir.sh`, which seeds the GitHub archives the same way.

## GitHub archive downloads are refused: `tools/cloud-distdir.sh`

**The symptom.** `bazel fetch`, `build` or `test` stops on

```
Error downloading [https://github.com/finos/legend-engine/archive/refs/tags/legend-engine-4.145.0.tar.gz] ...:
GET returned 403 Forbidden
```

and `curl` of the same URL answers `{"message":"GitHub access to this repository is not enabled for this session.
Use add_repo to request access. ..."}`.

**Why.** The session's proxy serves GitHub's archive downloads (`codeload.github.com/...`,
`github.com/<org>/<repo>/archive/...`) only for the repositories attached to the session, normally just this one.
What still works, measured on both dates: `git clone` of a public repository, GitHub release assets (bazelisk),
`mirror.bazel.build` and Maven Central. The build downloads GitHub archives for:

- the upstream sources the corpus, PCT and parity tests read: `legend-engine` and `legend-pure` at the release pinned
  in `release.MODULE.bazel` (with their `*_SRC_INTEGRITY`);
- the bats test helpers, through `aspect_bazel_lib`'s bats toolchain: `bats-core`, `bats-support`, `bats-assert`,
  `bats-file` (2026-10-11; a transitive module, so not named in this repository's own files).

**The fix.** From the repository root:

```
tools/cloud-distdir.sh              # or: tools/cloud-distdir.sh //core:core_tests  (default //gates:local)
```

It installs bazelisk as `bazel` if there is none, adds `common --distdir=~/.cache/legend-lite/distdir` to
`~/.bazelrc`, runs `bazel fetch --keep_going`, and gets every archive Bazel reports it could not download another
way: from Bazel's own mirror (`mirror.bazel.build/github.com/...`), else by cloning the tag with git (`--bare
--depth 1`, seconds) and packing it as GitHub does (`git archive --prefix=<repo>-<tag>/ | gzip -n`, byte-identical).
It fetches again until nothing new fails (Bazel names the next refused archive only after the previous one is
there). Bazel looks in the distdir first, by the URL's last path segment, and still checks each file against the
checksum the build pins, refusing a mismatch: nothing is trusted that the build does not already trust. Measured
2026-10-14: `legend-engine-4.145.0.tar.gz` packed to `sha256-zxwlJBBsHN4PmNp68juUPdrfV+ZjlF9us0fUaejm02E=` and
`legend-pure-5.99.0.tar.gz` to `sha256-8hb6qk1BTFWy6fuSzSX82Nk0ZWd16jDQCoZenP3yboo=`, the pins exactly; the four bats
archives came from the mirror with their pinned sha256.

The distdir and `~/.bazelrc` are the container's, so a new session runs the script again (it keeps what is already
there). `tools/cloud-distdir.sh --log <a bazel fetch log>` seeds from a log without running Bazel.

**Untested alternatives.** The 403 itself suggests attaching the repository to the session (`finos/legend-engine`,
`finos/legend-pure`), which may let the archive URLs through directly; a session's repositories are otherwise chosen
when it starts. An environment's setup script could also run `tools/cloud-distdir.sh` at every start.

## Other traps

- **The disk allowance.** The output base (about 8 GB) and the repository cache (about 20 GB) take most of it, and
  "No space left on device" then appears mid-build, often while copying inputs into a sandbox. When no build runs,
  `rm -rf "$(bazel info output_base)"/sandbox/*` and stale scratch output free a few GB. `df` reports the
  allowance, not the machine: "Avail" near zero means it is spent. The two big levers (2026-10-16, a `//gates:local`
  run that met the limit three times): Bazel 9's repository-contents cache (`~/.cache/bazel/_bazel_root/cache/repo_contents_cache`,
  about 16 GB: every fetched repository's extracted files, kept beside the output base's own copy) and the project's
  action disk cache (`--disk_cache`, `.bazelrc`; about 3 GB and growing). `common --repo_contents_cache=` in
  `~/.bazelrc` turns the first off for the container, `--disk_cache=` on a command turns the second off for that run;
  neither changes a result. The LLVM toolchain (`@@toolchains_llvm++llvm+llvm_toolchain_llvm`, 11 GB extracted, 1.9 GB
  as its tarball in the repository cache) is the one fetched repository the core tests need: delete its extraction
  only with its tarball kept, and re-fetch it with `bazel fetch --force --repo=@@toolchains_llvm++llvm+llvm_toolchain_llvm`
  (a plain fetch after an `rm` of the extraction keeps the stale marker and fails at analysis).
  `tools/cloud-distdir.sh`'s `bazel fetch` also pulls every platform's toolchains (Chromium, esbuild and DuckDB's
  binaries for macOS and Windows: about 1 GB, measured 2026-10-17), and the targets that read the reference sources
  (`//spec:spec_tests`, the `update_generated` checks) then build: a full local set that ran in the space before can
  fill it. Free space first; under a full disk a browser test fails as "Target crashed", not as "No space left on
  device".
- **Root.** The container runs as root, and the embedded Postgres (`initdb`) refuses root:
  `tools/postgres/as_unprivileged.sh` runs a Postgres test as an unprivileged user (its header says how).
  Measured 2026-10-21 (Bazel 9.2): give `--run_under` a copy of the script OUTSIDE the workspace
  (`cp tools/postgres/as_unprivileged.sh "$SCRATCH"/`). With the in-workspace path, a target that loads the root package
  (`//datacube:verify_app_test`, through `//datacube:app`'s `//:LICENSE`) fails its analysis with "Inconsistent filesystem
  operations" under `bazel-out/.../_javac`; the core Postgres targets run either way. The local gate's Postgres targets
  (eight: the seven `//core:*postgres*` tests and `//datacube:verify_app_test`) and `//pct:pct_postgres` all pass so.
- **The action disk cache in `~/.bazelrc`.** To turn it off for the container, write `build --disk_cache=`: a `common`
  line loses to the repository's own `build --disk_cache=~/.cache/bazel-disk` (2026-10-21; it went on filling the disk).
  Deleting the cache's files while a build runs costs only upload warnings ("Multiple errors during bulk transfer");
  recreate its `ac`, `cas` and `tmp` folders after.
- **DuckDB in a Node test asks nothing of the network** (2026-10-17). With no `XMLHttpRequest`, DuckDB-WASM's Node
  build fetched an extension it autoloads (ICU for the session's time zone, JSON, Parquet) from extensions.duckdb.org on
  a worker thread and waited with no timeout; in the sandbox the TLS to the agent proxy failed (no CA bundle), and eight
  tests ran until Bazel killed them (`//datacube:json_read_test`, the three `typed_values` tests, `upload_test`,
  `python_execute_test`, `verify_remote_test`, `//engine-client:model_data_test`). A test now starts DuckDB with
  engine-client's `startNodeDuckDb` (`engine-client/src/node-require.ts`), which answers from the pinned extensions
  `node_test` gives any test that takes DuckDB-WASM (`DUCKDB_EXTENSIONS`, `tools/js/defs.bzl`);
  `//engine-client:node_duckdb_test` holds it. The browser tests (`//datacube:verify_smoke_test`, `//query:verify_test`,
  `//site:verify_test`) pass in the container too (2026-10-17).
- **The browser tests' guard holds behind the proxy** (2026-10-25). The guard (`tools/browser/pinned-chromium.mjs`)
  makes extensions.duckdb.org unresolvable (`--host-resolver-rules`), but the container's `~/.bazelrc` passes
  `HTTPS_PROXY` to every test, and Chromium hands a proxied host to the proxy unresolved: the rule never applied, so
  `//datacube:browser_guard_test` met the proxy's certificate (`ERR_CERT_AUTHORITY_INVALID`), and a Chromium that
  trusts it downloaded the extension. The guard now gives Chromium the environment's proxy as its own flag, that host
  bypassing it (`proxyArgs`), so it is resolved locally and refused; every other host still goes through the proxy.
  Nothing the sites load comes from there: every DuckDB extension is the site's own `vendor/` copy, pinned by content
  in MODULE.bazel (`duckdb_wasm_<extension>_extension_wasm_eh`, served by `//engine-client:duckdb_extensions`).
- **`//tools/guards:classpath_test`** reaches an arm64-only target and cannot run on an x86 host.
- **WebAssembly.** The local core suite compiles no TeaVM target, so a JDK method TeaVM's class library lacks (such
  as `Character.toString(int)`) reaches CI first: after calling a JDK method new to core, build `//sdlc-server:page`
  and `//wasm:planner` locally.
- **The worker restarts.** A restarted session worker kills what it was running: run a long Bazel command in the
  background and commit a checkpoint to the branch before a long run.
