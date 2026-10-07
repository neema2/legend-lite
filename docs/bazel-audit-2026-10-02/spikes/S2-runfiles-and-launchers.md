# Spike S2: official runfiles, and serve/app without launchers (2026-10-03)

**Plan items:** Phase 1.2 (official runfiles everywhere) and Phase 4.3 (the warehouse and the app without bash).
**Base:** `main` at `23b441852`. **Branch:** `spike/s2-runfiles`, one local commit, `56b4ae329`. Not pushed.
**Machine:** macOS arm64, Bazel 9.2.0, rules_java 9.9.0, aspect_rules_js 3.4.1, rules_graalvm 0.12.0 (GraalVM CE 25.0.2).
Every build used `--local_resources=cpu=3 --jobs=3`. The warehouse is still a native image built with `--link-at-build-time`.

## Decisions

| # | Question | Decision |
|---|---|---|
| 1 | `//warehouse:serve` and `//datacube:app` as plain executables (no bash script, no hermetic-launcher), with the server finding its files through `@rules_java//java/runfiles` | **GO**. Measured on macOS, in tree mode and in manifest-only mode, under `bazel run`, as a test's data dependency, and run straight from `bazel-bin`. Windows is argued from Bazel's source below. It still needs one check on a Windows machine. |
| 1a | Replace the gzip `run_shell` with a non-shell action | **GO**. A 25-line `GZIPInputStream` program run through the existing `java_run`. |
| 2 | Official runfiles in a Java test (`//tools/deps:core_layering_test`) | **GO** for the code. It passes with `--noenable_runfiles` and no runfiles tree, while the `Repo` version fails. **Caveat:** on macOS (probably Linux too) rules_java's bash stub cannot start *any* `java_test` or `java_binary` tool in manifest-only mode inside the sandbox. The proof needs `--spawn_strategy=local` (or `--strategy=TestRunner=local`). |
| 3 | Official runfiles in a JS test (`//datacube:wasm_differential_test`) via `@bazel/runfiles` | **GO** for the code. It passes with `--noenable_runfiles` in rules_js's no-runfiles mode, the one Windows uses. **But:** (a) rules_js on Unix cannot run any `js_test` or `js_binary` tool without a runfiles tree; (b) `chdir = package_name()` (18 targets) needs the tree on every platform; (c) today's `new URL('../..', import.meta.url)` arithmetic *also* passes in that mode. So `@bazel/runfiles` is a hygiene win (declared inputs, layout independence), not the thing that unblocks Windows. |
| — | The plan's done-criterion "every test passes with `--noenable_runfiles` on Linux" | **NO-GO as written.** Two rule sets block it whatever our code does: rules_java's stub in the sandbox, and the rules_js Unix launcher. Change the criterion (see Recommendations). |

## Evidence

All commands were run from the spike worktree. Output is trimmed and paths are shortened.

### E1. What `bazel run` gives a plain native executable (probe: `spike/runfiles/`)

A small native image (`//spike/runfiles:probe_native`, `--link-at-build-time`) that depends on `@rules_java//java/runfiles`, run through a symlinking rule (`linked_binary`, the shape the recommended design uses):

```
$ bazel run //spike/runfiles:probe_link -- 'a&b'          # args = ["--fixed", "$(rlocationpath other.txt)", "fixed with space"]
arg[0]=<--fixed>
arg[1]=<_main/spike/runfiles/other.txt>
arg[2]=<fixed>  arg[3]=<with>  arg[4]=<space>        # `args` is Bourne-tokenized: quote inside the string
arg[5]=<a&b>
env RUNFILES_DIR=null  RUNFILES_MANIFEST_FILE=null  RUNFILES_MANIFEST_ONLY=null  JAVA_RUNFILES=null
env BUILD_WORKING_DIRECTORY=<repo>/warehouse
user.dir=<execroot>/bazel-out/darwin_arm64-fastbuild/bin/spike/runfiles/probe_link.runfiles/_main
command=<execroot>/bazel-out/.../spike/runfiles/probe_link      # the symlink's path, not server_native-bin
Exception in thread "main" java.io.IOException: Cannot find runfiles: $RUNFILES_DIR and $JAVA_RUNFILES are both unset or empty
```

What this shows:
- `bazel run` sets **no** `RUNFILES_*`. This is deliberate in Bazel: `RunCommand.ENV_VARIABLES_TO_CLEAR_UNCONDITIONALLY` clears `JAVA_RUNFILES`, `RUNFILES_DIR`, `RUNFILES_MANIFEST_FILE`, `RUNFILES_MANIFEST_ONLY` and `TEST_SRCDIR`.
- rules_java's library reads only the environment. Unlike the C++ library, it has no argv0 discovery, so `Runfiles.preload()` throws.
- The cure is to build the environment map ourselves and call `Runfiles.preload(Map)`. The library itself works in the native image with no reflection config: the same probe resolved files once given the map (E2).

### E2. Self-location, then the official library (tree, manifest-only, direct)

With a ~20-line `locate()` (look beside the executable for a manifest, then a tree; then the working directory) feeding `Runfiles.preload(env)`:

```
$ bazel run //spike/runfiles:probe_link                     # tree
locate: dir beside .../probe_link
rlocation(_main/spike/runfiles/data.txt)=.../probe_link.runfiles/_main/spike/runfiles/data.txt exists=true
$ <repo>/bazel-bin/spike/runfiles/probe_link                # straight from bazel-bin, cwd elsewhere
command=<repo>/bazel-bin/spike/runfiles/probe_link
rlocation(...)=<repo>/bazel-bin/spike/runfiles/probe_link.runfiles/_main/spike/runfiles/data.txt exists=true
$ bazel run --noenable_runfiles //spike/runfiles:probe_link   # first version, tree checked first
rlocation(...)=.../probe_link.runfiles/_main/spike/runfiles/data.txt exists=false
$ ls bazel-bin/spike/runfiles/probe_link.runfiles/
_main/   (empty)    MANIFEST -> .../probe_link.runfiles_manifest
```

So with runfiles off, the `.runfiles/` directory still exists (empty `_main/` plus `MANIFEST`). **Check the manifest first**: it is present in both modes. `ServerRunfiles` does this.

### E3. The real thing: `//warehouse:serve` is the native server (spike commit)

```
$ bazel build //warehouse:serve //warehouse:launcher_test_serve_site //datacube:app
INFO: Build completed successfully, 176 total actions          # native image rebuilt, --link-at-build-time

$ cd warehouse && bazel run //warehouse:serve -- --port 'x&y z"q'; echo exit=$?
warehouse: For input string: "x&y z"q"                          # '&', space and '"' intact
exit=2                                                          # exit code straight back (no launcher)

$ cd runs/s2 && bazel run //warehouse:serve -- --port 0 --user alice:pw \
      'postgresql://nobody@127.0.0.1:1/db?sslmode=disable&connect_timeout=2'
warehouse: could not attach Postgres catalog db: IO Error: Unable to connect to Postgres at "<dsn>": connection
  to server at "127.0.0.1", port 1 failed: Connection refused   # URL parsed whole; the extension loaded from runfiles
$ ls runs/s2
warehouse-data                                                  # default --data is where `bazel run` started

$ bazel run --noenable_runfiles //warehouse:serve -- --port 0 'postgresql://...&connect_timeout=2'
warehouse: could not attach Postgres catalog db: IO Error: Unable to connect to Postgres ...   # manifest only: same
$ ls -A bazel-bin/warehouse/serve.runfiles/_main                # (empty: no tree)

$ cd <scratch> && <repo>/bazel-bin/warehouse/serve --port 0 --data ./wd 'postgresql://...&connect_timeout=2'
warehouse: could not attach Postgres catalog db: IO Error: Unable to connect to Postgres ...   # direct: same
$ <repo>/bazel-bin/warehouse/server_native-bin --port 0 --data ./wd 'postgresql://...'          # control: the bare image
java.io.IOException: DuckDB's postgres extension is not at <repo>/bazel-bin/warehouse/postgres_scanner.duckdb_extension

$ cd runs/s2 && bazel run //warehouse:launcher_test_serve_site -- --port 0      # args = --site $(rlocationpath ...) --single-user
warehouse listening on 127.0.0.1:55156, catalogs [main]
DataCube: http://127.0.0.1:55156/#key=…
$ curl -s http://127.0.0.1:55156/ | grep -o 'served through the launcher'
served through the launcher

$ bazel run //datacube:app -- --port 'a&b c"d'                    # the app's fixed args + caller args, without opening a browser
warehouse: For input string: "a&b c"d"
```

The adapted `//warehouse:launcher_test` uses `$(rlocationpath)` and the runfiles library, and passes on to the server the runfiles environment the runfiles library returns (`getEnvVars()`). It starts the embedded Postgres 16, attaches it by a URL containing `&`, serves the site, and checks the exit code and the default data directory:

```
$ bazel test //warehouse:launcher_test
//warehouse:launcher_test     PASSED in 11.8s       [ 4 tests successful ]
$ bazel test --noenable_runfiles --spawn_strategy=local //warehouse:launcher_test
//warehouse:launcher_test     PASSED in 10.8s       [ 4 tests successful ]
$ ls -A bazel-bin/warehouse/launcher_test.runfiles/_main        # (empty: manifest only)
$ bazel test //warehouse:tests                       # the server's JVM suite, incl. command-line parsing
//warehouse:tests             PASSED in 55.2s       [ 99 tests successful ]
```

### E4. Gunzip without a shell

`//warehouse:duckdb_extensions` is a `java_run` of `tools/gunzip/Gunzip.java` (`GZIPInputStream.transferTo`), with output `warehouse/duckdb_extensions/postgres_scanner.duckdb_extension`. Its result is used by every E3 run above. One output for the whole repository replaces the per-launcher `<name>_extensions` `run_shell`. The `select` (`POSTGRES_EXTENSION`) goes through an `alias`, because `$(execpath)` needs a label.

### E5. Java test: `//tools/deps:core_layering_test`

The BUILD file passes `-Dcore.layers=$(rlocationpath core-layers.txt)` and `-Dcore.layer.files=<comma-joined $(rlocationpath :layer_X)>`. The test resolves them with `Runfiles.preload().unmapped().rlocation(...)`, and `//testing` (Repo) is no longer a dependency.

```
# BEFORE (Repo), manifest only, no sandbox:
$ bazel test --noenable_runfiles --strategy=TestRunner=local //tools/deps:core_layering_test
    => java.nio.file.NoSuchFileException: .../core_layering_test.runfiles/_main/tools/deps/core-layers.txt
[ 0 tests successful ] [ 1 tests failed ]
# AFTER (official library):
$ bazel test //tools/deps:core_layering_test                                              PASSED   [1 successful]
$ bazel test --noenable_runfiles --strategy=TestRunner=local //tools/deps:core_layering_test PASSED  [1 successful]
$ ls bazel-bin/tools/deps/core_layering_test.runfiles/_main/tools/deps   → No such file or directory
# BOTH, manifest only, sandboxed (macOS darwin-sandbox): rules_java's stub dies before the JVM starts
$ bazel test --noenable_runfiles //tools/deps:core_layering_test
grep: .../core_layering_test.runfiles/MANIFEST: No such file or directory
.../core_layering_test: line 409: exec: : not found
```

The stub (rules_java 9.9, `java_stub_template`) sets `RUNFILES_MANIFEST_FILE=${JAVA_RUNFILES}/MANIFEST` and `RUNFILES_MANIFEST_ONLY=1`. It then looks up the JDK in that manifest. With runfiles off, the sandbox does not stage `<test>.runfiles/MANIFEST`, so `JAVABIN` comes out empty. The same failure breaks every `java_binary` build tool (`//wasm:planner`'s TeaVM compile: `Exit 127`). On Windows the Java launcher is a native `.exe` that reads the manifest, so this does not apply there.

### E6. JS test: `//datacube:wasm_differential_test`

- `@bazel/runfiles@6.5.0` is an exact pin, added only through Bazel's pinned pnpm (`bazel run -- @pnpm//:pnpm --dir $PWD/datacube add -D @bazel/runfiles --lockfile-only`, then `install --lockfile-only` after pinning exactly). The lock diff is 8 lines with an integrity hash.
- The BUILD file passes `env = {"CUBE_JVM_ANSWERS": "$(rlocationpath :cube_jvm_answers)", "WASM_PLANNER": "$(rlocationpaths //wasm:planner)"}`. `//wasm:planner` is two files, so it needs `rlocationpaths`; the test takes the directory of `classes.wasm`. A `copy_to_directory` `planner_dir` would give one clean label.
- `typecheck_test` needs `:node_modules/@bazel/runfiles` in its data. The package ships its own types.

```
$ bazel test //datacube:wasm_differential_test                                   PASSED  (ℹ pass 2)
# manifest only, rules_js Unix launcher: fails before node starts, whatever the test code
$ bazel test --noenable_runfiles --spawn_strategy=local --action_env=JS_BINARY__NO_RUNFILES=1 //datacube:wasm_differential_test
.../wasm_differential_test: line 491: cd: datacube: No such file or directory     # chdir = package_name()
# (and without --action_env, the build tool //datacube:cube_queries dies first:
#  FATAL: aspect_rules_js[js_binary]: node binary '.../emit_cube_queries.runfiles/_main/../rules_nodejs.../node' not found)
# manifest only, rules_js's no-runfiles mode (what js_binary.bzl sets on Windows when runfiles are off),
# a variant without chdir and without the cwd-relative strict reporter:
$ bazel test --noenable_runfiles --spawn_strategy=local --action_env=JS_BINARY__NO_RUNFILES=1 \
      --test_env=JS_BINARY__NO_RUNFILES=1 //datacube:wasm_differential_manifest_test   PASSED  (# pass 2)
# control: today's compare.ts (URL arithmetic), same mode:
$ bazel test ...same flags... //datacube:wasm_differential_baseline_test                PASSED  (# pass 2)
```

Why the control passes: in no-runfiles mode rules_js runs from the execroot, and `copy_data_to_bin` puts the sources beside the generated files in `bazel-bin`. So relative URL arithmetic still lands on real files. The real Windows blockers for JS tests are `chdir` (cwd-relative, and the working directory is an empty `<test>.runfiles/_main`) and the cwd-relative `--test-reporter=./test/strict-reporter.mjs` / `../datacube/test/strict-reporter.mjs`.

## Windows reasoning (not measured here; measure on the desk, see Open questions)

1. **How `bazel run` starts a plain `.exe`.** The server-side `RunCommandLine.WindowsFormatter.formatArgv` builds the argv. Without `--run_under`, the binary goes first unescaped. Every other argument (the target's expanded `args`, then the caller's residue) goes through `ShellUtils.windowsEscapeArg`. That function quotes on a space or `"`, escapes `"` as `\"` and doubles the backslashes before a quote: the CommandLineToArgvW / MSVC CRT rules. The client (`blaze_util_windows.cc`, `ExecuteRunRequest` → `ExecuteProgram`) passes those strings unchanged to `CreateProcessW`, waits, and exits with the child's exit code. **No `cmd.exe` and no bash are involved, so `&` has no meaning**, and a `"` survives because it is CRT-escaped. This matches PR #14's measurement (docs/WINDOWS_APP_DESIGN_2026_10_02.md, §3 limit 2: "`bazel run` straight to the `.exe` passes both intact").
2. **The `args` attribute** is tokenized Bourne-style at analysis on every platform (E1: `"fixed with space"` became three arguments). Then it goes through the same `windowsEscapeArg`. hermetic-launcher's limits do not apply: no 10-argument cap, no 256-byte slots, no `"` mangling.
3. **RUNFILES_\*.** `bazel run` clears them on every platform (`ENV_VARIABLES_TO_CLEAR_UNCONDITIONALLY`). Under `bazel test` the test runner sets `RUNFILES_MANIFEST_FILE`, and a test that starts the server passes `Runfiles.getEnvVars()` on to it. Both paths are covered by `ServerRunfiles`.
4. **Working directory.** `RunCommand.ensureRunfilesBuilt`: "On Windows, runfiles tree is disabled. Workspace name directory doesn't exist, so don't add it." With runfiles off, the working directory is `<exe>.runfiles` itself. Bazel always creates `<exe>.runfiles/_main` (bazelbuild/bazel#10621). `ServerRunfiles` accepts both `<x>.runfiles/_main` and `<x>.runfiles` as the working directory, and reads `<x>.runfiles_manifest` / `<x>.runfiles/MANIFEST`. `--run_in_cwd` exists as a user flag (it is a `RunOptions` field); BUILD_WORKING_DIRECTORY handling makes it unnecessary.
5. **Self-location of a symlinked `.exe`.** This is the one real unknown. On macOS, `ProcessHandle.info().command()` returned the symlink's path. On Linux it reads `/proc/self/exe`, which resolves symlinks, so `ServerRunfiles` also tries argv[0] from `/proc/self/cmdline`. On Windows the JDK uses `QueryFullProcessImageNameW`, which may return the symlink's *target* (`server_native-bin.exe`). Coverage then:
   - Under `bazel run`, the working-directory fallback catches it.
   - Under `bazel test`, the environment does.
   - Only "double-click `bazel-bin\warehouse\serve.exe`" would miss. There the server falls back to "beside the executable", which finds the library but not the extension.
   - Remedy if measured: in `_warehouse_binary`, copy instead of symlinking on Windows. A plain `ctx.actions.symlink` already copies when `--windows_enable_symlinks` is off. Or read `GetModuleFileNameW` through FFM.
6. **The native image's argv on Windows.** GraalVM's launcher receives the C runtime's `argv`. That is CRT-parsed, so it is consistent with point 1, but it is in the ANSI code page: a non-ASCII argument could be lossy. hermetic-launcher has the same limitation. Note it in the docs.
7. **What gets simpler on Windows.**
   - One process: Ctrl+C reaches the server directly, no job object is needed, and the exit code is the server's.
   - The hermetic-launcher toolchain and its stubs disappear, and so does the select over `_posix`/`_windows`.
   - The executable is just `server_native-bin.exe` under another name, so Windows support for serve/app becomes only "does `server_native` build" (MSVC). That is the plan's stated aim.

## Recommended design

### Targets (as on the spike branch)

```python
# warehouse/defs.bzl  -- replaces _duckdb_extensions, _posix_launcher, shell_quote, the launcher_binary call,
# the 10-argument guard and the alias; drops the hermetic_launcher load.
def _warehouse_binary_impl(ctx):
    server = ctx.executable.server
    exe = ctx.actions.declare_file(ctx.label.name + (".exe" if server.extension == "exe" else ""))
    ctx.actions.symlink(output = exe, target_file = server, is_executable = True)
    runfiles = ctx.runfiles(files = ctx.files.data).merge(ctx.attr.server[DefaultInfo].default_runfiles)
    return [DefaultInfo(executable = exe, runfiles = runfiles)]

def warehouse_run(name, server, data = [], args = [], testonly = False, **kwargs):
    _warehouse_binary(name = name, server = server,
        data = ["//warehouse:duckdb_library", "//warehouse:duckdb_extensions"] + data,
        args = args, testonly = testonly, target_compatible_with = NOT_ON_WINDOWS_ARM64, **kwargs)
```

```python
# warehouse/BUILD.bazel
warehouse_run(name = "serve", server = ":server_native")
alias(name = "duckdb_extension_gz", actual = POSTGRES_EXTENSION)
java_run(
    name = "duckdb_extensions",
    srcs = [":duckdb_extension_gz"],
    outs = ["duckdb_extensions/postgres_scanner.duckdb_extension"],
    arguments = ["$(execpath :duckdb_extension_gz)", "{OUT}"],
    main_class = "com.legend.tools.gunzip.Gunzip",
    mnemonic = "GunzipDuckdbExtension",
    deps = ["//tools/gunzip"],
)
# server_lib: + "@rules_java//java/runfiles"

# datacube/BUILD.bazel
warehouse_run(
    name = "app",
    server = "//warehouse:server_native",
    data = [":dist"],
    args = ["--site", "$(rlocationpath :dist)", "--single-user", "--open"],
)
```

**Fixed arguments: both mechanisms, each for what it is good at.**
- *DuckDB's library and extension*: built into the server as runfiles defaults. When the server is a native image and no flag is given, it looks up `<repo>/warehouse/<DuckLibrary.resourceName()>` and `<repo>/warehouse/duckdb_extensions/postgres_scanner.duckdb_extension`. `<repo>` comes from `@AutoBazelRepository` (`_main` in this repository, the canonical name if legend-lite is ever a dependency). Reason: Bazel applies a target's `args` **only** under `bazel run` and `bazel test`. A test that starts `$(rlocationpath :serve)`, or a person running `bazel-bin/warehouse/serve`, gets no `args`, and the server must still work. `launcher_test` proves the defaults hold as a data dependency, in both runfiles modes.
- *What a particular target adds* (the app's `--site`, `--single-user`, `--open`): the `args` attribute with `$(rlocationpath)`. These only make sense under `bazel run`. Remember the Bourne tokenization: a value with a space must be quoted inside the string.

### Server changes (spike: +~150 lines, all in `warehouse/src/main/java/com/legend/warehouse/server/`)

- **New `ServerRunfiles`** (about 110 lines).
  - `find()` takes `RUNFILES_*` from the environment when set.
  - Otherwise it searches for `<exe>.runfiles_manifest`, `<exe>.runfiles/MANIFEST` and `<exe>.runfiles/`, where `<exe>` is ProcessHandle's command, or argv[0] from `/proc/self/cmdline`.
  - Failing that, it uses a working directory that is `<x>.runfiles` or `<x>.runfiles/<ws>`.
  - It hands the result to `Runfiles.preload(env).unmapped()`. `rlocation(path)` returns an existing absolute path or null.
- **`WarehouseServer.commandLine`.**
  - `--site`, `--duckdb-library` and `--duckdb-extensions` go through `named(value, startedIn)`: absolute stays as given; a relative value resolves against BUILD_WORKING_DIRECTORY if it exists there, otherwise as a runfiles path.
  - `--duckdb-extensions` accepts the directory or the extension file. `$(rlocationpath)` names a file, and a manifest lists files, not their parent.
  - `--token-key-file` is now caller-relative, like `--data` already was.
  - The runfiles defaults for library and extension come before the existing "beside the executable" fallback, which stays for the Phase 4.3 `dist` layout.
- **`DuckLibrary.resourceName()`** becomes public.
- **chdir via FFM:** not needed. Every caller-relative path in the server's own flags is now resolved. The one remaining case is a relative libpq parameter inside a DSN (`sslrootcert=./ca.pem`), which libpq resolves against the process's working directory. That is the runfiles directory under `bazel run` on every platform. It was also true of the Windows hermetic-launcher path, while the macOS/Linux bash `cd` used to hide it. A `chdir(BUILD_WORKING_DIRECTORY)` through FFM is about 10 lines (needs a downcall `int chdir(const char*)` / `_wchdir` registered in the reachability metadata), but resolve every runfile *before* it runs. Recommendation: document "use absolute paths in DSN file parameters", and add the chdir only if a user asks.

### Tests

- **Java.** Paths come from `$(rlocationpath …)` through `jvm_flags` or `env`, and are resolved with `Runfiles.preload().unmapped().rlocation()`. A child process gets `getEnvVars()`. Put a 10-line `runfile(String)` helper in `//testing`, replacing `Repo.path`/`Repo.module`. `Repo.out` stays (it is `TEST_UNDECLARED_OUTPUTS_DIR`, not a runfile).
- **JS.** `@bazel/runfiles` (exact pin) plus `env = {"X": "$(rlocationpath …)"}`, resolved with `runfiles.resolve(process.env.X)`. A `node_test` macro (Phase 1.4) should add the package and the env, and should **drop `chdir`** in favour of runfiles-resolved paths and a reporter given by `$(rlocationpath)`.

## What it deletes

| Deleted | Where |
|---|---|
| The generated bash launcher (`_posix_launcher`, `shell_quote`, the `cd "$BUILD_WORKING_DIRECTORY"` script) | `warehouse/defs.bzl` |
| hermetic-launcher: the `launcher_binary` call, the 10-argument guard, the `_posix`/`_windows` alias, `bazel_dep(name = "hermetic_launcher")` and its MODULE.bazel comment (the `platforms` comment still names it) | `warehouse/defs.bzl`, `MODULE.bazel` (+ lock) |
| The gzip `run_shell` (`_duckdb_extensions`), and one extension output per launcher | `warehouse/defs.bzl` → one `//warehouse:duckdb_extensions` |
| `Repo`'s environment parsing for the converted test; `//testing` as its dependency | `tools/deps` (1 of 72 `Repo` users; `Upstream` has 38) |
| `RUNFILES_DIR = Repo.root().getParent()` hand-off in `LauncherTest` | replaced by `Runfiles.getEnvVars()` |
| `new URL('../../../wasm/planner/', import.meta.url)` in the converted test | 1 of 51 files that use `import.meta.url` |

The spike diff is 13 files changed, +229/−175, plus new `ServerRunfiles.java`, `tools/gunzip/` and the `spike/` probe. `warehouse/defs.bzl` alone loses about 150 lines.

## Risks

1. **rules_java's bash stub in manifest-only mode, sandboxed (macOS measured, Linux likely).** It blocks a Unix "`--noenable_runfiles` everything" lane for any `java_test` or `java_binary` tool. Workaround: `--spawn_strategy=local` for that lane. Root cause to report upstream: the sandbox does not stage `<x>.runfiles/MANIFEST` while the stub insists on it.
2. **rules_js on Unix needs the runfiles tree** for every `js_test` and `js_binary` tool. Its no-runfiles mode (`JS_BINARY__NO_RUNFILES`) is only switched on for Windows. Driving it on Unix with `--action_env` / `--test_env=JS_BINARY__NO_RUNFILES=1` worked here but is undocumented internals. Don't build a gate on it.
3. **`chdir = package_name()` (18 js targets) needs the tree on Windows too.** Until those targets drop `chdir` and use runfiles-resolved paths, `build:windows --enable_runfiles` must stay.
4. **Windows symlinked `.exe` self-location** (Windows point 5). `bazel run` and `bazel test` are covered by fallbacks; a direct double-click might not be. Measure.
5. **Hard-coded runfiles paths in the server** (`warehouse/duckdb_extensions/...`, `warehouse/<resourceName>`). If a target is renamed, the server silently loses its default. `launcher_test` catches this (the URL-attach test fails). Add a one-line BUILD comment on both targets.
6. **The target's `args` apply only under `bazel run` and `bazel test`.** That is why the library and extension are server defaults. Anyone adding a new fixed flag to `serve` must know that a data-dependency caller will not get it.
7. **ANSI argv on Windows** for non-ASCII arguments (Windows point 6). Same as today.

## Recommendations for the plan

- **Phase 4.3:** adopt the design above as written. A `native_binary` from skylib is not needed: the 10-line `_warehouse_binary` rule is the whole of it. Keep the "beside the executable" fallback for `//warehouse:dist`.
- **Phase 1.2:** rewrite the done-criterion to:
  1. every Java test and `node_test` resolves runfiles only through the official libraries (a grep guard: no `Repo.path`/`Repo.module`, no `import.meta.url` path arithmetic, no `TEST_SRCDIR`);
  2. **Windows CI** runs `bazel test //...` without `--enable_runfiles` (manifest is native there: the Java `.exe` launcher, and rules_js's no-runfiles mode);
  3. on Linux, an optional lane `--noenable_runfiles --spawn_strategy=local` for Java targets only.

  Remove `build:windows --enable_runfiles` only once the 18 `chdir` js targets and the cwd-relative strict reporter are converted.
- Don't present `@bazel/runfiles` as the thing that unblocks Windows: today's URL arithmetic already works in rules_js's Windows mode. Its value is explicit inputs and layout independence (an external repository, a `copy_to_directory`, a renamed package).

## Effort (from what the spike took)

| Item | Spike | Production |
|---|---|---|
| Serve/app launcher-free (server + defs + BUILD + LauncherTest) | about 2 h including probes | **1–1.5 days**: unit tests for `named()`/`ServerRunfiles` with a fake manifest; renaming `LauncherTest` (there is no launcher left); docs (`WINDOWS_APP_DESIGN`, `DATACUBE_ON_POSTGRES`); a **Windows desk run** (E3's commands plus a direct `serve.exe`); the `platforms` comment |
| Gunzip as `java_run` | 15 min | done as is (0.25 day with review) |
| Java tests to official runfiles | 20 min for one test | 72 `Repo` users + 38 `Upstream` + EmbeddedPostgres + `junit_test`'s `-Dlegend.engine.root=../…`: mostly mechanical (`Repo.module("x")` → a `$(rlocationpath x)` flag). **3–4 days**, the upstream-tree users being the long tail (whole trees: pass one file such as `$(rlocationpath @legend_engine_src//:pom.xml)` and take its directory) |
| JS tests to `@bazel/runfiles` | 30 min for one test | 51 files using `import.meta.url` + dropping `chdir` from 18 targets + the reporter by label, best done inside the Phase 1.4 `node_test` macro: **2–3 days** |

## Open questions

1. On a Windows desk:
   - Does `ProcessHandle.info().command()` for the symlinked `serve.exe` report the link or the target?
   - Does `bazel-bin\warehouse\serve.exe` started directly find its runfiles?
   - Does `bazel run //warehouse:serve -- --port "x&y z\"q"` print the value intact? (By the Bazel source it should.)
2. Linux: confirm that rules_java's stub fails the same way in `linux-sandbox` with `--noenable_runfiles`. That decides whether a Linux manifest lane is worth having at all.
3. File the rules_java issue (`<x>.runfiles/MANIFEST` not staged in the sandbox when runfiles are off), or check whether a newer rules_java stub falls back to `<x>.runfiles_manifest`.
4. Should `//wasm:planner` get a single-directory output (`copy_to_directory`), so JS can name it with one `$(rlocationpath)`?
5. Do we want the FFM `chdir(BUILD_WORKING_DIRECTORY)` for relative libpq file parameters, or just document absolute paths?
6. Hard-coded defaults vs. a generated resource: the server could read its runfiles defaults from a small generated properties resource in `server_lib` (a `genrule` with `$(rlocationpath)`) instead of string constants. That gives one source of truth at the cost of one more target.

## Files on the spike branch (`spike/s2-runfiles`, commit `56b4ae329`)

- `warehouse/defs.bzl`, `warehouse/BUILD.bazel`, `datacube/BUILD.bazel`, `MODULE.bazel` (+ lock): the design above.
- `warehouse/src/main/java/com/legend/warehouse/server/ServerRunfiles.java` (new), `WarehouseServer.java`, `duck/DuckLibrary.java`.
- `tools/gunzip/` (new): `Gunzip.java` and its `java_library`.
- `warehouse/src/test/java/com/legend/warehouse/launcher/LauncherTest.java`: `$(rlocationpath)` and the runfiles library.
- `tools/deps/CoreLayeringTest.java`, `tools/deps/BUILD.bazel`: Q2.
- `datacube/test/wasm-differential/compare.ts`, `datacube/package.json`, `datacube/pnpm-lock.yaml`, `datacube/BUILD.bazel`: Q3. This includes the spike-only `wasm_differential_manifest_test` and `wasm_differential_baseline_test` (with `compare-baseline.ts`).
- `spike/runfiles/`: the native-image probe (E1, E2).
