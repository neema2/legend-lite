# The DataCube app on Windows: the design (2026-10-02)

**Goal.** On Windows x64, `bazel run //datacube:app -- postgresql://reader@127.0.0.1:5432/shop` does
what it does on macOS and Linux: Bazel builds the native warehouse, puts DuckDB's library and its
Postgres extension beside it, serves the DataCube site, opens the browser. `//warehouse:serve` the
same. Today both are refused on Windows (`docs/DATACUBE_ON_POSTGRES.md`: "Windows is not supported
yet").

**Baseline** (the same day, ahead of this design): the Windows developer setup. `.gitattributes`
(`* text=auto eol=lf`: text files LF in every working tree, whatever `core.autocrlf` says, and an
editor's CRLF normalized on commit; the four committed files that carry CR are `-text`. The first cut
was `* -text`, which also dropped commit-side normalization: changed in review, 2026-10-03); `.bazelrc` names Git
for Windows' bash (`BAZEL_SH`, `--shell_executable`), without which no Maven repository fetched from a
PowerShell or cmd prompt; two tests that failed on a Windows desk and not on CI's runners
(`WarehouseJdbcTest`'s reference session zone, `query-store`'s `lite_test` stopping only the launcher
of a two-process server); the README's Windows prerequisites. On a Windows 11 x64 desk, `bazel build
//...` and `bazel test //...` are green: 147 pass, 2 skipped (the native targets below).

## What stops the app on Windows

- `//warehouse:server_native`, `:duckdb_library`, `:tests_native`, `:postgres_live_native`, `:serve`,
  `//datacube:app` and `//datacube:live_snap_test` are incompatible with Windows (`NOT_ON_WINDOWS`, or
  the same `select` inline), each because the native image was not built there.
- `duckdb_library` and `POSTGRES_EXTENSION` (`warehouse/defs.bzl`) have no Windows arm, and
  `MODULE.bazel` pins DuckDB's postgres extension for four platforms, none of them Windows.
- `warehouse_run`'s launcher is a bash script, and `bazel run` cannot start a script on Windows.

The warehouse's Java needs nothing: it names DuckDB's Windows library (`DuckLibrary.resourceName`),
opens the browser with `rundll32`, and makes the token key owner-only only where the file system has
POSIX modes. DuckDB's JDBC jar 1.5.5.1 carries `libduckdb_java.so_windows_amd64` (35 MB), and
`extensions.duckdb.org` serves `v1.5.5/windows_amd64/postgres_scanner.duckdb_extension.gz` (10 MB,
published with the Linux build).

## Decisions, and what they rest on

1. **The native binary on Windows too.** The app is "native only" (`DATACUBE_APP_PLAN_2026_10_02.md`,
   A2), and the Windows native build is owed (`WAREHOUSE_W1_DESIGN_2026_09_26.md`, `gates-run.yml`).
   GraalVM 25 supports FFM downcalls and upcalls on Windows x64, which is how the server calls DuckDB.
   rules_graalvm 0.12.0 already hands MSVC's environment to `native-image`; no new rule, no patch.
   *Cost:* Visual Studio 2022 Build Tools on every Windows desk, as a C toolchain is on macOS and Linux;
   the hosted `windows-2022` runners carry Visual Studio 2022. *Measured:* a native image built under
   Bazel with Build Tools 17.14 (MSVC 14.44) in 15 s (a probe program).

2. **The Windows launcher is [hermetic-launcher](https://github.com/hermeticbuild/hermetic-launcher)**
   (BCR `hermetic_launcher` 0.0.16, MIT): a native stub, built from a released template, with the
   entrypoint and fixed arguments baked in, `$(rlocationpath …)` arguments resolved through runfiles at
   run time, and the caller's arguments appended. Every alternative was run on a Windows desk with the
   app's own argument shapes (`…?sslmode=require&connect_timeout=10`, `options=-c%20statement_timeout…`,
   a DSN with spaces, an empty argument):

   | The launcher on Windows | Result |
   |---|---|
   | a `.bat` | cmd.exe splits the URL at `&` and runs the rest as commands |
   | the bash script, as `warehouse_run` makes it | `bazel run`: not a valid Win32 application |
   | `sh_binary` (Bazel's bash launcher) | quotes only arguments with spaces into `bash -c`; the `&` backgrounds the rest |
   | `java_binary` (Bazel's Java launcher) | every argument intact; ruled out: no Java process beside the native server |
   | **hermetic-launcher** | every argument intact but those holding `"` (below); embedded paths resolved; the exit code passed through; Ctrl+C reaches the server, whose shutdown hooks run |

   PowerShell needs a `.bat` in front of it to be started by `bazel run`, and the `.bat` is where `&`
   is lost.

3. **macOS and Linux keep their bash launcher.** It `exec`s the server, so nothing stands between the
   terminal and the server, and it runs the server where `bazel run` was started. A single launcher
   for all three would change both platforms, and neither can be run from the Windows desk this work
   is verified on.

## The design

### 1. Toolchain and targets

- **Windows x64 only.** DuckDB's jar carries no Windows ARM64 library. On Windows ARM64 the native
  targets are incompatible (`NOT_ON_WINDOWS_ARM64` on `server_native`, `duckdb_library` and
  `warehouse_run`'s targets; a `windows_aarch64` `config_setting`), so `bazel build //...` skips them
  there instead of failing analysis on a `select` with no arm for it (added in review, 2026-10-03).
- `//warehouse:server_native` drops `NOT_ON_WINDOWS`; on Windows it is an `.exe`.
- `//warehouse:duckdb_library` gains a `windows_x86_64` arm (a new `config_setting` beside
  `linux_x86_64`) naming `libduckdb_java.so_windows_amd64`, out of the same pinned jar.
- `MODULE.bazel` pins `windows_amd64` in the `duckdb_postgres_extension_*` comprehension, by its
  sha256 like the four others; `POSTGRES_EXTENSION` selects it for `//warehouse:windows_x86_64`.
- Every Windows exclusion listed above is removed; each existed only for want of the native image.
- `MODULE.bazel` adds `bazel_dep(name = "hermetic_launcher", version = "0.0.16")` and moves `platforms`
  from 1.0.0 to 1.1.0, the version hermetic_launcher requires (`--check_direct_dependencies` otherwise
  warns that the root's pin is not the resolved one).

### 2. The launcher

`warehouse_run` becomes a macro with the same attributes (`server`, `library`,
`postgres_extension_gz`, `site`, `args_before`) and four targets:

- **`<name>_extensions`**: a directory holding `postgres_scanner.duckdb_extension`, gunzipped from the
  pinned download as today (a directory, so that a launcher can name it: `--duckdb-extensions` takes a
  directory, and a runfiles manifest lists a directory output where it does not list a file's parent).
- **`<name>_posix`**: today's rule and script, `target_compatible_with` everything but Windows. Its one
  change: the script names the extension directory instead of taking the extension file's `dirname`.
- **`<name>_windows`**: a `launcher_binary` whose `entrypoint` is the server and whose
  `embedded_args` are `--duckdb-library $(rlocationpath <library>)`,
  `--duckdb-extensions $(rlocationpath <name>_extensions)`, then, given a site,
  `--site $(rlocationpath <site>)`, then `args_before`; `data` holds the library, the extension
  directory and the site; Windows only. The app's launcher uses 9 of the stub's 10 embedded arguments
  (the entrypoint counts). The stub's finalizer refuses an eleventh ("Maximum 10 arguments
  supported"), but only when the Windows target is built, so `warehouse_run` counts them itself and
  `fail()`s when the BUILD file loads, on every platform (added 2026-10-03, review).
- **`<name>`**: an `alias` selecting `<name>_windows` on `@platforms//os:windows`, `<name>_posix`
  elsewhere. `bazel run //datacube:app` and `bazel run //warehouse:serve` keep their names and their
  arguments.

### 3. Tests, the harness, CI

- **Newly on Windows**, with their exclusions gone: `//warehouse:tests_native` (TestServer starts the
  `.exe` itself and stops it with `destroy()`: one process) and `//datacube:live_snap_test` (spawns
  the `.exe` itself, stops it with `kill()`).
- **`//warehouse:launcher_test`**, new, a `junit_test` on every platform. Nothing runs a
  `warehouse_run` launcher today (`verify_app` is manual). It runs `//warehouse:serve` (from runfiles,
  `$(rlocationpath :serve)`) twice:
  1. `--port` with the one argument `x&y z` (an `&` and a space): the launcher exits 2 and the server
     said `warehouse: For input string: "x&y z"`. The arguments reached the server intact, and its
     exit code came back.
  2. `--data <the test's temporary directory> --port 0 --user alice:alice-pw` and a Postgres catalog
     by URL, `postgresql://postgres@127.0.0.1:<port>/postgres?sslmode=disable&connect_timeout=10`, on
     the embedded Postgres 16 that gate 7P starts (`//testing` `EmbeddedPostgres`,
     `@embedded_postgres`): the server prints `warehouse listening on 127.0.0.1:<n>, catalogs [main,
     postgres]`. The server and the extension directory resolved through the launcher (without
     `--duckdb-extensions` a native server looks beside its executable, where no extension sits), and
     the extension loaded and attached. DuckDB's library is passed too, but the native server would
     also find it beside itself (`DuckLibrary`), where Bazel puts it, so this test does not judge that
     flag on its own. The test then stops the launcher's descendants and the launcher.
  3. `//warehouse:launcher_test_serve_site`, a test-only `warehouse_run` in the app's shape (a site, a
     small directory of its own, and `args_before = ["--single-user"]`; not the app itself, whose
     `--open` would start a browser), with `--data <the test's temporary directory> --port 0`: the
     server prints `DataCube: http://127.0.0.1:<n>/#key=…` (only with `--site` and `--single-user`,
     so the fixed argument arrived), and `GET /` answers the site's `index.html` (so the site's
     directory resolved through runfiles). Added 2026-10-03, review: no automated test ran a
     launcher with a site or fixed arguments before.
  4. `//warehouse:serve` with `BUILD_WORKING_DIRECTORY` set to a temporary directory and no `--data`:
     the server's default `warehouse-data` is made there, not in the runfiles folder the launcher
     starts it in (added in review, 2026-10-03; `AppModeTest` holds the resolution itself).
- **`//datacube:verify_app`** (manual) takes the launcher from Bazel
  (`env = {"WAREHOUSE_SERVE": "$(rlocationpath //warehouse:serve)"}`) instead of naming
  `warehouse/serve.sh`, and stops it with `taskkill /t /f` on Windows (the launcher and the server
  are two processes there; `kill()` would stop the launcher alone, as `lite_test` found) and
  `kill('SIGTERM')` elsewhere.
- **CI** (`gates-run.yml`): the `native` lane is no longer filtered out on Windows, and runs
  `//warehouse:tests_native //warehouse:launcher_test` on all three platforms, and builds
  `//datacube:app` on each (no lane built it; on Windows that builds its stub, which holds 9 of
  hermetic-launcher's 10 arguments). The `browser` lane stays Linux-only, as designed.

### 4. Docs

- **`docs/DATACUBE_ON_POSTGRES.md`**:
  - Windows x64 in the requirements, with Developer Mode, Git for Windows, and Visual Studio 2022
    Build Tools ("Desktop development with C++"), installed before the first build, or else
    `bazel fetch --configure --force` once after, since Bazel keeps the C++ toolchain it found first.
  - PowerShell forms of section 3's sample database (the `psql` here-document) and of
    `PGPASSWORD=… bazel run`.
  - libpq's password file on Windows: `%APPDATA%\postgresql\pgpass.conf`.
  - The known limits below.
- **`README.md`**: Visual Studio 2022 Build Tools joins the Windows prerequisites (`bazel build //...`
  builds the native targets on Windows now), with the same `bazel fetch --configure --force` note.
- **Owed → done**: `WAREHOUSE_W1_DESIGN_2026_09_26.md`'s "Owed: Windows native builds", the
  `NOT_ON_WINDOWS` comments in `warehouse/BUILD.bazel` and `datacube/BUILD.bazel`, the comments in
  `gates-run.yml`; a dated entry in `docs/GATES.md` for the lane.

## Known limits on Windows (documented, not fixed here)

1. **The server runs in Bazel's runfiles folder**, not where `bazel run` was started: hermetic-launcher
   has no working-directory option. A relative `--data`, `//warehouse:serve`'s default `warehouse-data`
   among them, is still where `bazel run` was started: the server resolves it against
   `BUILD_WORKING_DIRECTORY`, on every platform (added in review, 2026-10-03: the first cut put it in
   Bazel's output tree, where `bazel clean` deletes it). Any other relative path among the caller's
   arguments (a `--token-key-file`, an `sslrootcert` in a URL) resolves in the runfiles folder. The
   app's usual arguments (a URL, `--table`, `--port`) are unaffected. `bazel run --run_in_cwd` runs it
   where it was started, per command.
2. **An argument containing `"` arrives mangled**, and a quoted argument ending in `\` gains a `\`
   (hermetic-launcher 0.0.16's Windows quoting; `bazel run` straight to the `.exe` passes both
   intact). Postgres URLs and libpq DSNs, which quote with `'`, contain neither.
3. **Windows x64 only.** On Windows ARM64 the native targets are skipped.
4. **Visual Studio installed after Bazel first ran** needs `bazel fetch --configure --force` once.
5. **`bazel run //datacube:verify_app` leaves a temporary directory behind.** It stops the app with
   `taskkill /t /f` (TerminateProcess), so the server's shutdown hook does not run, and each run on
   Windows leaves one `%TEMP%\datacube-*` (the single-user server's temporary data directory);
   remove it by hand.

1 and 2 are for hermetic-launcher upstream: a request for a working-directory option and a bug report.

Also fixed in review (2026-10-03): `--single-user` takes the account running it as its one principal,
and a principal holds only `[A-Za-z0-9_.@-]`, so a Windows account name with a space (`John Madsen`)
stopped the app at start. Each character a principal cannot hold is now written as `_`
(`Identity.accountPrincipal`: `John_Madsen`).

## Done when

- On a Windows x64 desk: `bazel build //...` and `bazel test //...` green, now including
  `//warehouse:tests_native`, `//warehouse:launcher_test` and `//datacube:live_snap_test`.
- On that desk, against Postgres 16: `bazel run //datacube:app -- postgresql://…` opens the browser on
  the tables; `bazel run //datacube:verify_app` prints only `ok:` lines; Ctrl+C stops the app and
  leaves no process and no temporary directory behind.
- macOS and Linux: `bazel run //datacube:app` behaves as before. Their CI lanes, the `native` lane on
  all three platforms among them, are green once the change is pushed.

## Out of scope

Windows ARM64; the `browser` lane and `//query:verify` on Windows; one launcher for every platform;
fixing hermetic-launcher; releases and installers.

## Order of work

1. §1's targets: the native image and `//warehouse:tests_native` green on Windows.
2. §2's launcher, with `//warehouse:launcher_test`.
3. The harness and `//datacube:live_snap_test`.
4. CI.
5. The docs.
6. End to end on the desk: the app, `verify_app`, Ctrl+C.

## Measured (Windows 11 x64, 2026-10-02)

- `bazel test //...`: 150 tests, all pass, none skipped. (Bazel's test cache served them;
  `//warehouse:tests_native`, `//warehouse:launcher_test` and `//datacube:live_snap_test`, the three
  that Windows newly runs, were also run uncached: pass.)
- The app against Postgres 16.15 (the embedded binaries, port 5433, the guide's sample), started with
  `--port 8766 --table sales.orders`: it printed `warehouse listening on 127.0.0.1:8766, catalogs
  [main, shop]` and an address ending `&table=sales.orders`, and served the site (HTTP 200). A Ctrl+C
  sent to its console (scripted, by `GenerateConsoleCtrlEvent`, not a keypress) stopped it: no
  process left, and its one temporary directory (`%TEMP%\datacube-*`) removed. `bazel` reported the
  exit as `-1073741510`, which is `0xC000013A`, as the guide says.
- `bazel run --run_in_cwd //warehouse:serve -- --data <a relative path>`, started in the repository
  root: the data directory was created there, as the guide says. `taskkill /t /f` on the `bazel`
  process stopped the launcher and the server with it.
- `bazel run //datacube:verify_app`: every line `ok:` (9 lines, among them `opened sales.orders
  Live` and `grouped by channel: 3 groups`), exit 0, no `server_native` process left. It stops the
  server with `taskkill /t /f`, which skips the server's shutdown hook (a Ctrl+C runs it), so each
  run leaves one `%TEMP%\datacube-*` directory behind; this one was removed by hand.
- **Not observed on Windows:** the browser tab that `--open` opens (`rundll32`), and the terminal's
  password prompt (with no `PGPASSWORD`). Both are left to a person at the desk.
