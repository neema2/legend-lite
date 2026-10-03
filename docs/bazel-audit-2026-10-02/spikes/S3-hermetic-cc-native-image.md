# Spike S3: native-image linked by a hermetic, Bazel-declared C toolchain

Date: 2026-10-03. Branch: `spike/s3-hermetic-cc` (worktree `runs/spike-s3`), commit `d1a4d7deb`. The branch is local only and has not been pushed.

Plan row: Phase 1.3, "C toolchain for `native_image`" (`BAZEL_FIRST_CLASS_PLAN_2026_10_02.md`).

## Decision

| Platform | Decision | Toolchain | Tested? |
|---|---|---|---|
| macOS arm64 | **GO, with one condition: an SDK archive we are allowed to fetch** | `toolchains_llvm` 1.11.0 (LLVM 20.1.8 clang + `ld64.lld`), with `sysroot` = a macOS SDK fetched by Bazel | Built and `tests_native` passed (101 passed, 8 skipped, 0 failed). The Command Line Tools were blocked from the sandbox during the run. |
| Linux aarch64 | **GO** | `toolchains_llvm` 1.11.0 (clang + `ld.lld`), Chromium's Debian bullseye sysroot, zlib from BCR as `static_zlib` | Built in a Debian 12 container with no gcc, no libc headers and no zlib headers. The same build fails there with the host setup. `tests_native` crashes at server start, and it crashes **the same way with host gcc** (control run below), so the crash is not caused by the toolchain. |
| Linux x86_64 | **GO (inferred, not run)** | Same, with the bullseye amd64 sysroot | Untested. The coordinator stopped x86_64 emulation because the machine was out of resources. The configuration is the same with a different sysroot and LLVM archive. |
| Windows x64 | **NO-GO for now: keep host MSVC** | A theoretical path exists (clang-cl presented as `cl.exe`, plus `lld-link` and an xwin-style CRT/SDK). It is blocked by Microsoft's licence and GraalVM's MSVC-only support. | Not testable here. Analysis is below. |

**`hermetic_cc_toolchain` (zig) is NO-GO for native-image** on every platform:
- **Linux:** GraalVM always passes `-z text` straight to the compiler driver, and `zig cc` rejects it (`error: unsupported linker extension flag: -z text`). This is true in every zig release, 0.14.1 through master. Evidence item 4.
- **macOS:** zig has no SDK frameworks, which is upstream issue #10.
- **Windows:** zig targets mingw, but GraalVM's static JDK libraries are built with MSVC and need the MSVC ABI.

**Does rules_graalvm 0.12.0 use Bazel's resolved cc toolchain?** Yes, but only partly:
- `internal/native_image/toolchain.bzl` calls `find_cpp_toolchain(ctx)` and adds `cc_toolchain.all_files` to the inputs.
- It passes the C-compile tool path as `--native-compiler-path=`.
- It sets `PATH` to the tool directories plus `/bin:/usr/bin`, or `C:\Windows\System32` on Windows.
- It passes **none of the toolchain's flags**: no sysroot, no `-fuse-ld`, no `--target`.
- native-image then runs that compiler with its own arguments, inside its own temp directory (`/tmp/SVM-*`). It runs it twice: once to compile its C "query" probes, once to link.

So today's host dependency comes from the toolchain Bazel resolves, which is rules_cc's autodetected `local_config_cc`:
- On macOS that is `cc_wrapper.sh`, which runs `/usr/bin/clang`, an xcrun shim into the CLT.
- On Linux it is gcc, and the build fails at analysis if gcc is missing (evidence item 3).

The rule never calls `xcrun` or `cl.exe` itself. On Windows, GraalVM's own driver looks for `cl.exe` on `PATH`, or runs vswhere and vcvarsall.

## What changed on the spike branch

1. **`MODULE.bazel`** adds:
   - `toolchains_llvm` 1.11.0 and `zlib` 1.3.2.bcr.2;
   - three `llvm.sysroot` tags;
   - three registered toolchains: `cc-toolchain-aarch64-darwin`, `cc-toolchain-aarch64-linux` and `cc-toolchain-x86_64-linux`;
   - the sysroot archives. Excerpt:

```starlark
bazel_dep(name = "toolchains_llvm", version = "1.11.0")
bazel_dep(name = "zlib", version = "1.3.2.bcr.2")

llvm = use_extension("@toolchains_llvm//toolchain/extensions:llvm.bzl", "llvm")
llvm.toolchain(llvm_version = "20.1.8")
llvm.sysroot(label = "@macos_sdk//:sysroot", targets = ["darwin-aarch64"])
llvm.sysroot(label = "@linux_sysroot_arm64//:sysroot", targets = ["linux-aarch64"])
llvm.sysroot(label = "@linux_sysroot_amd64//:sysroot", targets = ["linux-x86_64"])
use_repo(llvm, "llvm_toolchain")
register_toolchains(
    "@llvm_toolchain//:cc-toolchain-aarch64-darwin",
    "@llvm_toolchain//:cc-toolchain-aarch64-linux",
    "@llvm_toolchain//:cc-toolchain-x86_64-linux",
)

# Chromium's sysroots: the URL is <bucket>/<Sha256Sum> (no file name), so type is needed.
http_archive(
    name = "linux_sysroot_arm64",
    build_file_content = 'filegroup(name = "sysroot", srcs = glob(["usr/include/**", "usr/lib/**", "lib/**"], exclude = ["lib/systemd/**", "usr/lib/systemd/**"]), visibility = ["//visibility:public"])',
    sha256 = "c7176a4c7aacbf46bda58a029f39f79a68008d3dee6518f154dcf5161a5486d8",
    type = "tar.xz",
    urls = ["https://commondatastorage.googleapis.com/chrome-linux-sysroot/c7176a4c7aacbf46bda58a029f39f79a68008d3dee6518f154dcf5161a5486d8"],
)
# linux_sysroot_amd64: sha256 52d61d4446ffebfaa3dda2cd02da4ab4876ff237853f46d273e7f9b666652e1d, same shape.
# macos_sdk: the spike uses file:// to a local tarball of the CLT's MacOSX15.5.sdk (minus Ruby.framework, which
#            has a symlink loop that breaks glob(), and usr/share/man). Production needs a real, permitted URL.
```

2. **`warehouse/BUILD.bazel`**, on `server_native`:
   - `static_zlib = "@zlib"`: rules_graalvm already supports this on Linux. `-lz` then finds a libz.a built from source.
   - `c_compiler_option = ["-fuse-ld=lld"]` everywhere except Windows. The hermetic clang then links with the lld next to it, not with the host `ld`.

3. **`third_party/rules_graalvm_command_line_tools.patch`**, extended.
   - `toolchain.bzl` now returns `cc_toolchain.sysroot`.
   - `rules.bzl` gets a new branch, tried first. When the toolchain has a sysroot, native-image runs through a one-line `run_shell` that appends `-H:CCompilerOption=--sysroot=$PWD/<sysroot>`.
   - The path must be absolute at run time because native-image compiles and links in its own temp directory.
   - `CCompilerOption` reaches both the query compile and the link: GraalVM's `CCompilerInvoker.createCompilerCommand` puts it straight after the compiler path.
   - The Xcode and plain branches are unchanged.

## Evidence

All runs used `--local_resources=cpu=3 --jobs=3`. Docker ran with `--cpus=3` and an 8 GB VM.

### 1. The rule's source (rules_graalvm 0.12.0)

```
internal/native_image/toolchain.bzl   cc_toolchain = find_cpp_toolchain(ctx); transitive_inputs.append(cc_toolchain.all_files)
                                      c_compiler_path = cc_common.get_tool_for_action(..., C_COMPILE_ACTION_NAME)
                                      env["PATH"] = <tool dirs> + /bin:/usr/bin      (Windows: + C:\Windows\System32)
internal/native_image/builder.bzl     args.add(c_compiler_path, format = "--native-compiler-path=%s")
                                      args.add("-H:-CheckToolchain")   (check_toolchains defaults to False)
                                      static_zlib (CcInfo, Linux only) -> -H:CLibraryPath=<dir of libz.a>
```

### 2. Baseline aquery on macOS, before the change

```
Environment: [PATH=/usr/bin:external/rules_cc++cc_configure_extension+local_config_cc:/bin, ZERO_AR_DATE=1]
'--native-compiler-path=external/rules_cc++cc_configure_extension+local_config_cc/cc_wrapper.sh'
```

The host binaries record Apple's linker:
- `bazel-bin/warehouse/server_native-bin` in the main checkout and in `runs/bazel-audit/runs/pr14-wt`;
- `otool -l` shows `LC_BUILD_VERSION ... sdk 15.5, tool 3 (LD) version 1167.5`, which is Apple ld64 from the CLT.

### 3. Linux aarch64, host setup on a gcc-less image: FAILS

The image is `debian:bookworm-slim` plus `ca-certificates curl git` and bazelisk (`runs/spike-s3/runs/s3-docker/Dockerfile`). It contains no `cc`, `gcc`, `clang`, `ld`, `as` or `ar`, and no `/usr/include/stdio.h` or `zlib.h`. The only zlib on it is `libz.so.1`.

```
$ bazel build //warehouse:server_native        # origin/main config
Auto-Configuration Error: Cannot find gcc or CC; either correct your path or set the CC environment variable
ERROR: //warehouse:server_native depends on @@rules_cc++cc_configure_extension+local_config_cc//:cc-compiler-aarch64 ... which failed to fetch
ELAPSED=14s RC=1
```

### 4. Linux aarch64 with zig (`hermetic_cc_toolchain` 4.3.0, zig 0.15.2): FAILS at link

The toolchain resolved and zlib compiled. native-image analysed and compiled the image, which used 1.46 GB RSS. Then the link failed:

```
Linker command executed:
<execroot>/external/hermetic_cc_toolchain++toolchains+zig_config/tools/aarch64-linux-gnu.2.28/c++ -z noexecstack -z text -Wl,--gc-sections ...
error: unsupported linker extension flag: -z text
```

The `-z text` comes from GraalVM, not from us. GraalVM's `CCLinkerInvocation.java` (vm-25.0.2) adds it unconditionally: `additionalPreOptions.add(SpawnIsolates.getValue() ? "text" : "notext")`. zig's `src/main.zig` accepts `notext` but not `text`, in 0.14.1, 0.15.1 and master.

The only flag that avoids it is `-H:-SpawnIsolates`, and that changes the runtime. Patching around it would need a wrapper compiler that drops the flag, which means our own cc toolchain. Rejected.

### 5. Linux aarch64 with LLVM + sysroot: BUILDS

Before the image needed anything extra, the first attempt failed:

```
clang: error: unable to execute command ...   ->   bin/ld.lld: error while loading shared libraries: libxml2.so.2
```

LLVM's official Linux aarch64 release links `lld` dynamically against `libxml2.so.2`. `clang-20` itself needs only libc, libm and libz. So the image gets a single runtime library, `libxml2` (`Dockerfile.libxml2`), and still has no compiler, binutils, headers or zlib-dev:

```
$ docker run ... legend-s3-nocc-xml:arm64 sh -c 'command -v cc gcc ld'   ->  no cc/gcc/ld
$ bazel build //warehouse:server_native
Finished generating 'server_native-bin' in 2m 41s.   Peak RSS: 1.49GB
INFO: Elapsed time: 238.868s ... ELAPSED=240s RC=0          (includes fetching LLVM 20.1.8 and the sysroot)
```

The binary shows where its toolchain came from:

```
.comment: "Linker: LLD 20.1.8", "GCC: (Debian 10.2.1-6) 10.2.1" (bullseye's crt objects from the sysroot)
NEEDED:   libdl.so.2 libpthread.so.0 librt.so.1 libc.so.6   -- no libz.so.1: zlib is linked statically from @zlib
size:     24,660,600 bytes
```

The host-gcc control binary is different:
- `.comment` reads `GCC: (Debian 12.2.0-14+deb12u1)`;
- it has `NEEDED libz.so.1`;
- it is 23,267,512 bytes.

The hermetic binary is linked against glibc 2.31 (bullseye), so it also runs on older distributions than the build host.

### 6. Linux aarch64 `tests_native`: crashes, but the crash is not the toolchain's

The hermetic run fails in 13 s:
- `server_native-bin exited before listening (exit 99)`;
- `SegfaultHandler caught a segfault ... si_code: 2`, with `R30` in `CodeSynchronizationOperations.clearCache`.

To check the cause, a **control** ran on the same machine with the unmodified origin/main config, host `gcc libc6-dev zlib1g-dev` (`Dockerfile.gcc`), and the same `bazel test //warehouse:tests_native`:
- the same 6 segfaults;
- the same frame (`clearCache`) and the same `si_code: 2`;
- `ELAPSED=135s RC=3`; the native-image step took 1m 43s.

So the native server crashes at startup on Linux aarch64 under Docker Desktop's linuxkit kernel (6.12.76, 4 KB pages), **whichever toolchain links it**. This is a separate, pre-existing issue (open question 1). The toolchain swap does not change it.

### 7. macOS arm64 with LLVM + fetched SDK: BUILDS and PASSES, with the CLT blocked

The aquery after the change. `PATH` is now the LLVM directory plus `/bin:/usr/bin`, and `/usr/bin` holds only the xcrun shims that the block makes fail:

```
Environment: [PATH=external/toolchains_llvm++llvm+llvm_toolchain/bin:/bin:/usr/bin, ZERO_AR_DATE=1]
Command Line: (exec /bin/bash -c 'tool="$1"; sysroot="$PWD/$2"; shift 2; exec "$tool" "$@" "-H:CCompilerOption=--sysroot=$sysroot"' ''
    external/rules_graalvm++graalvm+graalvm/bin/native-image
    external/+http_archive+macos_sdk/
    --no-fallback ... -H:-CheckToolchain -cp ... -Ob
    '--native-compiler-path=external/toolchains_llvm++llvm+llvm_toolchain/bin/cc_wrapper.sh'
    '-H:CCompilerOption=-fuse-ld=lld' ... --link-at-build-time '--enable-native-access=ALL-UNNAMED'
    '-EZERO_AR_DATE=1' '-EPATH=external/toolchains_llvm++llvm+llvm_toolchain/bin:/bin:/usr/bin')
```

The CLT were blocked from every sandboxed action. A Mac cannot run without `/usr/bin`, so blocking is the closest thing to hiding the tools:

```
--sandbox_block_path=/Library/Developer/CommandLineTools --sandbox_block_path=/usr/bin/ld
--sandbox_block_path=/usr/bin/clang --sandbox_block_path=/usr/bin/xcrun
```

A probe genrule confirms the block works. With the flags:

```
/usr/bin/clang --version  -> xcode-select: note: No developer tools were found ... rc=1
/usr/bin/ld -v            -> same, rc=1
ls /Library/Developer/CommandLineTools/usr/bin/ld -> Operation not permitted
```

Without the flags, the same probe printed `Apple clang version 17.0.0 ... InstalledDir: /Library/Developer/CommandLineTools/usr/bin`.

**Control under the block.** The host toolchain (`--extra_toolchains=@@rules_cc++cc_configure_extension+local_config_cc_toolchains//:cc-toolchain-darwin_arm64`) failed in 16 s, at native-image's first C query compile:

```
Error compiling query code (RISCV64LibCHelperDirectives.c). Compiler command '<execroot>/external/rules_cc++cc_configure_extension+local_config_cc/cc_wrapper.sh -fuse-ld=lld ...'
output included error: [xcode-select: note: No developer tools were found ...]
```

**Hermetic, same block:**

```
bazel build //warehouse:server_native   -> Finished generating 'server_native-bin' in 1m 43s. Peak RSS 1.29GB. Elapsed 140s (Java deps cached). darwin-sandbox.
bazel test //warehouse:tests_native     -> PASSED in 107.7s (111 s wall). 109 found, 101 successful, 8 skipped, 0 failed.
```

The binary shows which linker made it:

```
otool -l: LC_BUILD_VERSION minos 15.0, sdk 15.0, ntools 1, tool 4 (LLD) version 20.1.8
```

That is toolchains_llvm's lld:
- It is not Apple ld64 (`tool 3`, 1167.5).
- It is not GraalVM's own bundled fallback `lib/svm/bin/ld64.lld`, which reports `LLD 20.1.4`.

```
otool -L: libSystem.B.dylib, Foundation, CoreFoundation, libobjc.A.dylib, libz.1.dylib   (resolved from the SDK's .tbd stubs)
size: 22,580,960 bytes   (host-linked build of the same tree in runs/bazel-audit/runs/pr14-wt: 22,387,248)
```

One side effect: each blocked `/usr/bin/clang` call in the probe and the control made macOS show its "install developer tools" prompt. The hermetic build made none.

### Times and sizes

| Run | native-image step | Wall | Binary |
|---|---|---|---|
| macOS hermetic (CLT blocked) | 1m 43s | 140 s build, 111 s test | 22,580,960 B |
| macOS host (pr14 worktree, same tree) | not re-measured | — | 22,387,248 B |
| Linux aarch64 hermetic (container) | 2m 41s | 240 s incl. LLVM and sysroot fetch | 24,660,600 B (static zlib) |
| Linux aarch64 host gcc (control) | 1m 43s | 135 s test incl. build | 23,267,512 B |

The Linux timing gap is mostly noise. The hermetic run overlapped with a machine-wide overload that the coordinator flagged (load average around 80). Its peak RSS was the same as the control's.

## Windows: feasibility read

What GraalVM 25.0.2 itself requires, from its source at tag `vm-25.0.2`:

- **The driver.** `substratevm/src/com.oracle.svm.driver/.../WindowsBuildEnvironmentUtil.java` runs `where cl.exe`. If `cl.exe` is on `PATH`, it uses it as is. Otherwise it locates Visual Studio ≥ 17.6 with `vswhere.exe` (component `Microsoft.VisualStudio.Component.VC.Tools.x86.x64`), runs `vcvarsall.bat x64` and imports its environment.
- **The compiler check.** `CCompilerInvoker.WindowsCCompilerInvoker` parses the `Microsoft (R) C/C++ Optimizing Compiler` banner and `_MSC_FULL_VER`, but only when `CheckToolchain` is on. rules_graalvm passes `-H:-CheckToolchain`, so a compiler that is not cl, such as clang-cl, would not be rejected at this step.
- **The link.** `CCLinkerInvocation` (Windows) uses cl-style arguments: `/MD`, `/Fe`, `/link /FILEALIGN:4096 /IMPLIB:...`, and the system libraries `advapi32.lib ws2_32.lib secur32.lib iphlpapi.lib userenv.lib mswsock.lib`. It links GraalVM's static JDK `.lib`s, which are built with MSVC.

A hermetic candidate would look like this:
- LLVM's `clang-cl` exposed under the name `cl.exe`. Clang chooses cl mode from the program name, which also satisfies `where cl.exe`.
- `-fuse-ld=lld`, so `/link` reaches `lld-link`.
- The MSVC CRT plus the Windows SDK headers and libs, fetched with an xwin-style splat (github.com/Jake-Shadle/xwin). The toolchain would expose these through `INCLUDE` and `LIB`, or through `/winsysroot`.

Why it is NO-GO for now:
- **Licence.** The CRT and SDK come under Microsoft's licence. xwin requires `--accept-license` and downloads them from Microsoft per user. We cannot mirror them in a public repository's cache. A per-developer fetch is possible, but it puts a licence click into `bazel build`.
- **Support.** GraalVM documents and tests MSVC only. Two things are unverified:
  - whether GraalVM's MSVC-built static libraries link cleanly with `lld-link` (for example, any `/GL` LTCG objects would not);
  - whether the binary behaves the same at run time.
- **Tooling.** Wiring would need either `toolchains_llvm`'s Windows support or our own `cc_toolchain`, and rules_graalvm's `PATH` handling would need to find a `cl.exe` that is really clang-cl. None of this was verified here. It needs a Windows machine.
- **Upside.** Windows is already a declared prerequisite: VS 2022 Build Tools are in the README, and CI has a Windows native lane.

Recommendation: on Windows, keep MSVC from the host, but make it explicit. Native targets stay `target_compatible_with` as today. Add a configure-time check that fails with a clear message when `cl.exe`/vswhere is missing, instead of failing deep in native-image. Record it in the plan as the one remaining host C dependency.

## Recommended changes (production version of the spike)

1. **MODULE.bazel.** Use `toolchains_llvm` + `zlib` exactly as on the spike branch. Changes from the spike:
   - The macOS SDK comes from a real URL we are allowed to use (see risks), pinned by sha256. Prune `Ruby.framework`, which breaks `glob`, and `usr/share/man`. That leaves about 70 MB compressed.
   - Mirror the Chromium sysroots, for example in a GitHub release of the repository, with the Chromium bucket as the second URL. They are about 18 MB (arm64) and similar (amd64).
   - Keep the `register_toolchains` list explicit: darwin-aarch64, linux-aarch64, linux-x86_64. Add `cc-toolchain-x86_64-darwin` if Intel Macs matter. Windows keeps MSVC. On Windows, rules_cc's autodetected toolchain still resolves, because none of these match a Windows target.
   - Consider `--repo_env=BAZEL_DO_NOT_DETECT_CPP_TOOLCHAIN=1` on macOS and Linux (`build:macos` and `build:linux` in `.bazelrc`). Then a missing hermetic match fails loudly instead of silently falling back to the host.
2. **warehouse/BUILD.bazel.** Add `static_zlib = "@zlib"` and `c_compiler_option = ["-fuse-ld=lld"]` (not on Windows) to `native_image`. If more native targets appear, put these in a small macro.
3. **The rules_graalvm patch.** Keep it, rename it (for example `rules_graalvm_sysroot.patch`), and send the sysroot hunk upstream (sgammon/rules_graalvm). It is generic: "pass `cc_toolchain.sysroot` to native-image". A fuller upstream fix would also forward the toolchain's link flags (`-fuse-ld`, `--target`). Then `c_compiler_option` would not be needed either.
4. **CI.** Linux runners no longer need `gcc`/`zlib1g-dev`, but they need `libxml2` (see below). macOS runners no longer need Xcode or CLT selection for this target. git still needs the CLT on a Mac, but no build action does.
5. **A guard.** Add a CI step, or a `--config=hermetic-cc` in `.bazelrc`, that sets the four `--sandbox_block_path` flags on macOS. The proof in evidence item 7 then runs continuously, and any regression to the host toolchain fails at once.

**Can our rules_graalvm patch be dropped?** No. It changes shape instead:
- The original CLT hunk handled "a Mac with CLT but no Xcode.app". With a sysroot toolchain the new branch runs first, so that hunk is no longer reached on macOS and could be dropped.
- But the sysroot branch is itself a patch. It is needed because rules_graalvm passes the compiler path but not the sysroot. The patch drops only if upstream accepts the sysroot passthrough.
- The branch order matters. The sysroot branch must come before the Xcode branch. Otherwise, on a Mac with Xcode.app installed, `apple_support.run` takes over, the sysroot is never passed, and the build goes back to Xcode's SDK.

## What stays host-dependent, and why

- **Windows: MSVC, the Windows SDK and vcvars.** See the feasibility read above.
- **Linux: `libxml2.so.2` at build time.** LLVM's prebuilt `lld` links it dynamically. It is a runtime library, not a toolchain, but it is a host dependency. Ways to remove it:
  - an LLVM archive whose `lld` is built without libxml2 (Chromium's clang packages, or one we build);
  - a different LLVM release whose `lld` has no libxml2 dependency (not checked);
  - accept it and document it. It is present on most images; `debian:bookworm-slim` lacks it.
- **Linux: the host glibc runs the build tools.** This covers clang, lld, GraalVM and the JDK, as with any prebuilt binary. The target libc comes from the sysroot.
- **macOS: `/bin/bash`, `/usr/bin` coreutils, `/usr/lib/libSystem`.** toolchains_llvm's `cc_wrapper.sh` and our `run_shell` use `bash`, `realpath`, `mktemp` and `dirname`. These are OS components, not developer tools, and they are not blocked.
- **macOS: the SDK's contents.** The SDK is Apple's, whoever hosts it. The build no longer reads it from the machine, but it has to come from somewhere (see risks).
- **Runtime libraries for `tests_native`.** DuckDB's `libduckdb_java.so` needs `libstdc++` on Linux. That is the test's runtime, not the link.

## Risks

1. **macOS SDK redistribution.** Apple's Xcode and SDK licence does not allow redistributing the SDK. Hosting `MacOSX15.5.sdk.tar.gz` publicly, from a public repository, is legally risky. Options:
   - a private or internal mirror;
   - a per-developer repository rule that copies the local CLT SDK into an `http_archive`-like repository and checks its sha256. This is declared and checked, but not fetched. It still needs the CLT installed, though it no longer uses the CLT's compiler or linker;
   - an existing community mirror, which carries the same legal question.

   This is the main thing standing between this spike and a production GO on macOS.
2. **lld as the Mach-O linker.** GraalVM still adds `-Wl,-no_compact_unwind`; its own comment says lld does not understand it. LLVM 20.1.8's `ld64.lld` accepted it here, and the tests passed. But we now link the shipped Mac binary with lld instead of Apple ld64, which GraalVM tests with. `LC_BUILD_VERSION` also reports `sdk 15.0` instead of `15.5`. That looks harmless (the minimum OS is 15.0 either way), but it is a difference.
3. **glibc baseline.** Linking against bullseye's glibc 2.31 changes the binary: `libdl`/`libpthread`/`librt` become separate `NEEDED` entries, and zlib is linked statically. This is better for portability but should be checked against the deploy targets.
4. **Upstream drift.** If rules_graalvm changes how it runs native-image, our patch must be rebased. toolchains_llvm 1.11.0 is recent; its README already describes LLVM 23 with a darwin linker caveat.
5. **Download size.** The LLVM 20.1.8 archive is about 1 GB unpacked per host. It is fetched once, into Bazel's repository cache, and is shared across worktrees through the repo contents cache.
6. **x86_64 Linux is untested.** The configuration is the same with a different sysroot and LLVM archive, but the libxml2 behaviour and native-image's x86 link flags were not observed.

## Effort estimate

The spike took about half a day. That included two dead ends (zig, and the Chromium sysroot URL scheme) and the libxml2 discovery.

Productionising:
- **S:** MODULE and BUILD changes, mirrors for the two Linux sysroots, the `.bazelrc` guard config. About 0.5 day.
- **M:** the macOS SDK sourcing decision and its implementation. This is a legal call first, then 0.5–1 day for either the local-SDK repository rule or the mirror.
- **S:** upstream PR to rules_graalvm for the sysroot passthrough, with a test. About 0.5 day, plus review latency.
- **S–M:** CI changes: drop gcc/zlib-dev and Xcode selection, add libxml2 or swap the LLVM archive, and the blocked-CLT guard lane on macOS. About 0.5–1 day.
- **Windows:** keep MSVC. Add the explicit preflight check, about 0.25 day. A real clang-cl + xwin attempt would be **L** and needs a Windows machine and a licence decision.

Total for macOS and Linux: about 2–3 days, of which 1 day is gated on the SDK decision.

## Open questions

1. **The Linux aarch64 crash.** The native server segfaults at startup in `CodeSynchronizationOperations.clearCache` under Docker Desktop on Apple silicon. It happens with host gcc too, so it is not a toolchain issue. Does it reproduce on a real arm64 Linux host or a GitHub `ubuntu-24.04-arm` runner? Until that is known, `tests_native` has not been shown to pass on Linux aarch64 with either toolchain.
2. **The macOS SDK source.** Mirror, local-SDK repository rule, or keep the CLT SDK through toolchains_llvm's default `xcrun --show-sdk-path` sysroot? The last option still uses the hermetic compiler and linker, but reads SDK files from the host.
3. **libxml2.** Accept it on Linux, or switch to an LLVM build without it?
4. **Upstream.** Will rules_graalvm take the sysroot passthrough, or a more general "forward the cc toolchain's flags" change?
5. **x86_64.** Confirm Linux x86_64 on a native x86 runner. Decide whether `cc-toolchain-x86_64-darwin` is needed.
6. **Windows.** Is a one-time Microsoft licence acceptance per developer acceptable? Only if yes is the clang-cl + xwin experiment worth running on a Windows machine.

## Artefacts (local, not committed; `runs/` is gitignored)

- `runs/spike-s3/runs/s3-docker/`:
  - the images: `Dockerfile` (no C toolchain), `Dockerfile.libxml2` and `Dockerfile.gcc` (control);
  - `run.sh` and `bz.sh`;
  - `logs/`: `a1-baseline-host`, `a2-zig-build`, `a3-llvm-build`, `a4-llvm-build-libxml2`, `a5-tests-native`, `c1-gcc-control-tests-native`, `m0-mac-host-blocked`, `m1-mac-llvm-build` and `m2-mac-tests-native`.
- `runs/spike-s3/runs/s3-sdk/MacOSX15.5.sdk.tar.gz`: the local SDK stand-in. Do not publish.
- Docker volume `s3-bazel-arm64` (kept). No spike containers are running.
