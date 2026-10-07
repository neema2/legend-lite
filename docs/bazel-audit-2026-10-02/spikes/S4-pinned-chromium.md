# Spike S4: a pinned Chromium for hermetic browser tests

Date: 2026-10-03. Branch: `spike/s4-pinned-chromium` (worktree `runs/spike-s4`), commit `fdca09dd8`. This branch is local only and has not been pushed.

## Decision

**GO with (B):** a per-platform `http_archive` of Playwright's `chrome-headless-shell` zip, pinned by sha256. Playwright reaches it through `PLAYWRIGHT_BROWSERS_PATH`, which points into runfiles.

The evidence:
- `//datacube:verify_smoke` now runs as a `js_test` under `bazel test` and passes 16 of 16 shapes on:
  - macOS arm64, in the darwin-sandbox;
  - Linux arm64, in a Debian 12 container.
- In both runs the browser was launched from the Bazel external repo. On macOS the host Playwright cache was blocked at the same time.

**(A) rules_playwright is NO-GO** for Playwright 1.63:
- The latest BCR version is 0.5.3. GitHub has 0.5.4 (2026-02), but it is not on BCR.
- Both versions generate download paths in the old form `builds/chromium/<rev>/chromium-headless-shell-<plat>.zip`.
- For revision 1243 those paths return 404 or 400 on all three Playwright hosts (evidence item 3).
- Playwright now downloads Chrome for Testing builds from `builds/cft/<browserVersion>/...`. The rules' generator has no CfT mapping in any release or on `main`.
- Other problems:
  - It depends on `aspect_bazel_lib` 2.x, while this repo uses `bazel_lib` 3.7.
  - It ships a prebuilt Rust CLI that runs in the repository rule.
  - Its documented example needs `tags = ["no-sandbox"]`.
- The idea is still worth reusing: derive the archives from the locked playwright-core. Our version is a small guard test (`//tools/browser:revision_test`), not a dependency.

## Verified revision

From the fetched npm package (`external/aspect_rules_js++npm+npm__playwright-core__1.63.0/package.tgz`, file `package/browsers.json`):

```
{ "name": "chromium-headless-shell", "revision": "1243", "browserVersion": "153.0.8010.12", "title": "Chrome Headless Shell" }
```

Revision **1243**, Chrome for Testing **153.0.8010.12**. This confirms the audit's inference. However, the audit's guessed URL (`builds/chromium/1243/...`) is wrong; the correct URLs are below.

- **URL scheme.** From playwright-core's `coreBundle.js`: `cftUrl("<plat>/chrome-headless-shell-<plat>.zip")` resolves to `https://cdn.playwright.dev/builds/cft/${browserVersion}/<plat>/chrome-headless-shell-<plat>.zip`.
- **Mirror.** Google's `storage.googleapis.com/chrome-for-testing-public/<ver>/...` serves byte-identical files. The linux64 sha256 is the same from both hosts. That gives a second URL for every archive.
- **One copy for both locks.** Both locks resolve to one package repository (`npm__playwright-core__1.63.0`). rules_js shares the package store between `npm` and `npm_query`, and only the `__links` repos differ. So the same `browsers.json` serves both apps.

| platform | zip | sha256 |
|---|---|---|
| mac-arm64 | 98.8 MB | `89d80a6d26ccd0ccfd51e22d9e1297283862af2b0cd91dce07459b35ca0059f2` |
| mac-x64 | 104.1 MB | `5c2eaa1aad62111bb5a70dd0889dd3093f3142277b8f78957a238257ee85f009` |
| linux64 | 119.8 MB | `a9da028861a0cf789ff25c2fed45f5f1aaf969ed9247835b6a7821a4f7af9d1d` |
| linux-arm64 | 120.3 MB | `d433c45172c7836e38124fe545f767b02210bfb43a6262f08a297473a8e91c99` |
| win64 | 120.2 MB | `7aec872f3090e639c4237467624ea863c20fe2878914c93a6556bdbb52aa6c4c` |

## Evidence

**1. Revision.** The JSON above came from:

```
tar -xzOf $(bazel info output_base)/external/aspect_rules_js++npm+npm__playwright-core__1.63.0/package.tgz package/browsers.json
```

**2. All five archives exist on both hosts, with the same sizes:**

```
200 content-length: 98831293  https://cdn.playwright.dev/builds/cft  mac-arm64/chrome-headless-shell-mac-arm64
200 content-length: 98831293  https://storage.googleapis.com/chrome-for-testing-public  mac-arm64/...
... (mac-x64, linux64, linux-arm64, win64 likewise)
```

**3. The rules_playwright path scheme is dead for 1243:**

```
400 https://cdn.playwright.dev/dbazure/download/playwright/builds/chromium/1243/chromium-headless-shell-mac-arm64.zip
404 https://cdn.playwright.dev/builds/chromium/1243/chromium-headless-shell-mac-arm64.zip
400 https://playwright.download.prss.microsoft.com/dbazure/download/playwright/builds/chromium/1243/chromium-headless-shell-mac-arm64.zip
```

**4. The directory layout Playwright expects.** This is the unpinned `js_binary` with an empty browsers path:

```
PLAYWRIGHT_BROWSERS_PATH=$PWD/runs/s4/empty-browsers ONLY=nulls bazel run //datacube:verify_smoke
browserType.launch: Executable doesn't exist at .../empty-browsers/chromium_headless_shell-1243/chrome-headless-shell-mac-arm64/chrome-headless-shell
```

- The http_archive therefore uses `add_prefix = "chromium_headless_shell-1243"`, and the repo root is the browsers path.
- This doubles as a free guard: a pin that drifts from the lock fails at launch with this message. It never silently runs a different browser.

**5. macOS arm64, `bazel test` in the sandbox, with the host Playwright cache blocked:**

```
bazel test --local_resources=cpu=3 --jobs=3 //datacube:verify_smoke_test //tools/browser:revision_test \
  --sandbox_block_path=$HOME/Library/Caches/ms-playwright
INFO: 4 processes: 609 action cache hit, 2 internal, 3 darwin-sandbox.
//tools/browser:revision_test                                   (cached) PASSED in 0.1s
//datacube:verify_smoke_test                                             PASSED in 86.4s
```

From `test.log`:

```
pinned chromium: <output_base>/sandbox/darwin-sandbox/385/execroot/_main/bazel-out/darwin_arm64-fastbuild/bin/datacube/verify_smoke_test_/verify_smoke_test.runfiles/+http_archive+chromium_headless_shell_mac_arm64/chromium_headless_shell-1243/chrome-headless-shell-mac-arm64/chrome-headless-shell
browser: chromium 153.0.8010.12 from <...>/verify_smoke_test.runfiles/+http_archive+chromium_headless_shell_mac_arm64
...
16/16 shapes are sound
```

`test.outputs/summary.json` is written to `TEST_UNDECLARED_OUTPUTS_DIR`.

**6. The revision guard, for both locks** (on macOS and on Linux):

```
ok  datacube: playwright-core 1.63.0 wants chromium-headless-shell 1243 (153.0.8010.12); pinned 1243
ok  query: playwright-core 1.63.0 wants chromium-headless-shell 1243 (153.0.8010.12); pinned 1243
//tools/browser:revision_test   PASSED
```

**7. Linux arm64, Debian 12 container.** The setup:
- Image: `debian:bookworm-slim` plus bazelisk, git, gcc and python3; see `runs/s4/linux/Dockerfile` in the worktree.
- The worktree is mounted read-only. Caches live in the Docker volume `s4-bazel-cache`.
- Run with `--symlink_prefix=/ --lockfile_mode=off`.

In the image with only the build tools (stage `bare`), the first build compiled everything (147 s), then:

```
[pid=4795][err] .../+http_archive+chromium_headless_shell_linux_arm64/chromium_headless_shell-1243/chrome-headless-shell-linux-arm64/chrome-headless-shell:
  error while loading shared libraries: libglib-2.0.so.0: cannot open shared object file
//datacube:verify_smoke_test   FAILED in 0.8s
```

In stage `libs` (the 17 runtime packages below, with no fonts and no X server):

```
pinned chromium: /root/.cache/bazel/.../processwrapper-sandbox/2/.../verify_smoke_test.runfiles/+http_archive+chromium_headless_shell_linux_arm64/...
browser: chromium 153.0.8010.12 from .../+http_archive+chromium_headless_shell_linux_arm64
16/16 shapes are sound
//datacube:verify_smoke_test   PASSED in 89.7s
```

No `$HOME` cache was used: the container has never run `playwright install`.

**8. Test environment on macOS.** A probe `js_test` printed:
- `HOME=$TEST_TMPDIR`, which is good;
- `TMPDIR=/var/folders/.../T/`, the host's directory, passed through.

That means Playwright would put its browser profile in the host temp directory. `pinned-chromium.mjs` therefore sets `TMPDIR=TEST_TMPDIR`; `os.tmpdir()` reads the variable on every call.

## Linux system libraries

**Is the shell static?** No. The headless shell bundles only `libEGL`, `libGLESv2`, `libvk_swiftshader` and `libvulkan.so.1`. No fully static build exists, from either Chrome for Testing or Playwright, so that option is out.

**What `ldd` reports missing on Debian 12** (these libraries are linked at load time, i.e. DT_NEEDED):

```
libX11.so.6 libXcomposite.so.1 libXdamage.so.1 libXext.so.6 libXfixes.so.3 libXrandr.so.2 libasound.so.2
libatk-1.0.so.0 libatspi.so.0 libdbus-1.so.3 libexpat.so.1 libgbm.so.1 libgio-2.0.so.0 libglib-2.0.so.0
libgobject-2.0.so.0 libnspr4.so libnss3.so libnssutil3.so libxcb.so.1 libxkbcommon.so.0
```

**The minimal Debian 12 packages** (17 named; 30 debs with their dependencies; 6.9 MB):

```
libglib2.0-0 libnss3 libnspr4 libdbus-1-3 libatk1.0-0 libatspi2.0-0 libexpat1 libx11-6 libxcomposite1
libxdamage1 libxext6 libxfixes3 libxrandr2 libgbm1 libxcb1 libxkbcommon0 libasound2
```

- Compared with `playwright install --with-deps`, this set leaves out fonts, xvfb and GStreamer.
- The DataCube app ships its own web fonts, and the smoke test passed with no system fonts at all.
- A harness that screenshots text in system fonts would need `fonts-liberation`, for stable glyph metrics.

**Options compared:**

| option | verdict |
|---|---|
| **Pinned CI container image** (Debian 12 + the 17 packages, referenced by digest) | **Recommended.** Simplest, and it matches the plan's existing "Linux in a pinned container" item. Package versions are fixed by the digest. The same image also gives a local `docker run` repro. |
| **Hermetic .debs** (`rules_distroless` or http_files from snapshot.debian.org, plus `LD_LIBRARY_PATH` in `browser_test`) | Shown to work outside Bazel: 30 debs were downloaded but never installed, extracted with `dpkg-deb -x`, and with `LD_LIBRARY_PATH` set `ldd` showed 0 missing and `--dump-dom` worked. The catches: it still needs a compatible host glibc (2.36 or newer); it is per-arch work; and it means 30 pins to roll. Worth it later if tests must run on arbitrary Linux hosts. Not needed now. |
| **Fully static shell** | Does not exist. |

## Recommended design

### MODULE.bazel (as in the spike)

```python
CHROMIUM_HEADLESS_SHELL_REVISION = "1243"          # = playwright-core 1.63.0 browsers.json
CHROMIUM_HEADLESS_SHELL_VERSION = "153.0.8010.12"

_CHROMIUM_BUILD = """
filegroup(name = "files", srcs = glob(["chromium_headless_shell-*/**"]), visibility = ["//visibility:public"])
filegroup(name = "executable", srcs = ["chromium_headless_shell-%s/chrome-headless-shell-%s/chrome-headless-shell%s"], visibility = ["//visibility:public"])
"""

[http_archive(
    name = "chromium_headless_shell_" + plat.replace("-", "_"),
    add_prefix = "chromium_headless_shell-" + CHROMIUM_HEADLESS_SHELL_REVISION,
    build_file_content = _CHROMIUM_BUILD % (CHROMIUM_HEADLESS_SHELL_REVISION, plat, ".exe" if plat == "win64" else ""),
    sha256 = sha,
    urls = [
        "https://cdn.playwright.dev/builds/cft/%s/%s/chrome-headless-shell-%s.zip" % (VERSION, plat, plat),
        "https://storage.googleapis.com/chrome-for-testing-public/%s/%s/chrome-headless-shell-%s.zip" % (VERSION, plat, plat),
    ],
) for plat, sha in {"mac-arm64": "89d8…", "mac-x64": "5c2e…", "linux64": "a9da…", "linux-arm64": "d433…", "win64": "7aec…"}.items()]
```

- The archives are lazy: a host fetches only the one its `select` picks.
- For production, move this into a small module extension, `tools/browser/extensions.bzl`, so that MODULE.bazel holds a single `chromium.pin(revision, version, sha256s)` line.

### `//tools/browser` (in the spike)

- **`config_setting`s, one per platform, plus two aliases:**
  - `:chromium_headless_shell`, which is all the files;
  - `:chromium_headless_shell_executable`.

  Both use `select` over `@platforms`.
- **`pinned-chromium.mjs`, imported before `playwright`:**
  - resolves `PINNED_CHROMIUM` (an rlocationpath) against `JS_BINARY__RUNFILES`;
  - sets `PLAYWRIGHT_BROWSERS_PATH` to the repo root;
  - sets `TMPDIR` to `TEST_TMPDIR`;
  - sets `PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS=1`, because Playwright's ldd check is keyed to Ubuntu package names, and the launch itself is the real check;
  - throws if it is under a test but was not given a pin;
  - is a no-op under `bazel run`, so the dev js_binaries keep working.
- **`revision_test`:** checks that each lock's `browsers.json` revision equals the pinned directory. It is fast, launches nothing, and covers datacube and query.
- **The `browser_test` macro:**

```python
def browser_test(name, entry_point, data = [], env = {}, tags = [], size = "medium", **kwargs):
    js_test(
        name = name,
        entry_point = entry_point,
        data = data + ["//tools/browser:chromium_headless_shell",
                       "//tools/browser:chromium_headless_shell_executable",
                       "//tools/browser:pinned_chromium"],
        env = dict({"PINNED_CHROMIUM": "$(rlocationpath //tools/browser:chromium_headless_shell_executable)",
                    "PLAYWRIGHT_BROWSERS_PATH": "/nonexistent-use-pinned-chromium"}, **env),
        no_copy_to_bin = ["//tools/browser:chromium_headless_shell",
                          "//tools/browser:chromium_headless_shell_executable"],  # ~200 MB stays a runfiles symlink
        size = size,
        tags = tags + ["browser"],
        **kwargs
    )
```

Two things the spike found that the plan should note:
- **`no_copy_to_bin` is required.** Without it, rules_js refuses external files in `data`, or would copy the whole browser into bazel-bin.
- **The import-first module is needed** because Playwright reads `PLAYWRIGHT_BROWSERS_PATH` when its module loads. A cleaner alternative to the explicit import in every harness is `node_options = ["--import=..."]`, but it needs an absolute or cwd-relative path, which was not tested.

**Production additions:**
- a shared `demo/harness.mjs` with `serve(root)`, which serves on port 0 at 127.0.0.1 and returns the port, and `outPath(name)`, which writes into `TEST_UNDECLARED_OUTPUTS_DIR`;
- per-sample sharding (`shard_count`, which reads `TEST_SHARD_INDEX`) for smoke and features.

### Query and site share the same browser

- **Query.** `//query:verify` uses `//query:node_modules/playwright`, the second lock. Its playwright-core resolves to the same package repo, the same `browsers.json` and the same revision. It needs only `browser_test` and the import line. `revision_test` already guards the query lock.
- **Site.** `//site:verify` reaches into `../datacube/node_modules` with `createRequire`. It keeps that `data` dependency (`//datacube:node_modules/playwright`) and becomes a `browser_test` with the import line. Better, the plan item to unify Playwright versions (1.5) gives it its own link, or a shared `//tools/browser:playwright` js_library that re-exports one copy.
- **The rule for all three:** every Playwright copy in the repo must resolve to the revision `revision_test` checks. Add each new lock to its `LOCKS` list.

### CI changes

- Delete `//datacube:install_browser`, `demo/install-browser.mjs`, the `bazel run //datacube:install_browser -- --with-deps` step, and the `browser-ci` tag loop.
- Browser tests become ordinary members of `bazel test //...`, or of `//gates:browser`.
- The Linux jobs run in the pinned container image, Debian 12 plus the 17 packages, referenced by digest. No `sudo apt` step at run time.
- macOS needs nothing.
- The Bazel repository cache (`--repository_cache`) holds the roughly 100 to 120 MB zip, so the browser is not downloaded again on every CI run.

## Risks

- **Download size.** About 100 to 120 MB per platform per revision bump. It is fetched once per cache. If CI has no repository or disk cache, every run downloads it. Mitigation: keep the repository cache, plus the mirror URL.
- **The CDN path is internal to Playwright.** `builds/cft/<ver>/...` is internal and has changed before (from `builds/chromium/<rev>`), which is what broke rules_playwright. Mitigation: Google's public Chrome for Testing bucket is the stable second URL, and it is byte-identical.
- **Roll friction.** Every Playwright bump means 5 sha256 values plus the revision and version. `revision_test` fails loudly, so the bump cannot be silent. A `bazel run //tools/browser:roll` script could print the new block from `browsers.json`.
- **Windows not run.** The win64 archive is pinned but untested in this spike. The `.exe` name is handled; the rules_js launcher on Windows is not verified.
- **Linux x86_64 not run.** Only linux-arm64 ran, in Docker on Apple silicon. CI is linux64. The package list is architecture-independent on Debian, but run it once on an amd64 runner.
- **The Linux sandbox was processwrapper, not linux-sandbox.** The container had no user namespaces. On a CI VM with linux-sandbox, Chromium runs with `--no-sandbox` (Playwright's default for headless shell), so no sandbox nesting is expected. Confirm on the first CI run.
- **Fonts.** The no-fonts setup is fine for smoke because the app ships its own web fonts. Screenshot-diffing harnesses (`shots`, `verify_charts` SHOTS) may want `fonts-liberation` in the image.
- **Test sizes.** smoke takes about 87 s, so it is `size = "large"`. `verify_features` (5k lines) needs sharding before it fits a timeout.

## Effort to convert all CI harnesses

The 10 datacube `_CI_HARNESSES` plus `//query:verify` and `//site:verify`:

| item | estimate |
|---|---|
| `//tools/browser` (extension, macro, revision test, roll script), CI container image and workflow edits | 1 day |
| Shared `harness.mjs` (port 0, outputs, tmp), then the mechanical harnesses: smoke, wasm_browser, charts, cubes, page, upload, run_stress | 1 to 1.5 days |
| verify_remote (fixed :8741 on all interfaces) and verify_real_data (cwd Parquet writes into runfiles), per audit items N4 and N6 | 0.5 day |
| verify_features: sharding, and dropping the fixed tmp name and env knobs | 1 day |
| query:verify (random ports become port 0; `.scratch` becomes TEST_TMPDIR; runfiles library) and site:verify | 1 day |
| Linux amd64 and Windows validation; flake soak (3 or more repeated runs) | 0.5 to 1 day |
| **Total** | **about 5 to 6 days**, which fits inside the plan's Phase 4.1 (L) |

## Open questions

1. Should the pin live in MODULE.bazel as a list comprehension (as in the spike), or in a module extension that reads the revision and version from the locked `browsers.json`? With the extension, only the sha256 values are hand-edited.
2. Is Windows browser CI in scope? If so, run `browser_test` once on a Windows runner (rules_js launcher, `.exe`, runfiles symlinks).
3. Which container base: Debian 12 (tested), or Ubuntu 24.04 to match `ubuntu-latest`? Rerun the `ldd` probe for whichever is chosen.
4. Should `fonts-liberation` go into the image now, so future screenshot tests have stable glyph metrics, or only when a test needs it?
5. Is an explicit import in each harness acceptable, or should the preload go through `node_options --import` (cwd-dependent; not tested)?
6. Unify the two Playwright copies (plan 1.5), or keep two locks guarded by `revision_test`? Both work. Unifying removes the `site/verify.mjs` reach-around.
