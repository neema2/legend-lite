"""node_test: the ONE way a JavaScript test target is declared in this repository (Bazel workplan P1-23).

Every node:test file runs with the same settings, so no target spells them itself:

  * TypeScript run directly, types stripped (--experimental-strip-types), its warning silenced;
  * two reporters: `spec` for the log, and //tools/js:strict_reporter for the EXIT CODE (Node counts a suite whose
    body throws as `# fail 0` and exits 0; the strict reporter fails the process on any `test:fail`);
  * a pinned locale and clock, LANG=C, LC_ALL=C, TZ=UTC, so a verdict never depends on the desk; a target that
    tests another time zone says so in `env`, which wins. The zone also goes in LEGEND_TZ, which //tools/js:zone
    (preloaded) assigns to TZ: Windows' launcher drops TZ itself;
  * `wasm = True` adds the WebAssembly planner as one directory (//wasm:planner_dir), named in WASM_PLANNER by its
    runfiles path, and the flag Node needs to load it;
  * //tools/js:runfiles, through which a test finds its inputs (Bazel workplan P1-24): each input the BUILD file
    names in `env` with $(rlocationpath ...), never by `../..` arithmetic or the working directory.

ONE ENTRY POINT PER TARGET (A28): each test file is its own target and process, which is what makes a test's
global DOM mutation (jsdom on globalThis) safe. js_test takes one entry point; the convention this macro carries is
that the entry point is one test file and node_options loads no other (nothing checks what a file imports).
Every js_test comes from node_test, browser_test (built on it) or tsc_test: guards_package refuses any other
(tools/guards/defs.bzl).
"""

load("@aspect_rules_js//js:defs.bzl", "js_test")

_REPORTER = "tools/js/strict-reporter.mjs"

_ZONE = "tools/js/zone.mjs"

def node_test(name, entry_point, data = [], wasm = False, env = {}, node_options = [], **kwargs):
    """A js_test running one node:test entry point with the repository's settings.

    Args:
        name: the target.
        entry_point: the one test file (A28: one entry point per target).
        data: what the test reads.
        wasm: True to load the WebAssembly planner (//wasm:planner_dir, named in WASM_PLANNER).
        env: added over the pinned LANG/LC_ALL/TZ (a TZ lane overrides TZ here).
        node_options: added after the shared options.
        **kwargs: everything else js_test takes (size, chdir, tags, ...).
    """
    # the reporter and the zone preload by path from where the test runs: `chdir` (no test sets one since P3-29),
    # else the runfiles tree's main-repository directory, where rules_js runs a test
    chdir = kwargs.get("chdir")
    up = "/".join([".."] * len(chdir.split("/"))) if chdir else "."
    options = ["--experimental-strip-types", "--import=%s/%s" % (up, _ZONE)] + (["--experimental-wasm-exnref"] if wasm else []) + [
        "--disable-warning=ExperimentalWarning",
        "--test-reporter=spec",
        "--test-reporter-destination=stdout",
        "--test-reporter=%s/%s" % (up, _REPORTER),
        "--test-reporter-destination=stderr",
    ] + node_options
    pinned = {"LANG": "C", "LC_ALL": "C", "TZ": "UTC"}
    zone = (pinned | env)["TZ"]
    if wasm:
        pinned["WASM_PLANNER"] = "$(rlocationpath //wasm:planner_dir)"
    js_test(
        name = name,
        entry_point = entry_point,
        data = data + [Label("//tools/js:runfiles"), Label("//tools/js:strict_reporter"), Label("//tools/js:zone")] + ([Label("//wasm:planner_dir")] if wasm else []),
        env = pinned | env | {"LEGEND_TZ": zone},
        node_options = options,
        **kwargs
    )
