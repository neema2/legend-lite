"""node_test: the ONE way a JavaScript test target is declared in this repository (Bazel workplan P1-23).

Every node:test file runs with the same settings, so no target spells them itself:

  * TypeScript run directly, types stripped (--experimental-strip-types), its warning silenced;
  * two reporters: `spec` for the log, and //tools/js:strict_reporter for the EXIT CODE (Node counts a suite whose
    body throws as `# fail 0` and exits 0; the strict reporter fails the process on any `test:fail`);
  * a pinned locale and clock, LANG=C, LC_ALL=C, TZ=UTC, so a verdict never depends on the desk; a target that
    tests another time zone says so in `env`, which wins;
  * `wasm = True` adds the WebAssembly planner (//wasm:planner) and the flag Node needs to load it.

ONE ENTRY POINT PER TARGET (A28): each test file is its own process, which is what makes a test's global DOM
mutation (jsdom on globalThis) safe. node_test takes exactly one `entry_point` and nothing else runs in it.
"""

load("@aspect_rules_js//js:defs.bzl", "js_test")

_REPORTER = "tools/js/strict-reporter.mjs"

def node_test(name, entry_point, data = [], wasm = False, env = {}, node_options = [], **kwargs):
    """A js_test running one node:test entry point with the repository's settings.

    Args:
        name: the target.
        entry_point: the one test file (A28: one entry point per target).
        data: what the test reads.
        wasm: True to load the WebAssembly planner (//wasm:planner).
        env: added over the pinned LANG/LC_ALL/TZ (a TZ lane overrides TZ here).
        node_options: added after the shared options.
        **kwargs: everything else js_test takes (size, chdir, tags, ...).
    """
    if type(entry_point) != "string":
        fail("node_test %s: entry_point must be ONE file (a test file is its own process, A28)" % name)

    # the reporter by path from where the test runs: the package directory under `chdir` (P1-24 removes it)
    up = "/".join([".."] * len(native.package_name().split("/"))) if kwargs.get("chdir") else "."
    options = ["--experimental-strip-types"] + (["--experimental-wasm-exnref"] if wasm else []) + [
        "--disable-warning=ExperimentalWarning",
        "--test-reporter=spec",
        "--test-reporter-destination=stdout",
        "--test-reporter=%s/%s" % (up, _REPORTER),
        "--test-reporter-destination=stderr",
    ] + node_options
    js_test(
        name = name,
        entry_point = entry_point,
        data = data + [Label("//tools/js:strict_reporter")] + ([Label("//wasm:planner")] if wasm else []),
        env = {"LANG": "C", "LC_ALL": "C", "TZ": "UTC"} | env,
        node_options = options,
        **kwargs
    )
