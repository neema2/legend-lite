"""THE JAR POOLS and WHO MAY USE EACH (Bazel workplan P1-25): one list, read by every place that names the pools.

Three layers keep legend-engine / legend-pure (and the other test-only jars) out of what ships:

  1. Direct use. POOL_USERS names, for each rules_jvm_external pool in MODULE.bazel, the packages (or single
     targets, "package:name") that may depend on its jars. check_pool_use enforces it when a BUILD file loads, in
     the macros that make first-party Java targets: legend_java_library, legend_java_binary, junit_test, java_run,
     teavm_wasm. rules_jvm_external makes every LISTED jar public under strict_visibility, so Bazel's own
     visibility cannot say this. Other rule kinds (a raw java_library, java_import, a filegroup) are not checked
     here: layer 3 covers what ships, whatever the rule.
  2. Test-only by construction. A target made by those macros that uses a TESTONLY_POOLS pool directly must be
     testonly, so Bazel itself refuses every non-test target that depends on it, transitively and in every package (a pool cannot be marked
     testonly as a whole: its unlisted jars depend on its listed ones).
  3. What ships. //tools/deps:product_closure_test: every shipped root (the server jar, the warehouse server and
     its native image, the launchers, the wasm planner; a new one is added to its list) reaches no jar of a
     TESTONLY_POOLS pool or of @maven_test, through every explicit dependency of any rule kind; core_closure_test:
     core reaches no outside jar at all. A graph-wide layer 1 over every package comes with P6-00's inventory.

Adding a user is a reviewed edit here, with the reason.
"""

POOL_USERS = {
    # core's JDBC drivers: core's own targets, and spec, which runs the corpus through them
    "maven_core": ["core", "spec"],
    # H2 2.4.240, the alternative H2 one PCT lane runs on
    "maven_h2_modern": ["pct"],
    # legend-engine's execution stack, for the engine runner tool only
    "maven_runner": ["tools/engine-runner"],
    # TeaVM: the WebAssembly compiler and the class library the planner compiles against
    "maven_teavm": ["tools/teavm", "wasm"],
    # test tooling: JUnit, ArchUnit; for the packages with tests
    "maven_test": ["core", "json", "parser-equivalence", "pct", "spec", "tools/bump", "tools/deps", "tools/junit", "warehouse"],
    # compiler plugins (NullAway), never on a classpath
    "maven_tools": ["tools/nullaway"],
    # legend-engine and legend-pure: TEST INPUTS ONLY, for the packages that referee lite against them (AGENTS.md,
    # reference-checkout tenet); in tools/junit only runner_test, which takes JUnit 4 from here for its JUnit 3
    # fixtures (tools/junit:junit is on every test's classpath, so the package as a whole is not a user)
    "maven_upstream": ["parser-equivalence", "pct", "tools/junit:runner_test", "tools/par", "tools/reference"],
    # DuckDB's JDBC driver for the warehouse and the app built on it
    "maven_warehouse": ["datacube", "warehouse"],
}

POOLS = sorted(POOL_USERS.keys())

# Pools whose direct users must be testonly (layer 2), and whose jars no shipped root may reach (layer 3).
TESTONLY_POOLS = ["maven_runner", "maven_upstream"]

def _pool_of_label(label):
    # canonical repository names: rules_jvm_external++maven+maven_upstream
    repo = label.repo_name
    if "++maven+" not in repo:
        return None
    return repo.split("++maven+")[-1]

def check_pool_use(name, testonly, *label_lists):
    """In a macro: fails if this target may not use a pool one of the labels names.

    Args:
        name: the target.
        testonly: the target's testonly (a junit_test is always testonly).
        *label_lists: lists of labels (deps, runtime_deps, exports, ...); a select() is refused.
    """
    package = native.package_name()
    what = "//%s:%s" % (package, name)
    for labels in label_lists:
        if type(labels) != "list":
            fail("%s: a dependency list must be a plain list here, not a %s (tools/deps/pools.bzl checks it)" % (what, type(labels)))
        for label in labels:
            # every spelling (@maven_x//:y, @@rules_jvm_external++maven+maven_x//:y, a per-jar repository) resolves
            # to one canonical repository; an unknown ++maven+ repository fails closed below
            pool = _pool_of_label(native.package_relative_label(label))
            if not pool:
                continue
            users = POOL_USERS.get(pool)
            if users == None:
                fail("%s uses %s, from a Maven repository tools/deps/pools.bzl does not know as a pool" % (what, label))
            if package not in users and "%s:%s" % (package, name) not in users:
                fail(("%s depends on @%s, which only %s may use (tools/deps/pools.bzl, Bazel workplan P1-25): add " +
                      "it there, with the reason, if it really needs those jars") % (what, pool, ", ".join(["//" + u for u in users])))
            if pool in TESTONLY_POOLS and not testonly:
                fail(("%s depends on @%s and must be testonly = True: that pool's jars are test inputs, and a testonly " +
                      "target is one nothing that ships can depend on (tools/deps/pools.bzl)") % (what, pool))
