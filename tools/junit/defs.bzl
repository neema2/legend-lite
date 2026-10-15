"""junit_test: the ONE way a test target is declared in this repository.

Every JUnit suite runs through tools/junit/JUnitMain (the JUnit Platform Launcher,
speaking Bazel's test protocol: test.xml, --test_filter, sharding, the premature-exit
file) with the same settings, so no target sets them on its own:

  * the engine's test clock, -Duser.timezone=GMT (legend-engine minted its goldens in GMT);
  * counts always printed, and a run that finds no tests FAILS — a PASSED line
    cannot otherwise tell a full suite from an empty one;
  * the upstream source trees, when asked for, as declared inputs with the
    legend.engine.root.rlocation / legend.pure.root.rlocation properties naming the file at each root
    (ProgramPaths.rootOf);
  * ONE number for memory, `memory_mb` (Bazel workplan P1-21): the scheduler's
    `resources:memory:<n>` tag and the JVM's -Xmx<n>m, so Bazel packs tests by what
    each JVM may actually use, and CI and the desk run the same command line (and
    share cache keys). Set from a measured peak plus headroom: each run prints its
    live heap ("[bazel] heap: live peak ..."). The rule: the live peak plus half again, at least
    512 MB, rounded up to 256 MB; a lane sized otherwise says why beside it;
  * ONE choice of JIT compiler, `jit` (2026-10-15, PR #46; the measurements:
    docs/build-inventory/program/evidence/ci-test-jit/): "c1", the default, stops the JVM's
    tiered compilation at C1 (-XX:TieredStopAtLevel=1). A test JVM is mostly short-lived, and
    C2's compiles cost it more CPU than they repay: the core lane, about four JVMs on a CI
    runner's four vCPUs, spent 710 s of its 1,160 s of process CPU in the JIT on Windows (67 of
    605 s with C1), and //core:core_tests_integration took 277 s there against its 300 s limit
    (113 s with C1). "c2" keeps the full tiered compiler, for a long-running JVM that C2 repays;
    a target names it beside its measurement. The same on a desk and in CI, so the launcher a
    job builds is the one it tests.

`select` is selectors in the JUnit console launcher's vocabulary, e.g.
["--select-package=com.legend"] or ["--select-class=com.legend.rcorpus.MinimalCorpusTest"];
JUnitMain lists every one it accepts, and any other argument fails the run.
"""

load("@rules_java//java:defs.bzl", "java_test")
load("//tools/deps:pools.bzl", "check_pool_use")
load("//tools/java:defs.bzl", "LEGEND_JAVACOPTS")

def junit_test(
        name,
        select,
        memory_mb,
        exclude_tags = [],
        upstream = False,
        jvm_flags = [],
        data = [],
        deps = [],
        runtime_deps = [],
        size = "large",
        tags = [],
        jit = "c1",
        **kwargs):
    check_pool_use(name, deps, runtime_deps, data)  # the package may use each Maven pool named (tools/deps/pools.bzl)
    args = select + ["--exclude-tag=" + t for t in exclude_tags] + ["--fail-if-no-tests"]
    # one clock, one locale, one encoding, everywhere: a test's verdict never depends on the host's.
    # The temp directory is set by JUnitMain from TEST_TMPDIR (the Windows Java launcher does not
    # expand environment variables in jvm_flags).
    for flag in jvm_flags:
        if flag.startswith("-Xmx"):
            fail("%s: the heap is memory_mb, not a jvm_flags -Xmx (one number for the JVM and the scheduler)" % name)
        if flag.startswith("-XX:TieredStopAtLevel") or flag.startswith("-XX:-TieredCompilation"):
            fail("%s: the JIT is jit = \"c1\" or \"c2\", not a jvm_flags %s" % (name, flag))
    if jit not in ("c1", "c2"):
        fail("%s: jit is \"c1\" (the default) or \"c2\", not %r" % (name, jit))
    flags = [
        "-Duser.timezone=GMT",
        "-Duser.language=en",
        "-Duser.country=US",
        "-Dfile.encoding=UTF-8",
        "-Xmx%dm" % memory_mb,
    ] + (["-XX:TieredStopAtLevel=1"] if jit == "c1" else []) + jvm_flags
    inputs = list(data)
    if upstream:
        # each tree by the runfiles path of its root's pom.xml; Upstream resolves it through the runfiles
        # library and takes its directory (Bazel workplan P1-04)
        inputs += [
            "@legend_engine_src//:tree",
            "@legend_engine_src//:pom.xml",
            "@legend_pure_src//:tree",
            "@legend_pure_src//:pom.xml",
        ]
        flags += [
            "-Dlegend.engine.root.rlocation=$(rlocationpath @legend_engine_src//:pom.xml)",
            "-Dlegend.pure.root.rlocation=$(rlocationpath @legend_pure_src//:pom.xml)",
        ]
    java_test(
        name = name,
        size = size,
        main_class = "com.legend.tools.junit.JUnitMain",
        use_testrunner = False,
        args = args,
        jvm_flags = flags,
        tags = tags + ["resources:memory:%d" % memory_mb],
        data = inputs,
        deps = deps,
        runtime_deps = runtime_deps + ["//tools/junit"],
        # a test's own sources take the shared javacopts too: the locale checks (P3-28) hold in test code
        javacopts = LEGEND_JAVACOPTS + kwargs.pop("javacopts", []),
        **kwargs
    )
