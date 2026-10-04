"""junit_test: the ONE way a test target is declared in this repository.

Every JUnit suite runs through tools/junit/JUnitMain (the JUnit Platform Launcher,
speaking Bazel's test protocol: test.xml, --test_filter, sharding, the premature-exit
file) with the same settings, so no target sets them on its own:

  * the engine's test clock, -Duser.timezone=GMT (the root pom's surefire argLine:
    legend-engine minted its goldens in GMT);
  * counts always printed, and a run that finds no tests FAILS — a PASSED line
    cannot otherwise tell a full suite from an empty one;
  * the upstream source trees, when asked for, as declared inputs with the
    legend.engine.root / legend.pure.root properties pointing at them;
  * ONE number for memory, `memory_mb` (Bazel workplan P1-21): the scheduler's
    `resources:memory:<n>` tag and the JVM's -Xmx<n>m, so Bazel packs tests by what
    each JVM may actually use, and CI and the desk run the same command line (and
    share cache keys). Set from a measured peak plus headroom: each run prints its
    live heap ("[bazel] heap: live peak ..."). The rule: the live peak plus half again, at least
    512 MB, rounded up to 256 MB; a lane sized otherwise says why beside it.

`select` is selectors in the JUnit console launcher's vocabulary, e.g.
["--select-package=com.legend"] or ["--select-class=com.legend.rcorpus.MinimalCorpusTest"];
JUnitMain lists every one it accepts, and any other argument fails the run.
"""

load("@rules_java//java:defs.bzl", "java_test")

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
        **kwargs):
    args = select + ["--exclude-tag=" + t for t in exclude_tags] + ["--fail-if-no-tests"]
    # one clock, one locale, one encoding, everywhere: a test's verdict never depends on the host's.
    # The temp directory is set by JUnitMain from TEST_TMPDIR (the Windows Java launcher does not
    # expand environment variables in jvm_flags).
    for flag in jvm_flags:
        if flag.startswith("-Xmx"):
            fail("%s: the heap is memory_mb, not a jvm_flags -Xmx (one number for the JVM and the scheduler)" % name)
    flags = [
        "-Duser.timezone=GMT",
        "-Duser.language=en",
        "-Duser.country=US",
        "-Dfile.encoding=UTF-8",
        "-Xmx%dm" % memory_mb,
    ] + jvm_flags
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
            "-Dlegend.engine.root=$(rlocationpath @legend_engine_src//:pom.xml)",
            "-Dlegend.pure.root=$(rlocationpath @legend_pure_src//:pom.xml)",
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
        **kwargs
    )
