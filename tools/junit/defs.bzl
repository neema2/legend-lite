"""junit_test: the ONE way a test target is declared in this repository.

Every JUnit suite runs through tools/junit/JUnitMain (JUnit's console launcher,
told where Bazel wants its reports) with the same settings, so no target sets
them on its own:

  * the engine's test clock, -Duser.timezone=GMT (the root pom's surefire argLine:
    legend-engine minted its goldens in GMT);
  * counts always printed, and a run that finds no tests FAILS — a PASSED line
    cannot otherwise tell a full suite from an empty one;
  * the upstream source trees, when asked for, as declared inputs with the
    legend.engine.root / legend.pure.root properties pointing at them.

`select` is JUnit console-launcher selectors, e.g. ["--select-package=com.legend"]
or ["--select-class=com.legend.rcorpus.MinimalCorpusTest"].
"""

load("@rules_java//java:defs.bzl", "java_test")

def _runfiles_dir(label):
    # A runfiles path relative to the test's working directory ($RUNFILES/_main):
    # external repositories are its siblings.
    return "../" + Label(label).repo_name

def junit_test(
        name,
        select,
        exclude_tags = [],
        upstream = False,
        jvm_flags = [],
        data = [],
        deps = [],
        runtime_deps = [],
        size = "large",
        **kwargs):
    args = select + ["--exclude-tag=" + t for t in exclude_tags] + [
        "--details=summary",
        "--fail-if-no-tests",
        "--disable-banner",
        "--disable-ansi-colors",
    ]
    flags = ["-Duser.timezone=GMT"] + jvm_flags
    inputs = list(data)
    if upstream:
        inputs += ["@legend_engine_src//:tree", "@legend_pure_src//:tree"]
        flags += [
            "-Dlegend.engine.root=" + _runfiles_dir("@legend_engine_src//:tree"),
            "-Dlegend.pure.root=" + _runfiles_dir("@legend_pure_src//:tree"),
        ]
    java_test(
        name = name,
        size = size,
        main_class = "com.legend.tools.junit.JUnitMain",
        use_testrunner = False,
        args = args,
        jvm_flags = flags,
        data = inputs,
        deps = deps,
        runtime_deps = runtime_deps + ["//tools/junit"],
        **kwargs
    )
