"""guards_package: every package's files as one target the content guards read (Bazel workplan P6-00), and the
checks a package's own rules must pass when its BUILD file loads (G16).

Every BUILD file calls guards_package() once. It adds `all_files`, the package's own files (a glob stops at
subpackages), visible to //tools/guards, whose :repository_files collects one per package from the inventory's package
list: a package that forgets the call fails analysis there.
"""

load("//tools/platforms:defs.bzl", "PLATFORMS", "compatible_with")
load(":classpath.bzl", "classpath_report")
load(":markdown.bzl", "markdown_report")

# The macros that make a JVM test: junit_test, and the macros that call it (a rule's generator_function is its
# OUTERMOST macro). Each listed wrapper makes its tests only through junit_test.
_JUNIT_MACROS = [
    "junit_test",
    # spec/corpus.bzl: a corpus lane's host-judge action and its test (Bazel workplan P3-01)
    "corpus_lane",
    # tools/legend/defs.bzl: a Legend model project's compile check, and the graph's (Bazel workplan P3-23)
    "legend_library",
    "legend_graph_test",
]

# The JVM tests that are not junit_tests, each with its reason (G16's allowlist).
_NON_JUNIT_TESTS = {
    # its verdict is the JVM's own module limit (--limit-modules=java.base), which no runner on the class path can
    # keep (Bazel workplan P3-19)
    "//core:planner_on_java_base_test": "PlanOnJavaBase exits non-zero when the planner needs more than java.base",
}

def _check_tests():
    # G16 (Bazel workplan P6-16): every JVM test is a junit_test (tools/junit/defs.bzl), run by its JUnitMain:
    # one runner, one set of pinned settings, Bazel's test protocol. guards_package() is the LAST call of every BUILD
    # file (by convention: G0 checks only that it is there; a rule written after it escapes these checks), so it sees
    # every rule of the package, manual ones included.
    for rule in native.existing_rules().values():
        if rule["kind"] == "java_test" and "//%s:%s" % (native.package_name(), rule["name"]) not in _NON_JUNIT_TESTS and (
            rule.get("generator_function") not in _JUNIT_MACROS or rule.get("main_class") != "com.legend.tools.junit.JUnitMain"
        ):
            fail("//%s:%s is a java_test not made by junit_test (tools/junit/defs.bzl): every JVM test is a junit_test (G16)" %
                 (native.package_name(), rule["name"]))
        # the other way to run JUnit, as a build action (tools/junit/JUnitAction.java): only a corpus lane's host pass
        if rule.get("main_class") == "com.legend.tools.junit.JUnitAction" and rule.get("generator_function") != "corpus_lane":
            fail("//%s:%s runs JUnitAction outside corpus_lane (spec/corpus.bzl): a JUnit run as a build action is a corpus lane's host pass only (G16)" %
                 (native.package_name(), rule["name"]))

_JS_TEST_MACROS = ("node_test", "browser_test", "tsc_test")

def _check_js_tests():
    # A28 (Bazel workplan P1-23): every js_test comes from node_test (or browser_test, built on it, or the tsc_test
    # typechecks), so none spells its own node_options or reporters.
    for rule in native.existing_rules().values():
        if rule["kind"] == "js_test" and rule.get("generator_function") not in _JS_TEST_MACROS:
            fail("//%s:%s is a js_test not made by node_test (tools/js/defs.bzl): every JavaScript test is a node_test (A28)" %
                 (native.package_name(), rule["name"]))

def _check_libraries():
    # A19 (Bazel workplan P3-28): every first-party Java compile takes LEGEND_JAVACOPTS (tools/java/defs.bzl), so the
    # Error Prone locale checks hold repository-wide: a java_library is a legend_java_library, and a java_binary or
    # java_test that compiles sources of its own comes from legend_java_binary or a junit_test macro (G16)
    makers = {
        "java_library": ["legend_java_library"],
        "java_binary": ["legend_java_binary"],
        "java_test": _JUNIT_MACROS,
    }
    for rule in native.existing_rules().values():
        kind = rule["kind"]
        if kind not in makers or rule.get("generator_function") in makers[kind]:
            continue
        if kind == "java_library" or rule.get("srcs"):
            fail("//%s:%s is a %s with sources not made by %s: every first-party compile takes the shared javacopts (A19)" %
                 (native.package_name(), rule["name"], kind, " or ".join(makers[kind])))

def _check_config_settings():
    # P1-19 (G-nn): platform policy lives in //tools/platforms, so no other package declares a config_setting
    if native.package_name() != "tools/platforms":
        for rule in native.existing_rules().values():
            if rule["kind"] == "config_setting":
                fail("//%s:%s is a config_setting outside //tools/platforms: platform policy lives there (Bazel workplan P1-19)" %
                     (native.package_name(), rule["name"]))

def guards_package():
    _check_tests()
    _check_js_tests()
    _check_libraries()
    _check_config_settings()

    rules = native.existing_rules().values()

    # this repository's Markdown among every test's runtime files, manual tests included (G17,
    # //tools/guards:markdown_inputs_test)
    markdown_report(
        name = "guard_markdown",
        targets = [":" + r["name"] for r in rules if r["kind"].endswith("_test")],
        testonly = True,
        # the reports depend on every test, so on a platform no test is written for (A25's unlisted one) they are
        # skipped, not analyzed into the tests' toolchain resolution
        target_compatible_with = compatible_with(PLATFORMS),
        visibility = ["//tools/guards:__pkg__"],
    )
    # every JVM target's runtime classpath, as Maven coordinates (G11, //tools/guards:classpath_test)
    classpath_report(
        name = "guard_classpaths",
        targets = [":" + r["name"] for r in rules if r["kind"] in ("java_test", "java_binary", "_java_run")],
        testonly = True,
        target_compatible_with = compatible_with(PLATFORMS),
        visibility = ["//tools/guards:__pkg__"],
    )
    native.filegroup(
        name = "all_files",
        # the root package's glob would follow Bazel's convenience links (bazel-bin, bazel-out, ...) into the output
        # tree; their names vary by checkout, so .bazelignore cannot list them. .git is ignored there, but in a
        # worktree it is a FILE, which an ignored directory name does not cover.
        srcs = native.glob(["**"], exclude = [".git", "bazel-*/**"] if not native.package_name() else [], allow_empty = True),
        visibility = ["//tools/guards:__pkg__"],
    )
