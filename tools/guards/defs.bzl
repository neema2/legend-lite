"""guards_package: every package's files as one target the content guards read (Bazel workplan P6-00), and the
checks a package's own rules must pass when its BUILD file loads (G16).

Every BUILD file calls guards_package() once. It adds `all_files`, the package's own files (a glob stops at
subpackages), visible to //tools/guards, whose :repository_files collects one per package from the inventory's package
list: a package that forgets the call fails analysis there.
"""

load(":classpath.bzl", "classpath_report")

def _check_tests():
    # G16 (Bazel workplan P6-16): every JVM test is a junit_test (tools/junit/defs.bzl), run by its JUnitMain:
    # one runner, one set of pinned settings, Bazel's test protocol. guards_package() runs last in every BUILD
    # file (G0), so it sees every rule of the package, manual ones included.
    for rule in native.existing_rules().values():
        if rule["kind"] == "java_test" and (
            rule.get("generator_function") != "junit_test" or rule.get("main_class") != "com.legend.tools.junit.JUnitMain"
        ):
            fail("//%s:%s is a java_test not made by junit_test (tools/junit/defs.bzl): every JVM test is a junit_test (G16)" %
                 (native.package_name(), rule["name"]))

_JS_TEST_MACROS = ("node_test", "browser_test", "tsc_test")

def _check_js_tests():
    # A28 (Bazel workplan P1-23): every js_test comes from node_test (or browser_test, built on it, or the tsc_test
    # typechecks), so none spells its own node_options or reporters.
    for rule in native.existing_rules().values():
        if rule["kind"] == "js_test" and rule.get("generator_function") not in _JS_TEST_MACROS:
            fail("//%s:%s is a js_test not made by node_test (tools/js/defs.bzl): every JavaScript test is a node_test (A28)" %
                 (native.package_name(), rule["name"]))

def guards_package():
    _check_tests()
    _check_js_tests()

    # every test rule of the package, manual ones included (listed explicitly, so none is filtered out): the
    # graph guards' scope, collected per package from the inventory (//tools/guards, G17)
    native.test_suite(
        name = "guard_tests",
        tests = [":" + r["name"] for r in native.existing_rules().values() if r["kind"].endswith("_test")],
        tags = ["manual"],
        visibility = ["//tools/guards:__pkg__"],
    )
    # every JVM target's runtime classpath, as Maven coordinates (G11, //tools/guards:classpath_test)
    classpath_report(
        name = "guard_classpaths",
        targets = [":" + r["name"] for r in native.existing_rules().values() if r["kind"] in ("java_test", "java_binary")],
        testonly = True,
        visibility = ["//tools/guards:__pkg__"],
    )
    native.filegroup(
        name = "all_files",
        # the root package's glob would follow Bazel's convenience links (bazel-bin, bazel-out, ...) into the output
        # tree; their names vary by checkout, so .bazelignore cannot list them
        srcs = native.glob(["**"], exclude = [".git", "bazel-*/**"] if not native.package_name() else [], allow_empty = True),
        visibility = ["//tools/guards:__pkg__"],
    )
