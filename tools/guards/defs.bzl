"""guards_package: every package's files as one target the content guards read (Bazel workplan P6-00), and the
checks a package's own rules must pass when its BUILD file loads (G16).

Every BUILD file calls guards_package() once. It adds `all_files`, the package's own files (a glob stops at
subpackages), visible to //tools/guards, whose :repository_files collects one per package from the inventory's package
list: a package that forgets the call fails analysis there.
"""

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

def guards_package():
    _check_tests()
    native.filegroup(
        name = "all_files",
        # the root package's glob would follow Bazel's convenience links (bazel-bin, bazel-out, ...) into the output
        # tree; their names vary by checkout, so .bazelignore cannot list them
        srcs = native.glob(["**"], exclude = [".git", "bazel-*/**"] if not native.package_name() else [], allow_empty = True),
        visibility = ["//tools/guards:__pkg__"],
    )
