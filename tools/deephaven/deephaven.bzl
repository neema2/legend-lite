"""THE DEEPHAVEN THE DEEPHAVEN TESTS RUN ON (docs/DEEPHAVEN_PLAN_2026_10_22.md DH8, L3b).

A real Deephaven Community server -- its own server-jetty tarball, pinned by version and sha256, downloaded from its
GitHub release and unpacked by Bazel itself: no Docker, no host tool. A test starts its main class from its jars
(//testing EmbeddedDeephaven) on the test's own Java (the server's start script passes a flag Java 25 removed, so it is
deleted with bin/; its Java 25 options file is kept), with an HTTPS listener, since Deephaven's browser protocol needs
HTTP/2.

The server is under the Deephaven Community License: a test input only, never on a product classpath (DH11; the
filegroup is testonly), fetched by the build and not carried by anything it ships. Only lib/ (the jars) and etc/ (the
JVM options the server is built with) are kept. The jars are Java's, so one repository serves every platform.
"""

VERSION = "42.7"

SHA256 = "31c8d4d8fc2bbfae00b90f4aab3111fb321019bd512b991262fc70b86f750d37"

def _embedded_deephaven_impl(rctx):
    name = "server-jetty-" + VERSION
    rctx.download_and_extract(
        url = ["https://github.com/deephaven/deephaven-core/releases/download/v{v}/{n}.tar".format(v = VERSION, n = name)],
        sha256 = SHA256,
        output = "deephaven",
        stripPrefix = name,
    )
    rctx.delete("deephaven/bin")

    # DEEPHAVEN_ROOT marks the install's root for the launcher (its lib/ and etc/ are beside it)
    rctx.file("deephaven/DEEPHAVEN_ROOT", "deephaven " + VERSION + "\n")
    rctx.file("BUILD.bazel", """\
# Deephaven {v}: the server's jars and JVM options, and the marker a test finds its root by. Test input only.
filegroup(
    name = "deephaven",
    testonly = True,
    srcs = glob(["deephaven/lib/**", "deephaven/etc/**", "deephaven/DEEPHAVEN_ROOT"]),
    visibility = ["//visibility:public"],
)

exports_files(["deephaven/DEEPHAVEN_ROOT"])
""".format(v = VERSION))

embedded_deephaven_repository = repository_rule(
    implementation = _embedded_deephaven_impl,
    doc = "The pinned Deephaven Community server, unpacked and trimmed to its jars and JVM options.",
)
