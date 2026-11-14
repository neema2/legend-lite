"""THE CLICKHOUSE THE CLICKHOUSE LANES RUN ON (docs/CLOUD_BACKENDS_PCT_2026_10_11.md §19).

A real ClickHouse server -- ClickHouse's own static build (clickhouse-common-static), pinned by version and sha256,
downloaded from its GitHub release and unpacked by Bazel itself: no Docker, no host tool. A test starts the server from
it (//testing EmbeddedClickHouse). ClickHouse 26.3, the long-term release (26.3.60.2; 25.8.2.29 until 2026-11-07, the
user: "Upgrade to 26.3 LTS", docs/TS_DECISIONS_2026_11_07.md §4). The build is one self-contained binary; only it
is kept (the tarball's other files are its package scripts). Pinned for Linux x86_64; the hub skips the ClickHouse
targets as incompatible elsewhere.
"""

VERSION = "26.3.60.2"

# platform (//tools/platforms) -> (the tarball's architecture, its sha256)
BUILDS = {
    "linux_x86_64": ("amd64", "507593f610c009f8b51214948f88134550913189a553a6a0c52fc0e4730aa371"),
}

def _embedded_clickhouse_platform_impl(rctx):
    name = "clickhouse-common-static-{v}".format(v = VERSION)
    file = "{n}-{a}.tgz".format(n = name, a = rctx.attr.arch)
    rctx.download_and_extract(
        url = ["https://github.com/ClickHouse/ClickHouse/releases/download/v{v}-lts/{f}".format(v = VERSION, f = file)],
        sha256 = rctx.attr.sha256,
        output = "unpacked",
        stripPrefix = name,
    )
    rctx.symlink("unpacked/usr/bin/clickhouse", "clickhouse/bin/clickhouse")
    for d in ["unpacked/etc", "unpacked/lib", "unpacked/usr/share"]:
        rctx.delete(d)

    # CLICKHOUSE_ROOT marks the install's root for the launcher (its bin/ is beside it)
    rctx.file("clickhouse/CLICKHOUSE_ROOT", "clickhouse " + VERSION + " " + rctx.attr.platform + "\n")
    rctx.file("BUILD.bazel", """\
# ClickHouse {v} for {p}: the server's one binary, and the marker a test finds its root by.
filegroup(
    name = "clickhouse",
    srcs = glob(["clickhouse/**"]),
    visibility = ["//visibility:public"],
)

exports_files(["clickhouse/CLICKHOUSE_ROOT"])
""".format(v = VERSION, p = rctx.attr.platform))

embedded_clickhouse_platform = repository_rule(
    implementation = _embedded_clickhouse_platform_impl,
    doc = "One platform's pinned ClickHouse static build: its one binary.",
    attrs = {
        "arch": attr.string(mandatory = True),
        "platform": attr.string(mandatory = True),
        "sha256": attr.string(mandatory = True),
    },
)

def _embedded_clickhouse_hub_impl(rctx):
    platforms = sorted(rctx.attr.platforms)
    def select_of(target):
        return "select({\n" + "".join([
            '        "@@//tools/platforms:{p}": "@embedded_clickhouse_{p}//:{t}",\n'.format(p = p, t = target)
            for p in platforms
        ]) + '    }, no_match_error = "no embedded ClickHouse build is pinned for this platform (tools/clickhouse/clickhouse.bzl: ' + ", ".join(platforms) + ')")'
    compatible = "select({\n" + "".join(['        "@@//tools/platforms:{p}": [],\n'.format(p = p) for p in platforms]) + '        "//conditions:default": ["@platforms//:incompatible"],\n    })'
    rctx.file("BUILD.bazel", """\
# This platform's ClickHouse {v} (tools/clickhouse/clickhouse.bzl): the binary and its root marker, selected through
# //tools/platforms; skipped as incompatible where no build is pinned.
package(default_visibility = ["//visibility:public"])

alias(
    name = "clickhouse",
    actual = {files},
    target_compatible_with = {compatible},
)

alias(
    name = "clickhouse/CLICKHOUSE_ROOT",
    actual = {root},
    target_compatible_with = {compatible},
)
""".format(v = VERSION, files = select_of("clickhouse"), root = select_of("clickhouse/CLICKHOUSE_ROOT"), compatible = compatible))

embedded_clickhouse_hub = repository_rule(
    implementation = _embedded_clickhouse_hub_impl,
    doc = "@embedded_clickhouse: aliases to the platform repositories, by //tools/platforms.",
    attrs = {"platforms": attr.string_list(mandatory = True)},
)
