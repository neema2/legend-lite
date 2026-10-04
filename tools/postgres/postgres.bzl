"""THE POSTGRES THE POSTGRES LANES RUN ON (docs/POSTGRES_DIALECT_HOMEWORK_2026_10_01.md, Q5; leg P2).

A real Postgres server's binaries -- zonky's embedded-postgres builds, the same ones its Java launcher
unpacks -- pinned by version and sha256, downloaded from Maven Central and unpacked by Bazel itself (the jar
is a zip holding one .txz): no Docker (the hosted macOS and Windows runners have none), no host tool, and no
jar on any classpath. A test starts the server from them (//testing EmbeddedPostgres). Postgres 16, the
oldest the dialect is written for, so nothing newer creeps into it.

One repository per platform (Bazel workplan P1-15), made by the extension in extensions.bzl, and a hub,
@embedded_postgres, whose aliases select this platform's through //tools/platforms: only the host's is
fetched, and on a platform with no pinned build the Postgres targets are skipped as incompatible.
"""

VERSION = "16.15.0"

# platform (//tools/platforms) -> (zonky's artifact suffix, the archive inside the jar, the jar's sha256)
BUILDS = {
    "linux_aarch64": ("linux-arm64v8", "postgres-linux-arm_64.txz", "f846a9989d686b7977d6eca9bc9d8b2b69e3f70c7eb60e032197d9c574a6136c"),
    "linux_x86_64": ("linux-amd64", "postgres-linux-x86_64.txz", "653abc065c682b85d3da50168fb95dc524bd85426cdec9ce3695a550ef431df2"),
    "macos_arm64": ("darwin-arm64v8", "postgres-darwin-arm_64.txz", "65b953905f0a4d46030767a6b44a52360170d69a7985f1b02aa435f2f6256a83"),
    "macos_x86_64": ("darwin-amd64", "postgres-darwin-x86_64.txz", "1229188b99515d2160d5cb6222bc96d9c5f3a248268df3bc2dfe0047e6c233d8"),
    "windows_x86_64": ("windows-amd64", "postgres-windows-x86_64.txz", "51c7812dc1af47c9a2ccb64fe74efb88c515cff2da347ce72aab92b4cc8e1191"),
}

def _embedded_postgres_platform_impl(rctx):
    url = "https://repo1.maven.org/maven2/io/zonky/test/postgres/embedded-postgres-binaries-{s}/{v}/embedded-postgres-binaries-{s}-{v}.jar".format(s = rctx.attr.suffix, v = VERSION)
    rctx.download(url = url, output = "binaries.jar", sha256 = rctx.attr.sha256)
    rctx.extract("binaries.jar", output = "jar")
    rctx.extract("jar/" + rctx.attr.archive, output = "pg")
    rctx.delete("binaries.jar")
    rctx.delete("jar")

    # PG_ROOT marks the install's root for the launcher (its bin/, lib/, share/ are beside it)
    rctx.file("pg/PG_ROOT", "postgres " + VERSION + " " + rctx.attr.platform + "\n")
    rctx.file("BUILD.bazel", """\
# Postgres {v} for {p}: the whole install, and the marker a test finds its root by.
filegroup(
    name = "postgres",
    srcs = glob(["pg/**"]),
    visibility = ["//visibility:public"],
)

exports_files(["pg/PG_ROOT"])
""".format(v = VERSION, p = rctx.attr.platform))

embedded_postgres_platform = repository_rule(
    implementation = _embedded_postgres_platform_impl,
    doc = "One platform's pinned Postgres binaries, unpacked (the platform is an attribute, never the host's).",
    attrs = {
        "archive": attr.string(mandatory = True),
        "platform": attr.string(mandatory = True),
        "sha256": attr.string(mandatory = True),
        "suffix": attr.string(mandatory = True),
    },
)

def _embedded_postgres_hub_impl(rctx):
    platforms = sorted(rctx.attr.platforms)
    def select_of(target):
        return "select({\n" + "".join([
            '        "@@//tools/platforms:{p}": "@embedded_postgres_{p}//:{t}",\n'.format(p = p, t = target)
            for p in platforms
        ]) + '    }, no_match_error = "no embedded Postgres build is pinned for this platform (tools/postgres/postgres.bzl: ' + ", ".join(platforms) + ')")'
    compatible = "select({\n" + "".join(['        "@@//tools/platforms:{p}": [],\n'.format(p = p) for p in platforms]) + '        "//conditions:default": ["@platforms//:incompatible"],\n    })'
    rctx.file("BUILD.bazel", """\
# This platform's Postgres {v} (tools/postgres/postgres.bzl): the install and its root marker, selected through
# //tools/platforms; skipped as incompatible where no build is pinned.
package(default_visibility = ["//visibility:public"])

alias(
    name = "postgres",
    actual = {files},
    target_compatible_with = {compatible},
)

alias(
    name = "pg/PG_ROOT",
    actual = {root},
    target_compatible_with = {compatible},
)
""".format(v = VERSION, files = select_of("postgres"), root = select_of("pg/PG_ROOT"), compatible = compatible))

embedded_postgres_hub = repository_rule(
    implementation = _embedded_postgres_hub_impl,
    doc = "@embedded_postgres: aliases to the platform repositories, by //tools/platforms.",
    attrs = {"platforms": attr.string_list(mandatory = True)},
)
