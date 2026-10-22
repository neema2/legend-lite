"""THE MONGODB THE MONGODB LANES RUN ON (docs/MONGODB_PLAN_2026_10_16.md §5.3, decisions D8 and D9).

A real MongoDB server -- MongoDB's own Community Server tarball, pinned by version and sha256, downloaded from
fastdl.mongodb.org and unpacked by Bazel itself: no Docker, no host tool. A test starts the server from it (//testing
EmbeddedMongoDb). MongoDB 8.0, the current major; the platform's floor is 7.0 (D9). Only the server binary is kept (the
tarball's other files are its router and its notices). The server is test input under its own licence (SSPL): it is
fetched into Bazel's cache for a test JVM and never redistributed. Pinned for Linux x86_64 (the Ubuntu 22.04 build,
which runs on 24.04 too); the hub skips the MongoDB targets as incompatible elsewhere.
"""

VERSION = "8.0.17"

# platform (//tools/platforms) -> (the tarball's architecture and distribution, its sha256)
BUILDS = {
    "linux_x86_64": ("x86_64-ubuntu2204", "4372a8e503a61814c565d4ccbc5f6765787944772e8969c13ba96c99cca11f75"),
}

def _embedded_mongodb_platform_impl(rctx):
    name = "mongodb-linux-{a}-{v}".format(a = rctx.attr.arch, v = VERSION)
    rctx.download_and_extract(
        url = ["https://fastdl.mongodb.org/linux/{n}.tgz".format(n = name)],
        sha256 = rctx.attr.sha256,
        output = "unpacked",
        stripPrefix = name,
    )
    rctx.symlink("unpacked/bin/mongod", "mongodb/bin/mongod")
    rctx.symlink("unpacked/LICENSE-Community.txt", "mongodb/LICENSE-Community.txt")
    for f in ["unpacked/bin/mongos", "unpacked/bin/install_compass", "unpacked/MPL-2", "unpacked/README",
              "unpacked/THIRD-PARTY-NOTICES"]:
        rctx.delete(f)

    # MONGODB_ROOT marks the install's root for the launcher (its bin/ is beside it)
    rctx.file("mongodb/MONGODB_ROOT", "mongodb " + VERSION + " " + rctx.attr.platform + "\n")
    rctx.file("BUILD.bazel", """\
# MongoDB {v} for {p}: the server's binary and its licence, and the marker a test finds its root by.
filegroup(
    name = "mongodb",
    srcs = glob(["mongodb/**"]),
    visibility = ["//visibility:public"],
)

exports_files(["mongodb/MONGODB_ROOT"])
""".format(v = VERSION, p = rctx.attr.platform))

embedded_mongodb_platform = repository_rule(
    implementation = _embedded_mongodb_platform_impl,
    doc = "One platform's pinned MongoDB Community Server: its server binary.",
    attrs = {
        "arch": attr.string(mandatory = True),
        "platform": attr.string(mandatory = True),
        "sha256": attr.string(mandatory = True),
    },
)

def _embedded_mongodb_hub_impl(rctx):
    platforms = sorted(rctx.attr.platforms)

    def select_of(target):
        return "select({\n" + "".join([
            '        "@@//tools/platforms:{p}": "@embedded_mongodb_{p}//:{t}",\n'.format(p = p, t = target)
            for p in platforms
        ]) + '    }, no_match_error = "no embedded MongoDB build is pinned for this platform (tools/mongodb/mongodb.bzl: ' + ", ".join(platforms) + ')")'

    compatible = "select({\n" + "".join(['        "@@//tools/platforms:{p}": [],\n'.format(p = p) for p in platforms]) + '        "//conditions:default": ["@platforms//:incompatible"],\n    })'
    rctx.file("BUILD.bazel", """\
# This platform's MongoDB {v} (tools/mongodb/mongodb.bzl): the server binary and its root marker, selected through
# //tools/platforms; skipped as incompatible where no build is pinned.
package(default_visibility = ["//visibility:public"])

alias(
    name = "mongodb",
    actual = {files},
    target_compatible_with = {compatible},
)

alias(
    name = "mongodb/MONGODB_ROOT",
    actual = {root},
    target_compatible_with = {compatible},
)
""".format(v = VERSION, files = select_of("mongodb"), root = select_of("mongodb/MONGODB_ROOT"), compatible = compatible))

embedded_mongodb_hub = repository_rule(
    implementation = _embedded_mongodb_hub_impl,
    doc = "@embedded_mongodb: aliases to the platform repositories, by //tools/platforms.",
    attrs = {"platforms": attr.string_list(mandatory = True)},
)
