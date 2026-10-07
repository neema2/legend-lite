"""The warehouse's build helpers: jar_entry, POSTGRES_EXTENSION and warehouse_folder.

jar_entry takes one file out of a Java library's jar, as a build output. The warehouse's native
image loads DuckDB's native library from beside it (or from --duckdb-library), and that library
rides inside DuckDB's JDBC jar, one per platform. This takes it out with Bazel's own zipper, in an
action, so the file is an ordinary Bazel output with a runfiles path -- never unzipped by a script.

warehouse_folder is the native warehouse as one folder beside everything it loads -- DuckDB's library, its
postgres extension and, for the DataCube app, the site -- which `bazel run` starts and a package carries
(the build rebuild's L1c, docs/REBUILD_PROGRAM_2026_10_06.md §4: the server knows nothing of Bazel). The
folder is packaging: the image itself (//:native) stays a compile (tools/guards compile_only_test).
"""

load("@bazel_lib//lib:copy_directory.bzl", "copy_directory_bin_action")
load("@bazel_lib//lib:copy_file.bzl", "COPY_FILE_TOOLCHAINS", "copy_file_action")
load("//tools/platforms:defs.bzl", "platform_select")
load("@rules_java//java/common:java_info.bzl", "JavaInfo")

def _jar_entry_impl(ctx):
    jars = ctx.attr.jar[JavaInfo].runtime_output_jars
    if len(jars) != 1:
        fail("%s: expected one jar in %s, found %d" % (ctx.label, ctx.attr.jar.label, len(jars)))
    out = ctx.actions.declare_file(ctx.attr.entry)
    ctx.actions.run(
        executable = ctx.executable._zipper,
        arguments = ["x", jars[0].path, "-d", out.dirname, ctx.attr.entry],
        inputs = jars,
        outputs = [out],
        mnemonic = "JarEntry",
        progress_message = "Extracting %s from %s" % (ctx.attr.entry, jars[0].basename),
    )
    return [DefaultInfo(files = depset([out]), runfiles = ctx.runfiles(files = [out]))]

jar_entry = rule(
    implementation = _jar_entry_impl,
    attrs = {
        "jar": attr.label(providers = [JavaInfo], mandatory = True),
        "entry": attr.string(mandatory = True, doc = "The entry's path in the jar; also the output's name."),
        "_zipper": attr.label(
            default = "@bazel_tools//tools/zip:zipper",
            executable = True,
            cfg = "exec",
        ),
    },
    doc = "Extracts one entry of a Java library's jar as a file named after it.",
)

# DuckDB's postgres extension for the platform being built, as MODULE.bazel pins it: one choice, which
# //warehouse:postgres_extension unpacks beside the native server.
POSTGRES_EXTENSION, POSTGRES_EXTENSION_COMPATIBLE = platform_select({
    "macos_arm64": "@duckdb_postgres_extension_osx_arm64//file",
    "macos_x86_64": "@duckdb_postgres_extension_osx_amd64//file",
    "linux_x86_64": "@duckdb_postgres_extension_linux_amd64//file",
    "linux_aarch64": "@duckdb_postgres_extension_linux_arm64//file",
    "windows_x86_64": "@duckdb_postgres_extension_windows_amd64//file",
}, "DuckDB postgres extension (MODULE.bazel, duckdb_postgres_extension_*)")

_COPY_DIRECTORY_TOOLCHAIN = "@bazel_lib//lib:copy_directory_toolchain_type"

def _warehouse_folder_impl(ctx):
    server = ctx.executable.server
    folder = ctx.label.name

    # Real copies, never symlinks: a native executable finds its files beside its REAL path (DuckLibrary.executableDir
    # reads the process's command, /proc/self/exe on Linux, links resolved), so a link to the server in another folder
    # would find that other folder's files. The server under one name on every platform, .exe where the image has it.
    exe = ctx.actions.declare_file(folder + "/warehouse" + (".exe" if server.extension == "exe" else ""))
    copy_file_action(ctx, server, exe)
    files = [exe]
    for file in [ctx.file.library, ctx.file.extension]:
        out = ctx.actions.declare_file(folder + "/" + file.basename)
        copy_file_action(ctx, file, out)
        files.append(out)
    if ctx.file.site:
        site = ctx.actions.declare_directory(folder + "/site")
        copy_directory_bin_action(
            ctx,
            src = ctx.file.site,
            dst = site,
            copy_directory_bin = ctx.toolchains[_COPY_DIRECTORY_TOOLCHAIN].copy_directory_info.bin,
        )
        files.append(site)
    return [DefaultInfo(executable = exe, files = depset(files), runfiles = ctx.runfiles(files = files))]

warehouse_folder = rule(
    implementation = _warehouse_folder_impl,
    executable = True,
    attrs = {
        "server": attr.label(executable = True, cfg = "target", mandatory = True, doc = "The native warehouse."),
        "library": attr.label(allow_single_file = True, mandatory = True, doc = "DuckDB's native library for the platform."),
        "extension": attr.label(allow_single_file = True, mandatory = True, doc = "DuckDB's postgres extension."),
        "site": attr.label(allow_single_file = True, doc = "The site, a directory: the DataCube app's page (--app)."),
    },
    # bazel_lib's pinned coreutils and copy_directory binaries: no shell on any platform
    toolchains = COPY_FILE_TOOLCHAINS + [_COPY_DIRECTORY_TOOLCHAIN],
    doc = """The native warehouse as one folder, named after the target: `warehouse` (the server), DuckDB's library and
its postgres extension under their own names, and `site/` when a site is given (the DataCube app). `bazel run`
starts the server in that folder with the target's `args`; a package is the folder as an archive. The server knows
nothing of Bazel: it finds what is beside itself (DuckLibrary), and `--app` serves `site/`.""",
)
