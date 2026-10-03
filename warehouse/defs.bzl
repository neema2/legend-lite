"""The warehouse's build helpers: jar_entry, POSTGRES_EXTENSION and warehouse_run.

jar_entry takes one file out of a Java library's jar, as a build output. The warehouse's native
image loads DuckDB's native library from beside it (or from --duckdb-library), and that library
rides inside DuckDB's JDBC jar, one per platform. This takes it out with Bazel's own zipper, in an
action, so the file is an ordinary Bazel output with a runfiles path -- never unzipped by a script.

warehouse_run is `bazel run`'s launcher for the native warehouse (docs/WINDOWS_APP_DESIGN_2026_10_02.md).
"""

load("@hermetic_launcher//launcher:launcher_binary.bzl", "launcher_binary")
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

# DuckDB's postgres extension for the platform being built, as MODULE.bazel pins it: one choice for
# every launcher (//warehouse:serve, //datacube:app).
POSTGRES_EXTENSION = select({
    "//warehouse:macos_arm64": "@duckdb_postgres_extension_osx_arm64//file",
    "//warehouse:macos_x86_64": "@duckdb_postgres_extension_osx_amd64//file",
    "//warehouse:linux_x86_64": "@duckdb_postgres_extension_linux_amd64//file",
    "//warehouse:linux_aarch64": "@duckdb_postgres_extension_linux_arm64//file",
    "//warehouse:windows_x86_64": "@duckdb_postgres_extension_windows_amd64//file",
}, no_match_error = "no DuckDB postgres extension is pinned for this platform (MODULE.bazel, duckdb_postgres_extension_*; warehouse/defs.bzl POSTGRES_EXTENSION)")

# Windows on ARM: DuckDB's JDBC jar carries no windows_arm64 library, MODULE.bazel pins no postgres
# extension for it, and hermetic-launcher registers no windows/aarch64 stub. Its native targets are
# incompatible there, so `bazel build //...` skips them rather than failing analysis on a select that
# has no arm for it (review of neema2/legend-lite#14, 2026-10-03).
NOT_ON_WINDOWS_ARM64 = select({
    "//warehouse:windows_aarch64": ["@platforms//:incompatible"],
    "//conditions:default": [],
})

def _duckdb_extensions_impl(ctx):
    # DuckDB loads an extension file by name: the download is gzipped, so it is unpacked here, under the
    # exact name the server looks for, into a directory of its own -- what --duckdb-extensions names, and
    # what a launcher can name too (a runfiles manifest lists a directory output, not a file's parent)
    out = ctx.actions.declare_directory(ctx.label.name)
    ctx.actions.run_shell(
        inputs = [ctx.file.gz],
        outputs = [out],
        command = "mkdir -p \"$2\" && gzip -dc \"$1\" > \"$2/postgres_scanner.duckdb_extension\"",
        arguments = [ctx.file.gz.path, out.path],
        mnemonic = "GunzipDuckdbExtension",
    )
    return [DefaultInfo(files = depset([out]))]

_duckdb_extensions = rule(
    implementation = _duckdb_extensions_impl,
    attrs = {"gz": attr.label(allow_single_file = True, mandatory = True)},
    doc = "DuckDB's postgres extension, gunzipped into a directory of its own.",
)

def _posix_launcher_impl(ctx):
    server = ctx.executable.server
    files = [server, ctx.file.library, ctx.file.extensions]
    fixed = ""
    if ctx.file.site:
        files.append(ctx.file.site)
        fixed += " --site \"$here/{}\"".format(ctx.file.site.short_path)
    for arg in ctx.attr.args_before:
        fixed += " " + shell_quote(arg)
    script = ctx.actions.declare_file(ctx.label.name + ".sh")
    ctx.actions.write(script, is_executable = True, content = """#!/usr/bin/env bash
# The warehouse with everything it loads beside it -- DuckDB's library, its postgres extension and,
# for the app, the DataCube site -- from runfiles; then it runs where `bazel run` was started, so a
# relative path among the caller's arguments is the caller's.
set -euo pipefail
here="${{RUNFILES_DIR:-$0.runfiles}}/_main"
[[ -d "$here" ]] || here="$(pwd)"
server="$here/{server}"
library="$here/{library}"
extensions="$here/{extensions}"
cd "${{BUILD_WORKING_DIRECTORY:-.}}"
exec "$server" --duckdb-library "$library" --duckdb-extensions "$extensions"{fixed} "$@"
""".format(
        server = server.short_path,
        library = ctx.file.library.short_path,
        extensions = ctx.file.extensions.short_path,
        fixed = fixed,
    ))
    runfiles = ctx.runfiles(files = files).merge(ctx.attr.server[DefaultInfo].default_runfiles)
    return [DefaultInfo(executable = script, runfiles = runfiles)]

def shell_quote(s):
    return "'" + s.replace("'", "'\\''") + "'"

_posix_launcher = rule(
    implementation = _posix_launcher_impl,
    executable = True,
    attrs = {
        "server": attr.label(executable = True, cfg = "target", mandatory = True),
        "library": attr.label(allow_single_file = True, mandatory = True),
        "extensions": attr.label(allow_single_file = True, mandatory = True, doc = "A directory (--duckdb-extensions)."),
        "site": attr.label(allow_single_file = True, doc = "A directory served as the page (--site)."),
        "args_before": attr.string_list(doc = "Fixed arguments, before the caller's."),
    },
    doc = "macOS and Linux: a bash script that execs the native warehouse with its files from runfiles.",
)

def warehouse_run(name, server, library, postgres_extension_gz, site = None, args_before = [], testonly = False):
    """`bazel run`'s launcher for the native warehouse, one name on every platform.

    DuckDB's library, its postgres extension and (for the app) a site go beside the server, then the
    caller's arguments. On macOS and Linux a bash script execs the server where `bazel run` was started.
    On Windows `bazel run` can start no script, and a .bat or Bazel's bash launcher splits a Postgres URL
    at '&', so a hermetic-launcher stub starts the server with its arguments intact; it runs the server in
    the runfiles folder, so the server resolves a relative --data against BUILD_WORKING_DIRECTORY itself
    (docs/WINDOWS_APP_DESIGN_2026_10_02.md, §2 and its known limits). Windows x64 only: on Windows ARM64
    the targets are incompatible (NOT_ON_WINDOWS_ARM64).

    Args:
        name: the target `bazel run` runs (an alias of <name>_posix or <name>_windows).
        server: the native warehouse (//warehouse:server_native).
        library: DuckDB's native library for the platform (//warehouse:duckdb_library).
        postgres_extension_gz: DuckDB's postgres extension download (POSTGRES_EXTENSION).
        site: a directory served as the page (--site), or None.
        args_before: fixed arguments, before the caller's.
        testonly: for a launcher only tests run (//warehouse:launcher_test_serve_site).
    """
    extensions = name + "_extensions"
    _duckdb_extensions(
        name = extensions,
        gz = postgres_extension_gz,
        testonly = testonly,
        target_compatible_with = NOT_ON_WINDOWS_ARM64,
    )
    _posix_launcher(
        name = name + "_posix",
        server = server,
        library = library,
        extensions = ":" + extensions,
        site = site,
        args_before = args_before,
        testonly = testonly,
        target_compatible_with = select({
            "@platforms//os:windows": ["@platforms//:incompatible"],
            "//conditions:default": [],
        }),
    )
    embedded = [
        "--duckdb-library",
        "$(rlocationpath %s)" % library,
        "--duckdb-extensions",
        "$(rlocationpath :%s)" % extensions,
    ]
    data = [library, ":" + extensions]
    if site:
        embedded += ["--site", "$(rlocationpath %s)" % site]
        data.append(site)

    # the stub holds ten arguments, the entrypoint among them (hermetic-launcher 0.0.16's finalizer:
    # "Maximum 10 arguments supported"); the app uses nine. The finalizer runs only when the Windows
    # target is built, so one more would break only Windows desks: refused here, on every platform,
    # when the BUILD file loads (measured 2026-10-03: ten built, eleven refused).
    if 1 + len(embedded) + len(args_before) > 10:
        fail("warehouse_run %s: %d launcher arguments (the server, %d fixed, %d args_before); hermetic-launcher's Windows stub holds 10" %
             (name, 1 + len(embedded) + len(args_before), len(embedded), len(args_before)))
    launcher_binary(
        name = name + "_windows",
        entrypoint = server,
        embedded_args = embedded + args_before,
        data = data,
        testonly = testonly,
        target_compatible_with = select({
            "//warehouse:windows_x86_64": [],
            "//conditions:default": ["@platforms//:incompatible"],
        }),
    )
    native.alias(
        name = name,
        actual = select({
            "@platforms//os:windows": ":" + name + "_windows",
            "//conditions:default": ":" + name + "_posix",
        }),
        testonly = testonly,
    )
