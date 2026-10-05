"""java_jars: a Java target's jars, as a DECLARED input — a file listing them.

A program that reads jars (to enumerate the classes in them) is told which by the
build, which knows them (JavaInfo), instead of re-deriving them from its own class
path. java.class.path is the launcher's business: on Windows, and past a length
limit on Linux and macOS, Bazel's launcher puts ONE manifest-only jar there, and a
scan of it finds none of the jars it names (the first Bazel CI runs, 2026-09-23).

The list file has one path per line:
  * for a TEST (`exec_paths = False`): each jar's path relative to the main
    repository's runfiles directory (`../<repo>/…` for an external one), and the
    jars ride in the target's runfiles;
  * for a BUILD ACTION (`exec_paths = True`): each jar's execroot path; the action
    also takes the jars themselves as inputs, through a filegroup over this
    target's `jars` output group.
Either way a reader resolves a line against the root it resolves the list's own
path against (ProgramPaths.listed reads either form).
"""

load("@rules_java//java/common:java_info.bzl", "JavaInfo")

def _java_jars_impl(ctx):
    if ctx.attr.transitive:
        jars = depset(transitive = [d[JavaInfo].transitive_runtime_jars for d in ctx.attr.deps]).to_list()
    else:
        # the targets' own jars, in the order the deps are given
        jars = [j for d in ctx.attr.deps for j in d[JavaInfo].runtime_output_jars]
    out = ctx.actions.declare_file(ctx.label.name + ".jars")
    if ctx.attr.exec_paths and ctx.attr.rlocation_paths:
        fail("%s: exec_paths and rlocation_paths are two formats; set one" % ctx.label)
    if ctx.attr.exec_paths:
        lines = [j.path for j in jars]
    elif ctx.attr.rlocation_paths:
        lines = [_rlocationpath(ctx, j) for j in jars]
    else:
        lines = [j.short_path for j in jars]
    ctx.actions.write(out, "".join([line + "\n" for line in lines]))
    return [
        DefaultInfo(files = depset([out]), runfiles = ctx.runfiles(files = [out] + jars)),
        OutputGroupInfo(jars = depset(jars)),
    ]

java_jars = rule(
    implementation = _java_jars_impl,
    attrs = {
        "deps": attr.label_list(providers = [JavaInfo], mandatory = True),
        "transitive": attr.bool(
            default = True,
            doc = "Every jar the deps run with (True), or only the deps' own jars, in order.",
        ),
        "exec_paths": attr.bool(
            default = False,
            doc = "List execroot paths (a build action's input) instead of runfiles paths (a test's).",
        ),
        "rlocation_paths": attr.bool(
            default = False,
            doc = "List each jar as $(rlocationpath) gives it (<repo>/<path>), for Runfile.listed: the same under a " +
                  "runfiles tree and a manifest alone (Bazel workplan P3-27, A1's list format).",
        ),
    },
    doc = "Writes <name>.jars: the deps' jars, one path per line.",
)


def _rlocationpath(ctx, f):
    # what $(rlocationpath) gives: <repository>/<path>, the main repository by its workspace name
    return f.short_path[len("../"):] if f.short_path.startswith("../") else ctx.workspace_name + "/" + f.short_path

def _file_list_impl(ctx):
    files = sorted(depset(transitive = [t[DefaultInfo].files for t in ctx.attr.srcs]).to_list(), key = lambda f: f.short_path)
    out = ctx.actions.declare_file(ctx.label.name + ".files")
    ctx.actions.write(out, "".join([_rlocationpath(ctx, f) + "\n" for f in files]))
    return [DefaultInfo(files = depset([out]), runfiles = ctx.runfiles(files = [out] + files))]

file_list = rule(
    implementation = _file_list_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True, mandatory = True),
    },
    doc = """Writes <name>.files: every file of srcs by its runfiles path, one per line, sorted (Bazel workplan P3-27).

A test names the list with ONE -D<property>=$(rlocationpath :<name>) and reads it with SourceFiles (//testing): a set
of hundreds of files in jvm_flags would pass Windows' 32,767-character command-line limit (A4). The files ride in the
list's runfiles, so a test that depends on the list has them.""",
)
