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
path against (Repo.listed).
"""

load("@rules_java//java/common:java_info.bzl", "JavaInfo")

def _java_jars_impl(ctx):
    if ctx.attr.transitive:
        jars = depset(transitive = [d[JavaInfo].transitive_runtime_jars for d in ctx.attr.deps]).to_list()
    else:
        # the targets' own jars, in the order the deps are given
        jars = [j for d in ctx.attr.deps for j in d[JavaInfo].runtime_output_jars]
    out = ctx.actions.declare_file(ctx.label.name + ".jars")
    ctx.actions.write(out, "".join([(j.path if ctx.attr.exec_paths else j.short_path) + "\n" for j in jars]))
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
    },
    doc = "Writes <name>.jars: the deps' jars, one path per line.",
)
