"""java_run: a JVM program as a build action, over TARGET-configuration jars.

A build tool must run on the build machine, so Bazel builds a tool — and every
library it links — in the EXEC configuration: a genrule's java_binary tool meant a
second compile of everything it depends on (core, for the spec generators). For
the JVM, only the `java` executable is machine-specific; bytecode is not. So this
takes the program as ordinary target-configuration deps — compiled once, the same
jars the tests run — and runs them on the exec toolchain's Java runtime.

Arguments and JVM flags expand $(location)/$(execpath) against `srcs` and `roots`,
`{OUT}` to the (single) output's path, and each token of `roots` to the directory
of its file (a tree's root, named by a file that sits at it).
"""

load("@rules_java//java/common:java_common.bzl", "java_common")
load("@rules_java//java/common:java_info.bzl", "JavaInfo")

def _java_run_impl(ctx):
    runtime = ctx.attr._java_runtime[java_common.JavaRuntimeInfo]
    jars = depset(transitive = [d[JavaInfo].transitive_runtime_jars for d in ctx.attr.deps])
    if len(ctx.outputs.outs) == 0:
        fail("java_run needs at least one output")
    dirs = {}
    for target, token in ctx.attr.roots.items():
        files = target.files.to_list()
        if len(files) != 1:
            fail("a root is named by ONE file at it; %s has %d" % (target.label, len(files)))
        dirs[token] = files[0].dirname
    # a root's file may be among srcs too; expand_location wants each label once
    targets = {t.label: t for t in ctx.attr.srcs + ctx.attr.roots.keys()}.values()

    def expand(s):
        s = ctx.expand_location(s, targets = targets)
        for token, d in dirs.items():
            s = s.replace(token, d)
        if "{OUT}" in s:
            if len(ctx.outputs.outs) != 1:
                fail("{OUT} names the single output; this rule has %d" % len(ctx.outputs.outs))
            s = s.replace("{OUT}", ctx.outputs.outs[0].path)
        return s

    # Everything goes to `java` in an ARGUMENT FILE (java @file, JDK 9+): a class
    # path can outrun Windows' 32,767-character command line (the harvest's, with
    # the engine's test jars, did — CI, 2026-09-23). One argument per line; the
    # launcher splits a line on whitespace, so none may contain any.
    args = ctx.actions.args()
    args.use_param_file("@%s", use_always = True)
    args.set_param_file_format("multiline")
    flat = [expand(f) for f in ctx.attr.jvm_flags] + ["-cp"]
    for a in flat + [ctx.attr.main_class] + [expand(a) for a in ctx.attr.arguments]:
        if " " in a or "\t" in a:
            fail("java_run: an argument with whitespace cannot ride the argument file: %r" % a)
    args.add_all(flat)
    args.add_joined(jars, join_with = ctx.configuration.host_path_separator)
    args.add(ctx.attr.main_class)
    args.add_all([expand(a) for a in ctx.attr.arguments])
    ctx.actions.run(
        executable = runtime.java_executable_exec_path,
        arguments = [args],
        inputs = depset(ctx.files.srcs + ctx.files.roots, transitive = [jars, runtime.files]),
        outputs = ctx.outputs.outs,
        mnemonic = ctx.attr.mnemonic,
        progress_message = "%s %%{label}" % ctx.attr.mnemonic,
    )
    return [DefaultInfo(files = depset(ctx.outputs.outs))]

java_run = rule(
    implementation = _java_run_impl,
    attrs = {
        "main_class": attr.string(mandatory = True),
        "deps": attr.label_list(
            providers = [JavaInfo],
            mandatory = True,
            doc = "The program and what it links — TARGET configuration: compiled once.",
        ),
        "srcs": attr.label_list(allow_files = True, doc = "Files the program reads."),
        "roots": attr.label_keyed_string_dict(
            allow_files = True,
            doc = "A file -> a token replaced by that file's directory (a tree's root).",
        ),
        "outs": attr.output_list(mandatory = True),
        "arguments": attr.string_list(),
        "jvm_flags": attr.string_list(),
        "mnemonic": attr.string(default = "JavaRun"),
        "_java_runtime": attr.label(
            default = "@rules_java//toolchains:current_host_java_runtime",
            cfg = "exec",
            providers = [java_common.JavaRuntimeInfo],
        ),
    },
    doc = "Runs main_class over deps' target-configuration jars on the exec Java runtime.",
)
