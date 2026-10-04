"""classpath_report: the Maven coordinates on each JVM target's runtime classpath (G11, Bazel workplan P6-11).

For each target (a java_test or java_binary), every jar of its runtime classpath that a rules_jvm_external pool
supplies, as one line: `<target>\\t<group>:<artifact>:<version>\\t<pool>`. A pool keeps a jar at
`<pool repository>/<group dirs>/<artifact>/<version>/<file>.jar`, so the path names the coordinate.
//tools/guards:classpath_test reads every package's report (guards_package) and fails on two versions of one
group:artifact in one classpath. Covered: java_test, java_binary and java_run (the build steps). Not covered: jars
from outside rules_jvm_external (a BCR module's), the java_jars lists some tests load at run time, and a package's
report on a platform where one of its JVM targets is incompatible (the report is then incompatible too).
"""

load("@rules_java//java/common:java_common.bzl", "java_common")

def _coordinate(jar):
    marker = "++maven+"
    path = jar.path
    i = path.find(marker)
    if i < 0:
        return None
    rest = path[i + len(marker):].split("/")

    # rest: <pool>, <group dirs...>, <artifact>, <version>, <file>
    if len(rest) < 5:
        return None
    return (rest[0], ".".join(rest[1:-3]), rest[-3], rest[-2])

def _classpath_report_impl(ctx):
    lines = []
    for target in ctx.attr.targets:
        # a test's or binary's runtime classpath (its JavaInfo carries no runtime jars)
        if java_common.JavaRuntimeClasspathInfo not in target:
            continue
        for jar in target[java_common.JavaRuntimeClasspathInfo].runtime_classpath.to_list():
            c = _coordinate(jar)
            if c:
                lines.append("%s\t%s:%s:%s\t%s" % (target.label, c[1], c[2], c[3], c[0]))
    out = ctx.actions.declare_file(ctx.label.name + ".tsv")
    ctx.actions.write(out, "\n".join(sorted({l: True for l in lines}.keys())) + "\n")
    return [DefaultInfo(files = depset([out]))]

classpath_report = rule(
    implementation = _classpath_report_impl,
    attrs = {"targets": attr.label_list(doc = "java_test and java_binary targets.")},
    doc = "The Maven coordinates on each target's runtime classpath.",
)
