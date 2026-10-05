"""legend_library: a Legend model project as a target, compiled alone with its declared dependencies (Bazel workplan
P3-23).

A legend_library's files are its own .pure sources and, transitively, its dependencies'. `<name>_test` compiles exactly
that closure with legend-lite (CompilesTest, tools/legend): a project that compiles only beside a project it does not
declare fails, which is the defect projects/CONTRACT.md's dependency rule exists to catch. legend_graph_test compiles
several libraries at once, where collisions between projects (a table or set id declared twice) show.

A `quarantine` is a known legend-lite failure, recorded with its finding: the test then holds the library to failing
with that message, and turns red when it compiles, so the row comes out the day the defect is fixed.
"""

load("//tools/junit:defs.bzl", "junit_test")

LegendLibraryInfo = provider(
    doc = "A Legend model project's sources.",
    fields = {"srcs": "depset of .pure files: the project's own and its dependencies', dependencies first"},
)

def _legend_sources_impl(ctx):
    srcs = depset(ctx.files.srcs, transitive = [d[LegendLibraryInfo].srcs for d in ctx.attr.deps], order = "postorder")
    return [LegendLibraryInfo(srcs = srcs), DefaultInfo(files = srcs)]

_legend_sources = rule(
    implementation = _legend_sources_impl,
    attrs = {
        "deps": attr.label_list(providers = [LegendLibraryInfo]),
        "srcs": attr.label_list(allow_files = [".pure"], allow_empty = False),
    },
)

def _compiles_test(name, libraries, memory_mb, size, quarantine):
    env = {"LEGEND_LIBRARY_SRCS": " ".join(["$(rlocationpaths %s)" % l for l in libraries])}
    if quarantine:
        env["LEGEND_LIBRARY_QUARANTINE"] = quarantine
    junit_test(
        name = name,
        size = size,
        memory_mb = memory_mb,
        data = libraries,
        # an environment variable stays one value (Runfile.envList); a JVM flag would be split
        env = env,
        runtime_deps = ["//tools/legend:compiles_test_lib"],
        select = ["--select-class=com.legend.tools.legend.CompilesTest"],
    )

def legend_library(name, srcs, deps = [], quarantine = None, visibility = None):
    """`name`, the project's sources with its dependencies', and `<name>_test`, which compiles them.

    Args:
      name: the project.
      srcs: its .pure files.
      deps: the legend_library targets it may refer to; nothing else is in its compile.
      quarantine: the message of a known legend-lite failure (with its dated finding beside it), or None.
      visibility: the library's.
    """
    _legend_sources(name = name, srcs = srcs, deps = deps, visibility = visibility)
    _compiles_test(name = name + "_test", libraries = [":" + name], memory_mb = 1024, size = "small", quarantine = quarantine)

def legend_graph_test(name, libraries, memory_mb = 2048, quarantine = None):
    """Compiles every one of `libraries` together, where collisions between them show.

    Args:
      name: the test.
      libraries: the legend_library targets.
      memory_mb: the JVM's heap (junit_test's).
      quarantine: as legend_library's.
    """
    _compiles_test(name = name, libraries = libraries, memory_mb = memory_mb, size = "medium", quarantine = quarantine)
