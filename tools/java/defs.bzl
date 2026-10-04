"""legend_java_library: every first-party java_library, with the null gate built in (Bazel workplan P1-20).

One place for what each library's javac gets, so a module cannot drift from the others: NullAway's plugin and flags
(//tools/nullaway) unless the target opts out, and LEGEND_JAVACOPTS, the options every first-party library shares
(where P3-28 turns on Error Prone's locale checks). Private by default: a library is visible outside its package
only when its BUILD file says to whom.
"""

load("@rules_java//java:defs.bzl", "java_library")
load("//tools/nullaway:defs.bzl", "NULLAWAY_OPTS", "NULLAWAY_PLUGIN")

# The javac options every first-party library shares, null gate or not.
LEGEND_JAVACOPTS = []

def legend_java_library(name, nullaway = True, visibility = ["//visibility:private"], javacopts = [], plugins = [], **kwargs):
    """A java_library with the repository's javac options and, unless `nullaway = False`, the null gate.

    Args:
        name: the target.
        nullaway: False for a library not yet written to the null discipline; its BUILD file says why.
        visibility: private unless given.
        javacopts: added after the shared options.
        plugins: added after NullAway's.
        **kwargs: everything else java_library takes.
    """
    java_library(
        name = name,
        javacopts = LEGEND_JAVACOPTS + (NULLAWAY_OPTS if nullaway else []) + javacopts,
        plugins = ([NULLAWAY_PLUGIN] if nullaway else []) + plugins,
        visibility = visibility,
        **kwargs
    )
