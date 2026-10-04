"""legend_java_library: every first-party java_library, with the null gate built in (Bazel workplan P1-20).

One place for what each library's javac gets, so a module cannot drift from the others: NullAway's plugin and flags
(//tools/nullaway) unless the target opts out, and LEGEND_JAVACOPTS, the options every first-party library shares
(where P3-28 turns on Error Prone's locale checks). Private by default: a library is visible outside its package
only when its BUILD file says to whom. Both macros check that the package may use every Maven pool its
dependencies name (tools/deps/pools.bzl, Bazel workplan P1-25).
"""

load("@rules_java//java:defs.bzl", "java_binary", "java_library")
load("//tools/deps:pools.bzl", "check_pool_use")
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
    check_pool_use(name, kwargs.get("deps", []), kwargs.get("runtime_deps", []), kwargs.get("exports", []))
    java_library(
        name = name,
        javacopts = LEGEND_JAVACOPTS + (NULLAWAY_OPTS if nullaway else []) + javacopts,
        plugins = ([NULLAWAY_PLUGIN] if nullaway else []) + plugins,
        visibility = visibility,
        **kwargs
    )

def legend_java_binary(name, javacopts = [], **kwargs):
    """A java_binary with the repository's javac options and the Maven pool check (no null gate: a binary's own
    sources are a main class; its libraries carry the gate).

    Args:
        name: the target.
        javacopts: added after the shared options.
        **kwargs: everything else java_binary takes.
    """
    check_pool_use(name, kwargs.get("deps", []), kwargs.get("runtime_deps", []))
    java_binary(
        name = name,
        javacopts = LEGEND_JAVACOPTS + javacopts,
        **kwargs
    )
