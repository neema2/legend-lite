"""What every generator build action shares: the pinned upstream trees as inputs,
their roots as arguments, and the JVM flags that name each tree's root file.

A generator is a PROGRAM run as a sandboxed build action (java_run, which runs its
target-configuration jars — //tools/java_run) over declared inputs, writing one file; each package's write_source_files (update_generated) writes the
outputs into the checkout and diff-tests the committed copies.
"""

# The trees as declared inputs, plus a file each release has at its root.
UPSTREAM_TREES = [
    "@legend_engine_src//:pom.xml",
    "@legend_engine_src//:tree",
    "@legend_pure_src//:pom.xml",
    "@legend_pure_src//:tree",
]

# java_run's roots: each release's root, named by the file at it, as a token the
# generator's arguments and flags use
UPSTREAM_ROOTS = {
    "@legend_engine_src//:pom.xml": "{ENGINE_ROOT}",
    "@legend_pure_src//:pom.xml": "{PURE_ROOT}",
}

def program_jvm_flags(module, engine = True, pure = True):
    """JVM flags for a generator (java_run) that reads the pinned upstream trees (Bazel workplan P3-33).

    Each tree by the file at its root, as the action's exec path (ProgramPaths.rootOf): the action's srcs carry
    UPSTREAM_TREES. Nothing points at a repository root: a generator reads the files it is given.

    Args:
      module: the generator's package (kept for the callers' symmetry; nothing reads it now).
      engine: whether it reads legend-engine's tree.
      pure: whether it reads legend-pure's tree.
    """
    flags = ["-Duser.timezone=GMT"]
    if engine:
        flags.append("-Dlegend.engine.root=$(execpath @legend_engine_src//:pom.xml)")
    if pure:
        flags.append("-Dlegend.pure.root=$(execpath @legend_pure_src//:pom.xml)")
    return flags
