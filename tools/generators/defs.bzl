"""What every generator build action shares: the pinned upstream trees as inputs,
their roots as arguments, and the JVM flags that point the corpus machinery at the
action's own inputs.

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
    """JVM flags for a generator (java_run) that reads through Repo/Upstream.

    Repo resolves against the action's working directory (the execroot, where
    declared inputs sit at their repository paths), as `module`.
    """
    flags = [
        "-Dlegend.repo.root=.",
        "-Dlegend.repo.module=" + module,
        "-Duser.timezone=GMT",
    ]
    if engine:
        flags.append("-Dlegend.engine.root={ENGINE_ROOT}")
    if pure:
        flags.append("-Dlegend.pure.root={PURE_ROOT}")
    return flags
