"""What every generator build action shares: the pinned upstream trees as inputs,
their roots as arguments, and the JVM flags that point the corpus machinery at the
action's own inputs.

A generator is a PROGRAM run as a sandboxed build action over declared inputs,
writing one file; each package's write_source_files (update_generated) writes the
outputs into the checkout and diff-tests the committed copies.
"""

# The trees as declared inputs, plus a file each release has at its root.
UPSTREAM_TREES = [
    "@legend_engine_src//:pom.xml",
    "@legend_engine_src//:tree",
    "@legend_pure_src//:pom.xml",
    "@legend_pure_src//:tree",
]

# Shell expressions (genrule cmd) for the two roots: the directory of that file.
ENGINE_ROOT = "$$(dirname $(execpath @legend_engine_src//:pom.xml))"
PURE_ROOT = "$$(dirname $(execpath @legend_pure_src//:pom.xml))"

def program_flags(module, engine = True, pure = True):
    """java_binary launcher flags for a generator that reads through Repo/Upstream.

    Repo resolves against the action's working directory (the execroot, where
    declared inputs sit at their repository paths), as `module`.
    """
    flags = [
        "--jvm_flag=-Dlegend.repo.root=.",
        "--jvm_flag=-Dlegend.repo.module=" + module,
        "--jvm_flag=-Duser.timezone=GMT",
    ]
    if engine:
        flags.append("--jvm_flag=-Dlegend.engine.root=" + ENGINE_ROOT)
    if pure:
        flags.append("--jvm_flag=-Dlegend.pure.root=" + PURE_ROOT)
    return flags
