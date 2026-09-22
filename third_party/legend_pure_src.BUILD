# The pinned legend-pure SOURCE tree, as a Bazel input (MODULE.bazel: http_archive).
# Tests read it as the SPEC — corpus sources, platform .pure, parity fixtures — never
# as a runtime component (AGENTS.md, reference-checkout tenet). This replaces the
# hand-kept checkout at ~/legend/legend-pure, tools/oracle-pins.env and
# tools/oracle-roots.sh: the pin is the archive's sha256, so there is no "wrong
# checkout" to be on.
filegroup(
    name = "tree",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)
