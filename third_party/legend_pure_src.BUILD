# The pinned legend-pure SOURCE tree, as a Bazel input (MODULE.bazel: http_archive).
# Tests read it as the SPEC — corpus sources, platform .pure, parity fixtures — never
# as a runtime component (AGENTS.md, reference-checkout tenet). This replaces the
# hand-kept checkout at ~/legend/legend-pure (and the script that checked which
# commit it sat on): the pin is the archive's sha256, so there is no "wrong
# checkout" to be on.
filegroup(
    name = "tree",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)

# every file, individually addressable, for generators that read one named file
exports_files(glob(["**"]))
