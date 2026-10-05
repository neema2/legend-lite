"""oracle_pins: the upstream release's pins as a KEY=VALUE file, from release.MODULE.bazel (Bazel workplan P2-10).

The tests and generators read the release they are held to (parser-equivalence's OraclePins, the fixture snapshot's
header check) from this file; release.MODULE.bazel is the one place it is written, and //tools:oracle-pins.env puts
it in this repository's namespace.
"""

def _oracle_pins_impl(rctx):
    rctx.file("oracle-pins.env", "".join([
        "# THE ORACLE PINS, generated from release.MODULE.bazel (//tools:oracle_pins.bzl). Plain KEY=VALUE.\n",
    ] + ["%s=%s\n" % (k, v) for k, v in rctx.attr.pins.items()]))
    rctx.file("BUILD.bazel", 'exports_files(["oracle-pins.env"], visibility = ["@@//tools:__pkg__"])\n')

oracle_pins = repository_rule(
    implementation = _oracle_pins_impl,
    attrs = {"pins": attr.string_dict(mandatory = True, doc = "KEY -> value, in the file's order.")},
    doc = "Writes the release's pins as oracle-pins.env.",
)
