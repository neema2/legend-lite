"""core_layer_queries: one dependency query per core library, from the libraries themselves (Bazel workplan P2-12).

Called at the end of core/BUILD.bazel (before guards_package), it makes, for every java_library of the package but
the NOT_LAYERS it is given, a genquery `layer_<name>` of the library's direct deps, and `layer_queries`, all of them,
for //tools/deps:core_layering_test, which holds each against tools/deps/core-layers.txt (the hand-owned POLICY,
D9). A new library is in the test at once: with no line in core-layers.txt it fails, naming it. No hand list of
core's targets remains outside core-layers.txt and this macro's named exceptions.
"""

def core_layer_queries(not_layers):
    """Args:
      not_layers: library name -> why it is not a layer (an umbrella, a test library, ...).
    """
    libraries = [r["name"] for r in native.existing_rules().values() if r["kind"] == "java_library"]
    stale = sorted([n for n in not_layers if n not in libraries])
    if stale:
        fail("core_layer_queries: not_layers names %s, which is no java_library of this package" % stale)
    names = sorted([n for n in libraries if n not in not_layers])
    for name in names:
        native.genquery(
            name = "layer_" + name,
            expression = "labels(deps, //core:%s)" % name,
            scope = [":" + name],
            visibility = ["//tools/deps:__pkg__"],
        )
    native.filegroup(
        name = "layer_queries",
        srcs = [":layer_" + n for n in names],
        visibility = ["//tools/deps:__pkg__"],
    )
