"""The embedded Deephaven repository: one for every platform (the server is Java), lazily fetched."""

load(":deephaven.bzl", "embedded_deephaven_repository")

def _embedded_deephaven_impl(module_ctx):
    embedded_deephaven_repository(name = "embedded_deephaven")
    return module_ctx.extension_metadata(reproducible = True)

embedded_deephaven = module_extension(implementation = _embedded_deephaven_impl)
