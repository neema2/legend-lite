"""The embedded Postgres repositories (Bazel workplan P1-15): one per platform, lazily fetched, and the hub."""

load(":postgres.bzl", "BUILDS", "embedded_postgres_hub", "embedded_postgres_platform")

def _embedded_postgres_impl(module_ctx):
    for platform, (suffix, archive, integrity) in BUILDS.items():
        embedded_postgres_platform(
            name = "embedded_postgres_" + platform,
            archive = archive,
            platform = platform,
            integrity = integrity,
            suffix = suffix,
        )
    embedded_postgres_hub(name = "embedded_postgres", platforms = BUILDS.keys())
    return module_ctx.extension_metadata(reproducible = True)

embedded_postgres = module_extension(implementation = _embedded_postgres_impl)
