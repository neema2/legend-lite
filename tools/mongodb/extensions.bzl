"""The embedded MongoDB repositories: one per platform with a pinned build, lazily fetched, and the hub."""

load(":mongodb.bzl", "BUILDS", "embedded_mongodb_hub", "embedded_mongodb_platform")

def _embedded_mongodb_impl(module_ctx):
    for platform, (arch, sha256) in BUILDS.items():
        embedded_mongodb_platform(
            name = "embedded_mongodb_" + platform,
            arch = arch,
            platform = platform,
            sha256 = sha256,
        )
    embedded_mongodb_hub(name = "embedded_mongodb", platforms = BUILDS.keys())
    return module_ctx.extension_metadata(reproducible = True)

embedded_mongodb = module_extension(implementation = _embedded_mongodb_impl)
