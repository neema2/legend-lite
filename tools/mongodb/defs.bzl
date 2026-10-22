"""EMBEDDED_MONGODB_COMPATIBLE: the target_compatible_with of a target that starts the embedded MongoDB."""

load("//tools/platforms:defs.bzl", "compatible_with")
load(":mongodb.bzl", "BUILDS")

# A test that starts the embedded MongoDB declares it, so it is skipped where no build is pinned (Linux x86_64 only).
EMBEDDED_MONGODB_COMPATIBLE = compatible_with(BUILDS.keys())
