"""EMBEDDED_POSTGRES_COMPATIBLE: the target_compatible_with of a target that starts the embedded Postgres."""

load("//tools/platforms:defs.bzl", "compatible_with")
load(":postgres.bzl", "BUILDS")

# A test that starts the embedded Postgres declares it, so it is skipped where no build is pinned (P1-15).
EMBEDDED_POSTGRES_COMPATIBLE = compatible_with(BUILDS.keys())
