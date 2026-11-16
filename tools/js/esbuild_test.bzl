"""late_bundle_test: esbuild_bundle refuses a bundle declared after its package's guards_package() (tools/js/esbuild.bzl).

A refusal at loading time cannot be planted in a real package without breaking its load, so the plant is the decision
itself, asked with a stand-in for native.existing_rule (2026-11-15, the C5-T re-check's item 3).
"""

load("@bazel_skylib//lib:unittest.bzl", "asserts", "unittest")
load(":esbuild.bzl", "late_bundle_error")

def _late_bundle_test_impl(ctx):
    env = unittest.begin(ctx)

    # before guards_package(): no guard_bundles yet, the bundle declared
    asserts.equals(env, None, late_bundle_error("bundle", lambda name: None))

    # after it: guard_bundles is there, and the bundle is refused by name
    after = late_bundle_error("bundle", lambda name: {"kind": "filegroup"} if name == "guard_bundles" else None)
    asserts.true(env, after != None and after.startswith("esbuild_bundle bundle is declared after guards_package()"), str(after))
    return unittest.end(env)

late_bundle_test = unittest.make(_late_bundle_test_impl)
