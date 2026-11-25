"""Ruff and ty over the Python targets (2026-11-23): rules_lint's aspects, each on the binary that release pins,
both reading //:pyproject.toml.

By hand, a finding failing the build: `bazel build --config=lint //python/...` (Ruff), `--config=typecheck` (ty); the configs are
.bazelrc's. Held in the gate by the tests below: `ruff_test(name = ..., srcs = [<py targets>])`, and `ty_test`.
"""

load("@aspect_rules_lint//lint:lint_test.bzl", "lint_test")
load("@aspect_rules_lint//lint:ruff.bzl", "lint_ruff_aspect")
load("@aspect_rules_lint//lint:ty.bzl", "lint_ty_aspect")

ruff = lint_ruff_aspect(
    binary = Label("@aspect_rules_lint//lint:ruff_bin"),
    configs = [Label("//:pyproject.toml")],
)

ruff_test = lint_test(aspect = ruff)

ty = lint_ty_aspect(
    binary = Label("@aspect_rules_lint//lint:ty_bin"),
    config = Label("//:pyproject.toml"),
)

# ty finds a wheel's modules by the import path its target declares, and a wheel declares one only outside the venv
# layout the build runs Python in (.bazelrc: venvs_site_packages). So a ty_test reads its targets with that layout off --
# the analysis alone: nothing here runs Python -- as `--config=typecheck` does for a build.
_VENVS_SITE_PACKAGES = "@rules_python//python/config_settings:venvs_site_packages"

def _site_packages_off_impl(_settings, _attr):
    return {_VENVS_SITE_PACKAGES: "no"}

_site_packages_off = transition(
    implementation = _site_packages_off_impl,
    inputs = [],
    outputs = [_VENVS_SITE_PACKAGES],
)

def _ty_reports_impl(ctx):
    return [OutputGroupInfo(
        rules_lint_human = depset(transitive = [src[OutputGroupInfo].rules_lint_human for src in ctx.attr.srcs]),
    )]

_ty_reports = rule(
    implementation = _ty_reports_impl,
    doc = "ty's reports on the targets, each read with rules_python's venv layout off.",
    attrs = {
        "srcs": attr.label_list(aspects = [ty], cfg = _site_packages_off),
    },
)

def _no_reports_impl(_target, _ctx):
    return []

# rules_lint's test over reports made already (:_ty_reports): its own aspect adds none
_no_reports = aspect(implementation = _no_reports_impl)

_reports_test = lint_test(aspect = _no_reports)

def ty_test(name, srcs, **kwargs):
    """Fails when ty reports anything on the Python targets `srcs` (and on what they depend on in this repository).

    Args:
      name: the test's name.
      srcs: py_library, py_binary and py_test targets.
      **kwargs: the test's other attributes (tags, target_compatible_with, size).
    """
    _ty_reports(
        name = name + "_reports",
        testonly = True,
        srcs = srcs,
        tags = ["manual"],
        target_compatible_with = kwargs.get("target_compatible_with"),
    )
    _reports_test(
        name = name,
        srcs = [name + "_reports"],
        **kwargs
    )
