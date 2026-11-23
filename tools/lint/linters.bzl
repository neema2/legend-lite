"""Ruff and ty over the Python targets (2026-11-23): rules_lint's aspects, each on the binary that release pins,
both reading //:pyproject.toml.

Report without failing: `bazel build --config=lint //python/...` (Ruff), `--config=typecheck` (ty); the configs are
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

ty_test = lint_test(aspect = ty)
