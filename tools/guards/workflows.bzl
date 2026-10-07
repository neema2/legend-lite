"""git_marker: a `.git` entry at the root of a test's runfiles, for a tool that finds its project by it.

actionlint (//tools/guards:workflows_test) checks a call from one workflow to another (gate.yml's `uses:
./.github/workflows/gates-run.yml` and the inputs it passes) only inside a "project": a directory holding both
`.github/workflows` and `.git`. A test's runfiles hold the workflows but no `.git`, and without one the check is
silently skipped (found 2026-10-07: an undefined input to the reusable workflow was reported only in a directory with
a `.git`). An empty FILE named `.git` is enough (actionlint tests for existence, as a git worktree's `.git` is a file
too), so this rule's runfiles are one symlink, `.git`, to an empty file it writes.
"""

def _git_marker_impl(ctx):
    marker = ctx.actions.declare_file(ctx.label.name + ".marker")
    ctx.actions.write(marker, "")
    return [DefaultInfo(runfiles = ctx.runfiles(symlinks = {".git": marker}))]

git_marker = rule(
    implementation = _git_marker_impl,
    doc = "Runfiles with one entry, `.git`, an empty file at the runfiles root.",
)
