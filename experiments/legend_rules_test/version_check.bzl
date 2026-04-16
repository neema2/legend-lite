# Module extension that checks external repo versions.
#
# Key insight: repository rules CAN execute external programs!
# rctx.execute(["git", "ls-remote", ...]) works.
#
# This means we can:
# 1. Check the latest commit on a remote repo's main branch
# 2. Compare it with the pinned commit
# 3. Generate a report or even auto-update the commit hash

def _version_check_repo_impl(rctx):
    """Check if an external repo has a newer version available."""

    # Execute git ls-remote to get latest commit on main
    result = rctx.execute(
        ["git", "ls-remote", rctx.attr.remote, "refs/heads/" + rctx.attr.branch],
        timeout = 10,
    )

    latest_commit = ""
    if result.return_code == 0 and result.stdout.strip():
        latest_commit = result.stdout.strip().split("\t")[0]

    is_outdated = latest_commit != "" and latest_commit != rctx.attr.current_commit

    # Generate a status file
    status = "OUTDATED" if is_outdated else "UP_TO_DATE"
    rctx.file("BUILD.bazel", "# Version check status\n")
    rctx.file("status.bzl", """
STATUS = "{status}"
REPO = "{repo}"
CURRENT_COMMIT = "{current}"
LATEST_COMMIT = "{latest}"
BRANCH = "{branch}"
""".format(
        status = status,
        repo = rctx.attr.repo_name,
        current = rctx.attr.current_commit,
        latest = latest_commit,
        branch = rctx.attr.branch,
    ))

    # Also generate an updated external_repos.bzl snippet if outdated
    if is_outdated:
        rctx.file("update.bzl", """
# Run: bazel build @{name}//:update.bzl to see this
# Then copy the new commit hash to your external_repos.bzl
UPDATED_GIT_REPOSITORY = '''
git_repository(
    name = "{repo}",
    remote = "{remote}",
    commit = "{latest}",  # was: {current}
)
'''
""".format(
            name = rctx.attr.repo_name + "_version",
            repo = rctx.attr.repo_name,
            remote = rctx.attr.remote,
            latest = latest_commit,
            current = rctx.attr.current_commit,
        ))
    else:
        rctx.file("update.bzl", "UPDATED_GIT_REPOSITORY = None  # already up to date\n")

_version_check_repo = repository_rule(
    implementation = _version_check_repo_impl,
    attrs = {
        "repo_name": attr.string(mandatory = True),
        "remote": attr.string(mandatory = True),
        "current_commit": attr.string(mandatory = True),
        "branch": attr.string(default = "main"),
    },
)

def _version_check_impl(mctx):
    for tag in mctx.modules[0].tags.repo:
        _version_check_repo(
            name = tag.name + "_version",
            repo_name = tag.name,
            remote = tag.remote,
            current_commit = tag.current_commit,
            branch = tag.branch if tag.branch else "main",
        )

_repo_tag = tag_class(attrs = {
    "name": attr.string(mandatory = True),
    "remote": attr.string(mandatory = True),
    "current_commit": attr.string(mandatory = True),
    "branch": attr.string(default = "main"),
})

version_check = module_extension(
    implementation = _version_check_impl,
    tag_classes = {"repo": _repo_tag},
)
