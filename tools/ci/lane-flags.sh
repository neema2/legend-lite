# shellcheck shell=bash
# THE BAZEL FLAGS OF A GATE LANE (.github/workflows/gates-run.yml), in one place: every step that runs Bazel for a
# lane sources this, so the steps build and test in one configuration and a later step reuses what an earlier one
# built (2026-10-08: the warehouse lane's builds became a step of their own, and their flags were the lane step's,
# copied). It reads the step's env -- LOW_MEMORY, PLATFORM, LINUX_ONLY_TESTS -- and defines three arrays for the
# step that sources it:
#   flags   every command's: the CI config and the repository cache;
#   qflags  the queries': taken before ci-small, a build-only config, which `query` refuses by name (the cold run of
#           L1e, macOS's product job, 2026-10-07);
#   tflags  the builds' and tests': the browser harnesses run on Linux only (their tag, gates/BUILD.bazel) -- the same
#           on every platform, and live-vs-snap already checks x86_64 against the arm64 desks; a desk still runs
#           them anywhere. The filter applies to the test command alone, with --build_tests_only, so a skipped
#           test's closure (the native image) is not built either; a dispatch with linux-only-tests=everywhere
#           lifts it.
# shellcheck disable=SC2034  # the arrays are the sourcing step's, not this file's
flags=(--config=ci --repository_cache="$HOME/.cache/bazel-repo")
qflags=("${flags[@]}")
if [ "$LOW_MEMORY" = "true" ]; then flags+=(--config=ci-small); fi
tflags=("${flags[@]}")
if [ "$LINUX_ONLY_TESTS" != everywhere ]; then
  case "$PLATFORM" in linux*) ;; *) tflags+=(--test_tag_filters=-ci-linux-only --build_tests_only) ;; esac
fi
