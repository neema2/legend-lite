# Copyright 2026 Legend Contributors
# SPDX-License-Identifier: Apache-2.0
"""//tools/guards:shrink_only_check's entry. It runs the MERGE-BASE's copy of the check (shrink_only.py at the merge-base
of HEAD and --base), never the change's own, so a change cannot loosen the check that judges it (the review of
351e80daa2, 2026-11-18, M1); a merge-base without the script runs this tree's (the check's own first landing). The
library itself (:shrink_only_lib) is one target's, so Bazel compiles shrink_only.py once (two targets listing it wrote
one .pyc twice, a conflict that kept //gates:local from building; found 2026-11-18). CI does the same in gate.yml."""

import os
import sys
import types

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
import shrink_only  # noqa: E402


def arg(argv, name, default):
    for i, a in enumerate(argv):
        if a == name and i + 1 < len(argv):
            return argv[i + 1]
    return default


def base_copy(argv):
    """The merge-base's shrink_only.py as a module, or None when the merge-base has none."""
    base = arg(argv, "--base", "origin/main")
    where = arg(argv, "--repo", os.path.dirname(os.path.realpath(__file__)))
    repo = shrink_only.git(where, "rev-parse", "--show-toplevel").strip()
    merge_base = shrink_only.git(repo, "merge-base", "HEAD", base).strip()
    text = shrink_only.reader(repo, merge_base)(shrink_only.SELF)
    if text is None:
        return None, repo
    module = types.ModuleType("shrink_only_at_the_merge_base")
    module.__file__ = os.path.join(repo, shrink_only.SELF)
    exec(compile(text, f"{merge_base[:10]}:{shrink_only.SELF}", "exec"), module.__dict__)
    return module, repo


def main(argv):
    try:
        module, repo = base_copy(argv)
    except shrink_only.HistoryError as e:
        print(f"shrink_only_check: the history cannot be read ({e})", file=sys.stderr)
        return 2
    if module is None:
        return shrink_only.main(argv)
    print("shrink_only_check: the merge-base's copy of the check runs")
    return module.main(["--repo", repo] + [a for i, a in enumerate(argv) if a != "--repo"
                                           and (i == 0 or argv[i - 1] != "--repo")])


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
