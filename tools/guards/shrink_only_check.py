# Copyright 2026 Legend Contributors
# SPDX-License-Identifier: Apache-2.0
"""//tools/guards:shrink_only_check's entry. It runs the MERGE-BASE's copy of the check (shrink_only.py at the merge-base
of --head, default HEAD, and --base), never the change's own, so a change cannot loosen the check that judges it (the
review of 351e80daa2, 2026-11-18, M1); a merge-base without the script runs this tree's (the check's first landing).

The copy is chosen by its fixed path with git alone: nothing of the head's module is read or run before it is chosen
(review F of the guard line, 2026-11-18, MUST-FIX 3: the head's SELF and reader() picked a path the merge-base did not
have, and the head's main ran). The head's module is imported only when the merge-base has no copy. The library itself
(:shrink_only_lib) is one target's, so Bazel compiles shrink_only.py once (two targets listing it wrote one .pyc twice, a
conflict that kept //gates:local from building; found 2026-11-18). CI does the same in gate.yml."""

import os
import subprocess
import sys
import types

# the script's path in the repository, fixed here (never the head module's SELF)
SCRIPT = "tools/guards/shrink_only.py"


def arg(argv, name, default):
    for i, a in enumerate(argv):
        if a == name and i + 1 < len(argv):
            return argv[i + 1]
    return default


def git(repo, *args):
    return subprocess.run(["git", "-C", repo, *args], check=True, capture_output=True, text=True).stdout


def base_copy(argv):
    """The merge-base's shrink_only.py as a module, and the repository; the module is None when the merge-base has
    none. The merge-base is the head's (--head, default HEAD: review F's NOTE 5) with --base."""
    base = arg(argv, "--base", "origin/main")
    head = arg(argv, "--head", "HEAD")
    where = arg(argv, "--repo", os.path.dirname(os.path.realpath(__file__)))
    repo = git(where, "rev-parse", "--show-toplevel").strip()
    merge_base = git(repo, "merge-base", head, base).strip()
    if not git(repo, "ls-tree", "--name-only", merge_base, "--", SCRIPT).strip():
        return None, repo
    text = git(repo, "show", f"{merge_base}:{SCRIPT}")
    module = types.ModuleType("shrink_only_at_the_merge_base")
    module.__file__ = os.path.join(repo, SCRIPT)
    exec(compile(text, f"{merge_base[:10]}:{SCRIPT}", "exec"), module.__dict__)
    # the copy reads its lists from its own text (own_lists), never from the head's file at __file__
    module.SOURCE = text
    return module, repo


def main(argv):
    try:
        module, repo = base_copy(argv)
    except subprocess.CalledProcessError as e:
        print(f"shrink_only_check: the history cannot be read ({(e.stderr or '').strip()})", file=sys.stderr)
        return 2
    if module is None:
        # the check's first landing: the merge-base has no copy, so this tree's runs
        sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
        import shrink_only  # noqa: E402
        return shrink_only.main(argv)
    print("shrink_only_check: the merge-base's copy of the check runs")
    return module.main(["--repo", repo] + [a for i, a in enumerate(argv) if a != "--repo"
                                           and (i == 0 or argv[i - 1] != "--repo")])


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
