"""The part of CONTRACT.md that is read off the files, not compiled (Bazel workplan P3-23): no class-typed property is
mapped over a join without a target set id. That compiles, then fails at execution with `Void not supported!`
(FINDINGS.md, F57; scripts/projects/check.py's `unroutable`).

The manifests are prose and no test reads them (G17: a Markdown file never changes a verdict, and CI does not run on a
Markdown-only change); BUILD.bazel's PROJECT_DEPS is the dependency graph.

PROJECT_DEPS arrives in the environment as `name=dep,dep;name=;...`; the files as $(rlocationpaths) in PROJECT_FILES.
"""
import os
import re
import sys
import unittest
from pathlib import Path

from python.runfiles import runfiles

_RUNFILES = runfiles.Create()


def _project_deps() -> dict[str, set[str]]:
    out = {}
    for row in os.environ["PROJECT_DEPS"].split(";"):
        name, _, deps = row.partition("=")
        out[name] = {d for d in deps.split(",") if d}
    return out


def _files() -> dict[str, Path]:
    """Each declared file by its path from projects/ (`trade-capture/mapping.pure`)."""
    out = {}
    for rlocationpath in os.environ["PROJECT_FILES"].split():
        path = _RUNFILES.Rlocation(rlocationpath)
        if path is None:
            raise AssertionError(f"not in this test's runfiles: {rlocationpath}")
        out[rlocationpath.split("/projects/", 1)[1]] = Path(path)
    return out


DEPS = _project_deps()
FILES = _files()

_JOINPROP = re.compile(r"^\s*(\w+)(\[\w+\])?\s*:\s*\[[\w:]+\]\s*@([^\n]*)$", re.M)


class ContractTest(unittest.TestCase):

    def test_reads_every_project(self):
        self.assertGreater(len(DEPS), 50, "PROJECT_DEPS names too few projects: the scan is not looking")
        mappings = [f for f in FILES if f.endswith("/mapping.pure")]
        self.assertGreater(len(mappings), 50, f"only {len(mappings)} mapping files declared: the scan is not looking")

    def test_no_class_property_is_mapped_over_a_join_without_a_set_id(self):
        bad = []
        for name in sorted(DEPS):
            mapping = FILES.get(f"{name}/mapping.pure")
            if mapping is None:
                continue
            for m in _JOINPROP.finditer(mapping.read_text(encoding="utf-8")):
                prop, setid, tail = m.group(1), m.group(2), m.group(3)
                # a join chain ending on a column (`| [store]T.C`) lands on no class and needs no set id
                if "|" in tail or setid:
                    continue
                bad.append(f"{name}: {prop} is mapped over a join with no target set id")
        self.assertEqual([], bad, "compiles, then fails at execution with `Void not supported!`:\n" + "\n".join(bad))


if __name__ == "__main__":
    sys.exit(unittest.main())
