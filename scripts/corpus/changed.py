"""
The testables a batch must run on legend-engine: the services whose text or expected result changed
(docs/STRESS_CORPUS_PLAN_2026_10_19.md, rule 3 and Phase 0).

  bazel run //scripts/corpus:changed -- [--base <git revision>] [--out <file>]
  bazel run //scripts/corpus:run -- --only <file>

Compares every stress file and every linked project file as committed at --base (default HEAD) with the working
tree, element by element: regenerate first (`bazel run //core:update_stress_corpus`), then list. A `stress::` service
with test suites that is new, or whose text differs (its query, its tests, its expected results), is listed. A changed
or deleted element of any other kind (a class, a store, a mapping, a runtime, a `###Data` seed, a function) can change
any service's answer, so then every testable is listed and the summary names the elements: after a seed change the
whole corpus runs. An element that is only new changes no existing answer and adds nothing.

The names go to --out (relative to where the command was typed), or to standard output; the summary to standard error.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

# An element starts at column 0 with its kind and its path: `Class <<p::s.t>> a::B extends ...`, `Service a::S`,
# `function a::f(x: String[1]): ...`, `RelationalDatabaseConnection a::C`. Stereotypes and tagged values are read past,
# since they hold paths of their own.
_START = re.compile(r"^(?:[A-Z]\w*|function)\b(?:[ \t]+(?:<<[^>\n]*>>|\{[^}\n]*\}))*[ \t]+([A-Za-z_]\w*(?:::\w+)+)",
                    re.M)
_SECTION = re.compile(r"^###\w+", re.M)


def elements(text: str) -> dict[str, str]:
    """Each element's text by key: its path, and for a function its whole header line, since overloads share a path.
    An element runs to the next element or section header. Lines are compared without trailing space or CR, and
    without whole-line `//` comments, which change no answer (a generated file's comment between a mapping and a
    service would otherwise read as a changed mapping)."""
    text = text.replace("\r\n", "\n")
    bounds = sorted([(m.start(), m) for m in _START.finditer(text)]
                    + [(m.start(), None) for m in _SECTION.finditer(text)], key=lambda b: b[0])
    out = {}
    for i, (start, m) in enumerate(bounds):
        if m is None:
            continue
        end = bounds[i + 1][0] if i + 1 < len(bounds) else len(text)
        body = "\n".join(line.rstrip() for line in text[start:end].strip().split("\n")
                         if not line.lstrip().startswith("//"))
        nl = text.find("\n", start)
        header = text[start:nl if nl >= 0 else len(text)]
        key = header.split("{")[0].strip() if header.startswith("function") else m.group(1)
        out[key] = body
    return out


def is_tested_service(key: str, body: str) -> bool:
    """A `stress::` service with test suites: a testable of run.py (D4)."""
    return key.startswith("stress::") and body.startswith("Service ") and "testSuites" in body


def compare(before: dict[str, str], after: dict[str, str]) -> tuple[list[str], list[str]]:
    """(tested services new or changed, other elements changed or deleted)."""
    services, others = [], []
    for key, body in after.items():
        if is_tested_service(key, body):
            if before.get(key) != body:
                services.append(key)
        elif key in before and before[key] != body:
            others.append(key)
    for key, body in before.items():
        if key not in after and not body.startswith("Service "):
            others.append(key)
    return sorted(services), sorted(others)


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True, text=True,
                          encoding="utf-8").stdout


def _committed(repo: Path, base: str, path: str) -> str:
    try:
        return _git(repo, "show", f"{base}:{path}")
    except subprocess.CalledProcessError:
        return ""   # not in the base revision: a new file


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--base", default="HEAD", help="the git revision to compare with (default HEAD)")
    p.add_argument("--out", help="the file the names go to")
    p.add_argument("--repo", default=os.environ.get("BUILD_WORKSPACE_DIRECTORY", "."),
                   help="the checkout (under bazel run, the workspace)")
    args = p.parse_args()
    repo = Path(args.repo)

    # the corpus's files in either revision: the stress files and the linked projects (core/stress.bzl's layout)
    paths = set(_git(repo, "ls-tree", "-r", "--name-only", args.base, "--", "core/src/test/resources/stress",
                     "projects").split())
    paths |= {str(f.relative_to(repo)).replace("\\", "/")
              for f in (repo / "core/src/test/resources/stress").glob("*.pure")}
    layout = json.loads((repo / "core/src/test/resources/com/legend/integration/stress-layout.json")
                                      .read_text(encoding="utf-8"))
    linked = tuple(f"projects/{n}/" for n in layout["linked_projects"])
    paths = sorted(f for f in paths if f.endswith(".pure") and (f.startswith("core/") or f.startswith(linked)))

    before, after = {}, {}
    for f in paths:
        before.update(elements(_committed(repo, args.base, f)))
        if (repo / f).exists():
            after.update(elements((repo / f).read_text(encoding="utf-8")))
    services, others = compare(before, after)
    if others:
        names = sorted(k for k, b in after.items() if is_tested_service(k, b))
        # the function testables run.py names by a mangled id; every one runs when anything but a service changed.
        # functest reads the corpus's layout when imported (model.py), from the checkout compared here.
        os.environ.setdefault("CORPUS_ROOT", str(repo))
        import functest
        names += functest.testables()
        print(f"{len(others)} element(s) other than services changed or were deleted, so every testable is listed "
              f"({len(names)}): " + ", ".join(others[:12]) + (" ..." if len(others) > 12 else ""), file=sys.stderr)
    else:
        names = services
        print(f"{len(services)} service(s) new or changed against {args.base}", file=sys.stderr)
    text = "".join(f"{n}\n" for n in names)
    if args.out:
        out = Path(args.out)
        out = out if out.is_absolute() else Path(os.environ.get("BUILD_WORKING_DIRECTORY", ".")) / out
        out.write_text(text, encoding="utf-8")
    else:
        sys.stdout.write(text)


if __name__ == "__main__":
    main()
