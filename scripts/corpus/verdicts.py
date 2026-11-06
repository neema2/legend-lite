"""
The engine verdict's own arithmetic (docs/STRESS_CORPUS_PLAN_2026_10_19.md, Phase 0): what counts as unexpected, the
summary line run.py prints, a fingerprint of a whole verdict, and the merge of a verdict run in shards on several
machines (.github/workflows/engine-verdict.yml). Standard library only, so the merge runs with a bare `python3` on a CI
runner that has no Bazel, and run.py imports it so both print the one summary.

  python3 scripts/corpus/verdicts.py merge --shards 8 --out <dir> <shard dir>...   one verdict from a run's shards
  python3 scripts/corpus/verdicts.py fingerprint <engine-verdicts.tsv>...           compare two machines' verdicts

A shard's directory is run.py's --out: engine-verdicts.tsv (testable, verdict, why), engine-report.tsv, engine-pass.txt
and shard.txt (`k/n`, written by run.py). The merge fails unless the shards are exactly 0 to n-1 of the n asked for and
no testable is judged twice, so a lost job is never a smaller green verdict.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import sys
from collections import Counter
from pathlib import Path

# The verdicts that land where they are supposed to (run.py's docstring): passed, quarantined and still failing, and
# the hangs that are reported without being run. Every other verdict is unexpected and fails the run.
EXPECTED = ("PASS", "KNOWN-FAIL", "NOT RUN")


def unexpected(verdict: str) -> bool:
    return not verdict.startswith(EXPECTED)


def summary(verdicts: list[str]) -> str:
    """run.py's closing line, without the time and the outputs' place."""
    def count(prefix):
        return sum(1 for v in verdicts if v.startswith(prefix))
    return (f"{count('PASS')} passed, {count('KNOWN-FAIL')} known-fail (quarantined), {count('NOT RUN')} not run "
            f"(hangs), {sum(map(unexpected, verdicts))} unexpected, {len(verdicts)} total")


def read_verdicts(path: Path) -> list[tuple[str, str, str]]:
    """(testable, verdict, why) per line of an engine-verdicts.tsv."""
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line:
            name, verdict, why = (line.split("\t", 2) + ["", ""])[:3]
            rows.append((name, verdict, why))
    return rows


def fingerprint(rows: list[tuple[str, str, str]]) -> str:
    """The testables and their verdicts (the reason left out: an error's text carries a machine's paths), sorted, as
    16 hex digits. Two machines agree on the whole corpus exactly when their fingerprints are equal."""
    text = "".join(f"{n}\t{v}\n" for n, v, _ in sorted(rows))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _escape(text: str) -> str:
    """A GitHub workflow command's message: its own escapes, so one annotation stays one."""
    return text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def _escape_property(text: str) -> str:
    """A workflow command's property (the title): a comma ends a property and a colon the properties, so both are
    escaped too. Unescaped, `title=engine verdict, linux: ERROR (497)` kept only `engine verdict` (2026-11-01)."""
    return _escape(text).replace(":", "%3A").replace(",", "%2C")


# GitHub keeps an annotation's message to 4 KB, so a long list of names is cut to what fits and says how many more
# there are; the merged engine-verdicts.tsv, in the run's artifact, has them all
_MESSAGE_LIMIT = 4000


def _names(names: list[str]) -> str:
    """The names, space-separated, cut to _MESSAGE_LIMIT with a count of the rest."""
    whole = " ".join(names)
    if len(whole) <= _MESSAGE_LIMIT:
        return whole
    out, size = [], 0
    for i, n in enumerate(names):
        rest = f" ... and {len(names) - i} more (the artifact's engine-verdicts.tsv)"
        if size + len(n) + len(rest) > _MESSAGE_LIMIT:
            return " ".join(out) + rest
        out.append(n)
        size += len(n) + 1
    return whole


def merge(dirs: list[Path], shards: int, out: Path, title: str = "engine verdict") -> int:
    """The shards' verdicts as one, into `out`; 0 when every shard is there and every testable landed as intended.
    Under GitHub Actions the outcome is also written as annotations, which the public API serves without signing in
    (a job's log needs a token)."""
    problems, seen = [], {}
    rows, report, passes = [], [], []
    for d in dirs:
        marker = d / "shard.txt"
        if not marker.exists() or not (d / "engine-verdicts.tsv").exists():
            problems.append(f"{d}: not a finished shard (no shard.txt or engine-verdicts.tsv)")
            continue
        k, _, n = marker.read_text(encoding="utf-8").strip().partition("/")
        if int(n) != shards:
            problems.append(f"{d}: shard {k} of {n}, not of {shards}")
            continue
        if int(k) in seen:
            problems.append(f"{d}: shard {k} again (also {seen[int(k)]})")
            continue
        seen[int(k)] = d
        rows += read_verdicts(d / "engine-verdicts.tsv")
        report += (d / "engine-report.tsv").read_text(encoding="utf-8").splitlines(keepends=True) \
            if (d / "engine-report.tsv").exists() else []
        passes += (d / "engine-pass.txt").read_text(encoding="utf-8").splitlines() \
            if (d / "engine-pass.txt").exists() else []
    missing = sorted(set(range(shards)) - set(seen))
    if missing:
        problems.append(f"shards missing: {', '.join(map(str, missing))} of 0 to {shards - 1}")
    twice = sorted(n for n, c in Counter(n for n, _, _ in rows).items() if c > 1)
    if twice:
        problems.append(f"judged in more than one shard: {', '.join(twice[:20])}")

    out.mkdir(parents=True, exist_ok=True)
    (out / "engine-verdicts.tsv").write_text("".join(f"{n}\t{v}\t{w}\n" for n, v, w in sorted(rows)), encoding="utf-8")
    (out / "engine-report.tsv").write_text("".join(report), encoding="utf-8")
    (out / "engine-pass.txt").write_text("".join(f"{p}\n" for p in sorted(passes)), encoding="utf-8")

    line = f"{summary([v for _, v, _ in rows])}, fingerprint {fingerprint(rows)}, from {len(seen)} of {shards} shards"
    bad = [(n, v, w) for n, v, w in sorted(rows) if unexpected(v)]
    print(line)
    for n, v, w in bad:
        print(f"  {n}  {v}  {w}")
    for p in problems:
        print(f"  {p}")

    if os.environ.get("GITHUB_ACTIONS") == "true":
        print(f"::notice title={_escape_property(title)}::{_escape(line)}")
        # one annotation per kind, the names in it: a job keeps only a few error annotations
        kinds = sorted({v.split()[0] for _, v, _ in bad})
        for kind in kinds:
            these = [n for n, v, _ in bad if v.split()[0] == kind]
            print(f"::error title={_escape_property(f'{title}: {kind} ({len(these)})')}::{_escape(_names(these))}")
        for p in problems:
            print(f"::error title={_escape_property(f'{title}: incomplete')}::{_escape(p)}")
        if os.environ.get("GITHUB_STEP_SUMMARY"):
            with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as fh:
                fh.write(f"### {title}\n\n{line}\n\n")
                fh.write("".join(f"- `{n}` {v} {w}\n" for n, v, w in bad[:200]))
                fh.write("".join(f"- {p}\n" for p in problems))
    return 1 if bad or problems else 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    m = sub.add_parser("merge", help="one verdict from a run's shards")
    m.add_argument("--shards", type=int, required=True, help="how many shards the run was split into")
    m.add_argument("--out", required=True, help="where the merged pass list, verdicts and report go")
    m.add_argument("--title", default="engine verdict", help="the annotations' title (the platform, in CI)")
    m.add_argument("dirs", nargs="+", help="the shards' run.py --out directories")
    f = sub.add_parser("fingerprint", help="a verdict file's summary and fingerprint")
    f.add_argument("files", nargs="+")
    args = p.parse_args(argv)
    if args.cmd == "merge":
        return merge([Path(d) for d in args.dirs], args.shards, Path(args.out), args.title)
    for path in args.files:
        rows = read_verdicts(Path(path))
        print(f"{path}: {summary([v for _, v, _ in rows])}, fingerprint {fingerprint(rows)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
