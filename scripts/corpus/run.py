"""
The stress corpus through legend-engine, judged (Bazel workplan P3-25; docs/STRESS_CORPUS_PLAN_2026_10_19.md, Phase 0).

  bazel run //scripts/corpus:run -- [--only <file>] [--out <dir>] [--jobs N] [--batch N] [--limit SECONDS] [-v]
  bazel run //scripts/corpus:run -- --rows <dir> [--data <damaged.pure>]...      every test's rows (tools/wrongrows)
  bazel test //scripts/corpus:engine_stress                                       the whole corpus, sharded (manual)

The engine is //tools/engine-runner:testable: legend-engine at the release release.MODULE.bazel pins, the one version
every test uses (the plan's D3). The testables are the `stress::` services that have test suites, in every stress
file, and the function testables (D4: lite's runner takes the same `stress::` set), less HANGS. `--only` takes a file of
testable names, one per line (`#` starts a comment): a batch's changed services (changed.py writes that list).

Exit status is 0 only when every case lands where it is supposed to:

  PASS        not quarantined, passed          -- as intended
  KNOWN-FAIL  quarantined, failed              -- as intended; the defect is still there
  REGRESSION  not quarantined, failed          -- exit 1
  ERROR       not quarantined, threw           -- exit 1
  HUNG        still running after --limit      -- exit 1; a hang needs a repro and a HANGS entry
  DIED        its JVM exited while it ran      -- exit 1
  MISSING     ran, and reported no test        -- exit 1
  FIXED       quarantined, PASSED              -- exit 1; delete it from quarantine.py
                                                  and from docs/UPSTREAM_FINDINGS.md

The FIXED case is an error on purpose. A quarantine that silently keeps passing is how a
suite stops meaning anything: the entry rots, nobody notices the upstream fix, and the
next real failure in that area is assumed to be the old known one.

Paths on the command line are relative to where the command was typed. The run writes, into --out (under `bazel
test`, the test's outputs): engine-pass.txt, the passing tests in lite's pass-list form (`<service> / <suite> / <test>
:: PASS`, which the feature report's tools read beside lite's stress-suites-pass.txt); engine-verdicts.tsv, one
verdict per testable; engine-report.tsv, every test's line as the runner reported it; and batches/, each JVM's
arguments, report and log.
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from python.runfiles import runfiles

_RUNFILES = runfiles.Create()
# The corpus as this binary's runfiles hold it: the stress files, the linked projects and the layout are its declared
# data. model.py reads CORPUS_ROOT when it is imported, so it is set first.
_LAYOUT = "_main/core/src/test/resources/com/legend/integration/stress-layout.json"
os.environ.setdefault("CORPUS_ROOT", str(Path(_RUNFILES.Rlocation(_LAYOUT)).parents[7]))

import functest  # noqa: E402
import model  # noqa: E402
from quarantine import ENGINE_QUARANTINE, HANGS  # noqa: E402

# Testables per JVM. Each JVM first parses and compiles the whole corpus (about 25 s), then runs its testables at about
# 2 s each, so a batch amortises that over many.
#
# The ceiling used to be a CONNECTION POOL. legend-engine's service test runner keeps every
# test's pooled connection, so a JVM's pool drained after about a hundred tests (`DBPool_DuckDB_...
# Connection is not available, request timed out after 30001ms (total=100...)`), and each test's
# data setup grew slower than the last: nine services sharing stress::RT took 2.4 s to 25 s, one
# after another (2026-10-21). TestableMain now closes the pools after every testable, as
# legend-engine's own test helpers do, and the same twelve services took 1.7 s each. Batches of
# 40 were the workaround; they are no longer needed.
BATCH = 200

# Seconds one testable may run before the runner reports it HUNG and halts its JVM (TestableMain --limit). The slowest
# services take seconds; ten minutes is a hang.
LIMIT = 600

# The heap of each JVM: the engine holds the whole corpus's compiled model, under 3 GB of it.
HEAP = "6g"

# A JVM's report lines: status, testable, suite, test, the testable's millis, detail (TestableMain --report).
_STATUSES = ("PASS", "FAIL", "ERROR", "ROWS", "HUNG")

# One test's result, as a batch keeps it: status, suite, test, the testable's millis, detail.
Result = tuple[str, str, str, int, str]


def testables() -> list[str]:
    """The `stress::` services that have test suites, plus the FUNCTION testables -- which are addressed by a mangled
    id encoding the signature, not by the function's path."""
    out = []
    # EVERY source file, not a numbered subset. The glob has been wrong twice now for the
    # same reason: it read "9[24]-*" while the hier services lived in 97, and then
    # "9[2478]-*" while the surface-burndown services lived in 67 and 73. Both times the
    # services PARSED, COMPILED and were simply never run -- the total did not move and
    # nothing said why, which is the quietest way a test can fail to exist.
    #
    # Scanning everything costs a directory read and removes the whole class of mistake.
    for f in sorted(model.STRESS.glob("*.pure")):
        text = f.read_text(encoding="utf-8")
        # A service with no testSuites is a DECLARATION, not a testable. Listing one makes
        # it report MISSING for ever -- the runner asks for a result the service was never
        # going to produce, which reads like a failure and is not one.
        for m in re.finditer(r"^Service (\S+)", text, re.M):
            body = text[m.end():]
            nxt = re.search(r"^(?:Service|ExecutionEnvironment|###)", body, re.M)
            if m.group(1).startswith("stress::") and "testSuites" in (body[:nxt.start()] if nxt else body):
                out.append(m.group(1))
    return out + functest.testables()


def corpus_files() -> list[Path]:
    """The LINKED PROJECTS first, then the corpus. A corpus store includes a project store and a corpus mapping
    includes a project mapping, so the project's elements must be in the source before the corpus's own files refer to
    them. Omitting them entirely fails with a bare "Unexpected token" -- a parse error naming no file, because the
    corpus text references a package the parser has never seen."""
    return [p.absolute() for p in model.linked_files() + sorted(model.STRESS.glob("*.pure"))]


def _typed(path: str) -> Path:
    """A command-line path, relative to where the command was typed (`bazel run` runs in the runfiles tree)."""
    p = Path(path)
    return p if p.is_absolute() else Path(os.environ.get("BUILD_WORKING_DIRECTORY", ".")) / p


def _read_report(path: Path) -> list[tuple[str, Result]]:
    """(testable, result) per line of a JVM's report."""
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        st, testable, suite, test, millis, detail = (line.split("\t", 5) + [""] * 6)[:6]
        if st in _STATUSES:
            rows.append((testable, (st, suite, test, int(millis or 0), detail)))
    return rows


class Engine:
    """The runner, one JVM per batch."""

    def __init__(self, launcher: str, env: dict[str, str], files: list[Path], work: Path, limit: int, heap: str,
                 extra: list[str]):
        self.launcher, self.env, self.files, self.work = launcher, env, files, work
        self.limit, self.heap, self.extra = limit, heap, extra

    @staticmethod
    def from_runfiles(args, files: list[Path], work: Path) -> "Engine":
        launcher = _RUNFILES.Rlocation(os.environ["ENGINE_RUNNER"])
        if not launcher or not Path(launcher).exists():
            raise SystemExit(f"the engine runner is not in the runfiles: {os.environ['ENGINE_RUNNER']}")
        extra = ([f"--rows={_typed(args.rows).absolute()}"] if args.rows else []) \
            + [f"--data={_typed(d).absolute()}" for d in args.data]
        return Engine(launcher, dict(os.environ, **_RUNFILES.EnvVars()), files, work, args.limit, args.heap, extra)

    def launch(self, args: list[str], argfile: Path, log: Path, timeout: float) -> int | None:
        """One JVM of the runner over `args`, its output into `log`; its exit status, or None past `timeout`. The one
        place a corpus script starts Java (the Bazel workplan's D18): the probes move onto it in P7-03. The arguments
        go one per line through `argfile` (TestableMain's @<file>), since the corpus's paths and a batch's names
        overflow Windows' 32,767-character command line."""
        argfile.write_text("".join(f"{a}\n" for a in args), encoding="utf-8")
        with open(log, "w", encoding="utf-8") as fh:
            try:
                return subprocess.run([self.launcher, f"--jvm_flag=-Xmx{self.heap}", f"@{argfile}"],
                                      stdout=fh, stderr=subprocess.STDOUT, env=self.env, timeout=timeout).returncode
            except subprocess.TimeoutExpired:
                return None

    def batch(self, number: int, names: list[str]) -> dict[str, list[Result]]:
        """`names` in one JVM, started again after a testable that hangs or kills it, until each has a verdict.

        A JVM can die for a reason of its own: duckdb_jdbc's JNI crashes now and then, and a JVM started while Bazel
        rewrites the JDK it runs on dies at once (2026-10-21: seven did, in jimage.dll). So a death is charged only
        when it happens twice: to the testable that was running both times, or to the whole batch when the JVM twice
        dies before reaching a testable."""
        results: dict[str, list[Result]] = {}
        queue, attempt, deaths = list(names), 0, {}
        while queue:
            attempt += 1
            # batch-007, then batch-007-2 for the JVM started after a hang or a crash
            stem = f"batch-{number:03d}" + (f"-{attempt}" if attempt > 1 else "")
            report, argfile, log = (self.work / (stem + ext) for ext in (".tsv", ".args", ".log"))
            # the runner's own --limit halts a hung JVM; the timeout is the backstop for one that never reaches its
            # testables (a compile that does not end)
            status = self.launch([*map(str, self.files), *(f"--testable={n}" for n in queue), f"--report={report}",
                                  f"--limit={self.limit}", *self.extra],
                                 argfile, log, timeout=self.limit * (len(queue) + 2))
            for testable, result in _read_report(report):
                results.setdefault(testable, []).append(result)
            rest = [n for n in queue if n not in results]
            if status == 0 or not rest:
                for n in rest:
                    results[n] = [("MISSING", "", "", 0, "the runner reported no test for it")]
                break
            if status != 3:
                # not the runner's own halt after a HUNG: the JVM died, before any testable (the report opens after
                # the model compiles) or while the first testable without a verdict ran
                culprit = rest[0] if report.exists() else None
                deaths[culprit] = deaths.get(culprit, 0) + 1
                if deaths[culprit] == 2 and culprit is None:
                    why = f"the JVM twice exited with status {status} before running any testable; see {log.name}"
                    for n in rest:
                        results[n] = [("DIED", "", "", 0, why)]
                    break
                if deaths[culprit] == 2:
                    results[culprit] = [("DIED", "", "", 0, f"its JVM twice exited, with status {status}; see "
                                                            f"{log.name}")]
                    rest = rest[1:]
            queue = rest
        return results


def _verdict(statuses: list[str]) -> str:
    for s in ("HUNG", "DIED", "MISSING", "ERROR", "FAIL", "ROWS"):
        if s in statuses:
            return s
    return "PASS"


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--only", help="a file of testable names, one per line")
    p.add_argument("--out", help="where the pass list, the verdicts and the batches go")
    p.add_argument("--jobs", type=int, default=1, help="JVMs at once (each takes the heap and a core)")
    p.add_argument("--batch", type=int, default=BATCH)
    p.add_argument("--limit", type=int, default=LIMIT, help="seconds one testable may run")
    p.add_argument("--heap", default=HEAP)
    p.add_argument("--rows", help="rows mode: every test's actual rows into this directory, nothing judged")
    p.add_argument("--data", action="append", default=[], help="a .pure file whose elements replace the model's")
    p.add_argument("-v", action="store_true", help="every verdict, and each batch's log")
    args = p.parse_args()

    names = testables()
    if args.only:
        wanted = [line.split("#")[0].strip() for line in _typed(args.only).read_text(encoding="utf-8").splitlines()]
        wanted = [w for w in wanted if w]
        unknown = sorted(set(wanted) - set(names))
        if unknown:
            raise SystemExit(f"{len(unknown)} name(s) in {args.only} are not testables of the corpus:\n  "
                             + "\n  ".join(unknown))
        names = [n for n in names if n in set(wanted)]
    # Hanging cases are excluded from execution but still reported, so they cannot be
    # forgotten -- and so one non-returning test cannot block the whole suite.
    hangs = sorted(n for n in names if n in HANGS)
    names = [n for n in names if n not in HANGS]

    # a shard of `bazel test`: every TEST_TOTAL_SHARDS-th testable, so the slow families spread across the shards
    shards, shard = int(os.environ.get("TEST_TOTAL_SHARDS", "1")), int(os.environ.get("TEST_SHARD_INDEX", "0"))
    if os.environ.get("TEST_SHARD_STATUS_FILE"):
        Path(os.environ["TEST_SHARD_STATUS_FILE"]).touch()
    names, hangs = names[shard::shards], hangs[shard::shards]

    out = _typed(args.out) if args.out else (Path(os.environ["TEST_UNDECLARED_OUTPUTS_DIR"])
                                             if os.environ.get("TEST_UNDECLARED_OUTPUTS_DIR")
                                             else Path(tempfile.mkdtemp(prefix="engine-stress-")))
    work = out / "batches"
    work.mkdir(parents=True, exist_ok=True)
    engine = Engine.from_runfiles(args, corpus_files(), work)
    chunks = [names[i:i + args.batch] for i in range(0, len(names), args.batch)]
    print(f"{len(names)} testables in {len(chunks)} batches of {args.batch}, {args.jobs} at a time"
          + (f" (shard {shard + 1} of {shards})" if shards > 1 else "") + f"; batches in {work}", flush=True)

    t0 = time.monotonic()
    results: dict[str, list[Result]] = {}

    def one(i: int) -> None:
        b0 = time.monotonic()
        got = engine.batch(i + 1, chunks[i])
        results.update(got)
        tally = {}
        for v in (_verdict([r[0] for r in rs]) for rs in got.values()):
            tally[v] = tally.get(v, 0) + 1
        print(f"  batch {i + 1}/{len(chunks)}: {len(chunks[i])} in {time.monotonic() - b0:.0f} s  "
              + " ".join(f"{k}={v}" for k, v in sorted(tally.items())), flush=True)

    with ThreadPoolExecutor(max_workers=max(1, args.jobs)) as pool:
        list(pool.map(one, range(len(chunks))))

    report_lines, pass_lines = [], []
    for name in names:
        for st, suite, test, millis, detail in results[name]:
            report_lines.append("\t".join((st, name, suite, test, str(millis), detail)))
            if st == "PASS":
                pass_lines.append(f"{name} / {suite} / {test} :: PASS")
    (out / "engine-report.tsv").write_text("".join(f"{l}\n" for l in report_lines), encoding="utf-8")
    (out / "engine-pass.txt").write_text("".join(f"{l}\n" for l in sorted(pass_lines)), encoding="utf-8")
    # where the time went: the slowest testables (on the engine, seeding each test's database dominates)
    timed = sorted(((rs[0][3], n) for n, rs in results.items() if rs), reverse=True)
    print("slowest: " + ", ".join(f"{n.split('::')[-1]} {ms / 1000:.0f} s" for ms, n in timed[:8]))

    if args.rows:
        tally = {}
        for name in names:
            v = _verdict([r[0] for r in results[name]])
            tally[v] = tally.get(v, 0) + 1
            if v not in ("ROWS", "PASS"):
                print(f"  {name}  {v}  {results[name][0][4]}")
        print(f"\nrows mode, {time.monotonic() - t0:.0f} s: " + " ".join(f"{k}={v}" for k, v in sorted(tally.items()))
              + f"; rows in {_typed(args.rows)}")
        return

    verdicts, bad = [], 0
    for name in hangs:
        fid, why = HANGS[name]
        verdicts.append((name, f"NOT RUN {fid}", why, False))
    for name in sorted(names, key=lambda n: (n not in ENGINE_QUARANTINE, n)):
        rows = results[name]
        status = _verdict([r[0] for r in rows])
        detail = next((r[4] for r in rows if r[4]), "")
        q = ENGINE_QUARANTINE.get(name)
        if q and status in ("FAIL", "ERROR"):
            verdict, wrong, why = f"KNOWN-FAIL {q[0]}", False, q[1]
        elif q and status == "PASS":
            verdict, wrong, why = f"FIXED {q[0]} -- remove from quarantine.py", True, q[1]
        elif status == "PASS":
            verdict, wrong, why = "PASS", False, ""
        else:
            verdict, wrong, why = ("REGRESSION" if status == "FAIL" else status), True, detail
        bad += wrong
        verdicts.append((name, verdict, why, wrong))
    (out / "engine-verdicts.tsv").write_text(
        "".join(f"{n}\t{v}\t{w}\n" for n, v, w, _ in verdicts), encoding="utf-8")

    width = max((len(v[0]) for v in verdicts), default=0)
    for name, verdict, why, wrong in verdicts:
        if args.v or verdict != "PASS":
            print(f"  {name:<{width}}  {verdict:<34}{why}")
    if args.v:
        for log in sorted(work.glob("*.log")):
            print(f"\n=== {log.name}\n{log.read_text(encoding='utf-8', errors='replace')}")

    def count(prefix):
        return sum(1 for _, v, _, _ in verdicts if v.startswith(prefix))
    print(f"\n{count('PASS')} passed, {count('KNOWN-FAIL')} known-fail (quarantined), {count('NOT RUN')} not run "
          f"(hangs), {bad} unexpected, {len(verdicts)} total, in {time.monotonic() - t0:.0f} s; outputs in {out}")
    if bad:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
