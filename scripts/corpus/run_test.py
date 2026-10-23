"""run.py's batch loop over a stand-in for the engine's JVM: one that hangs, dies or reports nothing is started again
on the testables after it, and every testable gets exactly one verdict."""
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import run


class Jvm:
    """Each call takes the next (report lines or None for no report file, exit status) and records the testables the
    JVM was given."""

    def __init__(self, *script):
        self.script, self.calls = list(script), []

    def __call__(self, cmd, stdout, stderr, env, timeout):
        args = Path(cmd[-1][1:]).read_text(encoding="utf-8").splitlines()
        self.calls.append([a[len("--testable="):] for a in args if a.startswith("--testable=")])
        report = Path(next(a for a in args if a.startswith("--report="))[len("--report="):])
        lines, status = self.script.pop(0)
        if lines is not None:
            report.write_text("".join(f"{line}\n" for line in lines), encoding="utf-8")
        return subprocess.CompletedProcess(cmd, status)


def line(status, testable, detail=""):
    return f"{status}\t{testable}\t{testable}_suite\tt\t5\t{detail}"


class BatchTest(unittest.TestCase):

    def batch(self, jvm, names):
        with tempfile.TemporaryDirectory() as work, mock.patch.object(run.subprocess, "run", jvm):
            engine = run.Engine("runner", {}, [Path("a.pure")], Path(work), 600, "6g", [])
            got = engine.batch(1, names)
            files = sorted(p.name for p in Path(work).iterdir())
        return {n: run._verdict([r[0] for r in rs]) for n, rs in got.items()}, files

    def test_one_jvm_when_every_testable_reports(self):
        jvm = Jvm(([line("PASS", "A"), line("FAIL", "B")], 0))
        self.assertEqual(({"A": "PASS", "B": "FAIL"}, ["batch-001.args", "batch-001.log", "batch-001.tsv"]),
                         self.batch(jvm, ["A", "B"]))
        self.assertEqual([["A", "B"]], jvm.calls)

    def test_a_hang_costs_itself_and_the_rest_run_in_a_new_jvm(self):
        jvm = Jvm(([line("PASS", "A"), line("HUNG", "B", "still running after 600 s")], 3), ([line("PASS", "C")], 0))
        verdicts, files = self.batch(jvm, ["A", "B", "C"])
        self.assertEqual({"A": "PASS", "B": "HUNG", "C": "PASS"}, verdicts)
        self.assertEqual([["A", "B", "C"], ["C"]], jvm.calls)
        self.assertIn("batch-001-2.tsv", files, "the second JVM's files do not overwrite the first's")

    def test_a_crash_once_is_the_jvms_and_the_testable_runs_again(self):
        jvm = Jvm(([line("PASS", "A")], 1), ([line("PASS", "B"), line("PASS", "C")], 0))
        self.assertEqual({"A": "PASS", "B": "PASS", "C": "PASS"}, self.batch(jvm, ["A", "B", "C"])[0])
        self.assertEqual([["A", "B", "C"], ["B", "C"]], jvm.calls)

    def test_a_crash_twice_is_charged_to_the_testable_running_both_times(self):
        jvm = Jvm(([line("PASS", "A")], 1), ([], 1), ([line("PASS", "C")], 0))
        self.assertEqual({"A": "PASS", "B": "DIED", "C": "PASS"}, self.batch(jvm, ["A", "B", "C"])[0])
        self.assertEqual([["A", "B", "C"], ["B", "C"], ["C"]], jvm.calls)

    def test_a_jvm_that_dies_before_a_testable_is_started_once_more(self):
        jvm = Jvm((None, 1), ([line("PASS", "A"), line("PASS", "B")], 0))
        self.assertEqual({"A": "PASS", "B": "PASS"}, self.batch(jvm, ["A", "B"])[0])

    def test_a_jvm_that_twice_dies_before_a_testable_charges_the_batch(self):
        jvm = Jvm((None, 1), (None, 1))
        self.assertEqual({"A": "DIED", "B": "DIED"}, self.batch(jvm, ["A", "B"])[0])
        self.assertEqual([["A", "B"], ["A", "B"]], jvm.calls)

    def test_a_testable_that_reports_no_test_is_missing(self):
        jvm = Jvm(([line("PASS", "A")], 0))
        self.assertEqual({"A": "PASS", "B": "MISSING"}, self.batch(jvm, ["A", "B"])[0])


if __name__ == "__main__":
    unittest.main()
