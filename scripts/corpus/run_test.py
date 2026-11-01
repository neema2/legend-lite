"""run.py's batch loop over a stand-in for the engine's JVM: one that hangs, dies or reports nothing is started again
on the testables after it, and every testable gets exactly one verdict."""
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import run
import verdicts


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


class ShardTest(unittest.TestCase):
    """A run split across machines (.github/workflows/engine-verdict.yml): each takes engine_stress's slice, and the
    merge is one verdict only when every shard came back."""

    def test_shard_is_k_of_n_from_zero(self):
        self.assertEqual((0, 8), run.parse_shard("0/8"))
        self.assertEqual((7, 8), run.parse_shard("7/8"))
        for bad in ("8/8", "1", "a/8", "-1/8"):
            with self.assertRaises(SystemExit, msg=bad):
                run.parse_shard(bad)

    @staticmethod
    def shard(root, k, n, rows):
        d = Path(root) / f"shard-{k}"
        d.mkdir()
        (d / "shard.txt").write_text(f"{k}/{n}\n", encoding="utf-8")
        (d / "engine-verdicts.tsv").write_text("".join(f"{t}\t{v}\t\n" for t, v in rows), encoding="utf-8")
        (d / "engine-pass.txt").write_text("".join(f"{t} / s / t :: PASS\n" for t, v in rows if v == "PASS"),
                                           encoding="utf-8")
        return d

    def merge(self, *shards, n=2):
        with tempfile.TemporaryDirectory() as root:
            dirs = [self.shard(root, k, n, rows) for k, rows in shards]
            status = verdicts.merge(dirs, n, Path(root) / "merged")
            merged = verdicts.read_verdicts(Path(root) / "merged" / "engine-verdicts.tsv")
            passes = (Path(root) / "merged" / "engine-pass.txt").read_text(encoding="utf-8").splitlines()
        return status, merged, passes

    def test_every_shard_back_and_landed_as_intended_is_green(self):
        status, merged, passes = self.merge((0, [("A", "PASS"), ("C", "KNOWN-FAIL F1")]), (1, [("B", "NOT RUN F15")]))
        self.assertEqual(0, status)
        self.assertEqual(["A", "B", "C"], [n for n, _, _ in merged])
        self.assertEqual(["A / s / t :: PASS"], passes)

    def test_an_unexpected_verdict_in_any_shard_is_red(self):
        self.assertEqual(1, self.merge((0, [("A", "PASS")]), (1, [("B", "REGRESSION")]))[0])

    def test_a_missing_shard_is_red_not_a_smaller_green(self):
        self.assertEqual(1, self.merge((0, [("A", "PASS")]))[0])

    def test_a_testable_judged_twice_is_red(self):
        self.assertEqual(1, self.merge((0, [("A", "PASS")]), (1, [("A", "PASS")]))[0])

    def test_a_shard_of_another_split_is_red(self):
        self.assertEqual(1, self.merge((0, [("A", "PASS")]), (1, [("B", "PASS")]), n=3)[0])

    def test_the_fingerprint_reads_the_verdicts_not_their_order_or_reasons(self):
        self.assertEqual(verdicts.fingerprint([("A", "PASS", ""), ("B", "KNOWN-FAIL F1", "D:/x")]),
                         verdicts.fingerprint([("B", "KNOWN-FAIL F1", "/home/x"), ("A", "PASS", "")]))
        self.assertNotEqual(verdicts.fingerprint([("A", "PASS", "")]),
                            verdicts.fingerprint([("A", "REGRESSION", "")]))

    def test_the_summary_counts_as_run_does(self):
        self.assertEqual("1 passed, 1 known-fail (quarantined), 1 not run (hangs), 2 unexpected, 5 total",
                         verdicts.summary(["PASS", "KNOWN-FAIL F1", "NOT RUN F15", "FIXED F2 -- remove", "DIED"]))


if __name__ == "__main__":
    unittest.main()
