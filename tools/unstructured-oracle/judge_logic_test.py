"""The judge's verdict rules and its gate, on made-up rows: no corpus, database or server."""
import tempfile
import unittest
from pathlib import Path

import judge_logic as logic

A = logic.pointer("SALES", "a.txt", 0)
B = logic.pointer("SALES", "b.txt", 10)
OLD = logic.pointer("SALES", "old.txt", 5)


def result(rank, *pointers):
    return {"rank": rank, "sha256": f"hash{rank}", "score": 1.0 / rank, "locations": [
        {"source": s, "path": p, "charStart": c, "section": "S", "asOfDate": "2026-01-01"} for s, p, c in pointers]}


class TextVerdict(unittest.TestCase):
    def verdict(self, results, accepted=(A,), match="all", superseded=(), negative=False):
        return logic.text_verdict(list(results), list(accepted), match, list(superseded), negative)

    def test_pass_carries_a_receipt_with_the_rank(self):
        outcome = self.verdict([result(1, B), result(3, A)])
        self.assertEqual(outcome["verdict"], logic.PASS)
        self.assertEqual(outcome["receipts"], [{"path": "a.txt", "section": "S", "asOfDate": "2026-01-01", "sha256": "hash3", "rank": 3}])

    def test_outside_the_top_five_is_dropped(self):
        self.assertEqual(self.verdict([result(6, A)])["verdict"], logic.DROPPED)

    def test_a_row_is_a_hit_for_any_of_its_locations(self):
        self.assertEqual(self.verdict([result(1, B, A)])["verdict"], logic.PASS)

    def test_match_all_needs_every_pointer(self):
        self.assertEqual(self.verdict([result(1, A)], accepted=(A, B))["verdict"], logic.DROPPED)

    def test_match_any_needs_one(self):
        self.assertEqual(self.verdict([result(1, A)], accepted=(A, B), match="any")["verdict"], logic.PASS)
        self.assertEqual(self.verdict([result(1, OLD)], accepted=(A, B), match="any")["verdict"], logic.DROPPED)

    def test_an_unknown_match_is_refused(self):
        with self.assertRaises(ValueError):
            self.verdict([result(1, A)], match="some")

    def test_stale_is_reported_even_with_the_accepted_one_there(self):
        self.assertEqual(self.verdict([result(1, A), result(2, OLD)], superseded=(OLD,))["verdict"], logic.STALE_HIT)

    def test_stale_only_counts_in_the_top_five(self):
        self.assertEqual(self.verdict([result(1, A), result(6, OLD)], superseded=(OLD,))["verdict"], logic.PASS)

    def test_the_cut_is_five_or_the_number_accepted(self):
        self.assertEqual(logic.cut([]), 5)
        self.assertEqual(logic.cut([A] * 5), 5)
        self.assertEqual(logic.cut([A] * 12), 12)

    def test_a_case_accepting_more_than_five_can_pass(self):
        many = [logic.pointer("SALES", f"{n}.txt", 0) for n in range(1, 8)]
        results = [result(n, many[n - 1]) for n in range(1, 8)]
        self.assertEqual(self.verdict(results, accepted=many)["verdict"], logic.PASS)

    def test_the_wider_cut_still_ends(self):
        many = [logic.pointer("SALES", f"{n}.txt", 0) for n in range(1, 8)]
        results = [result(n, many[n - 1]) for n in range(1, 7)] + [result(8, many[6])]
        self.assertEqual(self.verdict(results, accepted=many)["verdict"], logic.DROPPED)

    def test_a_superseded_pointer_counts_inside_the_wider_cut(self):
        many = [logic.pointer("SALES", f"{n}.txt", 0) for n in range(1, 8)]
        results = [result(n, many[n - 1]) for n in range(1, 8)] + [result(7, OLD)]
        self.assertEqual(self.verdict(results, accepted=many, superseded=(OLD,))["verdict"], logic.STALE_HIT)

    def test_negative(self):
        self.assertEqual(self.verdict([], accepted=(), negative=True)["verdict"], logic.EMPTY_OK)
        self.assertEqual(self.verdict([result(1, A)], accepted=(), negative=True)["verdict"], logic.FALSE_HIT)


class StructuredVerdict(unittest.TestCase):
    def test_numbers_by_value_rows_as_a_set(self):
        self.assertEqual(logic.structured_verdict([[["x", 4.0], ["y", 1]]], [[["y", 1.00], ["x", 4]]], [], False)["verdict"], logic.PASS)

    def test_a_boolean_is_not_a_number(self):
        self.assertEqual(logic.structured_verdict([[[True]]], [[[1]]], [], False)["verdict"], logic.DROPPED)

    def test_a_superseded_row_by_value(self):
        self.assertEqual(logic.structured_verdict([[["DAL", 39809456.0]]], [[["DAL", 57320000]]], [["DAL", 39809456]], False)["verdict"], logic.STALE_HIT)

    def test_an_extra_row_is_not_a_pass(self):
        self.assertEqual(logic.structured_verdict([[["a"], ["b"]]], [[["a"]]], [], False)["verdict"], logic.DROPPED)

    def test_negative(self):
        self.assertEqual(logic.structured_verdict([[]], [[]], [], True)["verdict"], logic.EMPTY_OK)
        self.assertEqual(logic.structured_verdict([[["a"]]], [[]], [], True)["verdict"], logic.FALSE_HIT)


def row(case, verdict):
    return {"case": case, "route": "text", "arm": "keyword", "backend": "duckdb", "road": "direct-sql", "verdict": verdict,
            "path": "-", "section": "-", "asOfDate": "-", "sha256": "-", "rank": "-", "detail": "d"}


class Gate(unittest.TestCase):
    def test_a_pass_lost_fails_even_when_the_counts_improve(self):
        old = [row("C1", logic.PASS), row("C2", logic.DROPPED), row("C3", logic.DROPPED)]
        new = [row("C1", logic.DROPPED), row("C2", logic.PASS), row("C3", logic.PASS)]
        self.assertEqual(len(logic.regressions(old, new)), 1)

    def test_a_pass_row_gone_fails(self):
        self.assertEqual(len(logic.regressions([row("C1", logic.PASS)], [])), 1)

    def test_a_non_pass_moving_does_not_fail(self):
        self.assertEqual(logic.regressions([row("C1", logic.DROPPED)], [row("C1", logic.ERROR)]), [])

    def test_the_ledger_round_trips(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ledger.tsv"
            rows = [row("C1", logic.PASS)]
            logic.write_ledger(path, ["a header"], rows)
            self.assertEqual(logic.read_ledger(path), rows)


if __name__ == "__main__":
    unittest.main()
