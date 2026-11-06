"""Every quarantine and hang entry names a testable of the corpus (quarantine.stale_keys; the PR #87 review's finding
4). The engine is not run: the testables are the corpus's own, as run.py lists them."""
import unittest

import quarantine
import run


class StaleKeysTest(unittest.TestCase):

    def test_every_entry_names_a_testable(self):
        names = run.testables()
        self.assertGreater(len(names), 5000, "the corpus's testables were not found: the check is not looking")
        self.assertEqual([], quarantine.stale_keys(names))

    def test_a_stale_entry_is_named(self):
        names = [n for n in run.testables() if n != "stress::MD7_UnrevisedPrintsEq"]
        self.assertEqual(["ENGINE_QUARANTINE: stress::MD7_UnrevisedPrintsEq (F50) names no testable of the corpus"],
                         quarantine.stale_keys(names))


if __name__ == "__main__":
    unittest.main()
