# Copyright 2026 Legend Contributors
# SPDX-License-Identifier: Apache-2.0
"""shrink_only.py's reader and rule on planted pins (the GUARD stream's C.13-CI, 2026-11-17): each way a pin grows is
named with the entry and the rule, each way it may move passes. No git: the two sides are given as text."""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
import shrink_only  # noqa: E402

PATH = "core/src/test/java/com/legend/PlantedTest.java"
SINCE = "2026-11-15"


def source(body):
    return "class PlantedTest {\n" + body + "\n}\n"


BASE = source('''
    // the register (2026-10-01)
    static final List<String> REGISTER = List.of(
            "a/File.java | spell( | 2 | OPEN",
            "b/File.java | other( | 1 | RULED");

    static final Map<String, Integer> SIZES = Map.ofEntries(
            // the residue (2026-10-01)
            Map.entry("x/Big.java", 40),
            Map.entry("y/Small.java", 3));

    static final Map<String, String> ROWS = Map.ofEntries(
            Map.entry("k1", "the first reason"));
''')


def run(head, watched=None):
    return shrink_only.check(lambda p: BASE if p == PATH else None,
                             lambda p: head if p == PATH else None, SINCE,
                             watched or {PATH: ["REGISTER", "SIZES", "ROWS"]})


class ShrinkOnlyTest(unittest.TestCase):

    def test_unchanged_passes(self):
        self.assertEqual([], run(BASE))

    def test_removal_and_fall_pass(self):
        head = BASE.replace('''            "a/File.java | spell( | 2 | OPEN",\n''', "").replace('"x/Big.java", 40', '"x/Big.java", 39')
        self.assertEqual([], run(head))

    def test_an_added_row_without_a_note_is_named(self):
        head = BASE.replace('''"b/File.java | other( | 1 | RULED");''',
                            '''"b/File.java | other( | 1 | RULED",\n            "c/New.java | spell( | 1 | OPEN");''')
        failures = run(head)
        self.assertEqual(1, len(failures), failures)
        self.assertIn('REGISTER entry "c/New.java | spell( | 1 | OPEN" added', failures[0])
        self.assertIn(shrink_only.RULE, failures[0])

    def test_an_old_group_note_does_not_date_a_new_row(self):
        # the register's group note is 2026-10-01, before the merge-base: a row added under it is undated
        head = BASE.replace('"a/File.java | spell( | 2 | OPEN",', '"a/File.java | spell( | 2 | OPEN",\n'
                            '            "a/File.java | added( | 1 | OPEN",')
        self.assertEqual(1, len(run(head)))

    def test_a_dated_note_passes_an_added_row(self):
        for note in ('            // listed 2026-11-17 (the task)\n            "c/New.java | spell( | 1 | OPEN");',
                     '            "c/New.java | spell( | 1 | OPEN"); // 2026-11-16, the task'):
            head = BASE.replace('''"b/File.java | other( | 1 | RULED");''', '''"b/File.java | other( | 1 | RULED",\n'''
                                + note)
            self.assertEqual([], run(head), note)

    def test_a_raised_count_is_named_in_a_row_and_in_an_entry(self):
        head = BASE.replace('spell( | 2 | OPEN', 'spell( | 3 | OPEN').replace('"y/Small.java", 3', '"y/Small.java", 4')
        failures = run(head)
        self.assertEqual(2, len(failures), failures)
        self.assertIn('REGISTER entry "a/File.java | spell( | 3 | OPEN" raised: 2 -> 3', failures[0])
        self.assertIn('SIZES entry "y/Small.java" raised: 3 -> 4', failures[1])

    def test_a_note_inside_the_entry_call_dates_its_key(self):
        head = BASE.replace('''Map.entry("k1", "the first reason"));''',
                            '''Map.entry("k1", "the first reason"),\n            Map.entry("k2",\n'''
                            '''                    "the second, ruled 2026-11-17"));''')
        self.assertEqual([], run(head))
        undated = head.replace("ruled 2026-11-17", "ruled long ago")
        self.assertEqual(['k2', 'the second, ruled long ago'],
                         [f.split('entry "')[1].split('"')[0] for f in run(undated)])

    def test_a_concatenated_literal_is_one_entry(self):
        head = BASE.replace('''"b/File.java | other( | 1 | RULED");''', '''"b/File.java | other( | 1 | RULED",\n'''
                            '''            "c/Long.java | spell("\n                    + "ing( | 1 | OPEN");''')
        failures = run(head)
        self.assertEqual(1, len(failures), failures)
        self.assertIn('"c/Long.java | spell(ing( | 1 | OPEN" added', failures[0])

    def test_a_watched_pin_gone_fails_and_a_new_pin_is_not_checked(self):
        failures = run(BASE.replace("REGISTER =", "RENAMED ="))
        self.assertEqual(1, len(failures), failures)
        self.assertIn("the watched pin REGISTER is not found at HEAD", failures[0])
        head = BASE.replace("    static final Map<String, String> ROWS", '''    static final Set<String> FRESH = Set.of("z");

    static final Map<String, String> ROWS''')
        self.assertEqual([], run(head, {PATH: ["REGISTER", "SIZES", "ROWS", "FRESH"]}))

    def test_comments_and_their_strings_are_not_entries(self):
        head = BASE.replace('''"b/File.java | other( | 1 | RULED");''', '''"b/File.java | other( | 1 | RULED"\n'''
                            '''            // "d/Commented.java | spell( | 9 | OPEN"\n            );''')
        self.assertEqual([], run(head))


if __name__ == "__main__":
    unittest.main()
