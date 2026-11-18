# Copyright 2026 Legend Contributors
# SPDX-License-Identifier: Apache-2.0
"""shrink_only.py's reader and rule on planted pins (the GUARD stream's C.13-CI, 2026-11-17; its review's M1, M2 and
S1-S6, 2026-11-18): each way a pin grows is named with the entry and the rule, each way it may move passes, and each
text the reader cannot read is refused by name. The rule's cases need no git (the two sides are given as text); the
floor, the head and the history's errors are read from a throwaway repository."""

import importlib.util
import os
import re
import subprocess
import sys
import tempfile
import types
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


def run(head, watched=None, base=BASE, grow_only=None):
    return shrink_only.check(lambda p: base if p == PATH else None,
                             lambda p: head if p == PATH else None, SINCE,
                             watched or {PATH: ["REGISTER", "SIZES", "ROWS"]}, grow_only or {}, self_check=False)


def entries_named(failures):
    return [f.split('entry "')[1].split('"')[0] for f in failures]


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
        self.assertIn('REGISTER entry "c/New.java | spell( | # | OPEN" added', failures[0])
        self.assertIn(shrink_only.RULE, failures[0])

    def test_an_old_group_note_does_not_date_a_new_row(self):
        # the register's group note is 2026-10-01, before the floor: a row added under it is undated
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
        self.assertIn('REGISTER entry "a/File.java | spell( | # | OPEN" raised: 2 -> 3', failures[0])
        self.assertIn('SIZES entry "y/Small.java" raised: 3 -> 4', failures[1])

    def test_a_note_inside_the_entry_call_dates_its_key(self):
        head = BASE.replace('''Map.entry("k1", "the first reason"));''',
                            '''Map.entry("k1", "the first reason"),\n            Map.entry("k2",\n'''
                            '''                    "the second, ruled 2026-11-17"));''')
        self.assertEqual([], run(head))
        undated = head.replace("ruled 2026-11-17", "ruled long ago")
        self.assertEqual(['k2'], entries_named(run(undated)))

    def test_a_concatenated_literal_is_one_entry(self):
        head = BASE.replace('''"b/File.java | other( | 1 | RULED");''', '''"b/File.java | other( | 1 | RULED",\n'''
                            '''            "c/Long.java | spell("\n                    + "ing( | 1 | OPEN");''')
        failures = run(head)
        self.assertEqual(1, len(failures), failures)
        self.assertIn('"c/Long.java | spell(ing( | # | OPEN" added', failures[0])

    def test_a_watched_pin_gone_fails_and_a_new_pin_is_not_checked(self):
        failures = run(BASE.replace("REGISTER =", "RENAMED ="))
        self.assertEqual(1, len(failures), failures)
        self.assertIn("the watched pin REGISTER is not found at the head", failures[0])
        head = BASE.replace("    static final Map<String, String> ROWS", '''    static final Set<String> FRESH = Set.of("z");

    static final Map<String, String> ROWS''')
        self.assertEqual([], run(head, {PATH: ["REGISTER", "SIZES", "ROWS", "FRESH"]}))

    def test_comments_and_their_strings_are_not_entries(self):
        head = BASE.replace('''"b/File.java | other( | 1 | RULED");''', '''"b/File.java | other( | 1 | RULED"\n'''
                            '''            // "d/Commented.java | spell( | 9 | OPEN"\n            );''')
        self.assertEqual([], run(head))

    # S1: the javadoc above a declaration does not date the pin's rows (the review of C.13-CI, 2026-11-18)
    def test_the_declarations_javadoc_dates_no_row(self):
        base = source('''
    /** The pairs, measured 2026-11-17. */
    static final Map<String, Set<String>> AGG = Map.ofEntries(
            Map.entry("LIST", Set.of("a.A")));
''')
        head = base.replace('Map.entry("LIST"', 'Map.entry("EVIL", Set.of("a.B")),\n            Map.entry("LIST"')
        self.assertEqual(["EVIL"], entries_named(run(head, {PATH: ["AGG"]}, base)))
        dated = base.replace('Map.entry("LIST"', '// joined 2026-11-18 (the task)\n'
                             '            Map.entry("EVIL", Set.of("a.B")),\n            Map.entry("LIST"')
        self.assertEqual([], run(dated, {PATH: ["AGG"]}, base))

    # S2: a value is identified with its key; digits are part of an identity
    def test_a_value_moved_between_keys_is_added(self):
        base = source('''
    static final Map<String, Set<String>> ENTRIES = entries(
            "planner.QueryDoor#plan()", "com.legend.Compiler",
            "planner.Old#gone()", "com.legend.edge.PureV1Routes");
''')
        head = source('''
    static final Map<String, Set<String>> ENTRIES = entries(
            "planner.QueryDoor#plan()", "com.legend.edge.PureV1Routes");
''')
        self.assertEqual(["planner.QueryDoor#plan() / com.legend.edge.PureV1Routes"],
                         entries_named(run(head, {PATH: ["ENTRIES"]}, base)))

    def test_digits_are_part_of_an_identity(self):
        base = source('''
    static final Map<String, Set<String>> OVR = Map.ofEntries(
            Map.entry("e.PureText$2", Set.of("spellsDates")),
            Map.entry("e.PureText$1", Set.of("judgesEngineText")));
''')
        head = source('''
    static final Map<String, Set<String>> OVR = Map.ofEntries(
            Map.entry("e.PureText$1", Set.of("spellsDates")));
''')
        self.assertEqual(["e.PureText$1 / spellsDates"], entries_named(run(head, {PATH: ["OVR"]}, base)))

    def test_a_count_raised_beside_a_key_removed_is_named(self):
        head = BASE.replace('            Map.entry("x/Big.java", 40),\n', '').replace('"y/Small.java", 3', '"y/Small.java", 40')
        self.assertEqual(["y/Small.java"], entries_named(run(head)))

    def test_map_of_pairs_are_keyed(self):
        base = source('''
    static final Map<String, String> RB = Map.of(
            "a.A", "the first",
            "a.B", "the second");
''')
        head = base.replace('"the second"', '"the first"')
        self.assertEqual(["a.B / the first"], entries_named(run(head, {PATH: ["RB"]}, base)))

    # S3: whatever the reader does not read is refused by name, never read past
    def test_unread_text_is_refused(self):
        base = source('''
    static final Set<String> ALLOW = Set.of("a.A", "a.B");
    static final Map<String, Integer> N = Map.ofEntries(Map.entry("k", 40));
''')
        cases = {
            'Set.of("a.A", "a.B", EVIL)': "no constant of that name in this file",
            'Set.of("a.A", "a.B" + SUFFIX)': "no constant of that name in this file",
            'Set.of("a.A", Evil.class.getName())': "a call to Evil.class.getName",
            'Other.ALLOW': "a name of another class",
            'Set.of("a.A", "a.B", (String) "c")': "'('",
        }
        for initializer, why in cases.items():
            head = base.replace('Set.of("a.A", "a.B")', initializer)
            failures = run(head, {PATH: ["ALLOW"]}, base)
            self.assertEqual(1, len(failures), (initializer, failures))
            self.assertIn("the watched pin ALLOW holds text the check does not read", failures[0])
            self.assertIn(why, failures[0], initializer)
        for count, why in {"Integer.MAX_VALUE": "a name of another class", "0x7fffffff": "not a decimal integer",
                           "(int) 1e6": "not a decimal integer", "(int) 7": "'('", "-5": "'-'", "4.0": "not a decimal integer", "017": "not a decimal"}.items():
            head = base.replace('Map.entry("k", 40)', f'Map.entry("k", {count})')
            failures = run(head, {PATH: ["N"]}, base)
            self.assertEqual(1, len(failures), (count, failures))
            self.assertIn(why, failures[0], count)

    def test_a_same_file_constant_is_read(self):
        base = source('''
    private static final String MAIN = "core/src/main/";
    private static final String EDGE = MAIN + "edge/";
    static final Set<String> FILES = Set.of(EDGE + "A.java");
''')
        self.assertEqual([], run(base, {PATH: ["FILES"]}, base))
        head = base.replace('MAIN = "core/src/main/"', 'MAIN = "core/src/other/"')
        self.assertEqual(["core/src/other/edge/A.java"], entries_named(run(head, {PATH: ["FILES"]}, base)))

    def test_a_decoy_in_a_comment_is_not_the_declaration(self):
        base = source('''
    /** e.g. {@code static final Set<String> ALLOW = Set.of();} */
    // static final Set<String> ALLOW = Set.of();
    static final Set<String> ALLOW = Set.of("a.A");
''')
        head = base.replace('Set.of("a.A")', 'Set.of("a.A", "a.EVIL")')
        self.assertEqual(["a.EVIL"], entries_named(run(head, {PATH: ["ALLOW"]}, base)))

    def test_a_text_blocks_lines_are_counted(self):
        # a text block inside the pin spans five lines; the rows after it are read at their own lines, not five lines
        # up (where the block's date would date them)
        base = source('''
    static final List<String> ALLOW = List.of(
            """
            x
            y
            measured 2026-11-17
            """,
            "a.A",
            "a.B");
''')
        head = base.replace('"a.B");', '"a.B",\n            "a.EVIL");')
        failures = run(head, {PATH: ["ALLOW"]}, base)
        self.assertEqual(["a.EVIL"], entries_named(failures))
        self.assertIn(f"{PATH}:11:", failures[0])

    # S4: '+' joins two strings and nothing else
    def test_a_plus_joins_only_its_two_strings(self):
        base = source('''
    private static final String TEST = "core/src/test/";
    static final List<Item> OPEN = List.of(
            item("K14", "the judge's JSON", "E-Java (C1)", TEST + "testplan/JudgeJsonTest.java", "lowering/A.java"));
''')
        head = base.replace(', TEST + "testplan/JudgeJsonTest.java"', "")
        self.assertEqual([], run(head, {PATH: ["OPEN"]}, base))
        grown = base.replace('"lowering/A.java"', '"lowering/A.java", "lowering/B.java"')
        self.assertEqual(["K14 / lowering/B.java"], entries_named(run(grown, {PATH: ["OPEN"]}, base)))
        nonsense = base.replace('TEST + "testplan', 'TEST + 1 + "testplan')
        self.assertIn("a '+' at line", run(nonsense, {PATH: ["OPEN"]}, base)[0])

    # S5: a grow-only pin's removal needs a dated note naming it; an addition passes
    def test_a_grow_only_removal_needs_a_note(self):
        base = source('''
    static final Set<String> REFEREE = Set.of("t.SqlReplayReferee", "t.Appeals");
''')
        grow = {PATH: ["REFEREE"]}
        head = base.replace(', "t.Appeals"', "")
        failures = run(head, {PATH: ["REFEREE"]}, base, grow)
        self.assertEqual(1, len(failures), failures)
        self.assertIn('REFEREE entry "t.Appeals" -- ' + shrink_only.REMOVED_RULE, failures[0])
        noted = head.replace("    static final", "    // Appeals moved to the edge 2026-11-18 (census C2)\n    static final")
        self.assertEqual([], run(noted, {PATH: ["REFEREE"]}, base, grow))
        unnamed = head.replace("    static final", "    // a class moved 2026-11-18\n    static final")
        self.assertEqual(1, len(run(unnamed, {PATH: ["REFEREE"]}, base, grow)))
        added = base.replace('"t.Appeals"', '"t.Appeals", "t.More"')
        self.assertEqual([], run(added, {PATH: ["REFEREE"]}, base, grow))

    def test_a_pin_dropped_from_the_lists_needs_a_note(self):
        base = 'WATCHED = {\n    "a/X.java": ["ONE", "TWO"],\n}\nGROW_ONLY = {\n    "a/X.java": ["ONE"],\n}\n'
        head = 'WATCHED = {\n    "a/X.java": ["ONE"],\n}\nGROW_ONLY = {\n    "a/X.java": [],\n}\n'
        failures = shrink_only.watched_shrinks(base, head, SINCE)
        self.assertEqual(2, len(failures), failures)
        self.assertIn("TWO (a/X.java) dropped from WATCHED", failures[0] + failures[1])
        self.assertIn("ONE (a/X.java) dropped from GROW_ONLY", failures[0] + failures[1])
        noted = "# TWO unwatched 2026-11-18 (gone); ONE no longer grow-only 2026-11-18\n" + head
        self.assertEqual([], shrink_only.watched_shrinks(base, noted, SINCE))
        self.assertEqual([], shrink_only.watched_shrinks(None, head, SINCE))

    def test_this_scripts_own_lists_are_read(self):
        with open(os.path.realpath(shrink_only.__file__), encoding="utf-8") as f:
            lists = shrink_only.watched_lists(f.read())
        self.assertEqual({(p, n) for p, ns in shrink_only.WATCHED.items() for n in ns}, lists["WATCHED"])
        self.assertEqual({(p, n) for p, ns in shrink_only.GROW_ONLY.items() for n in ns}, lists["GROW_ONLY"])

    # the review of 351e80daa2 (2026-11-18), M3: a removal is excused only by a note naming it as a whole word
    def test_a_name_inside_a_longer_name_excuses_nothing(self):
        base = source('''
    static final Set<String> REFEREE = Set.of("t.Appeal", "t.PlannedAppeals");
''')
        head = base.replace('"t.Appeal", ', "").replace("    static final", "    // t.PlannedAppeals stays, 2026-11-18\n"
                                                         "    static final")
        failures = run(head, {PATH: ["REFEREE"]}, base, {PATH: ["REFEREE"]})
        self.assertEqual(1, len(failures), failures)
        self.assertIn('REFEREE entry "t.Appeal"', failures[0])

    # M1: a list of this script is one literal; any statement that could change it is refused
    def test_a_change_to_the_lists_outside_their_literal_is_refused(self):
        lists = 'WATCHED = {\n    "a/X.java": ["ONE", "TWO"],\n}\nGROW_ONLY = {\n    "a/X.java": ["ONE"],\n}\n'
        self.assertEqual({("a/X.java", "ONE"), ("a/X.java", "TWO")}, shrink_only.watched_lists(lists)["WATCHED"])
        for tail in ('WATCHED["a/X.java"].remove("TWO")', 'WATCHED["a/X.java"] = ["ONE"]', 'del WATCHED["a/X.java"]',
                     'WATCHED = {}', 'GROW_ONLY.clear()', 'WATCHED |= {}', 'def f():\n    global WATCHED\n',
                     'WATCHED.update({})'):
            with self.assertRaises(ValueError, msg=tail):
                shrink_only.watched_lists(lists + tail + "\n")
        self.assertEqual(1, len(shrink_only.watched_shrinks(lists, lists + 'WATCHED["a/X.java"].remove("TWO")\n',
                                                            SINCE)))
        reads = 'def g():\n    return WATCHED.get("a/X.java"), list(GROW_ONLY.items())\n'
        self.assertEqual({("a/X.java", "ONE")}, shrink_only.watched_lists(lists + reads)["GROW_ONLY"])

    # SHOULD: each line of a text block is its own entry
    def test_each_line_of_a_text_block_is_an_entry(self):
        base = source('''
    static final String ROWS = """
            a/File.java | spell( | 2 | OPEN
            """;
''')
        head = base.replace("            a/File.java | spell( | 2 | OPEN\n",
                            "            a/File.java | spell( | 2 | OPEN\n            b/New.java | x( | 1 | OPEN 2026-11-18\n"
                            "            c/New.java | y( | 1 | OPEN\n")
        failures = run(head, {PATH: ["ROWS"]}, base)
        self.assertEqual(["c/New.java | y( | # | OPEN"], entries_named(failures))
        self.assertIn(f"{PATH}:6:", failures[0])
        raised = base.replace("spell( | 2 | OPEN", "spell( | 3 | OPEN")
        self.assertIn("raised: 2 -> 3", run(raised, {PATH: ["ROWS"]}, base)[0])

    # NOTE: an entry added with its count is named once
    def test_an_entry_and_its_count_are_named_once(self):
        head = BASE.replace('Map.entry("y/Small.java", 3));', 'Map.entry("y/Small.java", 3),\n            Map.entry("z/New.java", 5));')
        self.assertEqual(["z/New.java"], entries_named(run(head)))


def git(repo, *args, date=None):
    env = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@example.com", GIT_COMMITTER_NAME="t",
               GIT_COMMITTER_EMAIL="t@example.com", GIT_CONFIG_NOSYSTEM="1", HOME=repo)
    if date:
        env["GIT_AUTHOR_DATE"] = env["GIT_COMMITTER_DATE"] = date + "T12:00:00Z"
    return subprocess.run(["git", "-C", repo, *args], check=True, capture_output=True, text=True, env=env).stdout


class HistoryTest(unittest.TestCase):
    """M1 (the floor is the change's own first day, the head the pull request's own commit) and S6 (only an absent
    path is absent; any other git failure exits 2), on a throwaway repository."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = self.tmp.name
        git(self.repo, "init", "-q", "-b", "main")
        self.write(PATH, BASE)
        self.write(shrink_only.SELF, "WATCHED = {}\nGROW_ONLY = {}\n")
        git(self.repo, "add", "-A")
        git(self.repo, "commit", "-q", "-m", "base", date="2026-11-10")
        git(self.repo, "checkout", "-q", "-b", "change")
        # the change begins 2026-11-12; its note is dated that day
        self.write(PATH, BASE.replace('"b/File.java | other( | 1 | RULED");', '"b/File.java | other( | 1 | RULED",\n'
                                      '            "c/New.java | spell( | 1 | OPEN"); // 2026-11-12, the task'))
        git(self.repo, "commit", "-q", "-am", "change", date="2026-11-12")
        # main moves on after it, then the pull request's merge ref merges the two
        git(self.repo, "checkout", "-q", "main")
        self.write("other.txt", "x")
        git(self.repo, "add", "-A")
        git(self.repo, "commit", "-q", "-m", "main moves", date="2026-11-20")
        git(self.repo, "merge", "-q", "--no-ff", "-m", "merge ref", "change", date="2026-11-20")

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, path, text):
        full = os.path.join(self.repo, path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w", encoding="utf-8") as f:
            f.write(text)

    def main(self, *args):
        saved = shrink_only.WATCHED, shrink_only.GROW_ONLY
        shrink_only.WATCHED, shrink_only.GROW_ONLY = {PATH: ["REGISTER"]}, {}
        try:
            return shrink_only.main(["--repo", self.repo, *args])
        finally:
            shrink_only.WATCHED, shrink_only.GROW_ONLY = saved

    def test_the_floor_is_the_changes_first_day(self):
        head = git(self.repo, "rev-parse", "change").strip()
        base = git(self.repo, "rev-parse", "main~1").strip()
        merge_base = git(self.repo, "merge-base", head, base).strip()
        self.assertEqual("2026-11-12", shrink_only.floor(self.repo, merge_base, head))
        # the change's own head against main: its 2026-11-12 note is dated
        self.assertEqual(0, self.main("--base", "main~1", "--head", "change"))
        # the pull request's merge ref (a merge of main's tip and the change) against main's tip: the merge-base is the
        # tip, dated 2026-11-20, but the floor is the change's own first day, so its 2026-11-12 note still dates the row
        self.assertEqual("2026-11-12", shrink_only.floor(self.repo, base, "main"))
        self.assertEqual(0, self.main("--base", "main~1", "--head", "main"))

    def test_an_undated_row_fails_on_the_changes_head(self):
        git(self.repo, "checkout", "-q", "change")
        with open(os.path.join(self.repo, PATH), encoding="utf-8") as f:
            text = f.read()
        self.write(PATH, text.replace(" // 2026-11-12, the task", ""))
        git(self.repo, "commit", "-q", "-am", "undated", date="2026-11-13")
        # HEAD on main, whose merge holds the dated row: the change's own head is what --head reads
        git(self.repo, "checkout", "-q", "main")
        self.assertEqual(0, self.main("--base", "main~1"))
        self.assertEqual(1, self.main("--base", "main~1", "--head", "change"))

    def test_a_note_counts_only_if_the_change_wrote_it(self):
        # the review of 351e80daa2 (2026-11-18), M2: an unrelated commit authored long ago lowered the floor of dates
        # until the register's old group note (2026-10-01) dated a row added under it; a note must be the change's own
        git(self.repo, "checkout", "-q", "change")
        self.write("unrelated.txt", "x")
        git(self.repo, "add", "-A")
        git(self.repo, "commit", "-q", "-m", "backdated", date="2026-09-01")
        with open(os.path.join(self.repo, PATH), encoding="utf-8") as f:
            text = f.read()
        # a row added under the residue's group note, inside the pin (written 2026-10-01, before the change)
        self.write(PATH, text.replace('Map.entry("x/Big.java", 40),', 'Map.entry("x/Big.java", 40),\n'
                                      '            Map.entry("x/Under.java", 1),'))
        git(self.repo, "commit", "-q", "-am", "under the old note", date="2026-11-14")
        saved = shrink_only.WATCHED, shrink_only.GROW_ONLY
        shrink_only.WATCHED, shrink_only.GROW_ONLY = {PATH: ["SIZES"]}, {}
        try:
            self.assertEqual(1, shrink_only.main(["--repo", self.repo, "--base", "main~1", "--head", "change"]))
        finally:
            shrink_only.WATCHED, shrink_only.GROW_ONLY = saved

    def test_a_note_written_before_its_commit_counts(self):
        # a squash merge's commit is dated when it lands: a note the change wrote counts whatever its date
        git(self.repo, "checkout", "-q", "change")
        with open(os.path.join(self.repo, PATH), encoding="utf-8") as f:
            text = f.read()
        self.write(PATH, text.replace('"b/File.java | other( | 1 | RULED",', '"b/File.java | other( | 1 | RULED",\n'
                                      '            "d/Late.java | z( | 1 | OPEN", // 2026-11-12, written before'))
        git(self.repo, "commit", "-q", "-am", "landed late", date="2026-12-01")
        self.assertEqual(0, self.main("--base", "main~1", "--head", "change"))

    def test_a_renamed_file_is_compared_with_its_old_text(self):
        # the review of 351e80daa2 (2026-11-18), SHOULD: renamed with the list, a file read as new hid its growth
        git(self.repo, "checkout", "-q", "change")
        renamed = PATH.replace("PlantedTest", "PlantedRenamedTest")
        git(self.repo, "mv", PATH, renamed)
        with open(os.path.join(self.repo, renamed), encoding="utf-8") as f:
            text = f.read()
        self.write(renamed, text.replace('"y/Small.java", 3', '"y/Small.java", 9'))
        git(self.repo, "commit", "-q", "-am", "renamed and raised", date="2026-11-14")
        saved = shrink_only.WATCHED, shrink_only.GROW_ONLY
        shrink_only.WATCHED, shrink_only.GROW_ONLY = {renamed: ["SIZES"]}, {}
        try:
            self.assertEqual(1, shrink_only.main(["--repo", self.repo, "--base", "main~1", "--head", "change"]))
        finally:
            shrink_only.WATCHED, shrink_only.GROW_ONLY = saved

    def test_an_absent_path_is_absent_and_a_bad_revision_exits_2(self):
        read = shrink_only.reader(self.repo, "main")
        self.assertIsNone(read("no/such/File.java"))
        self.assertIsNotNone(read(PATH))
        with self.assertRaises(shrink_only.HistoryError):
            shrink_only.reader(self.repo, "0" * 40)(PATH)
        self.assertEqual(2, self.main("--base", "no-such-branch"))


class BaseCopyTest(unittest.TestCase):
    """M1 (the review of 351e80daa2, 2026-11-18): the local check runs the merge-base's copy of the script, so a change
    that loosens the script is judged by the script it loosened."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = self.tmp.name
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "shrink_only.py"), encoding="utf-8") as f:
            script = f.read()
        start = script.index("WATCHED = {")
        script = script[:start] + 'WATCHED = {\n    "' + PATH + '": ["REGISTER"],\n}\n' \
            + script[script.index("\n}\n", start) + 3:]
        start = script.index("GROW_ONLY = {")
        script = script[:start] + "GROW_ONLY = {}\n" + script[script.index("\n}\n", start) + 3:]
        git(self.repo, "init", "-q", "-b", "main")
        HistoryTest.write(self, PATH, BASE)
        HistoryTest.write(self, shrink_only.SELF, script)
        git(self.repo, "add", "-A")
        git(self.repo, "commit", "-q", "-m", "base", date="2026-11-10")
        git(self.repo, "checkout", "-q", "-b", "change")
        # the change loosens the script (every line a note) and adds an undated row
        loosened = script.replace('    if fresh is None:\n        return recent(lines[n - 1], since)',
                                  '    return True\n    if fresh is None:\n        return recent(lines[n - 1], since)')
        assert loosened != script
        HistoryTest.write(self, shrink_only.SELF, loosened)
        HistoryTest.write(self, PATH, BASE.replace('"b/File.java | other( | 1 | RULED");',
                                                   '"b/File.java | other( | 1 | RULED",\n            "c/New.java | x( | 1 | OPEN");'))
        git(self.repo, "commit", "-q", "-am", "loosen", date="2026-11-12")
        spec = importlib.util.spec_from_file_location("shrink_only_check", os.path.join(here, "shrink_only_check.py"))
        self.check = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.check)

    def tearDown(self):
        self.tmp.cleanup()

    write = HistoryTest.write

    def test_the_merge_bases_copy_judges_the_change(self):
        loosened = types.ModuleType("loosened")
        with open(os.path.join(self.repo, shrink_only.SELF), encoding="utf-8") as f:
            exec(compile(f.read(), "loosened", "exec"), loosened.__dict__)
        self.assertEqual(0, loosened.main(["--repo", self.repo, "--base", "main"]), "the change's own copy passes it")
        self.assertEqual(1, self.check.main(["--repo", self.repo, "--base", "main"]), "the merge-base's copy does not")


class BuildTest(unittest.TestCase):
    """M2: the check is never answered from Bazel's cache (its inputs, the history and the guard files, are undeclared)."""

    def test_the_check_is_tagged_external(self):
        build = os.path.join(os.path.dirname(os.path.abspath(__file__)), "BUILD.bazel")
        with open(build, encoding="utf-8") as f:
            text = f.read()
        target = re.search(r'py_test\(\s*name = "shrink_only_check",(.*?)\n\)', text, re.S)
        self.assertIsNotNone(target, "the shrink_only_check target")
        tags = re.search(r"tags = \[(.*?)\]", target.group(1), re.S)
        self.assertIn('"external"', tags.group(1))


if __name__ == "__main__":
    unittest.main()
