#!/usr/bin/env python3
# Copyright 2026 Legend Contributors
# SPDX-License-Identifier: Apache-2.0
"""SHRINK-ONLY, BY THE MERGE-BASE (the edge audit of 2026-11-17, section C, item 11's second half; the GUARD stream's
C.13-CI, the orchestrator's GO of 2026-11-17).

The guard files' pins move only with a dated note (AGENTS.md: "a ceiling, pin, or allowlist entry moves ONLY with a
dated justification comment"). Until this check, that was a convention a reviewer read for: nothing compared a pin with
what it was. This script reads each WATCHED pin -- a constant of a guard file -- at the merge-base of HEAD and the base
branch and at HEAD, and fails on every entry ADDED, or whose number is RAISED, that carries no dated note: a YYYY-MM-DD
on or after the merge-base's date, in a comment on the entry's line or in the comment block directly above it, in a
string on its line, or within the same Map.entry(...) / item(...) call. A removal or a fall always passes.

What an entry is (one reader for every pin's shape): each string literal of the constant's initializer, keyed by its
text with every digit run as '#'; its numbers are the digit runs inside it and the integer literals that follow it up to
the next string literal (Map.entry("k", 3), a register row "file | spelling | 3 | OPEN"). Base and head entries are
aligned by key (difflib), so an entry kept is compared number by number and an entry inserted is added.

Its limits, named: a reordered entry reads as added (a dated note passes it); a pin whose constant is new at HEAD (absent
at the merge-base) is new, not grown, and is not checked -- its first contents are the change's own, reviewed with it; a
watched constant missing at HEAD fails (a rename would otherwise dodge the check).

Usage: shrink_only.py [--base <ref>] [--repo <dir>] [--worktree]. The base defaults to origin/main; --worktree reads
the head side from the working tree. Exit 0 when every grown entry is
dated, 1 otherwise, 2 when the history cannot be read (never a pass: no merge-base is a failure, not a skip).
"""

import argparse
import difflib
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field

# THE WATCHED PINS: file -> the constants whose growth loosens a guard. A pin joins this list with a dated note.
WATCHED = {
    # the edge's ledger and the hook rulings (the census, 2026-11-17)
    "core/src/test/java/com/legend/EdgeSeparationTest.java": [
        "COMPAT_CLASSES", "EDGE_ONLY_SQLFN", "REFEREE", "TEST_SIDE", "CODE_OPEN", "TEST_OPEN", "HOOKS_RULED",
        "CSG_OPEN", "OPEN_LINES", "BUILD_OPEN"],
    # the compatibility spelling register: a row admits a spelling, its count how many
    "core/src/test/java/com/legend/CompatSpellingGuardTest.java": ["REGISTER"],
    # the Java-eval ledger's named residue, file by file
    "core/src/test/java/com/legend/JavaEvalLedgerTest.java": ["EVICT_SIZE"],
    # the GUARD stream's pins (section C, 2026-11-17)
    "core/src/test/java/com/legend/TerminalRowsFrozenTest.java": ["TERMINAL"],
    "core/src/test/java/com/legend/HookLambdasTest.java": ["LAMBDA_HOOKS", "TWO_FORM_CALLERS", "CLASS_HOOKS"],
    "core/src/test/java/com/legend/SqlFnSortedTest.java": [
        "BOTH_WRITERS", "BOTH_AGG", "EDGE_ONLY_NODES", "NO_WRITER_NODES", "NODE_REBUILDERS"],
    "core/src/test/java/com/legend/ServiceLoaderSitesTest.java": ["SITES", "PROVIDERS", "REFLECTIVE"],
    "core/src/test/java/com/legend/PlanStyleSurfaceTest.java": ["TWO_FORM_EXCEPTIONS", "OVERRIDERS"],
    "core/src/test/java/com/legend/JudgingRulesReachTest.java": ["BEHAVIOUR_OPEN", "REACHES_PINNED", "NAMES_PINNED"],
    "core/src/test/java/com/legend/OpenClassMembersTest.java": ["MEMBERS"],
    "core/src/test/java/com/legend/EdgeAuditOpenTest.java": ["AUDIT_OPEN"],
    "core/src/test/java/com/legend/HookRulingsAgreeTest.java": ["RULED_NOT_HOOKS", "RULED_UNCOUNTED"],
    "core/src/test/java/com/legend/EdgePackageBoundaryTest.java": ["EDGE_JARS_IN_THE_HOSTS_LIST"],
    "core/src/test/java/com/legend/ArchitectureTest.java": ["PLANNER_ENTRIES"],
}

RULE = "added or raised without a dated note"

DATE = re.compile(r"\b(20\d\d-\d\d-\d\d)\b")

# the calls whose arguments are one entry: a note anywhere in the call dates each of its strings
ENTRY_CALLS = {"entry", "item", "Item", "row"}


@dataclass
class Token:
    kind: str          # "str", "int" or "plus"
    text: str
    line: int          # 1-based line in the file
    group: int         # the innermost entry call's id, or -1


@dataclass
class Entry:
    key: str
    text: str
    line: int
    group: int
    numbers: list = field(default_factory=list)
    lines: set = field(default_factory=set)


def lex(source, start, end):
    """The string and integer tokens of source[start:end], comments skipped, each with its line and entry group."""
    tokens = []
    line = source.count("\n", 0, start) + 1
    i = start
    stack = []         # one item per open paren: the group id if the call is an entry call, else -1
    next_group = 0
    last_ident = ""
    while i < end:
        c = source[i]
        if c == "\n":
            line += 1
            i += 1
        elif source.startswith("//", i):
            j = source.find("\n", i)
            i = end if j < 0 else j
        elif source.startswith("/*", i):
            j = source.find("*/", i + 2)
            j = end if j < 0 else j + 2
            line += source.count("\n", i, j)
            i = j
        elif c == '"':
            j = i + 1
            buf = []
            while j < end and source[j] != '"':
                if source[j] == "\\" and j + 1 < end:
                    buf.append(source[j:j + 2])
                    j += 2
                else:
                    buf.append(source[j])
                    j += 1
            group = next((g for g in reversed(stack) if g >= 0), -1)
            tokens.append(Token("str", "".join(buf), line, group))
            i = j + 1
            last_ident = ""
        elif c == "'":
            j = i + 1
            while j < end and source[j] != "'":
                j += 2 if source[j] == "\\" else 1
            i = j + 1
        elif c.isdigit() and (i == 0 or not (source[i - 1].isalnum() or source[i - 1] in "_.")):
            j = i
            while j < end and (source[j].isdigit() or source[j] == "_"):
                j += 1
            if j < end and (source[j].isalpha() and source[j] not in "lL" or source[j] == "."):
                i = j          # a float, a hex digit run, an identifier's tail: not a count
                continue
            tokens.append(Token("int", source[i:j].replace("_", ""), line,
                                next((g for g in reversed(stack) if g >= 0), -1)))
            i = j
        elif c.isalpha() or c == "_":
            j = i
            while j < end and (source[j].isalnum() or source[j] == "_"):
                j += 1
            last_ident = source[i:j]
            i = j
        elif c == "(":
            if last_ident in ENTRY_CALLS:
                stack.append(next_group)
                next_group += 1
            else:
                stack.append(-1)
            last_ident = ""
            i += 1
        elif c == ")":
            if stack:
                stack.pop()
            i += 1
        elif c == "+":
            tokens.append(Token("plus", "+", line, -1))
            i += 1
        else:
            if not c.isspace():
                last_ident = "" if c != "." else last_ident and ""
            i += 1
    return tokens


def initializer(source, name):
    """(start, end) of constant `name`'s initializer in source: after `name =`, to its `;` at depth 0; None if absent."""
    m = re.search(r"\b" + re.escape(name) + r"\s*=(?!=)", source)
    if not m:
        return None
    i = m.end()
    depth = 0
    while i < len(source):
        c = source[i]
        if source.startswith("//", i):
            i = source.find("\n", i)
            if i < 0:
                break
            continue
        if source.startswith("/*", i):
            i = source.find("*/", i + 2) + 2
            continue
        if c == '"':
            i += 1
            while i < len(source) and source[i] != '"':
                i += 2 if source[i] == "\\" else 1
        elif c == "'":
            i += 1
            while i < len(source) and source[i] != "'":
                i += 2 if source[i] == "\\" else 1
        elif c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
        elif c == ";" and depth == 0:
            return m.end(), i
        i += 1
    return None


def entries(source, name):
    """The entries of constant `name` in source, or None when it has no initializer there."""
    span = initializer(source, name)
    if span is None:
        return None
    out = []
    prev = None         # the kind of the token before
    joined = False      # a '+' right after a string: the next string continues it ("a" + "b")
    for t in lex(source, *span):
        if t.kind == "plus":
            joined = prev == "str"
            prev = "plus"
            continue
        kind, prev = prev, t.kind
        if t.kind == "str" and joined and kind == "plus" and out:
            e = out[-1]
            e.text += t.text
            e.lines.add(t.line)
        elif t.kind == "str":
            out.append(Entry("", t.text, t.line, t.group, [], {t.line}))
        elif out:
            out[-1].numbers.append(int(t.text))
        joined = False
    for e in out:
        e.key = re.sub(r"\d+", "#", e.text)
        e.numbers = [int(d) for d in re.findall(r"\d+", e.text)] + e.numbers
    return out


def grown(base, head):
    """The head entries added, or kept with a number raised: (entry, why)."""
    out = []
    matcher = difflib.SequenceMatcher(None, [e.key for e in base], [e.key for e in head], autojunk=False)
    for op, b0, b1, h0, h1 in matcher.get_opcodes():
        if op == "equal":
            pairs = zip(base[b0:b1], head[h0:h1])
        elif op in ("insert", "replace"):
            # within a replaced run, an entry whose key the run's base side holds is kept, compared by number
            pool = {}
            for e in base[b0:b1]:
                pool.setdefault(e.key, []).append(e)
            pairs = []
            for e in head[h0:h1]:
                if pool.get(e.key):
                    pairs.append((pool[e.key].pop(0), e))
                else:
                    out.append((e, "added"))
        else:
            continue
        for b, h in pairs:
            raised = [f"{x} -> {y}" for x, y in zip(b.numbers, h.numbers) if y > x]
            if len(h.numbers) > len(b.numbers):
                raised.append(f"a number added ({h.numbers[len(b.numbers):]})")
            if raised:
                out.append((h, "raised: " + ", ".join(raised)))
    return out


def is_comment(line):
    stripped = line.strip()
    return stripped.startswith("//") or stripped.startswith("*") or stripped.startswith("/*")


def dated(source_lines, head_entries, entry, since, top):
    """Whether `entry` carries a date on or after `since`: on its lines (or its entry call's), or in its group's note --
    the nearest comment block above it, read up to the pin's first line `top` (a note heads the rows under it)."""
    def ok(text):
        return any(d >= since for d in DATE.findall(text))

    lines = set(entry.lines)
    if entry.group >= 0:
        for e in head_entries:
            if e.group == entry.group:
                lines |= e.lines
    for n in sorted(lines):
        if ok(source_lines[n - 1]):
            return True
    k = min(lines) - 2
    while k >= top - 1 and not is_comment(source_lines[k]):
        k -= 1
    while k >= 0 and is_comment(source_lines[k]):
        if ok(source_lines[k]):
            return True
        k -= 1
    return False


def check(read_base, read_head, since, watched=None):
    """Every grown entry without a dated note, as messages naming the entry and the rule."""
    failures = []
    for path, names in (watched or WATCHED).items():
        head_source = read_head(path)
        if head_source is None:
            failures.append(f"{path}: a watched guard file is gone at HEAD: move its pins and this list with a dated"
                            " note (tools/guards/shrink_only.py)")
            continue
        base_source = read_base(path)
        head_lines = head_source.split("\n")
        for name in names:
            head = entries(head_source, name)
            if head is None:
                failures.append(f"{path}: the watched pin {name} is not found at HEAD (renamed or removed): update"
                                " tools/guards/shrink_only.py's list with a dated note")
                continue
            base = entries(base_source, name) if base_source is not None else None
            if base is None:
                continue        # a pin new at HEAD: its first contents are the change's own
            top = head_source.count("\n", 0, initializer(head_source, name)[0]) + 1
            for entry, why in grown(base, head):
                if not dated(head_lines, head, entry, since, top):
                    failures.append(f"{path}:{entry.line}: {name} entry \"{entry.text}\" {why} -- {RULE} (a"
                                    f" YYYY-MM-DD on or after {since} on its line or in the comment above it)")
    return failures


def git(repo, *args):
    return subprocess.run(["git", "-C", repo, *args], check=True, capture_output=True, text=True).stdout


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--base", default="origin/main")
    parser.add_argument("--repo", default=None)
    parser.add_argument("--worktree", action="store_true",
                        help="read the head side from the working tree, not HEAD (a check before a commit)")
    args = parser.parse_args(argv)
    # the repository this script is in (under Bazel, the runfiles link resolves to the checkout's own file)
    repo = args.repo or os.path.dirname(os.path.realpath(__file__))
    try:
        repo = git(repo, "rev-parse", "--show-toplevel").strip()
        merge_base = git(repo, "merge-base", "HEAD", args.base).strip()
        since = git(repo, "show", "-s", "--format=%cs", merge_base).strip()
    except subprocess.CalledProcessError as e:
        print(f"shrink_only: the history cannot be read ({e.stderr.strip()}): a full clone with {args.base} is needed",
              file=sys.stderr)
        return 2

    def read_at(rev):
        def read(path):
            try:
                return git(repo, "show", f"{rev}:{path}")
            except subprocess.CalledProcessError:
                return None
        return read

    def read_worktree(path):
        try:
            with open(os.path.join(repo, path), encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            return None

    failures = check(read_at(merge_base), read_worktree if args.worktree else read_at("HEAD"), since)
    print(f"shrink_only: HEAD against the merge-base {merge_base[:10]} ({args.base}, {since}): "
          f"{sum(len(n) for n in WATCHED.values())} pins in {len(WATCHED)} files")
    for f in failures:
        print("  " + f)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
