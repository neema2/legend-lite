#!/usr/bin/env python3
# Copyright 2026 Legend Contributors
# SPDX-License-Identifier: Apache-2.0
"""SHRINK-ONLY, BY THE MERGE-BASE (the edge audit of 2026-11-17, section C, item 11's second half; the GUARD stream's
C.13-CI, the orchestrator's GO of 2026-11-17; its review's M1, M2 and S1-S6 fixed 2026-11-18).

The guard files' pins move only with a dated note (AGENTS.md: "a ceiling, pin, or allowlist entry moves ONLY with a
dated justification comment"). This script reads each WATCHED pin -- a constant of a guard file -- at the merge-base of
the head and the base branch and at the head, and fails on every entry ADDED, or whose count is RAISED, that carries no
dated note. A GROW_ONLY pin is a vocabulary whose REMOVAL loosens its guard (a class dropped from it is no longer
flagged), so there a removal is what needs the note. WATCHED and GROW_ONLY are pins of this script too: a pin dropped
from either needs a dated note in this file naming it.

A NOTE IS THE CHANGE'S OWN. A note counts when its line is new in merge-base..head -- git blame attributes it to a
commit of the change, or to the working tree -- and it holds a YYYY-MM-DD (the review of 351e80daa2, 2026-11-18, M2: a
floor of dates was the author's to set -- one backdated commit, or a branch merged in, lowered it -- and a squash
merge's date moved it past every note). Without the history (the rule's own tests) a note is a line holding a date on or
after the floor given.

WHAT AN ENTRY IS. The pin's initializer is parsed as an expression: string literals (text blocks too), decimal integer
literals, true and false, '+' between strings, the collection calls Map.of, Map.ofEntries, Map.entry, Set.of,
List.of, and the guard files' own helpers entries(...) (pairs whose value is a space-separated list), item(...) and
new Item(...) (an id and its attributes), and an unqualified identifier naming a constant of the same file (resolved
by parsing its own initializer). Anything else -- a qualified name (Integer.MAX_VALUE, Other.ALLOW), a method call
(Evil.class.getName()), a cast, a negative, hex, octal or floating literal, a '+' with a non-string side -- is
REFUSED by name, never read past: the check cannot tell what it admits. An entry is a PATH of keys down to a member:
Map.entry("k", Set.of("a")) is ("k",) and ("k", "a"); a count is an integer at the end of a path, or the third field
of a register row ("file | spelling | 3 | STATUS", whose identity is the other three fields). Digits are part of an
identity ("PureText$1" is not "PureText$2"); a value moved from one key to another is an entry added under the new key.

WHERE A NOTE IS. On any line of the entry (its keys, its member, its Map.entry/item call), or in the nearest comment
block above the entry's first line within the pin (the declaration's own javadoc does not date its rows). A note for a
removal from a GROW_ONLY pin is a comment within the pin, or in the comment block directly above its declaration,
naming the removed entry (its text, or the simple name after the last '.', '$' or '#') with a date on or after the floor.

Its limits, named:
- A note heads the rows under it: one new group note dates every row below it up to the next comment, at any
  distance.
- The reader reads the constant's own initializer and the same file's constants it names; a same-file redefinition of
  what it reads by name -- a nested class named Set, a helper entries(...) redefined, a local variable shadowing the
  pin where a guard uses it -- is not seen (the review of 351e80daa2, 2026-11-18, SHOULD).
- A pin new at the head (absent at the merge-base) is new, not grown, and is not checked: its first contents are the
  change's own, reviewed with it. A pin dropped from WATCHED needs a dated note (above), so renaming a pin is visible.
- A note is text: the check proves a dated note was written, not that it is true.
- The head is a committed revision (--head, default HEAD) or, with --worktree, the working tree.
- A watched file renamed in the change is compared with its old path's text at the merge-base (git's rename
  detection, -M).
- CI and //tools/guards:shrink_only_check run the merge-base's copy of this script, so a change's own script never
  judges it; a base without one runs the head's (the check's first landing). WHAT CHOOSES THE COPY IS THE CHANGE'S OWN
  (the review of the guard line, G, SHOULD 3 and NOTE 2, 2026-11-18): under //gates:local the wrapper and its target
  are the head's (Bazel runs the head's entry); in CI gate.yml's step is the head's (GitHub runs the pushed or proposed
  commit's workflow, on every event). Each judges the other: the merge-base's copy the wrapper runs reads a change to
  gate.yml's job as one of the check's own files, and the copy CI's step runs reads a change to the wrapper or its
  target the same way (the review's scenario H: //gates:local passed it, CI failed it). A change that rewrites both
  choosers at once is judged by neither; the notes each must carry are what show it.

THE CHECK'S OWN FILES (review F of the guard line, 2026-11-18, MUST-FIX 3: a change could loosen the script and the next
exploit it, each passing). A change to this script, to its wrapper (shrink_only_check.py), to their targets in
tools/guards/BUILD.bazel, to gate.yml's jobs naming the check (the shrink-only job whole, and green, which needs it: the
review of the guard line, G, MUST-FIX 3, 2026-11-18 -- the step was found by a "- name:" line the real one does not
have, and a job-level edit lay outside every step), or to the gate's lines naming it (gates/BUILD.bazel, each with the
comment block above it: G's SHOULD 4) carries a note in that file's changed part, new in the change and holding a
YYYY-MM-DD, so a loosening is a reviewed, dated act. main() reads WATCHED and GROW_ONLY from this script's own text (its literals, never the module's globals), and
the wrapper chooses the merge-base's copy by its fixed path with git alone, reading nothing from the head's module first.
Remaining, named: what chooses the copy is the change's own (the limits above); a watched file renamed or a pin removed
fails under the merge-base's copy whatever the head's lists say, so such a move takes two changes (the lists first, with
their notes), and between the two landings every other change fails too ("a watched guard file is gone at the head":
closed, never open; G's NOTE 3); git blame runs with -w -M -C (a whitespace edit, a line moved or copied keeps its
origin), but a date typed by hand on a new line is a new note; inside a Java text block a line starting with // reads
as a comment (latent: no watched pin is a text block).

Usage: shrink_only.py [--base <ref>] [--head <rev>] [--repo <dir>] [--worktree]. Exit 0 when every move is dated,
1 otherwise, 2 when the history cannot be read (never a pass: no merge-base, an unreadable blob or a git error is a
failure, not a skip).
"""

import argparse
import ast
import os
import re
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass, field

SELF = "tools/guards/shrink_only.py"

# THE WATCHED PINS: file -> the constants whose growth loosens a guard. A pin joins this list with a dated note; a pin
# leaves it only with a dated note in this file naming it (WATCHED is read against the merge-base's own list).
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
    # the only-edge-callers guard (J40, 2026-11-18, the orchestrator's triage ruling): its census lines, its read
    # APIs, J21's ruled members and the hook entries move only with a dated note
    "core/src/test/java/com/legend/EdgeOnlyMembersTest.java": ["OPEN", "READ_API", "RULED_J21"],
    "core/src/test/java/com/legend/HookRulingTest.java": ["HOOK_ENTRIES"],
    "core/src/test/java/com/legend/HookRulingsAgreeTest.java": ["RULED_NOT_HOOKS", "RULED_UNCOUNTED"],
    "core/src/test/java/com/legend/EdgePackageBoundaryTest.java": ["EDGE_JARS_IN_THE_HOSTS_LIST"],
    "core/src/test/java/com/legend/ArchitectureTest.java": ["PLANNER_ENTRIES"],
}

# THE GROW-ONLY PINS (the review of C.13-CI, S5, 2026-11-18): vocabularies EdgeSeparationTest.found() flags by
# membership -- a class dropped from one is no longer flagged, so a removal loosens and needs the dated note; an
# addition flags more and passes.
GROW_ONLY = {
    "core/src/test/java/com/legend/EdgeSeparationTest.java": ["COMPAT_CLASSES", "EDGE_ONLY_SQLFN", "REFEREE"],
}

RULE = "added or raised without a dated note"
REMOVED_RULE = "removed from a grow-only pin without a dated note naming it"

DATE = re.compile(r"\b(20\d\d-\d\d-\d\d)\b")

# a register row: "file | spelling | count | STATUS" -- its identity the three other fields, its count the third
ROW = re.compile(r"^(.+?) \| (.+) \| (\d+) \| ([A-Z_]+)$")

# the collection calls read, by their last name (java.util.Map.of is Map.of)
PAIR_CALLS = {"Map.of", "entries"}
ENTRY_CALL = "Map.entry"
ENTRIES_CALL = "Map.ofEntries"
MEMBER_CALLS = {"Set.of", "List.of"}
ITEM_CALLS = {"item", "new Item"}


class Unread(Exception):
    """A pin holds text the check does not read: it fails, naming the text, rather than read past it."""


class HistoryError(Exception):
    """git could not answer: exit 2, never a pass."""


@dataclass
class Tok:
    kind: str           # "str", "int", "id", "new", "(", ")", ",", "+"
    text: str
    first: int          # its first and last line (a string or text block may span lines)
    last: int


@dataclass
class Node:
    kind: str           # "str", "int", "bool", "call"
    value: object       # the text, the integer, the boolean, or the call's name
    args: list = field(default_factory=list)
    lines: set = field(default_factory=set)     # every line of the node, (line, top) pairs
    span: tuple = None  # a call's (first, last, top)


@dataclass
class Fact:
    path: tuple
    count: object       # an int, or None
    anchors: set        # (line, top): the lines whose text or note above dates it
    spans: list         # (first, last, top): an entry call's lines, any of which may hold its note


def blanked(source):
    """source with every comment and every literal's content spaced out, lines and offsets kept: the code alone."""
    out = list(source)
    i, n = 0, len(source)

    def space(a, b):
        for k in range(a, b):
            if out[k] != "\n":
                out[k] = " "
    while i < n:
        if source.startswith("//", i):
            j = source.find("\n", i)
            j = n if j < 0 else j
            space(i, j)
            i = j
        elif source.startswith("/*", i):
            j = source.find("*/", i + 2)
            j = n if j < 0 else j + 2
            space(i, j)
            i = j
        elif source.startswith('"""', i):
            j = i + 3
            while j < n and not source.startswith('"""', j):
                j += 2 if source[j] == "\\" else 1
            j = min(n, j + 3)
            space(i + 1, j - 1)
            i = j
        elif source[i] in "\"'":
            q = source[i]
            j = i + 1
            while j < n and source[j] != q and source[j] != "\n":
                j += 2 if source[j] == "\\" else 1
            space(i + 1, j)
            i = j + 1
        else:
            i += 1
    return "".join(out)


def declaration(source, name):
    """(start, end, top) of constant `name`'s initializer -- after its `=`, to its `;` -- and its declaration's line,
    anchored on `static final ... NAME =` in the code alone (a comment's or a string's `NAME =` is no declaration)."""
    code = blanked(source)
    found = [m for m in re.finditer(r"\bstatic\s+final\s+[^;=(){}]*?\b" + re.escape(name) + r"\s*=(?!=)", code)]
    if not found:
        return None
    if len(found) > 1:
        raise Unread(f"{name} is declared {len(found)} times")
    m = found[0]
    depth = 0
    i = m.end()
    while i < len(code):
        c = code[i]
        if c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
        elif c == ";" and depth == 0:
            return m.end(), i, source.count("\n", 0, m.start()) + 1
        i += 1
    raise Unread(f"{name}'s initializer has no end")


def lex(source, start, end):
    """The tokens of source[start:end]: literals, names and the punctuation the reader knows; comments skipped;
    anything else refused."""
    toks = []
    line = source.count("\n", 0, start) + 1
    i = start
    while i < end:
        c = source[i]
        if c == "\n":
            line += 1
            i += 1
        elif c.isspace():
            i += 1
        elif source.startswith("//", i):
            j = source.find("\n", i)
            i = end if j < 0 or j > end else j
        elif source.startswith("/*", i):
            j = source.find("*/", i + 2)
            j = end if j < 0 else j + 2
            line += source.count("\n", i, j)
            i = j
        elif source.startswith('"""', i):
            j = source.find("\n", i) + 1
            buf = []
            while j < end and not source.startswith('"""', j):
                if source[j] == "\\":
                    buf.append(source[j:j + 2])
                    j += 2
                else:
                    buf.append(source[j])
                    j += 1
            text = "".join(buf)
            first = line
            line += source.count("\n", i, j + 3)
            toks.append(Tok("str", text, first, line))
            i = j + 3
        elif c == '"':
            j = i + 1
            buf = []
            while j < end and source[j] != '"':
                if source[j] == "\n":
                    raise Unread(f"an unterminated string at line {line}")
                if source[j] == "\\":
                    buf.append(source[j:j + 2])
                    j += 2
                else:
                    buf.append(source[j])
                    j += 1
            toks.append(Tok("str", "".join(buf), line, line))
            i = j + 1
        elif c.isdigit():
            m = re.match(r"\d[\d_]*[lL]?(?![\w.])", source[i:end])
            if not m or (len(m.group().rstrip("lL").replace("_", "")) > 1 and m.group().startswith("0")):
                raise Unread(f"a number at line {line} that is not a decimal integer literal: "
                             f"{re.match(r'[\w.]+', source[i:end]).group()}")
            toks.append(Tok("int", m.group().rstrip("lL").replace("_", ""), line, line))
            i += len(m.group())
        elif c.isalpha() or c in "_$":
            m = re.match(r"[A-Za-z_$][\w$]*(?:\s*\.\s*[A-Za-z_$][\w$]*)*", source[i:end])
            text = re.sub(r"\s+", "", m.group())
            line_end = line + m.group().count("\n")
            toks.append(Tok("new" if text == "new" else "id", text, line, line_end))
            line = line_end
            i += len(m.group())
        elif c in "(),+":
            toks.append(Tok(c, c, line, line))
            i += 1
        else:
            raise Unread(f"'{c}' at line {line}")
    return toks


class Parser:
    """The pin's initializer as a tree of literals and the known collection calls; a constant of the same file is
    resolved by parsing its own initializer."""

    def __init__(self, source, top, constants, seen=()):
        self.source = source
        self.top = top
        self.constants = constants      # name -> the file's source, to resolve a same-file constant
        self.seen = seen

    def parse(self, start, end):
        self.toks = lex(self.source, start, end)
        self.i = 0
        node = self.expr()
        if self.i != len(self.toks):
            t = self.toks[self.i]
            raise Unread(f"'{t.text}' at line {t.first} after the initializer's expression")
        return node

    def peek(self, kind=None):
        if self.i >= len(self.toks):
            return None
        t = self.toks[self.i]
        return t if kind is None or t.kind == kind else None

    def take(self, kind):
        t = self.peek(kind)
        if t is None:
            got = self.peek()
            raise Unread(f"expected '{kind}' but read {('the end' if got is None else repr(got.text))}"
                         + ("" if got is None else f" at line {got.first}"))
        self.i += 1
        return t

    def lines(self, t):
        return {(k, self.top) for k in range(t.first, t.last + 1)}

    def expr(self):
        node = self.term()
        while self.peek("+"):
            plus = self.take("+")
            right = self.term()
            if node.kind != "str" or right.kind != "str":
                raise Unread(f"a '+' at line {plus.first} with a side that is not a string")
            node = Node("str", node.value + right.value, lines=node.lines | right.lines)
        return node

    def term(self):
        t = self.peek()
        if t is None:
            raise Unread("an initializer that ends early")
        if t.kind == "str":
            self.i += 1
            return Node("str", t.text, lines=self.lines(t))
        if t.kind == "int":
            self.i += 1
            return Node("int", int(t.text), lines=self.lines(t))
        if t.kind == "new":
            self.i += 1
            name = self.take("id")
            return self.call("new " + name.text, t)
        if t.kind == "id":
            self.i += 1
            if t.text in ("true", "false"):
                return Node("bool", t.text == "true", lines=self.lines(t))
            if self.peek("("):
                return self.call(t.text, t)
            return self.constant(t)
        raise Unread(f"'{t.text}' at line {t.first}")

    def call(self, name, start):
        short = re.sub(r"^java\.util\.", "", name)
        known = PAIR_CALLS | MEMBER_CALLS | ITEM_CALLS | {ENTRY_CALL, ENTRIES_CALL}
        if short not in known:
            raise Unread(f"a call to {name} at line {start.first}")
        self.take("(")
        args = []
        if not self.peek(")"):
            args.append(self.expr())
            while self.peek(","):
                self.take(",")
                args.append(self.expr())
        close = self.take(")")
        lines = set().union(*(a.lines for a in args)) | self.lines(start) | self.lines(close)
        return Node("call", short, args, lines, (start.first, close.last, self.top))

    def constant(self, t):
        if "." in t.text:
            raise Unread(f"{t.text} at line {t.first}: a name of another class (the check reads this file's"
                         " constants alone)")
        if t.text in self.seen:
            raise Unread(f"{t.text} refers to itself")
        span = declaration(self.constants, t.text)
        if span is None:
            raise Unread(f"{t.text} at line {t.first}: no constant of that name in this file")
        start, end, top = span
        node = Parser(self.constants, top, self.constants, self.seen + (t.text,)).parse(start, end)
        node.lines = node.lines | self.lines(t)
        return node


def identity(text):
    """A string's identity and its count: a register row's other three fields and its count, else the text."""
    m = ROW.match(text)
    if m:
        return f"{m.group(1)} | {m.group(2)} | # | {m.group(4)}", int(m.group(3))
    return text, None


def key(node):
    if node.kind != "str":
        raise Unread(f"a key that is not a string at line {min(node.lines)[0]}")
    return node


def facts(node, prefix=(), spans=()):
    """The entries of a parsed pin, each a path from its keys to its member."""
    out = []
    if node.kind == "str" and "\n" in node.value:
        # a text block: each of its lines an entry at its own line (the review of 351e80daa2, 2026-11-18: one dated
        # line dated the whole block)
        top = min(node.lines)[1]
        first = min(ln for ln, _ in node.lines) + 1
        for i, text in enumerate(node.value.split("\n")):
            if text.strip():
                ident, count = identity(text.strip())
                out.append(Fact(prefix + (ident,), count, {(first + i, top)}, list(spans)))
    elif node.kind == "str":
        ident, count = identity(node.value)
        out.append(Fact(prefix + (ident,), count, set(node.lines), list(spans)))
    elif node.kind == "int":
        out.append(Fact(prefix, node.value, set(node.lines), list(spans)))
    elif node.kind == "bool":
        out.append(Fact(prefix + (f"<{str(node.value).lower()}>",), None, set(node.lines), list(spans)))
    elif node.value in MEMBER_CALLS:
        for a in node.args:
            out += facts(a, prefix, spans)
    elif node.value == ENTRIES_CALL:
        for a in node.args:
            if a.kind != "call" or a.value != ENTRY_CALL:
                raise Unread(f"Map.ofEntries holds something other than Map.entry at line {min(a.lines)[0]}")
            out += facts(a, prefix, spans)
    elif node.value == ENTRY_CALL:
        if len(node.args) != 2:
            raise Unread(f"Map.entry with {len(node.args)} arguments at line {node.span[0]}")
        out += pair(node.args[0], node.args[1], prefix, spans + (node.span,), False)
    elif node.value in PAIR_CALLS:
        if len(node.args) % 2:
            raise Unread(f"{node.value} with an odd number of arguments at line {node.span[0]}")
        for k, v in zip(node.args[0::2], node.args[1::2]):
            first = min(k.lines | v.lines)
            last = max(x for x, _ in k.lines | v.lines)
            out += pair(k, v, prefix, spans + ((first[0], last, first[1]),), node.value == "entries")
    elif node.value in ITEM_CALLS:
        if not node.args:
            raise Unread(f"{node.value} with no id at line {node.span[0]}")
        k = key(node.args[0])
        here = spans + (node.span,)
        kid, kcount = identity(k.value)
        out.append(Fact(prefix + (kid,), kcount, set(k.lines), list(here)))
        for a in node.args[1:]:
            out += [Fact(f.path, f.count, f.anchors | k.lines, f.spans) for f in facts(a, prefix + (kid,), here)]
    else:
        raise Unread(f"a call to {node.value}")
    return out


def pair(k, v, prefix, spans, split):
    """A key and its value: the key itself an entry (its count a register row's), the value's entries under it."""
    k = key(k)
    kid, kcount = identity(k.value)
    out = [Fact(prefix + (kid,), kcount, set(k.lines), list(spans))]
    if split:
        # entries(...)'s value is a space-separated list of members (ArchitectureTest.entries)
        if v.kind != "str":
            raise Unread(f"an entries(...) value that is not a string at line {min(v.lines)[0]}")
        for member in v.value.split(" "):
            out.append(Fact(prefix + (kid, member), None, set(k.lines) | v.lines, list(spans)))
    else:
        out += [Fact(f.path, f.count, f.anchors | k.lines, f.spans) for f in facts(v, prefix + (kid,), spans)]
    return out


def read_pin(source, name):
    """The entries of constant `name` in source, and its span (start, end, top); None when it is not declared."""
    span = declaration(source, name)
    if span is None:
        return None
    start, end, top = span
    node = Parser(source, top, source, (name,)).parse(start, end)
    return facts(node), span


def counted(entries):
    """Each path's multiplicity and its counts (a path may repeat: a list keeps duplicates)."""
    many = Counter(f.path for f in entries)
    counts = {}
    for f in entries:
        if f.count is not None:
            counts.setdefault(f.path, []).append(f.count)
    return many, counts


def grown(base, head):
    """The head entries added, or with a count raised: (fact, why). An entry under a key that is itself added is
    named once, by its key."""
    bmany, bcounts = counted(base)
    hmany, hcounts = counted(head)
    out = []
    added = set()
    seen = Counter()
    for f in head:
        seen[f.path] += 1
        if seen[f.path] > bmany.get(f.path, 0):
            if f.path not in added and not any(f.path[:k] in added for k in range(1, len(f.path))):
                out.append((f, "added"))
            added.add(f.path)
    for path, hs in hcounts.items():
        if path in added:
            continue
        bs = sorted(bcounts.get(path, []))
        hs_sorted = sorted(hs)
        raised = [f"{b} -> {h}" for b, h in zip(bs, hs_sorted) if h > b]
        if len(hs_sorted) > len(bs):
            raised.append(f"a count added ({hs_sorted[len(bs):]})")
        if raised:
            f = next(x for x in head if x.path == path and x.count is not None)
            out.append((f, "raised: " + ", ".join(raised)))
    return out


def removed(base, head):
    """The base entries the head no longer holds (a grow-only pin's loosening)."""
    hmany, _ = counted(head)
    out = []
    seen = Counter()
    gone = set()
    for f in base:
        seen[f.path] += 1
        if seen[f.path] > hmany.get(f.path, 0) and not any(f.path[:k] in gone for k in range(1, len(f.path))):
            out.append(f)
            gone.add(f.path)
    return out


def is_comment(line):
    stripped = line.strip()
    return stripped.startswith("//") or stripped.startswith("*") or stripped.startswith("/*")


def recent(text, since):
    return any(d >= since for d in DATE.findall(text))


def noted(lines, n, since, fresh):
    """Whether line n (1-based) is a note: new in the change (fresh, from git blame) holding a date, or, without the
    history (fresh None), holding a date on or after `since`."""
    if fresh is None:
        return recent(lines[n - 1], since)
    return n in fresh and bool(DATE.search(lines[n - 1]))


def dated(lines, fact, since, fresh=None):
    """Whether `fact` carries a note: on one of its lines or its entry call's, or in the nearest comment block above its
    first line, within the declaration it is read from (never the declaration's javadoc)."""
    own = {ln for ln, _ in fact.anchors}
    for first, last, _ in fact.spans:
        own |= set(range(first, last + 1))
    if any(noted(lines, n, since, fresh) for n in own):
        return True
    firsts = {}
    for ln, top in list(fact.anchors) + [(first, top) for first, _, top in fact.spans]:
        firsts[top] = min(ln, firsts.get(top, ln))
    for top, first in firsts.items():
        k = first - 2                               # the line above, as an index
        while k >= top - 1 and not is_comment(lines[k]):
            k -= 1
        while k >= top - 1 and is_comment(lines[k]):
            if noted(lines, k + 1, since, fresh):
                return True
            k -= 1
    return False


def comment_text(line):
    """A line's comment: the whole line when it is one, else the text after its '//' (or nothing)."""
    if is_comment(line):
        return line
    k = line.find("//")
    return line[k:] if k >= 0 else ""


def names_it(text, name):
    """Whether `text` names `name` as a whole word: not inside a longer name (the review of 351e80daa2, 2026-11-18, M3:
    "PlannedAppeals" excused the removal of "Appeal")."""
    return re.search(r"(?<![\w.$#])" + re.escape(name) + r"(?![\w$])", text) is not None


def named(source, span, fact, since, fresh=None):
    """Whether a note within the pin, or in the comment block directly above its declaration, names the removed
    `fact` (its text, or the simple name after the last '.', '$' or '#') as a whole word."""
    _, end, top = span
    lines = source.split("\n")
    leaf = fact.path[-1]
    names = {n for n in (leaf, re.split(r"[.$#]", leaf)[-1]) if n}
    at = []
    k = top - 2
    while k >= 0 and is_comment(lines[k]):
        at.append(k + 1)
        k -= 1
    at += list(range(top, source.count("\n", 0, end) + 2))
    return any(noted(lines, n, since, fresh) and any(names_it(comment_text(lines[n - 1]), m) for m in names)
               for n in at)


LISTS = ("WATCHED", "GROW_ONLY")

# the text this module was made from, when it was not loaded from its own file (the wrapper sets it on the merge-base's
# copy): own_lists() reads the lists from it
SOURCE = None


def own_lists():
    """WATCHED and GROW_ONLY as this script's own text writes them -- its literals, read from its source, never the
    module's globals (review F, 2026-11-18, MUST-FIX 3: an alias of WATCHED changed it at run time where the statement
    check refused none)."""
    text = SOURCE
    if text is None:
        with open(os.path.realpath(__file__), encoding="utf-8") as f:
            text = f.read()
    lists = watched_lists(text)

    def grouped(pairs):
        out = {}
        for path, name in sorted(pairs):
            out.setdefault(path, []).append(name)
        return out
    return grouped(lists.get("WATCHED", set())), grouped(lists.get("GROW_ONLY", set()))


def watched_lists(script):
    """WATCHED and GROW_ONLY as this script's text at some revision declares them: {(file, name)} for each. Each is
    one literal, assigned once at the module's top; any other statement that could change one -- an assignment, an
    augmented one, a subscript or attribute store, a delete, a mutating method call, a global declaration -- is refused
    (the review of 351e80daa2, 2026-11-18, M1: a .remove() after the literal dropped a pin the literal still listed)."""
    tree = ast.parse(script)
    out = {}
    literals = set()
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) \
                and node.targets[0].id in LISTS:
            if node.targets[0].id in out:
                raise ValueError(f"{node.targets[0].id} is assigned twice")
            value = ast.literal_eval(node.value)
            out[node.targets[0].id] = {(f, n) for f, names in value.items() for n in names}
            literals.add(id(node.targets[0]))
    def root(node):
        # the name an attribute or subscript chain starts from (WATCHED["a"].remove: WATCHED)
        while isinstance(node, (ast.Attribute, ast.Subscript)):
            node = node.value
        return node.id if isinstance(node, ast.Name) else None

    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id in LISTS and isinstance(node.ctx, (ast.Store, ast.Del)) \
                and id(node) not in literals:
            raise ValueError(f"{node.id} is changed at line {node.lineno}")
        if isinstance(node, (ast.Subscript, ast.Attribute)) and isinstance(node.ctx, (ast.Store, ast.Del)) \
                and root(node) in LISTS:
            raise ValueError(f"{root(node)} is changed at line {node.lineno}")
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and root(node.func) in LISTS \
                and node.func.attr not in ("get", "items", "keys", "values", "count", "index", "copy"):
            raise ValueError(f"{root(node.func)}...{node.func.attr}(...) at line {node.lineno}")
        if isinstance(node, ast.Global) and set(node.names) & set(LISTS):
            raise ValueError(f"a global declaration of {set(node.names) & set(LISTS)} at line {node.lineno}")
    return out


def watched_shrinks(base_script, head_script, since, fresh=None):
    """A pin dropped from WATCHED or GROW_ONLY since the merge-base, with no note in this script naming it."""
    if base_script is None:
        return []        # this script is new at the head: its lists are the change's own
    failures = []
    try:
        base, head = watched_lists(base_script), watched_lists(head_script)
    except (SyntaxError, ValueError) as e:
        return [f"{SELF}: its WATCHED or GROW_ONLY list cannot be read ({e})"]
    lines = head_script.split("\n")
    notes = [(n, ln) for n, ln in enumerate(lines, 1) if ln.strip().startswith("#") or " # " in ln]
    for listed in LISTS:
        for path, name in sorted(base.get(listed, set()) - head.get(listed, set())):
            if not any(noted(lines, n, since, fresh) and names_it(ln, name) for n, ln in notes):
                failures.append(f"{SELF}: {name} ({path}) dropped from {listed} -- a pin leaves the list only with a"
                                f" note in this file naming it (new in this change, holding a YYYY-MM-DD)")
    return failures


# the check's own files: a change to one carries a dated note new in the change, in the file -- or, for a shared file, in
# the region of it that is the check's (review F, 2026-11-18, MUST-FIX 3)
OWN_FILES = (SELF, "tools/guards/shrink_only_check.py", "tools/guards/BUILD.bazel", ".github/workflows/gate.yml",
             # the gate's suites, which run the check by name (the review of the guard line, G, SHOULD 4, 2026-11-18)
             "gates/BUILD.bazel")


def own_region(path, text):
    """The lines (1-based, a set) of `text` that are the check's own: the whole of the two scripts; tools/guards'
    BUILD file's targets naming shrink_only; the gate's lines naming shrink_only, each with the comment block above it;
    the workflow's jobs naming the check, whole, and its every other line naming it. None when the file holds none."""
    lines = text.split("\n")
    if path in (SELF, "tools/guards/shrink_only_check.py"):
        return set(range(1, len(lines) + 1))
    out = set()
    if path == "gates/BUILD.bazel":
        # the suites list the check by name (the review of the guard line, G, SHOULD 4, 2026-11-18: dropping it from
        # //gates:local passed): each line naming it, and the comment block directly above it
        for n, line in enumerate(lines, 1):
            if "shrink_only" in line:
                out.add(n)
                k = n - 1
                while k >= 1 and lines[k - 1].lstrip().startswith("#"):
                    out.add(k)
                    k -= 1
        return out or None
    if path.endswith("BUILD.bazel"):
        # a top-level call: from a line opening it at column 0 to the line closing it at column 0
        start = None
        for n, line in enumerate(lines, 1):
            if start is None and re.match(r"\w+\(", line):
                start = n
            if start is not None and line.startswith(")"):
                if any("shrink_only" in lines[k - 1] for k in range(start, n + 1)):
                    out |= set(range(start, n + 1))
                start = None
        return out or None
    # the workflow (the review of the guard line, G, MUST-FIX 3, 2026-11-18: a step was found by its "- name:" line, and
    # the real step opens "- env:", so the workflow held no region; and a job-level edit -- continue-on-error, the check
    # dropped from green's needs -- lay outside every step): each job a line of which names the check, from its key to
    # the line before the next one indented as little (the shrink-only job whole, and green, which needs it), and every
    # other line naming it
    def names(line):
        return "shrink_only" in line or "shrink-only" in line
    jobs = next((n for n, line in enumerate(lines, 1) if re.match(r"jobs:\s*(#.*)?$", line)), None)
    if jobs is not None:
        indent, keys = None, []
        for n in range(jobs + 1, len(lines) + 1):
            line = lines[n - 1]
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            at = len(line) - len(line.lstrip())
            indent = at if indent is None else indent
            if at < indent:
                break
            if at == indent and re.match(r"\s*[\w-]+:\s*(#.*)?$", line):
                keys.append(n)
        for n in keys:
            end = len(lines)
            for k in range(n + 1, len(lines) + 1):
                line = lines[k - 1]
                if line.strip() and len(line) - len(line.lstrip()) <= indent:
                    end = k - 1
                    break
            if any(names(lines[k - 1]) for k in range(n, end + 1)):
                out |= set(range(n, end + 1))
    out |= {n for n, line in enumerate(lines, 1) if names(line)}
    return out or None


def own_files_moved(read_base, read_head, since, fresh):
    """Each of the check's own files whose region changed since the merge-base without a note new in the change in that
    region (a line holding a YYYY-MM-DD)."""
    failures = []
    for path in OWN_FILES:
        base, head = read_base(path), read_head(path)
        if base is None:
            continue        # new at the head: the check's first landing, its contents the change's own
        base_region = own_region(path, base)
        if base_region is None:
            continue
        if head is None:
            failures.append(f"{path}: one of the check's own files is gone at the head")
            continue
        head_region = own_region(path, head)
        if head_region is None:
            failures.append(f"{path}: the check's part of it is gone at the head")
            continue
        base_lines, head_lines = base.split("\n"), head.split("\n")
        if [base_lines[n - 1] for n in sorted(base_region)] == [head_lines[n - 1] for n in sorted(head_region)]:
            continue
        new = fresh(path) if fresh is not None else None
        if not any(noted(head_lines, n, since, new) for n in head_region):
            failures.append(f"{path}: the check's own text changed without a note -- a change to the check carries a"
                            f" note new in this change, in that file's changed part, holding a YYYY-MM-DD")
    return failures


def check(read_base, read_head, since, watched=None, grow_only=None, self_check=True, fresh=None):
    """Every move without a note, as messages naming the entry and the rule. `fresh(path)` gives the head's lines new
    in the change (git blame); None reads a note's date against `since` instead."""
    failures = []
    new = (lambda path: fresh(path)) if fresh is not None else (lambda path: None)
    if self_check:
        head_script = read_head(SELF)
        if head_script is None:
            failures.append(f"{SELF}: this script is gone at the head")
        else:
            failures += watched_shrinks(read_base(SELF), head_script, since, new(SELF))
        failures += own_files_moved(read_base, read_head, since, fresh)
    grow_only = GROW_ONLY if grow_only is None else grow_only
    for path, names in (WATCHED if watched is None else watched).items():
        head_source = read_head(path)
        if head_source is None:
            failures.append(f"{path}: a watched guard file is gone at the head: move its pins and this list with a"
                            f" dated note ({SELF})")
            continue
        base_source = read_base(path)
        head_lines = head_source.split("\n")
        head_new = None
        for name in names:
            try:
                read = read_pin(head_source, name)
            except Unread as e:
                failures.append(f"{path}: the watched pin {name} holds text the check does not read: {e}")
                continue
            if read is None:
                failures.append(f"{path}: the watched pin {name} is not found at the head (renamed or removed):"
                                f" update {SELF}'s list with a dated note")
                continue
            head, span = read
            try:
                base_read = read_pin(base_source, name) if base_source is not None else None
            except Unread:
                base_read = None    # a merge-base form the reader refuses: the head's entries are read as new
            if base_read is None:
                continue        # a pin new at the head: its first contents are the change's own
            base = base_read[0]
            if head_new is None:
                head_new = new(path)
            if name in grow_only.get(path, ()):
                for fact in removed(base, head):
                    if not named(head_source, span, fact, since, head_new):
                        failures.append(f"{path}:{span[2]}: {name} entry \"{' / '.join(fact.path)}\" -- {REMOVED_RULE}"
                                        f" (a note new in this change, in the pin or above it, naming it and holding a"
                                        f" YYYY-MM-DD)")
                continue
            for fact, why in grown(base, head):
                if not dated(head_lines, fact, since, head_new):
                    line = min(ln for ln, _ in fact.anchors)
                    failures.append(f"{path}:{line}: {name} entry \"{' / '.join(fact.path)}\" {why} -- {RULE} (a"
                                    f" note new in this change, holding a YYYY-MM-DD, on its line or in the comment"
                                    f" above it)")
    return failures


def git(repo, *args):
    try:
        return subprocess.run(["git", "-C", repo, *args], check=True, capture_output=True, text=True).stdout
    except subprocess.CalledProcessError as e:
        raise HistoryError(f"git {' '.join(args)}: {e.stderr.strip()}") from e


def reader(repo, rev):
    """A path's text at `rev`, or None when the path does not exist there; any other git failure is a HistoryError."""
    def read(path):
        if not git(repo, "ls-tree", "--name-only", rev, "--", path).strip():
            return None
        return git(repo, "show", f"{rev}:{path}")
    return read


def blamed_new(repo, merge_base, head, worktree):
    """path -> the head's lines (1-based) new in merge_base..head: those git blame attributes to a commit of the change
    (not a boundary commit), or, with the working tree read, not yet committed."""
    def fresh(path):
        # -w -M -C: a whitespace edit, or a line moved or copied, keeps its origin (review F, 2026-11-18, SHOULD 4)
        args = ["blame", "--porcelain", "-w", "-M", "-C"]
        if worktree:
            args += ["--contents", os.path.join(repo, path), f"{merge_base}..HEAD"]
        else:
            args += [f"{merge_base}..{head}"]
        out = git(repo, *args, "--", path)
        boundary, new = set(), set()
        sha = final = None
        for line in out.split("\n"):
            if line.startswith("\t"):
                if sha not in boundary:
                    new.add(final)
                continue
            parts = line.split(" ")
            if len(parts) >= 3 and re.fullmatch(r"[0-9a-f]{40}", parts[0]) and parts[1].isdigit() \
                    and parts[2].isdigit():
                sha, final = parts[0], int(parts[2])
            elif line == "boundary":
                boundary.add(sha)
        return new
    return fresh


def renames(repo, merge_base, head, worktree):
    """The head's path -> its merge-base path, for each file git's rename detection finds renamed in the change."""
    args = ["diff", "--name-status", "-M", merge_base] + ([] if worktree else [head])
    out = {}
    for line in git(repo, *args).split("\n"):
        parts = line.split("\t")
        if len(parts) == 3 and parts[0].startswith("R"):
            out[parts[2]] = parts[1]
    return out


def floor(repo, merge_base, head):
    """The earliest author date of the commits in merge_base..head (the day the change began); the merge-base's own
    date when there are none (then nothing can have moved)."""
    dates = git(repo, "log", "--format=%as", f"{merge_base}..{head}").split()
    return min(dates) if dates else git(repo, "show", "-s", "--format=%as", merge_base).strip()


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--base", default="origin/main")
    parser.add_argument("--head", default="HEAD", help="the revision read (in CI, the pull request's head commit)")
    parser.add_argument("--repo", default=None)
    parser.add_argument("--worktree", action="store_true",
                        help="read the head side from the working tree, not --head (a check before a commit)")
    args = parser.parse_args(argv)
    # the repository this script is in (under Bazel, the runfiles link resolves to the checkout's own file)
    repo = args.repo or os.path.dirname(os.path.realpath(__file__))
    try:
        repo = git(repo, "rev-parse", "--show-toplevel").strip()
        head = git(repo, "rev-parse", "--verify", args.head + "^{commit}").strip()
        merge_base = git(repo, "merge-base", head, args.base).strip()
        since = floor(repo, merge_base, head)

        def read_worktree(path):
            try:
                with open(os.path.join(repo, path), encoding="utf-8") as f:
                    return f.read()
            except FileNotFoundError:
                return None

        moved = renames(repo, merge_base, head, args.worktree)
        base_reader = reader(repo, merge_base)

        def read_base(path):
            # a file renamed in the change is compared with its old path's text (the review of 351e80daa2, SHOULD)
            return base_reader(moved.get(path, path))

        watched, grow_only = own_lists()
        failures = check(read_base, read_worktree if args.worktree else reader(repo, head), since, watched=watched,
                         grow_only=grow_only, fresh=blamed_new(repo, merge_base, head, args.worktree))
    except HistoryError as e:
        print(f"shrink_only: the history cannot be read ({e}): a full clone with {args.base} is needed",
              file=sys.stderr)
        return 2
    print(f"shrink_only: {'the working tree' if args.worktree else head[:10]} against the merge-base"
          f" {merge_base[:10]} ({args.base}); a note is a line new since the merge-base holding a date: "
          f"{sum(len(n) for n in watched.values())} pins in {len(watched)} files")
    for f in failures:
        print("  " + f)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
