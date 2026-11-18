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

THE FLOOR. A note is dated when it holds a YYYY-MM-DD on or after the floor: the earliest author date of the commits in
merge-base..head, the day the change began (an author date survives a rebase; the merge-base's own date does not stand
still: on a pull request's merge ref it is main's tip, so every note written before main's latest commit read as old).

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
- A note heads the rows under it: one recent group note dates every row below it up to the next comment, at any
  distance.
- A pin new at the head (absent at the merge-base) is new, not grown, and is not checked: its first contents are the
  change's own, reviewed with it. A pin dropped from WATCHED needs a dated note (above), so renaming a pin is visible.
- A note is text: the check proves a dated note was written, not that it is true.
- The head is a committed revision (--head, default HEAD) or, with --worktree, the working tree.

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
    if node.kind == "str":
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
            if not any(f.path[:k] in added for k in range(1, len(f.path))):
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


def dated(lines, fact, since):
    """Whether `fact` carries a date on or after `since`: on one of its lines or its entry call's, or in the nearest
    comment block above its first line, within the declaration it is read from (never the declaration's javadoc)."""
    own = {ln for ln, _ in fact.anchors}
    for first, last, _ in fact.spans:
        own |= set(range(first, last + 1))
    if any(recent(lines[n - 1], since) for n in own):
        return True
    firsts = {}
    for ln, top in list(fact.anchors) + [(first, top) for first, _, top in fact.spans]:
        firsts[top] = min(ln, firsts.get(top, ln))
    for top, first in firsts.items():
        k = first - 2                               # the line above, as an index
        while k >= top - 1 and not is_comment(lines[k]):
            k -= 1
        while k >= top - 1 and is_comment(lines[k]):
            if recent(lines[k], since):
                return True
            k -= 1
    return False


def comment_text(line):
    """A line's comment: the whole line when it is one, else the text after its '//' (or nothing)."""
    if is_comment(line):
        return line
    k = line.find("//")
    return line[k:] if k >= 0 else ""


def named(source, span, fact, since):
    """Whether a comment within the pin, or in the block directly above its declaration, names the removed `fact`
    (its text, or the simple name after the last '.', '$' or '#') with a date on or after `since`."""
    _, end, top = span
    lines = source.split("\n")
    leaf = fact.path[-1]
    names = {n for n in (leaf, re.split(r"[.$#]", leaf)[-1]) if n}
    texts = []
    k = top - 2
    while k >= 0 and is_comment(lines[k]):
        texts.append(lines[k])
        k -= 1
    texts += [comment_text(ln) for ln in lines[top - 1:source.count("\n", 0, end) + 1]]
    return any(recent(t, since) and any(n in t for n in names) for t in texts)


def watched_lists(script):
    """WATCHED and GROW_ONLY as this script's text at some revision declares them: {(file, name)} for each."""
    tree = ast.parse(script)
    out = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) \
                and node.targets[0].id in ("WATCHED", "GROW_ONLY"):
            value = ast.literal_eval(node.value)
            out[node.targets[0].id] = {(f, n) for f, names in value.items() for n in names}
    return out


def watched_shrinks(base_script, head_script, since):
    """A pin dropped from WATCHED or GROW_ONLY since the merge-base, with no dated comment in this script naming it."""
    if base_script is None:
        return []        # this script is new at the head: its lists are the change's own
    failures = []
    try:
        base, head = watched_lists(base_script), watched_lists(head_script)
    except (SyntaxError, ValueError) as e:
        return [f"{SELF}: its WATCHED or GROW_ONLY list cannot be read ({e})"]
    notes = [ln for ln in head_script.split("\n") if ln.strip().startswith("#") or " # " in ln]
    for listed in ("WATCHED", "GROW_ONLY"):
        for path, name in sorted(base.get(listed, set()) - head.get(listed, set())):
            if not any(recent(ln, since) and re.search(r"\b" + re.escape(name) + r"\b", ln) for ln in notes):
                failures.append(f"{SELF}: {name} ({path}) dropped from {listed} -- a pin leaves the list only with a"
                                f" dated note in this file naming it (a YYYY-MM-DD on or after {since})")
    return failures


def check(read_base, read_head, since, watched=None, grow_only=None, self_check=True):
    """Every move without a dated note, as messages naming the entry and the rule."""
    failures = []
    if self_check:
        head_script = read_head(SELF)
        if head_script is None:
            failures.append(f"{SELF}: this script is gone at the head")
        else:
            failures += watched_shrinks(read_base(SELF), head_script, since)
    grow_only = GROW_ONLY if grow_only is None else grow_only
    for path, names in (watched or WATCHED).items():
        head_source = read_head(path)
        if head_source is None:
            failures.append(f"{path}: a watched guard file is gone at the head: move its pins and this list with a"
                            f" dated note ({SELF})")
            continue
        base_source = read_base(path)
        head_lines = head_source.split("\n")
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
            if name in grow_only.get(path, ()):
                for fact in removed(base, head):
                    if not named(head_source, span, fact, since):
                        failures.append(f"{path}:{span[2]}: {name} entry \"{' / '.join(fact.path)}\" -- {REMOVED_RULE}"
                                        f" (a comment in the pin, or above it, naming it with a YYYY-MM-DD on or after"
                                        f" {since})")
                continue
            for fact, why in grown(base, head):
                if not dated(head_lines, fact, since):
                    line = min(ln for ln, _ in fact.anchors)
                    failures.append(f"{path}:{line}: {name} entry \"{' / '.join(fact.path)}\" {why} -- {RULE} (a"
                                    f" YYYY-MM-DD on or after {since} on its line or in the comment above it)")
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

        failures = check(reader(repo, merge_base), read_worktree if args.worktree else reader(repo, head), since)
    except HistoryError as e:
        print(f"shrink_only: the history cannot be read ({e}): a full clone with {args.base} is needed",
              file=sys.stderr)
        return 2
    print(f"shrink_only: {'the working tree' if args.worktree else head[:10]} against the merge-base"
          f" {merge_base[:10]} ({args.base}); notes dated {since} or later: "
          f"{sum(len(n) for n in WATCHED.values())} pins in {len(WATCHED)} files")
    for f in failures:
        print("  " + f)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
