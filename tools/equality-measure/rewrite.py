#!/usr/bin/env python3
"""Rewrite each assert of a Pure body to its plain comparison (the sheet's section 4 table), on the text.

The text is scanned with Pure's brackets and quotes respected; each `assertX(` call's arguments are split at top-level
commas and rewritten recursively (an assert inside a forAll's lambda too). A body holding an assert that is not a plain
comparison (JSON, TDS, error asserts, assertIs, instance-of) is left out, its status naming the first such assert.
"""
import re

PLAIN = {"assert", "assertFalse", "assertEquals", "assertNotEquals", "assertSameElements", "assertEq",
         "assertNotEq", "assertContains", "assertNotContains", "assertSize", "assertNotSize", "assertEmpty",
         "assertNotEmpty", "assertEqWithinTolerance"}
NAME_RE = re.compile(r"(?:meta::pure::functions::asserts::)?(assert\w*|fail)\s*\(")


class NotPlain(Exception):
    pass


def skip_opaque(s, i):
    """Past a quoted string or an island (#TDS ... #, #{ ... }#, #>{ ... }#) starting at s[i]; else i."""
    if s[i] == "'":
        j = i + 1
        while j < len(s) and s[j] != "'":
            j += 2 if s[j] == "\\" else 1
        return j + 1
    if s[i] == "#" and i + 1 < len(s) and (s[i + 1].isalpha() or s[i + 1] in "{>"):
        j = s.find("#", i + 1)
        return len(s) if j < 0 else j + 1
    return i


def split_args(s):
    """Top-level comma split of an argument list's text."""
    args, depth, cur, i = [], 0, [], 0
    while i < len(s):
        c = s[i]
        j = skip_opaque(s, i)
        if j != i:
            cur.append(s[i:j])
            i = j
            continue
        if c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
        if c == "," and depth == 0:
            args.append("".join(cur).strip())
            cur = []
        else:
            cur.append(c)
        i += 1
    if "".join(cur).strip():
        args.append("".join(cur).strip())
    return args


def close_of(s, i):
    depth, j = 0, i
    while j < len(s):
        c = s[j]
        k = skip_opaque(s, j)
        if k != j:
            j = k
            continue
        if c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
            if depth == 0:
                return j
        j += 1
    raise NotPlain("fragment-joined-to-code")


def top_level_ops(e):
    """Whether e holds an operator outside brackets (so it needs parentheses as an operand)."""
    depth, i = 0, 0
    t = e.replace("->", "  ")
    while i < len(t):
        c = t[i]
        j = skip_opaque(t, i)
        if j != i:
            i = j
            continue
        if c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
        elif depth == 0 and c in "+-*/<>=!&|" and not (c == "-" and i == 0):
            return True
        i += 1
    return False


def p(e):
    if e.startswith("#") and e.endswith("#"):
        return e  # an island (#TDS ... #): never parenthesized, its text is not an expression's
    return "(" + e + ")" if top_level_ops(e) or e.startswith("-") else e


def message(extra):
    if not extra:
        return ""
    if len(extra) == 1:
        return ", " + extra[0]
    return ", | format(" + extra[0] + ", " + ", ".join(extra[1:]) + ")"


def plain(name, a):
    def need(n):
        if len(a) < n:
            raise NotPlain(name + "/arity")
    if name == "assert":
        return "assert(" + ", ".join(a) + ")"
    if name == "assertFalse":
        need(1)
        return "assert(!" + p(a[0]) + message(a[1:]) + ")" if not top_level_ops(a[0]) else \
            "assert(!(" + a[0] + ")" + message(a[1:]) + ")"
    if name in ("assertEquals", "assertNotEquals"):
        need(2)
        cmp = p(a[0]) + " == " + p(a[1])
        return "assert(" + (cmp if name == "assertEquals" else "!(" + cmp + ")") + message(a[2:]) + ")"
    if name == "assertSameElements":
        need(2)
        # the any-order comparison the product has since step 6 (hasSameElements, register S430): the assert and it
        # are one comparison, so the pair is measured as assertEquals and == are (until the closing batch,
        # 2026-11-27, this wrote a sort of each side and ==, from before the function existed)
        return "assert(hasSameElements(" + a[0] + ", " + a[1] + ")" + message(a[2:]) + ")"
    if name in ("assertEq", "assertNotEq"):
        need(2)
        return "assert(" + ("" if name == "assertEq" else "!") + "eq(" + a[0] + ", " + a[1] + ")" + message(a[2:]) + ")"
    if name in ("assertContains", "assertNotContains"):
        need(2)
        c = p(a[0]) + "->contains(" + a[1] + ")"
        return "assert(" + (c if name == "assertContains" else "!(" + c + ")") + message(a[2:]) + ")"
    if name in ("assertSize", "assertNotSize"):
        need(2)
        c = p(a[0]) + "->size() == " + p(a[1])
        return "assert(" + (c if name == "assertSize" else "!(" + c + ")") + message(a[2:]) + ")"
    if name in ("assertEmpty", "assertNotEmpty"):
        need(1)
        return "assert(" + p(a[0]) + ("->isEmpty()" if name == "assertEmpty" else "->isNotEmpty()") + message(a[1:]) + ")"
    if name == "assertEqWithinTolerance":
        need(3)
        return "assert(abs(" + p(a[0]) + " - " + p(a[1]) + ") <= abs(" + a[2] + ")" + message(a[3:]) + ")"
    raise NotPlain(name)


def rw(s):
    out, i = [], 0
    while True:
        m = NAME_RE.search(s, i)
        if not m:
            out.append(s[i:])
            return "".join(out)
        start = m.start()
        if start > 0 and (s[start - 1].isalnum() or s[start - 1] in "_:"):
            out.append(s[i:m.end()])
            i = m.end()
            continue
        name = m.group(1)
        before = s[:start].rstrip()
        if before.endswith("->"):
            raise NotPlain("arrow-form " + name)
        if name not in PLAIN:
            raise NotPlain(name)
        open_i = m.end() - 1
        close_i = close_of(s, open_i)
        args = [rw(x) for x in split_args(s[open_i + 1:close_i])]
        out.append(s[i:start])
        out.append(plain(name, args))
        i = close_i + 1


def rewrite_body(body):
    """(status, rewritten): status 'rewritten', 'unchanged' (asserts already plain) or 'excluded:<why>'."""
    try:
        r = rw(body)
    except NotPlain as e:
        return "excluded:" + str(e), None
    if not NAME_RE.search(body):
        return "excluded:no-assert-call", None
    return ("unchanged" if r == body else "rewritten"), r


if __name__ == "__main__":
    import sys
    for line in sys.stdin:
        print(rewrite_body(line.strip()))
