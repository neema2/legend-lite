#!/usr/bin/env python3
"""Census: tests whose asserts sit inside a helper function rather than in the test body itself.

helpers.py <out dir> <legend-pure src> <legend-engine src>: reads <out dir>/bodies.tsv (the core bodies and their
models) and the PCT sources; writes <out dir>/helpers.txt."""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import collect  # noqa: E402

FUNC_RE = re.compile(r"function\s+(<<[^>]*>>\s*)?(\{[^}]*\}\s*)?([\w:]+)\s*(<[^>]*>)?\s*\(", re.S)
ASSERT_RE = re.compile(r"(?<![\w:])(?:meta::pure::functions::asserts::)?assert\w*\s*\(")


def functions(src):
    """(name, is_pct_test, body text) of every function of a Pure source."""
    out = []
    for m in FUNC_RE.finditer(src):
        close = collect.balanced(src, m.end() - 1, "(", ")")
        brace = src.find("{", close)
        if brace < 0:
            continue
        out.append((m.group(3), bool(m.group(1) and "PCT.test" in m.group(1)), src[brace + 1:collect.balanced(src, brace) - 1],
                    (m.group(1) or "").lower()))
    return out


CALL_RE = re.compile(r"([A-Za-z_][\w:]*)\s*\(")


def calls(body, names):
    """The names among {@code names} (full paths) that body calls, matched by their simple names."""
    by_simple = {}
    for n in names:
        by_simple.setdefault(n.split("::")[-1], []).append(n)
    return {n for c in CALL_RE.findall(body) for n in by_simple.get(c.split("::")[-1], [])}


def helper_name(name):
    """A helper a test calls: a function of a test package, never an assert native's own definition."""
    simple = name.split("::")[-1]
    return "test" in name.rsplit("::", 1)[0].lower() and not simple.startswith("assert") \
        and not name.startswith("meta::pure::functions::asserts::")


def pct(roots):
    helpers, tests = {}, []
    for root in roots:
        for d, _, fs in os.walk(root):
            for f in fs:
                if f.endswith(".pure"):
                    src = collect.strip_comments(open(os.path.join(d, f), encoding="utf-8", errors="replace").read())
                    imports = collect.IMPORT_RE.findall(src)
                    for name, is_test, body, stereo in functions(src):
                        if is_test:
                            tests.append((name, body, imports))
                        elif "test" not in stereo and ASSERT_RE.search(body) and helper_name(name):
                            helpers[name] = body
    by_simple = {}
    for n in helpers:
        by_simple.setdefault(n.split("::")[-1], []).append(n)
    via, only = 0, 0
    examples = []
    for name, body, imports in tests:
        scope = set(imports) | {name.rsplit("::", 1)[0]}
        hit = set()
        for c in CALL_RE.findall(body):
            for h in by_simple.get(c.split("::")[-1], []):
                if c == h or ("::" not in c and h.rsplit("::", 1)[0] in scope):
                    hit.add(h)
        if hit:
            via += 1
            if not ASSERT_RE.search(body):
                only += 1
            if len(examples) < 3:
                examples.append(name.split("::")[-1] + " -> " + sorted(hit)[0].split("::")[-1])
    none = sum(1 for _, b, _ in tests if not ASSERT_RE.search(b))
    return len(tests), len(helpers), via, only, examples + [f"{none} PCT tests hold no assert call at all"]


def core(out):
    rows = [l.rstrip("\n").split("\t") for l in open(os.path.join(out, "bodies.tsv"), encoding="utf-8")][1:]
    n = via = 0
    examples = []
    for r in rows:
        if r[0].split(":")[0] in ("pure", "engine"):
            continue
        n += 1
        model = r[4].replace("\\n", "\n")
        helpers = {name for name, _, b, _ in functions(model) if ASSERT_RE.search(b)
                   and not name.startswith("meta::pure::functions::asserts::")}
        hit = calls(r[5].replace("\\n", "\n"), helpers)
        if hit:
            via += 1
            if len(examples) < 3:
                examples.append(r[0] + " -> " + sorted(hit)[0])
    return n, via, examples


def main():
    out = sys.argv[1]
    t, h, via, only, ex = pct(sys.argv[2:4])
    n, cvia, cex = core(out)
    text = (f"asserts inside a helper function:\n"
            f"- core bodies: {cvia} of {n} call a model function that holds an assert (e.g. {', '.join(cex)})\n"
            f"- PCT tests: {via} of {t} call one of {h} helper functions that hold asserts; {only} of them hold no assert"
            f" of their own (e.g. {', '.join(ex)})\n")
    open(os.path.join(out, "helpers.txt"), "w", encoding="utf-8").write(text)
    print(text)


if __name__ == "__main__":
    main()
