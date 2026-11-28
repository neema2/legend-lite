#!/usr/bin/env python3
"""Collect assert-holding test bodies and write bodies.tsv (id, source, model, body_as_written, body_rewritten, status).

Sources:
  * core's Java tests that run a Pure body through the test door: every string literal (adjacent literals joined
    by `+`) that is a lambda body (`{|...}` or `|...`) holding an assert; the model is the file's text block that
    declares every element the body names (else a storeless empty model);
  * the <<PCT.test>> functions of the pinned legend-pure and legend-engine sources, each `$f->eval(|e)` replaced by
    `e`, the file's Class/Enum declarations as its model.

Usage: collect.py <repo root> <out dir> [<legend-pure src> <legend-engine src>]
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import rewrite  # noqa: E402

JAVA_FILES_RE = re.compile(r"Program\.(run|runFresh|last|plan)\b|EdgeProgram|runProgram")


def java_strings(src):
    """Every string literal of a Java source as (start offset, text), adjacent `+`-joined literals merged."""
    out = []  # (start, end, text)
    i, n = 0, len(src)
    while i < n:
        c = src[i]
        if src.startswith("//", i):
            j = src.find("\n", i)
            i = n if j < 0 else j
            continue
        if src.startswith("/*", i):
            j = src.find("*/", i + 2)
            i = n if j < 0 else j + 2
            continue
        if c == "'":
            j = i + 1
            while j < n and src[j] != "'":
                j += 2 if src[j] == "\\" else 1
            i = j + 1
            continue
        if src.startswith('"""', i):
            j = src.find('"""', i + 3)
            while j > 0 and src[j - 1] == "\\":
                j = src.find('"""', j + 1)
            raw = src[src.find("\n", i) + 1:j]
            out.append((i, j + 3, text_block(raw)))
            i = j + 3
            continue
        if c == '"':
            j = i + 1
            buf = []
            while j < n and src[j] != '"':
                if src[j] == "\\":
                    buf.append(unescape(src[j:j + 2]))
                    j += 2
                else:
                    buf.append(src[j])
                    j += 1
            out.append((i, j + 1, "".join(buf)))
            i = j + 1
            continue
        i += 1
    merged = []
    for s, e, t in out:
        if merged and re.fullmatch(r"\s*\+\s*", src[merged[-1][1]:s]):
            ps, _, pt = merged[-1]
            merged[-1] = (ps, e, pt + t)
        else:
            merged.append((s, e, t))
    return merged


def unescape(esc):
    return {"\\n": "\n", "\\t": "\t", '\\"': '"', "\\'": "'", "\\\\": "\\"}.get(esc, esc[1:])


def text_block(raw):
    lines = raw.split("\n")
    closing_indent = len(lines[-1]) - len(lines[-1].lstrip()) if lines[-1].strip() == "" else None
    content = [l for l in lines if l.strip()]
    indent = min([len(l) - len(l.lstrip()) for l in content] + ([closing_indent] if closing_indent is not None else []))
    text = "\n".join(l[indent:] for l in lines)
    text = re.sub(r"\\\n", "", text)
    return re.sub(r"\\(.)", lambda m: unescape("\\" + m.group(1)), text)


DECL_RE = re.compile(r"\b(?:Class|Enum|function|Association|Profile|Database|Mapping|Runtime)\s+(?:<<[^>]*>>\s*)?"
                     r"(?:\{[^}]*\}\s*)?([A-Za-z_][\w]*(?:::[\w]+)+)")
REF_RE = re.compile(r"(?<![\w:])([A-Za-z_]\w*(?:::\w+)+)")


def is_model(t):
    return "###" in t or re.search(r"^\s*(Class|Enum|function) ", t, re.M) is not None


def choose_model(body, models):
    refs = {r for r in REF_RE.findall(body)}
    # an enum value or property after the element (t::E.A) is fine: REF_RE stops at the dot
    for m in models:
        declared = set(DECL_RE.findall(m))
        if refs and refs <= declared:
            return m
    if not refs:
        return ""
    return None


def from_java(root):
    bodies = []
    base = os.path.join(root, "core/src/test/java")
    for d, _, fs in os.walk(base):
        for f in sorted(fs):
            if not f.endswith(".java"):
                continue
            p = os.path.join(d, f)
            src = open(p, encoding="utf-8").read()
            if not JAVA_FILES_RE.search(src):
                continue
            strings = java_strings(src)
            models = [t for _, _, t in strings if is_model(t) and "{|" not in t[:3]]
            seen = set()
            for k, (s, _, t) in enumerate(strings):
                b = t.strip()
                if not (b.startswith("{|") or b.startswith("|")) or "assert" not in b:
                    continue
                if re.search(r"%[sd]", b) or b in seen:
                    continue
                seen.add(b)
                line = src.count("\n", 0, s) + 1
                rel = os.path.relpath(p, base)
                bodies.append((f"{f[:-5]}:{line}", rel, choose_model(b, models), b, []))
    return bodies


def balanced(src, i, open_c="{", close_c="}"):
    """The index just past the bracket closing the one at src[i] (strings in quotes skipped)."""
    depth, j = 0, i
    while j < len(src):
        c = src[j]
        if c == "'":
            j += 1
            while j < len(src) and src[j] != "'":
                j += 2 if src[j] == "\\" else 1
        elif src.startswith("//", j):
            j = src.find("\n", j)
            if j < 0:
                return len(src)
        elif src.startswith("/*", j):
            j = src.find("*/", j) + 1
        elif c == open_c:
            depth += 1
        elif c == close_c:
            depth -= 1
            if depth == 0:
                return j + 1
        j += 1
    return len(src)


PCT_RE = re.compile(r"function\s+<<[^>]*PCT\.test[^>]*>>\s*(?:\{[^}]*\}\s*)?([\w:]+)\s*<[^>]*>\s*\(", re.S)
ELEM_RE = re.compile(r"^(Class|Enum|Profile|Association|Primitive)\b[^\n{]*", re.M)


def pct_models(src):
    parts = []
    for m in ELEM_RE.finditer(src):
        start = m.start()
        brace = src.find("{", m.end())
        if brace < 0:
            continue
        # a Class can carry a constraint block [..] or stereotype before its body
        parts.append(src[start:balanced(src, brace)])
    return ("###Pure\n" + "\n".join(parts)) if parts else ""


def strip_comments(src):
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return re.sub(r"(?m)^\s*//.*$", "", src)


def replace_eval(text):
    """Each `$f->eval(|e)` (and `$f->eval(x, |e)` forms) replaced by `e`."""
    out, i = [], 0
    while True:
        k = text.find("$f->eval(", i)
        if k < 0:
            out.append(text[i:])
            return "".join(out)
        out.append(text[i:k])
        open_i = k + len("$f->eval")
        end = balanced(text, open_i, "(", ")")
        inner = text[open_i + 1:end - 1].strip()
        if inner.startswith("|"):
            inner = inner[1:].strip()
        elif re.fullmatch(r"\$\w+", inner):
            inner = inner + "->eval()"  # a lambda held by a let: evaluated in place
        else:
            return None  # a lambda with parameters: not this shape
        out.append("(" + inner + ")" if needs_parens(inner) else inner)
        i = end


def needs_parens(e):
    return re.search(r"[+\-*/<>=!&|]", e.replace("->", "")) is not None and not (e.startswith("(") and e.endswith(")"))


IMPORT_RE = re.compile(r"^\s*import\s+([\w:]+)::\*\s*;", re.M)
NAMED_ELEM_RE = re.compile(r"^(?:Class|Enum|Profile|Association)\s+(?:<<[^>]*>>\s*)?(?:\{[^}]*\}\s*)?([\w:]+)", re.M)


def index_test_elements(src_root):
    """package -> [(file, its imports, its test-model declarations)] over every .pure file: declarations whose path
    names a test package only (never the platform's own metamodel)."""
    index = {}
    for d, _, fs in os.walk(src_root):
        for f in fs:
            if not f.endswith(".pure"):
                continue
            p = os.path.join(d, f)
            src = strip_comments(open(p, encoding="utf-8", errors="replace").read())
            decls = []
            for m in ELEM_RE.finditer(src):
                nm = NAMED_ELEM_RE.match(src, m.start())
                if not nm or "test" not in nm.group(1).lower():
                    continue
                brace = src.find("{", m.end())
                if brace >= 0:
                    decls.append(src[m.start():balanced(src, brace)])
            if decls:
                pkgs = {NAMED_ELEM_RE.match(x).group(1).rsplit("::", 1)[0] for x in decls if NAMED_ELEM_RE.match(x)}
                for pkg in pkgs:
                    index.setdefault(pkg, []).append((p, IMPORT_RE.findall(src), decls))
    return index


def section(imports, decls):
    return "###Pure\n" + "".join("import " + i + "::*;\n" for i in imports) + "\n".join(decls)


def pct_model(path, src, index):
    """The test file's own declarations and the test-model declarations of the packages it imports (and theirs)."""
    own_imports = IMPORT_RE.findall(src)
    files, todo, seen = {}, list(own_imports), set()
    while todo:
        pkg = todo.pop()
        if pkg in seen:
            continue
        seen.add(pkg)
        for p, imps, decls in index.get(pkg, []):
            if p not in files:
                files[p] = (imps, decls)
                todo.extend(imps)
    own = [src[m.start():balanced(src, src.find("{", m.end()))] for m in ELEM_RE.finditer(src)
           if NAMED_ELEM_RE.match(src, m.start()) and src.find("{", m.end()) >= 0]
    files.pop(path, None)
    parts = ([section(own_imports, own)] if own else []) + [section(i, d) for i, d in files.values()]
    return "\n".join(parts), own_imports


def from_pct(src_root, tag):
    bodies = []
    index = index_test_elements(src_root)
    for d, _, fs in os.walk(src_root):
        for f in sorted(fs):
            if not f.endswith(".pure"):
                continue
            p = os.path.join(d, f)
            src = strip_comments(open(p, encoding="utf-8", errors="replace").read())
            if "PCT.test" not in src:
                continue
            model, imports = pct_model(p, src, index)
            for m in PCT_RE.finditer(src):
                brace = src.find("{", balanced(src, m.end() - 1, "(", ")"))
                end = balanced(src, brace)
                text = src[brace + 1:end - 1].strip()
                if "assert" not in text:
                    continue
                text = replace_eval(text)
                if text is None or re.search(r"\$f\s*->\s*eval\b", text):
                    bodies.append((f"{tag}:{m.group(1)}", os.path.relpath(p, src_root), model, None, imports))
                    continue
                bodies.append((f"{tag}:{m.group(1)}", os.path.relpath(p, src_root), model, "{|" + text + "}", imports))
    return bodies


def esc(s):
    return s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")


def main():
    root, out = sys.argv[1], sys.argv[2]
    bodies = from_java(root)
    if len(sys.argv) > 4:
        bodies += from_pct(sys.argv[3], "pure") + from_pct(sys.argv[4], "engine")
    os.makedirs(out, exist_ok=True)
    counts = {}
    with open(os.path.join(out, "bodies.tsv"), "w", encoding="utf-8") as w:
        w.write("id\tsource\tstatus\timports\tmodel\tas_written\trewritten\n")
        for bid, srcf, model, body, imports in bodies:
            if body is None:
                status, rew = "excluded:eval-with-parameters", ""
            elif model is None:
                status, rew = "excluded:no-model-declares-its-names", ""
            else:
                status, rew = rewrite.rewrite_body(body)
            counts[status] = counts.get(status, 0) + 1
            w.write("\t".join([bid, srcf, status, ",".join(imports), esc(model or ""), esc(body or ""), esc(rew or "")]) + "\n")
    print(len(bodies), "bodies")
    for k, v in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"  {v:5d} {k}")


if __name__ == "__main__":
    main()
