#!/usr/bin/env python3
"""Move Java classes between packages — the untangle's one mechanism.

Every move group in docs/standard-build-audit-2026-09-22/UNTANGLE_HOMEWORK.md is
applied with this, never by hand, so that re-basing the untangle onto a newer
`main` means re-running the groups rather than resolving conflicts in hundreds of
touched files. The script is the source of truth; the resulting diff is output.

What a move has to do, each item learned the hard way on group A:

  1. git mv the file and rewrite its `package` line.
  2. Rewrite every fully-qualified reference `old.pkg.Cls` -> `new.pkg.Cls`
     across .java, pom.xml, BUILD.bazel and .bzl files. Build configuration
     carries FQNs too: core/pom.xml names `com.legend.Nullable` in the NullAway
     flags, and missing it leaves the null gate silently matching nothing.
  3. Files that stay in the OLD package and used the class by simple name
     (legal while they were package-mates) now need access. A cross-package
     graph cannot see these by construction; javac can.
  4. The MOVED class may use siblings from its old package by simple name; it
     now needs imports for them.
  5. Package-private access across the new boundary cannot be fixed
     mechanically — widening visibility is a design change. The script reports
     candidates; the compile is the authority.

Access for (3) is by import by default. `--inline` instead qualifies usages in
place, which keeps every line count unchanged — the house style for the null
annotations, which the codebase already writes as `@com.legend.Nullable` at
2,826 sites, and the choice that moves no source-size pin.

Usage:
    move_classes.py com.legend.Nullable com.legend.NonNull --to com.legend.base --inline
    move_classes.py --group I            # from groups.txt beside this file
    move_classes.py ... --dry-run
"""
import argparse, os, re, subprocess, sys

ROOTS = ["core", "spec", "pct", "parser-equivalence"]
SRC_DIRS = ["src/main/java", "src/test/java"]
TEXT_EXT = (".java",)
CONFIG_NAMES = ("pom.xml", "BUILD.bazel", "BUILD")
CONFIG_EXT = (".bzl",)


def blank(m):
    return re.sub(r"[^\n]", " ", m.group(0))


def code_only(s):
    """Comments and string literals blanked, line structure preserved."""
    s = re.sub(r"/\*.*?\*/", blank, s, flags=re.S)
    s = re.sub(r"//[^\n]*", blank, s)
    return re.sub(r'"(?:\\.|[^"\\\n])*"', blank, s)


def java_files(repo):
    for r in ROOTS:
        for d in SRC_DIRS:
            base = os.path.join(repo, r, d)
            for dp, _, fns in os.walk(base):
                for fn in fns:
                    if fn.endswith(".java"):
                        yield os.path.join(dp, fn)


def config_files(repo):
    for dp, dns, fns in os.walk(repo):
        dns[:] = [x for x in dns if x not in (".git", "target", "node_modules")
                  and not x.startswith("bazel-")]
        for fn in fns:
            if fn in CONFIG_NAMES or fn.endswith(CONFIG_EXT):
                yield os.path.join(dp, fn)


def pkg_of_file(path, text):
    m = re.search(r"^\s*package\s+([\w.]+)\s*;", text, re.M)
    return m.group(1) if m else None


def add_imports(text, fqns):
    """Insert `import X;` lines, keeping the file compiling; no-op if present."""
    need = [f for f in sorted(fqns) if not re.search(r"^\s*import\s+" + re.escape(f) + r"\s*;", text, re.M)]
    if not need:
        return text
    block = "".join(f"import {f};\n" for f in need)
    m = re.search(r"^\s*import\s", text, re.M)
    if m:
        return text[:m.start()] + block + text[m.start():]
    m = re.search(r"^\s*package\s+[\w.]+\s*;\s*\n", text, re.M)
    return text[:m.end()] + "\n" + block + text[m.end():]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("classes", nargs="*", help="fully-qualified classes to move")
    ap.add_argument("--to", help="destination package")
    ap.add_argument("--group", help="apply a named group from groups.txt")
    ap.add_argument("--inline", action="store_true",
                    help="qualify same-package @Annotation uses in place instead of importing")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--repo", default=".")
    a = ap.parse_args()
    repo = os.path.abspath(a.repo)

    if a.group:
        here = os.path.dirname(os.path.abspath(__file__))
        found = False
        for ln in open(os.path.join(here, "groups.txt"), encoding="utf-8"):
            ln = ln.split("#", 1)[0].strip()
            if not ln:
                continue
            name, rest = ln.split(":", 1)
            if name.strip() != a.group:
                continue
            dest, clss = rest.split("<-", 1)
            a.to = dest.strip()
            flags = [c for c in clss.split() if c.startswith("--")]
            a.classes = [c for c in clss.split() if not c.startswith("--")]
            a.inline = a.inline or "--inline" in flags
            found = True
        if not found:
            sys.exit(f"no group {a.group} in groups.txt")
    if not a.classes or not a.to:
        sys.exit("need classes and --to (or --group)")

    # locate each class's file
    moves = {}
    all_java = list(java_files(repo))
    for fq in a.classes:
        pkg, cls = fq.rsplit(".", 1)
        rel = fq.replace(".", "/") + ".java"
        hits = [p for p in all_java if p.endswith("/" + rel)]
        if len(hits) != 1:
            sys.exit(f"{fq}: expected one source file, found {hits}")
        moves[fq] = (pkg, cls, hits[0])
    moved_simple = {cls for _, cls, _ in moves.values()}

    # package -> simple class names in it (main+test), to resolve sibling use
    pkg_classes = {}
    for p in all_java:
        t = open(p, encoding="utf-8", errors="replace").read()
        pk = pkg_of_file(p, t)
        if pk:
            pkg_classes.setdefault(pk, set()).add(os.path.basename(p)[:-5])

    edits = {}  # path -> new text

    def get(p):
        return edits.get(p) or open(p, encoding="utf-8", errors="replace").read()

    # (2) FQN rewrite everywhere
    fq_pat = {fq: re.compile(r"\b" + re.escape(fq) + r"\b") for fq in moves}
    for p in list(all_java) + list(config_files(repo)):
        t = get(p)
        n = t
        for fq, pat in fq_pat.items():
            n = pat.sub(a.to + "." + moves[fq][1], n)
        if n != t:
            edits[p] = n

    report = {"fqn_files": len(edits), "same_pkg_users": 0, "moved_sibling_imports": 0}

    # (3) same-package users left behind
    for fq, (pkg, cls, path) in moves.items():
        simple = re.compile(r"(?<![\w.])" + re.escape(cls) + r"\b")
        at_simple = re.compile(r"@" + re.escape(cls) + r"\b")
        for p in all_java:
            if p in [m[2] for m in moves.values()]:
                continue
            t = get(p)
            if pkg_of_file(p, t) != pkg:
                continue
            code = code_only(t)
            if not simple.search(code):
                continue
            report["same_pkg_users"] += 1
            if a.inline and at_simple.search(code):
                # rewrite only annotation uses, outside comments/strings
                out, last = [], 0
                for m in at_simple.finditer(code):
                    out.append(t[last:m.start()] + "@" + a.to + "." + cls)
                    last = m.end()
                out.append(t[last:])
                edits[p] = "".join(out)
                if simple.search(code_only(edits[p]).replace("@" + a.to + "." + cls, "")):
                    edits[p] = add_imports(edits[p], {a.to + "." + cls})
            else:
                edits[p] = add_imports(t, {a.to + "." + cls})

    # (1)+(4) the moved files themselves
    for fq, (pkg, cls, path) in moves.items():
        t = get(path)
        t = re.sub(r"^(\s*package\s+)[\w.]+(\s*;)", r"\g<1>" + a.to + r"\g<2>", t, count=1, flags=re.M)
        code = code_only(t)
        siblings = set()
        for sib in pkg_classes.get(pkg, ()):
            if sib in moved_simple or sib == cls:
                continue
            if re.search(r"(?<![\w.])" + re.escape(sib) + r"\b", code):
                siblings.add(pkg + "." + sib)
        if siblings:
            t = add_imports(t, siblings)
            report["moved_sibling_imports"] += len(siblings)
        edits[path] = t

    print(f"move {len(moves)} class(es) -> {a.to}")
    print(f"  files with FQN rewrites : {report['fqn_files']}")
    print(f"  same-package users fixed: {report['same_pkg_users']}")
    print(f"  sibling imports in moved: {report['moved_sibling_imports']}")
    print(f"  total files changed     : {len(edits)}")
    print("  NOTE: package-private access across the new boundary is not fixable")
    print("        mechanically — the compile is the authority (see docstring, item 5).")
    if a.dry_run:
        return

    for p, t in edits.items():
        open(p, "w", encoding="utf-8").write(t)
    for fq, (pkg, cls, path) in moves.items():
        dest_dir = os.path.join(path.split("/src/")[0], "src",
                                path.split("/src/")[1].split("/")[0], "java",
                                *a.to.split("."))
        os.makedirs(dest_dir, exist_ok=True)
        dest = os.path.join(dest_dir, cls + ".java")
        subprocess.run(["git", "-C", repo, "mv", path, dest], check=True)


if __name__ == "__main__":
    main()
