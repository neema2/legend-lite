"""
Measures FEATURE DENSITY of the stress corpus, per the mapping/store taxonomy.

The corpus grew broad before it grew deep. It reached 210 classes, 397 class mappings and
182 executing tests while using almost none of what makes a Legend mapping interesting: half
the class mappings are `~mainTable` plus a list of direct columns, and the model has two
databases, one view, one filter and no schemas at all.

That is not a small omission. Every one of the twenty-four findings so far came from
NAVIGATION and QUERY SHAPE, because that is the only dimension the corpus had. Mapping and
store mechanics -- join chains, dynafunctions, scopes, embedded trees, unions, set-id
inheritance, non-equality joins -- have never been exercised enough to find anything, and
absence of evidence there is purely absence of testing.

So this counts them, and the count is a RATCHET: a feature that reaches 1 must not fall back
to 0, and the plain-1:1 percentage must not rise. Without that, the next generator that adds
five hundred flat mappings would improve every headline number while making the corpus
shallower.

The taxonomy is not invented here. It follows docs/MAPPING_FEATURE_CHECKLIST.md (A: property
mapping types, B: class-mapping directives, C: mapping-level, D: database objects, E: class
mapping types) and the rosetta catalogue beside it, both of which predate this corpus and
neither of which it had been checked against.
"""
from __future__ import annotations

import glob
import re
import sys

STRESS = "core/src/test/resources/stress/*.pure"

# A class mapping's body, so a feature is credited to the mapping that uses it rather than
# to the file it appears in. `Otherwise` in a comment at the top of a file is not a mapping
# that uses Otherwise.
# The header must admit `extends [id]` between the set id and the colon. Without it, a
# mapping using inheritance did not match this pattern AT ALL -- so it was not counted as a
# class mapping, and every feature inside it was invisible too. Adding three extending
# mappings changed nothing in the report, which is what made it noticeable.
#
# Fourth regex in this file to understate the corpus. They share a shape: each was written
# against the constructs that existed WHEN it was written, so each new construct is exactly
# the thing it cannot see.
_BLOCK = re.compile(
    r"^\s*\*?[\w:]+(?:\[\w+\])?(?:\s+extends\s*\[\w+\])?\s*:\s*"
    r"(Relational|Pure|Operation|XStore|AggregationAware|Relation)\s*\{(.*?)\n\s*\}",
    re.S | re.M)

# `extends [id]` lives in the mapping HEADER, not the body, so it needs its own count
# rather than a body pattern.
_EXTENDS = re.compile(r"^\s*\*?[\w:]+(?:\[\w+\])?\s+extends\s*\[\w+\]\s*:", re.M)

# (label, pattern, scope) -- 'body' counts class mappings using it, 'src' counts occurrences
# anywhere in the corpus (for store objects, which live outside class mappings).
FEATURES = [
    ("A2  join chain",            r"@\w+\s*>\s*@\w+", "body"),
    ("A3  dynafunction",          r":\s*(concat|upper|lower|substring|trim|coalesce|plus|"
                                  r"minus|times|divide|if|case|length|toUpper|toLower)\s*\(", "body"),
    # `toUpper`, not `upper`. This pattern said `upper` -- the same non-existent name the
    # superseded doc uses and the verification corrected -- so a dyna-over-join written
    # CORRECTLY scored zero. Second time this file has encoded a spelling that does not
    # exist; the meter keeps inheriting the errors it was built to detect.
    ("A4  dyna over join",
     r"(concat|toUpper|toLower|substring|trim|coalesce)\s*\(.{0,120}?@\w+", "body"),
    ("A5  enum transformer",      r"EnumerationMapping\s+[\w:]+\s*:", "body"),
    # `prop ( sub: [db]T.col )` -- a bare property name, parens, property mappings inside.
    #
    # This pattern was `\w+\s*\(\s*\)\s*\{`, i.e. `prop() {`, which is the spelling the
    # SUPERSEDED docs/MAPPING_FEATURE_CHECKLIST.md shows and which does not exist: empty
    # parens belong to Inline. The meter was built from the same wrong source as the brief,
    # so it would have scored an INVALID mapping as present and scores the valid one as
    # absent. A measurement inheriting the error it is meant to detect.
    #
    # Negative lookahead on scope/AssociationMapping, which share the shape.
    ("A6  embedded",
     r"^\s*(?!scope\b|AssociationMapping\b)[a-z]\w*\s*\(\s*\n\s*\w+\s*:\s*\[", "body"),
    ("A7  Otherwise",             r"Otherwise\s*\(", "body"),
    ("A8  Inline",                r"Inline\s*\[", "body"),
    ("A9  Binding transformer",   r":\s*Binding\s+[\w:]+\s*:", "body"),
    ("A10 src/tgt ids",           r"\w+\[\s*\w+\s*,\s*\w+\s*\]\s*:", "body"),
    ("B2  ~filter",               r"~filter", "body"),
    ("B3  ~filter via join",      r"~filter\s*\[[\w:]+\]\s*@", "body"),
    ("B4  ~distinct",             r"~distinct", "body"),
    ("B5  ~groupBy",              r"~groupBy", "body"),
    ("B6  ~primaryKey",           r"~primaryKey", "body"),
    ("B9  extends [id]",          r"^\s*\*?[\w:]+(?:\[\w+\])?\s+extends\s*\[\w+\]\s*:", "src"),
    ("B10 scope block",           r"scope\s*\(", "body"),
    ("C5  local property +",      r"^\s*\+\w+\s*:", "body"),
    ("E2  Pure/M2M ~src",         r"~src\s", "body"),
    ("E5  Relation ~func",        r"~func\s", "body"),

    ("C2  store substitution",    r"include\s+[\w:]+\s*\[[\w:]+\s*->", "src"),
    ("D3  multi-column join",     r"Join\s+\w+\s*\([^)]*\band\b[^)]*\)", "src"),
    ("D4  join w/ dynafunction",  r"Join\s+\w+\s*\(\s*(concat|upper|lower|substring)", "src"),
    ("D5  self-join {target}",    r"\{target\}", "src"),
    ("D6  Filter",                r"^\s*Filter\s+\w+", "src"),
    ("D7  View",                  r"^\s*View\s+\w+", "src"),
    ("D8  Schema",                r"^\s*Schema\s+", "src"),
    ("D10 TabularFunction",       r"^\s*TabularFunction\s+", "src"),
    ("D11 MultiGrainFilter",      r"^\s*MultiGrainFilter\s+", "src"),
    ("E3  XStore",                r":\s*XStore", "src"),
    ("E4  AggregationAware",      r":\s*AggregationAware", "src"),
    ("E6  Operation union",       r"union_OperationSetImplementation|:\s*Operation\s*\{", "src"),
    ("--  join non-equality",     r"Join\s+\w+\s*\(.{0,200}?(<|>|is\s+not\s+null|is\s+null)", "src"),
    ("--  join with or",          r"Join\s+\w+\s*\(.{0,200}?\bor\b", "src"),
    ("--  composite PK",          r"PRIMARY KEY[^)]*,[^)]*PRIMARY KEY", "src"),
    ("--  milestoning",           r"milestoning\s*\(", "src"),
]


def load() -> tuple[str, list[tuple[str, str]]]:
    import model
    src = "\n".join(f.read_text() for f in model.stress_sources())
    # Comments must not count. The corpus is heavily commented and several of these feature
    # names appear in prose explaining why they are ABSENT.
    src = re.sub(r"//[^\n]*", " ", src)
    return src, _BLOCK.findall(src)


def main() -> None:
    src, blocks = load()
    print(f"class mappings: {len(blocks)}    files: {len(glob.glob(STRESS))}")

    plain = 0
    body_pats = [(n, p) for n, p, s in FEATURES if s == "body"]
    for _kind, body in blocks:
        if not any(re.search(p, body, re.M) for _n, p in body_pats):
            plain += 1
    pct = plain / len(blocks) if blocks else 0
    print(f"PLAIN 1:1 (only ~mainTable + direct columns): {plain}  ({pct:.0%})\n")

    absent = []
    for name, pat, scope in FEATURES:
        if scope == "body":
            n = sum(1 for _k, b in blocks if re.search(pat, b, re.M))
        else:
            n = len(re.findall(pat, src, re.M))
        if n == 0:
            absent.append(name)
        print(f"  {n:>5}  {name}")

    print(f"\n{len(FEATURES) - len(absent)} of {len(FEATURES)} features present; "
          f"{len(absent)} ABSENT")
    if absent:
        print("  absent: " + ", ".join(a.split()[0] for a in absent))

    if "--gate" in sys.argv:
        # The ratchet. Density can otherwise be destroyed by success elsewhere: a generator
        # that adds five hundred flat mappings improves every other number in this corpus
        # while making it shallower.
        bad = []
        if pct > MAX_PLAIN_RATIO:
            bad.append(f"plain-1:1 ratio rose to {pct:.0%} (max {MAX_PLAIN_RATIO:.0%})")
        if len(absent) > MAX_ABSENT:
            bad.append(f"{len(absent)} features absent (max {MAX_ABSENT}): "
                       f"{[a.split()[0] for a in absent]}")
        for b in bad:
            print("GATE:", b)
        sys.exit(1 if bad else 0)


# Baselines: TODAY'S REALITY, not a target. Lower them as features land; never raise them
# to make a run green.
#
# 94% is the honest figure and a first pass said 51%, because that pass counted any join
# reference `@J` as an advanced feature. A single `@J` in a property mapping is ordinary
# association navigation -- it is what every one of the 174 association mappings does. The
# feature worth counting is a join CHAIN (`@J1 > @J2`), of which the corpus has none. The
# looser definition flattered the corpus by a factor of eight.
MAX_PLAIN_RATIO = 0.81   # 375/464
MAX_ABSENT = 0


if __name__ == "__main__":
    main()
