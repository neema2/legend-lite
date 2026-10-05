"""
Parser completeness — the keyword surface, measured against sources WE own.

There is already a keyword census in parser-equivalence, and it answers a different
question: does this keyword appear anywhere in legend-engine's own corpus. That is the
right denominator for "has the parity harness ever seen this grammar arm", and it is the
wrong one for "is legend-lite's parser complete", because a construct nobody upstream
happened to write is invisible to it. Ninety percent of THEIR fixtures says nothing about
the other ten percent of the grammar.

This measures the grammar directly:

  * harvest every literal keyword from all 156 .g4 files, grouped by grammar
  * check each against the .pure sources in this repository
  * report what is missing, BY GRAMMAR, so the gap is a work queue rather than a number

A keyword here is a lexer rule with a quoted literal — `DATABASE: 'Database'` — which is
exactly what a user can type. Parser rules, fragments and character classes are excluded:
they are structure, not surface.

Deliberately NOT measured here: whether the construct is handled correctly once parsed.
That is what the stress corpus is for. A keyword can be lexed, routed, and still mean
nothing — this only establishes that the surface is reachable.
"""
from __future__ import annotations

import re
import sys
from collections import defaultdict
from pathlib import Path

import tiers

# Every input arrives by argument, from Bazel (scripts/parser/BUILD.bazel; Bazel workplan P2-20): the pinned
# legend-engine tree (@legend_engine_src, by its pom.xml), the runner's vocabulary (//tools/engine-runner:vocab) and
# the roots of the .pure this repository owns. Paths are relative to the working directory: the execution root in
# //scripts/parser:keyword_coverage, the runfiles tree under `bazel run //scripts/parser:keywords`.
def _arg(name: str) -> str | None:
    return sys.argv[sys.argv.index(name) + 1] if name in sys.argv else None


def _args(name: str) -> list[str]:
    return [sys.argv[i + 1] for i, a in enumerate(sys.argv) if a == name and i + 1 < len(sys.argv)]


_ENGINE_POM = _arg("--engine-pom")
_ENGINE_ROOT = _arg("--engine-root")   # a tree of the grammars alone (//scripts/parser:engine_grammars)
ENGINE = Path(_ENGINE_ROOT) if _ENGINE_ROOT else (Path(_ENGINE_POM).parent if _ENGINE_POM else None)

# `NAME: 'literal'` at the start of a line — a lexer token with a typeable spelling.
_TOKEN = re.compile(r"^([A-Z_][A-Z0-9_]*)\s*:\s*'([^']+)'", re.M)

# Keywords that are punctuation or operators rather than words a user "types" as a
# construct. Excluded from the target so the number means something: nobody writes a test
# to cover '{'.
_PUNCT = re.compile(r"^[^A-Za-z]+$")

# Escape sequences, not spellings. GraphQL declares its three byte-order marks as literal
# tokens -- UTF8_BOM: 'BF' and friends -- and the harvest was reading the escape TEXT
# and reporting `BF` as a keyword nobody had covered. A BOM is a file encoding
# artifact, not a construct a user types, and demanding a fixture for one would be asking
# the corpus to prove something about its own file headers.
_ESCAPE = re.compile(r"^\\[uU]")


def _grammar_key(g4: Path, duplicated: set[str]) -> str:
    """Grammar identity. The file stem, EXCEPT where two modules ship the same stem.

    legend-engine has exactly one such collision and it is not cosmetic:
    SnowflakeLexerGrammar exists twice, once under relationalStore-snowflake (the
    ###Connection datasource spec, 20 tokens) and once under xts-snowflake (the ###Snowflake
    function activator, 17 tokens). Keying on stem merged two grammars that route to
    DIFFERENT sections into one bucket of 37. The total happened to be right, which is worse
    than being wrong -- the number looked fine while the attribution was nonsense, and
    anyone reading "SnowflakeLexerGrammar: 37" would go looking for one grammar.
    """
    if g4.stem not in duplicated:
        return g4.stem
    # The owning maven module disambiguates and stays readable in the work queue.
    module = next((part for part in reversed(g4.parts)
                   if part.startswith("legend-engine-xt")), g4.parent.name)
    return f"{g4.stem}@{module}"


def harvest() -> dict[str, set[str]]:
    """grammar identity -> the literal keywords it defines."""
    out: dict[str, set[str]] = defaultdict(set)
    stems = [g.stem for g in ENGINE.rglob("*.g4")]
    duplicated = {s for s in stems if stems.count(s) > 1}
    for g4 in sorted(ENGINE.rglob("*.g4")):
        text = g4.read_text(errors="replace")
        for _token, literal in _TOKEN.findall(text):
            if _PUNCT.match(literal) or _ESCAPE.match(literal):
                continue
            # Composite tokens keep only their first quoted fragment through the regex
            # above. MappingLexerGrammar declares
            #     INCLUDETYPE: 'include '[a-z]([a-z])*' ';
            # and the harvest read `include ` -- with a trailing space, which no word-
            # boundary search can ever match, since what follows is always a letter. The
            # text a user actually types is `include`, so trim. It collapses onto the plain
            # INCLUDE token, which is right: they are one spelling with two arms.
            out[_grammar_key(g4, duplicated)].add(literal.strip())
    return dict(out)


_VOCAB = re.compile(r"tokenVocab\s*=\s*([A-Za-z0-9_]+)")

# A token whose rule is NOTHING BUT a quoted literal. Only these can be checked against a
# compiled Vocabulary: ANTLR gives a literal name only to tokens defined as a single literal,
# so a composite like
#     CONSTRAINT_OWNER: '~owner' CONSTRAINT_SEPARATOR;
# has no literal name at all and would look "missing from the jar" while parsing perfectly.
# Five of those in DomainLexerGrammar alone, every one of them covered by a passing fixture.
_SIMPLE_TOKEN = re.compile(r"^([A-Z_][A-Z0-9_]*)\s*:\s*'([^']+)'\s*;", re.M)

RUNNER_VOCAB = Path(_arg("--vocab")) if _arg("--vocab") else None


def runner_vocabulary() -> dict[str, set[str]]:
    """lexer simple-name -> the literals the RUNNER'S JARS can lex.

    Produced by perf.TokenDump; regenerate whenever the engine version moves. Absent file
    means the check is skipped rather than silently passing -- see version_skew.
    """
    if RUNNER_VOCAB is None or not RUNNER_VOCAB.is_file():
        return {}
    out = {}
    for line in RUNNER_VOCAB.read_text(encoding="utf-8").splitlines():
        parts = line.split("\t")
        if parts:
            out[parts[0]] = set(parts[1:])
    return out


def version_skew() -> dict[str, set[str]]:
    """Keywords the .g4 SOURCE declares that the runner's JARS do not have.

    The census reads a legend-engine working copy at git HEAD; the runner parses with
    released jars. Where they disagree, the source is describing a parser nobody here is
    running, and no fixture can cover the difference. Reporting those as "missing" would
    make 100% unreachable for a reason that has nothing to do with legend-lite.

    Only simple-literal tokens are comparable, and only for lexers that exist as a class in
    the jars -- Core, M3 and GraphQL are compiled into their importers and have no class of
    their own, so for them this returns nothing rather than guessing.
    """
    vocab = runner_vocabulary()
    if not vocab:
        return {}
    out: dict[str, set[str]] = defaultdict(set)
    stems = [g.stem for g in ENGINE.rglob("*.g4")]
    duplicated = {s for s in stems if stems.count(s) > 1}
    for g4 in sorted(ENGINE.rglob("*.g4")):
        if g4.stem not in vocab:
            continue
        for _name, literal in _SIMPLE_TOKEN.findall(g4.read_text(errors="replace")):
            lit = literal.strip()
            if _PUNCT.match(lit) or _ESCAPE.match(lit):
                continue
            if lit not in vocab[g4.stem]:
                out[_grammar_key(g4, duplicated)].add(lit)
    return dict(out)


def dead_tokens() -> dict[str, set[str]]:
    """Keywords a lexer DECLARES that no parser rule can ever reach.

    ANTLR token namespaces are per-grammar, and the link is `options { tokenVocab = X; }`.
    So a token is reachable only if a parser grammar importing THAT vocabulary names it.
    Searching all parser grammars instead would call `host` reachable because some other
    grammar happens to define its own HOST -- which is how this was nearly missed.

    AuthenticationStrategyLexerGrammar declares ten of these. Writing `host` inside an
    `auth:` block is rejected with "Valid alternatives: ['baseVaultReference',
    'userNameVaultReference', 'passwordVaultReference']" -- the token lexes and no rule
    accepts it. They are excluded from the denominator because no fixture can cover them:
    100% is not a target if part of the target is unreachable by construction.

    A grammar nothing imports by vocabulary (combined grammars like GraphQL; lexers pulled
    in by ANTLR `import` like Core and M3) falls back to every grammar's text, which is
    generous on purpose -- silence is the right answer when the link cannot be established.
    """
    stems = [g.stem for g in ENGINE.rglob("*.g4")]
    duplicated = {s for s in stems if stems.count(s) > 1}
    consumers: dict[str, list[str]] = defaultdict(list)
    every = []
    for g4 in ENGINE.rglob("*.g4"):
        text = g4.read_text(errors="replace")
        every.append(text)
        for vocab in _VOCAB.findall(text):
            consumers[vocab].append(text)
    all_text = "\n".join(every)

    out: dict[str, set[str]] = defaultdict(set)
    for g4 in sorted(ENGINE.rglob("*.g4")):
        reachable_in = "\n".join(consumers.get(g4.stem, [])) or all_text
        key = _grammar_key(g4, duplicated)
        for name, literal in _TOKEN.findall(g4.read_text(errors="replace")):
            if _PUNCT.match(literal) or _ESCAPE.match(literal):
                continue
            if not re.search(rf"(?<![A-Za-z0-9_]){name}(?![A-Za-z0-9_])", reachable_in):
                out[key].add(literal)
    return dict(out)


# Comments and string literals, removed before counting. A keyword inside a comment or
# inside a quoted CSV payload is not a keyword the parser ever saw -- and this corpus is
# full of both, since the generated ###Data blocks are megabytes of quoted rows and the
# hand-written files carry long explanatory comments that name constructs.
#
# ONE PASS, not three. The obvious implementation runs three regex substitutions in
# sequence, and it is wrong in a way that hides coverage rather than inflating it: a `//`
# inside a string literal -- `'http://host'` -- is stripped as a comment first, taking the
# closing quote with it, which unbalances every string scan that follows and swallows real
# code to the end of the file. Found by a fixture whose `authentication` keyword read as
# uncovered because a URL three lines earlier had a scheme in it.
#
# Whichever construct opens FIRST wins, which is what the lexer does.
_OPENERS = re.compile(r"/\*|//|'")


def code_spans(text: str) -> list[tuple[int, int]]:
    """(start, end) ranges of `text` that are CODE -- outside comments and string literals.

    Same single-pass scan as strip_noncode, reporting positions instead of substituting. It
    exists for the mutation harness, which was editing text inside comments and recording
    the result as "legend-engine accepted this mutation" -- a mutant that changes only a
    comment changes nothing, and counting it as accepted makes the parser look more
    permissive than it is.
    """
    spans, i, start = [], 0, 0
    while True:
        m = _OPENERS.search(text, i)
        if not m:
            spans.append((start, len(text)))
            return [(a, b) for a, b in spans if b > a]
        tok = m.group()
        spans.append((start, m.start()))
        if tok == "/*":
            end = text.find("*/", m.end())
            i = len(text) if end < 0 else end + 2
        elif tok == "//":
            end = text.find("\n", m.end())
            i = len(text) if end < 0 else end
        else:
            j = m.end()
            while j < len(text):
                if text[j] == "\\":
                    j += 2
                    continue
                if text[j] == "'":
                    j += 1
                    break
                j += 1
            i = j
        start = i


def strip_noncode(text: str) -> str:
    out, i = [], 0
    while True:
        m = _OPENERS.search(text, i)
        if not m:
            out.append(text[i:])
            return "".join(out)
        out.append(text[i:m.start()])
        tok = m.group()
        if tok == "/*":
            end = text.find("*/", m.end())
            i = len(text) if end < 0 else end + 2
            out.append(" ")
        elif tok == "//":
            end = text.find("\n", m.end())
            i = len(text) if end < 0 else end
            out.append(" ")
        else:
            # A Pure string literal. Backslash escapes the next character, including a
            # quote; an unterminated literal runs to end of input rather than re-opening.
            j = m.end()
            while j < len(text):
                if text[j] == "\\":
                    j += 2
                    continue
                if text[j] == "'":
                    j += 1
                    break
                j += 1
            i = j
            out.append(" '' ")


def our_sources() -> str:
    """Every .pure this repository owns, with comments and string literals removed.

    Coverage is measured against what WE wrote, not against what upstream happens to
    contain — and against CODE, not prose about code.
    """
    parts = []
    roots = _args("--ours") or ["core/src/test/resources", "scripts"]
    for root in roots:
        base = Path(root)
        if not base.is_dir():
            raise SystemExit(f"keywords.py: --ours {root} is not a directory")
        for p in sorted(base.rglob("*.pure")):
            parts.append(strip_noncode(p.read_text(errors="replace")))
    if not parts:
        raise SystemExit("keywords.py: no .pure under " + ", ".join(roots))
    return "\n".join(parts)


def covered(keywords: set[str], text: str) -> set[str]:
    """A keyword counts as covered when it appears as a WORD in our CODE.

    Word boundaries, because substring matching would be wildly generous — 'in' occurs
    inside a thousand identifiers. Comments and string literals are already stripped.

    What remains over-counted, stated plainly: a keyword is credited wherever it appears,
    even in a section whose grammar does not define it. `Schema` in a Service section
    would count toward the Relational grammar. Removing that needs per-section lexing with
    the grammar's own lexer, which is the exact fix; this is the honest approximation
    until fixtures replace the search entirely — a fixture that PARSES proves reachability
    in a way no text search can.
    """
    found = set()
    for k in keywords:
        if re.search(word_pattern(k), text):
            found.add(k)
    return found


def word_pattern(keyword: str) -> str:
    """Boundary-anchored pattern, anchored only where a boundary can exist.

    Applying both lookarounds unconditionally is wrong for any literal that does not begin
    and end with a word character, and it silently reports such a keyword as never covered.
    Two in tier 1 alone: GraphFetchTree's entire lexer is `->subType(@`, whose next
    character is always a type name, and MappingLexerGrammar's INCLUDETYPE fragment ends in
    a space. Both were sitting at zero coverage with fixtures that plainly used them.
    """
    pattern = re.escape(keyword)
    if keyword[:1].isalnum() or keyword[:1] == "_":
        pattern = r"(?<![A-Za-z0-9_])" + pattern
    if keyword[-1:].isalnum() or keyword[-1:] == "_":
        pattern = pattern + r"(?![A-Za-z0-9_])"
    return pattern


def _keywords_in(grammars: dict[str, set[str]], stems) -> set[str]:
    return set().union(*[grammars[s] for s in stems if s in grammars]) if stems else set()


def coverage_tsv(grammars: dict[str, set[str]], have: set[str]) -> str:
    """The measurement as a file (keyword-coverage.tsv, a diff-tested golden): one row per in-scope grammar --
    tier, grammar, covered, total, the missing keywords -- then a row per tier and the in-scope total."""
    lines = ["# keyword coverage of the pinned legend-engine grammars by this repository's .pure (scripts/parser/keywords.py)."
             " Regenerate: bazel run //scripts/parser:update_keyword_coverage",
             "tier\tgrammar\tcovered\ttotal\tmissing"]
    for stem in sorted(grammars):
        tier = tiers.tier_of(stem)
        if tier in ("out", "unclassified"):
            continue
        kws = grammars[stem]
        lines.append(f"{tier}\t{stem}\t{len(kws & have)}\t{len(kws)}\t{','.join(sorted(kws - have))}")
    for label, stems in (("1", tiers.TIER1), ("1-embedded", tiers.TIER1_EMBEDDED), ("2-vendor", tiers.TIER2_VENDOR),
                         ("2-dsl", tiers.TIER2_DSL),
                         ("in-scope", tiers.TIER1 | tiers.TIER1_EMBEDDED | tiers.TIER2)):
        kws = _keywords_in(grammars, stems)
        lines.append(f"TOTAL\t{label}\t{len(kws & have)}\t{len(kws)}\t{','.join(sorted(kws - have))}")
    return "\n".join(lines) + "\n"


def main() -> None:
    if ENGINE is None or not ENGINE.is_dir():
        raise SystemExit("keywords.py: --engine-pom <the pinned tree's pom.xml> or --engine-root <a grammar tree> (Bazel passes one)")
    grammars = harvest()
    text = our_sources()
    all_kw = {k for ks in grammars.values() for k in ks}
    have = covered(all_kw, text)
    out = _arg("--out")
    if out:
        unclassified = sorted(s for s in grammars if tiers.tier_of(s) == "unclassified")
        if unclassified:
            raise SystemExit(f"UNCLASSIFIED GRAMMARS (add to scripts/parser/tiers.py): {unclassified}")
        Path(out).write_text(coverage_tsv(grammars, have), encoding="utf-8", newline="\n")
        return

    # A grammar upstream adds must land in a tier deliberately. Without this it would
    # simply be absent from every total, and the coverage percentage would go UP because
    # new surface appeared -- the exact failure this harness exists to prevent.
    unclassified = sorted(s for s in grammars if tiers.tier_of(s) == "unclassified")
    if unclassified:
        print(f"UNCLASSIFIED GRAMMARS (add to scripts/parser/tiers.py): {unclassified}\n")

    scope = tiers.TIER1 | tiers.TIER1_EMBEDDED | tiers.TIER2
    in_scope = _keywords_in(grammars, scope)
    dropped = _keywords_in(grammars, tiers.OUT_OF_SCOPE) - in_scope

    print(f"{len(grammars)} grammars, {len(all_kw)} distinct typeable keywords")
    print(f"  out of scope: {len(dropped)} keywords in {len(tiers.OUT_OF_SCOPE)} grammars "
          f"unreachable from .pure text (see tiers.py)")
    print(f"  IN SCOPE:     {len(in_scope)}\n")

    for label, stems in (("TIER 1  core Legend surface", tiers.TIER1),
                         ("TIER 1  embedded (GraphQL)", tiers.TIER1_EMBEDDED),
                         ("TIER 2  vendor connectors", tiers.TIER2_VENDOR),
                         ("TIER 2  extension DSLs", tiers.TIER2_DSL)):
        kws = _keywords_in(grammars, stems)
        print(f"{label:<32}{len(kws & have):>5} of {len(kws):<5} missing {len(kws - have)}")
    print(f"{'ALL IN SCOPE':<32}{len(in_scope & have):>5} of {len(in_scope):<5} "
          f"missing {len(in_scope - have)}")

    missing_embedded = [m for m in tiers.EMBEDDED if m not in text]
    print(f"{'embedded Pure parsers':<32}"
          f"{len(tiers.EMBEDDED) - len(missing_embedded):>5} of {len(tiers.EMBEDDED)}"
          + (f"    missing {missing_embedded}" if missing_embedded else ""))

    rows = []
    for stem, kws in grammars.items():
        miss = kws - have
        if miss and tiers.tier_of(stem) != "out":
            rows.append((tiers.tier_of(stem), len(miss), len(kws), stem, sorted(miss)))

    if "--tier1" in sys.argv:
        rows = [r for r in rows if r[0].startswith("1")]
    rows.sort(key=lambda r: (r[0], -r[1]))

    if "--gaps" in sys.argv or "--tier1" in sys.argv:
        print("\nwork queue:\n")
        for tier, miss, total, stem, words in rows:
            print(f"[t{tier}] [{total - miss:>3} of {total:>3}] "
                  f"{stem.replace('LexerGrammar', '')}")
            print("       " + ", ".join(words))
    else:
        print(f"\n{'grammar':<50}{'tier':>5}{'missing':>9}{'of':>6}")
        for tier, miss, total, stem, _ in sorted(rows, key=lambda r: -r[1])[:20]:
            print(f"  {stem:<48}{tier:>5}{miss:>9}{total:>6}")
        print(f"\n({len(rows)} in-scope grammars have gaps; "
              f"--tier1 or --gaps lists the keywords)")


if __name__ == "__main__":
    main()
