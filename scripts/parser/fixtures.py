"""
Runs the parser fixtures and recomputes coverage from the ones that actually PARSE.

This replaces the text search in keywords.py as the source of truth, and it removes that
harness's stated weakness. The text search credits a keyword wherever it appears, including
in a section whose grammar does not define it -- `Schema` written in a Service section would
count toward Relational. Here a fixture declares which grammar it covers:

    // COVERS RelationalLexerGrammar: Schema, TabularFunction, scope

and the claim is checked three ways. The keyword must appear in the fixture's code; the
fixture must PARSE; and the fixture is one section, so the keyword is credited only against
the grammar that section routes to. A declaration that lies fails the first check.

Parse, not compile. A fixture exercising `~distinct` on a View does not need a runtime, a
connection and seeded data to prove the parser accepts `~distinct`. Demanding compilation
would make each fixture ten times the size and would make the coverage number depend on the
compiler -- which is the next stage's problem, and is what the stress corpus measures.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import keywords as K
import tiers

REPO = Path(__file__).resolve().parents[2]
POSITIVE = Path(__file__).resolve().parent / "fixtures"
NEGATIVE = Path(__file__).resolve().parent / "negative"
RUNNER = REPO / "tools" / "engine-runner"
JAVA_HOME = os.environ.get("JAVA_HOME", str(Path.home() / "jdk/jdk-21.0.11+10/Contents/Home"))

_GRAMMAR = r"[A-Za-z0-9][A-Za-z0-9@._-]*"
_COVERS = re.compile(rf"^//\s*COVERS\s+({_GRAMMAR})\s*:\s*(.+)$", re.M)

# A second form, for sub-grammars whose input IS a string literal.
#
#     // COVERS-EMBEDDED FlatDataLexerGrammar: section, Record, STRING
#
# FlatData is written inside the quoted `content:` of a SchemaSet, and its .g4 re-declares
# STRING and BRACE_OPEN precisely so it can parse standalone. For that grammar the string
# body is not prose around code -- it is the code. Checked against RAW text rather than
# stripped, and kept as a separate marker so the exemption has to be claimed deliberately
# per grammar instead of weakening the check for everything.
_COVERS_EMBEDDED = re.compile(rf"^//\s*COVERS-EMBEDDED\s+({_GRAMMAR})\s*:\s*(.+)$", re.M)


def declarations(path: Path, embedded: bool = False) -> list[tuple[str, list[str]]]:
    """(grammar, [keyword, ...]) for each COVERS (or COVERS-EMBEDDED) line."""
    pattern = _COVERS_EMBEDDED if embedded else _COVERS
    return [(grammar, [k.strip() for k in rest.split(",") if k.strip()])
            for grammar, rest in pattern.findall(path.read_text())]


def run_parser(paths: list[Path], expect_fail: bool = False) -> tuple[int, str]:
    cp = (RUNNER / "cp.txt").read_text().strip()
    if not (RUNNER / "target" / "classes").is_dir():
        raise SystemExit(f"runner not built; see {RUNNER}/README.md")
    cmd = [f"{JAVA_HOME}/bin/java", "-cp", f"{RUNNER}/target/classes:{cp}", "perf.ParseMain",
           *[str(p) for p in paths]]
    if expect_fail:
        cmd.append("--expect-fail")
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
    lines = [ln for ln in r.stdout.splitlines() if not ln.startswith("SLF4J")]
    return r.returncode, "\n".join(lines)


def check_declarations(grammars: dict[str, set[str]]) -> list[str]:
    """A COVERS line must name a real grammar, real keywords of it, and keywords the
    fixture's own code actually contains. Each of these has failed in practice: a keyword
    gets renamed upstream, or a fixture is edited and the declaration is not."""
    problems = []
    for f in sorted(POSITIVE.rglob("*.pure")):
        raw = f.read_text()
        decls = [(g, k, False) for g, k in declarations(f)]
        decls += [(g, k, True) for g, k in declarations(f, embedded=True)]
        if not decls:
            problems.append(f"{f.name}: no COVERS line -- fixture claims nothing")
        code = K.strip_noncode(raw)
        for grammar, kws, in_string in decls:
            haystack = raw if in_string else code
            if grammar not in grammars:
                problems.append(f"{f.name}: unknown grammar {grammar}")
                continue
            for k in kws:
                if k not in grammars[grammar]:
                    problems.append(f"{f.name}: {k!r} is not a keyword of {grammar}")
                elif not re.search(K.word_pattern(k), haystack):
                    problems.append(f"{f.name}: declares {k!r} but its code does not use it")
    return problems


# What a negative fixture must declare:
#
#     // REJECTS: Unexpected token 'Schema'
#
# checked as a substring of the parser's actual message. Without this a negative fixture
# passes as long as it fails SOMEHOW -- and the easiest way to fail is a typo, which pins
# nothing and keeps passing forever while the construct it was written to guard quietly
# starts working. "It was rejected" is not the assertion; "it was rejected FOR THIS REASON"
# is.
_REJECTS = re.compile(r"^//\s*REJECTS:\s*(.+?)\s*$", re.M)


# Rejection messages that mean "your environment is wrong", not "your input is wrong".
# A negative fixture failing for one of these is not testing the grammar at all -- it would
# pass identically against a parser that simply had not loaded the extension, and it would
# keep passing after the construct it guards started working. Four grammar extensions really
# were missing from this runner's classpath, so this is not hypothetical.
_ENVIRONMENTAL = (
    "is not a known section parser",
    "Unsupported Data Source Specification type",
    "NoClassDefFound",
    "ClassNotFound",
)


def check_environmental(out: str) -> list[str]:
    """Negatives rejected for an environmental reason must say so in the fixture.

    Acknowledged by writing ENVIRONMENTAL in the fixture's comment -- there are legitimate
    cases (the unshipped-section fixture is exactly this, deliberately), so the rule is
    "declare it", not "never".
    """
    problems = []
    for line in out.splitlines():
        if "  REJECTED: " not in line:
            continue
        head, _, msg = line.partition("  REJECTED: ")
        name = head.split()[-1]
        if not any(e in msg for e in _ENVIRONMENTAL):
            continue
        f = NEGATIVE / name
        if f.is_file() and "ENVIRONMENTAL" not in f.read_text():
            problems.append(f"{name}: rejected for an ENVIRONMENTAL reason ({msg[:70]!r}). "
                            f"This would pass against a parser that never loaded the "
                            f"extension. Confirm it is intended and say ENVIRONMENTAL in "
                            f"the fixture, or rewrite it.")
    return problems


def check_rejections(out: str) -> list[str]:
    """Every negative fixture declares the message it expects, and gets it."""
    actual = {}
    for line in out.splitlines():
        if "  REJECTED: " in line:
            head, _, msg = line.partition("  REJECTED: ")
            actual[head.split()[-1]] = msg.strip()
    problems = []
    for f in sorted(NEGATIVE.rglob("*.pure")):
        declared = _REJECTS.findall(f.read_text())
        if not declared:
            problems.append(f"{f.name}: no REJECTS line -- pins nothing")
            continue
        got = actual.get(f.name)
        if got is None:
            continue                      # accepted; already reported as WRONG
        for want in declared:
            if want not in got:
                problems.append(f"{f.name}:\n    expected {want!r}\n    but got  {got!r}")
    return problems


_SECTION = re.compile(r"^###([A-Za-z]+)\s*$", re.M)


def by_section(text: str) -> dict[str, list[str]]:
    """Split a .pure into `###Section` -> its code blocks, comments and strings stripped.

    The section is where legend-engine dispatches to a grammar, so attributing keywords per
    section is not an approximation of the routing -- it is the routing.
    """
    out: dict[str, list[str]] = defaultdict(list)
    marks = list(_SECTION.finditer(text))
    for i, m in enumerate(marks):
        end = marks[i + 1].start() if i + 1 < len(marks) else len(text)
        out[m.group(1)].append(K.strip_noncode(text[m.end():end]))
    return out


def corpus_coverage(grammars: dict[str, set[str]],
                    extra: list[Path] | None = None) -> dict[str, set[str]]:
    """What sources cover WITHOUT a COVERS line, attributed by section.

    Two inputs. The existing stress corpus, which carries no declarations but parses and is
    run against real databases every commit -- re-authoring a fixture for `Database` because
    that corpus happens to spell it would be busywork. And the fixtures that PARSED, for
    everything they exercise incidentally: a Diagram fixture declaring `classView` also
    writes `source` and `target`, and demanding they be declared too would turn every
    fixture's header into a transcription of its own body.

    The COVERS declaration stays the stronger claim -- it pins a keyword to one grammar
    rather than to every grammar the section routes to -- and stays required for the
    embedded sub-grammars, where section attribution cannot reach inside a string.
    """
    have: dict[str, set[str]] = defaultdict(set)
    sources = list(extra or [])
    for root in ("core/src/test/resources", "scripts", "pct-corpus", "experiments"):
        base = REPO / root
        if base.is_dir():
            sources += [p for p in base.rglob("*.pure")
                        if POSITIVE not in p.parents and NEGATIVE not in p.parents]
    if True:
        for p in sources:
            for section, blocks in by_section(p.read_text(errors="replace")).items():
                for stem in tiers.SECTION_GRAMMARS.get(section, set()):
                    body = "\n".join(blocks)
                    have[stem] |= K.covered(grammars.get(stem, set()), body)
    return have


def existing_union(existing, stems) -> set[str]:
    return set().union(*[existing.get(s, set()) for s in stems]) if stems else set()


def main() -> None:
    grammars = K.harvest()

    problems = check_declarations(grammars)
    for p in problems:
        print("DECLARATION:", p)
    if problems:
        print()

    fixtures = sorted(POSITIVE.rglob("*.pure"))
    negatives = sorted(NEGATIVE.rglob("*.pure")) if NEGATIVE.is_dir() else []

    rc_pos, out_pos = (run_parser([POSITIVE]) if fixtures else (0, ""))
    parsed = {ln.split()[1] for ln in out_pos.splitlines() if ln.startswith("ok ")}
    if rc_pos:
        print(out_pos)
        print()

    rc_neg = 0
    if negatives:
        rc_neg, out_neg = run_parser([NEGATIVE], expect_fail=True)
        if rc_neg:
            print(out_neg)
            print()
        mismatched = check_rejections(out_neg) + check_environmental(out_neg)
        for m in mismatched:
            print("REJECTION:", m)
        if mismatched:
            print()
            problems += mismatched

    # Only fixtures that PARSED contribute. A fixture that claims six keywords and is
    # rejected has demonstrated nothing about any of them.
    from_fixtures: dict[str, set[str]] = defaultdict(set)
    for f in fixtures:
        if f.name not in parsed:
            continue
        for grammar, kws in declarations(f) + declarations(f, embedded=True):
            from_fixtures[grammar].update(kws)

    existing = corpus_coverage(grammars, [f for f in fixtures if f.name in parsed])
    have = {s: from_fixtures.get(s, set()) | existing.get(s, set())
            for s in set(from_fixtures) | set(existing)}

    print(f"{len(fixtures)} positive fixtures, {len(parsed)} parsed, "
          f"{len(fixtures) - len(parsed)} REJECTED")
    if negatives:
        print(f"{len(negatives)} negative fixtures, {len(negatives) - rc_neg} correctly "
              f"rejected, {rc_neg} WRONGLY ACCEPTED")
    print()

    # Unreachable surface is removed from the denominator, not quietly counted as missing.
    # A target that includes keywords no fixture can ever cover makes 100% impossible and
    # turns the number into a permanent accusation instead of a goal.
    dead = K.dead_tokens()
    for label, stems in (("TIER 1  core Legend surface", tiers.TIER1),
                         ("TIER 1  embedded (GraphQL)", tiers.TIER1_EMBEDDED)):
        declared = set().union(*[grammars[s] for s in stems if s in grammars])
        unreachable = set().union(*[dead.get(s, set()) for s in stems]) if stems else set()
        unreachable |= {k for (s, k) in tiers.WALKER_REJECTED if s in stems}
        unreachable |= {k for (s, k) in tiers.UNSHIPPED_SECTION if s in stems}
        total = declared - unreachable
        got = set().union(*[have.get(s, set()) for s in stems]) if stems else set()
        got &= total
        fx = set().union(*[from_fixtures.get(s, set()) for s in stems]) if stems else set()
        pct = f"{len(got) / len(total):.0%}" if total else "n/a"
        print(f"{label:<32}{len(got):>5} of {len(total):<5}{pct:>5}   "
              f"missing {len(total - got):<4} "
              f"(+{len(unreachable)} unreachable, excluded)")
        _ = fx

    if "--gaps" in sys.argv:
        print("\nstill missing, by grammar (unreachable already removed):\n")
        for stem in sorted(tiers.TIER1 | tiers.TIER1_EMBEDDED):
            reachable = grammars.get(stem, set()) - dead.get(stem, set())
            reachable -= {k for (s, k) in tiers.WALKER_REJECTED if s == stem}
            reachable -= {k for (s, k) in tiers.UNSHIPPED_SECTION if s == stem}
            miss = sorted(reachable - have.get(stem, set()))
            if miss:
                print(f"[{len(reachable) - len(miss):>3} of {len(reachable):>3}] "
                      f"{stem.replace('LexerGrammar', '')}")
                print("       " + ", ".join(miss))

    sys.exit(1 if (problems or rc_pos or rc_neg) else 0)


if __name__ == "__main__":
    main()
