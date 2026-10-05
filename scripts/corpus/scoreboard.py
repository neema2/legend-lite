"""
The SURFACE scoreboard: the corpus scored against what legend-engine can actually express.

Every coverage number before this one was scored against a taxonomy I wrote. That measures
how well the corpus covers what I thought of, which is not the question -- the constructs
most likely to be untested are exactly the ones nobody remembered to list. density.py's
taxonomy had 35 entries. The engine's declarative surface, enumerated from its ANTLR
grammars and parse-tree walkers, has 263, and its executable function registry has 440.

docs/ENGINE_SURFACE.tsv is that inventory. Each row carries a detection pattern, so
"present" is decided by matching the corpus against a construct derived from the GRAMMAR
rather than from memory. Where a construct cannot be detected by text the row carries no
pattern and is reported UNKNOWN, never assumed absent and never assumed present.

Three columns, and the gap between them is the point:

    PRESENT     the corpus writes it somewhere
    EXECUTED    a service that PASSES depends on it        (executed.py)
    STACKED     it co-occurs with another construct        (stacking.py)

A construct can be present and unexecuted -- 60 scope-using class mappings sat in a Mapping
no runtime bound. It can be executed and unstacked -- half this corpus's services exercise
exactly one thing. Only the third column measures what the corpus was built to find.

Deliberately NOT scored as a percentage of 263. Some rows are alternative spellings of one
idea (nine authentication strategies, twenty-one column types), and averaging them produces
a number that improves by adding the cheapest possible cases. The report is per DSL, with
the absent rows named, because the list is the actionable artefact and the ratio is not.
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

# the repository root as model.py takes it: CORPUS_ROOT under Bazel (declared inputs only), never __file__, which
# follows the runfiles symlink back into the checkout
ROOT = Path(os.environ["CORPUS_ROOT"]) if os.environ.get("CORPUS_ROOT") else Path(__file__).resolve().parents[2]
SURFACE = ROOT / "docs/ENGINE_SURFACE.tsv"


def load_surface() -> list[dict]:
    rows = []
    for line in SURFACE.read_text().splitlines()[1:]:
        parts = line.split("\t")
        if len(parts) < 5:
            parts += [""] * (5 - len(parts))
        rows.append(dict(zip(("id", "dsl", "name", "syntax", "detect"), parts)))
    return rows


def corpus_text() -> str:
    """Every .pure source, comments STRIPPED.

    Comments must not count. This corpus is heavily commented and several constructs are
    named in prose explaining why they are absent -- scoring those as present would make the
    documentation of a gap close the gap.
    """
    import model

    src = "\n".join(p.read_text() for p in sorted(model.STRESS.glob("*.pure")))
    return re.sub(r"//[^\n]*", " ", src)


# Constructs a regex over the sources cannot decide, answered from the RESOLVED MODEL
# instead. Leaving them UNKNOWN was honest but useless: fourteen rows reported neither
# present nor absent, so the total could never close and nobody could tell which of the two
# they were. A predicate over what the reader actually built is the right instrument -- it
# knows an embedded hop from a property called `client`, which text cannot.
def _model_checks(c) -> dict:
    embedded = {k: v for k, v in c.embedded.items() if v not in c.json_backed}
    nested = [k for k, child in embedded.items()
              if any(o == child for o, _p in embedded)]
    return {
        "PM-27": bool(embedded),
        "PM-28": bool(nested),
        # An embedded block declaring its own ~primaryKey: the child class carries a main
        # table AND the parent does too, which only an embedded block with a key produces.
        "PM-29": any("~primaryKey" in b for b in _embedded_bodies()),
        "PM-02": bool(c.scope_columns) if hasattr(c, "scope_columns") else
                 any("scope(" in b for b in _class_bodies()),
        "PM-17": any("=" in b and ":" in b for b in _value_comparisons()),
        "PM-19": any(" and " in b or " or " in b for b in _value_comparisons()),
        "PUR-05": any(re.search(r"^\s*\+\w+\s*:\s*[\w:]+\s*\[[^\]]+\]\s*:", b, re.M)
                      for b in _pure_bodies()),
        "ENM-03": any(isinstance(v, str) for m in c.enum_maps.values() for v in m),
        # Source codes are stored as TEXT by the reader, so "is it an integer" is a
        # question about the SPELLING in the source, not about the Python type.
        "ENM-04": any(str(v).strip().lstrip("-").isdigit()
                      for m in c.enum_maps.values() for v in m),
        "ENM-05": any("::" in str(v) for m in c.enum_maps.values() for v in m),
        "ENM-06": any(len([x for x in m if x]) > 1 for m in c.enum_maps.values()),
        "P50": any(k.derived for fqn, k in c.classes.items() if False) or _assoc_qualified(),
        "SV-34": _nested_exec_env(),
        "P12": False,   # see BLOCKED: the documentation slot is rejected (F33)
    }


def _sections(pattern):
    import model
    for f in sorted(model.STRESS.glob("*.pure")):
        txt = re.sub(r"//[^\n]*", " ", f.read_text())
        for m in re.finditer(pattern, txt, re.S | re.M):
            yield m.group(0)


def _class_bodies():
    return _sections(r"^\s*\*?[\w:]+(?:\[\w+\])?\s*:\s*Relational\s*\{.*?\n\s*\}")


def _pure_bodies():
    return _sections(r"^\s*\*?[\w:]+(?:\[\w+\])?\s*:\s*Pure\s*\{.*?\n\s*\}")


def _embedded_bodies():
    return _sections(r"^\s*[a-z]\w*\s*\n\s*\(.*?\n\s*\)")


def _value_comparisons():
    """Property-mapping right-hand sides that are a COMPARISON rather than a column."""
    return _sections(r"^\s*\w+\s*:\s*\[[\w:]+\][\w.]+\s*(?:=|is\s+not\s+null|is\s+null)[^,\n]*")


def _assoc_qualified():
    return any(re.search(r"^\s*\w+\s*\([^)]*\)\s*$", b, re.M)
               for b in _sections(r"^Association\s+[\w:]+\s*\n\s*\{.*?\n\}"))


def _nested_exec_env():
    return any(re.search(r"^\s*\w+\s*:\s*\[\s*\n\s*\w+\s*:\s*\{", b, re.M)
               for b in _sections(r"ExecutionEnvironment[\s\S]*?\n\}"))


def score():
    import executed
    import model
    import quarantine
    import stacking

    rows = load_surface()
    text = corpus_text()
    c = model.load()
    q = set(quarantine.ENGINE_QUARANTINE) | set(quarantine.HANGS)
    prof = stacking.profile(c, executed.all_specs(c), q)
    # Constructs that appear in a passing service, and those that appear in one ALONGSIDE
    # another -- stacking.py keys by its own vocabulary, so this maps by name fragment.
    executed_names = {f for feats in prof.values() for f in feats}
    stacked_names = {f for feats in prof.values() if len(feats) > 1 for f in feats}

    checks = _model_checks(c)

    def status(row):
        if not row["detect"]:
            got = checks.get(row["id"])
            if got is None:
                return "UNKNOWN"
            if not got:
                return "absent"
            key = row["name"].lower()
            hit = next((f for f in executed_names
                        if f.split("  ")[-1].lower() in key), None)
            return ("stacked" if hit in stacked_names else "executed") if hit else "present"
        if not re.search(row["detect"], text, re.M):
            return "absent"
        key = row["name"].lower()
        hit = next((f for f in executed_names if f.split("  ")[-1].lower() in key
                    or key in f.split("  ")[-1].lower()), None)
        if hit is None:
            return "present"
        return "stacked" if hit in stacked_names else "executed"

    return [(r, status(r)) for r in rows]


def main() -> None:
    scored = score()
    by_dsl: dict[str, list] = {}
    for row, st in scored:
        by_dsl.setdefault(row["dsl"], []).append((row, st))

    print(f"ENGINE SURFACE: {len(scored)} constructs enumerated from the grammars\n")
    print(f"  {'dsl':<9} {'present':>8} {'absent':>8} {'unknown':>8}   of")
    for dsl, items in sorted(by_dsl.items()):
        pres = sum(1 for _r, s in items if s in ("present", "executed", "stacked"))
        absent = sum(1 for _r, s in items if s == "absent")
        unk = sum(1 for _r, s in items if s == "UNKNOWN")
        print(f"  {dsl:<9} {pres:>8} {absent:>8} {unk:>8}   {len(items)}")

    total_pres = sum(1 for _r, s in scored if s in ("present", "executed", "stacked"))
    total_abs = sum(1 for _r, s in scored if s == "absent")
    print(f"\n  {'TOTAL':<9} {total_pres:>8} {total_abs:>8} "
          f"{sum(1 for _r, s in scored if s == 'UNKNOWN'):>8}   {len(scored)}")

    if "--absent" in sys.argv:
        print("\nABSENT -- constructs the engine can express and this corpus never writes:")
        for dsl, items in sorted(by_dsl.items()):
            miss = [r for r, s in items if s == "absent"]
            if not miss:
                continue
            print(f"\n  [{dsl}] {len(miss)}")
            for r in miss:
                print(f"    {r['id']:<7} {r['name']:<38} {r['syntax'][:44]}")


def blocked() -> dict:
    """Constructs that cannot be closed from the corpus side, with the reason each."""
    f = SURFACE.parent / "SURFACE_BLOCKED.tsv"
    if not f.exists():
        return {}
    return dict(line.split("\t", 1) for line in f.read_text().splitlines()[1:] if "\t" in line)


def regressions() -> list[str]:
    """Absent constructs that are NOT on the blocked list.

    The ratchet is `absent is a subset of blocked`, not a count. A count can be held steady
    by closing one construct while another quietly falls out, and the whole point of the
    inventory is that each row is answerable individually.
    """
    allowed = blocked()
    return [f"{r['id']} {r['name']}" for r, s in score()
            if s == "absent" and r["id"] not in allowed]


if __name__ == "__main__":
    main()
    if "--gate" in sys.argv:
        gone = regressions()
        if gone:
            raise SystemExit(
                "\nconstructs absent and NOT recorded as blocked:\n  "
                + "\n  ".join(gone)
                + "\n\nEither write the construct, or record why it cannot be written in "
                  "docs/SURFACE_BLOCKED.tsv. Silence is the one option the inventory does "
                  "not offer.")
