"""
The FUNCTION burndown: what the oracle can evaluate, against what the engine can execute.

docs/ENGINE_FUNCTIONS.tsv is derived from `getSupportedFunctions()` in pureToSQLQuery.pure --
the map the engine consults before it emits "No SQL translation exists for the PURE function".
It is generated, not transcribed: the list is too long to copy without error and too important
to get wrong.

Three counts, and they are not interchangeable:

    427   entries in the registry, overloads included
    292   (package, name) pairs
    262   DISTINCT names

30 names appear under more than one package -- `filter` and `size` in both collection and
relation, `contains` and `and` and `or` in both collection and scalar. The per-family rows
below count PAIRS, because a collection `filter` and a relation `filter` are different
functions and closing one does not close the other. The totals count DISTINCT names, because
summing the family rows counts a shared name once per family and inflates the result. An
earlier version summed the rows and reported 234 implemented where 205 names were; the
scoreboard was flattering itself by exactly the amount the families overlap.

A function is IMPLEMENTED here when the oracle computes its result independently of
legend-engine. That is the whole bar, and it is deliberately high. Reading the engine's
lowering and reproducing it would make every assertion circular -- the corpus would agree
with the engine by construction and could never disagree with it, which is the only thing it
exists to do.

So each entry lands in one of three states, and the middle one is not a failure:

    IMPLEMENTED   the oracle has an independent implementation and states its NULL rule
    REFUSED       the oracle raises Unsupported, with a reason
    ABSENT        neither -- nobody has looked at it yet

and a fourth column, which is the one that means something:

    EXECUTED      the function RAN against the engine and agreed with the oracle

An implementation with no executing test proves nothing. 234 of these were implemented in an
afternoon and 76 of them had ever run; the gap is not a rounding error, it is the difference
between a scoreboard that measures the corpus and one that measures my typing. The evidence
comes from docs/FUNCTIONS_EXECUTED.tsv, which probe_functions.py WRITES rather than anyone
maintaining -- a hand-kept list of "functions we have tested" drifts, and drifts flatteringly.

Note what does not count. A function that ran and disagreed is recorded but not counted, or
the scoreboard would improve every time the corpus found a bug.

REFUSED is a real answer. `concat` taught the lesson: its behaviour over NULL is decided by
the DIALECT, not by the function, so an implementation written from what concat "means" was
wrong and the corpus asserted it confidently. Some of these 292 cannot be implemented
honestly without picking a dialect, and saying so beats guessing.

Not every function is reachable from a mapping either. The registry mixes scalar functions
usable in a property expression with TDS and Relation operations that are query SHAPES, with
collection operations that are navigation, with 30 calendar aggregations that need a business
calendar to mean anything. The scoreboard reports by family so the shape of the remaining
work is visible rather than a single discouraging number.
"""
from __future__ import annotations

import os
import sys
from collections import Counter
from pathlib import Path

# the repository root as model.py takes it: CORPUS_ROOT under Bazel (declared inputs only), never __file__, which
# follows the runfiles symlink back into the checkout
ROOT = Path(os.environ["CORPUS_ROOT"]) if os.environ.get("CORPUS_ROOT") else Path(__file__).resolve().parents[2]
INVENTORY = ROOT / "docs/ENGINE_FUNCTIONS.tsv"

# Families, in the order the burndown works them. The split is by what a function IS, not by
# package: a scalar usable in a mapping expression is a different kind of work from a TDS
# operation that changes the shape of a result.
FAMILY = {
    "meta::pure::functions::math": "scalar",
    "meta::pure::functions::string": "scalar",
    "meta::pure::functions::boolean": "scalar",
    "meta::pure::functions::date": "scalar-date",
    "meta::pure::functions::date::calendar": "calendar",
    "meta::pure::functions::collection": "collection",
    "meta::pure::functions::lang": "lang",
    "meta::pure::functions::multiplicity": "lang",
    "meta::pure::functions::meta": "lang",
    "meta::pure::functions::relation": "relation",
    "meta::pure::tds": "tds",
    "meta::pure::tds::extensions": "tds",
    "meta::pure::functions::variant::convert": "variant",
    "meta::pure::functions::hash": "scalar",
    "meta::pure::functions::runtime": "scalar",
    "meta::core::runtime": "scalar",
    "meta::pure::graphFetch::execution": "graphfetch",
    "meta::pure::mutation": "mutation",
    "meta::relational::functions": "relational-native",
}


def inventory() -> list[tuple[str, str, int]]:
    rows = []
    for line in INVENTORY.read_text().splitlines()[1:]:
        pkg, name, n = line.split("\t")
        rows.append((pkg, name, int(n)))
    return rows


def family(pkg: str) -> str:
    return FAMILY.get(pkg, "other")


# The two registries are consulted PER FAMILY, never merged. `filter`, `sort`, `distinct`
# and `size` exist in both the collection and relation families with different signatures,
# so a merged lookup would let one implementation mark the other done -- the same flattering
# every scoreboard here has had to be rescued from at least once.
_RELATION_FAMILIES = {"relation", "tds"}


def implemented(fam: str | None = None) -> set[str]:
    """Names the oracle evaluates, read FROM the oracle rather than listed here, so the two
    cannot drift: a function deleted from a registry disappears from the count."""
    import oracle
    if fam in _RELATION_FAMILIES:
        return set(oracle.RELATION_IMPLEMENTED)
    return set(oracle.IMPLEMENTED)


def refused(fam: str | None = None) -> dict[str, str]:
    import oracle
    if fam in _RELATION_FAMILIES:
        return dict(oracle.RELATION_REFUSED)
    return dict(oracle.REFUSED)


EVIDENCE = ROOT / "docs/FUNCTIONS_EXECUTED.tsv"


# Which families each probe's verdicts apply to. Evidence is per (probe, name), and a probe
# only speaks for the families it can actually reach: the relation probe running `size` says
# nothing about the COLLECTION `size`, which is a different function that happens to share a
# name. Without this the two overwrite each other and the scoreboard moves by a dozen
# functions depending on which probe ran last.
PROBE_FAMILIES = {
    "scalar": {"scalar", "scalar-date", "lang", "variant", "other", "calendar"},
    "aggregate": {"scalar", "collection", "other"},
    "relation": {"relation"},
    "tds": {"tds"},
    "collection": {"collection"},
    # The long-tail probe reaches names scattered across every family, so it speaks for all
    # of them -- it is defined by what the other probes cannot reach, not by a family.
    "remaining": {"collection", "lang", "other", "tds", "variant", "scalar", "scalar-date",
                  "relation"},
}


def _counts(evidence: str | None) -> bool:
    """Whether an evidence value means the function ran AND agreed.

    Defined by EXCLUSION -- anything that is not a recorded refusal or disagreement counts --
    so a new probe contributes without editing this list. The whitelist version silently
    ignored the aggregate probe's 17 functions because it named its evidence differently, and
    a scoreboard that quietly drops evidence is worse than one that has none.
    """
    return evidence == "ok"


def evidence_rows() -> list[tuple[str, str, str]]:
    """(function, probe, verdict) as recorded by the probes themselves."""
    if not EVIDENCE.exists():
        return []
    return [tuple(line.split("\t")) for line in EVIDENCE.read_text().splitlines()[1:]
            if line.count("\t") == 2]


def executed() -> dict[tuple[str, str], str]:
    """(function, family) -> verdict, expanding each probe's rows to the families it covers."""
    out = {}
    for name, probe, verdict in evidence_rows():
        for fam in PROBE_FAMILIES.get(probe, ()):
            # An "ok" from any probe covering this family wins over a failure from another.
            if out.get((name, fam)) != "ok":
                out[(name, fam)] = verdict
    return out


def report():
    rows = inventory()
    by_family: dict[str, list] = {}
    for pkg, name, n in rows:
        by_family.setdefault(family(pkg), []).append(name)

    distinct = {name for _p, name, _n in rows}
    print(f"ENGINE FUNCTION REGISTRY: {len(distinct)} distinct names, {len(rows)} "
          f"(package, name) pairs, {sum(n for _p, _n, n in rows)} entries with overloads\n")
    ev = executed()
    print(f"  {'family':<18} {'impl':>5} {'refused':>8} {'absent':>7} {'EXEC':>6}   of")
    tot_i = tot_r = tot_e = 0
    for fam, names in sorted(by_family.items()):
        impl, refu = implemented(fam), refused(fam)
        i = sum(1 for x in names if x in impl)
        r = sum(1 for x in names if x in refu)
        e = sum(1 for x in names if _counts(ev.get((x, fam))))
        tot_i += i
        tot_r += r
        tot_e += e
        print(f"  {fam:<18} {i:>5} {r:>8} {len(names) - i - r:>7} {e:>6}   {len(names)}")
    # Totals over DISTINCT names, not the sum of the rows above. A name shared by two
    # families has one implementation and must not be counted twice.
    fam_of: dict[str, set] = {}
    for pkg, name, _n in rows:
        fam_of.setdefault(name, set()).add(family(pkg))
    d_i = sum(1 for n in distinct if any(n in implemented(f) for f in fam_of[n]))
    d_r = sum(1 for n in distinct
              if not any(n in implemented(f) for f in fam_of[n])
              and any(n in refused(f) for f in fam_of[n]))
    d_e = sum(1 for n in distinct
              if any(_counts(ev.get((n, f))) for f in fam_of[n]))
    print(f"  {'':<18} {'-' * 5} {'-' * 8} {'-' * 7} {'-' * 6}")
    print(f"  {'DISTINCT NAMES':<18} {d_i:>5} {d_r:>8} "
          f"{len(distinct) - d_i - d_r:>7} {d_e:>6}   {len(distinct)}")
    # The burndown's real terminal condition. "Executed" cannot reach 205, because some
    # functions the oracle implements are ones the ENGINE refuses -- and a refusal recorded
    # with its message is a finished answer, not an outstanding task. What has to reach zero
    # is the third number: names nobody has ever put in front of the engine.
    ran = {n for n, _p, _v in evidence_rows()}
    never = sorted(n for n in distinct
                   if any(n in implemented(f) for f in fam_of[n]) and n not in ran)
    print(f"\n  {d_e} executed and agreeing")
    print(f"  {d_i - d_e - len(never)} run and refused or disagreeing -- each recorded with "
          f"the engine's own message")
    print(f"  {len(never)} never run against the engine"
          + ("" if never else "   <- the burndown's terminal condition"))

    if "--absent" in sys.argv:
        fam_want = next((a.split("=")[1] for a in sys.argv if a.startswith("--family=")), None)
        print("\nABSENT -- no independent implementation and no stated refusal:")
        for fam, names in sorted(by_family.items()):
            if fam_want and fam != fam_want:
                continue
            impl, refu = implemented(fam), refused(fam)
            missing = sorted(x for x in names if x not in impl and x not in refu)
            if missing:
                print(f"\n  [{fam}] {len(missing)}")
                for i in range(0, len(missing), 6):
                    print("    " + "  ".join(f"{m:<20}" for m in missing[i:i + 6]))

    if "--overloads" in sys.argv:
        ok, total, thin = overloads()
        print(f"\nOVERLOADS: {sum(c for _p, _n, c in rows)} registry entries over "
              f"{len(distinct)} names; {total} names carry more than one.")
        print(f"  {ok} of {total} are served at two or more argument shapes.")
        # The synthetic shapes are a COARSE check and this says so. An operation with a
        # specific signature -- groupBy(rows, cols, aggs) -- will not be satisfied by a
        # generic probe shape, and counting that as thin measures the audit. The real
        # evidence is the probes, which call these at their true signatures, so the two are
        # reported side by side and only the residue is a genuine question.
        import functions as _self
        ev_ok = {n for n, _pr, v in _self.evidence_rows() if v == "ok"}
        real = [n for n, _k in thin if n in ev_ok]
        rest = [n for n, _k in thin if n not in ev_ok]
        if real:
            print(f"  {len(real)} of those are exercised at their REAL signature by a probe, "
                  f"which the\n  synthetic shapes cannot reach: " + ", ".join(real))
        if rest:
            print(f"  {len(rest)} are served at one shape and not probed at another: "
                  + ", ".join(rest))

    if "--unexecuted" in sys.argv:
        # Split, because the two halves are not the same kind of work. A function the engine
        # REFUSED or that DISAGREED has been run and written up; a function nobody has tried
        # is the actual remaining task. Reporting them as one number reads as 73 untouched
        # when a ninth of them are findings with repro directories.
        tried = {f"{n} [{fam}]": v for (n, fam), v in ev.items() if v and not _counts(v)}
        print("\nRUN, AND DID NOT AGREE -- each is a recorded finding, not a gap:")
        for name, why in sorted(tried.items()):
            print(f"    {name:<24} {why}")
        print("\nNEVER RUN AGAINST THE ENGINE:")
        for fam, names in sorted(by_family.items()):
            impl = implemented(fam)
            gap = sorted(x for x in names if x in impl
                         and ev.get((x, fam)) is None)
            if gap:
                print(f"\n  [{fam}] {len(gap)}")
                for i in range(0, len(gap), 6):
                    print("    " + "  ".join(f"{m:<20}" for m in gap[i:i + 6]))

    if "--refused" in sys.argv:
        print("\nREFUSED -- deliberately not implemented, with the reason:")
        allrefu = {**refused(), **refused("relation")}
        for name, why in sorted(allrefu.items()):
            print(f"  {name:<24} {why}")


def overloads() -> tuple[int, int, list]:
    """How many multi-overload names the oracle serves at more than one shape.

    The registry has 427 entries over 262 names, and most of the difference is TYPE overloads
    -- six spellings of `max`, four of `abs` -- which a Python implementation serves with one
    body because it dispatches on the value. The ones that matter are ARITY overloads, where
    a trailing optional argument changes the answer: `variance(xs, isSample)`, `between` with
    and without its bounds, `timeBucket`'s unit.

    So this feeds each implementation several shapes and counts how many it answers, where a
    STATED refusal of an input region counts as an answer -- refusing `sqrt` of a negative is
    serving the call, not failing it. Relation and TDS names get row sets; everything else
    gets scalars, because handing a string to `groupBy` measures the probe rather than the
    function, and the first version of this audit did exactly that and reported seventeen
    healthy implementations as thin.
    """
    import oracle

    rows = inventory()
    fam_of: dict[str, set] = {}
    for pkg, name, _c in rows:
        fam_of.setdefault(name, set()).add(family(pkg))
    multi = sorted({n for _p, n, c in rows if c > 1})

    scalar_shapes = [["alpha"], [7, 3], ["2024-06-03", "x"], [7, 3, 1], [[1, 2, 3]]]
    table = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    rel_shapes = [(table,), (table, 1), (table, ["a"]), (table, "a", "b")]

    served, thin = [], []
    for n in multi:
        is_rel = bool(fam_of[n] & {"relation", "tds"})
        reg = oracle.RELATION_IMPL if is_rel else oracle.IMPL
        if n not in reg:
            continue
        ok = 0
        for shape in (rel_shapes if is_rel else scalar_shapes):
            try:
                reg[n](*shape) if is_rel else reg[n](shape)
                ok += 1
            except oracle.Unsupported:
                ok += 1
            except Exception:
                pass
        (served if ok >= 2 else thin).append((n, ok))
    return len(served), len(served) + len(thin), thin


def never_run() -> list[str]:
    """Implemented names with no evidence of ever having been run against the engine."""
    rows = inventory()
    distinct = {name for _p, name, _n in rows}
    fam_of: dict[str, set] = {}
    for pkg, name, _n in rows:
        fam_of.setdefault(name, set()).add(family(pkg))
    ran = {n for n, _p, _v in evidence_rows()}
    return sorted(n for n in distinct
                  if any(n in implemented(f) for f in fam_of[n]) and n not in ran)


if __name__ == "__main__":
    report()
    if "--gate" in sys.argv:
        # The ratchet is "nothing implemented is unrun", not a count. A count can be held
        # steady by running one function while another falls out, and the whole point of the
        # inventory is that every name is answerable individually.
        gone = never_run()
        if gone:
            raise SystemExit(
                f"\n{len(gone)} implemented functions have never been run against the "
                f"engine:\n  " + "\n  ".join(gone)
                + "\n\nAdd a case to whichever probe can reach it. A refusal recorded with "
                  "the engine's\nmessage counts; silence does not.")
