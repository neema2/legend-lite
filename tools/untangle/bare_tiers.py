#!/usr/bin/env python3
"""Execution plan step 3 homework (2026-09-26): which tier of the bare-name rule served each
bare call in PURE SOURCE? Reads the BARE-TIER rows the shadow probe writes
(LL_SHADOW=1 over //spec:corpus_duckdb, //spec:corpus_h2 and //core:census; shadow.tsv in each
target's test.outputs) and classifies every bare name:

  CORE       every FQN with declarations is spelled by the core import group (tier 2)
  FORM       ... or by a form's owned declarations (tier 3), none by the engine surface alone
  ENGINE-ONLY at least one FQN with declarations is spelled by the engine surface (tier 1) and
             by no other tier — under the reference's rule (imports ∪ core group ∪ root) this
             call would NOT see that declaration: a missing declaration to add to a core
             package, or a call the reference resolves differently. Named, one line each.

usage: bare_tiers.py <shadow.tsv>... [--out <tsv>]
"""
import collections, sys

paths = [a for a in sys.argv[1:] if not a.startswith("--")]
out = None
if "--out" in sys.argv:
    out = sys.argv[sys.argv.index("--out") + 1]
    paths = [p for p in paths if p != out]

# name -> fqn -> {tier}, and name -> fqn -> {site}
tiers = collections.defaultdict(lambda: collections.defaultdict(set))
sites = collections.defaultdict(lambda: collections.defaultdict(set))
witness = {}
rows = 0
for p in paths:
    with open(p, encoding="utf-8") as f:
        for line in f:
            if not line.startswith("BARE-TIER\t"):
                continue
            rows += 1
            parts = line.rstrip("\n").split("\t")
            _, name, fqn, tier, site = parts[:5]
            tiers[name][fqn].add(tier)
            sites[name][fqn].add(site)
            witness.setdefault((name, fqn), parts[5] if len(parts) > 5 else "")

classes = collections.Counter()
lines = ["name\tclass\tfqn\ttiers\tsites\twitness"]
engine_only = []
for name in sorted(tiers):
    cls = "CORE"
    per = tiers[name]
    if any(t == {"ENGINE"} for t in per.values()):
        cls = "ENGINE-ONLY"
    elif not any("CORE" in t for t in per.values()):
        cls = "FORM"
    classes[cls] += 1
    for fqn in sorted(per):
        lines.append("\t".join([name, cls, fqn, "|".join(sorted(per[fqn])),
                                "|".join(sorted(sites[name][fqn])), witness[(name, fqn)]]))
        if per[fqn] == {"ENGINE"}:
            engine_only.append((name, fqn, witness[(name, fqn)]))

print(f"rows={rows} files={len(paths)} bare names={len(tiers)} "
      + " ".join(f"{k}={v}" for k, v in sorted(classes.items())))
print("ENGINE-ONLY (name, fqn, first witness):")
for name, fqn, w in engine_only:
    print(f"  {name}\t{fqn}\t{w}")
if out:
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print("wrote", out)
