#!/usr/bin/env python3
"""Execution plan step 3, the probe push (2026-09-27, program-audit-2026-09-27.md §E): the counts
the step 3 design owes before 3a/3b/3c, read from the shadow probe's rows (LL_SHADOW=1 over the
corpus lanes and the census; shadow.tsv in each target's test.outputs).

Rows summarised (kind, columns after the kind, then the witness):
  RESOLVER-TIER  position name fqn tier detail     own-package hits; core-group first-match with
                                                   the competing hits (which hits are functions is
                                                   read by hand: the reference keeps kinds apart)
  RETRY-ACCEPT   name firstRanked accepted index   the deferred-argument loop accepted a
                                                   non-first candidate (silent-survival bound)
  LIFTED         site name fqn n                   a qualified property / row getter resolved
                                                   among n lifted overloads; site carries :dot/:call
  LITERAL-MULT   n lo hi                           a collection literal whose bound sum != [n]
  UNKNOWN-FN     name dot|call site                a call that failed with no candidate at all
  BARE-CALL      name parsed|minted dot|call infix a call the typer asked about with no resolver
                                                   candidates on the node
  LITE-INTERNAL  fqn id                            catalog natives a user may not name
  CANDIDATES     name n ids source                 (existing) the candidate set per spelled name

usage: probe_counts.py <shadow.tsv>...
"""
import collections
import sys

paths = [a for a in sys.argv[1:] if not a.startswith("--")]
rows = collections.defaultdict(list)
for p in paths:
    with open(p, encoding="utf-8") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            rows[parts[0]].append(parts[1:])


def distinct(kind, key):
    return sorted({key(r) for r in rows.get(kind, [])})


def show(title, items, limit=60):
    print(f"\n== {title}: {len(items)}")
    for it in items[:limit]:
        print("  " + "\t".join(it if isinstance(it, (list, tuple)) else (it,)))
    if len(items) > limit:
        print(f"  … {len(items) - limit} more")


print(f"files={len(paths)} kinds=" + " ".join(f"{k}={len(v)}" for k, v in sorted(rows.items())))

# the core import group, read from the resolver's source (the generated list), so an own-package
# hit inside a core-group package — which the reference reaches through the group anyway — is told
# apart from a hit the reference would need an import for
import os
import re
core_imports = set()
src = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..",
                   "core", "src", "main", "java", "com", "legend", "compiler", "CoreImports.java")
try:
    text = open(src, encoding="utf-8").read()
    block = text[text.index("SEQUENCE = List.of("):]
    block = block[:block.index(");")]
    core_imports = set(re.findall(r'"([^"]+)"', block))
except (OSError, ValueError):
    pass
print(f"core import group read from CoreImports.java: {len(core_imports)} packages")


def in_core_group(fqn):
    return fqn.rsplit("::", 1)[0] in core_imports


# 1. own-package hits, by position, split by whether the package is in the core group
own = [r for r in rows.get("RESOLVER-TIER", []) if r[3] == "own-package"]
for pos in ("call", "type"):
    hits = sorted({(r[1], r[2], r[4]) for r in own if r[0] == pos})
    show(f"own-package hits, {pos} position, package NOT in the core group (name, fqn, detail)",
         [h for h in hits if not in_core_group(h[1])])
    print(f"  (own-package hits inside a core-group package, which the reference reaches through the"
          f" group: {len([h for h in hits if in_core_group(h[1])])})")

# 2. core first-match with more than one core hit, by position
core = [r for r in rows.get("RESOLVER-TIER", []) if r[3] == "core-first-match"]
for pos in ("call", "type"):
    show(f"core-group first-match with competing hits, {pos} position (name, taken, all hits)",
         sorted({(r[1], r[2], r[4]) for r in core if r[0] == pos}))

# 3. silent-survival upper bound
show("RETRY-ACCEPT: a non-first candidate accepted (name, first ranked, accepted, index)",
     distinct("RETRY-ACCEPT", lambda r: (r[0], r[1], r[2], r[3])))

# 4. qualified properties / row getters with more than one lifted overload
lifted = rows.get("LIFTED", [])
multi = sorted({(r[0], r[1], r[2], r[3]) for r in lifted if int(r[3]) > 1})
show("LIFTED with n > 1 (site, name, fqn, n)", multi)
by_site = collections.Counter((r[0]) for r in lifted)
print("  all LIFTED rows by site: " + ", ".join(f"{k}={v}" for k, v in sorted(by_site.items())))
arrow = sorted({(r[0], r[1]) for r in lifted if r[0].endswith(":call")})
show("qualified properties reached from the ARROW/prefix spelling (site, name) — the reference routes only the dot spelling", arrow)

# 5. literals whose sum != size
show("LITERAL-MULT (n, lo, hi) distinct shapes", distinct("LITERAL-MULT", lambda r: (r[0], r[1], r[2])))

# 6. lite-internal catalog partition by package
lite = rows.get("LITE-INTERNAL", [])
pk = collections.Counter(r[0].rsplit("::", 1)[0] for r in {tuple(x[:2]) for x in lite})
show("LITE-INTERNAL natives by package (package, count)", [(k, str(v)) for k, v in sorted(pk.items())])

# 7. unknown functions
unk = rows.get("UNKNOWN-FN", [])
show("UNKNOWN-FN (name, spelling, site)", sorted({(r[0], r[1], r[2]) for r in unk}))

# 8/9. calls with no candidates on the node, by producer; a name with "::" is a single match the
# resolver REWROTE to its FQN (or a user-qualified spelling) — only a name without "::" reached the
# typer bare
bare = rows.get("BARE-CALL", [])
distinct_bare = {tuple(x[:4]) for x in bare}
prod = collections.Counter((r[1], r[2], "qualified" if "::" in r[0] else "bare") for r in distinct_bare)
print("\n== BARE-CALL distinct (name, producer, spelling, infix) by (producer, spelling, qualified|bare): "
      + ", ".join(f"{k[0]}/{k[1]}/{k[2]}={v}" for k, v in sorted(prod.items())))
show("BARE-CALL parsed and truly bare (a user- or parser-spelled call the resolver left unresolved):"
     " name, spelling, infix",
     sorted({(r[0], r[2], r[3]) for r in distinct_bare if r[1] == "parsed" and "::" not in r[0]}), 200)
show("BARE-CALL minted and truly bare (a compiler mint by bare name): name, spelling",
     sorted({(r[0], r[2]) for r in distinct_bare if r[1] == "minted" and "::" not in r[0]}), 200)
