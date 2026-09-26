#!/usr/bin/env python3
"""Join the reference's resolutions (RefResolutions.java) with ours (OurResolutionsTest),
CALL BY CALL: the key is (source id, line, column) — the call-name token's position, which both
dumps print — so overloads of an enclosing function never merge, a call one side elides is
counted as an elision, and a declaration that exists on one side only (the jar is 4.138.5, our
pin 4.145.0) is counted as drift, not as a disagreement. Before 2026-09-26 the join keyed on
(enclosing function NAME, spelling) and compared sets, which produced 5 of the 28 "package"
rows out of thin air (execution plan step 1).

usage: join.py ref-resolutions.tsv our-resolutions-<module>.txt
               [--source-drift source-drift.tsv] [--elide FQN ...]

Columns (both files, tab-separated, header row):
  reference: sourceId line column spelling resolvedFqn resolvedId enclosingFqn enclosingId
  ours:      sourceId line column kind resolvedFqn resolvedId enclosingFqn enclosingId
             (kind = CALL with a position; NODE with none; FAILED for a body we could not type)

Sections printed:
  1. functions: how many bodies each side typed, and the compile-status differential
     (bodies the reference typed that we FAILED or never saw, and vice versa)
  2. calls, by position: AGREE / OVERLOAD (same declaration name, other overload) / PACKAGE
     (other declaration) / DRIFT (the reference's or our declaration is absent on the other
     side) / SOURCE_DRIFT (the file's text differs between the jar and our pinned tree, so the
     position means nothing) / ELIDED (a declared elision) / ABSENT (no call of ours at that
     position: a form node, a property read, a call we do not emit) / PROPERTY_AS_CALL (the
     reference has a member read where we have a call) / EXTRA (a call of ours the reference
     has no call for)
  3. the disagreement pairs, most frequent first, with one example position each

Two spellings do not join by column alone:
  - an INFIX OPERATOR RUN (a + b + c; ==, &&, ||, !, <, …) is one call on both sides, but the
    reference positions it at the LAST operator token and our parser keeps the engine grammar's
    span convention (the parity test pins it). An operator row joins by exact column when the
    same operator sits there, else with the next unmatched row of the same operator on the same
    line, in column order. Runs that one side splits differently (a mixed `a + b - c`) stay
    ABSENT/EXTRA under `plus`/`minus`: an artefact of the two conventions, not a resolution;
  - a PROPERTY READ ($x.name, $r.qualified(...)) is a member on the reference's side (spelling
    "null") and, for a qualified property we model as a function, a CALL on ours: reported as
    PROPERTY_AS_CALL by our callee, which is step A4's inventory, not an overload disagreement.
"""
import argparse, collections

OPERATORS = {"plus", "minus", "times", "divide", "equal", "and", "or", "not",
             "lessThan", "lessThanEqual", "greaterThan", "greaterThanEqual"}

ap = argparse.ArgumentParser()
ap.add_argument("ref")
ap.add_argument("ours")
ap.add_argument("--elide", action="append", default=[],
                help="declaration FQN (no signature) whose calls we deliberately do not emit")
ap.add_argument("--source-drift", help="source_drift.py output: positions in a DIFFERS/MISSING source are SOURCE_DRIFT")
a = ap.parse_args()

drifted = set()
if a.source_drift:
    with open(a.source_drift, encoding="utf-8") as f:
        f.readline()
        for line in f:
            sid, status = line.rstrip("\n").split("\t")
            if status != "SAME":
                drifted.add(sid)

def rows(path):
    with open(path, encoding="utf-8") as f:
        hdr = f.readline().rstrip("\n").split("\t")
        for line in f:
            c = line.rstrip("\n").split("\t")
            if len(c) < len(hdr):
                c += [""] * (len(hdr) - len(c))
            yield dict(zip(hdr, c))

def simple(fqn):
    return fqn.split("::")[-1]

# reference
ref_calls = {}      # (sid, line, col) -> (spelling, resolvedFqn, resolvedId, enclosingFqn)
ref_props = {}      # (sid, line, col) -> (property, enclosingFqn)
ref_fns, ref_decls = set(), set()
for r in rows(a.ref):
    ref_fns.add(r["enclosingFqn"])
    key = (r["sourceId"], r["line"], r["column"])
    if r["spelling"] == "null":
        ref_props[key] = (r["resolvedFqn"], r["enclosingFqn"])
        continue
    if not r["resolvedId"]:
        continue
    ref_decls.add(r["resolvedId"])
    ref_calls[key] = (r["spelling"], r["resolvedFqn"], r["resolvedId"], r["enclosingFqn"])

# ours
our_calls = {}      # (sid, line, col) -> (resolvedFqn, resolvedId, enclosingFqn)
our_ops = collections.defaultdict(list)   # (sid, line, op) -> [col, ...] in column order
our_fns, our_failed, our_decls = set(), set(), set()
for r in rows(a.ours):
    our_fns.add(r["enclosingFqn"])
    if r["kind"] == "FAILED":
        our_failed.add(r["enclosingFqn"])
    elif r["kind"] == "CALL" and r["line"]:
        our_decls.add(r["resolvedId"])
        key = (r["sourceId"], r["line"], r["column"])
        our_calls[key] = (r["resolvedFqn"], r["resolvedId"], r["enclosingFqn"])
        sp = simple(r["resolvedFqn"])
        if sp in OPERATORS:
            our_ops[(r["sourceId"], r["line"], sp)].append(r["column"])
for lst in our_ops.values():
    lst.sort(key=int)

elide = set(a.elide)
print("== 1. functions")
print("reference typed %d function names; ours %d (failed %d)" % (len(ref_fns), len(our_fns), len(our_failed)))
both = ref_fns & our_fns
print("in both: %d | reference only (we never saw the body): %d | ours only: %d"
      % (len(both), len(ref_fns - our_fns), len(our_fns - ref_fns)))
print("reference typed, we FAILED: %d" % len(ref_fns & our_failed))

print("\n== 2. calls, by position (reference calls whose enclosing function we typed without failure)")
kinds = collections.Counter()
absent = collections.Counter()
dis = collections.Counter()
example = {}
matched_ours = set()

def compare(key, sp, rfqn, rid, partner):
    matched_ours.add(partner)
    ofqn, oid, _ = our_calls[partner]
    if oid == rid:
        kinds["AGREE"] += 1
    elif rid not in our_decls and oid not in ref_decls:
        kinds["DRIFT"] += 1
    else:
        kind = "OVERLOAD" if ofqn == rfqn else "PACKAGE"
        kinds[kind] += 1
        k = (kind, sp, rid, oid)
        dis[k] += 1
        example.setdefault(k, "%s:%s:%s" % key)

# pass 1: exact column
pending_ops = []
for key, (sp, rfqn, rid, enc) in ref_calls.items():
    if enc not in both or enc in our_failed:
        continue
    if key[0] in drifted:
        kinds["SOURCE_DRIFT"] += 1
        continue
    # an exact-column partner is taken unless one side is an operator and the
    # other a different spelling: a `/` of the reference's and a `*` of ours can
    # share a column under the two span conventions and are not the same call
    if key in our_calls and not (
            (simple(sp) in OPERATORS or simple(our_calls[key][0]) in OPERATORS)
            and simple(our_calls[key][0]) != simple(sp)):
        compare(key, sp, rfqn, rid, key)
    elif simple(sp) in OPERATORS:
        pending_ops.append(key)
    elif rfqn in elide:
        kinds["ELIDED"] += 1
    else:
        kinds["ABSENT"] += 1
        absent[sp] += 1

# pass 2: operators without an exact column partner: the next unmatched same-op row on the line
for key in pending_ops:
    sp, rfqn, rid, enc = ref_calls[key]
    sid, line, _ = key
    partner = next(((sid, line, col) for col in our_ops.get((sid, line, simple(sp)), [])
                    if (sid, line, col) not in matched_ours), None)
    if partner is None:
        kinds["ABSENT"] += 1
        absent[sp] += 1
    else:
        compare(key, sp, rfqn, rid, partner)

extra = collections.Counter()
prop_as_call = collections.Counter()
for key, (ofqn, _, enc) in our_calls.items():
    if key in matched_ours or enc not in both or enc in our_failed or key[0] in drifted:
        continue
    if key in ref_props:
        kinds["PROPERTY_AS_CALL"] += 1
        prop_as_call[ofqn] += 1
    else:
        kinds["EXTRA"] += 1
        extra[ofqn] += 1
for k in ("AGREE", "OVERLOAD", "PACKAGE", "DRIFT", "SOURCE_DRIFT", "ELIDED", "ABSENT", "PROPERTY_AS_CALL", "EXTRA"):
    print("%-16s %d" % (k, kinds[k]))
print("\nPROPERTY_AS_CALL by our callee (top 15; a qualified property we model as a function — step A4):")
for fq, n in prop_as_call.most_common(15):
    print("  %6d  %s" % (n, fq))
print("\nABSENT by reference spelling (top 30):")
for sp, n in absent.most_common(30):
    print("  %6d  %s" % (n, sp))
print("\nEXTRA by our callee (top 30):")
for fq, n in extra.most_common(30):
    print("  %6d  %s" % (n, fq))

print("\n== 3. disagreements (kind, spelling, reference id, our id, count, example)")
for (kind, sp, rid, oid), n in dis.most_common():
    print("%s\t%s\t%s\t%s\t%d\t%s" % (kind, sp, rid, oid, n, example[(kind, sp, rid, oid)]))
