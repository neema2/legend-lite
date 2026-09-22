// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.Repo;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The CARRIER PURITY RATCHET (CARRIER_REDESIGN.md tenet #1, R0): the
 * pre-dialect layers (lowering + resolver + plan) must not construct
 * backend-DATA-MODEL idioms — list/array values and their functions,
 * UNNEST placements. Each rung of the redesign moves a family into the
 * semantic-node + strategy-pass world and DELETES the direct emission;
 * the pins below only shrink, then freeze at ZERO. A count above a pin
 * is silent scope creep and fails the build; a count below means the
 * rung landed — tighten the pin in the SAME commit.
 *
 * <p>SqlFn entries are SEMANTIC vocabulary (Spellings maps them) —
 * the violation class is the backend data-model idiom, per the doc's
 * §4 definition. LIST_* / UNNEST SqlFn references upstream are counted
 * because those functions presuppose the list carrier itself.
 */
class   CarrierPurityRatchetTest {

    /** Pattern -> R0 census pin (2026-08-01, main @ 85ff6c8a; raw
     * occurrences, comment mentions included — the instrument's own
     * counting semantics). */
    private static final Map<String, Integer> PINS = Map.of(
            // 34→36 (2026-08-19): ListEncodings.map's singleton-wrap and
            // empty-list spellings — the map SEMANTIC NODE's wire-shape
            // rule (scalar sources wrap; [0..1] empties stay empty),
            // not new ad-hoc idioms. 37→36 (2026-08-19 Clause-2c
            // redesign): equalityWireOperand's dual wrap DELETED —
            // verdicts moved to World 1. 36→37 (slice 9): the static-
            // pivot IN-membership pre-filter — the ENGINE's own
            // row-restriction semantics, a semantic emission.
            // 37→38 (2026-08-21 stamp C1): the collection-mapper cell
            // RE-BOX — scalar-STAMPED cells wrap [cell] so the flatten
            // contract holds once singleton literals lower as their
            // element (the box moved from the literal to the ONE
            // consumer whose contract needs it; net honest).
            // 38→34 (burn slice 8, ListShapes dissolution): the dead
            // listArg/definitelyScalar wraps and the shape-decided
            // ArrayLit escapes are GONE; wrap helpers are stamp-read.
            // 34→35 (multiplicity audit slice 3): toOneMany over a
            // to-one operand RE-CARRIERS to the list its [1..*] stamp
            // promises downstream — a designed one-value carrier at the
            // stamp seam, not a shape guess.
            // 35→36 (audit slice 4): distinct's to-one guard — the
            // same one-element re-carrier its synonym removeDuplicates
            // always had (a [0..1] value hit the list-lambda binder).
            // 36→38 (2026-08-22 X5): the keyed-instance canon's TWO
            // empty-list normalizers (COALESCE(list_transform(...), []) —
            // NULL and empty are both the EMPTY key collection, engine
            // equal([],[]) TRUE). Verdict-lane only, same F3/F10
            // migration note as the LIST_ row above.
            // 135→141 (2026-08-30 TDG lane S1): ListEncodings.lowerSortBy
            // — the sortBy SEMANTIC NODE's wire-shape rule (the stable
            // {k,i,v} struct-sort, mirroring the pinned 3-arg native sort
            // rule), same class as the map rule's row above: a semantic
            // node's own carrier spellings, not new ad-hoc idioms.
            // 38→39 (2026-08-23 F10 slice 2): LiteralSpelling.
            // mixedNumericArray — the kind-faithful carrier REBUILDS a
            // mixed-numeric literal collection as spelling texts (the
            // DOUBLE-promoted array erased Integer identity); a
            // designed carrier at the label seam, absorbed when
            // collections move behind their semantic node.
            // 39→40 (2026-08-23 F10 3b groundwork): format's
            // DECOMPOSITION of a spelled argument list rebuilds the raw
            // arg array through the structural inverse (unspell) — a
            // consumption artifact, not a new carrier emission.
            // 40→42 (2026-08-24 F10 3b harmonization): unspellMarked's
            // rebuilt raw array (the structural inverse of a marked
            // collection) + CastPolicy's per-element TO_VARIANT re-wrap
            // (reproducing the exact pre-carrier conformance shape) —
            // both CONSUMPTION-side inversions, not new carrier
            // emissions; absorbed with the collection semantic node.
            // 42→39 (2026-08-24 spell-debt burn-down): all three
            // inversion rebuilds DELETED with the unspell family —
            // no live flow delivered a spelled value to a
            // mid-expression consumer (probe + full chain); future
            // consumers conform BY EMISSION, never by inverting.
            // 39→41 (M4 re-land 2026-08-25, the hetero-LITERAL claim):
            // the claim's ONE designed emission at the label seam (the
            // Lowerer's Array(LITERAL) construction — same family as
            // the mixedNumericArray row) + format's spelled-slot PRINT
            // projection array (emission by static kind, the doctrine's
            // transform — not an inversion rebuild). Both absorb with
            // the collection semantic node.
            // 41→42 (disagree-9 burn 2026-08-31): staticTemporalText
            // REBUILDS an existing literal array with its elements'
            // written spellings (fidelity rewrite of an emission that
            // already existed — the mixedNumericArray class above);
            // absorbs with the collection semantic node.
            // 42 -> 40 (batch 88, 2026-09-06): tightened to the measured count
            "new SqlExpr\\.ArrayLit\\(", 40,
            "new SqlExpr\\.OrderedListAgg\\(", 1,
            // 136→137 (2026-08-19): ListEncodings.map's LIST_GET — the
            // map SEMANTIC NODE's wire-shape rule (a to-one result
            // unwraps from its singleton transform; Phase 4 channel B),
            // not a new ad-hoc idiom. 138→137 (2026-08-19 Clause-2c
            // redesign): Scalars.emptinessOf's static-empty arm DELETED —
            // verdicts moved to World 1. (A 2026-08-20 C2 toOne-unwrap
            // 137→138 was built, MEASURED against the full corpus —
            // milestoning −16 / union −23 — and REVERTED same day: the
            // list-shaped toOne operands are mostly synthesized
            // conformance markers whose list downstream consumes;
            // STAMP_DISCIPLINE_PROGRAM records the provenance-split
            // design that replaces the blanket emission.)
            // 137→139 (2026-08-20 stamp endgame): Scalars.
            // checkedExtract — the CHECKED toOne over a definite
            // list-producing CALL (LIST_LENGTH guard + LIST_GET), the
            // agg-strip's semantics spelled directly. UNLIKE the
            // reverted blanket unwrap this landed corpus-green with the
            // ledger byte-identical (the synthesized-conformance
            // operands it would have broken were healed first — C2c +
            // frame-honest stamps); the checked-narrowing SEMANTIC NODE
            // (guard-emission design) re-absorbs both sites when built.
            // 139→141 (deletion-leg rebuild): the minus LIST arm's
            // runtime-size-1 negate guard (LIST_LENGTH + LIST_GET) —
            // real pure negates a size-1 collection; the first-element
            // seed returned +x (residue recorded at the C1 landing).
            // 141→129 (burn slice 8): LIST_PRODUCERS became SqlFn
            // metadata (function facts live on the enum, outside the
            // pre-dialect ban zone) and the dissolved ListShapes' dead
            // arms took their references with them.
            // 127→129 (2026-08-22 X5): TWO sites in the keyed-instance
            // canon (CanonicalRenderSql — list_transform over key-value
            // collections). VERDICT-LANE ONLY, never product data flow:
            // a dialect without the carrier fails the wrapped query and
            // the canon-exec tunnel declines, counted — a wrong verdict
            // is impossible. Migrates to the carrier strategy with
            // F3/F10 (the H2 list-encoding leg).
            // 129→132 (2026-08-22 F12): the map canon's THREE sites
            // (LIST_GET on map_extract, LIST_SORT + LIST_TRANSFORM over
            // the entry texts) — same verdict-lane-only argument, same
            // F3/F10 migration note.
            // 129→127 (D1): checkedExtract's guard spelling moved INTO the
            // renderer behind the CheckedOne SEMANTIC NODE — the exact
            // re-absorption its justification promised.
            // 132→133 (2026-08-23 F13c): InstanceEquality's canonical
            // MEMBERSHIP (contains/in over instances = equal() per
            // element — LIST_TRANSFORM to the verdict canon's texts).
            // Verdict-lane only, same family and F3/F10 migration note
            // as the instanceCanon/mapCanon rows above; absorbed when
            // the canon family moves behind its semantic node.
            // 133→134 (2026-08-23 F13b(a)): the property-nav FLATTEN —
            // pure's auto-map over a collection FLATTENS a to-many
            // property ([$p1,$p2].locations is the flat union; the
            // model's declared multiplicity decides, never a shape
            // guess). Sits in the SAME arm as the pre-existing
            // property-nav LIST_TRANSFORM; both absorb together when
            // property navigation moves behind its semantic node.
            // 134→135 (2026-08-27 Channel B leg-4 batch): the sort
            // rule's dedup SEE-THROUGH pattern-MATCHES the existing
            // orderedDedup emission (dc.fn() == SqlFn.LIST_FILTER) —
            // a structural READ of a carrier idiom, not a new
            // emission; absorbs when dedup moves behind its semantic
            // node with the rest of the family.
            // 141→142 (disagree-9 burn 2026-08-31): staticTemporalText's
            // LIST_FILTER see-through — the same structural READ as its
            // UNNEST twin below, not a new emission.
            // 142→141 (corpus-zero cluster A, 2026-09-12): the checked
            // envelope's child hoist landed as SEMANTIC nodes
            // (SqlExpr.CheckedDefects / CheckedChildValue) spelled by the
            // DuckDB strategy CheckedDefectsToLists — and the envelope's
            // own-defects list emission folded one direct site away
            "SqlFn\\.LIST_", 141,
            // 12→13 (disagree-9 burn 2026-08-31): staticTemporalText
            // SEE-THROUGH pattern-MATCHES the existing literal-list
            // egress (UNNEST/LIST_FILTER wrappers) to restore written
            // temporal fidelity — a structural READ of a carrier
            // idiom, not a new emission (the dedup-see-through
            // precedent above); absorbs with the family.
            "SqlFn\\.UNNEST", 13,
            // the collect-carrier reducer (R1 recognizes it for fusion;
            // burns with R3/R4 when sources/values migrate)
            "new SqlAgg\\.Reducer\\(\"LIST\"", 0);

    @Test
    void carrierIdiomsOnlyShrink() throws IOException {
        for (var e : PINS.entrySet()) {
            int n = countAcrossPreDialectSources(Pattern.compile(e.getKey()));
            assertTrue(n <= e.getValue(),
                    "carrier purity ratchet: '" + e.getKey() + "' has " + n
                    + " pre-dialect sites (pin " + e.getValue()
                    + ") — new direct carrier emission is banned; lower to"
                    + " a semantic node and add strategy rules instead"
                    + " (CARRIER_REDESIGN.md tenet #1)");
            if (n < e.getValue()) {
                System.out.println("[carrier-ratchet] '" + e.getKey()
                        + "' shrank to " + n + " (pin " + e.getValue()
                        + ") — tighten the pin");
            }
        }
    }

    private static int countAcrossPreDialectSources(Pattern p)
            throws IOException {
        int n = 0;
        for (Path f : preDialectSources()) {
            Matcher m = p.matcher(Files.readString(f));
            while (m.find()) {
                n++;
            }
        }
        return n;
    }

    /** The layers UPSTREAM of the dialect strategy pass — where carrier
     * idioms are banned. The dialect package itself is exempt: that is
     * exactly where the idioms belong (as strategy rules). */
    private static java.util.List<Path> preDialectSources()
            throws IOException {
        Path root = Repo.module("src/main/java/com/legend");
        try (Stream<Path> s = Files.walk(root)) {
            java.util.List<Path> out = s
                    .filter(f -> f.toString().endsWith(".java"))
                    .filter(f -> {
                        // '/' ALWAYS: Path.toString uses the platform
                        // separator, so on Windows every one of these
                        // contains() checks is false, the guard scans ZERO
                        // files, and its floor trips for a reason that has
                        // nothing to do with carrier purity (Windows CI,
                        // 2026-09-09). And RELATIVE to the walk root: the
                        // root is absolute, so the checkout's own directory
                        // names would otherwise reach these contains() —
                        // a checkout under .../plan/ would match every file.
                        String path = Repo.rel(root, f);
                        return (path.contains("/lowering/")
                                || path.contains("/resolver/")
                                || path.contains("/plan/"))
                                && !path.contains("/sql/dialect/");
                    })
                    .toList();
            GuardCoverage.assertFloor("CarrierPurityRatchetTest",
                    out.size(), 74);
            return out;
        }
    }
}
