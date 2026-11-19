# B5's ratchet: designed and prototyped, not turned on (census B2, 2026-11-19)

The compatibility program's stage-B guard. The reviewer's ruling sets what it measures:
- **The strand ratchet:** the BYTECODE count of the compatibility strands' reach into the core, H8 (a)'s 49 parameters
  in 19 classes, 4 fields and 50 calls of `PlanStyle` (not `ModeBranchCensusTest`), with `LoweringForms`,
  `PlanStyleFacets` and upstream's `Feature` flags beside it.
- **The census ratchet:** the same idea over the meaning census's (a) hits, upstream-specific meaning in the core.

Both are prototyped on `claude/census-b2`. **Neither is turned on:** they land with stage B, on the user's word. No
existing pin, ceiling, ledger or allowlist moved. The census test gains a new report-only check beside its own,
untouched.

## 1. The strand ratchet

**The code:**
- `tools/meaning/StrandRatchet.java` measures, in the `census` library;
- `tools/guards/StrandRatchetTest.java` holds the pins;
- `//tools/guards:strand_ratchet_test` is the target.

**The scope:** every class of `//core:product_jars` outside `com/legend/edge/`, the strands' home, and outside each hook
type's own nest. The edge is not in `product_jars` at all; the package filter is a second fence.

**The measures and their pins, at `main` `b75d6c73a`:**

| Measure | Pin | What it counts |
|---|---:|---|
| `PlanStyle.parameters` | 49 | parameters of type `PlanStyle` on declared methods (19 classes) |
| `PlanStyle.fields` | 4 | fields of type `PlanStyle` |
| `PlanStyle.calls` | 50 | invokes of `PlanStyle`'s methods, and method references to them |
| `PlanStyle.carrying` | 112 | invokes whose target takes a `PlanStyle` (the style passed along) |
| `PlanStyle.classes` | 36 | classes naming `PlanStyle` |
| `LoweringForms.parameters` / `fields` / `calls` / `carrying` / `classes` | 6 / 1 / 25 / 11 / 10 | the same, for the lowering's two-form choices |
| `PlanStyleFacets.kinds` | 21 | the class constants its initializer loads: `KINDS` 5 and `TWO_FORMS` 16 |
| `PlanStyleFacets.reads`, `.parameters`, `.fields`, `.calls`, `.carrying`, `.classes` | 0 each | it is read by tests and the edge only, and it stays that way |
| `hooks.facet` / `hooks.twoForm` | 16 / 16 | the product's call sites of the style's keyed hooks |
| `Feature.classes` | 20 | classes naming upstream's `Feature` |
| `Feature.signatures` | 62 | generic signatures carrying it (`Set<Feature>`, which erasure hides from descriptors) |
| `Feature.loads` | 4 | `getstatic` of a member (`FeatureRules`, `Lowerer.withFeatures`) |
| `Feature.flagReads` | 12 | reads of the Lowerer's `legacyNullUnsafeEquals` and `features` |
| `Feature.parameters` / `fields` / `calls` / `carrying` | 1 / 0 / 1 / 1 | by descriptor |

The values equal H8 (a.1) and (a.3) measure for measure. They end at 0 when `PlanStyle` dies (slice 4.1).

**The rule: shrink-only, and exact.**
- A measure above its pin is the strand growing. It is refused, and the breach lists the sites counted.
- A measure below its pin is progress. It is refused too, until the pin is lowered to the measure with a dated note, so
  a gain is locked in and cannot be spent later.
- `PINS` is a Java constant in the form `tools/guards/shrink_only.py` reads (`Map.ofEntries` of `Map.entry`), so turning
  the ratchet on adds it to that script's `WATCHED` list. A raise then needs a dated note by the merge-base rule.
- Until `TURNED_ON`, breaches are printed (`[strand-ratchet] BREACH ...`), never failed on.

**It bites a plant whether or not it is on.** `itBitesAPlantedParameterAndACall` synthesizes a class in memory with
the JDK's class-file API (`java.lang.classfile`, JDK 25, the build's toolchain), so no planted library is needed. The
class is `com.legend.planted.ratchet.PlantedStrandUser`, with one method `use(PlanStyle, LoweringForms)` that calls
`PlanStyle.judgesEngineText()` and reads `Feature.LEGACY_SQL_NULL_UNSAFE_EQUALS`. Measured beside the product, it
breaches 7 measures:
- `PlanStyle.parameters` 49 → 50;
- `PlanStyle.calls` 50 → 51;
- `PlanStyle.classes` 36 → 37;
- `LoweringForms.parameters` 6 → 7;
- `LoweringForms.classes` 10 → 11;
- `Feature.loads` 4 → 5;
- `Feature.classes` 20 → 21.

Each breach names the plant. The test asserts the first five and that the plant is named.

**The prototype's result, 2026-11-19:**
- `bazel test //tools/guards:strand_ratchet_test`: 2 tests, both passing, in 2.6-3.0 s.
- Over 3,456 product classes, reading the jars takes 535 ms and measuring 539 ms.
- Every measure equals its pin, 0 breaches; report-only.

## 2. The census ratchet

**The census already runs as a test:** `//tools/guards:meaning_census_test`, in `//gates:checks`, report-only. Its
classes at `main`: (a) 142, (b) 621, (c) 479, (p) 2, 1,244 hits.

**The prototype** is inside `tools/guards/MeaningCensusTest.java`, beside its existing checks:
- `RATCHET_PINS = Map.of("a", 142, "p", 2)`, `RATCHET_ON = false` and `ratchetBreaches(byClass)`, with the same exact
  rule as the strand ratchet;
- the real run prints `[census-ratchet] (a) 142 (pin 142), (p) 2 (pin 2); REPORT-ONLY, breaches []`;
- `theRatchetSeesAPlantedHit` holds the comparison to its plant: one (a) hit more is the breach "(a) GREW 142 -> 143".
  The lenses' own ability to see their plants is `eachLensBitesItsPlant`'s, as before.

**The result:** 3 tests, all passing, in 11.3 s. The ratchet adds nothing measurable to the census's own run.

## 3. Where they would sit, and what turning them on takes

- **The gate:** `//gates:checks`, which `//gates:local` runs.
  - The census ratchet is already in it, as part of `meaning_census_test`.
  - The strand ratchet would join beside it, about 3 s. Its target needs `visibility = ["//gates:__pkg__"]`.
  - With the prototype in place, `bazel test --lockfile_mode=error //gates:checks` reads 198 pass and 1 skipped.
- **Turning them on** is stage B, on the user's word:
  - `TURNED_ON = true` and `RATCHET_ON = true`;
  - the target added to `//gates:checks`;
  - both pin constants added to `shrink_only.py`'s `WATCHED`;
  - a dated note on each.

## 4. Its limits, named

- **It counts sites, not identities.** A strand that leaves one site and arrives at another keeps the count, and passes.
  To close that, commit each measure's site list beside its pin, as the census commits its inventory. A breach then
  names only the new site, and a swap shows as one removal and one addition. The cost is a committed TSV (about 300
  lines) that moves with every change to the strands. It should land with the ratchet if the user wants identities.
- **The keyed hooks' key** is read from the nearest `ldc Class` before the call (within 12 instructions). `hooks.facet`
  and `hooks.twoForm` count sites, whatever their key.
- **`Feature` is carried through generics,** so its parameters and fields are counted by signature strings (distinct
  per class), not one by one.
- **The (a) ratchet is as strong as the census's classification.** A hit classed (b) or (c) by an inventory rule is not
  (a). The inventory moves only with the user's rulings, as now.
