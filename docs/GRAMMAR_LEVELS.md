# The three grammar levels

User directive 2026-08-12. A parser always knows which level it serves —
`com.legend.parser.GrammarLevel` — and there is no neutral default a caller can
grab by accident.

| level | contract | entry points | census |
|---|---|---|---|
| **LEGEND_PLATFORM** | legend-lite's OWN legend-pure dialect (m3/m2: `^$x(...)`, `native`, generics, function-type literals, `.allVersionsInRange`, m2 mapping forms). The analogue of legend-engine depending on precompiled legend-pure jars. | `ElementParser.parseLegendPlatform` — bootstrap/platform loading + the test harness ONLY, caller-whitelisted by `PlatformSurfaceGuardrailTest` (shrink-only) | unreachable from user code, by gate |
| **LEGEND_ENGINE** | user-facing EXACT legend-engine. Refuses the platform dialect AND the lite extensions, with the engine's own messages. | `PmcdParser` / the SPI bridge / `ElementParser.parseLegendEngine` — the drop-in parity surface | the corpus sweep: byte parity + verdict symmetry (`docs/refusal-allowlist.tsv`) |
| **LEGEND_LITE** | LEGEND_ENGINE **plus** the DECLARED extensions (`docs/OWN_CORPUS_DECISIONS.md` LITE-DESIGN families: mapping-as-function, inline-association, sqlite-backend, function-types-generics) — nothing undeclared rides along. | `ElementParser.parseLegendLite` — the product surface; the SERVERS migrated here 2026-08-12, the Compiler user path is the remaining candidate | its delta vs LEGEND must classify to a declared family (sentinel census) |

**Extension gates** (refuse at LEGEND_ENGINE, parse at LEGEND_LITE):
`cleanSheetAhead()` (mapping-as-function + inline-association detector
returns false, engine-shaped paths take over), the relational `->get`
arrow chain, the `SQLite` datasource specification.

**A gate that went red and taught us something:** `#TDS` is NOT
platform-only — the engine's `xt-tds` extension parses `#TDS` accessors
(6 oracle-accepted files byte-match). The allowlisted TDS rows are the
finer leniency: TDS content the engine's extension refuses and lite's
parser accepts.

**Found at introduction:** the old `legendStrict` boolean CONFLATED the
levels — "strict" was LEGEND_LITE (extensions parsed on the drop-in
surface), "lenient" was PLATFORM. LEGEND-exact did not exist until now.

**Migration state:** the servers (`PureLspServer`, `DiagramService`,
`ConnectionResolver`) migrated to `parseLegendLite` 2026-08-12; the
Compiler user path is the remaining candidate (needs the user/internal
split — its `compileModel` is also every own test's entry).
