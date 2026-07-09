# Wave 4c: the FQN catalog migration

GOAL: the catalog keyed by REAL packages (per OVERLOAD — 32 bare names are
multi-home, e.g. filter ∈ {collection, relation, tds}), killing the
silent-capture hazard (a user function at meta::pure::custom::map rebinding
to the native) and enabling legacy-TDS + Relation-API coexistence.

INVENTORY (tools: /tmp fqn-homes.json script in git history):
- 268 catalog bare names; 240 resolve to real packages; 32 multi-home;
  28 unresolved = invented natives (tableReference, tds, legacyNavigate,
  otherwise, navigate, sourceUrl, legacyAssocPredicate, newTDSRelationAccessor)
  + engine-only (divideRound, percentileCont/Disc, md5/sha*, isNull/isNotNull,
  maxDate/minDate, avg, concat, notEqualAnsi) + multiline-regex misses
  (notEqual IS real: essential/boolean/inequalities/notEqual.pure — REDO the
  derivation with a multiline-tolerant pass before assigning).

STEP 1 — dual-key infrastructure (SAFE, no behavior flip):
  a. Signature strings gain their REAL packages (per-overload mapping by
     bare name + arity + param shapes against extracted real signatures).
     Invented/engine-only natives stay bare (commented).
  b. Pure.Index: FN_BY_FQN keys the DECLARED (now qualified) name; ADD a
     bare-name index (BARE_TO_FQNS) so nativeFunctionsAt(bare) keeps
     working (returns the union across packages — overload resolution
     already picks by shape).
  c. normalizePlatformFunction VALIDATES instead of blind-stripping: an
     FQN call resolves ONLY if the catalog actually declares that FQN
     (kills silent capture NOW); bare calls resolve via the bare index.
  d. signatureKey() now embeds the qualified name — Scalars/Aggregates/
     Windows registration strings and Pure.nativeKeysAt/nativeNamed keep
     working unchanged (they go through the same helpers); the golden
     catalog file regenerates with FQNs (big deliberate diff).
STEP 2 — resolution flip (separate slice):
  e. NameResolver prelude gains FUNCTION imports (the platform packages
     as wildcards, mirroring class/enum handling); bare user calls
     resolve to FQNs at RESOLUTION; normalizePlatformFunction retires.
     ! THE IMPLICIT IMPORT SET MUST INCLUDE meta::legend::lite (the 23
     invented natives / 30 overloads live there as of step 1) alongside
     the real platform packages — else every bare lite call (isNull,
     notEqual, navigate, tds, ...) breaks. Today bare calls resolve via
     the package-agnostic FN_BY_BARE union index, so no import set
     exists yet.
  f. CoreFn.of maps FQNs (parse-names stay for parser desugar).
STEP 3 — emission-site migration (separate slice):
  g. MappingNormalizer's ~20 emitted names, SpecParser's tds desugar,
     engine corpus fixtures — emit FQNs (or keep bare; the resolver
     handles both after e).

RISKS: multi-home assignment wrong for an overload -> wrong-package FQN
(golden diff review is the gate); tds-package legacy functions NOT yet in
catalog (register when corpus needs them); CoreFn parse-name collisions
across packages (filter core-dispatch must see all homes).

RESOLUTION (2026-07-09) — steps 2 and 3-emission are CLOSED BY DESIGN:
- Step 2 (resolution flip): wrong as imagined. The multi-home names are
  NOT overloads of one function — they are DIFFERENT functions sharing a
  simple name (collection::filter(T[*],fn) vs relation::filter(
  Relation<T>,fn), each in its own .pure file). Real pure pools every
  imported function with that simple name and lets OVERLOAD RESOLUTION
  pick by argument shape; a resolver rewriting bare->single-FQN before
  types are known would have to guess. The dual-key catalog's bare-union
  lookup IS that candidate pool; permanent design.
- Imported bare USER-function calls already resolve (probed: import
  my::pkg::*; helper() works in model bodies).
- KNOWN DIVERGENCE (found by user question, probed): when an imported
  user function shares a simple name with a NATIVE, the import wins
  COMPLETELY — native candidates drop out of the pool (real pure would
  pool both). Fails LOUD (type error naming the wrong-package function),
  never silent; corpus never hits it. Fix = multi-candidate resolution
  plumbing through functionsAt; ledgered, not blocking.
- Step 3 emission-site FQNs: unnecessary — user functions always carry
  packages, so bare emissions can only hit natives; no shadowing channel.
- Step 3's REAL remainder: the deferred fake-native retirements
  (sub/avg/concat, percentileCont/Disc, divideRound, notEqualAnsi,
  maxDate/minDate). Slice 1 (notEqual, md5/sha family, isNull/isNotNull)
  landed 2026-07-09.

