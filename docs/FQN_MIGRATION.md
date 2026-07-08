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
  f. CoreFn.of maps FQNs (parse-names stay for parser desugar).
STEP 3 — emission-site migration (separate slice):
  g. MappingNormalizer's ~20 emitted names, SpecParser's tds desugar,
     engine corpus fixtures — emit FQNs (or keep bare; the resolver
     handles both after e).

RISKS: multi-home assignment wrong for an overload -> wrong-package FQN
(golden diff review is the gate); tds-package legacy functions NOT yet in
catalog (register when corpus needs them); CoreFn parse-name collisions
across packages (filter core-dispatch must see all homes).
