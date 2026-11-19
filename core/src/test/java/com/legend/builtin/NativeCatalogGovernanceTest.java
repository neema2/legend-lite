// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.builtin;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The NATIVE-CATALOG governance gate (invention audit 2026-08-14 §5/§6):
 * the grammar surface is oracle-checked every gate-8 run, but the
 * FUNCTION catalog was pinned only against its own golden file — a 20th
 * invented native could land by regenerating the golden. This test
 * closes that, and pins the USER-REACHABILITY PARTITION: every native
 * in the lite-internal package must be one of the three NAMED,
 * per-name-verified sets; the internal vocabulary must be spelled by
 * the {@link Pure.Lite} constants (each bound to a registered catalog
 * native — a typo'd constant cannot survive one run); and no internal
 * name may resolve from user bare-name text.
 */
class NativeCatalogGovernanceTest {

    @Test
    void everyLiteInternalNativeIsNamedAndVerified() {
        var unaccounted = Pure.liteInternalNatives().stream()
                .map(q -> q.substring(q.lastIndexOf("::") + 2))
                .filter(bare -> !Pure.INTERNAL_DESUGAR.contains(bare)
                        && !Pure.ENGINE_VOCAB_SHIMS.contains(bare)
                        && !Pure.LITE_SURFACE.contains(bare))
                .sorted().distinct().toList();
        assertEquals(List.of(), unaccounted,
                "lite-internal natives outside the verified sets — a new"
                        + " name needs the per-name upstream check"
                        + " (LITE_INVENTION_CENSUS.md) before it exists");
    }

    @Test
    void theVerifiedSetsOnlyShrink() {
        // 11 internal-desugar + 7 engine-vocabulary shims + 2 user-facing
        // surface natives, verified per-name 2026-08-14 (maxDate/minDate/
        // variantTo/percentileCont/percentileDisc deleted; traverse +
        // _Traversal deleted — navigate subsumed it; navigate and
        // sourceUrl reclassified LITE_SURFACE: both pinned from user
        // query text by dedicated tests — navigate has zero internal
        // emitters, sourceUrl is deliberately user-callable per
        // SourceUrlUserCallableTest). Growth is a NEW invention; shrink
        // is always allowed.
        // +1 2026-08-15: adjustTemporal — date::adjust semantics with the
        // LEGACY-print channel mark (engine extensionDefaults
        // mapToDBUnitType prints dateadd units UPPERCASE, the new
        // sqlDialectTranslation defaults lowercase; TemporalFrame stamps
        // milestoning window dates; census row in
        // docs/LITE_INVENTION_CENSUS.md).
        // 12→13 (multiplicity audit slice 3): trustOne — the SQL-lane
        // to-one conformance wrap, the C2 provenance split's synth
        // spelling (user toOne is CHECKED; synth trust is not).
        // 13→14 (2026-09-02, single-table hierarchies): unionScan — the
        // identity marker around a merged single-scan union body; the
        // resolver's "is a union" facts read the node kind where the
        // concatenate shape no longer exists (lowering is erasure).
        // 14→16 (batch 72b, 2026-09-05): asorPkValue / asorDecodePkMap —
        // the store-object-reference READERS: the reference decodes IN
        // SQL (base64 + framing regex); the resolver mints them where
        // the engine's objectReferenceIn collection is a runtime value
        // and where decodeObjectReferencesAndGetPkMap reads a frame.
        // 16→17 (batch 5 leg 5d, 2026-09-11): groupByOverInstances — the
        // relation-style group-by over a collection of instances, the landing
        // shape of the legacy tds::groupBy desugar and the mapping ~groupBy
        // synthesis; it had been carried as two invented overloads of
        // upstream's tds::groupBy (divergent rows), now its own internal identity.
        // 17→18: groupByComputedKeys — the relation group-by with key EXPRESSIONS
        // (mapping ~groupBy / view ~groupBy synthesis; the engine emits GROUP BY expr)
        // 18→15 (batch 5 audit leg A, 2026-09-11): the four *Format date shims
        // are ENGINE vocabulary (the translator's format arms land on them) and
        // move to ENGINE_VOCAB_SHIMS; the pipeline SLOT join (internal IR the
        // normalizer emits) moves IN from the shim set where it was mis-filed.
        // 15→16 (corpus-zero cluster A, 2026-09-12): tuple — the ROW of a
        // graph-fetch tree's leaf values, the desugar target of pure's by-tree
        // isDistinct(collection, tree) (IsDistinctChecker: map(e | tuple($e.a,
        // $e.b))->isDistinct()); upstream implements that overload only in
        // generated Java (IsDistinctFetchTreeCoder) — here it is a struct
        // literal distinct-counted as one value; census row added.
        // B3.1 (2026-09-13) added memberColumn + unionArm (16 -> 18); B3.1b
        // the same day deleted both: a routed navigation reads the LINK KEY
        // its target member publishes as a plain column (USER review: no
        // mapping-DSL set ids inside generated Pure). 18 -> 16.
        // +1 2026-09-13 (legacy routes as composition, step 1): `route` — one
        // route of a several-route legacyNavigate (the target set's own
        // function, its rows, the join as written); the union stops
        // publishing keys once the emitter switches (step 2). 16 -> 17.
        // +2 2026-10-16 (the Cypher island's time-of-day leg, docs/CYPHER_STUDY_2026_10_15.md §12.7): timeOf /
        // atTime — a time of day (Pure's StrictTime) in SQL: the date-time's time (a CAST to TIME) and the
        // date-time at a time of a date (the database's date plus time). legend-pure declares StrictTime and only
        // prints it; legend-engine's relational lowering refuses it (pureToSQLQuery.pure "not supported yet"), so
        // neither has an upstream counterpart; emitted by the Cypher translator only, by FQN. 17 -> 19.
        // +2 2026-10-16 (the Cypher island's zoned date-time leg, docs/CYPHER_STUDY_2026_10_15.md §12.8): instantIn
        // / localIn — a named time zone read by the database: the instant a wall time names in a zone, and the
        // wall time of an instant in it, each the convertTimeZone lowering's pair of zone conversions with a
        // DateTime in and out. Upstream knows zones only at formatting and on the connection. Emitted by the
        // Cypher translator only, by FQN. 19 -> 21.
        // +1 2026-10-16 (the SQL islands' inline views; docs/LITE_INVENTION_CENSUS.md): inlineView — a derived
        // table or CTE whose own query computes windows, typed as its relation with each window a boundary
        // (InlineViewChecker; ClassSources' extent rule) and erased: SQL computes such windows before the enclosing
        // WHERE, which Pure's relation composition folds under them. Within the ceiling: merged with the Cypher
        // island's four (2026-10-16), whose 21 counted from the ceiling 17, the count is 16 + 4 + 1 = 21.
        // +1 2026-10-17 (the Cypher island's identity leg, docs/CYPHER_TCK_TO_ZERO_PLAN_2026_10_17.md W2):
        // elementId — the identity of a mapped instance as a value (its class's primary key by the engine's
        // precedence, as text), what Cypher's elementId(n) and id(n) answer and node comparisons, DISTINCT and
        // grouping use; Pure has no expression for it. Resolved by the store resolver through the primary-key
        // pseudo-binding; emitted by the Cypher translator only, by FQN. 21 -> 22.
        // +1 2026-10-17 (the Cypher island's rand-and-clocks leg, docs/CYPHER_TCK_TO_ZERO_PLAN_2026_10_17.md W3):
        // random — the database's random float in [0, 1), what Cypher's rand() answers. 22 -> 23; and -1 the same
        // day (the spec guards' repair, which regenerated the prelude): legend-pure DOES declare it
        // (essential/math/random.pure, meta::pure::functions::math::random():Float[1]), which the leg had missed,
        // so the platform implements upstream's native (native-membership.tsv, Scalars' RANDOM rule) and lite's
        // twin left. The counts below are as landed; the ceiling is one lower.
        // +1 2026-10-17 (the Cypher island's tail leg 2, docs/CYPHER_TCK_TO_ZERO_PLAN_2026_10_17.md W12): isKind — the
        // database's reading of a Variant's JSON kind (SqlFn.JSON_TYPE, which every dialect spells, against the
        // dialect's own kind name, SqlExpr.JsonKindLit), what Cypher's predicates over a value the rows hold as a
        // Variant ask first (STARTS WITH on a value that is a string, null otherwise); legend-pure's Variant has no
        // kind function. Emitted by the Cypher translator only, by FQN. 23 -> 24.
        // +2 2026-10-17 (the Cypher island's W11, docs/CYPHER_TCK_TO_ZERO_PLAN_2026_10_17.md): minBy and maxBy over
        // rowMapper(value, key) rows with a TEXT key (the lowering's ARG_MIN and ARG_MAX, which every dialect
        // renders), what Cypher's min and max over values of several kinds answer by the island's orderability
        // text; legend-pure's own minBy and maxBy take a number key, which a type rank over Variants is not.
        // Emitted by the Cypher translator only, by FQN. 24 -> 26.
        // +1 2026-10-17 (the Cypher island's W10 leg 2a, docs/CYPHER_TCK_TO_ZERO_PLAN_2026_10_17.md): recurse —
        // recurse(base, r | step): the database's recursive common table expression (WITH RECURSIVE name AS (base
        // UNION ALL step)), the step reading the previous round's rows through its parameter until a round yields
        // none; what Cypher's unbounded variable-length pattern walks. legend-pure has no recursion over a relation
        // (its Pure functions recurse in the host, which the execution tenet forbids). Emitted by the Cypher
        // translator only, by FQN. 26 -> 27; 27 -> 26 for random (above).
        // +2 2026-10-18 (the Cypher island's nanosecond values as a date and a time of day, the user's decision,
        // docs/CYPHER_TCK_TO_ZERO_PLAN_2026_10_17.md W13): dateTimeOfText / timeOfText — a temporal value in text
        // carriage, the database's ISO text labelled TEMPORAL_TEXT (a cast that never renders) and read by the
        // executor as the date-time or time of day it spells: a value finer than every database's date-time type
        // (Postgres keeps microseconds, DuckDB's TIMESTAMP_NS holds 1678 to 2261) leaves the database so.
        // legend-engine carries a date-time in the database's own type, so neither has an upstream counterpart.
        // Emitted by the Cypher translator only, by FQN, at a result's columns. 26 -> 28.
        // +1 2026-10-17 (the IQ island's scale, docs/IQ_CENSUS_CHECKLIST.md "the refusals the live IQ answers"):
        // quotient — an integer quotient cut toward zero, the database's own integer division (MIR's INT_DIVIDE,
        // every dialect's arm). legend-pure's divide of integers is a Float and upstream has no integer division,
        // so the SQL islands wrote one as a Float estimate corrected by remainders, reading each operand about
        // fourteen times; nested quotients made statements of megabytes (44 corpus queries refused by the
        // translation-size bound). Emitted by the SQL islands only, by FQN. 28 -> 29 (merging main, 2026-10-18).
        // +1 2026-10-20 (the writes design's leg 6, docs/WRITES_DESIGN_2026_10_18.md §8: Cypher's writes on the native
        // graph store): stage -- let s = rows->stage(), the rows computed once at the let's turn into a temporary table
        // of the session, which the statements after read, so the writes between read the rows as they were (a
        // relation expression is recomputed at each read, and a write changes what it finds). legend-pure's lets bind
        // values, not a relation's rows at a point of a program's writes; legend-engine has no counterpart. Emitted by
        // the Cypher translator only, by FQN, a let's value only. 28 -> 29.
        // +5 2026-10-20 (the writes design's leg 5, §4 L2: Cypher's writes on a graph of a domain model):
        // writeInstances / mergeInstances / deleteInstances / link / unlink -- the class writes over a direct mapping,
        // each a relation of rows and a class (the rows' key column each instance's key, the others its properties),
        // which the store resolver writes as L1's statement over the class's mapped table (resolver/ClassWrites).
        // legend-pure has no write through a mapping; legend-engine's is mutation::save, an upsert of one object.
        // Emitted by the Cypher translator only, by FQN (GraphQL's mutations will be their second emitter). 29 -> 34.
        // 34 -> 35 (2026-10-22, merging main into the Cypher writes branch): main's quotient (+1, above) beside this
        // branch's stage and the five class writes (+6).
        // 35 -> 36 (2026-11-15, phase D's D1.3: the user's ruling of 2026-11-10, a TDS cell's toString answers the
        // empty value where the cell is, relayed by the orchestrator 2026-11-15): cellText, the text of a TDS cell that
        // TdsDesugars emits for $r.get('c')->toString() -- the product's empty, the compatibility edge's TDSNull, one
        // node (SqlFn.CELL_TEXT) the two styles spell; it replaced the desugar's literal 'TDSNull' branch.
        // 36 -> 37 (2026-11-16, W5 of docs/records/d-plan-2026-11-16.md, D1.1's last rows): typeRaise, a run-time type
        // check's failure as the product's raise of its kind (CAST_IMPOSSIBLE, INSTANCE_NOT_OF) -- emitted by the
        // resolver's chain dispatch and subtype reads, which raised legend-pure's sentence through fail until then;
        // the sentence is the compatibility edge's (edge.PureMessages)
        // 37 -> 38 (PR #118 fix round, 2026-11-18: N2, the in-body assert's default message chosen by the plan's style
        // at lowering; the user's word "raise it"): assertFailedText, which LambdaBodies writes for fail's message where
        // the assert gives none -- the product's lite sentence, the compatibility edge's legend-pure "Assert failed"
        // 38 -> 40 (2026-11-19, A3 of the compatibility program, piece 1, the translator's: sqlSubstring and
        // sqlPosition, SQL's SUBSTRING and POSITION by name, which the islands and the ###Relational dynaFunctions
        // write; its own dated note replaces this line where piece 1 lands, claude/compat-a3-switch)
        // 40 -> 41 (2026-11-19, A3, the user's ruling ~21:10 "Yes to all three for substring", relayed by the compat
        // owner; the reviewer's D-S1 as corrected, ca8ea4a5): substringAsGiven, the database's own SUBSTRING over the
        // arguments as given -- lite-internal, its one producer the legend-engine door's engine rewrite, its one
        // product claimer Scalars (substringAsGivensOneProductClaimerIsScalars)
        assertTrue(Pure.INTERNAL_DESUGAR.size() <= 41,
                "INTERNAL_DESUGAR grew: " + Pure.INTERNAL_DESUGAR);
        // +4 2026-08-16: lessThan/lessThanEqual/greaterThan/
        // greaterThanEqual Any-shims — engine DynaFunc ordering
        // comparisons carry UNTYPED operands (RelationalParseTreeWalker
        // Literal); routing them through pure's same-family ordering
        // overloads killed whole class mappings (ledger cluster 18;
        // notEqualAnsi precedent; census row updated).
        // +1 2026-09-11 (upstream boundary batch 5 leg 5c): isDistinct —
        // the engine's relational dynaFn isDistinct(a, b) (SQL IS DISTINCT
        // FROM; legend-engine extensionDefaults.pure registers it) has no
        // pure counterpart (pure's isDistinct is the 1-arg collection test);
        // it had been carried as an invented 2-arg overload of the pure name.
        // 12→12 with a DIFFERENT membership (batch 5 audit leg A, 2026-09-11):
        // OUT — hash (1-arg; no engine dynafunction, no producer), avg (the
        // engine spells avg only as SQL for pure's average), sub (the
        // translator's arm rewrites every 2-arg sub into the minus run; other
        // arities are an error, as the engine's %s-%s format makes them), and
        // the slot join (internal IR). IN — the four *Format date shims. The
        // membership is now DERIVED-VERIFIED against the dynafunction registry
        // (DynaFnRegistryTest: SHIM rows + the translator's declared landings).
        assertTrue(Pure.ENGINE_VOCAB_SHIMS.size() <= 12,
                "ENGINE_VOCAB_SHIMS grew: " + Pure.ENGINE_VOCAB_SHIMS);
        // +2 2026-09-11 (USER, upstream boundary batch 5 leg 5d): joinWithPrefix /
        // asOfJoinWithPrefix — the relation join with a right-column PREFIX, a
        // lite-dialect feature kept on purpose (upstream resolves collisions by
        // rename before the join); the user spelling join(…, 'p_') routes by arity.
        // +3 2026-10-18 (the writes design's leg 2, docs/WRITES_DESIGN_2026_10_18.md §4, L1): delete / update /
        // merge, the store writes beside upstream's write -- legend-pure has no DML over a relation accessor and
        // legend-engine's only write through a mapping is mutation::save (an H2/MemSQL upsert of one object); each
        // is the database's statement over a store table, user-facing by design (the design's D2)
        // +1 2026-10-27 (the user's "do the without/except function", docs/PURE_LIBRARY_SPIKE_2026_10_27.md GAP 1,
        // docs/SEMANTICS_REGISTER.md S113): without -- the relation less the named columns (T-Z), typed as the select
        // of the rest; legend-pure 5.99.0 and legend-engine 4.145.0 drop rows but never columns (rename's T-Z+V is
        // upstream's only column-subtracting signature), and a function generic over any table cannot name the
        // caller's columns to select them
        // +1 2026-10-27 (the user's "look at legend-pure first but yeah lets def build some primitive like that",
        // docs/SEMANTICS_REGISTER.md S114): read -- a row's column through a column spec, typed as $r.col; upstream's
        // ColSpec is a name alone and a column is read only as a reflected Column evaluated on a row (typed Any)
        assertTrue(Pure.LITE_SURFACE.size() <= 9,
                "LITE_SURFACE grew: " + Pure.LITE_SURFACE);
    }

    @Test
    void everyLiteConstantIsARegisteredNative() throws Exception {
        // The Pure.Lite constants are the ONLY sanctioned spellings at
        // internal emit/match/registration sites; each must be a real
        // catalog FQN so a typo fails here, not at some runtime miss.
        for (var field : Pure.Lite.class.getDeclaredFields()) {
            if (field.getType() != String.class || field.getName().equals("PKG")) {
                continue;
            }
            String fqn = (String) field.get(null);
            assertTrue(!Pure.nativeFunctionsAt(fqn).isEmpty(),
                    "Pure.Lite." + field.getName() + " = " + fqn
                            + " is not a registered catalog native");
        }
    }

    @Test
    void internalVocabularyIsNotUserResolvable() {
        // The partition itself: a bare name is a query against the USER
        // namespace, and the internal vocabulary is not in it. Shim
        // names that collide with REAL pure functions (join, hash) keep
        // their real overloads and lose only the lite-package ones.
        var leaks = new TreeSet<String>();
        for (String bare : union(Pure.INTERNAL_DESUGAR, Pure.ENGINE_VOCAB_SHIMS)) {
            for (var def : catalog(bare)) {
                if (def.qualifiedName().startsWith(Pure.Lite.PKG)) {
                    leaks.add(def.qualifiedName());
                }
            }
        }
        assertEquals(Set.of(), leaks,
                "lite-internal natives reachable from user bare-name text");
    }

    /** The catalog natives a bare name may denote, every tier's (the product's own flattening of it,
     *  {@code BareNames.catalog}, went with the ledger's PARK-5, L7 2026-10-10). */
    private static java.util.List<com.legend.model.NativeFunctionDefinition> catalog(String name) {
        java.util.List<com.legend.model.NativeFunctionDefinition> out = new java.util.ArrayList<>();
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (var tier : com.legend.compiler.BareNames.catalogTiered(name)) {
            if (seen.add(tier.getKey().fqn())) {
                out.addAll(tier.getValue());
            }
        }
        return out;
    }

    @Test
    void liteSurfaceStaysResolvable() {
        for (String bare : Pure.LITE_SURFACE) {
            assertTrue(!catalog(bare).isEmpty(),
                    bare + " is user-facing lite product surface"
                            + " — it must stay bare-name resolvable");
        }
    }

    private static Set<String> union(Set<String> a, Set<String> b) {
        var u = new TreeSet<>(a);
        u.addAll(b);
        return u;
    }

    /** The edge's claimers of {@code substringAsGiven}: the class that builds the engine rewrite's substitutes. */
    static final Set<String> SUBSTRING_AS_GIVEN_EDGE_CLAIMERS = Set.of("EngineSubstitutes");

    /**
     * NO PRODUCT CLAIMER OF {@code substringAsGiven} BUT {@code Scalars} (the pin the user ruled, 2026-11-19 ~21:10, "Yes
     * to all three for substring", relayed by the compat owner; the reviewer's D-S1 as corrected, ca8ea4a5): the native
     * is the legend-engine door's alone, so in the generated claims (native-claims.tsv, ClaimsGenerator's {@code also}
     * column: the files naming one of its overload constants, its id group or its quoted name) its claimers are the
     * lowering's rule and the edge's substitute builder, and no other. A product frontend that wrote it would be seen
     * here. Named limit: the column does not read a use through the {@code Pure.Lite} name constant alone.
     */
    @Test
    void substringAsGivensOneProductClaimerIsScalars() throws java.io.IOException {
        List<String[]> rows = new java.util.ArrayList<>();
        try (java.io.InputStream in = Pure.class.getResourceAsStream("/com/legend/builtin/native-claims.tsv")) {
            for (String line : new String(java.util.Objects.requireNonNull(in, "native-claims.tsv").readAllBytes(),
                    java.nio.charset.StandardCharsets.UTF_8).split("\n")) {
                String[] cells = line.split("\t", -1);
                if (cells.length >= 6 && cells[0].equals(Pure.Lite.SUBSTRING_AS_GIVEN)) {
                    rows.add(cells);
                }
            }
        }
        assertEquals(2, rows.size(), "substringAsGiven's two overloads");
        for (String[] row : rows) {
            assertEquals("", strayClaimers(row[5]), row[2]);
            assertTrue(Set.of(row[5].split(";")).contains("Scalars"), row[2] + ": " + row[5]);
        }
        // the pin bites: a planted product claimer is named
        assertEquals("CypherToPure", strayClaimers("CypherToPure;EngineSubstitutes;Scalars"));
    }

    /** The claimers in {@code also} that are neither Scalars nor the edge's, joined; empty when there are none. */
    private static String strayClaimers(String also) {
        return String.join(";", java.util.Arrays.stream(also.split(";"))
                .filter(c -> !c.isEmpty() && !c.equals("Scalars") && !SUBSTRING_AS_GIVEN_EDGE_CLAIMERS.contains(c))
                .toList());
    }
}
