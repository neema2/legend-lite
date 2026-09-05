// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.harness;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE ASSERT LEDGER (user design 2026-09-04): for every test that is not a
 * clean platform pass, one row PER ASSERT — pass, or the truthful bucket
 * that names why the platform could not verify it — plus one row for the
 * asserts the attempt never reached. A clean test counts at the test
 * level (no rows here). Buckets are facts about the assert, never a
 * euphemism for failure:
 * <ul>
 *   <li>{@code pass} — verified on rows or values;</li>
 *   <li>{@code zero-assert} — the test has no verdict statement;</li>
 *   <li>{@code sql-text-assert} — the assert's subject is SQL TEXT
 *       (a {@code contains}/equality on the emitted text — the engine's
 *       spelling, ours judged as text, not rows);</li>
 *   <li>{@code referee-cannot-replay} — golden SQL the referee cannot
 *       execute (temp tables, executeInDb setups);</li>
 *   <li>{@code decision:<name>} — the standing decisions (chained fetch,
 *       objectReferenceIn, routeFunction, protocol transforms,
 *       recursion, dynamic compilation);</li>
 *   <li>{@code wall:<owner>} — a platform gap with its owner;</li>
 *   <li>{@code divergence} — rows produced and wrong;</li>
 *   <li>{@code engine-golden-defect:<name>} — rows produced and
 *       different because the GOLDEN carries the engine's own departure
 *       from Pure (registered per exact test with a receipt; see
 *       {@link #ENGINE_GOLDEN_DEFECTS});</li>
 *   <li>{@code not-reached} — asserts after the first failure.</li>
 * </ul>
 */
public final class AssertLedger {

    public record Row(String test, int ordinal, String form, String outcome, String detail) {
    }

    private static final Map<String, List<Row>> ROWS =
            java.util.Collections.synchronizedMap(new LinkedHashMap<>());

    private AssertLedger() {
    }

    public static void record(String test, List<Row> rows) {
        ROWS.put(test, List.copyOf(rows));
    }

    public static Map<String, List<Row>> rows() {
        synchronized (ROWS) {
            return new LinkedHashMap<>(ROWS);
        }
    }

    public static void reset() {
        ROWS.clear();
    }

    /**
     * ENGINE GOLDEN DEFECTS (user ruling 2026-09-05, "quarantine/bucket
     * those as engine bugs"): tests whose GOLDEN encodes the engine's own
     * departure from Pure's semantics — ours follows Pure, the rows
     * verdict truthfully fails, and the bucket says WHY. One row per exact
     * test FQN → the defect, each with a receipt in
     * docs/V7_ASSERT_VERDICT_CHARTER.md §8.0. Consulted only when the
     * platform produced rows that differ (a wall stays a wall; a pass
     * never reaches the ledger), so a register row can hide nothing.
     * <ul>
     *   <li>{@code joinStrings-rendering} — the engine renders
     *       {@code joinStrings([a, b], sep)} as {@code concat(a, b, sep)}
     *       on EVERY dialect (the separator trails instead of joining:
     *       'PeterSmith|'; the digest goldens are md5 of that string);</li>
     *   <li>{@code h2-week-start} — under the engine's
     *       {@code date_trunc('week')} H2 starts the week on Sunday; Pure's
     *       own dateExtension tests say Monday, as ours (and DuckDB) do —
     *       the engine's H2 dialect fails to normalize.</li>
     * </ul>
     */
    private static final Map<String, String> ENGINE_GOLDEN_DEFECTS = Map.of(
            "meta::relational::tests::functions::sqlstring::testToSQLStringForTDSStringJoin",
            "joinStrings-rendering",
            "meta::pure::tds::tests::extensions::testExtendDigest_Relational",
            "joinStrings-rendering",
            "meta::relational::tests::tds::tdsJoin::alloy::testJoinWithExtendWithDigestOnColumnsOnBothQueries",
            "joinStrings-rendering",
            // traced 2026-09-05: its golden's `tds_digest` column is
            // rawtohex(hash('MD5', concat(FIRSTNAME, LASTNAME, '|'))) —
            // the golden-only row is Anthony Allen with
            // aceae941… = md5('AnthonyAllen|'), ours 0a8c4f1f… =
            // md5('Anthony|Allen'); the five other columns agree cell for
            // cell (lowercase hex on both sides)
            "meta::relational::tests::functions::sqlstring::testHashFunctions",
            "joinStrings-rendering",
            "meta::relational::tests::functions::sqlstring::testToSqlGenerationFirstDayOfWeek",
            "h2-week-start",
            // traced 2026-09-05: <<test.AlloyOnly>> — the executor's
            // relational adjust(date, 0, DAYS) comes back a TIMESTAMP and
            // prints '2014-12-01T00:00:00.000000000+0000'; the interpreter
            // sibling (columnValueDifferenceTest, line 152 of
            // testTdsExtension.pure) asserts the SAME relational rows as
            // '2014-12-01'. Every other cell agrees. Pure's adjust keeps
            // the input's precision (PCT); ours prints the date.
            "meta::pure::tds::tests::extensions::columnValueDifferenceWithoutPrevalTest",
            "alloy-adjust-widening",
            // batch 70 (2026-09-05, corrected): the qualifier
            // employeesByCityOrManagerAndLastName ends in ->toOne() (Person[1]),
            // so for a firm with no matching Smith pure's `[]->toOne()` is a
            // RUNTIME ERROR — pure has no answer. The engine never raises it
            // relationally: its DEFAULT strategy drops the parent (1 row, the
            // structure goldens, row-identical to ours) and its FORCED debug
            // strategy (RelationalDebugContext.forcedIsolation =
            // BuildCorrelatedSubQuery) keeps it with a NULL (4 rows, these
            // goldens). Two conventions for an undefined case: a DECISION row,
            // not a defect and not a divergence of ours.
            "meta::relational::tests::advanced::forced::structure::testQualifierWithOperation",
            "decision:empty-toOne-forced-isolation",
            "meta::relational::tests::advanced::forced::structure::testTwoQualifiersWithOperation",
            "decision:empty-toOne-forced-isolation",
            // batch 72a (2026-09-05): both goldens end in `]"` — a stray
            // quote after the JSON array. The engine's assertJsonStringsEqual
            // → equalJsonStrings → json-simple JSONParser returns after the
            // first complete value (probed on the 1.1.1 jar: `[{"id":2}]"`
            // parses, `[…] junk` throws). Our rows are byte-identical to
            // the golden up to that tail (probe 2026-09-05); the strict
            // parse names the GOLDEN ("golden JSON does not parse").
            "meta::relational::graphFetch::tests::embedded::otherwise::testMilestonedRootAndMilestonedProperty",
            "malformed-json-golden",
            "meta::relational::graphFetch::tests::milestoning::testMilestonedRootAndMilestonedProperty",
            "malformed-json-golden");

    /** The bucket of a failing ASSERT of {@code test} (exact FQN): the
     * reason's bucket, refined to the registered engine-golden defect
     * only when rows were produced and differ. */
    public static String bucketOf(String test, String reason, boolean subjectIsSqlText) {
        String bucket = bucketOf(reason, subjectIsSqlText);
        String defect = ENGINE_GOLDEN_DEFECTS.get(test);
        // rows produced and judged (divergence / sql-text), or the golden
        // itself refused by the strict parser after our rows came back
        boolean rowsDiffer = bucket.equals("divergence") || bucket.equals("sql-text-assert")
                || String.valueOf(reason).contains("golden JSON does not parse");
        if (defect == null || !rowsDiffer) {
            return bucket;
        }
        // a registered "decision:<name>" is the bucket verbatim (the golden
        // is one engine convention for a case pure leaves undefined)
        return defect.startsWith("decision:") ? defect : "engine-golden-defect:" + defect;
    }

    /** The bucket of a whole-test fallback reason (the flip's reason text). */
    public static String bucketOf(String reason, boolean subjectIsSqlText) {
        String r = String.valueOf(reason);
        if (r.startsWith("assert-free")) {
            return "zero-assert";
        }
        if (r.contains("generateObjectReferences")) {
            return "decision:objectReferenceIn";
        }
        if (r.contains("routeFunction")) {
            return "decision:routeFunction";
        }
        if (r.contains("compileLegendGrammar") || r.contains("getNoArgFlattenMapping")) {
            return "decision:dynamic-compilation";
        }
        if (r.contains("transformPlan") || r.contains("PureModelContextData")) {
            return "decision:protocol-transform";
        }
        if (r.contains("convertSemiStructuredArrayFlatten") || r.contains("recursive")) {
            return "decision:recursion";
        }
        if (r.contains("chained fetch")) {
            return "decision:tdg-chained-fetch";
        }
        if (r.contains("population statement of a chained plan")) {
            // the engine's two-statement in-list plan asserted by index:
            // golden(0) is its population statement — a plan-structure
            // contract with no counterpart in our one-statement plan
            return "decision:plan-structure";
        }
        if (r.contains("rows underivable") && r.contains("does not exist")) {
            // the frame's own rows could not be produced because the
            // fixture never created the table (testProp3's m2m2r schema
            // has no setUp anywhere in the engine): nobody can replay a
            // plan over a store that was never seeded — the engine's own
            // test is plan-text only, by construction
            return "referee-cannot-replay:no-fixture";
        }
        if (r.contains("oracle declined") || r.contains("rows underivable")
                || r.contains("declined:")) {
            return "referee-cannot-replay";
        }
        if (r.startsWith("platform-fail")) {
            return subjectIsSqlText ? "sql-text-assert" : "divergence";
        }
        if (r.startsWith("wall-type")) {
            return "wall:typer";
        }
        if (r.startsWith("wall-resolve")) {
            return "wall:resolver";
        }
        if (r.startsWith("wall-exec")) {
            String m = r.toLowerCase(java.util.Locale.ROOT);
            if (m.contains("lowering") || m.contains("no sql type") || m.contains("dialect")) {
                return "wall:lowering";
            }
            if (m.contains("resolvable") || m.contains("substitutable") || m.contains("navigation")
                    || m.contains("store resolution") || m.contains("mappingresolution")) {
                return "wall:resolver";
            }
            if (m.contains("typeinferenceexception") || m.contains("unknown function")) {
                return "wall:typer";
            }
            return "wall:exec";
        }
        return "wall:" + r.replaceAll(":.*", "");
    }

    /** The ledger text: totals by bucket, then one block per test. */
    public static String render() {
        Map<String, Integer> totals = new LinkedHashMap<>();
        StringBuilder rows = new StringBuilder();
        List<String> tests = new ArrayList<>(rows().keySet());
        for (String t : tests) {
            List<Row> rs = rows().get(t);
            for (Row r : rs) {
                totals.merge(r.outcome(), 1, Integer::sum);
                rows.append("- ").append(r.test().substring(r.test().lastIndexOf("::") + 2))
                        .append(" #").append(r.ordinal()).append(' ').append(r.form())
                        .append(" -> ").append(r.outcome());
                if (!r.detail().isEmpty()) {
                    rows.append(": ").append(r.detail().replace("\n", "\\n"));
                }
                rows.append('\n');
            }
        }
        StringBuilder sb = new StringBuilder();
        sb.append("\n### assert ledger (partial and failing tests; clean tests count at the test level)\n\n");
        sb.append("tests in the ledger: ").append(tests.size()).append("\n\n");
        sb.append("| bucket | asserts |\n|---|---|\n");
        for (var e : totals.entrySet()) {
            sb.append("| ").append(e.getKey()).append(" | ").append(e.getValue()).append(" |\n");
        }
        sb.append("\n").append(rows);
        return sb.toString();
    }
}
