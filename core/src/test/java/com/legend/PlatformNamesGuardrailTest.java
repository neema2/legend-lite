// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The two ratchets of docs/EXECUTION_CONTEXT_DESIGN_2026_09_06.md (user
 * ruling 2026-09-06: "are we building a general purpose pure runner or a
 * super hard-coded test runner?").
 *
 * <ol>
 *   <li>A Pure function or class NAME is spelled in ONE place — the catalog
 *       ({@link com.legend.compiler.element.type.PlatformTypes}). Every
 *       other file asks the catalog. The literal {@code equals("meta::…")}
 *       checks outside it are counted and may only SHRINK.</li>
 *   <li>The execution context is a VALUE read once: no file outside
 *       {@link com.legend.compiler.spec.typed.ExecutionContext} walks a
 *       runtime expression for its shape. The retired walkers are named
 *       so they cannot return under their old spelling.</li>
 * </ol>
 */
@Tag("guardrail")
class PlatformNamesGuardrailTest {

    private static final String MAIN = "core/src/main/java/com/legend";
    private static final Pattern LITERAL_NAME_CHECK =
            Pattern.compile("equals\\(\"meta::");
    /** The retired runtime-shape walkers: their names may not reappear as
     * methods anywhere outside the one reader. */
    private static final List<String> RETIRED_WALKERS = List.of(
            "chainMappingsIn(", "jsonSourcesIn(", "sqlSetupsIn(", "setupsIn(",
            "connectionNameIn(", "quoteIdentifiersOf(", "timeZoneOf(",
            "connectionInstanceOf(", "databaseTypeOf(", "connectionStoreElementOf(",
            "runRuntimeSetups(");

    @Test
    void pureNamesAreSpelledInTheCatalogOnly() throws IOException {
        int count = 0;
        StringBuilder where = new StringBuilder();
        try (Stream<Path> files = SourceFiles.under(MAIN).stream()) {
            for (Path f : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                if (f.getFileName().toString().equals("PlatformTypes.java")) {
                    continue;
                }
                Matcher m = LITERAL_NAME_CHECK.matcher(Files.readString(f));
                int n = 0;
                while (m.find()) {
                    n++;
                }
                if (n > 0) {
                    count += n;
                    where.append(SourceFiles.rel(MAIN, f).substring(1)).append('=').append(n).append(' ');
                }
            }
        }
        // 73 at batch 114 (2026-09-06) — SHRINK-ONLY: every burn moves a
        // spelling into PlatformTypes; a new literal check anywhere else
        // fails here
        // 60 measured 2026-09-11 (was 73 at batch 114): re-pinned to the
        // measurement — headroom is not a pin
        assertTrue(count <= 60, "literal Pure-name checks outside PlatformTypes grew: "
                + count + " > 60 — " + where);
    }

    /** The catalogs — the files where a Pure FUNCTION name may be spelled
     *  as a literal: the signatures, the registered families, the subsumed
     *  and walled registries, the system metamodel, the type spellings. */
    private static final java.util.Set<String> CATALOG_FILES = java.util.Set.of(
            "Pure.java", "NativeFn.java", "Subsumed.java", "SystemMetamodel.java",
            "PlatformTypes.java", "WalledBodies.java",
            // the legacy TDS vocabulary as a closed enum (batch 5 audit leg C,
            // 2026-09-11): upstream tds.pure / math::olap identities the Typer
            // desugars by exact spelling — a catalog of names, like NativeFn
            "TdsLegacy.java",
            // the language forms' ownership registry (untangle step 2,
            // 2026-09-24): each form names the exact FQNs whose overloads it
            // owns — a registration, like a NativeFn member's
            // DynaFn.java left 2026-10-06 (the build rebuild's Phase 2): it is
            // generated from the engine alone and names no declaration; a PURE
            // name's FQNs are joined from the engine surface when it loads
            // (DynaFnDecisions), so no file spells them
            "CoreFn.java");

    /** A function FQN literal: {@code "meta::…::lowerCamel"} (a class or enum
     *  FQN ends in an upper-case segment and is a TYPE spelling, not dispatch). */
    private static final Pattern FUNCTION_FQN_LITERAL =
            Pattern.compile("\"meta::[A-Za-z_:]*::[a-z][A-Za-z0-9_]*\"");

    /** A bare-name switch arm: {@code case "lowerCamel"} — dispatch on a
     *  simple function name, the string-dispatch shape batch 4b retired from
     *  the executor. The parser's section grammars switch on KEYWORDS and
     *  are not counted. */
    private static final Pattern BARE_NAME_ARM =
            Pattern.compile("case \"[a-z][A-Za-z0-9]*\"");

    @Test
    void functionFqnLiteralsOutsideTheCatalogsOnlyShrink() throws IOException {
        int count = 0;
        StringBuilder where = new StringBuilder();
        try (Stream<Path> files = SourceFiles.under(MAIN).stream()) {
            for (Path f : files.filter(p -> p.toString().endsWith(".java")).sorted().toList()) {
                if (CATALOG_FILES.contains(f.getFileName().toString())) {
                    continue;
                }
                int n = 0;
                for (String line : Files.readAllLines(f)) {
                    String code = line.stripLeading();
                    if (code.startsWith("//") || code.startsWith("*") || code.startsWith("/*")) {
                        continue;
                    }
                    Matcher m = FUNCTION_FQN_LITERAL.matcher(line);
                    while (m.find()) {
                        n++;
                    }
                }
                if (n > 0) {
                    count += n;
                    where.append(SourceFiles.rel(MAIN, f).substring(1)).append('=').append(n).append(' ');
                }
            }
        }
        // MEASURED 2026-09-11 (upstream boundary, after batch 4b): the
        // compiler's own string dispatch — SHRINK-ONLY; a new function-name
        // literal anywhere but a catalog fails here (USER: "only on typed
        // things that are registered")
        assertTrue(count <= FUNCTION_FQN_LITERALS_MAX, "function-FQN literals outside the catalogs grew: "
                + count + " > " + FUNCTION_FQN_LITERALS_MAX + " — " + where);
    }

    @Test
    void bareNameSwitchArmsOutsideTheParserOnlyShrink() throws IOException {
        int count = 0;
        StringBuilder where = new StringBuilder();
        try (Stream<Path> files = SourceFiles.under(MAIN).stream()) {
            for (Path f : files.filter(p -> p.toString().endsWith(".java")).sorted().toList()) {
                // path ELEMENTS, never a slash-spelled substring: Windows paths
                // are backslash-separated (CI 2026-09-11: the parser package was
                // not excluded there and the count read 417)
                boolean underParser = false;
                for (Path part : Path.of(SourceFiles.rel(MAIN, f).substring(1))) {
                    if (part.toString().equals("parser")) {
                        underParser = true;
                    }
                }
                if (underParser || CATALOG_FILES.contains(f.getFileName().toString())) {
                    continue;
                }
                int n = 0;
                for (String line : Files.readAllLines(f)) {
                    String code = line.stripLeading();
                    if (code.startsWith("//") || code.startsWith("*") || code.startsWith("/*")) {
                        continue;
                    }
                    Matcher m = BARE_NAME_ARM.matcher(line);
                    while (m.find()) {
                        n++;
                    }
                }
                if (n > 0) {
                    count += n;
                    where.append(SourceFiles.rel(MAIN, f).substring(1)).append('=').append(n).append(' ');
                }
            }
        }
        assertTrue(count <= BARE_NAME_ARMS_MAX, "bare-name switch arms outside the parser grew: "
                + count + " > " + BARE_NAME_ARMS_MAX + " — " + where);
    }

    /** Pins, MEASURED at their introduction (2026-09-11) — set from the
     *  first run's count, never from a guess. */
    static final int FUNCTION_FQN_LITERALS_MAX = 338;   // 335 -> 338 (2026-10-22, main's merge into the plan path branch, measured; object identity step 3, 2026-10-20, docs/OBJECT_IDENTITY_2026_10_20.md §3.3): lowering/InstanceEquality, the one owner of every operation built on equal() over objects, claims removeDuplicates, distinct, isDistinct and indexOf by name beside eq, equal, contains and in -- three more names in the owner's claim, not new dispatch elsewhere; its move to the catalog's ids is the identity guard's item; 332 -> 334 (2026-10-18, the Cypher island's nanosecond values as a date and a time of day, docs/CYPHER_TCK_TO_ZERO_PLAN_2026_10_17.md W13): protocol/cypher/CypherToPure spells lite's dateTimeOfText and timeOfText, the natives a nanosecond value leaves the database through, as it spells timeOf and atTime -- two more lite natives, not new dispatch; 331 -> 332 (2026-10-17, the Cypher island's W10 leg 2b, docs/CYPHER_TCK_TO_ZERO_PLAN_2026_10_17.md, docs/CYPHER_STUDY_2026_10_15.md §12.68: protocol/cypher/CypherToPure spells lite's recurse, the recursion an unbounded variable-length pattern walks through, as it spells elementId -- the protocol layer reads no catalog; one more lite native, not new dispatch); 330 -> 331 (2026-10-17, the Cypher island's W11 landing, docs/CYPHER_TCK_TO_ZERO_PLAN_2026_10_17.md, docs/CYPHER_STUDY_2026_10_15.md §12.58: the translator spells lite's trustOne, the platform's unguarded to-one conformance, at the orderability key's places its own kind test proves present -- toOne's raise there is a constant Postgres's planner folds at plan time inside a subquery's WHERE; one more lite native, not new dispatch); 327 -> 330 (2026-10-17, the Cypher island's W11, docs/CYPHER_TCK_TO_ZERO_PLAN_2026_10_17.md, docs/CYPHER_STUDY_2026_10_15.md §12.58: the translator names lite's minBy and maxBy and Pure's rowMapper by FQN for min and max over Variants); 326 -> 327 (2026-10-17, the Cypher island's tail leg 2, docs/CYPHER_TCK_TO_ZERO_PLAN_2026_10_17.md W12, docs/CYPHER_STUDY_2026_10_15.md §12.53): protocol/cypher/CypherToPure spells lite's isKind, the database's reading of a Variant's JSON kind, as it spells random -- one more lite native, not new dispatch; 325 -> 326 (2026-10-17, the Cypher island's rand-and-clocks leg, docs/CYPHER_TCK_TO_ZERO_PLAN_2026_10_17.md W3, docs/CYPHER_STUDY_2026_10_15.md §12.51): protocol/cypher/CypherToPure spells lite's random, the native Cypher's rand() reads through, as it spells elementId and timeOf -- one more lite native, not new dispatch; 324 -> 325 (2026-10-17, the Cypher island's identity leg, docs/CYPHER_TCK_TO_ZERO_PLAN_2026_10_17.md W2, docs/CYPHER_STUDY_2026_10_15.md §12.46): protocol/cypher/CypherToPure spells lite's elementId, the one native a node's identity reads through -- the protocol layer reads no catalog (AGENTS invariant 7b), so a lite native's name is spelled there as timeOf, atTime, instantIn and localIn are; one more lite native, not new dispatch
    // 334 -> 335 (2026-10-18, merging main into the IQ island's landing 5): main's 332 -> 334 above and the island's
    // (2026-10-17, the IQ island's scale, docs/IQ_CENSUS_CHECKLIST.md "the refusals the live IQ answers"): protocol/sql/SqlToPure.wholeQuotient names lite's integer quotient, meta::legend::lite::quotient (Pure.Lite.QUOTIENT), which the protocol layer cannot import -- the Cypher translator's lite natives' precedent; it replaced a Float estimate that read each operand about fourteen times
    static final int BARE_NAME_ARMS_MAX = 194;   // 193 -> 194 (2026-10-16, the Cypher island's nanosecond leg, docs/CYPHER_STUDY_2026_10_15.md §12.12): lowering/Scalars.diffPart's table of the platform's DurationUnit names gains NANOSECONDS (the unit dateDiff lowers to the nanosecond epoch), one more row of a table already counted, not new dispatch. 165 -> 193 (2026-10-12, the merge of main ca6c883 into the cloud-backends branch for PR neema2/legend-lite#30): main's #SQL{...}# reader, protocol/sql/SqlParser 15 and SqlToPure 13 (SQL keywords and Postgres function names, a reader of SQL text outside the parser package), landed under main's pin of 145 with main measured at 125; this branch measured 165; the merge joins the two measured sets, no new site. 145 -> 165 (2026-10-12, Trino and ClickHouse dialects, docs/CLOUD_BACKENDS_PCT_2026_10_11.md §18-19): each dialect's spelling of the platform's date parts and interval units (year, quarter, doy, microsecond ...), keyed by the MIR's part names as Databricks' 13 and Snowflake's 13 are -- Trino 14, ClickHouse 19 at this count; a SQL date part, not function dispatch in the compiler. 116 -> 145 (2026-09-27, T4a): protocol/PureComposer, the WIRE PRINTER (jsonToGrammar) -- its arms are protocol _type tags and upstream's printer's own special names (cast, new, not, if ...), the mirror of ProtocolReader's 19 wire-tag arms already counted; not function dispatch in the compiler

    @Test
    void runtimeShapesAreReadByTheOneReaderOnly() throws IOException {
        StringBuilder found = new StringBuilder();
        try (Stream<Path> files = SourceFiles.under(MAIN).stream()) {
            for (Path f : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                if (f.endsWith("ExecutionContext.java")) {
                    continue;
                }
                String text = Files.readString(f);
                for (String w : RETIRED_WALKERS) {
                    if (text.contains(" " + w) || text.contains("." + w)) {
                        found.append(SourceFiles.rel(MAIN, f).substring(1)).append(':').append(w).append(' ');
                    }
                }
            }
        }
        assertEquals("", found.toString(),
                "a runtime-shape walker reappeared outside ExecutionContext.Reader: ");
        assertTrue(!SourceFiles.has(MAIN + "/ConnectionFlags.java"),
                "ConnectionFlags is retired: its readers live in ExecutionContext.Reader");
    }
}
