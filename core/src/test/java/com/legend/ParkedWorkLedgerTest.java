// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

/**
 * THE PARKED-WORK LEDGER (2026-09-15). Work deliberately NOT done yet is
 * recorded in {@code docs/PARKED_WORK_LEDGER.md} — one row per item, each
 * with the date, who parked it, why, the cost of leaving it parked, and the
 * acceptance test that closes it — and every row is ANCHORED here.
 *
 * <p>An anchor is a mechanical fact about today's code that holds only while
 * the item is still parked: the wall a missing capability raises, the single
 * call site a missing pass would displace. It turns "we'll remember" into a
 * red test. When the situation changes — someone builds the capability, or
 * the shape drifts — this goes red and the row must be CLOSED or restated in
 * the same commit.
 *
 * <p>A green anchor is not approval: each row is a debt with a stated price.
 * Rows leave by being fixed, never by being loosened.
 */
@Tag("guardrail")
class ParkedWorkLedgerTest {

    /** Ledger row id &rarr; (what the anchor matches, the product files that
     * may contain it). The file LIST is the pin: a new site, a removed site
     * or a moved site all fail. */
    private static final Map<String, Anchor> REGISTER = new TreeMap<>(Map.ofEntries(
            // PARK-1: cross-store associations require ONE shared predicate;
            // the engine's model is per-end. The wall is the anchor, and its
            // two sites also pin the duplicated implementation.
            Map.entry("PARK-1 xstore per-end predicates",
                    new Anchor("has direction-specific conditions",
                            List.of("MappingNormalizer.java", "XStorePureEnds.java"))),
            // PARK-2: no common-subexpression pass in the normal lowering
            // path, so a union read twice is built twice. The CTE builder is
            // reachable ONLY from the opt-in parity post-processor.
            Map.entry("PARK-2 union common-subexpression pass (call site)",
                    new Anchor("extractSubqueriesAsCtes\\(", List.of("SqlPostProcessors.java"))),
            // leg 3.1 (2026-09-18): VerdictSql builds a WITH too — the
            // database-mode verdict statement (two side CTEs + one verdict
            // row), NOT a common-subexpression pass; PARK-2 stays parked
            // leg 3.4 step 2 (2026-09-20): SqlWith.prepend hoists a statement's
            // frame CTEs to its head — a construction helper, not a pass
            Map.entry("PARK-2 union common-subexpression pass (construction)",
                    new Anchor("new SqlWith\\(", List.of("SqlRewriter.java", "SqlWith.java", "VerdictSql.java"))),
            // PARK-3: the relational toString renders as the DATABASE's cast
            // in the engine; ours passes through to pure's ISO form. The
            // obvious arm collapses multiplicity (it LOST a corpus row), so
            // NOTHING dispatches on the member — that is the anchor.
            // Restated 2026-10-06 (the build rebuild's Phase 2): the platform's
            // decisions left the generated registry for DynaFnDecisions, which
            // names every decided member, TO_STRING's PURE decision among them.
            // That row is the one site; an arm naming it anywhere else is red.
            Map.entry("PARK-3 toString emits pure's ISO form, not the database's cast",
                    new Anchor("DynaFn\\.TO_STRING", List.of("DynaFnDecisions.java"))),
            // PARK-4: the ~groupBy wrapper. The prune's refusal to touch a
            // grouped select is NOT the cause (the engine projects those
            // columns too — lifting it LOST a row); the wrapper needs a
            // select-merge pass. The refusal is the anchor.
            Map.entry("PARK-4 the ~groupBy wrapper projects unread columns",
                    new Anchor("projections\\(\\)\\.isEmpty\\(\\) \\|\\| sel\\.distinct\\(\\)\\s*\\n\\s*\\|\\| !sel\\.groupBy\\(\\)",
                            List.of("SubselectPrune.java"))),
            // PARK-5 to PARK-14: the build rebuild's debts (2026-10-07, the user: fixed correctly after the program
            // lands, never worked around meanwhile). PARK-5: a platform call's names are worked out again at every
            // check, from the spelling.
            Map.entry("PARK-5 a platform call is never resolved once",
                            new Anchor("BareNames\\.catalog\\(", List.of("ResolvedNames.java"))),
            // PARK-6: a receiver typed to choose a route, then typed again by the route (the dot-call branch, the
            // auto-map probe, the legacy-TDS receiver checks)
            Map.entry("PARK-6 arguments typed more than once",
                            new Anchor("recv = (t\\.)?synth\\(af\\.parameters\\(\\)\\.get\\(0\\), env\\)",
                                    List.of("CallShapes.java", "TdsDesugars.java", "Typer.java"))),
            Map.entry("PARK-6 arguments typed more than once (the property's body call re-applied)",
                            new Anchor("applyGeneric\\(new AppliedFunction\\(d\\.bodyFunctionFqn\\(\\), qargs\\), env\\)",
                                    List.of("Overloads.java", "Typer.java"))),
            Map.entry("PARK-6 arguments typed more than once (the receiver-owned check)",
                            new Anchor("Type rt = t\\.synth\\(recv, env\\)", List.of("ReceiverOwnedFunctions.java"))),
            Map.entry("PARK-6 arguments typed more than once (the must-inline substitution)",
                            new Anchor("subst\\.put\\(chosen\\.parameters\\(\\)\\.get\\(i\\)\\.name\\(\\), af\\.parameters\\(\\)\\.get\\(i\\)\\)",
                                    List.of("Overloads.java"))),
            // PARK-7: Any ranked with the type parameters, legend-pure's literal order as the last tie-break
            Map.entry("PARK-7 Any ranked with the type parameters",
                            new Anchor("anyConcrete", List.of("InferenceKernel.java"))),
            // PARK-8: tie-breaks legend-pure does not have
            Map.entry("PARK-8 tie-breaks legend-pure does not have",
                            new Anchor("nativeWinners", List.of("InferenceKernel.java"))),
            // PARK-9: fits only this compiler's acceptance test admits, ranked at one fixed distance
            Map.entry("PARK-9 the acceptance test admits what legend-pure rejects",
                            new Anchor("PLATFORM_RULE_DISTANCE", List.of("InferenceKernel.java"))),
            // PARK-10: relation columns, type operations and type arguments not ported
            Map.entry("PARK-10 parts of legend-pure's ranking not ported",
                            new Anchor("Type\\.SchemaAlgebra ignored -> FunctionMatch\\.TypeFit\\.NULL",
                                    List.of("InferenceKernel.java"))),
            Map.entry("PARK-10 parts of legend-pure's ranking not ported (relation columns)",
                            new Anchor("Type\\.RelationType ignored -> FunctionMatch\\.TypeFit\\.of\\(FunctionMatch\\.Kind\\.RELATION\\)",
                                    List.of("InferenceKernel.java"))),
            Map.entry("PARK-10 parts of legend-pure's ranking not ported (type arguments by position)",
                            new Anchor("if \\(actualArgs\\.size\\(\\) == fg\\.arguments\\(\\)\\.size\\(\\)\\)",
                                    List.of("InferenceKernel.java"))),
            // PARK-11: legacy TDS functions recognized by name, falling back to the spelling
            Map.entry("PARK-11 legacy TDS functions and agg by name, not rows",
                            new Anchor("candidateFqns\\(\\)\\.isEmpty\\(\\) \\? name\\.equals\\(bare\\(\\)\\)",
                                    List.of("TdsLegacy.java"))),
            // PARK-15 (2026-10-08, execution plan boundary step 2): the legacy plan
            // picks an enumeration mapping without the place it is used — the first
            // over the enum for a parameter, the first declared for a result column
            // whose mapping names none; legend-engine chooses per place, from the
            // property mapping. The three choices with no place are the anchors.
            Map.entry("PARK-15 the legacy plan's enum parameter map (PlanText)",
                    new Anchor("var em = enumMappingOf\\(ctx, mappingFqn, enumFqn\\);", List.of("PlanText.java"))),
            Map.entry("PARK-15 the legacy plan's enum parameter map (PlanAllocations)",
                    new Anchor("PlanText\\.enumMappingOf\\(\\s*env\\.ctx\\(\\), pmr\\.fullPath\\(\\), et\\.fqn\\(\\)\\)",
                            List.of("PlanAllocations.java"))),
            Map.entry("PARK-15 the legacy plan's enum result-column fallback",
                    new Anchor("candidates\\.isEmpty\\(\\) \\? null\\s*\\n\\s*: candidates\\.get\\(0\\)\\.mappingId\\(\\)",
                            List.of("PlanText.java"))),
            // PARK-16 (2026-10-08, the user: "record the ddl fix so that we actually
            // do it"; restated 2026-10-09 when DDL and DML were fixed, "product now,
            // generator later"): the test-data generator's hand-built SQL spells a
            // table or schema name RAW, outside the dialects.
            Map.entry("PARK-16 the test-data generator's hand-built SQL spells names raw",
                    new Anchor("\\|\\| \"default\"\\.equals\\(schema\\) \\? table : schema \\+ \"\\.\" \\+ table;",
                            List.of("TestDataGenerator.java"))),
            // PARK-17 (2026-10-09, the DataCube + Python line, on the protocol
            // program's leg 4): Python's refusal kind is the engine's for its
            // grammar and a Java class name for the rest, until leg 6. Leg 6's
            // one whole-model compile replaces the copy PureV1Api.compile strings
            // together: that copy is the anchor.
            Map.entry("PARK-17 Python's refusal kind is mixed until leg 6",
                    new Anchor("com\\.legend\\.Compiler\\.compileAllBodies\\(\\s*\\n\\s*com\\.legend\\.Compiler\\.compileModel\\(",
                            List.of("PureV1Api.java"))),
            // PARK-18 (2026-10-09, E-4b): the legacy printer writes no null placement, the
            // IR not telling a query's explicit emptyFirst()/emptyLast() from pure's own null order
            Map.entry("PARK-18 the legacy printer cannot write an explicit null placement",
                    new Anchor("protected String aggOrderNullPlacement\\(com\\.legend\\.sql\\.SqlSelect"
                            + "\\.SortKey k\\) \\{\\s*return \"\";", List.of("EngineStyleH2.java"))),
            // PARK-20 (2026-10-09, step 2's landing 2 slice (e)): DuckDB's driver makes a decimal array of
            // scale 3, and a Date's or Number's list has no one element type
            Map.entry("PARK-20 a list of decimals, Dates or Numbers is not bound as a parameter",
                    new Anchor("a list of decimals, Dates or Numbers has no one element type",
                            List.of("QueryParameters.java"))),
            // PARK-21 (2026-10-09, step 2's landing 2 audit): what a plan does not bind yet, each refused by name
            Map.entry("PARK-21 an optional enumeration's absence is not bound",
                    new Anchor("an optional enumeration's absence is not bound", List.of("QueryParameters.java"))),
            Map.entry("PARK-21 a class instance is not bound as a plan's parameter",
                    new Anchor("a class instance is not bound as a plan's parameter", List.of("QueryParameters.java"))),
            Map.entry("PARK-21 a Byte, LatestDate or StrictTime value is not bound",
                    new Anchor("a Byte, LatestDate or StrictTime value is not bound", List.of("QueryParameters.java"))),
            // (2026-10-09, step 3's audit): the runner refuses the same, should a plan carry a Byte or a Variant
            // PARK-24 (2026-10-10, measured against Pure and legend-engine): lite's answers differ from Pure's values
            // in four places, Float arithmetic in decimal the first (the numeric charter's Rule 1: a literal bare)
            Map.entry("PARK-24 lite's answers differ from Pure's values in four measured places",
                    new Anchor("static String plainFloat\\(", List.of("AnsiSqlRenderer.java"))),
            Map.entry("PARK-21 the runner binds no Byte or Variant value",
                    new Anchor("is not bound by a plan \\(PARK-21\\)", List.of("PlanParameters.java"))),
            // PARK-22 (2026-10-09, the protocol program's leg 5): engine JSON with spans for a path literal across
            // lines is refused, the spans not saying where its lines break; designed with leg 8
            Map.entry("PARK-22 engine JSON with spans for a path literal across lines is refused",
                    new Anchor("a multi-line path literal span", List.of("SpecIslandReader.java")))));

    private record Anchor(String pattern, List<String> files) {
    }

    @Test
    @DisplayName("every parked row's anchor still holds (docs/PARKED_WORK_LEDGER.md)")
    void parkedRowsStillHold() throws IOException {
        List<Path> sources = mainSources();
        for (var row : REGISTER.entrySet()) {
            Anchor anchor = row.getValue();
            Pattern p = Pattern.compile(anchor.pattern());
            List<String> found = new java.util.ArrayList<>();
            for (Path f : sources) {
                if (p.matcher(Files.readString(f)).find()) {
                    found.add(f.getFileName().toString());
                }
            }
            java.util.Collections.sort(found);
            List<String> expected = anchor.files().stream().sorted().toList();
            assertEquals(expected, found, () -> "PARKED WORK CHANGED — " + row.getKey()
                    + ": the anchor /" + anchor.pattern() + "/ no longer sits in exactly "
                    + expected + ". If you CLOSED this item, delete its row here and in"
                    + " docs/PARKED_WORK_LEDGER.md in this commit. If you moved the code,"
                    + " re-point the row. A parked item is a debt with a stated price —"
                    + " it never leaves by being loosened.");
        }
    }

    private static List<Path> mainSources() throws IOException {
        try (Stream<Path> s = SourceFiles.under("core/src/main/java").stream()) {
            List<Path> out = s.filter(p -> p.toString().endsWith(".java")).toList();
            GuardCoverage.assertFloor("ParkedWorkLedgerTest", out.size(), 490);
            return out;
        }
    }
}
