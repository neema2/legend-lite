// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE COMPATIBILITY SPELLING GUARD: phase D's D2 (docs/data-delivery-homework/D_BRIEF_2026_11_10.md §4(a); the user,
 * 2026-11-12: "push ALL compatibility to the edge so never have to worry about leaking into product"). No product
 * source outside {@code com.legend.edge} carries one of legend-pure's or legend-engine's spellings: an upstream error
 * sentence raised in SQL or thrown at compile time (the D0 inventory's K1, K3, K8), Pure's print forms ({@code +0000},
 * {@code @_}, the TDS null cell, {@code %r}, Pure's Float text), the engine's trace comment, the engine-text printers,
 * the engine writers, the engine-corpus scan order and lexicon, or a compatibility mode a product class branches on.
 *
 * <p>The scan: every main source of core outside the edge package, comments stripped; each string literal is matched
 * against {@link #LITERALS} and the code (literals blanked) against {@link #SYMBOLS} as whole words. Every hit is
 * REGISTERED ({@link #REGISTER}) by file and spelling, with the count and the D step that removes it, exact both ways:
 * a new hit fails (it is compatibility leaking into the product: spell it at the edge), a count that falls fails until
 * re-pinned (the list is SHRINK-ONLY: a pin rises only with a dated justification, AGENTS.md "Guard files are
 * load-bearing"). D2 ends with no row naming a D step: what remains is {@code JUDGE} (the database and host judges'
 * own canon, the test road's lattice, D0 §10) or {@code STAYS} (a product semantic that happens to share the
 * spelling, its reason given).
 *
 * <p>Limits, stated (the orchestrator's caution, 2026-11-12): a fixed list catches only what is on it -- this is the
 * drift alarm. The weight is carried by the structural guards: {@code ArchitectureTest.noProductClassReachesTheEdge}
 * (G5, with its planted violator), the typed raise (a product raise is a {@code RaiseKind}, D1.1) and the hook
 * implemented only at the edge (D1.0).
 */
@Tag("guardrail")
class CompatSpellingGuardTest {

    private static final String MAIN = "core/src/main";
    private static final String JAVA_ROOT = "core/src/main/java/com/legend/";
    private static final String EDGE = "core/src/main/java/com/legend/edge/";

    /** Upstream's texts, matched inside a string literal (a pattern in {@code ^...$} matches the whole literal). */
    static final List<String> LITERALS = List.of(
            // K1: legend-pure's runtime sentences raised in SQL (D1.1); its compile-time twin in TdsDesugars (D1.6)
            "Cannot divide ", "Division by zero", "Unsupported number of bits to shift", "^Cannot get $",
            "trying to get an element at offset", "Unable to compute sqrt of", "Cannot cast a collection of size",
            "can't be higher than the high bound", "Invalid day: ", "Cast exception: ", "range step must not be 0",
            "Unable to evaluate constraint",
            // narrowed 2026-11-12 (D1.1): the whole of lite's old insert sentence, so the product's own wording of an
            // index's raise (exec.RaisedErrors, "index out of bounds") is no hit
            "add(set, index, value): index out of bounds",
            // K3: the reference database's (DuckDB's) words raised on another database (D1.1)
            "The map key list does not align", "Map keys can not be NULL", "Map keys must be unique",
            "Could not convert string '", "Could not parse '",
            // K8: legend-engine's compile and server sentences (D1.6; the query store's at pure/v1, D1.8)
            "Can't find ", "Invalid supertype", "arguments count (", "variables count (",
            "Missing external parameter(s)",
            // 2026-11-17, J10 of the edge audit: legend-engine's and legend-pure's model sentences, raised as compile
            // kinds (CompileKind.DUPLICATE_ELEMENT ... MISSING_STORE_TABLE) and worded at the edge; listed with no row
            "Duplicated class mappings", "Duplicated mapping include", "Duplicated element", "Found duplicated ",
            "can't be found", "Default values are supported only",
            // legend-pure's match failure (W7 of docs/records/d-plan-2026-11-16.md, 2026-11-16: raised as a kind,
            // MATCH_FAILURE, its sentence the edge's); its collection offset sentence is listed with K1 above (the P2
            // checkpoint audit's S6, 2026-11-16: it was listed twice)
            "Match failure:",
            // legend-pure's window frame assert, and the "Invalid window frame boundary - " opening the edge gives S170's
            // refusal (the P2 checkpoint audit's M4, 2026-11-16: raised as compile kinds, worded at the edge)
            "Invalid window frame boundary",
            // K9 and K7: Pure's print forms and the engine's trace comment
            "+0000", "@_", "executionTraceID",
            // K4: the engine corpus's scan order flag
            "legend.exec.engineScanOrder",
            // legend-engine's TDS builder's type, and the session statement freeing the engine's words on H2 (the P2
            // checkpoint audit's S5, 2026-11-16: until then caught by no list). "TDSNull" is not listed yet: as a
            // literal it is also the Pure type's own name (16 product literals), so its sites are sorted in Phase 7
            "tdsBuilder", "SET NON_KEYWORDS",
            // legend-engine's text of an empty TDS cell (the separation census's B10, 2026-11-17: until then not listed)
            "TDSNull");

    /** The compatibility machinery, matched as whole words in code. */
    static final List<String> SYMBOLS = List.of(
            "FLOAT_PRINT", "FLOAT_CANON", "TDS_NULL_CELL", "dateLiteralPrint", "signedZeroPad", "Repr",
            "presentedName", "PureV1Writer", "TdsObjectsWriter", "traceComment",
            "StableScanOrder", "ENGINE_LEXICON",
            "withEngineText", "EngineText", "EnginePlans", "EngineTextDialect", "EngineStyleH2", "EngineStyleDB2",
            "EngineStyleComposite", "RawSqlBoundary",
            "COMPAT",
            // the judges' compatibility mode (the separation census's C1, 2026-11-17: its branches outside TestPlanner
            // stood unlisted)
            "COMPILED_PURE_COMPAT");

    /** The steps a row may name: a D slice that removes it, or a terminal class that keeps it, with its reason. */
    static final Set<String> STEPS = Set.of(
            "D1.1",   // Raise/RaiseKind: a product raise is a kind and a place; legend-pure's sentence is the edge's
            "D1.2",   // the scalar text nodes: Float, DateTime and the literal print forms; TestDataPlanner's COMPAT
            "D1.3",   // lists, maps, makeString (TDSNull) and format (%r, %0Nd), the instance and type forms refused
            "D1.4",   // the writers: CellText's engine half, PureV1Writer, TdsObjectsWriter, the pivot quoting, the trace
            "D1.5",   // the decorator: the engine corpus's scan order, the engine lexicon
            "D1.6",   // the compile messages in the product's own wording, keyed by kind
            "D1.7",   // engine text (EngineText, EnginePlans' text half, EngineStyle*), RawSqlBoundary, appeals, referee
            "D1.8",   // pure/v1's upstream routes (and the query store they answer) to the edge
            "D1.9",   // the judges' compatibility to the edge, handed in by a hook (the user, 2026-11-15: COMPILED_PURE_COMPAT's
                      // branches, Pure's sentences the asserts expect, the canons that reproduce legend-pure's equality)
            "HOOK",   // the edge's hook: a type the product's plan style names so the edge hands its printers in, the
                      // product's style refusing it by name (O3; the D part 1 audit's nit, 2026-11-15)
            "HOOK-RULED", // a HOOK the orchestrator's ruling of 2026-11-17 closes ("hooks are terminal": an interface of
                      // signatures only, refused by the product by name, implemented in the edge alone, its implementers
                      // pinned -- HookRulingTest): the separation itself, not a leak
            "JUDGE",  // the judges' own canon on the test road (D0 §10): not compatibility, never moved
            "RULED",  // a row the USER RULED stays, its ruling and evidence beside it (U5, 2026-11-15/16: a surviving row
                      // shows who calls it and what breaks without it; added 2026-11-16 with the P2 checkpoint audit's A-M1)
            "STAYS"); // a product semantic sharing the spelling: lowering/DateFormats.java's pattern letter Z is +0000

    /**
     * Every hit standing today, "file | spelling | count | step"; the file is under {@code core/src/main/java/com/legend/}
     * (else under {@code core/src/main/}). Registered 2026-11-12 (phase D, step 1, at 3a20560f9's descendant): 178 rows.
     * SHRINK-ONLY: a row falls or goes as its step lands; a row rises or is added only with a dated justification.
     */
    static final List<String> REGISTER = List.of(
            // D1.6's compile messages, gone 2026-11-16 (W7 of docs/records/d-plan-2026-11-16.md): ModelIntegrity,
            // TypeClassifier, TableReferenceChecker and the normalizer's "Can't find" raise a CompileKind, worded by the
            // product (CompileKind.sentence) and, for upstream's clients, by the edge (PureMessages.compile)
            // D1.3 -> D1.9 (2026-11-15): the constant's users are the judges' (VerdictSql, SideShapes), the #TDS
            // literal's input read (Scalars, STAYS) and the edge (EngineStyleH2); no product text writes it any more
            // compiler/spec/TdsDesugars.java | TDS_NULL_CELL: gone 2026-11-15 (D1.3: a TDS cell's toString is cellText,
            // empty where the cell is in the product, legend-engine's TDSNull at the edge)
            // 2026-11-13 (phase D's O3): the plan style names the type it hands the planner -- the edge's printers
            // implement it; the product's style refuses it by name -- so it stays while the engine-text natives plan
            // 4 -> 5 (2026-11-14, S310 RULED by the user, DE2 (a): an execute's activity SQL is the style's pick, the
            // edge's printer handed to the planner's Activity.engines)
            // reclassified STAYS -> HOOK 2026-11-15 (the D part 1 audit's nit): the edge's hook, not a product semantic
            // 5 -> 9 (2026-11-16, W9's judge half, E-Java: the judges' engine-text methods on the one hook, declared and refused by the product --
            // engineTestText and engineTestPlanText -- so the test planner names no engine text of its own)
            // 9 -> 8 (2026-11-16, W9's planner half: the activity's engines() takes no printer, the edge's facet picks it)
            // GONE 2026-11-17 (Phase 6's A, the orchestrator's approved method list): PlanStyle names no engine text;
            // upstream's forms are its UpstreamPlans facet's, the edge's (8 -> 0, the row deleted)
            // (Missing external parameter(s): gone 2026-11-16, W7: error.MissingParameters, worded by the edge)
            // moved from testplan/ActivityComments.java (2026-11-12, C's J10 merged at 4ef947a: the execution id one per
            // execute(), minted by the door; the activity's comment now the program runner's): a move, the count the same
            // exec/PlanRunner.java | traceComment and executionplan/ExecutionPlan.java's executionTraceID and traceComment:
            // gone 2026-11-15 (D1.4: the runner binds the execution's id; an activity row's comment is the TRACE_TEXT
            // node, the id in the product, legend-engine's comment around it written by the edge, PurePrintForms; the
            // comment's one Java spelling, the engine answer's activity, moved into PureV1Writer, which goes at D1.8)
            "lowering/DateFormats.java | +0000 | 1 | STAYS",
            // 8 -> 7 (2026-11-15, D1.2: dateTimeText's composition moved to sql/dialect/DateTimePrintText, the judges' and
            // the edge's node DATETIME_PRINT; the product's DateTime text is DATETIME_TO_TEXT, ISO with Z). What stays:
            // the judges' canon (temporalCanon, dateTimeLiteralAt: D1.9) and the TDS text's (Render's: D1.3)
            // D1.3 -> D1.9 (2026-11-15, D1.3's end): 4 are the judges' canon (temporalCanon, dateTimeLiteralAt), 3 the
            // TDS text's (Render's dateTimeTextTds, dateTimeTextOfText), which the product now refuses by name
            // (RELATION_TEXT) and only the edge writes: both move with D1.9's hook, which hands the lowering the edge's
            // and the judges' compositions
            // listed 2026-11-16 (the P2 checkpoint audit's S5): an execute's plan JSON written in SQL, legend-engine's
            // builder type; Phase 6 takes the plan text to the edge
            // lowering/JsonEmission.java | tdsBuilder: GONE 2026-11-17 (Phase 6's B5): legend-engine's result envelope is
            // the edge's (edge.EngineResultEnvelopes, the lowering.ResultEnvelopes facet), refused by the product at LOWER
            // 7 -> 6 (2026-11-17, F's separation census line B11): the dead pair dateTimeLiteral / dateTimePrint (no
            // caller) deleted. 6 -> 3 (2026-11-17, B17's forwarders: dateTimeTextTds and dateTimeTextOfText, which only a
            // relation's text wrote, moved to edge.RelationTextLowering). What stays: the judges' canon (temporalCanon x2,
            // dateTimeLiteralAt: census line C5, E-Java's)
            // 3 -> 1, D1.9 -> STAYS (2026-11-17, census C5's step C, E-SQL's): the judges' canon is lite's spelling
            // (temporalCanon, dateTimeLiteralAt: Z, built from product nodes); what stays reads legend-pure's +0000 as
            // INPUT (a wire text already suffixed is normalized before the canon), no spelling written
            "lowering/LiteralSpelling.java | +0000 | 1 | STAYS",
            // FLOAT_PRINT's rows: GONE 2026-11-17 (census B9, the orchestrator's ruling): measured equal to lite's writers'
            // text inside its windows, the node is lite's canonical Float text, FLOAT_SHORTEST_TEXT, a product node in
            // every dialect and the judges' message text alike; the symbol stays scanned so the old name cannot return
            // FLOAT_PRINT, reclassified D1.2 -> JUDGE 2026-11-15 (phase D's D1.2): the product's text of a Float is
            // FLOAT_TO_TEXT, lite's six decimals (register S311); FLOAT_PRINT, legend-pure's print form, stays in each
            // product dialect as the JUDGES' %r of a Float in an assert's message (lowering/Repr: the database judge
            // prints a value as the host judge does, judge parity G2) -- the edge writes it for FLOAT_TO_TEXT
            // (edge/PurePrintForms). LiteralSpelling's row went (it writes FLOAT_TO_TEXT); Repr's added (the judges' arm)
            // lowering/Lowerer.java | TDS_NULL_CELL: gone 2026-11-15 (D1.3: a row's cells joined skip an empty one, the
            // CELL_TEXT node; legend-engine's TDSNull is the edge's, edge/PurePrintForms)
            // lowering/Lowerer.java | withEngineText: gone 2026-11-17 (Phase 6's B5: the lowering takes a LoweringShape, the
            // product's LEAN; legend-engine's shape is the edge's choice, EnginePlans.UPSTREAM_SHAPE)
            // D1.3 -> D1.9 (2026-11-15): Repr is the judges' %r (and format's %r, which the assert bodies write): it
            // moves with the judges
            // (lowering/Repr.java | Repr: gone 2026-11-17 with the class, census B12's 2b -- legend-pure's representation
            // the edge's EdgePureOnlyTexts, format's %r refused in the product by name)
            // 2 -> 1 (2026-11-15, D1.2: a DateTime literal's text is a node carrying its written text, the edge's +0000).
            // What stays reads a TDS cell's zone suffix as INPUT (a PCT fixture's +0000 or Z dropped), no spelling
            // written: D2 reviews it
            "lowering/Scalars.java | +0000 | 1 | STAYS",
            // lowering/Scalars.java | @_: gone 2026-11-15 (D1.3: an instance's toString is the INSTANCE_TEXT node, refused
            // by the product's renderer -- an object is written by graph fetch -- and composed by the edge)
            // toRepresentation's (STAYS: its answer is Pure's source form by definition, the orchestrator's ruling of
            // 2026-11-15, on D2's list for the user) and format's %r, which the assert bodies' messages write (the
            // judges' Repr, measured 2026-11-15: AssertBarTest's assertNotEq message): it moves with them, D1.9, and
            // the row falls to 1 STAYS
            // 2 -> 1 (2026-11-17, C5 step A: toRepresentation's representation is the edge's, EdgePureOnlyTexts through
            // the PureOnlyTexts facet; format's %r remains, B12's 2b)
            // 1 -> gone (2026-11-17, census B12's 2b: format's %r asks the facet, the edge's formatR)
            // 2 -> 1 (2026-11-15, D1.3: makeString's empty element skipped, the CELL_TEXT node, legend-engine's
            // TDSNull the edge's), reclassified STAYS: what stays reads a #TDS literal's TDSNull cell as NULL -- INPUT,
            // the text legend-pure's TDS literal writes for an empty cell, read, never written: D2 reviews it
            // lowering/Scalars.java | dateLiteralPrint: gone 2026-11-15 (D1.2: dateLiteralText, a literal that is no
            // DateTime; a DateTime literal's text the DATETIME_TO_TEXT node, its written text the edge's)
            // lowering/Scalars.java | signedZeroPad: gone 2026-11-15 (D1.3: format's %0<w>d is the FORMAT_PADDED node,
            // refused by the product's renderer, legend-pure's padding composed by the edge, edge/PurePrintForms)
            // lowering/VerdictSql.java | Repr: gone 2026-11-17 (C5 step A: an assert's message elements are legend-pure's
            // representation, asked of the PureOnlyTexts facet, the edge's EdgePureOnlyTexts)
            // D1.4 -> D1.7 (2026-11-15): PlanNode is legend-engine's plan JSON (generatePlan's nodes), its comment the
            // engine's post-processor's constant: it moves with EnginePlans' text half
            // plan/PlanNode.java | executionTraceID: GONE 2026-11-17 (Phase 6's B5): legend-engine's trace comment is
            // edge.EnginePlans' constant, its one reader
            // planner/BodyPlanner, EnginePlans, EngineText, PlanAllocations, PlanEnvelope and ProductStatements rows:
            // GONE 2026-11-16 (W9's planner half, docs/records/d-plan-2026-11-16.md): upstream's plan text moved to the
            // edge (edge.EnginePlans, EngineText, PlanEnvelope, EngineAllocations), the planner reaching it as the
            // style's facet planner.UpstreamPlans; its routine table (UpstreamPlans.routines) asks the facet when called
            // queries/SavedCubes.java and SavedQueries.java | Can't find: gone 2026-11-16 (W8): a refusal's kind
            // (SavedQueries.Refusal.Kind), legend-engine's words at the edge's mount (EdgeServer.storeWords)
            // 2026-11-14 (merged main 3a217086a, #81): the #TS island reads a Pure date literal's own text into its SQL
            // literal, dropping the zone the compiler's text writes ("+0000"): input read, not a spelling written
            "protocol/ts/TsShow.java | +0000 | 1 | STAYS",
            // resolver/ChainDispatch.java | Cast exception: gone 2026-11-16 (W5 of docs/records/d-plan-2026-11-16.md): a run-time
            // type check raises its kind (Lite.TYPE_RAISE: CAST_IMPOSSIBLE, INSTANCE_NOT_OF), the sentence the edge's
            // resolver/ClassSources.java | TDS_NULL_CELL: gone 2026-11-15 (D1.3: an absent system fact is an empty
            // cell, which a literal cell reads as NULL as it read TDSNull -- an internal marker, never a spelling)
            // resolver/GraphEmission.java | +0000: gone 2026-11-15 (D1.2, the orchestrator's ruling under the user's of
            // 2026-11-10, S312: a graph fetch's key arguments in the plan style's text, PlanStyle.graphKeyDate/String;
            // legend-engine's +0000 and quoting the edge's)
            // resolver/Substitution.java | Cast exception: gone 2026-11-16 (W5 of docs/records/d-plan-2026-11-16.md): a run-time
            // type check raises its kind (Lite.TYPE_RAISE: CAST_IMPOSSIBLE, INSTANCE_NOT_OF), the sentence the edge's
            // result/CellText.java | @_: gone 2026-11-16 (W4 of docs/records/d-plan-2026-11-16.md): lite's writers refuse
            // an instance's text (S314). +0000: back 2026-11-16 (W4's audit, DoD-2): legend-pure's date text, the pure/v1
            // Arrow route's (CellText.DateText.LEGEND_PURE), where the product writes S312's six digits and Z; it moves to
            // the edge with the engine writers. +0000 and presentedName: gone 2026-11-16 (W8 of
            // docs/records/d-plan-2026-11-16.md): legend-pure's date text and legend-engine's pivot quoting are
            // edge.Presenter
            // result/CsvWriter.java and result/JsonRowsWriter.java | presentedName: gone 2026-11-15 (D1.4: a pivot's
            // generated name bare in lite's writers, register S315)
            // D1.4 -> D1.8 (2026-11-15, the orchestrator's ruling: no exception; PureV1Writer, Output.Engine/Arrow and
            // CellText's engine half move at D1.8, with D-1's host halves un-parked): the engine's answer writer and its
            // formats. traceComment 1 -> 2 and executionTraceID +1 (2026-11-15): the trace comment's method moved here
            // from ExecutionPlan (its two rows gone above), the engine answer's activity its one Java reader
            // result/Output.java and result/PureV1Writer.java rows: gone 2026-11-16 (W8): legend-engine's writer is the
            // edge's (edge.PureV1Writer), written through Output.Rows by edge.EngineAnswers; server/LiteV1Api.java's
            // PureV1Writer 17 gone with it (its host answer halves are edge.HostAnswers)
            // result/TdsObjectsWriter.java (TdsObjectsWriter 2, presentedName 1) and test/ServiceTestRunner.java
            // (PureV1Writer 1, TdsObjectsWriter 1): gone 2026-11-15 (D1.4: legend-engine's service tests and their
            // PURE_TDSOBJECT writer moved to the edge, //core:edge_service_tests)
            // D1.3 -> D1.9 (2026-11-15): the constant PlatformTypes names, the judges' and the edge's (above)
            // 1 -> 2 each (2026-11-16, the PR1 re-review's A6): both named again in SqlFn.MAY_SKIP_AN_ARGUMENT, a set of
            // functions, not a spelling
            // sql/dialect/{AnsiSqlRenderer,DuckDb,Postgres,RawSqlBoundary}.java's RawSqlBoundary rows: GONE 2026-11-17 (Phase
            // 6's B4 (ii)): the product runs a raw statement as written; the translation is the edge's
            // sql/dialect/EngineTextDialect.java: GONE 2026-11-17 (Phase 6's B3, the orchestrator's GO): the interface is
            // com.legend.edge's, no product class names it
            // testdatagen/TestDataGenerationNatives.java (2) and TestDataGenerator.java (3): GONE 2026-11-17 (Phase 6's
            // B13): the generator's plan is data, legend-engine's text of it the edge's (edge.TestDataPlanText)
            // 2 -> 3 (2026-11-13, the merge of claude/total-wall @ 549c018: C4's bare-H2 lexicon, c42369dec, reads the
            // engine's list once more to derive H2's own keywords; the same D1.5 move takes it)
            // 2026-11-15 (D1.2): legend-pure's DateTime text, moved here from lowering/LiteralSpelling (dateTimeText) as the
            // node DATETIME_PRINT the judges' %r and canon read, and the edge writes for DATETIME_TO_TEXT; D1.9 moves it
            // B9 -> C5 (E-P5) 2026-11-17 (census B9, the orchestrator's ruling (a)): a message's DateTime text is the
            // two-form hook lowering.DateTimeTexts (lite's DATETIME_TO_TEXT in the product); the composition stays for
            // the judges' canon alone, its one emitter LiteralSpelling.temporalCanon, closed by C5's step C
            // JUDGE, now B12's (2026-11-17, census C5's step C): the judges' canon no longer emits the node; its last
            // product emitter, format's %r (Scalars), refused in the product by B12's 2b (F's, 2026-11-17): the edge's
            // DateTime texts write it now
            "sql/dialect/DateTimePrintText.java | +0000 | 2 | JUDGE",
            // RULED by the user, 2026-11-16, the edge program §10 item 9 ("I agree with all"): it stays in the product
            // (the P2 checkpoint audit's A-M1; W6's change, 4fc24e0a3, reverted the same day). The product's H2 quotes legend-engine's word list as written, upper-casing the words its session
            // frees (NON_KEYWORDS), and that decides WHICH COLUMN a declared name reads in H2 data made legend-engine's
            // way. Probed on the H2 2.5.252 jar (MODE=LEGACY, the product's NON_KEYWORDS): a table made as legend-engine
            // quotes, CREATE TABLE T (ID INT, "order" INT, "range" INT, "select" INT), is read by "t0"."order"; W6's
            // H2-keyword quoting ("t0"."ORDER", "t0".range, "t0"."SELECT") fails with Column "t0.ORDER" not found.
            // Reached through a LocalH2 connection's testDataSetupSqls run as written (CsvSeed.declaredSteps via
            // LoadPlanner), H2 file and tcp databases (H2Connector), and H2 files an earlier lite made.
            "sql/dialect/H2.java | ENGINE_LEXICON | 3 | RULED",
            "sql/dialect/H2.java | SET NON_KEYWORDS | 1 | RULED", // listed 2026-11-16 (S5): the same ruling's session half
            // sql/dialect/MongoDb.java | +0000: gone 2026-11-15 (D1.2: its date-time format a @SpellingArm hook,
            // dateTimeFormat, the product's ISO with Z; legend-pure's +0000 the edge's MongoDb's)
            // testdatagen/TestDataPlanner.java | +0000: gone 2026-11-17 (F's separation census line B11): legend-engine's
            // seed route's key (upstreamKeyText: SeedDataGeneration's ValueTransformer, nine digits and +0000) moved to
            // the edge's routes (edge/TestDataRoutes), which hand it to seedJson; the generator writes no zone text
            // testdatagen/TestDataPlanner.java | COMPAT: GONE 2026-11-17 (Phase 6's B13): the printing is the timestamp text
            // itself; upstream's is the UpstreamPlans facet's
            // (the EngineText rows of AssertRuns, HostJudgments, HostJson, HostJudgment, NamedExtensions, Verdicts and
            // AssertVerdicts: gone 2026-11-17, census C3 -- the engine-text comparison the edge's own record
            // (edge/EngineTextJudgment), held by the product's neutral HostJudgment.EdgeJudged)
            "testexec/CanonicalForm.java | +0000 | 1 | JUDGE",
            // testexec/SideShapes.java | Cannot cast a collection of size | GONE 2026-11-17 (C5's step E, E-P5): a side's
            // and the body's answer's size refusal worded at planning (SideShape.sizeWords, TestPlan.answerSizeWords)
            // (testexec/SqlTextRuns.java's rows went with it to the edge: 2026-11-17, census C2: the referee and the appeals moved to the edge (the orchestrator's P1+P2))
            // (PlannedAppeals' generic half renamed NamedExtensions, the same pattern case: 2026-11-17, census C2: the referee and the appeals moved to the edge (the orchestrator's P1+P2))
            // 2026-11-16 (W8 of docs/records/d-plan-2026-11-16.md): legend-pure's toRepresentation of a date, the judges'
            // own printer (Verdicts.pureText), which PureDateLiteral's toString was until then; with the judges' print
            // forms at W10
            // testexecutionplan/Verdicts.java | +0000 gone (2026-11-17, census C5's row, E-Java last: Verdicts.repr and
            // pureText deleted -- legend-pure's date text the edge's UpstreamDateLiteralTexts)
            // (testplan/AgreementAsserts.java | EngineText went: 2026-11-17, (b) of the orchestrator's order: the fused agreement path deleted)
            // (BodyDriver's EngineText row gone, 2026-11-16, W9's judge half: the style's engineTestText,
            // engineTestPlanText and engineTestConnection name the engine test database, the planner none)
            // (SeedSqlForms' EngineText row gone, 2026-11-16, W9's judge half: the style's engineTestText)
            // testplan/BodyDriver.java and Seeds.java | EnginePlans: gone 2026-11-16 (W9's planner half: the routine table
            // and a toDDL statement's text through planner.UpstreamPlans)
            // retagged 2026-11-15 (D1.2's end): the judging rules' choice of the generator's printing, D1.9's
            // (2026-11-16, W9's judge half S2c: testplan/SqlTextVerdicts' two rows gone with the arm to the edge;
            // BodyDriver's withEngineText row gone, ExecEnv.withCollector names what it installs, the arm's collector)
            // testplan/TestPlanner.java | COMPAT: GONE 2026-11-17 (Phase 6's B13): upstream's test-data timestamp text is
            // the UpstreamPlans facet's (the edge's), asked when one is printed; lite's own is LitePrinting.INSTANCE
            // REGISTERED 2026-11-17 (the separation census, docs/records/f-separation-census-2026-11-17.md): 'TDSNull'
            // (B10) and COMPILED_PURE_COMPAT (C1) joined the scan; what stood is registered, each with its owner.
            // C5's step C, C-2 (2026-11-17, the orchestrator's ruling (B)): the judges' TDSNull sentinel is gone -- an
            // expected present null cell is typed (TypedNullCell), its wire value lite's SqlExpr.NULL_CELL_WIRE, its canon
            // lite's bare null (CanonicalRenderSql.NULL_CELL); the rows of PlatformTypes, SqlExpr, CanonicalRenderSql,
            // VerdictSql, Equality and SideShapes deleted, Scalars' TDS_NULL_CELL read as its literal
            // TDSNull as INPUT and LANGUAGE (STAYS): the #TDS literal's cell token, Pure's meta::pure::tds::TDSNull
            // class, and the sentinel ^TDSNull() lowers to
            "compiler/element/type/PlatformTypes.java | TDSNull | 1 | STAYS",
            "compiler/spec/TdsChecker.java | TDSNull | 1 | STAYS",
            "compiler/spec/TdsNullForms.java | TDSNull | 3 | STAYS",
            "compiler/spec/Typer.java | TDSNull | 4 | STAYS",
            // 1 -> 2 (2026-11-17, census C5's step C, C-2): the #TDS literal's null-cell token, read as INPUT, names its
            // literal where it read TDS_NULL_CELL (the constant gone)
            "lowering/Scalars.java | TDSNull | 2 | STAYS",
            // TDSNull as the judges' text (E-Java, Phase 4b: census B10/C5)
            // 5 -> 4, D1.9 -> STAYS (2026-11-17, census C5's step C, C-2): an expected ^TDSNull(), a written 'TDSNull'
            // cell and a golden's null cell are a typed TypedNullCell; what stays reads that written text as INPUT
            "compiler/spec/VerdictQueries.java | TDSNull | 1 | STAYS",   // 4 -> 1 (2026-11-17, census C1: the golden's TDSNull word read by the edge's UpstreamGoldenNullWord, the dead grid branch gone; tdsNullSentinel's stays)
            // (lowering/Render.java | TDSNull | D1.3: gone 2026-11-17, census B17 -- a relation's text moved to the edge,
            // edge.RelationTextLowering)
            // the judges' compatibility mode's branches (E-Java, census C1)
            // testexec/Equality.java | COMPILED_PURE_COMPAT gone (2026-11-17, census C1 2c: the cross-kind rule asked of the run hook, HostForms)
            "testexecutionplan/TestPlan.java | COMPILED_PURE_COMPAT | 1 | D1.9",   // 3 -> 1 (2026-11-17, C1 step 1: each judging fact a named accessor over one field)
            "testexecutionplan/TestPlanJson.java | COMPILED_PURE_COMPAT | 2 | D1.9",
            "testplan/PlanningOptions.java | COMPILED_PURE_COMPAT | 1 | D1.9"   // 2 -> 1 (2026-11-17, C1 step 1: the edge check asks edgeAppeals())
            // retagged 2026-11-15 (D1.2's end): Java's spelling of a date value. D1.4 -> D1.8 (2026-11-16, W4): lite's
            // writers no longer read it (CellText's product half prints S312's text); what remains is legend-engine's
            // JSON of a date (toEngineJson, read by CellText's engine half, which moves with PureV1Writer at W8) and
            // legend-pure's print form (the three toStrings, the judges' and the edge's), moved at W8 with the writers.
            // GONE 2026-11-16 (W8): toEngineJson is toNineDigits, zone-free (the product adds Z, edge.Presenter
            // +0000); the toStrings are the literal body; legend-pure's print form is the judges' printer
            // (testexecutionplan.Verdicts.pureText, W10's) and the edge's (Presenter)
    );

    @Test
    void noCompatibilitySpellingLeaksIntoTheProduct() throws IOException {
        Map<String, Map<String, Integer>> found = scan();
        Map<String, Map<String, Integer>> pinned = new TreeMap<>();
        StringBuilder drift = new StringBuilder();
        for (String row : REGISTER) {
            String[] cells = row.split(" \\| ", -1);
            assertEquals(4, cells.length, "a register row is 'file | spelling | count | step': " + row);
            assertTrue(STEPS.contains(cells[3]), "a register row names a known step: " + row);
            assertTrue(LITERALS.contains(cells[1]) || LITERALS.contains("^" + cells[1] + "$")
                    || SYMBOLS.contains(cells[1]), "a register row names a listed spelling: " + row);
            Integer was = pinned.computeIfAbsent(cells[0], k -> new TreeMap<>()).put(cells[1],
                    Integer.parseInt(cells[2]));
            assertTrue(was == null, "a register row is named once: " + row);
        }
        for (var file : found.entrySet()) {
            for (var hit : file.getValue().entrySet()) {
                int pin = pinned.getOrDefault(file.getKey(), Map.of()).getOrDefault(hit.getKey(), 0);
                if (hit.getValue() > pin) {
                    drift.append("\n  ").append(file.getKey()).append(": '").append(hit.getKey()).append("' x")
                            .append(hit.getValue()).append(pin == 0
                                    ? " -- a compatibility spelling in the product: spell it at the edge (G5, E1)"
                                    : " > " + pin + " registered -- grew: the same");
                } else if (hit.getValue() < pin) {
                    drift.append("\n  ").append(file.getKey()).append(": '").append(hit.getKey()).append("' x")
                            .append(hit.getValue()).append(" < ").append(pin)
                            .append(" registered -- shrank: re-pin the row, with the date");
                }
            }
        }
        for (var file : pinned.entrySet()) {
            for (var pin : file.getValue().entrySet()) {
                if (!found.getOrDefault(file.getKey(), Map.of()).containsKey(pin.getKey())) {
                    drift.append("\n  ").append(file.getKey()).append(": '").append(pin.getKey())
                            .append("' registered x").append(pin.getValue()).append(" and GONE -- delete its row");
                }
            }
        }
        assertTrue(drift.length() == 0, "no compatibility spelling in the product (D2) -- drift:" + drift
                + "\n\ntoday's hits, as register rows (each row's step is yours to name):\n" + rows(found));
    }

    /** The scan bites: a product source holding an engine sentence, a Pure print form and the engine printer is
     *  found on each (a planted source, read as the guard reads a file). */
    @Test
    void theScanBitesAPlantedSource() {
        String planted = """
                package com.legend.lowering;
                // "+0000" in a comment is no hit
                final class Planted {
                    static final String S = "Cannot divide " + "x" + " by zero";
                    static String at(String s) { return s + "+0000"; }
                    static Object e() { return new com.legend.edge.EngineStyleH2(); }
                    static char q() { return '"'; }
                    /* "Can't find " in a block comment is no hit */
                }
                """;
        Map<String, Integer> hits = hits(planted);
        assertEquals(Map.of("Cannot divide ", 1, "+0000", 1, "EngineStyleH2", 1), hits);
    }

    /** Every product main source outside the edge, keyed by its path under core's java root (else under main). */
    private static Map<String, Map<String, Integer>> scan() throws IOException {
        Map<String, Map<String, Integer>> found = new TreeMap<>();
        int scanned = 0;
        for (Path f : SourceFiles.under(MAIN)) {
            String path = SourceFiles.path(f);
            if (!path.endsWith(".java") || path.startsWith(EDGE)) {
                continue;
            }
            scanned++;
            Map<String, Integer> h = hits(Files.readString(f));
            if (!h.isEmpty()) {
                found.put(path.startsWith(JAVA_ROOT) ? path.substring(JAVA_ROOT.length())
                        : path.substring(MAIN.length() + 1), h);
            }
        }
        GuardCoverage.assertFloor("CompatSpellingGuardTest", scanned, 1000);
        return found;
    }

    /** A source's hits: its string literals against the literal list, its code against the symbols. */
    static Map<String, Integer> hits(String source) {
        List<String> literals = new ArrayList<>();
        String code = lex(source, literals);
        Map<String, Integer> out = new TreeMap<>();
        for (String lit : literals) {
            for (String want : LITERALS) {
                if (want.startsWith("^")) {
                    if (lit.matches(Pattern.quote(want.substring(1, want.length() - 1)))) {
                        out.merge(want.substring(1, want.length() - 1), 1, Integer::sum);
                    }
                } else {
                    for (int at = lit.indexOf(want); at >= 0; at = lit.indexOf(want, at + want.length())) {
                        out.merge(want, 1, Integer::sum);
                    }
                }
            }
        }
        for (String sym : SYMBOLS) {
            Matcher m = Pattern.compile("\\b" + Pattern.quote(sym) + "\\b").matcher(code);
            while (m.find()) {
                out.merge(sym, 1, Integer::sum);
            }
        }
        return out;
    }

    /** The code with comments dropped and every string, text block and char literal blanked; each string's and text
     *  block's raw content (escapes unread) is collected. */
    static String lex(String s, List<String> literals) {
        StringBuilder code = new StringBuilder(s.length());
        int i = 0;
        int n = s.length();
        while (i < n) {
            char c = s.charAt(i);
            if (s.startsWith("/*", i)) {
                int end = s.indexOf("*/", i + 2);
                i = end < 0 ? n : end + 2;
                code.append(' ');
            } else if (s.startsWith("//", i)) {
                int end = s.indexOf('\n', i);
                i = end < 0 ? n : end;
            } else if (s.startsWith("\"\"\"", i)) {
                int end = s.indexOf("\"\"\"", i + 3);
                while (end > 0 && s.charAt(end - 1) == '\\') {
                    end = s.indexOf("\"\"\"", end + 1);
                }
                int stop = end < 0 ? n : end;
                literals.add(s.substring(i + 3, stop));
                i = end < 0 ? n : end + 3;
                code.append("\"\"");
            } else if (c == '"' || c == '\'') {
                int j = i + 1;
                while (j < n && s.charAt(j) != c && s.charAt(j) != '\n') {
                    j += s.charAt(j) == '\\' ? 2 : 1;
                }
                if (c == '"') {
                    literals.add(s.substring(i + 1, Math.min(j, n)));
                }
                i = j + 1;
                code.append(c).append(c);
            } else {
                code.append(c);
                i++;
            }
        }
        return code.toString();
    }

    /** Today's hits as register rows, the step left for the reader to name. */
    private static String rows(Map<String, Map<String, Integer>> found) {
        StringBuilder b = new StringBuilder();
        found.forEach((file, h) -> h.forEach((spelling, count) -> b.append("            \"").append(file)
                .append(" | ").append(spelling).append(" | ").append(count).append(" | ?\",\n")));
        return b.toString();
    }
}
