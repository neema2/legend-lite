// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CODE-SHAPE GUARDRAILS — build-failing, not aspirational. Added after
 * the audit-window review found a 1,207-line method and a 4,974-line
 * file that no scoreboard had ever surfaced: the corpus measures
 * behavior, THIS measures whether the next reader can hold the code.
 *
 * <p>The allowlists are a BURN-DOWN ledger, not a loophole: every entry
 * names a known offender with a planned split. Shrink them; never grow
 * them — a new entry needs the same justification a corpus regression
 * would.
 */
@Tag("guardrail")
class CodeShapeGuardrailTest {

    private static final int METHOD_LIMIT = 250;

    /** Known oversized METHODS, pending their planned splits — ceilings
     * at measured size + small slack; SHRINK only. */
    private static final Map<String, Integer> METHOD_ALLOWLIST = Map.of();

    // The 3,500-line FILE limit was dropped on 2026-09-29 (execution plan W0.5): every early item of the
    // compiler rebuild edits a file at 3,472-3,499 lines, and a file's size is a symptom the stage split
    // (W1.6, W4.3, W5.1) removes by design; a line cap only forced reflowing. The METHOD limit stays.

    /** Mutable instance fields that are DELIBERATE: hand-rolled parser
     * cursors (Lexer/ElementParser/SpecParser walk positions and scope
     * stacks), per-resolution frames, fresh-name counters and one
     * memo cache. Everything else must be final or become part of an
     * explicit frame object. */
    private static final Set<String> MUTABLE_FIELD_ALLOWLIST = Set.of(
            // 2026-11-11 (B4, the audit's finding 2): one program's decision of its model's registration on the
            // process's catalog, a latch set once by the program's planning (ProgramState.registerOnTheCatalog), and the
            // load it decided, placed ahead of every step that reads the catalog -- one instance per program pass
            "ProgramState.catalogRegistered", "ProgramState.registration",
            // (2026-11-11, the JSON pass: "DataSources.home", the one body's data: tables' home database, left with the
            // tables themselves -- a payload is its statement's own, bound or inline, and the rewrite holds no state)
            // 2026-11-09 (the user's question "why do we have any callbacks?"): the service test's answer is a receiver
            // told the answer's shape before its columns (Output.Rows), where the run called a factory of the runner's
            // until then; it holds the writer it made for that shape, set once, read by each row after
            "ServiceTestRunner.writer",
            // 2026-11-01, the test-is-the-product plan's M5 audit: until then a field whose TYPE carried an annotation
            // (@Nullable, Outer.@Nullable Inner, Object @Nullable []) was outside this scan; the scan learned them and
            // found these, each listed with what it is. LAZY, set once from the instance's own inputs and read back
            // (a memo of a pure derivation): the typed body; the model builder's indexes; the context's two tables;
            // the C3 linearizer; a substitution's read and mentioned sets; the lexer's line and token positions
            // (benign-racy, documented at the field); the parser's non-Pure ranges; the resolver's element
            // references and association navigators; a mapping closure's sets (the runner's fixtures-by-store left
            // 2026-11-10 with the on-demand fixtures it indexed: census M9)
            "TypedQuery.body", "ModelBuilder.legacySetsById", "ModelBuilder.directSubclasses", "ModelBuilder.knowledge",
            "PureModelContext.declarations", "PureModelContext.implementations", "InferenceKernel.linearizer",
            "SourceSubst.reads", "SourceSubst.mentioned", "TokenStream.lineStarts", "TokenStream.tokenPositions",
            "ElementParser.nonPureRanges", "StoreResolver.elementsRef", "AssociationJoins.navMaterializer",
            "AssociationJoins.unionHeads", "MappingClosures.mappings",
            "MappingClosures.sets", "MappingClosures.unions", "MappingClosures.inheritances", "MappingClosures.roots",
            "MappingClosures.enums", "MappingClosures.relationalSets",
            // (same date) A SESSION'S facts, learned once from its database: the decoding its driver needs (known at
            // check), its server's version; the test runner's shared session and the package it holds. (The next
            // query's minted stamp, JdbcStatements.reserved and ExecutionTrace.reserved, went with C1, 2026-11-12: the
            // database receives exactly the plan's text, the execution id the runner's bound value.)
            "JdbcStatements.driver", "Sessions.version",
            // 2026-11-12, C2 (D4): the error read-back the plan's target declared when it last checked the session, a
            // session's fact as its checked driver is -- what the seam sends after a statement, or a bracket's commit,
            // fails on it (the charter's A3(c))
            "SessionState.raiseReadBack",
            // 2026-11-10, the A checkpoint's tenets audit (2): the session's checked target's connector, learned at
            // check as the driver's decoding is, whose facts (a transaction taken, an error aborting it) a run acts on
            "JdbcStatements.declared",
            // 2026-11-10, the tenets audit (8): the referee's copy of a seed, written when an edge first asks, once
            "CsvSeed.made",
            "PureTestRunner.sessionConn", "PureTestRunner.sessionPkg",
            // (same date) A FRAME'S cursor, one instance per pass and never shared: the verdict batch's open root, the
            // canon rider's wrap and its decline, the lowerer's connection time zone and id minter (set at
            // configuration), the temporal frame's sweep column, the database parser's schema spans, the Cypher
            // translator's leading USE, a select item's taken name, a vector's lazily allocated nulls, the PURE v1
            // writer's first object, the walk's segment state (deleted at M10), a statement's own stamp in its
            // frames' recorder (frozen into its reads), the body router's opened answer sink (opened only to answer,
            // as the walk opens it)
            "VerdictBatch.current", "CanonRider.wrap", "CanonRider.declined", "Lowerer.dbTimeZone",
            // 2026-11-01, the test-is-the-product plan's M7: the planned batch's one-shot latch -- the flush counted
            // once, when its groups start to run (the walk's flush, or the program road's first row), never for a
            // batch planned and left for the walk -- since 2026-11-08 (the plan-as-data design's step 5) the latch of
            // one segment's run, RootLoops.Run, the planned batch being data
            "RootLoops.counted",
            // 2026-11-07, P1.2 (the goals of record: a compatibility appeal is decided after the body's run): one body's
            // held appeals, one instance per body -- deciding, latched when the run is over, sends the appeals' own
            // reports straight to the caller's listener
            "Appeals.deciding",
            // 2026-11-07, the P1 milestone audit's 2: whether a write of the body's ran (wrote) and ran after the first
            // held appeal (wroteAfterHold) -- told by the runner's sink at each writing step, read when the appeals are
            // decided: an appeal whose rows a write moved, or a failed run's rollback undid, is refused by name
            "Appeals.wrote", "Appeals.wroteAfterHold",
            // 2026-11-08, the plan-as-data design's step 0: a body's verdict ids -- one counter per body, the last id
            // given; planning state, never read at run
            "VerdictIds.last",
            // 2026-11-08, the plan-as-data design's step 3 (docs/PLAN_AS_DATA_2026_11_07.md 2.8): the open program's end, set once when the program ends (its reads refused after it), and the
            // test run's handing of the program's reader to the judges, once at the first callback
            "ProgramRunner.closed", "TestRun.reading",
            // 2026-11-08, the plan-as-data design's step 4: the body's engine-text appeals' last number given (planning
            // state, one per body; since step 8b planning's registry alone, never published for the run)
            "EngineTextAppeals.last",
            // 2026-11-08, the plan-as-data design's step 6: one agreement-judged assert's run state -- the host half's
            // verdict held until the database half's rows are in, and an assertError's raising row -- once per run,
            // read from the plan's pairing (the planner's One held them until then, unscanned: no visibility)
            "AgreementRuns.held", "AgreementRuns.heldHost", "AgreementRuns.raisingRow",
            "Lowerer.instanceIdOf", "TemporalFrame.forEachDateColumn", "DatabaseProtocolParser.currentSchemaDeclSpan",
            "DatabaseProtocolParser.schemaStretchSpan", "CypherToPure.leading", "SqlToPure.name", "Batch.other",
            "PureV1Writer.firstObject",
            "ProgramFrames.ownStamp", "TestRunner.opened",
            // (2026-11-01, the test-is-the-product plan's M6) the body router's last judged verdict: one moved body's
            // sink, one instance per run and never shared, set at each verdict step so a body ending on an assert
            // answers it (the walk's Segments.last beside it until the walk was deleted, 2026-11-06)
            "TestRunner.lastVerdict",
            // (2026-11-04, the assert bar) the body router's reader: the running program's, handed by the runner before
            // its first step and taken back when its session closes (PlanRunner.ProgramSink#reader) -- one moved body's
            // sink, one instance per run and never shared; a verdict's caller's message is read through it only where
            // the verdict fails
            "TestRunner.reader",
            // (2026-11-09, slice 5: the run sink is the test's run's, TestRunner's; its three fields above moved with it)
            // the run record (hole 6): how many messaged asserts are being judged -- one body's run, never shared, the
            // thread-local CallerMessage.DEPTH it replaced made explicit
            "BodyRun.messaged",
            // the program's step mark for the reads a callback makes (slice 5: the thread-scoped statement origin made
            // explicit) -- one program's run, set before each callback and restored after it
            "ProgramRunner.stepMark",
            // the activity comment reserved for a body's fused statement, minted once per body at planning (slice 5:
            // planning's own, exec's execution trace until then)
            "ActivityComments.reserved",
            // the text collector's origin in force while an arm reads its texts (slice 5: the planning-time statement
            // mark made explicit on the collector the planning environment carries)
            "SqlTextVerdicts.originInForce",
            // (same date) regex artifact, NOT mutable: an interface's field, implicitly static final
            "DecisionProbe.INSTALLED",
            // a parser cursor (2026-10-27, the result boundary program's phase 5: the reader of a database's JSON
            // with its non-finite tokens moved here from the parity harnesses' test sources): one parse's position,
            // the instance one parse's and never shared
            "NonFiniteJson.at",
            // (Lowerer.relationDepth row DELETED 2026-08-24 — the audit
            // found the counter write-only; the allowlist justification
            // was a rationalization, exactly what this guardrail is for)
            // renderer nesting cursor: NAMED-frame (view) subselect depth —
            // same lifecycle as a parser cursor, scoped to one render()
            // the inliner's UNROLL BUDGET spend (batch 149): a per-compile
            // counter of call expansions inside a named budget object —
            // same lifecycle as the inliner (the compile artifact); it
            // exists so a program-sized tree walk walls instead of hanging
            "UserCallInliner.spent",
            // the fold budget's remainder (2026-10-15): the expression nodes a pass's written projections may
            // still add (Fold.Budget, Calcite's bloat); one object per pass, the inliner's shape
            "Fold.left",
            // reads in flight on an enclosing row's behalf (2026-10-15): the depth the resolvers consult so a
            // correlated read spends no pass's fold budget; raised and lowered around the read
            "Lowerer.correlatedReads",
            // a raw grid report's input in flight (2026-10-31, the planner-driver seam program's S3c): the depth
            // rawGrid consults so a report reads its grid as the grid itself, never as its rows' cells; raised and
            // lowered around the input's lowering, correlatedReads' lifecycle
            "Lowerer.reportInputs",
            // the raw grids whose rows the statement reads as their cells (2026-10-31, the planner-driver seam
            // program's S3c and its audit): computed once over the whole body before any of it is lowered, read by
            // rawGrid; the builder flags' lifecycle (set once, then read during the lowering)
            "Lowerer.cellGrids",
            // a writer's depth inside a set function's arguments the database evaluates when it plans (2026-10-17,
            // the Cypher island's range of a bound that may be null, docs/CYPHER_STUDY_2026_10_15.md §12.73):
            // raised and lowered around the write (SqlWriter.planned), one writer per statement; Postgres's raise
            // reads it so no planner pre-evaluates a raise there
            "SqlWriter.planned",
            // a LAZY value (2026-09-23): an execute() call's activity rows
            // carry a second render of its chain, computed on the first
            // read and kept — set once, never reassigned after
            "LazyRows.computed",
            // a session's write mark (2026-10-29, the result boundary program's track B, B1b): set when a writing
            // statement is about to run on the session, cleared when its seeds are re-established; the same
            // lifecycle as the session's connection (SessionState lives in a weak map keyed by it). The body
            // driver's nested Established.dirty before, moved to the seam
            "SessionState.dirty",
            "EngineStyleH2.frameDepth",
            // anonymous-subselect nesting cursor (bare DISTINCT-key
            // spelling scope) — same lifecycle as frameDepth
            "EngineStyleH2.anonDistinctDepth",
            // mapping-section context for AggregationAware span-shift
            // emulation: set once at parse entry / per ~modelOperation
            // view — same lifecycle as a parser cursor
            "MappingProtocolParser.sectionStartLine",
            "MappingProtocolParser.aggLambdaShift",
            // JSON writer's field/value alternation cursor — same
            // lifecycle as a parser cursor, scoped to one document write
            "Json.awaitingFieldValue",
            // a keyed plan's text sink's first-row cursor (2026-10-20, the dynamic pivot's probe plan): the JSON
            // array's comma before every row but the first -- the Json.awaitingFieldValue shape, scoped to one
            // result write (PlanRunner.TextSink)
            "PlanRunner.first",
            // a typed result streamed to its sink (2026-10-27, the result boundary program's R8): the batch being
            // filled (its row count, built once), the batcher's open batch, the collector's end, and the runner's
            // columns-sent cursor -- each scoped to one result's stream, the Json.awaitingFieldValue shape
            "Batch.size", "Batch.built", "Batcher.builder", "Batcher.started", "Batcher.ended",
            "Collector.columns", "Collector.ended", "PlanRunner.started", "PlanRunner.streamed",
            "PlanRunner.columns",
            // the writers at the edge (2026-10-27, the result boundary program's phase 2): each sink's columns as the
            // stream announced them, and the cursor of what it has written (the first row's comma, the row count, a
            // graph fetch's first object) -- scoped to one result's stream, as the batcher's
            "CsvWriter.columns", "JsonRowsWriter.columns", "JsonRowsWriter.first", "PureV1Writer.columns",
            // the Arrow writer's encoders, one per column, made when the columns are announced and read per batch
            // (track A, 2026-10-29): the sink protocol's state, as the other writers' columns are
            "ArrowWriter.encoders",
            "PureV1Writer.order", "PureV1Writer.rows",
            // 2026-11-12 (C1's second half, the C checkpoint's code audit SHOULD-FIX 4): the execution the run names
            // before its columns (ResultSink.execution) -- the sink protocol's state, scoped to one result's stream as
            // the columns are; the service runner's receiver holds it until its shape makes the writer it hands it to.
            // The same day (the exec-id item, C4): the engine's writers are MADE for their execution (the door's id,
            // Output.writerFor), PureV1Writer's final and ArrowWriter's gone, so they left this list
            "ServiceTestRunner.execution",
            // the collector's, the same day (C4: PCT's plan route writes a collected relation later, through a writer
            // whose activity names the execution the run named to the collector)
            "Collector.execution",
            // legend-engine's PURE_TDSOBJECT writer (the doors, door 1, 2026-10-29): the same cursor shape -- its
            // columns as announced, whether the one value column is to-one, the row count for the comma and the
            // to-one refusal
            "TdsObjectsWriter.columns", "TdsObjectsWriter.toOne", "TdsObjectsWriter.rows",
            // the streamed answer's cardinality sink (com.legend.testexec.SideShapes.Cardinality, the doors, door 1, 2026-10-29): the rows
            // seen so far on their way to the writer, the same cursor shape, scoped to one answer's stream
            "SideShapes.rows",
            // parser cursors + scope state
            "Lexer.pos", "Lexer.islandDepth", "Lexer.types", "Lexer.starts",
            "Lexer.ends", "Lexer.count",
            "ElementParser.pos", 
            // DatabaseProtocolParser: the same parser-cursor shape
            "DatabaseProtocolParser.pos",
            // GqlParser: the raw-text scanner cursor, same family as the
            // other parser cursors above
            "GqlParser.pos",
            // the #SQL{...}# island's Postgres reader (2026-10-11, docs/LEGEND_SQL_STUDY_2026_10_11.md §3):
            // SqlLexer's scanner cursor and its line/column tracking, SqlParser's token cursor -- the same
            // parser-cursor family, scoped to one parse -- and SqlToPure's fresh-name counter, scoped to one
            // translation (the StoreResolver.freshVarCounter shape)
            "SqlLexer.at", "SqlLexer.line", "SqlLexer.column", "SqlParser.at", "SqlToPure.fresh",
            // IQ island, 2026-10-13 (docs/IQ_SQL_STUDY_2026_10_13.md §5.2): the #IQ{...}# island's reader --
            // IqLexer's scanner cursor and its line/column tracking, IqParser's token cursor: the SqlLexer/SqlParser
            // parser-cursor family above, scoped to one parse
            "IqLexer.at", "IqLexer.line", "IqLexer.column", "IqParser.at",
            // Cypher island, 2026-10-15 (docs/CYPHER_STUDY_2026_10_15.md §5.1): the #Cypher{...}# island's reader --
            // CypherLexer's scanner cursor and its line/column tracking, CypherParser's token cursor (the same
            // parser-cursor family, scoped to one parse), and CypherToPure's fresh-name counter, scoped to one
            // translation (the SqlToPure.fresh shape)
            "CypherLexer.at", "CypherLexer.line", "CypherLexer.column", "CypherParser.at", "CypherToPure.fresh",
            // Cypher island PR2, 2026-10-15 (docs/CYPHER_STUDY_2026_10_15.md §12.5.1): CypherToPure's depth of the
            // lambdas the database computes (a comprehension's or predicate's over a collection), raised and lowered
            // around their reading, scoped to one translation (the fresh-name counter's lifecycle)
            "CypherToPure.databaseLambdas",
            // Writes leg 6, 2026-10-20 (docs/WRITES_DESIGN_2026_10_18.md §8, leg 6): CypherToPure.Writer's frame of one
            // statement's program -- its native store, the stages' counter, the clause being read, the last stage's
            // key, row-number and held-identity columns, the high-water mark staged as the program starts -- set as
            // the program's clauses are read, scoped to one program (the fresh-name counter's lifecycle)
            "CypherToPure.store", "CypherToPure.stages", "CypherToPure.at", "CypherToPure.keys",
            "CypherToPure.ordinal", "CypherToPure.heldIdentities", "CypherToPure.highWater",
            // Writes leg 5, 2026-10-20 (docs/WRITES_DESIGN_2026_10_18.md §8, leg 5): whether the program's graph is a
            // domain model's (its writes the class writes, L2), set with the store as the program starts
            "CypherToPure.domain",
            // the Cypher translator's let (2026-10-19, the parity lanes' exponential temporal SQL): Shares' count of
            // the hidden columns it has named, scoped to one translation (the CypherToPure.fresh shape)
            "Shares.next",
            // the island's reader of a date's or a timestamp's text (2026-10-13, Postgres's date spellings,
            // docs/LEGEND_SQL_STUDY_2026_10_11.md §9.4): PostgresDateTime.Decoder is datetime.c's DecodeDateTime,
            // these its struct pg_tm and the field mask it fills field by field -- scoped to one constant's read,
            // the parser-cursor lifecycle
            "PostgresDateTime.fmask", "PostgresDateTime.year", "PostgresDateTime.mon", "PostgresDateTime.mday",
            "PostgresDateTime.yday", "PostgresDateTime.hour", "PostgresDateTime.min", "PostgresDateTime.sec",
            "PostgresDateTime.fsec", "PostgresDateTime.is2digits", "PostgresDateTime.julian",
            "MappingProtocolParser.pos",
            // ConnectionSectionGrammar.Cursor / RuntimeSectionGrammar
            // .SliceCursor: the re-lex feeds' parser cursors — same shape,
            // scoped to one island/SPI parse
            "ConnectionSectionGrammar.pos",
            "RuntimeSectionGrammar.pos",
            "ServiceSectionGrammar.pos",
            "DataSpaceSectionGrammar.pos",
            "PersistenceSectionGrammar.pos",
            "FunctionActivatorSectionGrammar.pos",
            // the SHARED SliceCursor (ElementwiseSectionGrammar's SPI feed)
            "SliceCursor.pos",
            // DiagramSectionGrammar.Raw: the RAW section's char walker —
            // a parser cursor (position + its own line/col tracking, since
            // diagram content never reaches the shared lexer)
            "DiagramSectionGrammar.i", "DiagramSectionGrammar.line",
            "DiagramSectionGrammar.col",
            // parse-surface MODE, set once at construction by the factory that owns
            // it (at() = the engine-strict drop-in surface) — never flipped mid-parse
             
            "SpecParser.pos",
            // the shared minimal JSON reader's walk position — a parser
            // cursor, same family as Lexer.pos
            "Json.i",
            // 2026-11-07, register S144: json-simple 1.1.1's grammar (JudgeJson, the compatibility rules' JSON) -- a
            // parser cursor and the token it last read (its kind and value), one instance per text, never shared
            "JudgeJson.at", "JudgeJson.kind", "JudgeJson.value",
            // ';'-ownership context for unbraced lambda code blocks: where the
            // enclosing sequence's statement that owes the next ';' began (or
            // -1) -- a cursor-adjacent mark, saved and restored around each
            // sequence, same lifecycle as pos (replaced boundedDepth, the
            // protocol program's leg 5, 2026-10-09)
            "SpecParser.owingStatement",
            // per-resolution frames + counters
            "StoreResolver.freshVarCounter", "StoreResolver.temporal",
            // driver-scoped emission opt-in (builder-style, set once
            // before lower()): engine-parity join-distinct exists form —
            // the standalone-SQL surface constructs without it
            "Lowerer.engineExistsJoinForm",
            // Phase 2a (batch 136): the two lowering MODES that were
            // thread-locals — verbatim equality (save/set/restore around
            // the two mapping-definition sites) and the engine-text
            // option (builder-style, set once before lower())
            "Lowerer.verbatimEquality", "Lowerer.engineText",
            // the query's execution feature flags (Feature): builder-style,
            // set once by the executor before lower(), same lifecycle as
            // engineText — a flag selects an emission during the lowering
            "Lowerer.features", "Lowerer.legacyNullUnsafeEquals",
            // (ExecutionTrace.last, the execution-trace stamp's state since batch 137: DELETED with its class by C1,
            // 2026-11-12 -- no comment is written in front of a sent statement)
            // builder-flag, same lifecycle as engineExistsJoinForm: set
            // once by the driver before lower(), read during the lowering
            "Lowerer.streamingGraphRoot",
            // object identity (2026-10-20, docs/OBJECT_IDENTITY_2026_10_20.md §3.1, §3.4; allowlisted at main's merge,
            // 2026-10-22, the first run of this guard on the branch): builder flags, same lifecycle as
            // streamingGraphRoot -- whether every object carries its identity, and whether a scalar root shows its
            // objects' declared properties alone; set once before lower()
            "Lowerer.objectIdentity", "Lowerer.declaredEgress",
            // JSON source frames (XStore §1): the execution context's
            // JsonModelConnection map, set per from()-scope — same
            // per-resolution frame lifecycle as `temporal`
            "ClassSources.jsonSources",
            // serialize(..., config) type-key config: set at the serialize
            // arm, consumed at the envelope build — same per-resolution
            // frame lifecycle as `temporal`
            "StoreResolver.serializeTypeCfg",
            // graphFetchChecked flag: set beside serializeTypeCfg at the
            // serialize arm, consumed at the envelope build — same
            // per-resolution frame lifecycle
            "StoreResolver.checkedEnvelope",
            "SyntheticHeads.count", "Lowerer.tdsCounter", "Lowerer.aliasCounter",
            "UserCallInliner.fresh", "Bindings.contravariantDepth",
            // walk-scoped mode flag: inside a postprocessor-CONFIG
            // property user calls stand for structural extraction —
            // same lifecycle as a parser cursor, scoped to one rewrite
            "UserCallInliner.configMode",
            // object identity step 2 (2026-10-20, docs/OBJECT_IDENTITY_2026_10_20.md §3.2; allowlisted at main's merge,
            // 2026-10-22, the first run of this guard on the branch): the walk's instantiation counter, its lambda depth
            // and the frame of its point -- the same lifecycle as a parser cursor, scoped to one rewrite (the path of
            // instantiations around the point is a final list, saved and restored around each lambda)
            "UserCallInliner.instantiations", "UserCallInliner.lambdaDepth", "UserCallInliner.frame",
            // §4AD slice 1: filter-predicate nesting depth during the
            // one liftFilteredHeads walk (value-position scope gate) —
            // same lifecycle as a parser cursor
            "SyntheticHeads.filterPredDepth",
            // §4AD slice 1: the walk's pre-scanned single-pred head set
            // (scalar-lift scope gate) — set once at walk entry, read
            // during the same walk; same lifecycle as a parser cursor
            "SyntheticHeads.scalarLiftHeads",
            // NormalizeRequired inline α-rename counter — same lifecycle as
            // UserCallInliner.fresh (fresh-name generation per compile)
            "AlphaRename.nrFresh",
            // 2026-10-28 (PR #77's checks lane; docs/SEMANTICS_REGISTER.md S112): a generic function's expansions' fresh
            // column names -- the columns its body makes for its own use, renamed at each call (ColumnHygiene, the user,
            // 2026-10-27) -- the AlphaRename.nrFresh lifecycle: unique within one compile, nothing read back
            "ColumnHygiene.fresh",
            // 2026-10-11 (cloud backends, docs/CLOUD_BACKENDS_PCT_2026_10_11.md §9): the Snowflake dialect's fresh
            // names (its lambda parameters and FLATTEN aliases) and the select-list explosion pass's aliases
            // (ExplosionMarkers, shared with BigQuery): unique within one statement, the same lifecycle as
            // Lowerer.aliasCounter
            "Snowflake.fresh", "ExplosionMarkers.next",
            // render mode toggle + import memo
            "AnsiSqlRenderer.inlineMode", "ModelOrchestrator.cachedImports",
            // 2026-10-17 (docs/MONGODB_PLAN_2026_10_16.md §5.1): the MongoDb pipeline writer's stage counter -- Stages
            // IS the frame object, one per pipeline level, scoped to one render() as the renderer's cursor is: it
            // puts the comma before every stage but the first
            "MongoDbPipeline.written",
            // normalizer emission frame: Pipeline IS the frame object; expr
            // is the accumulating pipeline AST
            "Pipeline.expr",
            // regex artifact, NOT mutable: implicitly public-static-final
            // interface constant (interfaces cannot spell 'final' on fields)
            "TokenStreamCursor.IDENTIFIER_TOKENS",
            // nested-class cursor: PureDateLiteral's date parser (keys are
            // filename-scoped; the old runner's ExecJson cursor died with
            // it in batch 115)
            "PureDateLiteral.pos");

    private static final Pattern SIG = Pattern.compile(
            // 4-space (top-level class members) OR 8-space (nested-class
            // members — F1.7: the 4-space anchor left nested methods
            // unscanned); statement keywords excluded so 8-space method
            // BODY lines (multiline `return foo(...)` calls) cannot
            // false-match as signatures
            "^(?:    |        )(?! )(?!return |throw |if |while |for "
            + "|switch |case |else |new |yield |assert )"
            + "(?:private |public |protected |static |final |synchronized )*"
            + "[\\w.<>\\[\\], ?]+ (\\w+)\\(");

    /** Class-level non-final instance fields, ANY visibility — package-
     * private has no keyword, so the visibility group is optional (audit 15:
     * the private-only pattern let two refactors widen mutable fields out
     * of the scan unaudited). A type may carry annotations ({@code @Nullable},
     * {@code Outer.@Nullable Inner}, {@code Object @Nullable []}): until
     * 2026-11-01 the type's characters held no {@code @}, and every annotated
     * mutable field was outside the scan (the test-is-the-product plan's M5
     * audit). */
    private static final Pattern MUTABLE_FIELD = Pattern.compile(
            "^    (?! )(?:(?:private|protected|public) )?"
            + "(?!static |final |private |protected |public |record |class"
            + " |interface |enum |abstract |sealed |non-sealed )(?! )"
            + "[\\w.@<>\\[\\], ?]+ (\\w+)( =.*)?;");

    /** Nested-class fields (8-space indent). A visibility modifier is
     * REQUIRED here to distinguish fields from method locals — declare
     * nested-class fields private/protected/public, never package-private. */
    private static final Pattern NESTED_MUTABLE_FIELD = Pattern.compile(
            "^        (?! )(?:private|protected|public) (?!static |final )(?! )"
            + "[\\w.@<>\\[\\], ?]+ (\\w+)( =.*)?;");

    /** Static mutable state is banned outright — NO allowlist. */

    /** Strip string/char literals and comments so braces inside them
     * never skew the counts (parseDerivedProperty false-positived at
     * 3,064 lines from a brace inside a string). */
    private static String sanitize(String line) {
        StringBuilder out = new StringBuilder(line.length());
        boolean inStr = false;
        boolean inChar = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if ((inStr || inChar) && c == '\\') {
                i++;
                continue;
            }
            if (!inChar && c == '"') {
                inStr = !inStr;
                continue;
            }
            if (!inStr && c == '\'') {
                inChar = !inChar;
                continue;
            }
            if (!inStr && !inChar) {
                if (c == '/' && i + 1 < line.length()
                        && line.charAt(i + 1) == '/') {
                    break;
                }
                out.append(c);
            }
        }
        return out.toString();
    }

    private static List<Path> mainSources() throws IOException {
        // core's main sources: src/main/java and the DuckDB-only src/main/duckdb (Bazel workplan P3-27, CT-N10)
        String root = "core/src/main/java";
        try (Stream<Path> s = SourceFiles.under(root, "core/src/main/duckdb").stream()) {
            List<Path> out = s.filter(p -> p.toString().endsWith(".java"))
                    .toList();
            GuardCoverage.assertFloor(/* 499->498: HostEval DELETED, Phase 1 batch 2 */ "CodeShapeGuardrailTest",
                    out.size(), 498);
            return out;
        }
    }

    // F1.8: unreferenced private methods. Counting rule (Tier-2 audit
    // 2026-08-18): a private method group is dead when its name has
    // ZERO mentions outside its own declaration+body spans — masking
    // the spans makes RECURSION invisible to the count (the audit's
    // probe: a dead recursive method was green because `uses` counted
    // its own self-call). The sound scanner + span rule found 11 dead
    // methods (the 9 backlogged F0.1 sites plus 2 the old rule could
    // not see); ALL were deleted the same day — the pin is ZERO and
    // stays there: dead private code is deleted, never backlogged.
    private static final int DEAD_PRIVATE_METHODS = 0;

    /** Class NAMES used as LOGIC ({@code getSimpleName().startsWith("TypedC")}
     * standing in for "is a literal"): a decision names its node kind (a
     * {@code switch} over the sealed variants), never a name prefix. The
     * one site (FunctionBodyRows) went typed on 2026-09-15 with the
     * reflection ban; the pin is ZERO. {@code getSimpleName()} inside an
     * error MESSAGE is fine and is not matched. */
    private static final int CLASS_NAME_LOGIC_SITES = 0;

    /** The set-id default spelling ({@code ::} as {@code _}) has ONE owner,
     * {@code protocol/SetIds.java} (audit 2026-09-15 P2-1: sixteen sites spelled
     * it, one spelled a different rule; it moved from {@code model/SetId.java},
     * which delegates to it, 2026-10-21 with the protocol program's leg 8 last
     * step: the parser, which builds records only, needs the rule too). The two
     * other files listed spell the same substitution for OTHER names (a plan's
     * enum-map label, an expression alias) and are not set ids. */
    private static final Set<String> SET_ID_SPELLING_OWNERS = Set.of(
            "SetIds.java", "PlanText.java", "CallShapes.java");

    @Test
    void setIdSpellingHasOneOwner() throws IOException {
        Pattern spelling = Pattern.compile("replace\\(\"::\",\\s*\"_\"\\)");
        List<String> strays = new ArrayList<>();
        for (Path p : mainSources()) {
            String code = blankNonCode(Files.readString(p));
            if (spelling.matcher(code).find()
                    && !SET_ID_SPELLING_OWNERS.contains(p.getFileName().toString())) {
                strays.add(p.getFileName().toString());
            }
        }
        assertTrue(strays.isEmpty(), () -> "the set-id default spelling is SetIds.of /"
                + " SetIds.defaultFor — never spelled in place; strays: " + strays);
    }

    @Test
    void classNamesAreNeverLogic() throws IOException {
        Pattern logic = Pattern.compile(
                "getSimpleName\\(\\)\\s*\\.\\s*(equals|equalsIgnoreCase|startsWith|endsWith"
                + "|contains|matches|hashCode|compareTo)\\s*\\("
                + "|switch\\s*\\(\\s*[\\w.]*\\s*\\.?\\s*getClass\\(\\)\\s*\\.\\s*getSimpleName\\(\\)"
                + "|getClass\\(\\)\\s*==|getClass\\(\\)\\s*\\.\\s*equals\\(");
        List<String> sites = new ArrayList<>();
        for (Path p : mainSources()) {
            String code = blankNonCode(Files.readString(p));
            Matcher m = logic.matcher(code);
            while (m.find()) {
                sites.add(p.getFileName() + ": " + m.group());
            }
        }
        assertTrue(sites.size() <= CLASS_NAME_LOGIC_SITES,
                () -> "class names used as logic grew to " + sites.size()
                        + " (pinned at " + CLASS_NAME_LOGIC_SITES + "): " + sites
                        + " — switch on the node kind instead");
    }

    @Test
    void deadPrivateMethodsOnlyShrink() throws IOException {
        List<String> dead = new ArrayList<>();
        for (Path p : mainSources()) {
            String cls = p.getFileName().toString();
            // strip comments AND string/char literals with a real
            // scanner — brace-walking must not see a "{" inside a
            // string, and regex-order stripping mangles quotes ('"'
            // char literals, // inside string URLs) into unbalanced
            // braces (Tier-2 audit: the first cut false-flagged 25
            // live methods exactly this way)
            String code = blankNonCode(Files.readString(p));
            // each declaration's span = signature through matching
            // close-brace; a self-call sits INSIDE the span, so masking
            // the spans makes recursion invisible to the use count
            // (ADVERSARIAL_TENET_AUDIT §3 probe: a dead RECURSIVE
            // method was green — `uses` counted its own self-call)
            Matcher d = PRIVATE_METHOD.matcher(code);
            java.util.Map<String, List<int[]>> spans =
                    new java.util.HashMap<>();
            while (d.find()) {
                int open = code.indexOf('{', d.end());
                int semi = code.indexOf(';', d.end());
                if (open < 0 || (semi >= 0 && semi < open)) {
                    continue;
                }
                int depth = 1;
                int i = open + 1;
                while (i < code.length() && depth > 0) {
                    char ch = code.charAt(i);
                    if (ch == '{') {
                        depth++;
                    } else if (ch == '}') {
                        depth--;
                    }
                    i++;
                }
                spans.computeIfAbsent(d.group(1),
                        k -> new ArrayList<>()).add(new int[]{d.start(), i});
            }
            // ONE pass over the file's calls (name( ) and references
            // (::name): a private method is live when one of them falls
            // OUTSIDE its own declarations' spans
            java.util.Set<String> live = new java.util.HashSet<>();
            Matcher u = CALL_OR_REF.matcher(code);
            while (u.find()) {
                int g = u.group(1) != null ? 1 : 2;
                String name = u.group(g);
                List<int[]> own = spans.get(name);
                if (own == null || live.contains(name)) {
                    continue;
                }
                int at = u.start(g);
                if (own.stream().noneMatch(sp -> at >= sp[0] && at < sp[1])) {
                    live.add(name);
                }
            }
            for (String name : spans.keySet()) {
                if (!live.contains(name)) {
                    dead.add(cls + "." + name);
                }
            }
        }
        assertTrue(dead.size() <= DEAD_PRIVATE_METHODS,
                "unreferenced private methods grew to " + dead.size()
                + " (pinned at " + DEAD_PRIVATE_METHODS + "): " + dead
                + " — delete the dead code or reference it");
    }

    /** Comments, string literals (incl. text blocks), and char
     * literals become spaces (newlines kept); code text and its brace
     * structure survive exactly. A single state-machine pass — regex
     * stripping cannot handle {@code '"'} or {@code //} inside a
     * string without corrupting quote balance. */
    private static String blankNonCode(String src) {
        StringBuilder out = new StringBuilder(src.length());
        int i = 0;
        int n = src.length();
        while (i < n) {
            char c = src.charAt(i);
            char c1 = i + 1 < n ? src.charAt(i + 1) : '\0';
            if (c == '/' && c1 == '/') {
                while (i < n && src.charAt(i) != '\n') {
                    out.append(' ');
                    i++;
                }
            } else if (c == '/' && c1 == '*') {
                out.append("  ");
                i += 2;
                while (i < n && !(src.charAt(i) == '*' && i + 1 < n
                        && src.charAt(i + 1) == '/')) {
                    out.append(src.charAt(i) == '\n' ? '\n' : ' ');
                    i++;
                }
                if (i < n) {
                    out.append("  ");
                    i += 2;
                }
            } else if (c == '"' && c1 == '"' && i + 2 < n
                    && src.charAt(i + 2) == '"') {
                out.append("   ");
                i += 3;
                while (i < n && !(src.charAt(i) == '"' && i + 2 < n
                        && src.charAt(i + 1) == '"'
                        && src.charAt(i + 2) == '"')) {
                    if (src.charAt(i) == '\\') {
                        out.append("  ");
                        i += 2;
                    } else {
                        out.append(src.charAt(i) == '\n' ? '\n' : ' ');
                        i++;
                    }
                }
                if (i < n) {
                    out.append("   ");
                    i += 3;
                }
            } else if (c == '"' || c == '\'') {
                out.append(' ');
                i++;
                while (i < n && src.charAt(i) != c) {
                    if (src.charAt(i) == '\\') {
                        out.append("  ");
                        i += 2;
                    } else {
                        out.append(' ');
                        i++;
                    }
                }
                if (i < n) {
                    out.append(' ');
                    i++;
                }
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    private static final Pattern PRIVATE_METHOD = Pattern.compile(
            "(?m)^\\s*private\\s+(?:static\\s+|final\\s+"
            + "|synchronized\\s+|@[\\w.]+\\s+|<[^>]+>\\s+)*"
            + "[\\w.<>\\[\\], ?@]+\\s+(\\w+)\\(");
    private static final Pattern CALL_OR_REF = Pattern.compile("::(\\w+)\\b|\\b(\\w+)\\s*\\(");

    @Test
    void noMethodBeyondTheLimit() throws IOException {
        List<String> violations = new ArrayList<>();
        for (Path p : mainSources()) {
            String cls = p.getFileName().toString().replace(".java", "");
            List<String> lines = Files.readAllLines(p);
            for (int i = 0; i < lines.size(); i++) {
                Matcher m = SIG.matcher(lines.get(i));
                if (!m.find() || lines.get(i).contains(";")) {
                    continue;
                }
                int depth = 0;
                boolean started = false;
                int j = i;
                while (j < lines.size()) {
                    String ln = sanitize(lines.get(j));
                    depth += count(ln, '{') - count(ln, '}');
                    if (ln.indexOf('{') >= 0) {
                        started = true;
                    }
                    if (started && depth == 0) {
                        break;
                    }
                    j++;
                }
                int len = j - i + 1;
                String key = cls + "." + m.group(1);
                int limit = METHOD_ALLOWLIST.getOrDefault(key, METHOD_LIMIT);
                if (len > limit) {
                    violations.add(key + " is " + len + " lines (limit "
                            + limit + ") — split it; the numbered-comment"
                            + " sections are the seams");
                }
                i = j;
            }
        }
        assertTrue(violations.isEmpty(), String.join("\n", violations));
    }

    /** Convergence slice 2 EXECUTED (SQL-IR backend-agnosticism):
     * the 62 origin-unstamped sites burned to 7, every survivor a
     * 4-arg construction CARRYING its origin (rebuild-transports +
     * three adjudicated stamps). The 2-arg M1 door survives for TEST
     * fixtures and the of(outs,name) unclaimed-name fallback — a
     * production null-origin reaching the strict H2 renderer WALLS
     * (slice 3), so any residue surfaces as a counted decline, never
     * a guessed spelling. Count only SHRINKS. 7 -> 8 on 2026-10-18, the plan path program's step 1b (docs/PLAN_PATH_PROGRAM_2026_10_17.md §3):
     * MirJson.readExpr rebuilds a Column from the plan's JSON with the origin it was written with -- an
     * origin-preserving rebuild, the typed tree read back. */
    @Test
    void unstampedColumnConstructionOnlyShrinks() throws IOException {
        int count = 0;
        for (Path p : mainSources()) {
            if (p.getFileName().toString().equals("SqlExpr.java")) {
                continue;
            }
            String src = Files.readString(p);
            int i = 0;
            while ((i = src.indexOf("new SqlExpr.Column(", i)) >= 0) {
                count++;
                i++;
            }
        }
        assertTrue(count <= 8, "raw SqlExpr.Column construction sites"
                + " grew: " + count + " > 8 — new references go through"
                + " the stamped doors (Column.of over an OutputCol /"
                + " derived / physical); raw construction is for"
                + " origin-preserving rebuilds only");
    }

    /** SQL-IR slice 2 finish (outputs-from-projections): a projection
     * frame's outputs DERIVE from its projections; {@code outputsOf}
     * survives only as the schema DOORS (three physical doors,
     * TDS/VALUES literals, the Pivot source) and the per-projection
     * CONTRACT supplier ({@code paired}/{@code named} attachment at
     * construction). {@code withOutputs} is the star-frame door with
     * ONE production caller (the pivot probe — genuinely external
     * knowledge). A new call site is a new frame ASSERTING outputs the
     * projections should carry — count only SHRINKS. */
    @Test
    void schemaAssertedOutputsOnlyShrink() throws IOException {
        int outputsOf = 0;
        int withOutputs = 0;
        for (Path p : mainSources()) {
            String src = Files.readString(p);
            for (int i = 0; (i = src.indexOf("outputsOf(", i)) >= 0; i++) {
                outputsOf++;
            }
            for (int i = 0; (i = src.indexOf("withOutputs(", i)) >= 0; i++) {
                withOutputs++;
            }
        }
        assertTrue(outputsOf <= 23, "outputsOf call sites grew: "
                + outputsOf + " > 23 — outputs derive from projections;"
                + " attach the contract slot per projection"
                + " (SqlSelect.paired / Fold.named), never assert a"
                + " frame's outputs from the schema");
        // 2 -> 3 (leg 3.3, 2026-09-19): WireProbe.staticized (exec.WireTypes's until D71) frames a raw
        // executeInDb grid by the DATABASE's reported columns — the pivot
        // probe's own class of externally-discovered outputs
        assertTrue(withOutputs <= 3, "withOutputs call sites grew: "
                + withOutputs + " > 3 — star frames read their source;"
                + " only externally-discovered outputs (pivot probe) may"
                + " use this door");
    }

    @Test
    void mutableInstanceStateIsExplicit() throws IOException {
        List<String> violations = new ArrayList<>();
        for (Path p : mainSources()) {
            String cls = p.getFileName().toString().replace(".java", "");
            for (String ln : Files.readAllLines(p)) {
                Matcher m = MUTABLE_FIELD.matcher(ln);
                if (!m.find()) {
                    m = NESTED_MUTABLE_FIELD.matcher(ln);
                    if (!m.find()) {
                        continue;
                    }
                }
                // an enum-constant list (EQ, NEQ, GTE;) is commas outside
                // generics — a field's type only holds commas inside <>
                String beforeName = ln.substring(0, m.start(1));
                if (beforeName.contains(",") && !beforeName.contains("<")) {
                    continue;
                }
                if (!MUTABLE_FIELD_ALLOWLIST.contains(cls + "." + m.group(1))) {
                    violations.add(cls + "." + m.group(1)
                            + " is a mutable instance field — make it final,"
                            + " move it into an explicit frame object, or"
                            + " allowlist it WITH the reason");
                }
            }
        }
        assertTrue(violations.isEmpty(), String.join("\n", violations));
    }

    private static int count(String s, char c) {
        int n = 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == c) {
                n++;
            }
        }
        return n;
    }
}
