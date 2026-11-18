// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import com.tngtech.archunit.core.domain.JavaAccess;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaCodeUnit;
import com.tngtech.archunit.core.domain.JavaConstructor;
import com.tngtech.archunit.core.domain.JavaField;
import com.tngtech.archunit.core.domain.JavaFieldAccess;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.ClassFileImporter;
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
import java.util.TreeSet;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A PRODUCT MEMBER ONLY THE EDGE USES IS COUNTED (J40 of the edge audit, the orchestrator's order of 2026-11-18 on the
 * confirmation audit's suspected list: "a product public member whose only callers are com.legend.edge is counted,
 * unless it is a pinned hook"). A public member of a product class -- a method, a constructor, a field -- that the
 * edge uses and nothing else in the product's, the hosts' and the edge's jars uses is the edge's work standing in the
 * product. Each one is an open census line ({@link #OPEN}, by its audit id) until it moves to the edge, or until it is
 * ruled a product API the edge reads ({@link #READ_API}, with the reason, not counted). The two together are exact, so
 * a new one is seen.
 *
 * <p>Not counted:
 * <ul>
 *   <li>a member of a hook type, or of a class implementing one ({@code HookLambdasTest.hookTypes()}: the hook is the
 *       edge's extension point, HookRulingTest's);</li>
 *   <li>a method overriding a supertype's: it is reached by dispatch, never by its own name;</li>
 *   <li>a synthetic or bridge member;</li>
 *   <li>a member nothing uses (dead code is another question).</li>
 * </ul>
 * A use from the member's own class, or from a host's door jar outside {@code com.legend.edge}, is a product use. A
 * compile-time constant is inlined where it is read, so its bytecode shows no reader: a static final String or
 * primitive with none is read from the sources the guard rails declare ({@code Owner.NAME}, or a static import of it).
 * Named limits: a constant read only through a wildcard static import is not seen, and a {@code Name.CONSTANT} in a
 * product file reads as a product use whichever {@code Name} it means (both fail toward counting less).
 */
@Tag("guardrail")
class EdgeOnlyMembersTest {

    /** The members only the edge uses that MOVE to the edge or are DELETED (the orchestrator's triage ruling of
     *  2026-11-18), each "member" -> "J40.Class OWNER KIND (date): why": ONE CENSUS LINE PER CLASS ({@link #lines}),
     *  listing its members here (J33's and J40's audit items fold into it). Shrink-only: a member leaves when it moves,
     *  and its class's line closes with its last. */
    static final Map<String, String> OPEN = Map.ofEntries(
            // (ExecuteChainAssembly#concatenated and NativeFn$OperatorRun#fqn: ruled READ_API by the orchestrator 2026-11-18,
            // train 18, below -- J40.ExecuteChainAssembly and J40.NativeFn close)
            // (PlanStyle$Activity#engines(): gone 2026-11-18 by F's J40.PlanStyle commit, 7bd52379 -- the activity SQL a
            // two-form hook, planner.ActivityTexts, the edge's form legend-engine's text; J40.PlanStyle closes)
            // (ScanRelations#rootImpl, both overloads: moved 2026-11-18 by E-Java's j40c, 71be053489, to the edge's
            // SetImplementations -- J40.ScanRelations closes)
            // (VerdictSql#textEquals: moved 2026-11-18 by E-Java's j40c, 9dd14f6fdc -- the product keeps constantTextsEqual,
            // READ_API below; J40.VerdictSql closes)
            );
            // re-measured 2026-11-18 on train 15 1931d419b1 (the GUARD stream's re-cut): train 15 closed seven pins
            // (PlatformTypes' TO_SQL_STRING and TO_SQL_STRING_PRETTY, BodyPlanner#pushDownEnums, QueryParameters$Declared's
            // planParam and signature, AggregationAwareRouting#mainSetId, Verdicts#scalarString -- two class lines gone,
            // J40.AggregationAwareRouting and J40.Verdicts) and left three members only the edge uses, pinned here: J29's
            // %r newline form (VerdictSql), J29's legend-pure sort for assertSameElements' words (PureAsserts, a new class
            // line) and J31's residue, a declared parameter's multiplicity (QueryParameters)
            // re-measured on train 16 b836ff636f (2026-11-18, the GUARD stream): 25 rows closed by the train's landings --
            // E-Java's J40 (185d54a494: PlatformTypes' three DDL names, CanonicalDivergence#probeGridText, ReplayGates'
            // three, VerdictQueries#extentSubset, ExecutionContext#withPostProcessors, PostProcessors#withNonExecutable,
            // PlanConnection#planText), F's J40 census (e92c465d: SqlTextInputs' seven, BodyPlanner#boundContext x2,
            // TypedQuery#answersADiagnostic) and F's a55e1147 (VerdictQueries#text): the J40.CanonicalDivergence,
            // J40.AssertVerdicts, J40.VerdictQueries, J40.ExecutionContext, J40.SqlTextInputs, J40.BodyPlanner and
            // J40.TypedQuery lines go; one found: legend-engine's toSQL native's name, its reader moved with F's
            // SqlTextInputs to the edge
            // (PlatformTypes#TO_SQL: gone 2026-11-18 by F's J40.PlatformTypes commit -- the edge's SqlTextInputs names
            // the toSQL handle by the native registry's member, NativeFn.Handle.TO_SQL; J40.PlatformTypes closes)
            // (PlatformTypes#TO_NON_EXECUTABLE_SQL_STRING and VerdictSql#reprNewlines: moved to the edge 2026-11-18 by
            // E-Java's j40b, 9099e1d6d3 and 0f2eba0c85, train 17 -- J40.PlatformTypes stays open on TO_SQL, J40.VerdictSql
            // on textEquals)
            // (testexec.PureAsserts#sorted: ruled READ_API by the orchestrator 2026-11-18, below -- J40.PureAsserts closes)

    /** The members only the edge uses that are a product API the edge reads (READ_API: a fact the product owns and names
     *  in lite's terms; the orchestrator's triage ruling of 2026-11-18), each with its dated reason. Not counted. */
    static final Map<String, String> READ_API = Map.ofEntries(
            // the orchestrator's ruling of 2026-11-18 (train 18, J40.VerdictSql, E-Java's 9dd14f6fdc): VerdictSql keeps the
            // verdict builder in lite's terms; what the texts are (a golden SQL text and ours) is the edge's SqlTextVerdicts'
            // -- J40.VerdictSql closes
            Map.entry("com.legend.lowering.VerdictSql#constantTextsEqual(java.lang.String, java.lang.String)",
                    "2026-11-18: a verdict row over two constant texts, string-equal: lite's own verdict builder; the edge's SQL-text arm composes it"),
            // ruled READ_API by the orchestrator (2026-11-18, train 18, on the GUARD stream's triage of E-Java's b70d85ab9):
            // a typed call to the platform's string::plus, lite's own building block (like the builders exposed for
            // textEquals), and a native's platform name; the edge's EngineExecutedValues builds its envelope with them
            Map.entry("com.legend.compiler.spec.ExecuteChainAssembly#concatenated(com.legend.compiler.element.ModelContext, java.util.List)",
                    "2026-11-18: a typed call to the platform's string::plus, lite's own building block (the orchestrator's ruling)"),
            Map.entry("com.legend.builtin.NativeFn$OperatorRun#fqn()",
                    "2026-11-18: an operator run's platform name, lite's own (the orchestrator's ruling)"),
            // ruled READ_API by the orchestrator (2026-11-18, ~07:20, on E-Java's proposal): the product judge's own total
            // order (pure's sort(): numbers, strings, booleans, temporals), a public face over Equality.sorted that the
            // product's PureAssertsTest reads too; the edge's EdgeHostWords sorts assertSameElements' words by it
            Map.entry("com.legend.testexec.PureAsserts#sorted(java.util.List)",
                    "2026-11-18: the product judge's own total order over Any, pure's sort() (the orchestrator's ruling)"),
            // the orchestrator's ruling at J40 (2026-11-18, E-Java's 185d54a494): a plan connection's facts stay READ_API,
            // its engine spelling the edge's printer's -- left the edge's alone to read when planText moved (train 16)
            Map.entry("com.legend.compiler.spec.typed.ExecutionContext$PlanConnection#className()",
                    "2026-11-18: a plan connection's own fact, its class (the orchestrator's ruling at J40; planText moved to the edge's EnginePlans)"),
            Map.entry("com.legend.compiler.spec.typed.ExecutionContext$PlanConnection#type()",
                    "2026-11-18: a plan connection's own fact, its database type (the orchestrator's ruling at J40; planText moved to the edge's EnginePlans)"),
            // ruled READ_API by the orchestrator (2026-11-18, as F asks, F's e92c465d): which specification carries a
            // connection's test data is the model's vocabulary (a database's name, LocalH2), and lite's product reads a
            // connection's own test data as loads; the J40.ConnectionSpecification line goes
            Map.entry("com.legend.model.ConnectionSpecification#carryingTestData(java.lang.String)",
                    "2026-11-18: the model's vocabulary, which specification carries a connection's test data (the orchestrator's ruling, as F asks)"),
            // ruled READ_API by the orchestrator (2026-11-18, on the re-cut's triage): a declared parameter's multiplicity
            // is lite's own fact about the user's query, though only the edge's printer reads it today -- the line
            // between L-B's shape and a real fact is what it MEANS, not who reads it; the J40.QueryParameters line goes
            Map.entry("com.legend.planner.QueryParameters$Declared#multiplicity()",
                    "2026-11-18: lite's own fact about the user's query, a declared parameter's multiplicity (the orchestrator's ruling)"),
            // re-ruled READ_API by the orchestrator (2026-11-18, on E-SQL's five): MappingDefinition's enum and
            // AggregationAware facts are lite's own model facts, read by the edge (stamped at phase E; no reach-back
            // growth) -- the J40.MappingDefinition line goes
            Map.entry("com.legend.model.MappingDefinition$EnumColumn#column()",
                    "2026-11-18: lite's mapping's own enum-mapping fact, read by the edge's plan text (the orchestrator's ruling on E-SQL's five)"),
            Map.entry("com.legend.model.MappingDefinition$EnumColumn#enumMappingId()",
                    "2026-11-18: lite's mapping's own enum-mapping fact, read by the edge's plan text (the orchestrator's ruling on E-SQL's five)"),
            Map.entry("com.legend.model.MappingDefinition$EnumColumn#table()",
                    "2026-11-18: lite's mapping's own enum-mapping fact, read by the edge's plan text (the orchestrator's ruling on E-SQL's five)"),
            Map.entry("com.legend.model.MappingDefinition$RelationalSource$Table#aggregationAwareMain()",
                    "2026-11-18: lite's mapping's own AggregationAware fact, read by the edge's plan text (the orchestrator's ruling on E-SQL's five)"),
            Map.entry("com.legend.model.MappingDefinition$RelationalSource$Table#enumColumns()",
                    "2026-11-18: lite's mapping's own enum-mapping fact, read by the edge's plan text (the orchestrator's ruling on E-SQL's five)"),
            // re-ruled READ_API by the orchestrator (2026-11-18, to E-Java): the facts are lite's own, read by the edge's
            // printer; PlanConnection.planText, legend-engine's plan text line, stays OPEN
            Map.entry("com.legend.compiler.spec.typed.ExecutionContext#connectionInstance()",
                    "2026-11-18: lite's execution frame's own connection fact (the orchestrator's ruling to E-Java)"),
            Map.entry("com.legend.compiler.spec.typed.ExecutionContext#connectionName()",
                    "2026-11-18: lite's execution frame's own connection fact (the orchestrator's ruling to E-Java)"),
            Map.entry("com.legend.compiler.spec.typed.ExecutionContext#quoteIdentifiers()",
                    "2026-11-18: lite's execution frame's own quoting fact (the orchestrator's ruling to E-Java)"),
            Map.entry("com.legend.compiler.spec.typed.ExecutionContext#storeFqn()",
                    "2026-11-18: lite's execution frame's own store fact (the orchestrator's ruling to E-Java)"),
            Map.entry("com.legend.compiler.spec.typed.TypedFrom#connectionName()",
                    "2026-11-18: lite's from()'s own connection name (the orchestrator's ruling to E-Java)"),
            Map.entry("com.legend.compiler.spec.typed.TypedTableReference#accessor()",
                    "2026-11-18: lite's table reference's own accessor (the orchestrator's ruling to E-Java)"),
            Map.entry("com.legend.testplan.BodyDriver$ExecEnv#appealOn()",
                    "2026-11-18: lite's test planner's own appeal environment (the orchestrator's ruling to E-Java)"),
            Map.entry("com.legend.Compiler$Target#<init>(java.lang.String)",
                    "2026-11-18: a query's execution target (U3), lite's own; the edge's appeal codec rebuilds and reads it"),
            Map.entry("com.legend.Compiler$Target#store()",
                    "2026-11-18: a query's execution target (U3), lite's own; the edge's appeal codec rebuilds and reads it"),
            Map.entry("com.legend.TypedQuery#runtime(java.lang.String)",
                    "2026-11-18: the runtime a query runs on, lite's one rule for every door"),
            Map.entry("com.legend.TypedQuery#target()",
                    "2026-11-18: the query's execution target (Compiler.Target), lite's own"),
            Map.entry("com.legend.arrowipc.ZstdFrame#of([B)",
                    "2026-11-18: lite's own Arrow IPC zstd frame; its product caller lands with the host train (J36)"),
            Map.entry("com.legend.base.DataSize$TooLarge#characters()",
                    "2026-11-18: lite's size-limit refusal's own facts, which the edge words as upstream does"),
            Map.entry("com.legend.base.DataSize$TooLarge#limitMegabytes()",
                    "2026-11-18: lite's size-limit refusal's own facts, which the edge words as upstream does"),
            Map.entry("com.legend.base.RunScope#judging()",
                    "2026-11-18: the run witness's post-run judging scope (lite's G4 proof), opened by the edge's appeals"),
            Map.entry("com.legend.compiler.StoreLookups#findOwnTableDefinition(java.lang.String, java.lang.String)",
                    "2026-11-18: a lookup over lite's compiled store: which database owns a table"),
            Map.entry("com.legend.compiler.element.PureModelContext#withExecutionOverlay(com.legend.model.RuntimeDefinition, com.legend.model.ConnectionDefinition)",
                    "2026-11-18: lite's model with a driver-supplied runtime and connection overlay"),
            Map.entry("com.legend.compiler.spec.NativeDispatch$RoutineCall#args()",
                    "2026-11-18: lite's native dispatch's routine call (a JAVA_ROUTINE's fqn and args)"),
            Map.entry("com.legend.compiler.spec.NativeDispatch$RoutineCall#fqn()",
                    "2026-11-18: lite's native dispatch's routine call (a JAVA_ROUTINE's fqn and args)"),
            Map.entry("com.legend.error.InvalidParameters#problems()",
                    "2026-11-18: lite's invalid-parameters refusal's own facts"),
            Map.entry("com.legend.error.MissingParameters#missing()",
                    "2026-11-18: lite's missing-parameters refusal's own facts"),
            Map.entry("com.legend.exec.PlanRunner#rows(com.legend.executionplan.ExecutionPlan, java.util.Map, com.legend.exec.PlanSessions$Source, java.util.UUID)",
                    "2026-11-18: exec's run of a plan to typed rows, lite's own"),
            Map.entry("com.legend.executionplan.ColumnFacts#store(com.legend.sql.SqlDdl$ColumnType)",
                    "2026-11-18: a column's declared store type, lite's column fact"),
            Map.entry("com.legend.executionplan.PlanJson#loadStepJson(com.legend.executionplan.ExecutionPlan$LoadStep)",
                    "2026-11-18: a half of lite's own plan codec (E-P5's ask: load steps and columns)"),
            Map.entry("com.legend.executionplan.PlanJson#loadStepOf(com.legend.json.Json$Obj)",
                    "2026-11-18: a half of lite's own plan codec (E-P5's ask: load steps and columns)"),
            Map.entry("com.legend.executionplan.PlanJson#readColumns(java.lang.String)",
                    "2026-11-18: a half of lite's own plan codec (E-P5's ask: load steps and columns)"),
            Map.entry("com.legend.executionplan.PlanJson#writeColumns(java.util.List)",
                    "2026-11-18: a half of lite's own plan codec (E-P5's ask: load steps and columns)"),
            Map.entry("com.legend.lowering.ReprParts#cat([Lcom.legend.sql.SqlExpr;)",
                    "2026-11-18: the lowering's own representation parts (Scalars, LiteralSpelling), which the edge's texts compose"),
            Map.entry("com.legend.lowering.ReprParts#dateLiteralText(com.legend.compiler.spec.typed.TypedSpec)",
                    "2026-11-18: the lowering's own representation parts (Scalars, LiteralSpelling), which the edge's texts compose"),
            Map.entry("com.legend.lowering.ReprParts#heldAsInteger(com.legend.sql.SqlExpr)",
                    "2026-11-18: the lowering's own representation parts (Scalars, LiteralSpelling), which the edge's texts compose"),
            Map.entry("com.legend.lowering.ReprParts#pureToString(com.legend.lowering.Styled, com.legend.compiler.element.type.Type, com.legend.sql.SqlExpr)",
                    "2026-11-18: the lowering's own representation parts (Scalars, LiteralSpelling), which the edge's texts compose"),
            Map.entry("com.legend.plan.EnumMappings#of(com.legend.compiler.element.ModelContext, java.lang.String, java.lang.String)",
                    "2026-11-18: a lookup over lite's normalized mapping: an enum's enumeration mapping"),
            Map.entry("com.legend.plan.QueryPlan#columns()",
                    "2026-11-18: lite's query plan's own columns and shape"),
            Map.entry("com.legend.plan.QueryPlan#shape()",
                    "2026-11-18: lite's query plan's own columns and shape"),
            Map.entry("com.legend.planner.PlanEnv#withFeatures(java.util.Set)",
                    "2026-11-18: lite's planning environment's own features and lets"),
            Map.entry("com.legend.planner.PlanEnv#withOwnQueryLets()",
                    "2026-11-18: lite's planning environment's own features and lets"),
            Map.entry("com.legend.planner.PlanEnv#withQueryLets(java.util.Map)",
                    "2026-11-18: lite's planning environment's own features and lets"),
            Map.entry("com.legend.protocol.EmbeddedDataKinds#name(com.legend.protocol.Protocol$PEmbeddedDataValue)",
                    "2026-11-18: an embedded-data kind's name, lite's protocol reader's own"),
            Map.entry("com.legend.protocol.PureComposer#lambda(com.legend.json.Json$Obj, com.legend.protocol.PureComposer$Style)",
                    "2026-11-18: a half of lite's protocol printer: a lambda's JSON as Pure text"),
            Map.entry("com.legend.queries.SavedQueries$Refusal#kind()",
                    "2026-11-18: lite's saved-query refusal's own facts"),
            Map.entry("com.legend.queries.SavedQueries$Refusal#operands()",
                    "2026-11-18: lite's saved-query refusal's own facts"),
            Map.entry("com.legend.result.HostRows#stream(java.lang.String, java.util.List, com.legend.result.ResultSink)",
                    "2026-11-18: a half of lite's own host-rows codec (a host's answer to its statement)"),
            Map.entry("com.legend.result.Output$Rows#<init>(com.legend.result.ResultSink)",
                    "2026-11-18: lite's Output: a caller's own sink"),
            Map.entry("com.legend.server.LegendHttpServer$Context#cubeStore()",
                    "2026-11-18: the server's mount context, lite's own"),
            Map.entry("com.legend.server.LegendHttpServer$Context#queryStore()",
                    "2026-11-18: the server's mount context, lite's own"),
            Map.entry("com.legend.server.LegendHttpServer$Context#sources()",
                    "2026-11-18: the server's mount context, lite's own"),
            Map.entry("com.legend.server.LiteV1Api$Answer#bytes(int, [B, java.lang.String, java.util.Map)",
                    "2026-11-18: lite v1's own answer and runner seams"),
            Map.entry("com.legend.server.LiteV1Api$Runner#run(com.legend.TypedQuery, java.lang.String, com.legend.result.Output)",
                    "2026-11-18: lite v1's own answer and runner seams"),
            Map.entry("com.legend.server.ModelInput$Data#model()",
                    "2026-11-18: lite's model input, its own records"),
            Map.entry("com.legend.server.ModelInput$Text#code()",
                    "2026-11-18: lite's model input, its own records"),
            Map.entry("com.legend.sql.DateFmt#TIME_ONLY",
                    "2026-11-18: a MIR date format, lite's own vocabulary (the edge cannot add one: MIR is sealed)"),
            Map.entry("com.legend.sql.SqlDdl$DropSchema#<init>(java.lang.String, boolean)",
                    "2026-11-18: MIR DDL, lite's own vocabulary (the edge cannot add one: MIR is sealed)"),
            Map.entry("com.legend.testdatagen.TestDataPlanner#seedFacts(com.legend.compiler.element.ModelContext, com.legend.testdatagen.TestDataPlanner$Seed)",
                    "2026-11-18: lite's test-data generator's own facts (its seed, its keys, its refusals), which the edge writes as upstream's JSON"),
            Map.entry("com.legend.testdatagen.TestDataPlanner$NoRowIdentifiers#database()",
                    "2026-11-18: lite's test-data generator's own facts (its seed, its keys, its refusals), which the edge writes as upstream's JSON"),
            Map.entry("com.legend.testdatagen.TestDataPlanner$NoRowIdentifiers#keys()",
                    "2026-11-18: lite's test-data generator's own facts (its seed, its keys, its refusals), which the edge writes as upstream's JSON"),
            Map.entry("com.legend.testdatagen.TestDataPlanner$NoRowIdentifiers#schema()",
                    "2026-11-18: lite's test-data generator's own facts (its seed, its keys, its refusals), which the edge writes as upstream's JSON"),
            Map.entry("com.legend.testdatagen.TestDataPlanner$NoRowIdentifiers#table()",
                    "2026-11-18: lite's test-data generator's own facts (its seed, its keys, its refusals), which the edge writes as upstream's JSON"),
            Map.entry("com.legend.testdatagen.TestDataPlanner$NoRowIdentifiers#view()",
                    "2026-11-18: lite's test-data generator's own facts (its seed, its keys, its refusals), which the edge writes as upstream's JSON"),
            Map.entry("com.legend.testdatagen.TestDataPlanner$RowIdentifiersRepeated#schema()",
                    "2026-11-18: lite's test-data generator's own facts (its seed, its keys, its refusals), which the edge writes as upstream's JSON"),
            Map.entry("com.legend.testdatagen.TestDataPlanner$RowIdentifiersRepeated#table()",
                    "2026-11-18: lite's test-data generator's own facts (its seed, its keys, its refusals), which the edge writes as upstream's JSON"),
            Map.entry("com.legend.testdatagen.TestDataPlanner$SeedFacts#<init>(java.lang.String, java.lang.String, java.lang.String, com.legend.sql.SqlSelect, java.util.List)",
                    "2026-11-18: lite's test-data generator's own facts (its seed, its keys, its refusals), which the edge writes as upstream's JSON"),
            Map.entry("com.legend.testdatagen.TestDataPlanner$SeedFacts#database()",
                    "2026-11-18: lite's test-data generator's own facts (its seed, its keys, its refusals), which the edge writes as upstream's JSON"),
            Map.entry("com.legend.testdatagen.TestDataPlanner$SeedFacts#keys()",
                    "2026-11-18: lite's test-data generator's own facts (its seed, its keys, its refusals), which the edge writes as upstream's JSON"),
            Map.entry("com.legend.testdatagen.TestDataPlanner$SeedFacts#rows()",
                    "2026-11-18: lite's test-data generator's own facts (its seed, its keys, its refusals), which the edge writes as upstream's JSON"),
            Map.entry("com.legend.testdatagen.TestDataPlanner$SeedFacts#schema()",
                    "2026-11-18: lite's test-data generator's own facts (its seed, its keys, its refusals), which the edge writes as upstream's JSON"),
            Map.entry("com.legend.testdatagen.TestDataPlanner$SeedFacts#table()",
                    "2026-11-18: lite's test-data generator's own facts (its seed, its keys, its refusals), which the edge writes as upstream's JSON"),
            Map.entry("com.legend.testdatagen.TestDataPlanner$SeedKey#<init>(java.lang.String, com.legend.sql.SqlExpr, boolean, com.legend.sql.SqlExpr)",
                    "2026-11-18: lite's test-data generator's own facts (its seed, its keys, its refusals), which the edge writes as upstream's JSON"),
            Map.entry("com.legend.testdatagen.TestDataPlanner$SeedKey#cell()",
                    "2026-11-18: lite's test-data generator's own facts (its seed, its keys, its refusals), which the edge writes as upstream's JSON"),
            Map.entry("com.legend.testdatagen.TestDataPlanner$SeedKey#name()",
                    "2026-11-18: lite's test-data generator's own facts (its seed, its keys, its refusals), which the edge writes as upstream's JSON"),
            Map.entry("com.legend.testdatagen.TestDataPlanner$SeedKey#order()",
                    "2026-11-18: lite's test-data generator's own facts (its seed, its keys, its refusals), which the edge writes as upstream's JSON"),
            Map.entry("com.legend.testdatagen.TestDataPlanner$SeedKey#timestamp()",
                    "2026-11-18: lite's test-data generator's own facts (its seed, its keys, its refusals), which the edge writes as upstream's JSON"),
            Map.entry("com.legend.testexecutionplan.EnumFacts#<init>(java.util.List, java.util.Map, java.util.Map)",
                    "2026-11-18: lite's test plan's enum facts (plan-as-data slice 4), carried by the edge's appeal codec"),
            Map.entry("com.legend.testexecutionplan.EnumFacts#decodes()",
                    "2026-11-18: lite's test plan's enum facts (plan-as-data slice 4), carried by the edge's appeal codec"),
            Map.entry("com.legend.testexecutionplan.EnumFacts#enums()",
                    "2026-11-18: lite's test plan's enum facts (plan-as-data slice 4), carried by the edge's appeal codec"),
            Map.entry("com.legend.testexecutionplan.EnumFacts#properties()",
                    "2026-11-18: lite's test plan's enum facts (plan-as-data slice 4), carried by the edge's appeal codec"),
            Map.entry("com.legend.testexecutionplan.RecordJson#failure(com.legend.testexecutionplan.Failure)",
                    "2026-11-18: a half of lite's own test-plan record codec"),
            Map.entry("com.legend.testexecutionplan.RecordJson#readFailure(com.legend.json.Json$Obj)",
                    "2026-11-18: a half of lite's own test-plan record codec"),
            Map.entry("com.legend.testexecutionplan.RecordJson#readSide(com.legend.json.Json$Obj)",
                    "2026-11-18: a half of lite's own test-plan record codec"),
            Map.entry("com.legend.testexecutionplan.RecordJson#readSideShape(com.legend.json.Json$Obj)",
                    "2026-11-18: a half of lite's own test-plan record codec"),
            Map.entry("com.legend.testexecutionplan.RecordJson#side(com.legend.testexecutionplan.TestPlan$Side)",
                    "2026-11-18: a half of lite's own test-plan record codec"),
            Map.entry("com.legend.testexecutionplan.RecordJson#sideShape(com.legend.testexecutionplan.SideShape)",
                    "2026-11-18: a half of lite's own test-plan record codec"),
            Map.entry("com.legend.testplan.JudgePlans$PlannedSide#root()",
                    "2026-11-18: lite's judge planning's own side record"));
            // (testplan.NotAConstant: moved to the edge 2026-11-18 by E-Java's train-17 item 5, 645c3e4464 -- the edge's
            // collector raises it)

    /** The members only the edge uses that J21 ruled the product's (one parser, its legend-engine level the edge's
     *  alone), each with its dated reason. Not counted. */
    static final Map<String, String> RULED_J21 = Map.ofEntries(
            Map.entry("com.legend.door.Parse#engineDocumentJson(java.lang.String)",
                    "J21 (2026-11-18): one parser (G1), its legend-engine level reached by the edge alone (ArchitectureTest.legendEnginesGrammarIsTheEdgesAlone); the user's \"parse legend-pure and legend-engine projects as-is\""),
            Map.entry("com.legend.door.Parse#engineLambdaJson(java.lang.String)",
                    "J21 (2026-11-18): one parser (G1), its legend-engine level reached by the edge alone (ArchitectureTest.legendEnginesGrammarIsTheEdgesAlone); the user's \"parse legend-pure and legend-engine projects as-is\""),
            Map.entry("com.legend.door.Parse#engineRecords(java.lang.String)",
                    "J21 (2026-11-18): one parser (G1), its legend-engine level reached by the edge alone (ArchitectureTest.legendEnginesGrammarIsTheEdgesAlone); the user's \"parse legend-pure and legend-engine projects as-is\""),
            Map.entry("com.legend.parser.PmcdParser#parseDocument(java.lang.String, boolean)",
                    "J21 (2026-11-18): one parser (G1), its legend-engine level reached by the edge alone (ArchitectureTest.legendEnginesGrammarIsTheEdgesAlone); the user's \"parse legend-pure and legend-engine projects as-is\""),
            Map.entry("com.legend.parser.SpecParser#lambdaJson(java.lang.String, boolean)",
                    "J21 (2026-11-18): one parser (G1), its legend-engine level reached by the edge alone (ArchitectureTest.legendEnginesGrammarIsTheEdgesAlone); the user's \"parse legend-pure and legend-engine projects as-is\""));

    /** The census lines: one per class holding a member of {@link #OPEN}. */
    static Set<String> lines() {
        Set<String> out = new TreeSet<>();
        OPEN.values().forEach(v -> out.add(v.split(" ")[0]));
        return out;
    }

    private static final String EDGE = "com.legend.edge.";

    @Test
    void theEdgeOnlyMembersAreCountedOrRuled() throws IOException {
        JavaClasses classes = new ClassFileImporter().importJars(jarFiles(ConstantPools.jars("legend.product.jars",
                "legend.authored.jars", "legend.edge.jars")));
        // a member of a class already on a census line (EdgeSeparationTest.COMPAT_CLASSES: a marked compatibility class
        // and its product dependents) is counted by that line, never twice (the triage of 2026-11-18: EngineTextPlanning,
        // PlanStyleFacets, C4's)
        Set<String> found = new TreeSet<>();
        for (String member : found(classes, HookLambdasTest.hookTypes(), sources())) {
            String owner = member.substring(0, member.indexOf('#'));
            if (!EdgeSeparationTest.COMPAT_CLASSES.containsKey(owner.contains("$") ? owner.substring(0,
                    owner.indexOf('$')) : owner)) {
                found.add(member);
            }
        }
        Set<String> pinned = new TreeSet<>(OPEN.keySet());
        pinned.addAll(READ_API.keySet());
        pinned.addAll(RULED_J21.keySet());
        pinned.addAll(HookRulingTest.HOOK_ENTRIES.keySet());
        assertEquals(pinned, found, "a public product member only the edge uses: move it to the edge, or pin it in OPEN"
                + " (its class's census line), READ_API or RULED_J21 (with its reason) or HookRulingTest.HOOK_ENTRIES;"
                + " one pinned that the product uses again, or that went, is deleted");
        int pins = OPEN.size() + READ_API.size() + RULED_J21.size() + HookRulingTest.HOOK_ENTRIES.size();
        assertEquals(pins, pinned.size(), "a member pinned twice (OPEN, READ_API, RULED_J21, HOOK_ENTRIES)");
        // each line is its member's class: "J40.<the top-level class's simple name>"
        OPEN.forEach((member, line) -> {
            String owner = member.substring(0, member.indexOf('#'));
            String top = owner.contains("$") ? owner.substring(0, owner.indexOf('$')) : owner;
            assertTrue(line.startsWith("J40." + top.substring(top.lastIndexOf('.') + 1) + " "), member + ": " + line);
        });
    }

    /** Planted: a product class whose members the edge alone uses -- a static, a constructor, a field, a constant read
     *  in source -- is found; a member the product uses too, an override, a member nothing uses, and a hook's member
     *  are not. */
    @Test
    void theGuardBites() {
        JavaClasses classes = new ClassFileImporter().importClasses(PlantedEdgeOnlyApi.class,
                PlantedEdgeOnlyApi.Hooked.class, PlantedEdgeOnlyApi.Kind.class, com.legend.edge.PlantedEdgeOnlyCaller.class);
        String api = PlantedEdgeOnlyApi.class.getName();
        Map<String, String> sources = Map.of(
                "core/src/test/java/com/legend/edge/PlantedEdgeOnlyCaller.java",
                "class PlantedEdgeOnlyCaller { String c() { return PlantedEdgeOnlyApi.EDGE_CONSTANT; } }",
                "core/src/test/java/com/legend/PlantedEdgeOnlyApi.java",
                "public final class PlantedEdgeOnlyApi { public static final String EDGE_CONSTANT = \"e\";"
                        + " public static final String SHARED_CONSTANT = \"s\"; String own() { return SHARED_CONSTANT; } }");
        assertEquals(Set.of(api + "#edgeOnly()", api + "#viaEdgeOnly()", api + "#viaLambda()", api + "#countdown(int)",
                        api + "#viaDead()", api + "#<init>(java.lang.String)", api + "#edgeField", api + "#EDGE_CONSTANT"),
                found(classes, Set.of(PlantedEdgeOnlyApi.Hook.class.getName()), sources),
                "the edge-only static, constructor, field and source-read constant are found, and the members reached"
                        + " only through an edge-only member or its lambda, or by themselves or dead non-public code besides; the shared"
                        + " method, the"
                        + " override, the unused member, the cycle no edge use enters, the enum's values(), a member a main or a public method also uses, the shared"
                        + " constant and the hook's member are not");
    }

    /** Each public member of a product class in {@code classes} that the edge alone uses, TRANSITIVELY (the
     *  orchestrator's ruling of 2026-11-18, (4): a use from a member that is itself edge-only is the edge's -- VerdictQueries'
     *  fromWrapped, reached only through text(), which only the edge calls). Computed over every code unit and field of
     *  the product classes, private helpers and lambda bodies included, as a greatest fixpoint: start from every unit with
     *  a use, drop each one a product use reaches (an origin neither the edge nor a unit still held, a use of a unit by
     *  itself aside, and a use from a non-public method nothing uses -- dead code's, or a test's -- aside), until none
     *  drops; then keep the units an edge use reaches through held units (a cycle no edge use
     *  enters is no edge's). Reported: the public members of public classes, hooks, overrides, synthetic members and an
     *  enum's compiler-written values() aside (the ruling's (3)). A lambda body's use is its enclosing method's (javac's
     *  lambda$name$n: every method of that name in the class); an anonymous class's methods are reached by dispatch and
     *  are never held, so their uses are the product's (counting less). Named limits: a non-public method reached by
     *  reflection alone reads as dead, so its uses read as nobody's (counting more); a PUBLIC method nothing in the jars
     *  uses reads as an entry point, so its uses are the product's even when it is dead (counting less: fromWrapped's
     *  two-argument overload, which nothing calls, keeps the three-argument one from being counted until it goes). */
    static Set<String> found(JavaClasses classes, Set<String> hooks, Map<String, String> sources) {
        Map<String, List<JavaAccess<?>>> uses = new TreeMap<>();
        Map<String, List<String>> byName = new TreeMap<>();
        for (JavaClass c : classes) {
            if (!c.getName().startsWith("com.legend.") || c.getName().startsWith(EDGE)) {
                continue;
            }
            for (JavaCodeUnit u : c.getCodeUnits()) {
                byName.computeIfAbsent(c.getName() + "#" + u.getName(), k -> new ArrayList<>())
                        .add(ServiceLoaderSitesTest.key(c, u));
                List<JavaAccess<?>> us = new ArrayList<>();
                if (u instanceof JavaMethod m) {
                    if (overrides(c, m) || hooks.stream().anyMatch(h -> c.isAssignableTo(h))) {
                        continue;
                    }
                    us.addAll(m.getCallsOfSelf());
                    us.addAll(m.getReferencesToSelf());
                } else if (u instanceof JavaConstructor k) {
                    if (hooks.stream().anyMatch(h -> c.isAssignableTo(h))) {
                        continue;
                    }
                    us.addAll(k.getCallsOfSelf());
                    us.addAll(k.getReferencesToSelf());
                }
                if (!us.isEmpty()) {
                    uses.put(ServiceLoaderSitesTest.key(c, u), us);
                }
            }
            for (JavaField f : c.getFields()) {
                // a field's READS (a write is its maker's: a field the product writes and the edge alone reads is
                // carried for the edge)
                List<JavaAccess<?>> us = new ArrayList<>();
                f.getAccessesToSelf().stream().filter(x -> x.getAccessType() == JavaFieldAccess.AccessType.GET)
                        .forEach(us::add);
                if (!us.isEmpty() && !hooks.stream().anyMatch(h -> c.isAssignableTo(h))) {
                    uses.put(c.getName() + "#" + f.getName(), us);
                }
            }
        }
        // a plain non-public method nothing in the jars uses: dead, or a test's, so its uses are nobody's. A PUBLIC one is
        // an entry point -- a door a test, a host or a framework calls (measured 2026-11-18: read as dead, they added 240
        // members) -- and so are a constructor, a static initializer, a main, an annotated method (an export a native or
        // JavaScript host calls), an override and a hook's member
        Set<String> quiet = new TreeSet<>();
        for (JavaClass c : classes) {
            if (!c.getName().startsWith("com.legend.") || c.getName().startsWith(EDGE)
                    || hooks.stream().anyMatch(h -> c.isAssignableTo(h))) {
                continue;
            }
            for (JavaMethod m : c.getMethods()) {
                String key = ServiceLoaderSitesTest.key(c, m);
                if (!uses.containsKey(key) && !m.getModifiers().contains(JavaModifier.PUBLIC) && !overrides(c, m)
                        && m.getAnnotations().isEmpty()
                        && !m.getModifiers().contains(JavaModifier.SYNTHETIC) && !m.getName().startsWith("lambda$")
                        && !(m.getName().equals("main") && m.getRawParameterTypes().size() == 1)) {
                    quiet.add(key);
                }
            }
        }
        // the greatest fixpoint: a held unit's every use is the edge's, its own, a held unit's or a quiet one's
        Set<String> held = new TreeSet<>(uses.keySet());
        boolean dropped = true;
        while (dropped) {
            dropped = false;
            for (String key : new ArrayList<>(held)) {
                for (JavaAccess<?> use : uses.get(key)) {
                    if (!edgeOrHeld(use, key, held, quiet, byName)) {
                        held.remove(key);
                        dropped = true;
                        break;
                    }
                }
            }
        }
        // ... entered by an edge use: a cycle of product units no edge use reaches is nobody's
        Set<String> reached = new TreeSet<>();
        boolean grew = true;
        while (grew) {
            grew = false;
            for (String key : held) {
                if (!reached.contains(key) && uses.get(key).stream().anyMatch(use -> top(use.getOriginOwner().getName())
                        .startsWith(EDGE) || origins(use, byName).stream().anyMatch(reached::contains))) {
                    reached.add(key);
                    grew = true;
                }
            }
        }
        Set<String> out = new TreeSet<>();
        for (JavaClass c : classes) {
            if (!c.getName().startsWith("com.legend.") || c.getName().startsWith(EDGE) || !reachable(c)
                    || hooks.stream().anyMatch(h -> c.isAssignableTo(h))) {
                continue;
            }
            for (JavaMethod m : c.getMethods()) {
                boolean values = c.isEnum() && m.getName().equals("values") && m.getRawParameterTypes().isEmpty();
                if (open(m.getModifiers()) && !values && reached.contains(ServiceLoaderSitesTest.key(c, m))) {
                    out.add(ServiceLoaderSitesTest.key(c, m));
                }
            }
            for (JavaConstructor k : c.getConstructors()) {
                if (open(k.getModifiers()) && reached.contains(ServiceLoaderSitesTest.key(c, k))) {
                    out.add(ServiceLoaderSitesTest.key(c, k));
                }
            }
            for (JavaField f : c.getFields()) {
                if (!open(f.getModifiers())) {
                    continue;
                }
                String key = c.getName() + "#" + f.getName();
                if (!uses.containsKey(key) && constant(f)) {
                    // a compile-time constant is inlined where it is read: read from the sources (not transitive)
                    if (edgeAloneInSource(c, f.getName(), sources)) {
                        out.add(key);
                    }
                } else if (reached.contains(key)) {
                    out.add(key);
                }
            }
        }
        return out;
    }

    /** Whether {@code use} of the unit {@code key} comes from the edge, from the unit itself, or from units all held or
     *  quiet (nothing uses them). */
    private static boolean edgeOrHeld(JavaAccess<?> use, String key, Set<String> held, Set<String> quiet,
            Map<String, List<String>> byName) {
        if (top(use.getOriginOwner().getName()).startsWith(EDGE)) {
            return true;
        }
        List<String> from = origins(use, byName);
        return !from.isEmpty() && from.stream().allMatch(o -> o.equals(key) || held.contains(o) || quiet.contains(o));
    }

    /** The units a use comes from: its origin, or -- for a lambda body ({@code lambda$name$n}) -- every unit of that name
     *  in the origin's class ({@code new} a constructor, {@code static} the class's initializer). */
    private static List<String> origins(JavaAccess<?> use, Map<String, List<String>> byName) {
        String owner = use.getOriginOwner().getName();
        String name = use.getOrigin().getName();
        java.util.regex.Matcher lambda = Pattern.compile("lambda\\$(.+)\\$\\d+").matcher(name);
        if (lambda.matches()) {
            String enclosing = switch (lambda.group(1)) {
                case "new" -> "<init>";
                case "static" -> "<clinit>";
                default -> lambda.group(1);
            };
            return byName.getOrDefault(owner + "#" + enclosing, List.of());
        }
        return List.of(ServiceLoaderSitesTest.key(use.getOriginOwner(), use.getOrigin()));
    }

    /** Whether {@code c} and every class enclosing it are public: the edge, another package, can reach its members. */
    private static boolean reachable(JavaClass c) {
        for (JavaClass at = c; at != null; at = at.getEnclosingClass().orElse(null)) {
            if (!at.getModifiers().contains(JavaModifier.PUBLIC)) {
                return false;
            }
        }
        return true;
    }

    private static boolean open(Set<JavaModifier> modifiers) {
        return modifiers.contains(JavaModifier.PUBLIC) && !modifiers.contains(JavaModifier.SYNTHETIC)
                && !modifiers.contains(JavaModifier.BRIDGE);
    }

    /** Whether {@code m} overrides or implements a supertype's method (same name, same raw parameters). */
    private static boolean overrides(JavaClass c, JavaMethod m) {
        if (m.getModifiers().contains(JavaModifier.STATIC)) {
            return false;
        }
        List<JavaClass> supers = new ArrayList<>(c.getAllRawSuperclasses());
        supers.addAll(c.getAllRawInterfaces());
        for (JavaClass s : supers) {
            for (JavaMethod sm : s.getMethods()) {
                if (sm.getName().equals(m.getName()) && !sm.getModifiers().contains(JavaModifier.PRIVATE)
                        && !sm.getModifiers().contains(JavaModifier.STATIC)
                        && sm.getRawParameterTypes().stream().map(JavaClass::getName).toList()
                        .equals(m.getRawParameterTypes().stream().map(JavaClass::getName).toList())) {
                    return true;
                }
            }
        }
        // a class outside the import (java.lang.Object's toString, a JDK interface's method) answers too
        return OBJECT_METHODS.contains(m.getName() + m.getRawParameterTypes().stream().map(JavaClass::getName).toList());
    }

    private static final Set<String> OBJECT_METHODS = Set.of("toString[]", "hashCode[]", "equals[java.lang.Object]");

    /** Whether {@code f} may be a compile-time constant: static final, a primitive or a String. */
    private static boolean constant(JavaField f) {
        return f.getModifiers().contains(JavaModifier.STATIC) && f.getModifiers().contains(JavaModifier.FINAL)
                && (f.getRawType().isPrimitive() || f.getRawType().getName().equals("java.lang.String"));
    }

    /** Whether the sources read {@code owner}'s constant {@code name} in the edge alone: {@code Owner.NAME} or a static
     *  import of it outside the owner's file, the bare name inside it beyond its declaration. */
    private static boolean edgeAloneInSource(JavaClass owner, String name, Map<String, String> sources) {
        String ownPath = "/" + top(owner.getName()).replace('.', '/') + ".java";
        Pattern qualified = Pattern.compile("\\b" + Pattern.quote(owner.getSimpleName()) + "\\s*\\.\\s*"
                + Pattern.quote(name) + "\\b");
        Pattern imported = Pattern.compile("import\\s+static\\s+[\\w.$]*\\." + Pattern.quote(owner.getSimpleName())
                + "\\s*\\.\\s*" + Pattern.quote(name) + "\\s*;");
        Pattern bare = Pattern.compile("\\b" + Pattern.quote(name) + "\\b");
        boolean edge = false;
        for (Map.Entry<String, String> e : sources.entrySet()) {
            String text = e.getValue();
            boolean read;
            if (e.getKey().endsWith(ownPath)) {
                read = bare.matcher(text).results().count() > 1;
            } else {
                read = qualified.matcher(text).find() || imported.matcher(text).find();
            }
            if (!read) {
                continue;
            }
            if (e.getKey().contains("/com/legend/edge/")) {
                edge = true;
            } else {
                return false;
            }
        }
        return edge;
    }

    /** The guard rails' declared sources under core's main tree, comments and strings left in (a name in a comment
     *  reads as a use: it fails toward counting nothing). */
    private static Map<String, String> sources() throws IOException {
        Map<String, String> out = new TreeMap<>();
        for (Path p : SourceFiles.under("core/src/main/java/com/legend")) {
            if (p.toString().endsWith(".java")) {
                out.put(SourceFiles.path(p), Files.readString(p));
            }
        }
        return out;
    }

    private static String top(String n) {
        int dollar = n.indexOf('$');
        return dollar < 0 ? n : n.substring(0, dollar);
    }

    private static List<java.util.jar.JarFile> jarFiles(List<Path> jars) throws IOException {
        List<java.util.jar.JarFile> out = new ArrayList<>();
        for (Path jar : jars) {
            out.add(new java.util.jar.JarFile(jar.toFile()));
        }
        return out;
    }
}
