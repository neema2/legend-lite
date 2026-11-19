// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.legend.base.RunScope;
import com.legend.compiler.ModelBuilder;
import com.legend.compiler.NameResolver;
import com.legend.compiler.element.PureModelContext;
import com.legend.model.NormalizedModel;
import com.legend.model.ParsedModel;
import com.legend.normalizer.ModelNormalizer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/**
 * EVERY PHASE'S ENTRY IS REFUSED INSIDE A RUN (the next PR's item 2, "separation made foolproof": the standing witness,
 * 2026-11-13). Java's RunScope refused planning in phases G to J; A to F -- the model's parse, its names, its
 * normalization and its typed model -- asked nothing, so a run could compile a model, or a query's names, unseen. Each
 * entry now asks first: inside a run each refuses by its phase's name, and outside one each answers.
 *
 * <p>The entries the final audit of 9af16fbe58 found still open (J5, 2026-11-14) ask too: an element read into the
 * model ({@code ModelFromText.element}), a model's records read ({@code Compiler.compileModel(records)}), a model's index
 * ({@code ModelBuilder.from}), a model's mappings normalized ({@code MappingNormalizer.normalize}, both).
 *
 * <p>Since C5-J item 1 (2026-11-15) the lexer and the parser ask too: until then they were let off, since the lexer
 * ({@code Lexer.tokenize}: JDK-only, invariant 7a) and the parser ({@code ElementParser}, {@code SpecParser},
 * {@code PmcdParser}: tokens in, protocol records out, invariant 7c) depend on nothing above them, {@code RunScope}
 * included, and the model's own door ({@code ModelFromProtocol}) is pure data (6j). They still cannot ask; their
 * callers cannot reach them but through the parse doors -- {@code door.Parse}, {@code door.ModelFromText} and the IDE's
 * {@code ide.DocumentParse} -- each of
 * which asks first ({@code ArchitectureTest.everyParseIsReachedThroughADoorThatAsks}). So a run lexes nothing, parses no
 * model, no element, no import, no query and no lambda, and reads no model's records, each refused by its phase's name:
 * the doors below, and the product's callers of them (the compiler's query parse, the IDE's document).
 *
 * <p>Since C5-J's last round (A1, 2026-11-15) the embedded languages' readers ask too, through the same door: an
 * island's SQL by its dialect ({@code #SQL}'s Postgres, {@code #IQ}'s SAP IQ), a {@code #TS} query or item, a
 * {@code #Cypher} statement -- each reader in the protocol layer, which cannot ask (invariant 7b), its translator taking
 * the tree; the compiler's island seams and the server's {@code #TS} routes read the text through {@code door.Parse}.
 *
 * <p>A correction to 02e111ef0's message, which called {@code ModelFromText.parse} "the text door every model's parse
 * goes through": it is the door the compiler's model texts go through ({@code Compiler.parseModel}, {@code compileModel},
 * the platform's built-ins), but a quoted spec's model text ({@code QuotedSpecParser}: the parser's own
 * {@code PmcdParser.parse}, phase C, under invariant 7c) does not pass it, and a model given as records
 * ({@code ModelFromProtocol.of}) never did -- its door, {@code Compiler.compileModel(records)}, now asks (J5).
 */
class PhaseEntriesRefusedInsideARunTest {

    private static final String MODEL = """
            ###Pure
            Class m::A { x: Integer[1]; }
            ###Relational
            Database m::DB ( Table T ( ID INTEGER PRIMARY KEY ) )
            ###Connection
            RelationalDatabaseConnection m::Conn { store: m::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime m::RT { mappings: [ ]; connections: [ m::DB: [ c1: m::Conn ] ]; }
            """;

    private static final String QUERY = "|#>{m::DB.T}#->select(~[ID])";

    /** Each phase's entry, by the name its refusal gives, as a call over inputs made outside any run. */
    private static Map<String, Supplier<Object>> entries() {
        ParsedModel parsed = Compiler.parseModel(MODEL);
        ParsedModel resolved = NameResolver.resolve(parsed);
        ModelBuilder index = ModelBuilder.from(resolved);
        NormalizedModel normalized = ModelNormalizer.normalize(resolved, ModelBuilder.from(resolved), null);
        var ctx = Compiler.compileModel(MODEL);
        TypedQuery query = Compiler.query(ctx, QUERY);
        Map<String, Supplier<Object>> out = new LinkedHashMap<>();
        out.put("parse a model (phases A and B) <- Compiler.parseModel", () -> Compiler.parseModel(MODEL));
        out.put("resolve a model's names (phase D)", () -> NameResolver.resolve(parsed));
        // each normalization's index built outside the run (an index is phase E's and F's own entry, and asks: J5)
        ModelBuilder forNormalize = ModelBuilder.from(resolved);
        out.put("normalize a model (phase E)", () -> ModelNormalizer.normalize(resolved, forNormalize, null));
        out.put("build a typed model (phase F)", () -> PureModelContext.from(normalized, index,
                com.legend.lowering.PlatformRegistrations.current()));
        // J5 (2026-11-14): the entries that asked nothing until the final audit of 9af16fbe58
        com.legend.protocol.Protocol.PureModelContextData records = com.legend.parser.PmcdParser.parseModel(MODEL);
        com.legend.protocol.Protocol.Element element = records.elements().get(0);
        out.put("read a parsed element into the model (phase B)", () -> com.legend.door.ModelFromText.element(element));
        out.put("read a model's records (phase B) <- Compiler.compileModel(records)", () -> Compiler.compileModel(records));
        out.put("index a model's elements (phases E and F)", () -> ModelBuilder.from(resolved));
        ModelBuilder forMappings = ModelBuilder.from(resolved);
        ModelBuilder forTolerant = ModelBuilder.from(resolved);
        out.put("normalize a model's mappings (phase E)", () -> com.legend.normalizer.MappingNormalizer.normalize(
                resolved, forMappings));
        out.put("normalize a model's mappings (phase E) <- the tolerant one", () -> com.legend.normalizer
                .MappingNormalizer.normalize(resolved, forTolerant, new java.util.HashMap<>()));
        // the driver's own doors, which reach the phases: the first phase each reaches refuses it
        out.put("parse a model (phases A and B) <- Compiler.compileModel", () -> Compiler.compileModel(MODEL));
        // C5-J item 1 (2026-11-15): a query's text is parsed (phase C) before its names are resolved (D), and the
        // parse asks now -- until then Compiler.query's refusal was phase D's
        out.put("parse a query (phase C) <- Compiler.query", () -> Compiler.query(ctx, QUERY));
        out.put("parse a query (phase C) <- Compiler.parseQuery", () -> Compiler.parseQuery(QUERY));
        // C5-J item 1 (2026-11-15): the parse doors, each the one way past the lexer, the parser and the model's
        // records door
        com.legend.lexer.TokenStream tokens = com.legend.lexer.Lexer.tokenize("Class m::B { y: String[1]; }");
        com.legend.lexer.TokenStream imported = com.legend.lexer.Lexer.tokenize("import m::*;");
        out.put("lex a model's text (phase A)", () -> com.legend.ide.DocumentParse.tokens(MODEL));
        out.put("lex a model's text (phase A) <- the IDE's document", () -> new com.legend.ide.ModelOrchestrator(MODEL));
        out.put("parse a model's element (phase B)", () -> com.legend.ide.DocumentParse.element(tokens,
                com.legend.parser.GrammarLevel.LEGEND_LITE));
        out.put("parse a model's import (phase B)", () -> com.legend.ide.DocumentParse.importOf(imported,
                com.legend.parser.GrammarLevel.LEGEND_LITE));
        // the level-taking entries (20:10-R7, S1m, 2026-11-18: until then engineRecords and engineDocumentJson)
        out.put("parse a model (phases A and B) <- Parse.records", () -> com.legend.door.Parse.records(MODEL,
                com.legend.parser.GrammarLevel.LEGEND_LITE));
        out.put("parse a model (phases A and B) <- Parse.documentJson at a level",
                () -> com.legend.door.Parse.documentJson(MODEL, true, com.legend.parser.GrammarLevel.LEGEND_LITE));
        out.put("parse a model (phases A and B) <- Parse.documentJson, its source information named",
                () -> com.legend.door.Parse.documentJson(MODEL, true));
        out.put("parse a query (phase C) <- Parse.query", () -> com.legend.door.Parse.query(QUERY,
                com.legend.parser.GrammarLevel.LEGEND_LITE));
        out.put("parse a lambda (phase C)", () -> com.legend.door.Parse.lambdaJson(QUERY, false));
        out.put("parse a lambda (phase C) <- Parse.lambdaJson at a level", () -> com.legend.door.Parse.lambdaJson(QUERY,
                true, com.legend.parser.GrammarLevel.LEGEND_LITE));
        // lite's API v1 (2026-11-17): query/parse reads lite's own grammar, its islands included, through the
        // compiler's product entry (the level the compiler's, ParserBoundaryArchTest)
        out.put("parse a lambda (phase C) <- Compiler.parseQueryJson", () -> Compiler.parseQueryJson(QUERY));
        out.put("read a model's records (phase B) <- Parse.model", () -> com.legend.door.Parse.model(records));
        // C5-J's last round (A1, 2026-11-15): the embedded languages' readers, each through its door -- an island's
        // SQL by each dialect (Postgres's for #SQL, SAP IQ's for #IQ), a #TS query and item, a #Cypher statement
        out.put("parse a SQL island (phase C) <- Parse.sqlIsland, #SQL's reader", () -> com.legend.door.Parse
                .sqlIsland("SELECT 1 AS a", java.util.Objects.requireNonNull(com.legend.protocol.sql.InputLanguages.of(
                        com.legend.protocol.spec.IslandLanguage.SQL, null))));
        out.put("parse a SQL island (phase C) <- Parse.sqlIsland, #IQ's reader", () -> com.legend.door.Parse
                .sqlIsland("SELECT 1 AS a", java.util.Objects.requireNonNull(com.legend.protocol.sql.InputLanguages.of(
                        com.legend.protocol.spec.IslandLanguage.IQ, null))));
        out.put("parse a timeseries island (phase C)", () -> com.legend.door.Parse.tsIsland(
                "SELECT avg(CLOSE, 3) AS m FROM $px ON DAY BY SYMBOL"));
        out.put("parse a timeseries item (phase C)", () -> com.legend.door.Parse.tsItem("avg(CLOSE, 3) AS m"));
        out.put("parse a Cypher island (phase C)", () -> com.legend.door.Parse.cypherIsland(
                "MATCH (p:Person) RETURN p.name AS name"));
        // and a product caller of them, the server's #TS item route (the compiler's island seams read their text while
        // typing, phase G, whose own question is asked first: "type a spec")
        // (lite's API v1, 2026-11-17: the route's own Java is tsItemOf, its request read by the one road)
        out.put("parse a timeseries item (phase C) <- the server's tsItem", () -> com.legend.server.LiteV1Api.tsItemOf(
                "avg(CLOSE, 3) AS m"));
        // a test body's facts (the C5-J audit's nit, 2026-11-15): planning that asks on its own, never by the typing it
        // starts -- the body typed outside the run, so only the facts planner's own question can refuse it
        TypedQuery body = Compiler.query(ctx, QUERY);
        body.body();
        out.put("state a test body's facts <- TestExecution.facts", () -> TestExecution.facts(body));
        out.put("TypedQuery.executionPlan", () -> query.executionPlan("m::RT"));
        return out;
    }

    @Test
    void eachPhaseEntryIsRefusedInsideARunByItsPhaseAndAnswersOutsideOne() {
        Map<String, Supplier<Object>> entries = entries();
        List<String> wrong = new ArrayList<>();
        try (var run = RunScope.enter()) {
            entries.forEach((name, entry) -> {
                String phase = name.contains(" <- ") ? name.substring(0, name.indexOf(" <- ")) : name;
                try {
                    Object answered = entry.get();
                    wrong.add(name + ": answered inside a run (" + answered.getClass().getSimpleName() + ")");
                } catch (RunScope.Refused refused) {
                    if (!name.startsWith("TypedQuery.") && !String.valueOf(refused.getMessage()).contains(
                            "the runtime wall: " + phase + " while a plan runs")) {
                        wrong.add(name + ": refused, but not by its phase: " + refused.getMessage());
                    }
                }
            });
        }
        assertEquals(List.of(), wrong);
        // outside any run, each answers
        entries.forEach((name, entry) -> assertNotNull(entry.get(), name));
    }
}
