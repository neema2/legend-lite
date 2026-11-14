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
 * model ({@code ModelFromText.element}), a model's records read ({@code ModelFromProtocol.of}, both), a model's index
 * ({@code ModelBuilder.from}), a model's mappings normalized ({@code MappingNormalizer.normalize}, both). What does
 * NOT ask, and why: the lexer ({@code Lexer.tokenize}: JDK-only, invariant 7a) and the parser ({@code ElementParser},
 * {@code SpecParser}, {@code PmcdParser}: tokens in, protocol records out, invariant 7c) depend on nothing above them,
 * {@code RunScope} included. They make data, never a model or a plan: a parse a run made reaches the planner only
 * through an entry above it -- phase B's reads, D's names, E's normalization, F's typed model -- each of which asks.
 * A spec's parse (phase C) is name-resolved in phase D, which asks.
 *
 * <p>A correction to 02e111ef0's message, which called {@code ModelFromText.parse} "the text door every model's parse
 * goes through": it is the door the compiler's model texts go through ({@code Compiler.parseModel}, {@code compileModel},
 * the platform's built-ins), but a quoted spec's model text ({@code QuotedSpecParser}: the parser's own
 * {@code PmcdParser.parse}, phase C, under invariant 7c) does not pass it, and a model given as records
 * ({@code ModelFromProtocol.of}) never did -- the latter now asks itself (J5).
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
        com.legend.protocol.Protocol.Element element = com.legend.parser.ElementParser.parseSingle(
                com.legend.lexer.Lexer.tokenize("Class m::B { y: Integer[1]; }"), com.legend.parser.Dialect.LEGEND_LITE);
        out.put("read a parsed element into the model (phase B)", () -> com.legend.door.ModelFromText.element(element));
        out.put("read a model's records (phase B)", () -> com.legend.model.ModelFromProtocol.of(records));
        out.put("read a model's records (phase B) <- the source's overload", () -> com.legend.model.ModelFromProtocol.of(
                records, MODEL, List.of()));
        out.put("index a model's elements (phases E and F)", () -> ModelBuilder.from(resolved));
        ModelBuilder forMappings = ModelBuilder.from(resolved);
        ModelBuilder forTolerant = ModelBuilder.from(resolved);
        out.put("normalize a model's mappings (phase E)", () -> com.legend.normalizer.MappingNormalizer.normalize(
                resolved, forMappings));
        out.put("normalize a model's mappings (phase E) <- the tolerant one", () -> com.legend.normalizer
                .MappingNormalizer.normalize(resolved, forTolerant, new java.util.HashMap<>()));
        // the driver's own doors, which reach the phases: the first phase each reaches refuses it
        out.put("parse a model (phases A and B) <- Compiler.compileModel", () -> Compiler.compileModel(MODEL));
        out.put("resolve a model's names (phase D) <- Compiler.query", () -> Compiler.query(ctx, QUERY));
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
