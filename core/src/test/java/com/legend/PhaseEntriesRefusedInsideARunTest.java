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
 * entry now asks first: inside a run each refuses by its phase's name, and outside one each answers. A spec's parse
 * (phase C) is the parser's, which depends on nothing above it (ArchitectureTest's invariant 7c): a spec the compiler
 * takes is name-resolved in phase D, which asks.
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
        out.put("normalize a model (phase E)", () -> ModelNormalizer.normalize(resolved, ModelBuilder.from(resolved),
                null));
        out.put("build a typed model (phase F)", () -> PureModelContext.from(normalized, index,
                com.legend.lowering.PlatformRegistrations.current()));
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
