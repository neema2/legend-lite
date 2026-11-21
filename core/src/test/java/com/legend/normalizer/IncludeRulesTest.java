// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.Compiler;
import com.legend.compiler.ModelBuilder;
import com.legend.compiler.NameResolver;
import com.legend.error.ModelException;
import com.legend.model.ParsedModel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Clean-sheet homework B2 — the engine's include rules, verbatim
 * (legend-pure functions_Mapping.pure, engine MappingValidator):
 * R1 a class's root set is the LAST root over includes-then-own; R5 a set
 * id taken by two distinct sets across a closure, or an include listed
 * twice, is a compile error. The corpus never reaches these (probe
 * 2026-09-13: 0 / 0 / 0), so these witnesses are the judges.
 */
class IncludeRulesTest {

    private static final String CLASSES = """
            Class w::Person { name: String[1]; }
            ###Relational
            Database w::DB (
              Table PERSON_A (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))
              Table PERSON_B (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))
              Table PERSON_C (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))
            )
            """;

    @Test
    @DisplayName("R1: a class mapped in two included mappings resolves to the LATER include's root, not a wall")
    void laterIncludeWins() {
        String model = CLASSES + """
                ###Mapping
                Mapping w::A ( *w::Person[a] : Relational { ~mainTable [w::DB] PERSON_A name: PERSON_A.NAME } )
                Mapping w::B ( *w::Person[b] : Relational { ~mainTable [w::DB] PERSON_B name: PERSON_B.NAME } )
                Mapping w::AB ( include w::A include w::B )
                Mapping w::BA ( include w::B include w::A )
                ###Runtime
                Runtime w::RtAB { mappings: [w::AB]; connections: [ w::DB: [ c: #{ RelationalDatabaseConnection { type: DuckDB; specification: LocalH2 {}; auth: DefaultH2; } }# ] ]; }
                Runtime w::RtBA { mappings: [w::BA]; connections: [ w::DB: [ c: #{ RelationalDatabaseConnection { type: DuckDB; specification: LocalH2 {}; auth: DefaultH2; } }# ] ]; }
                """;
        String ab = Compiler.query(Compiler.compileModel(model), "w::Person.all()->project([p | $p.name], ['name'])").plan("w::RtAB").sql();
        String ba = Compiler.query(Compiler.compileModel(model), "w::Person.all()->project([p | $p.name], ['name'])").plan("w::RtBA").sql();
        assertTrue(ab.contains("PERSON_B") && !ab.contains("PERSON_A"), ab);
        assertTrue(ba.contains("PERSON_A") && !ba.contains("PERSON_B"), ba);
    }

    @Test
    @DisplayName("R1: the mapping's own root beats every include; operation sets follow the same last-wins order")
    void ownBeatsIncludesAndOpsAreLastWins() {
        String model = CLASSES + """
                ###Mapping
                Mapping w::A ( *w::Person[a] : Relational { ~mainTable [w::DB] PERSON_A name: PERSON_A.NAME } )
                Mapping w::B ( *w::Person[b] : Relational { ~mainTable [w::DB] PERSON_B name: PERSON_B.NAME } )
                Mapping w::Own ( include w::A include w::B
                  *w::Person[own] : Relational { ~mainTable [w::DB] PERSON_C name: PERSON_C.NAME } )
                ###Runtime
                Runtime w::Rt { mappings: [w::Own]; connections: [ w::DB: [ c: #{ RelationalDatabaseConnection { type: DuckDB; specification: LocalH2 {}; auth: DefaultH2; } }# ] ]; }
                """;
        String sql = Compiler.query(Compiler.compileModel(model), "w::Person.all()->project([p | $p.name], ['name'])").plan("w::Rt").sql();
        assertTrue(sql.contains("PERSON_C"), sql);
        // operation sets: two includes each declare a union for Person — the later include's wins
        String ops = CLASSES + """
                ###Mapping
                Mapping w::U1 ( *w::Person[u1] : Operation { meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_(u1a) }
                  w::Person[u1a] : Relational { ~mainTable [w::DB] PERSON_A name: PERSON_A.NAME } )
                Mapping w::U2 ( *w::Person[u2] : Operation { meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_(u2b) }
                  w::Person[u2b] : Relational { ~mainTable [w::DB] PERSON_B name: PERSON_B.NAME } )
                Mapping w::Top ( include w::U1 include w::U2 )
                """;
        ParsedModel resolved = NameResolver.resolve(com.legend.testing.Own.model(ops));
        ModelBuilder index = ModelBuilder.from(resolved);
        Map<String, ResolvedMapping> rm = MappingPrePass.run(resolved, index, null,
                new LiftedViews(resolved, index));
        assertEquals(List.of("u2b"), rm.get("w::Top").unionOf("w::Person").memberSetIds());
    }

    @Test
    @DisplayName("R5: a set id taken by two distinct sets across the closure is a compile error; so is a duplicate include")
    void duplicateIdsAndIncludesAreRejected() {
        String dupId = CLASSES + """
                ###Mapping
                Mapping w::A ( *w::Person[p] : Relational { ~mainTable [w::DB] PERSON_A name: PERSON_A.NAME } )
                Mapping w::B ( *w::Person[p] : Relational { ~mainTable [w::DB] PERSON_B name: PERSON_B.NAME } )
                Mapping w::AB ( include w::A include w::B )
                """;
        ModelException e = assertThrows(ModelException.class, () -> Compiler.compileModel(dupId));
        // the product's kind and lite's sentence (J10, 2026-11-17); legend-engine's words are the edge's
        assertEquals(com.legend.error.CompileKind.DUPLICATE_CLASS_MAPPING_IDS, e.kind(), e.getMessage());
        assertTrue(e.getMessage().contains("Duplicated class mappings found with ID [p (w::A, w::B)] in mapping 'w::AB'"),
                e.getMessage());
        Compiler.BuiltModule module = Compiler.buildModule(Compiler.parseSources(List.of(
                new Compiler.ModelSource("m.pure", dupId))).model());
        assertTrue(module.walls().containsKey("w::AB"), module.walls().toString());
        assertFalse(module.walls().containsKey("w::A"), "the included mappings themselves are fine");

        String dupInclude = CLASSES + """
                ###Mapping
                Mapping w::A ( *w::Person[a] : Relational { ~mainTable [w::DB] PERSON_A name: PERSON_A.NAME } )
                Mapping w::AA ( include w::A include w::A )
                """;
        ModelException e2 = assertThrows(ModelException.class, () -> Compiler.compileModel(dupInclude));
        assertEquals(com.legend.error.CompileKind.DUPLICATE_MAPPING_INCLUDE, e2.kind(), e2.getMessage());
        assertTrue(e2.getMessage().contains("Duplicated mapping include 'w::A' in mapping 'w::AA'"), e2.getMessage());
    }

    @Test
    @DisplayName("a mapping defined twice is a duplicate element, never an include cycle")
    void duplicateMappingIsADuplicateElement() {
        // 2026-10-11: the include ordering's cycle check counted the parsed mappings (a duplicate twice)
        // against the ordered names (once) and named an empty cycle: an index error, not a refusal
        String dupMapping = CLASSES + """
                ###Mapping
                Mapping w::A ( *w::Person[a] : Relational { ~mainTable [w::DB] PERSON_A name: PERSON_A.NAME } )
                Mapping w::M ( include mapping w::A )
                Mapping w::M ( include mapping w::A )
                """;
        ModelException e = assertThrows(ModelException.class, () -> Compiler.compileModel(dupMapping));
        assertTrue(e.getMessage().contains("Duplicated element 'w::M'"), e.getMessage());
        // the module parse drops the second definition itself and reports it; the survivor compiles
        Compiler.ParsedModule parsed = Compiler.parseSources(List.of(new Compiler.ModelSource("m.pure", dupMapping)));
        assertTrue(parsed.duplicateElements().toString().contains("w::M"), parsed.duplicateElements().toString());
        Map<String, String> survivorWalls = Compiler.buildModule(parsed.model()).walls();
        assertTrue(survivorWalls.isEmpty(), survivorWalls.toString());
        // the second copy includes itself: refused by name before the include ordering reads it (the engine's
        // order), not as a cycle
        String selfInclude = CLASSES + """
                ###Mapping
                Mapping w::A ( *w::Person[a] : Relational { ~mainTable [w::DB] PERSON_A name: PERSON_A.NAME } )
                Mapping w::M ( include mapping w::A )
                Mapping w::M ( include mapping w::M )
                """;
        ModelException e2 = assertThrows(ModelException.class, () -> Compiler.compileModel(selfInclude));
        assertTrue(e2.getMessage().contains("Duplicated element 'w::M'"), e2.getMessage());
        // across kinds: a class and a mapping of one name; the module parse keeps both (it keys by kind), the
        // build walls the name
        String crossKind = CLASSES + """
                ###Pure
                Class w::M { id: Integer[1]; }
                ###Mapping
                Mapping w::A ( *w::Person[a] : Relational { ~mainTable [w::DB] PERSON_A name: PERSON_A.NAME } )
                Mapping w::M ( include mapping w::A )
                """;
        ModelException e3 = assertThrows(ModelException.class, () -> Compiler.compileModel(crossKind));
        assertTrue(e3.getMessage().contains("Duplicated element 'w::M'"), e3.getMessage());
        Compiler.ParsedModule both = Compiler.parseSources(List.of(new Compiler.ModelSource("m.pure", crossKind)));
        assertTrue(both.duplicateElements().isEmpty(), both.duplicateElements().toString());
        Map<String, String> walls = Compiler.buildModule(both.model()).walls();
        assertEquals("Duplicated element 'w::M'", walls.get("w::M"), walls.toString());
    }
}
