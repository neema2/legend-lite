// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.PureModelContext;
import com.legend.model.ClassDefinition;
import com.legend.model.MappingDefinition;
import com.legend.model.NormalizedModel;
import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;
import com.legend.normalizer.ModelNormalizer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T4.1 step 2 witness: ONE index per graph. Built before Phase E, read by
 * it, unchanged by it (invariant 1: F1's answers are identical before and
 * after E), then grown at the E&rarr;F gate by exactly Phase E's products
 * (invariant: nothing re-indexed, pass-through elements skipped by
 * identity, ingest order kept). What Phase E learns rides the compiled
 * mapping and reaches the context off that artifact.
 */
class OneIndexTest {

    private static final String MODEL = """
            Class w::Person { name: String[1]; firm: w::Firm[0..1]; greeting() { $this.name }: String[1]; }
            Class w::Firm { legalName: String[1]; }
            Class w::Employee extends w::Person { grade: Integer[1]; }
            Association w::Membership { members: w::Person[*]; club: w::Club[0..1]; }
            Class w::Club { title: String[1]; }
            ###Relational
            Database w::DB (
              Table PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), FIRM_ID INTEGER)
              Table FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100))
              Join PersonFirm (PERSON.FIRM_ID = FIRM.ID)
            )
            ###Mapping
            Mapping w::M (
              *w::Person : Relational { ~mainTable [w::DB] PERSON
                name: PERSON.NAME, firm: [w::DB] @PersonFirm }
              *w::Firm : Relational { ~mainTable [w::DB] FIRM legalName: FIRM.LEGAL_NAME }
            )
            ###Runtime
            Runtime w::RT { mappings: [w::M]; }
            """;

    /** Everything F1 knows, by identity and in order. */
    private static List<Object> knowledge(ModelBuilder index) {
        List<Object> out = new ArrayList<>();
        index.classes().forEach(out::add);
        index.associations().forEach(out::add);
        index.enums().forEach(out::add);
        index.measures().forEach(out::add);
        index.databases().forEach(out::add);
        index.legacyMappings().forEach(out::add);
        index.runtimes().forEach(out::add);
        index.functions().forEach(out::add);
        index.classes().forEach(cd -> out.add(index.directSubclasses(cd.qualifiedName())));
        out.add(index.findAssociationEnd("w::Person", "club").orElseThrow());
        out.add(index.findAssociationEnd("w::Club", "members").orElseThrow());
        return out;
    }

    private static List<Object> identities(List<Object> xs) {
        List<Object> out = new ArrayList<>();
        for (Object x : xs) {
            out.add(x instanceof List<?> l ? l : System.identityHashCode(x));
        }
        return out;
    }

    @Test
    @DisplayName("invariant 1: F1's answers are identical before and after Phase E; E writes nothing")
    void knowledgeBeforeEqualsKnowledgeAfter() {
        ParsedModel resolved = NameResolver.resolve(com.legend.testing.Own.model(MODEL));
        ModelBuilder index = ModelBuilder.from(resolved);
        List<Object> before = identities(knowledge(index));
        long functionsBefore = index.functions().count();
        assertEquals(0, index.mappings().count(), "no compiled mapping before E");

        NormalizedModel normalized = ModelNormalizer.normalize(resolved, index, null);

        assertEquals(before, identities(knowledge(index)), "Phase E changed the index");
        assertEquals(functionsBefore, index.functions().count(), "Phase E added functions to the index");
        assertEquals(0, index.mappings().count(), "Phase E indexed a compiled mapping");
        // and its products exist only on the normalized model
        assertTrue(normalized.elements().stream().anyMatch(e -> e instanceof MappingDefinition));
        assertTrue(normalized.elements().stream().anyMatch(e ->
                e.qualifiedName().equals(SynthFqn.prop("w::Person", "greeting"))));
    }

    @Test
    @DisplayName("the gate adds exactly Phase E's products: pass-through elements skipped by identity, order kept")
    void gateAddsOnlyTheProducts() {
        ParsedModel resolved = NameResolver.resolve(com.legend.testing.Own.model(MODEL));
        ModelBuilder index = ModelBuilder.from(resolved);
        ClassDefinition personBefore = index.findClass("w::Person").orElseThrow();
        List<String> classOrderBefore = index.classes().map(PackageableElement::qualifiedName).toList();
        long functionsBefore = index.functions().count();
        NormalizedModel normalized = ModelNormalizer.normalize(resolved, index, null);

        PureModelContext ctx = PureModelContext.from(normalized, index, com.legend.lowering.PlatformRegistrations.current());

        // the same class object, at the same position — not re-registered
        assertSame(personBefore, index.findClass("w::Person").orElseThrow());
        assertEquals(classOrderBefore, index.classes().map(PackageableElement::qualifiedName).toList());
        assertTrue(index.duplicateElements().isEmpty(), index.duplicateElements().toString());
        // the products are in: the compiled mapping, the lifted functions
        assertEquals(1, index.mappings().count());
        long lifted = normalized.elements().stream()
                .filter(e -> e instanceof com.legend.model.FunctionDefinition fd && fd.isSynthesized())
                .count();
        assertEquals(functionsBefore + lifted, index.functions().count());
        assertEquals(1, ctx.findFunction(SynthFqn.prop("w::Person", "greeting")).size());
        // a second add of the same list is a no-op
        index.add(normalized.elements());
        assertEquals(functionsBefore + lifted, index.functions().count());
        assertEquals(1, index.mappings().count());
    }

    @Test
    @DisplayName("Phase E's facts ride the compiled mapping and reach the context off that artifact")
    void factsAreStampedOnTheMapping() {
        // Person.firm targets Firm through a join; the census pairs [1]
        // properties with their columns — a stamped fact, read back
        // through the context, never through a side channel
        ModelContext ctx = Compiler.compileModel(MODEL);
        MappingDefinition md = ctx.findMapping("w::M").orElseThrow();
        assertTrue(md.facts().poisons().isEmpty(), md.facts().poisons().toString());
        assertTrue(ctx.mappingPoison("w::M", "w::Person").isEmpty());
        // an unmapped class-typed target is a USER-model error: a strict
        // build rejects it (§6's line, step 6); a MODULE build stamps a
        // per-class POISON under the class key, raised at use
        ModelContext poisoned = Compiler.buildModule(Compiler.parseSources(List.of(
                new Compiler.ModelSource("m.pure", """
                Class w::Person { name: String[1]; firm: w::Firm[0..1]; }
                Class w::Firm { legalName: String[1]; }
                ###Relational
                Database w::DB (
                  Table PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), FIRM_ID INTEGER)
                  Table FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100))
                  Join PersonFirm (PERSON.FIRM_ID = FIRM.ID)
                )
                ###Mapping
                Mapping w::M2 (
                  *w::Person : Pure { ~src w::Person name: $src.name, firm: $src.firm }
                )
                """))).model()).context();
        String reason = poisoned.findMapping("w::M2").orElseThrow().facts().poisons()
                .get(new com.legend.model.PoisonKey.ForClass("w::Person"));
        assertTrue(reason != null && reason.contains("w::Firm"), String.valueOf(reason));
        assertEquals(reason, poisoned.mappingPoison("w::M2", "w::Person").orElseThrow());
    }

    @Test
    @DisplayName("P1-1: a non-root set's recorded reason is readable by its own key, and reaches the user")
    void perSetPoisonIsReadableByItsKey() {
        // audit 2026-09-15 P1-1: the per-set poison was written under a
        // "class[setId]" string no reader composed. Person has two non-root
        // sets; set b names a property Person does not declare, so the
        // validation walls THAT set and the reason must be addressable by
        // (class, set) — and the class key carries only the multi-set text,
        // never a masked cause.
        ModelContext ctx = Compiler.buildModule(Compiler.parseSources(List.of(
                new Compiler.ModelSource("m.pure", """
                Class w::Person { name: String[1]; firm: w::Firm[0..1]; }
                Class w::Firm { legalName: String[1]; }
                ###Relational
                Database w::DB (
                  Table PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), FIRM_ID INTEGER)
                  Table FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100))
                  Join PersonFirm (PERSON.FIRM_ID = FIRM.ID)
                )
                ###Mapping
                Mapping w::M3 (
                  w::Person[a] : Relational { ~mainTable [w::DB] PERSON name: PERSON.NAME }
                  w::Person[b] : Relational { ~mainTable [w::DB] PERSON name: PERSON.NAME, nope: PERSON.NAME }
                )
                """))).model()).context();
        MappingDefinition md = ctx.findMapping("w::M3").orElseThrow();
        String setReason = md.facts().poisons()
                .get(new com.legend.model.PoisonKey.ForSet("w::Person", "b"));
        assertTrue(setReason != null && setReason.contains("'nope'"), String.valueOf(setReason));
        assertEquals(setReason, ctx.mappingSetPoison("w::M3", "w::Person", "b").orElseThrow(),
                "the context reads the per-set reason by the same key the writer used");
        assertTrue(ctx.mappingSetPoison("w::M3", "w::Person", "a").isEmpty(),
                "set a synthesized: no reason recorded");
        String classReason = ctx.mappingPoison("w::M3", "w::Person").orElseThrow();
        assertTrue(classReason.contains("multiple set IDs") && !classReason.contains("'nope'"),
                "the class key carries the multi-set text; set b's cause lives under its own key: "
                        + classReason);
    }

    @Test
    @DisplayName("step 4b: union members, routed target classes and routed sets are read off the compiled mapping")
    void surfaceFactsAreStamped() {
        ModelContext ctx = Compiler.compileModel("""
                Class w::Vehicle { plate: String[1]; }
                Class w::Car extends w::Vehicle { doors: Integer[1]; }
                Class w::Bike extends w::Vehicle { gears: Integer[1]; }
                Class w::Owner { name: String[1]; car: w::Car[0..1]; }
                ###Relational
                Database w::DB (
                  Table CAR (ID INTEGER PRIMARY KEY, PLATE VARCHAR(20), DOORS INTEGER)
                  Table BIKE (ID INTEGER PRIMARY KEY, PLATE VARCHAR(20), GEARS INTEGER)
                  Table OWNER (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), CAR_ID INTEGER)
                  Join OwnerCar (OWNER.CAR_ID = CAR.ID)
                )
                ###Mapping
                Mapping w::M (
                  *w::Vehicle : Operation { meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_(car, bike) }
                  w::Car[car] : Relational { ~mainTable [w::DB] CAR plate: CAR.PLATE, doors: CAR.DOORS }
                  w::Bike[bike] : Relational { ~mainTable [w::DB] BIKE plate: BIKE.PLATE, gears: BIKE.GEARS }
                  *w::Owner : Relational { ~mainTable [w::DB] OWNER name: OWNER.NAME, car[car]: [w::DB] @OwnerCar }
                )
                """);
        MappingDefinition md = ctx.findMapping("w::M").orElseThrow();
        assertEquals(List.of("w::Car", "w::Bike"), md.facts().unionMembers().get("w::Vehicle"));
        assertEquals(List.of("w::Car", "w::Bike"), ctx.unionMemberClasses("w::M", "w::Vehicle"));
        assertEquals("w::Car", ctx.routedTargetClass("w::M", "w::Owner", "car"));
        assertEquals(null, ctx.routedTargetClass("w::M", "w::Owner", "name"));
    }
}
