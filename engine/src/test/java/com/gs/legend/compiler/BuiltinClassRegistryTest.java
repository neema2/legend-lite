package com.gs.legend.compiler;

import com.gs.legend.model.PureModelBuilder;
import com.gs.legend.model.m3.Property;
import com.gs.legend.model.m3.PureClass;
import com.gs.legend.model.m3.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stage C of phase 2.5e (L4): the {@link BuiltinClassRegistry} catalog parses cleanly at
 * JVM init and every fresh {@link PureModelBuilder} has the platform classes (Pair, native
 * Type stub, etc.) seeded and findable.
 *
 * <p>These tests pin the <em>mechanism</em> end-to-end — subsequent commits will expand
 * {@code BUILTIN_CLASS_SOURCES} to the full 26-entry catalog, but the shape asserted here
 * (parse → seed → find by FQN → find by simple name → verify shape) stays stable.
 */
class BuiltinClassRegistryTest {

    @Test
    void catalogStaticInitializationProducesExpectedClasses() {
        // Touching BUILTIN_CLASS_DEFINITIONS forces the static initializer. Any parse
        // failure in a source string aborts class init with an IllegalStateException;
        // a clean access plus the FQN spot-checks below prove every current source
        // produced a ClassDefinition with the right identity.
        assertEquals(BuiltinClassRegistry.BUILTIN_CLASS_DEFINITIONS.size(),
                BuiltinClassRegistry.BUILTIN_CLASS_FQNS.size(),
                "Every definition contributes exactly one FQN");

        // Spot-check: current catalog must ship at least these two anchors. When the
        // catalog expands, add assertions here rather than a "size == N" test, which
        // breaks on every addition without buying signal.
        assertTrue(BuiltinClassRegistry.BUILTIN_CLASS_FQNS.contains(
                        "meta::pure::metamodel::type::Type"),
                "Catalog must include the m3 Type root");
        assertTrue(BuiltinClassRegistry.BUILTIN_CLASS_FQNS.contains(
                        "meta::pure::functions::collection::Pair"),
                "Catalog must include Pair");
    }

    @Test
    void nativeTypeStubIsRegistered() {
        // The m3 root Type class ships as a bootstrap stub (body not expressible in Pure).
        var typeDef = BuiltinClassRegistry.BUILTIN_CLASS_DEFINITIONS.stream()
                .filter(cd -> "meta::pure::metamodel::type::Type".equals(cd.qualifiedName()))
                .findFirst().orElseThrow();

        assertTrue(typeDef.isNative(), "Type must be declared native Class");
        assertTrue(typeDef.properties().isEmpty(), "native Type has no properties");
        assertTrue(typeDef.typeParams().isEmpty(), "native Type has no type parameters");
    }

    @Test
    void pairDefinitionCarriesTypeParamsAndKeyStereotypes() {
        // AST-layer sanity: the catalog source for Pair parsed through NameResolver-free
        // pipeline must preserve its two type parameters and the equality.Key stereotypes
        // on both properties.
        var pairDef = BuiltinClassRegistry.BUILTIN_CLASS_DEFINITIONS.stream()
                .filter(cd -> "meta::pure::functions::collection::Pair".equals(cd.qualifiedName()))
                .findFirst().orElseThrow();

        assertEquals(List.of("U", "V"), pairDef.typeParams());
        assertFalse(pairDef.isNative());

        // Both properties must carry the equality.Key stereotype with FQN-qualified profile.
        for (var prop : pairDef.properties()) {
            var keyStereotype = prop.stereotypes().stream()
                    .filter(s -> "Key".equals(s.stereotypeName()))
                    .findFirst().orElseThrow(() -> new AssertionError(
                            "Pair." + prop.name() + " should carry equality.Key stereotype"));
            assertEquals("meta::pure::profiles::equality", keyStereotype.profileName());
        }
    }

    @Test
    void freshBuilderSeedsBuiltinClassesByFqn() {
        // End-to-end: a blank PureModelBuilder — no addSource call — must already host
        // every catalog entry in its symbol table, discoverable by findClass(fqn).
        PureModelBuilder builder = new PureModelBuilder();

        for (String fqn : BuiltinClassRegistry.BUILTIN_CLASS_FQNS) {
            assertTrue(builder.findClass(fqn).isPresent(),
                    "Fresh builder should already have builtin class: " + fqn);
        }
    }

    @Test
    void seededPairResolvesPropertiesToTypeVars() {
        // Exercises Stage A's type-var machinery through the seeded catalog: because Pair
        // is declared Class Pair<U,V> { first: U[1]; second: V[1]; }, the seeded PureClass
        // must have first/second resolved to Type.TypeVar, not NameRef or ClassType.
        PureModelBuilder builder = new PureModelBuilder();
        PureClass pair = builder.findClass("meta::pure::functions::collection::Pair")
                .orElseThrow();

        assertEquals(List.of("U", "V"), pair.typeParams());

        Property first = pair.findLocalProperty("first").orElseThrow();
        Property second = pair.findLocalProperty("second").orElseThrow();
        assertEquals("U", ((Type.TypeVar) first.type()).name());
        assertEquals("V", ((Type.TypeVar) second.type()).name());
    }

    @Test
    void seededPairCarriesPropertyStereotypes() {
        // Stage B × Stage C interaction: property stereotypes survive symbol conversion
        // for catalog classes, not just user source (phase 2.5e Property gained its
        // stereotypes field so this works end-to-end).
        PureClass pair = new PureModelBuilder()
                .findClass("meta::pure::functions::collection::Pair").orElseThrow();

        Property first = pair.findLocalProperty("first").orElseThrow();
        assertTrue(first.hasStereotype("meta::pure::profiles::equality", "Key"));
    }

    @Test
    void userSourceCanReferenceBuiltinByBareSimpleName() {
        // End-to-end import wiring check: if BUILTIN_IMPORTS is configured correctly,
        // user source must be able to say `Pair` (simple name) for a non-generic
        // property type and have it resolve to the full Pair ClassType — no explicit
        // `import meta::pure::functions::collection::*` required. Exercises the full
        // path (parse → NameResolver → convertProperty → findType).
        String source = """
                Class model::UsesPair
                {
                    holder: Pair[1];
                }
                """;
        PureModelBuilder builder = new PureModelBuilder().addSource(source);
        PureClass usesPair = builder.findClass("model::UsesPair").orElseThrow();

        Type holderType = usesPair.findLocalProperty("holder").orElseThrow().type();
        Type.ClassType ct = assertInstanceOf(Type.ClassType.class, holderType,
                "Bare 'Pair' on a property must resolve to ClassType, got "
                        + holderType.getClass().getSimpleName());
        assertEquals("meta::pure::functions::collection::Pair", ct.qualifiedName(),
                "Simple name 'Pair' must canonicalize to its catalog FQN via BUILTIN_IMPORTS");
    }

    @Test
    void builtinImportsHasNoDuplicates() {
        // Invariant: BUILTIN_IMPORTS is assembled from multiple sources (primitives,
        // platform enums, platform profiles, BuiltinClassRegistry). A future maintainer
        // could accidentally list the same FQN in two places and silently create a
        // duplicate in the import scope — this test pins that against regression.
        List<String> imports = BuiltinRegistry.BUILTIN_IMPORTS;
        long distinct = imports.stream().distinct().count();
        assertEquals(imports.size(), distinct,
                "BUILTIN_IMPORTS must contain no duplicate FQNs. Duplicates found: "
                        + imports.stream()
                                .filter(fqn -> imports.stream().filter(fqn::equals).count() > 1)
                                .distinct()
                                .toList());
    }

    @Test
    void userClassSimpleNameResolutionStillWorks() {
        // Regression guard: seeding builtin classes must not break ordinary user-class
        // resolution. A trivial user model alongside the seeded catalog should still build.
        String source = """
                Class model::Person
                {
                    firstName: String[1];
                    age: Integer[0..1];
                }
                """;
        PureModelBuilder builder = new PureModelBuilder().addSource(source);
        assertNotNull(builder.findClass("model::Person").orElse(null));
        // And the builtin is still there:
        assertTrue(builder.findClass("meta::pure::functions::collection::Pair").isPresent());
    }
}
