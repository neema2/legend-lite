// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.Property;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.model.ClassDefinition;
import com.legend.model.ParsedModel;
import com.legend.protocol.TypeExpression;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T4.1 step 1 witness: association QUALIFIED properties adopt into their
 * owning class in the knowledge layer (F1), before Phase E. Modeled on the
 * corpus's {@code meta::relational::tests::model::simple::ProdSynonym}
 * (distinct ends, a parameterized qualifier, both return multiplicities),
 * plus the self-association and the no-unique-end error.
 */
class KnowledgeLayerTest {

    private static final String PROD_SYNONYM = """
            Class w::Product { name: String[1]; }
            Class w::Synonym { name: String[1]; type: String[1]; }
            Association w::ProdSynonym
            {
              synonyms: w::Synonym[*];
              product: w::Product[1];
              synonymByType(type: String[1]) { $this.synonyms->filter(s | $s.type == $type)->toOne() }: w::Synonym[1];
              synonymsByTypes(types: String[*]) { $this.synonyms->filter(s | $s.type->in($types)) }: w::Synonym[*];
            }
            """;

    private static Property.Derived derived(ModelContext ctx, String cls, String name) {
        return ctx.findClass(cls).orElseThrow().properties().stream()
                .filter(p -> p instanceof Property.Derived d && d.name().equals(name))
                .map(p -> (Property.Derived) p)
                .findFirst().orElseThrow(() -> new AssertionError(
                        cls + " does not carry qualified property " + name));
    }

    private static boolean carries(ModelContext ctx, String cls, String name) {
        return ctx.findClass(cls).orElseThrow().properties().stream()
                .anyMatch(p -> p instanceof Property.Derived d && d.name().equals(name));
    }

    @Test
    @DisplayName("distinct ends: the OTHER end's class owns the qualified property (both multiplicities)")
    void distinctEndsOtherEndOwns() {
        ModelContext ctx = Compiler.compileModel(PROD_SYNONYM);
        // both return Synonym, so Product (the other end) owns both
        derived(ctx, "w::Product", "synonymByType");
        derived(ctx, "w::Product", "synonymsByTypes");
        assertFalse(carries(ctx, "w::Synonym", "synonymByType"));
        assertFalse(carries(ctx, "w::Synonym", "synonymsByTypes"));
        // the adopted property lifts through the ONE class-derived funnel
        assertEquals(SynthFqn.prop("w::Product", "synonymByType"),
                derived(ctx, "w::Product", "synonymByType").bodyFunctionFqn());
    }

    @Test
    @DisplayName("parameterized qualifier: the parameter rides the adopted property")
    void parameterizedQualifierKeepsItsParameter() {
        ModelContext ctx = Compiler.compileModel(PROD_SYNONYM);
        Property.Derived byType = derived(ctx, "w::Product", "synonymByType");
        assertEquals(1, byType.parameters().size(), byType.parameters().toString());
        assertEquals("type", byType.parameters().get(0).name());
        Property.Derived byTypes = derived(ctx, "w::Product", "synonymsByTypes");
        assertEquals("types", byTypes.parameters().get(0).name());
    }

    @Test
    @DisplayName("self-association: the one class owns its qualified property")
    void selfAssociationOwnsItself() {
        ModelContext ctx = Compiler.compileModel("""
                Class w::Person { name: String[1]; }
                Association w::Friends
                {
                  a: w::Person[*];
                  b: w::Person[*];
                  friendsNamed(n: String[1]) { $this.a->filter(p | $p.name == $n) }: w::Person[*];
                }
                """);
        derived(ctx, "w::Person", "friendsNamed");
    }

    private static final String NO_UNIQUE_END = """
            Class w::Product { name: String[1]; }
            Class w::Synonym { name: String[1]; }
            Class w::Other { name: String[1]; }
            Association w::ProdSynonym
            {
              synonyms: w::Synonym[*];
              product: w::Product[1];
              stray() { $this.synonyms->map(s | ^w::Other(name = $s.name)) }: w::Other[*];
            }
            """;

    @Test
    @DisplayName("no unique owning end: a MODEL error — strict throws, a module build walls the association")
    void noUniqueOwningEnd() {
        ModelException strict = assertThrows(ModelException.class,
                () -> Compiler.compileModel(NO_UNIQUE_END));
        assertEquals(LegendCompileException.Phase.MODEL, strict.phase());
        assertTrue(strict.getMessage().contains(
                "qualified property 'stray' returns 'w::Other', which does not identify"
                        + " a unique owning end"), strict.getMessage());
        Compiler.BuiltModule module = Compiler.buildModule(Compiler.parseSources(List.of(
                new Compiler.ModelSource("m.pure", NO_UNIQUE_END))).model());
        // same message; the strict path additionally carries the element's
        // [line:col] because the error now names the association
        String wall = module.walls().get("w::ProdSynonym");
        assertTrue(wall != null && strict.getMessage().endsWith(wall),
                strict.getMessage() + " vs " + module.walls());
        // the rest of the module stands; nothing adopted the stray property
        assertFalse(carries(module.context(), "w::Product", "stray"));
        assertFalse(carries(module.context(), "w::Synonym", "stray"));
    }

    @Test
    @DisplayName("the normalizer refuses a model the knowledge layer has not adopted")
    void normalizerRequiresAdoption() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> com.legend.testing.Phases.normalize(
                        NameResolver.resolve(com.legend.testing.Own.model(PROD_SYNONYM))));
        assertTrue(ex.getMessage().contains("w::ProdSynonym"), ex.getMessage());
        assertTrue(ex.getMessage().contains("synonymByType"), ex.getMessage());
    }

    // ==================================================================
    // Step 3a — the knowledge kernel (subtype family)
    // ==================================================================

    private static KnowledgeLayer kernel(String src) {
        return ModelBuilder.from(NameResolver.resolve(com.legend.testing.Own.model(src))).knowledge();
    }

    @Test
    @DisplayName("kernel: a bare superclass name under an import resolves — no bare-name gap")
    void bareSuperclassNameUnderImportIsResolved() {
        KnowledgeLayer k = kernel("""
                import w::base::*;
                Class w::base::Person { name: String[1]; }
                Class w::Employee extends Person { grade: Integer[1]; }
                Class w::Manager extends w::Employee { reports: Integer[1]; }
                """);
        assertTrue(k.isSubtype("w::Employee", "w::base::Person"));
        assertTrue(k.isSubtype("w::Manager", "w::base::Person"));
        assertFalse(k.isSubtype("w::base::Person", "w::Employee"));
        assertFalse(k.isSubtype("w::Nobody", "w::base::Person"));
        // a primitive is not a class for the hierarchy (the catalog's lattice
        // node is not walked), while classDef still answers the catalog
        assertFalse(k.isSubtype("Integer", "Number"));
        assertTrue(k.hierarchyClass("Integer").isEmpty());
        assertTrue(k.hierarchyClass(null).isEmpty());
        assertEquals(List.of("w::Manager", "w::Employee", "w::base::Person"),
                List.copyOf(k.ancestorsAndSelf("w::Manager")));
        assertEquals(java.util.Set.of("w::Employee", "w::Manager"), k.subtree("w::base::Person"));
        // lineage: definitions nearest first; LOUD for an unknown class
        assertEquals(List.of("w::Manager", "w::Employee", "w::base::Person"),
                k.lineage("w::Manager").stream().map(ClassDefinition::qualifiedName).toList());
        assertThrows(IllegalStateException.class, () -> k.lineage("w::Nobody"));
        assertEquals(List.of("w::Employee"), k.directSubtypes("w::base::Person"));
    }

    private static ClassDefinition cls(String fqn, TypeExpression... supers) {
        return new ClassDefinition(fqn, List.of(), List.of(), List.of(supers), List.of(),
                List.of(), List.of(), List.of(), List.of(), false, List.of());
    }

    @Test
    @DisplayName("kernel: a generic superclass head IS a superclass; ancestorsBelow stops at the root")
    void genericSuperAndAncestorsBelow() {
        // records, not Pure text: generics are platform-dialect grammar the
        // product surface refuses, and the kernel's rule is what is pinned
        ParsedModel parsed = new ParsedModel(List.of(
                cls("w::Box"),
                cls("w::Named"),
                cls("w::Root", new TypeExpression.NameRef("w::Named")),
                cls("w::Mid", new TypeExpression.NameRef("w::Root"),
                        new TypeExpression.Generic("w::Box",
                                List.of(new TypeExpression.NameRef("String")))),
                cls("w::Leaf", new TypeExpression.NameRef("w::Mid"))),
                com.legend.model.ImportScope.empty());
        KnowledgeLayer k = ModelBuilder.from(parsed).knowledge();
        assertTrue(k.isSubtype("w::Mid", "w::Box"), "extends Box<String> is a subtype of Box");
        assertTrue(k.isSubtype("w::Leaf", "w::Named"));
        // below Root: Leaf, Mid and Mid's OTHER parent Box — never Root or Named
        assertEquals(List.of("w::Leaf", "w::Mid", "w::Box"),
                List.copyOf(k.ancestorsBelow("w::Leaf", "w::Root")));
        // memoized answers survive repeated asks
        assertTrue(k.isSubtype("w::Leaf", "w::Named"));
    }

    // ==================================================================
    // Step 3d — the store family
    // ==================================================================

    @Test
    @DisplayName("kernel: tables and columns resolve through database includes, schemas and views")
    void storeLookupsWalkIncludesSchemasAndViews() {
        KnowledgeLayer k = kernel("""
                ###Relational
                Database w::Base (
                  Schema hr ( Table EMP (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), SALARY DECIMAL(10,2)) )
                  Table DEPT (ID INTEGER PRIMARY KEY, TITLE VARCHAR(50))
                  View DeptView ( id: DEPT.ID, title: DEPT.TITLE )
                )
                Database w::Top ( include w::Base
                  Table LOCAL (ID INTEGER PRIMARY KEY, WHEN_ TIMESTAMP) )
                """);
        // include walk; table and column names compare exactly, as the engine's do
        assertTrue(k.table("w::Top", "DEPT").isPresent());
        assertTrue(k.table("w::Top", "dept").isEmpty());
        assertEquals("TITLE", k.column("w::Top", "DEPT", "TITLE").orElseThrow().name());
        assertTrue(k.column("w::Top", "DEPT", "title").isEmpty());
        // a dotted spelling names the schema's table only; a bare name reaches every schema
        assertTrue(k.table("w::Top", "hr.EMP").isPresent());
        assertTrue(k.table("w::Top", "EMP").isPresent());
        // a WRONG schema qualifier resolves nothing (the engine refuses it)
        assertTrue(k.table("w::Top", "sales.EMP").isEmpty());
        assertTrue(k.table("w::Top", "default.DEPT").isPresent());
        assertTrue(k.table("w::Top", null).isEmpty());
        assertTrue(k.table("w::Nowhere", "DEPT").isEmpty());
        // kinds: a physical column's, a view column's through its ColumnRef, a local table's
        assertEquals("Decimal", k.columnKind("w::Base", "hr.EMP", "SALARY"));
        assertEquals("String", k.columnKind("w::Base", "DeptView", "title"));
        assertEquals("DateTime", k.columnKind("w::Top", "LOCAL", "WHEN_"));
        assertEquals(null, k.columnKind("w::Base", "DeptView", "nope"));
    }
}
