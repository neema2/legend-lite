package com.legend.compiler;

import com.legend.model.ParsedModel;
import com.legend.model.AssociationDefinition;
import com.legend.model.ClassDefinition;
import com.legend.model.DatabaseDefinition;
import com.legend.model.DatabaseDefinition.FilterDefinition;
import com.legend.model.DatabaseDefinition.JoinDefinition;
import com.legend.model.EnumDefinition;
import com.legend.model.Function;
import com.legend.model.LegacyMappingDefinition;
import com.legend.normalizer.MappingNormalizer;
import com.legend.model.NormalizedModel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("ModelBuilder — indexed view of ParsedModel")
class ModelBuilderTest {

    // ====================================================================
    // Construction & passthroughs
    // ====================================================================

    @Test
    void emptyModelBuildsEmptyView() {
        ModelBuilder mb = ModelBuilder.from(ParsedModel.class.cast(
                com.legend.testing.Own.model("")));
        assertEquals(0, mb.symbols().size());
        assertTrue(mb.classes().toList().isEmpty());
        assertTrue(mb.mappings().toList().isEmpty());
        assertTrue(mb.databases().toList().isEmpty());
        assertEquals(Optional.empty(), mb.findClass("model::Missing"));
    }

    @Test
    void importsArePassedThroughUnchanged() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "import model::*; "
              + "Class other::Foo { x: String[1]; }");
        ModelBuilder mb = ModelBuilder.from(parsed);
        assertSame(parsed.imports(), mb.imports());
    }

    @Test
    void buildIsIdempotentReferentially() {
        // Two builds from the same ParsedModel return distinct instances
        // but indexes are content-equal: same FQNs interned, same elements
        // retrievable. This documents that ModelBuilder.from is a pure
        // function of its input.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::A { x: String[1]; }");
        ModelBuilder a = ModelBuilder.from(parsed);
        ModelBuilder b = ModelBuilder.from(parsed);
        assertEquals(a.symbols().size(), b.symbols().size());
        assertSame(
                a.findClass("model::A").orElseThrow(),
                b.findClass("model::A").orElseThrow(),
                "Both builds reference the same parser-produced ClassDefinition");
    }

    // ====================================================================
    // findClass / findEnum / findAssociation
    // ====================================================================

    @Test
    void findClassReturnsParsedDefinition() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; }");
        ModelBuilder mb = ModelBuilder.from(parsed);
        ClassDefinition found = mb.findClass("model::Person").orElseThrow();
        assertEquals("model::Person", found.qualifiedName());
        assertEquals(1, found.properties().size());
    }

    @Test
    void findClassReturnsEmptyForMissing() {
        ModelBuilder mb = ModelBuilder.from(com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; }"));
        assertEquals(Optional.empty(), mb.findClass("model::Missing"));
    }

    @Test
    void findEnumReturnsParsedDefinition() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "Enum model::Color { RED, GREEN, BLUE }");
        ModelBuilder mb = ModelBuilder.from(parsed);
        EnumDefinition found = mb.findEnum("model::Color").orElseThrow();
        assertEquals("model::Color", found.qualifiedName());
    }

    @Test
    void findAssociationReturnsParsedDefinition() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::A { x: String[1]; } "
              + "Class model::B { y: String[1]; } "
              + "Association model::A_B { a: model::A[1]; b: model::B[1]; }");
        ModelBuilder mb = ModelBuilder.from(parsed);
        AssociationDefinition found = mb.findAssociation("model::A_B").orElseThrow();
        assertEquals("model::A_B", found.qualifiedName());
    }

    // ====================================================================
    // findDatabase + nested filter/join lookups
    // ====================================================================

    @Test
    void findDatabaseReturnsParsedDefinition() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "\n###Relational\nDatabase model::DB ( "
              + "  Table PERSON(ID INTEGER PRIMARY KEY, NAME VARCHAR(64)) "
              + ")");
        ModelBuilder mb = ModelBuilder.from(parsed);
        DatabaseDefinition db = mb.findDatabase("model::DB").orElseThrow();
        assertEquals("model::DB", db.qualifiedName());
        assertEquals(1, db.tables().size());
    }

    @Test
    void findFilterResolvesByDbAndName() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "\n###Relational\nDatabase model::DB ( "
              + "  Table PERSON(ID INTEGER PRIMARY KEY, AGE INTEGER) "
              + "  Filter activeOnly(PERSON.AGE > 18) "
              + ")");
        ModelBuilder mb = ModelBuilder.from(parsed);
        FilterDefinition f = mb.findFilter("model::DB", "activeOnly").orElseThrow();
        assertEquals("activeOnly", f.name());
        assertNotNull(f.condition());
    }

    @Test
    void findFilterReturnsEmptyForMissingFilter() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "\n###Relational\nDatabase model::DB ( "
              + "  Table PERSON(ID INTEGER PRIMARY KEY, AGE INTEGER) "
              + ")");
        ModelBuilder mb = ModelBuilder.from(parsed);
        assertEquals(Optional.empty(), mb.findFilter("model::DB", "nope"));
    }

    @Test
    void findFilterReturnsEmptyForUnknownDatabase() {
        ModelBuilder mb = ModelBuilder.from(com.legend.testing.Own.model(
                "Class model::A { x: String[1]; }"));
        assertEquals(Optional.empty(), mb.findFilter("other::DB", "nope"));
    }

    @Test
    void findJoinResolvesByDbAndName() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "\n###Relational\nDatabase model::DB ( "
              + "  Table PERSON(ID INTEGER PRIMARY KEY, FIRM_ID INTEGER) "
              + "  Table FIRM(ID INTEGER PRIMARY KEY, NAME VARCHAR(64)) "
              + "  Join person_firm(PERSON.FIRM_ID = FIRM.ID) "
              + ")");
        ModelBuilder mb = ModelBuilder.from(parsed);
        JoinDefinition j = mb.findJoin("model::DB", "person_firm").orElseThrow();
        assertEquals("person_firm", j.name());
        assertNotNull(j.operation());
    }

    @Test
    void findJoinReturnsEmptyForMissingJoin() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "\n###Relational\nDatabase model::DB ( "
              + "  Table PERSON(ID INTEGER PRIMARY KEY) "
              + ")");
        ModelBuilder mb = ModelBuilder.from(parsed);
        assertEquals(Optional.empty(), mb.findJoin("model::DB", "anything"));
    }

    // ====================================================================
    // findMapping
    // ====================================================================

    @Test
    void findMappingReturnsParsedDefinition() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
              + "\n###Mapping\nMapping pkg::M ( "
              + "  *model::Person: Pure { ~src model::Person name: $src.name } "
              + ")");
        ModelBuilder mb = ModelBuilder.from(parsed);
        LegacyMappingDefinition md = mb.findLegacyMapping("pkg::M").orElseThrow();
        assertEquals("pkg::M", md.qualifiedName());
        assertEquals(1, md.classMappings().size());
    }

    @Test
    void duplicateClassMappingWithinOneMappingThrows() {
        // Two ClassMappings for the same class within a single
        // MappingDefinition is a user error: each MappingDefinition
        // may map a given class at most once.
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
              + "Class model::SrcA   { name: String[1]; } "
              + "Class model::SrcB   { name: String[1]; } "
              + "\n###Mapping\nMapping pkg::M ( "
              + "  *model::Person: Pure { ~src model::SrcA name: $src.name } "
              + "  *model::Person[alt]: Pure { ~src model::SrcB name: $src.name } "
              + ")");
        com.legend.error.ModelException ex = assertThrows(
                com.legend.error.ModelException.class,
                () -> ModelBuilder.from(parsed));
        assertTrue(String.valueOf(ex.getMessage()).contains("model::Person"),
                () -> "Error should name the duplicated class; got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("pkg::M"),
                () -> "Error should name the mapping; got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("multiple ClassMappings"),
                () -> "Error should mention 'multiple ClassMappings' for parity with the "
                    + "existing R2 contract; got: " + ex.getMessage());
    }

    // ====================================================================
    // findFunction (overloads)
    // ====================================================================

    @Test
    void findFunctionReturnsEmptyListForMissing() {
        ModelBuilder mb = ModelBuilder.from(com.legend.testing.Own.model(
                "Class model::A { x: String[1]; }"));
        assertTrue(mb.findFunction("model::nothing").isEmpty());
    }

    @Test
    void findFunctionReturnsAllOverloadsForFqn() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "function model::greet(name: String[1]): String[1] { $name } "
              + "function model::greet(name: String[1], greeting: String[1]): String[1] { $greeting }");
        ModelBuilder mb = ModelBuilder.from(parsed);
        List<Function> overloads = mb.findFunction("model::greet");
        assertEquals(2, overloads.size(),
                () -> "Both overloads should be retrievable under the shared FQN");
        for (Function fn : overloads) {
            assertEquals("model::greet", fn.qualifiedName());
        }
    }

    // ====================================================================
    // Synth mapping functions: owner-stored, lookup-flattened
    // ====================================================================

    @Test
    @DisplayName("lifted mapping functions resolve through findFunction")
    void mappingFunctionsAreIngestedIntoFindFunction() {
        ParsedModel raw = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
              + "Class model::RawPerson { name: String[1]; } "
              + "\n###Mapping\nMapping my::M ( "
              + "  *model::Person: Pure { ~src model::RawPerson name: $src.name } "
              + ")");
        NormalizedModel normalized = MappingNormalizer.normalize(raw, ModelBuilder.from(new com.legend.model.ParsedModel(raw.elements(), raw.imports())));
        ModelBuilder mb = ModelBuilder.from(new com.legend.model.ParsedModel(normalized.elements(), normalized.imports()));
        // The lifted realizing function resolves through the ONE findFunction
        // path, identical to a user-written function: it is an ordinary
        // top-level FunctionDefinition element ingested by the same function
        // arm (no bespoke flatten — docs/CLEAN_SHEET_INVERSION.md §2.2).
        List<Function> hit = mb.findFunction("my::M$class$model::Person");
        assertEquals(1, hit.size(),
                "lifted mapping fn must be resolvable via findFunction");
        assertEquals("my::M$class$model::Person", hit.get(0).qualifiedName());
    }

    @Test
    @DisplayName("the function index is a pure function of the normalized input (implicit invalidation)")
    void mappingFunctionIndexIsDrivenByNormalizedInput() {
        ParsedModel raw = com.legend.testing.Own.model(
                "Class model::Person { name: String[1]; } "
              + "Class model::RawPerson { name: String[1]; } "
              + "\n###Mapping\nMapping my::M ( "
              + "  *model::Person: Pure { ~src model::RawPerson name: $src.name } "
              + ")");
        // Pre-normalize: no lifted function exists yet, so findFunction is
        // empty for the lifted FQN.
        assertTrue(ModelBuilder.from(new com.legend.model.ParsedModel(raw.elements(), raw.imports())).findFunction("my::M$class$model::Person").isEmpty(),
                "before normalization there is no lifted fn to index");
        // Post-normalize: rebuilding from the normalized model re-derives the
        // index and surfaces the lifted fn. Invalidation is implicit — there is
        // no cache; the index is a pure function of the (normalized) input.
        NormalizedModel normalized = MappingNormalizer.normalize(raw, ModelBuilder.from(new com.legend.model.ParsedModel(raw.elements(), raw.imports())));
        assertFalse(ModelBuilder.from(new com.legend.model.ParsedModel(normalized.elements(), normalized.imports())).findFunction("my::M$class$model::Person").isEmpty(),
                "after normalization the rebuilt index includes the synth fn");
    }

    // ====================================================================
    // Iteration accessors
    // ====================================================================

    @Test
    void iterationStreamsReturnIngestedElements() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::A { x: String[1]; } "
              + "Class model::B { y: String[1]; } "
              + "\n###Relational\nDatabase model::DB ( Table T(ID INTEGER PRIMARY KEY) ) "
              + "\n###Mapping\nMapping pkg::M ( *model::A: Pure { ~src model::A x: $src.x } )");
        ModelBuilder mb = ModelBuilder.from(parsed);
        assertEquals(2, mb.classes().count());
        assertEquals(1, mb.databases().count());
        assertEquals(1, mb.legacyMappings().count());
    }

    // ====================================================================
    // Symbol-table integration
    // ====================================================================

    @Test
    void symbolTableInternsEveryIngestedFqn() {
        ParsedModel parsed = com.legend.testing.Own.model(
                "Class model::A { x: String[1]; } "
              + "Class model::B { y: String[1]; } "
              + "\n###Relational\nDatabase model::DB ( Table T(ID INTEGER PRIMARY KEY) )");
        ModelBuilder mb = ModelBuilder.from(parsed);
        assertTrue(mb.symbols().allFqns().contains("model::A"));
        assertTrue(mb.symbols().allFqns().contains("model::B"));
        assertTrue(mb.symbols().allFqns().contains("model::DB"));
    }

    /** The names fixes, item 6 (2026-11-13, docs/rust-port/PHASE3_SHIM_DESIGN.md §10 ruling 28): a database registered
     * twice is last-wins in every index (the first's joins and views stayed when the second had none); an association
     * end injects through a generic supertype's head too (only a bare name was climbed); a join-only view's root reads
     * its join over the include closure (only the database's own joins, else refused). */
    @Test
    void indexIsLastWinsClimbsGenericHeadsAndReadsJoinsThroughIncludes() {
        ModelBuilder twice = ModelBuilder.from(com.legend.testing.Own.model("""
                ###Relational
                Database a::Db ( Table T (ID INTEGER) Join J (T.ID = T.ID) View V (id: T.ID) )
                Database a::Db ( Table T (ID INTEGER) )
                """));
        assertEquals(1, twice.duplicateElements().size());
        assertTrue(twice.findJoin("a::Db", "J").isEmpty(), "the second definition has no join");
        assertTrue(twice.findView("a::Db", "V").isEmpty(), "the second definition has no view");
        ModelBuilder generic = ModelBuilder.from(com.legend.testing.Own.model("""
                Class a::B {}
                Class a::C extends a::B<String> {}
                Class a::X {}
                Association a::BX { bs: a::B[*]; x: a::X[1]; }
                """));
        assertEquals("a::BX", generic.findAssociationOf("a::C", "x").orElseThrow().qualifiedName());
        ModelBuilder included = ModelBuilder.from(com.legend.testing.Own.model("""
                ###Relational
                Database a::Base ( Table P (ID INTEGER, F INTEGER) Table F (ID INTEGER, N VARCHAR(10)) Join PF (P.F = F.ID) )
                Database a::Top ( include a::Base View V ( name: @PF | F.N ) )
                """));
        assertEquals("P", included.viewMainTable("a::Top", included.findView("a::Top", "V").orElseThrow()));
    }
}
