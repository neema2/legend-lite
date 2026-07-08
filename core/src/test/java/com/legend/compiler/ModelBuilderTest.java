package com.legend.compiler;

import com.legend.parser.ElementParser;
import com.legend.parser.ParsedModel;
import com.legend.parser.element.AssociationDefinition;
import com.legend.parser.element.ClassDefinition;
import com.legend.parser.element.DatabaseDefinition;
import com.legend.parser.element.DatabaseDefinition.FilterDefinition;
import com.legend.parser.element.DatabaseDefinition.JoinDefinition;
import com.legend.parser.element.EnumDefinition;
import com.legend.parser.element.Function;
import com.legend.parser.element.LegacyMappingDefinition;
import com.legend.normalizer.MappingNormalizer;
import com.legend.parser.NormalizedModel;
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
                ElementParser.parse("")));
        assertEquals(0, mb.symbols().size());
        assertTrue(mb.classes().toList().isEmpty());
        assertTrue(mb.mappings().toList().isEmpty());
        assertTrue(mb.databases().toList().isEmpty());
        assertEquals(Optional.empty(), mb.findClass("model::Missing"));
    }

    @Test
    void importsArePassedThroughUnchanged() {
        ParsedModel parsed = ElementParser.parse(
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
        ParsedModel parsed = ElementParser.parse(
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
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; }");
        ModelBuilder mb = ModelBuilder.from(parsed);
        ClassDefinition found = mb.findClass("model::Person").orElseThrow();
        assertEquals("model::Person", found.qualifiedName());
        assertEquals(1, found.properties().size());
    }

    @Test
    void findClassReturnsEmptyForMissing() {
        ModelBuilder mb = ModelBuilder.from(ElementParser.parse(
                "Class model::Person { name: String[1]; }"));
        assertEquals(Optional.empty(), mb.findClass("model::Missing"));
    }

    @Test
    void findEnumReturnsParsedDefinition() {
        ParsedModel parsed = ElementParser.parse(
                "Enum model::Color { RED, GREEN, BLUE }");
        ModelBuilder mb = ModelBuilder.from(parsed);
        EnumDefinition found = mb.findEnum("model::Color").orElseThrow();
        assertEquals("model::Color", found.qualifiedName());
    }

    @Test
    void findAssociationReturnsParsedDefinition() {
        ParsedModel parsed = ElementParser.parse(
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
        ParsedModel parsed = ElementParser.parse(
                "Database model::DB ( "
              + "  Table PERSON(ID INTEGER PRIMARY KEY, NAME VARCHAR(64)) "
              + ")");
        ModelBuilder mb = ModelBuilder.from(parsed);
        DatabaseDefinition db = mb.findDatabase("model::DB").orElseThrow();
        assertEquals("model::DB", db.qualifiedName());
        assertEquals(1, db.tables().size());
    }

    @Test
    void findFilterResolvesByDbAndName() {
        ParsedModel parsed = ElementParser.parse(
                "Database model::DB ( "
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
        ParsedModel parsed = ElementParser.parse(
                "Database model::DB ( "
              + "  Table PERSON(ID INTEGER PRIMARY KEY, AGE INTEGER) "
              + ")");
        ModelBuilder mb = ModelBuilder.from(parsed);
        assertEquals(Optional.empty(), mb.findFilter("model::DB", "nope"));
    }

    @Test
    void findFilterReturnsEmptyForUnknownDatabase() {
        ModelBuilder mb = ModelBuilder.from(ElementParser.parse(
                "Class model::A { x: String[1]; }"));
        assertEquals(Optional.empty(), mb.findFilter("other::DB", "nope"));
    }

    @Test
    void findJoinResolvesByDbAndName() {
        ParsedModel parsed = ElementParser.parse(
                "Database model::DB ( "
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
        ParsedModel parsed = ElementParser.parse(
                "Database model::DB ( "
              + "  Table PERSON(ID INTEGER PRIMARY KEY) "
              + ")");
        ModelBuilder mb = ModelBuilder.from(parsed);
        assertEquals(Optional.empty(), mb.findJoin("model::DB", "anything"));
    }

    // ====================================================================
    // findMapping + isMappedClass
    // ====================================================================

    @Test
    void findMappingReturnsParsedDefinition() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
              + "Mapping pkg::M ( "
              + "  *model::Person: Pure { ~src model::Person name: $src.name } "
              + ")");
        ModelBuilder mb = ModelBuilder.from(parsed);
        LegacyMappingDefinition md = mb.findLegacyMapping("pkg::M").orElseThrow();
        assertEquals("pkg::M", md.qualifiedName());
        assertEquals(1, md.classMappings().size());
    }

    @Test
    void isMappedClassTrueForAnyClassWithClassMapping() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
              + "Mapping pkg::M ( "
              + "  *model::Person: Pure { ~src model::Person name: $src.name } "
              + ")");
        ModelBuilder mb = ModelBuilder.from(parsed);
        assertTrue(mb.isMappedClass("model::Person"));
    }

    @Test
    void isMappedClassFalseForUnmappedClass() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
              + "Class model::Other  { x:    String[1]; } "
              + "Mapping pkg::M ( "
              + "  *model::Person: Pure { ~src model::Person name: $src.name } "
              + ")");
        ModelBuilder mb = ModelBuilder.from(parsed);
        assertFalse(mb.isMappedClass("model::Other"));
        assertFalse(mb.isMappedClass("model::TotallyUnknown"));
    }

    @Test
    void isMappedClassTrueAcrossMultipleMappings() {
        // Same class mapped in two different MappingDefinitions: allowed
        // (each is its own setId namespace) and the mapped-class set
        // collapses the duplicates.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
              + "Mapping pkg::M1 ( "
              + "  *model::Person: Pure { ~src model::Person name: $src.name } "
              + ") "
              + "Mapping pkg::M2 ( "
              + "  *model::Person: Pure { ~src model::Person name: $src.name } "
              + ")");
        ModelBuilder mb = ModelBuilder.from(parsed);
        assertTrue(mb.isMappedClass("model::Person"));
        // Both mappings are independently retrievable.
        assertNotNull(mb.findLegacyMapping("pkg::M1").orElseThrow());
        assertNotNull(mb.findLegacyMapping("pkg::M2").orElseThrow());
    }

    @Test
    void duplicateClassMappingWithinOneMappingThrows() {
        // Two ClassMappings for the same class within a single
        // MappingDefinition is a user error: each MappingDefinition
        // may map a given class at most once.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
              + "Class model::SrcA   { name: String[1]; } "
              + "Class model::SrcB   { name: String[1]; } "
              + "Mapping pkg::M ( "
              + "  *model::Person: Pure { ~src model::SrcA name: $src.name } "
              + "  *model::Person[alt]: Pure { ~src model::SrcB name: $src.name } "
              + ")");
        com.legend.error.ModelException ex = assertThrows(
                com.legend.error.ModelException.class,
                () -> ModelBuilder.from(parsed));
        assertTrue(ex.getMessage().contains("model::Person"),
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
        ModelBuilder mb = ModelBuilder.from(ElementParser.parse(
                "Class model::A { x: String[1]; }"));
        assertTrue(mb.findFunction("model::nothing").isEmpty());
    }

    @Test
    void findFunctionReturnsAllOverloadsForFqn() {
        ParsedModel parsed = ElementParser.parse(
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
        ParsedModel raw = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
              + "Class model::RawPerson { name: String[1]; } "
              + "Mapping my::M ( "
              + "  *model::Person: Pure { ~src model::RawPerson name: $src.name } "
              + ")");
        NormalizedModel normalized = MappingNormalizer.normalize(raw, ModelBuilder.from(new com.legend.parser.ParsedModel(raw.elements(), raw.imports())));
        ModelBuilder mb = ModelBuilder.from(new com.legend.parser.ParsedModel(normalized.elements(), normalized.imports()));
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
        ParsedModel raw = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
              + "Class model::RawPerson { name: String[1]; } "
              + "Mapping my::M ( "
              + "  *model::Person: Pure { ~src model::RawPerson name: $src.name } "
              + ")");
        // Pre-normalize: no lifted function exists yet, so findFunction is
        // empty for the lifted FQN.
        assertTrue(ModelBuilder.from(new com.legend.parser.ParsedModel(raw.elements(), raw.imports())).findFunction("my::M$class$model::Person").isEmpty(),
                "before normalization there is no lifted fn to index");
        // Post-normalize: rebuilding from the normalized model re-derives the
        // index and surfaces the lifted fn. Invalidation is implicit — there is
        // no cache; the index is a pure function of the (normalized) input.
        NormalizedModel normalized = MappingNormalizer.normalize(raw, ModelBuilder.from(new com.legend.parser.ParsedModel(raw.elements(), raw.imports())));
        assertFalse(ModelBuilder.from(new com.legend.parser.ParsedModel(normalized.elements(), normalized.imports())).findFunction("my::M$class$model::Person").isEmpty(),
                "after normalization the rebuilt index includes the synth fn");
    }

    // ====================================================================
    // Iteration accessors
    // ====================================================================

    @Test
    void iterationStreamsReturnIngestedElements() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::A { x: String[1]; } "
              + "Class model::B { y: String[1]; } "
              + "Database model::DB ( Table T(ID INTEGER PRIMARY KEY) ) "
              + "Mapping pkg::M ( *model::A: Pure { ~src model::A x: $src.x } )");
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
        ParsedModel parsed = ElementParser.parse(
                "Class model::A { x: String[1]; } "
              + "Class model::B { y: String[1]; } "
              + "Database model::DB ( Table T(ID INTEGER PRIMARY KEY) )");
        ModelBuilder mb = ModelBuilder.from(parsed);
        assertTrue(mb.symbols().allFqns().contains("model::A"));
        assertTrue(mb.symbols().allFqns().contains("model::B"));
        assertTrue(mb.symbols().allFqns().contains("model::DB"));
    }
}
