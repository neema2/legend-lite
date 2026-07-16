package com.legend.normalizer;

import com.legend.model.NormalizedModel;
import com.legend.parser.ElementParser;
import com.legend.model.Multiplicity;
import com.legend.model.ParsedModel;
import com.legend.model.TypeExpression;
import com.legend.model.FunctionDefinition;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.MappingDefinition;
import com.legend.model.Realization;
import com.legend.model.PackageableElement;
import com.legend.model.SynthHat;
import com.legend.builtin.Pure;
import com.legend.compiler.ModelBuilder;
import com.legend.model.spec.AppliedFunction;
import com.legend.model.spec.AppliedProperty;
import com.legend.model.spec.ColSpec;
import com.legend.model.spec.ColSpecArray;
import com.legend.model.spec.KeyExpression;
import com.legend.model.spec.LambdaFunction;
import com.legend.model.spec.NewInstance;
import com.legend.model.spec.NewInstanceCast;
import com.legend.model.spec.PackageableElementPtr;
import com.legend.model.spec.PureCollection;
import com.legend.model.spec.ValueSpecification;
import com.legend.model.spec.Variable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Phase E.1 MappingNormalizer tests &mdash; Increment 1.
 *
 * <p>Coverage in this increment:
 * <ul>
 *   <li>Foundation: passthrough of non-Mapping elements, empty mappings,
 *       synth FQN convention, return type shape.</li>
 *   <li>M2M (Pure) with all-primitive bindings, no filter, no class-typed
 *       properties.</li>
 *   <li>Relational with Column PMs only, no filter, no joins, no distinct,
 *       no groupBy.</li>
 * </ul>
 * Subsequent increments add filters, joins/associates, embedded/inline
 * embedded, class-typed materialization, distinct/groupBy, etc.
 */
@DisplayName("MappingNormalizer — Phase E.1 (Increment 1)")
class MappingNormalizerTest {

    // ====================================================================
    // 1 & 2 — Foundation
    // ====================================================================

    @Test
    void passthroughForNonMappingElements() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { firstName: String[1]; }");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        assertEquals(1, normalized.elements().size());
        assertSame(parsed.elements().get(0), normalized.elements().get(0),
                "Non-Mapping elements should be referentially identical (not rebuilt)");
    }

    @Test
    void preNormalize_noLiftedFunctions_postNormalize_oneFnPerClassMapping() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "Class model::Firm   { legalName: String[1]; } "
                        + "Class model::RawPerson { name: String[1]; } "
                        + "Class model::RawFirm   { legalName: String[1]; } "
                        + "Mapping my::M ( "
                        + "  *model::Person: Pure { ~src model::RawPerson name: $src.name } "
                        + "  *model::Firm:   Pure { ~src model::RawFirm   legalName: $src.legalName } "
                        + ")");
        // Pre-normalize: the parsed model carries no lifted functions at all.
        long preLifted = parsed.elements().stream()
                .filter(e -> e instanceof FunctionDefinition fd && fd.isSynthesized())
                .count();
        assertEquals(0, preLifted, "Pre-normalize: no lifted functions in the model");

        NormalizedModel normalized = normalizeViaPipeline(parsed);
        List<FunctionDefinition> lifted = liftedFunctions(normalized);
        assertEquals(2, lifted.size(),
                "Post-normalize: one lifted fn per ClassMapping, preserving declaration order");
        assertEquals("my::M$class$model::Person", lifted.get(0).qualifiedName());
        assertEquals("my::M$class$model::Firm",   lifted.get(1).qualifiedName());
        // Body shape sanity: each must end in ^Target(...). If the body were
        // empty or missing the terminal map(...|^Target), this asserts that.
        for (FunctionDefinition fn : lifted) {
            AppliedFunction map = (AppliedFunction) sole(fn.body());
            assertEquals("map", map.function(),
                    "every synth body terminates in map(...)");
            LambdaFunction lambda = (LambdaFunction) map.parameters().get(1);
            AppliedFunction newCall = (AppliedFunction) sole(lambda.body());
            assertEquals("new", newCall.function(),
                    "every map lambda body is ^Target(...) ie new()");
        }
    }

    // ====================================================================
    // M3 — canonical binding table (surface -> canonical rewrite)
    // ====================================================================

    @Test
    @DisplayName("M3: legacy mapping rewrites to a canonical binding table; bindings point at the lifted FQNs")
    void legacyRewritesToCanonicalBindingTable() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "Class model::Firm   { legalName: String[1]; } "
                        + "Class model::RawPerson { name: String[1]; } "
                        + "Class model::RawFirm   { legalName: String[1]; } "
                        + "Mapping my::M ( "
                        + "  *model::Person:    Pure { ~src model::RawPerson name: $src.name } "
                        + "   model::Firm:      Pure { ~src model::RawFirm   legalName: $src.legalName } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        var md = canonicalMapping(normalized, "my::M");

        assertEquals(2, md.classBindings().size(), "one binding per class mapping, in order");
        var personBinding = md.classBindings().get(0);
        assertEquals("model::Person", personBinding.classFqn());
        assertEquals(com.legend.model.MappingDefinition.Kind.PURE, personBinding.kind());
        assertTrue(personBinding.root(), "leading * marks the binding root");
        assertEquals("my::M$class$model::Person", personBinding.functionFqn(),
                "binding references the lifted function by its exact FQN");

        var firmBinding = md.classBindings().get(1);
        assertEquals("model::Firm", firmBinding.classFqn());
        assertFalse(firmBinding.root(), "no * => not root");

        // The binding's functionFqn must resolve to an actually-lifted function
        // — binding and lifted function agree by construction.
        for (var b : md.classBindings()) {
            assertTrue(liftedFunctions(normalized).stream()
                            .anyMatch(f -> f.qualifiedName().equals(b.functionFqn())),
                    "every class binding points at a present lifted function: " + b.functionFqn());
        }
    }

    @Test
    @DisplayName("M3: no LegacyMappingDefinition survives into the NormalizedModel (§7.4 guard)")
    void noLegacyMappingSurvivesPhaseE() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "Class model::RawPerson { name: String[1]; } "
                        + "Mapping my::M ( "
                        + "  *model::Person: Pure { ~src model::RawPerson name: $src.name } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        for (PackageableElement el : normalized.elements()) {
            assertFalse(el instanceof LegacyMappingDefinition,
                    "the legacy surface tree must not flow past Phase E — only the "
                  + "canonical binding table does (CLEAN_SHEET_INVERSION §1.5)");
        }
        // And the canonical form IS present.
        assertEquals(1, normalized.elements().stream()
                .filter(e -> e instanceof com.legend.model.MappingDefinition)
                .count(), "exactly the canonical MappingDefinition is carried forward");
    }

    @Test
    @DisplayName("M3: a single-hop association mapping produces an association binding")
    void associationBindingProduced() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Firm   { id: Integer[1]; } "
                        + "Class model::Person { firmId: Integer[1]; } "
                        + "Association model::Person_Firm { "
                        + "  firm: model::Firm[1]; person: model::Person[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_FIRM   (ID INTEGER) "
                        + "  Table T_PERSON (FIRM_ID INTEGER) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Firm: Relational { ~mainTable [db::DB] T_FIRM  id: T_FIRM.ID } "
                        + "  *model::Person: Relational { ~mainTable [db::DB] T_PERSON  firmId: T_PERSON.FIRM_ID } "
                        + "  model::Person_Firm: Relational { AssociationMapping ( "
                        + "    firm: [db::DB] @Person_Firm "
                        + "  ) } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        var md = canonicalMapping(normalized, "my::M");
        assertEquals(1, md.associationBindings().size());
        var ab = md.associationBindings().get(0);
        assertEquals("model::Person_Firm", ab.associationFqn());
        assertEquals("my::M$assoc$model::Person_Firm", ab.predicateFunctionFqn());
        assertTrue(liftedFunctions(normalized).stream()
                        .anyMatch(f -> f.qualifiedName().equals(ab.predicateFunctionFqn())),
                "association binding points at the lifted predicate function");
    }

    @Test
    @DisplayName("M4: a clean-sheet mapping passes through Phase E unchanged (no lift, no legacy)")
    void cleanSheetMappingPassesThroughNormalizeUnchanged() {
        // A function-form mapping parses straight to the canonical binding
        // table (Door 1); its realizing functions are user-authored, so the
        // normalizer lifts NOTHING and the binding table flows through verbatim.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "function acme::funcs::personMapping(): model::Person[*] "
                        + "  { model::Person.all() } "
                        + "Mapping acme::M ( "
                        + "  *model::Person: Pure { acme::funcs::personMapping } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);

        // No lifted (synthesized) mapping functions — the user wrote the body.
        assertTrue(liftedFunctions(normalized).isEmpty(),
                "clean-sheet mappings lift nothing; the function is user-authored");
        // The canonical binding table is carried forward, binding intact.
        var md = canonicalMapping(normalized, "acme::M");
        assertEquals(1, md.classBindings().size());
        assertEquals("model::Person", md.classBindings().get(0).classFqn());
        assertEquals("acme::funcs::personMapping",
                md.classBindings().get(0).functionFqn(),
                "binding still references the user function by FQN");
        // No legacy surface tree anywhere.
        for (PackageableElement el : normalized.elements()) {
            assertFalse(el instanceof LegacyMappingDefinition);
        }
    }

    @Test
    @DisplayName("M5: an inline class binding is lambda-lifted to a function; the binding becomes a Ref")
    void cleanSheetInlineClassBindingIsLifted() {
        // Door 3: the body is an inline pipeline (NOT a named function). Phase E
        // lambda-lifts it to <mapping>$class$<classFqn>(): Class[*] and rewrites
        // the binding to a function ref.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "Mapping acme::M ( "
                        + "  *model::Person: Relational { model::Person.all() } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);

        // The inline body WAS lifted into exactly one function.
        List<FunctionDefinition> lifted = liftedFunctions(normalized);
        assertEquals(1, lifted.size(), "the inline body is lifted to one function");
        FunctionDefinition fn = lifted.get(0);
        assertEquals("acme::M$class$model::Person", fn.qualifiedName());
        assertTrue(fn.parameters().isEmpty(), "a class realizing fn is param-less");
        assertEquals("model::Person",
                ((TypeExpression.NameRef) fn.returnType()).name());
        assertEquals(Multiplicity.Concrete.ZERO_MANY, fn.returnMultiplicity(),
                "returns Class[*]");
        assertEquals(SynthHat.CLASS, fn.synthesizedFrom().hat());

        // The binding now points at the lifted function (no Inline survives).
        var md = canonicalMapping(normalized, "acme::M");
        var b = md.classBindings().get(0);
        assertInstanceOf(Realization.Ref.class, b.realization(),
                "no inline realization survives Phase E");
        assertEquals("acme::M$class$model::Person", b.functionFqn());
    }

    @Test
    @DisplayName("M5: an inline association lambda lifts to a 2-param Boolean predicate typed from the ends")
    void cleanSheetInlineAssociationIsLifted() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { firmId: Integer[1]; } "
                        + "Class model::Firm { id: Integer[1]; } "
                        + "Association model::Person_Firm { "
                        + "  firm: model::Firm[1]; person: model::Person[1]; } "
                        + "Mapping acme::M ( "
                        + "  model::Person_Firm: AssociationMapping { {p, f | $p.firmId == $f.id} } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);

        FunctionDefinition fn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().equals("acme::M$assoc$model::Person_Firm"))
                .findFirst().orElseThrow();
        assertEquals(2, fn.parameters().size(), "predicate takes (source, target)");
        // Param names come from the user's lambda; types from the association ends.
        assertEquals("p", fn.parameters().get(0).name());
        assertEquals("model::Firm",
                ((TypeExpression.NameRef) fn.parameters().get(0).type()).name(),
                "param type from association end1 (Firm)");
        assertEquals("f", fn.parameters().get(1).name());
        assertEquals("model::Person",
                ((TypeExpression.NameRef) fn.parameters().get(1).type()).name(),
                "param type from association end2 (Person)");
        assertEquals("meta::pure::metamodel::type::Boolean", ((TypeExpression.NameRef) fn.returnType()).name());
        assertEquals(SynthHat.ASSOC, fn.synthesizedFrom().hat());

        var md = canonicalMapping(normalized, "acme::M");
        assertInstanceOf(Realization.Ref.class,
                md.associationBindings().get(0).realization());
    }

    @Test
    @DisplayName("M5: inline works for Pure (M2M) bindings too — same lift, kind-agnostic")
    void cleanSheetInlinePureBindingIsLifted() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "Class model::RawPerson { name: String[1]; } "
                        + "Mapping acme::M ( "
                        + "  *model::Person: Pure { model::RawPerson.all() } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition fn = soleSynth(normalized);
        assertEquals("acme::M$class$model::Person", fn.qualifiedName());
        assertEquals(Multiplicity.Concrete.ZERO_MANY, fn.returnMultiplicity());
        // The kind tag stays PURE on the binding; the lift didn't inspect it.
        var b = canonicalMapping(normalized, "acme::M").classBindings().get(0);
        assertEquals(MappingDefinition.Kind.PURE, b.kind());
        assertEquals("acme::M$class$model::Person", b.functionFqn());
    }

    @Test
    @DisplayName("M5: §7.4 guard — no inline realization survives a NormalizedModel")
    void noInlineRealizationSurvivesPhaseE() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "Mapping acme::M ( "
                        + "  *model::Person: Relational { model::Person.all() } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        for (PackageableElement el : normalized.elements()) {
            if (el instanceof MappingDefinition cmd) {
                for (var b : cmd.classBindings()) {
                    assertInstanceOf(Realization.Ref.class, b.realization(),
                            "class binding must be a Ref post-E");
                }
                for (var b : cmd.associationBindings()) {
                    assertInstanceOf(Realization.Ref.class, b.realization(),
                            "association binding must be a Ref post-E");
                }
            }
        }
    }

    // ====================================================================
    // M2M
    // ====================================================================

    @Test
    @DisplayName("m2mSimpleBindings_producesGetAllThenMap")
    void m2mSimpleBindings_producesGetAllThenMap() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { fullName: String[1]; age: Integer[1]; } "
                        + "Class model::RawPerson { firstName: String[1]; lastName: String[1]; age: Integer[1]; } "
                        + "Mapping my::M ( "
                        + "  *model::Person: Pure { "
                        + "    ~src model::RawPerson "
                        + "    fullName: $src.firstName + ' ' + $src.lastName, "
                        + "    age: $src.age "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition fn = soleSynth(normalized);

        // FQN: my::M$Person
        assertEquals("my::M$class$model::Person", fn.qualifiedName());

        // Return type: model::Person [*]
        assertEquals(new TypeExpression.NameRef("model::Person"), fn.returnType());
        assertEquals(Multiplicity.Concrete.ZERO_MANY, fn.returnMultiplicity());

        // Body: map(getAll(model::RawPerson), src | ^model::Person(...))
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        assertEquals("map", mapCall.function());
        assertEquals(2, mapCall.parameters().size());

        AppliedFunction getAll = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("getAll", getAll.function());
        assertEquals(new PackageableElementPtr("model::RawPerson"),
                getAll.parameters().get(0));

        LambdaFunction lambda = (LambdaFunction) mapCall.parameters().get(1);
        assertEquals(List.of(new Variable("src")), lambda.parameters());

        // ^Person(fullName=..., age=...)
        AppliedFunction newCall = (AppliedFunction) sole(lambda.body());
        assertEquals("new", newCall.function());
        assertEquals(new PackageableElementPtr("model::Person"), newCall.parameters().get(0));
        NewInstance ni = (NewInstance) newCall.parameters().get(1);
        assertEquals("model::Person", ni.className());
        assertEquals(List.of("fullName", "age"), List.copyOf(ni.properties().keySet()),
                "Property keys preserve declaration order");

        // Each KeyExpression carries the verbatim parsed expression
        // (isAdd=false for '=' bindings).
        for (KeyExpression ke : ni.properties().values()) {
            assertNotNull(ke.value());
            assertEquals(false, ke.isAdd());
        }

        // Referential identity check: the KeyExpression value should be the
        // EXACT object the parser produced on the PropertyBinding. The
        // normalizer must not rebuild or transform M2M bindings -- those
        // ValueSpecs are already parsed and reference $src, which is what
        // we want in the map() lambda.
        var pcm = (com.legend.model.ClassMapping.Pure)
                firstMapping(parsed).classMappings().get(0);
        for (var pb : pcm.propertyBindings()) {
            assertSame(pb.expression(), ni.properties().get(pb.propertyName()).value(),
                    "M2M PropertyBinding expression must be the same object "
                            + "(no rebuild) in the synth fn body; property="
                            + pb.propertyName());
        }

        // age's value is the bare $src.age property access -- spot check the
        // structural shape (not just identity) so a parser regression that
        // changes shape would also surface here.
        ValueSpecification ageExpr = ni.properties().get("age").value();
        AppliedProperty ageProp = assertInstanceOf(AppliedProperty.class, ageExpr);
        assertEquals("age", ageProp.property());
        assertEquals(new Variable("src"), ageProp.receiver());
    }

    // ====================================================================
    // Relational
    // ====================================================================

    @Test
    @DisplayName("simpleColumnsPM_producesTableRefThenMap")
    void simpleColumnsPM_producesTableRefThenMap() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { firstName: String[1]; lastName: String[1]; } "
                        + "Database db::DB ( Table T_PERSON (FIRST VARCHAR(50), LAST VARCHAR(50)) ) "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    firstName: T_PERSON.FIRST, "
                        + "    lastName:  T_PERSON.LAST "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition fn = soleSynth(normalized);

        // FQN + return type
        assertEquals("my::M$class$model::Person", fn.qualifiedName());
        assertEquals(new TypeExpression.NameRef("model::Person"), fn.returnType());
        assertEquals(Multiplicity.Concrete.ZERO_MANY, fn.returnMultiplicity());

        // Body: map(tableReference("db::DB", "T_PERSON"), row | ^Person(...))
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        assertEquals("map", mapCall.function());

        AppliedFunction tableRef = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("tableReference", tableRef.function());
        assertEquals(2, tableRef.parameters().size());
        // Args are PackageableElementPtr(db) + CString(table) — query-parser
        // parity (H1: TableReferenceChecker serves both surfaces).
        assertEquals(new PackageableElementPtr("db::DB"),
                tableRef.parameters().get(0));
        assertEquals(new com.legend.model.spec.CString("T_PERSON"),
                tableRef.parameters().get(1));

        LambdaFunction lambda = (LambdaFunction) mapCall.parameters().get(1);
        assertEquals(List.of(new Variable("row")), lambda.parameters());

        AppliedFunction newCall = (AppliedFunction) sole(lambda.body());
        assertEquals("new", newCall.function());
        NewInstance ni = (NewInstance) newCall.parameters().get(1);
        assertEquals("model::Person", ni.className());
        assertEquals(List.of("firstName", "lastName"), List.copyOf(ni.properties().keySet()));

        // firstName value is $row.FIRST (AppliedProperty(receiver=row, property=FIRST))
        AppliedProperty firstName = (AppliedProperty) toOneInner(ni.properties().get("firstName").value());
        assertEquals(new Variable("row"), firstName.receiver());
        assertEquals("FIRST", firstName.property());

        AppliedProperty lastName = (AppliedProperty) toOneInner(ni.properties().get("lastName").value());
        assertEquals("LAST", lastName.property());
    }

    // ====================================================================
    // FQN / return-type convention
    // ====================================================================

    @Test
    @DisplayName("synthFunctionFqnFollowsConvention")
    void synthFunctionFqnFollowsConvention() {
        // Class FQN model::Person + mapping FQN pkg::M::PersonMap should
        // produce lifted fn pkg::M::PersonMap$class$model::Person: the full
        // mapping FQN, the $class$ hat segment, and the FULL class FQN (not
        // its simple name — the encoding is injective so distinct classes
        // never collide; docs/CLEAN_SHEET_INVERSION.md §3, SynthFqn.mappingClass).
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "Class model::RawPerson { name: String[1]; } "
                        + "Mapping pkg::M::PersonMap ( "
                        + "  *model::Person: Pure { "
                        + "    ~src model::RawPerson "
                        + "    name: $src.name "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition fn = soleSynth(normalized);
        assertEquals("pkg::M::PersonMap$class$model::Person", fn.qualifiedName(),
                "lifted FQN is <mapping>$class$<full classFqn>");
        assertEquals(SynthHat.CLASS, fn.synthesizedFrom().hat());
        // The hat enum is the SINGLE source of truth for both the provenance
        // tag and the FQN segment — they cannot drift. Pin it: the FQN's middle
        // segment IS hat.segment().
        assertTrue(fn.qualifiedName().contains(
                        "$" + fn.synthesizedFrom().hat().segment() + "$"),
                "the lifted FQN's $-segment must equal the provenance hat's segment() "
                        + "(one source of truth — SynthHat)");
    }

    // ====================================================================
    // Increment 2 - M2M filter
    // ====================================================================

    @Test
    @DisplayName("m2mWithFilter_wrapsGetAllWithFilter")
    void m2mWithFilter_wrapsGetAllWithFilter() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::ActivePerson { name: String[1]; } "
                        + "Class model::RawPerson { name: String[1]; isActive: Boolean[1]; } "
                        + "Mapping my::M ( "
                        + "  *model::ActivePerson: Pure { "
                        + "    ~src model::RawPerson "
                        + "    ~filter $src.isActive == true "
                        + "    name: $src.name "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition fn = soleSynth(normalized);

        // Body: map(filter(getAll(RawPerson), src | <filter>), src | ^ActivePerson(...))
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        assertEquals("map", mapCall.function());

        AppliedFunction filterCall = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("filter", filterCall.function(),
                "M2M filter wraps the getAll source, lives BEFORE the map terminus");

        AppliedFunction getAll = (AppliedFunction) filterCall.parameters().get(0);
        assertEquals("getAll", getAll.function());
        assertEquals(new PackageableElementPtr("model::RawPerson"),
                getAll.parameters().get(0));

        LambdaFunction filterLambda = (LambdaFunction) filterCall.parameters().get(1);
        assertEquals(List.of(new Variable("src")), filterLambda.parameters());

        // The filter body must be the verbatim parsed ValueSpec from the
        // ClassMapping (referential identity).
        var pcm = (com.legend.model.ClassMapping.Pure)
                firstMapping(parsed).classMappings().get(0);
        assertSame(pcm.filter(), sole(filterLambda.body()),
                "Filter expression is the verbatim parsed ValueSpec");
    }

    // ====================================================================
    // Increment 2 - Relational Direct filter
    // ====================================================================

    @Test
    @DisplayName("relationalDirectFilter_wrapsTableRefWithFilter")
    void relationalDirectFilter_wrapsTableRefWithFilter() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (NAME VARCHAR(50), IS_ACTIVE BIT) "
                        + "  Filter ActiveOnly( T_PERSON.IS_ACTIVE = 1 ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~filter ActiveOnly "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name: T_PERSON.NAME "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition fn = soleSynth(normalized);

        // Body: map(filter(tableReference(...), row | $row.IS_ACTIVE == 1), row | ^Person(...))
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        assertEquals("map", mapCall.function());

        AppliedFunction filterCall = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("filter", filterCall.function(),
                "Relational ~filter Direct wraps tableReference");

        AppliedFunction tableRef = (AppliedFunction) filterCall.parameters().get(0);
        assertEquals("tableReference", tableRef.function());

        // The filter lambda binds 'row' and the body is the translated
        // RelationalOperation: equal($row.IS_ACTIVE, 1)
        LambdaFunction filterLambda = (LambdaFunction) filterCall.parameters().get(1);
        assertEquals(List.of(new Variable("row")), filterLambda.parameters());
        AppliedFunction condition = (AppliedFunction) sole(filterLambda.body());
        assertEquals("equal", condition.function(),
                "T_PERSON.IS_ACTIVE = 1 translates to equal(...)");
        assertEquals(2, condition.parameters().size());
        // left = $row.IS_ACTIVE
        AppliedProperty left = (AppliedProperty) condition.parameters().get(0);
        assertEquals("IS_ACTIVE", left.property());
        assertEquals(new Variable("row"), left.receiver());
        // right = 1 (CInteger)
        assertEquals(new com.legend.model.spec.CInteger(1L),
                condition.parameters().get(1));
    }

    @Test
    void relationalFilter_unknownDatabaseThrows() {
        // ~filter [other::DB] ActiveOnly  - the cross-db filter pointer
        // references a database that isn't in the model. Phase E should
        // throw clearly.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "Database db::DB ( Table T_PERSON (NAME VARCHAR(50)) ) "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~filter [other::DB] MissingFilter "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name: T_PERSON.NAME "
                        + "  } "
                        + ")");
        String exMsg = poisonReasons(parsed);
        assertTrue(exMsg.contains("other::DB"),
                () -> "Expected unknown-db error to name the missing db; got: " + exMsg);
    }

    @Test
    void relationalFilter_unknownFilterNameThrows() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "Database db::DB ( Table T_PERSON (NAME VARCHAR(50)) ) "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~filter NonexistentFilter "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name: T_PERSON.NAME "
                        + "  } "
                        + ")");
        String exMsg = poisonReasons(parsed);
        assertTrue(exMsg.contains("NonexistentFilter"),
                () -> "Expected missing-filter error to name the filter; got: " + exMsg);
    }

    // ====================================================================
    // Increment 2 - Class-typed M2M materialization
    // ====================================================================

    @Test
    @DisplayName("m2mClassTypedBinding_emitsNewInstanceCast")
    void m2mClassTypedBinding_emitsNewInstanceCast() {
        // Post-rewrite (R3): a class-typed M2M binding does NOT inline
        // the sibling mapping's body with `map(d | ^Inner(...))`. Instead
        // it emits `^DeptInfo($src.department)` — a positional
        // {@link NewInstanceCast} carrying the FQN of the target class.
        // Sibling materialization is owned by DeptInfo's own synth fn;
        // the lowerer dispatches by FQN. This decouples M2M-class binding
        // emission from the sibling's shape and removes a recursion path.
        ParsedModel parsed = ElementParser.parse(
                "Class model::DeptInfo { name: String[1]; location: String[1]; } "
                        + "Class model::Department { name: String[1]; location: String[1]; } "
                        + "Class model::Employee { firstName: String[1]; department: model::Department[*]; } "
                        + "Class model::StaffWithDept { fullName: String[1]; department: model::DeptInfo[*]; } "
                        + "Mapping my::M ( "
                        + "  *model::StaffWithDept: Pure { "
                        + "    ~src model::Employee "
                        + "    fullName: $src.firstName, "
                        + "    department: $src.department "
                        + "  } "
                        + "  model::DeptInfo: Pure { "
                        + "    ~src model::Department "
                        + "    name: $src.name, "
                        + "    location: $src.location "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);

        // Find the StaffWithDept synth fn (not DeptInfo's).
        FunctionDefinition swdFn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().endsWith("::StaffWithDept"))
                .findFirst().orElseThrow();

        AppliedFunction mapCall = (AppliedFunction) sole(swdFn.body());
        LambdaFunction outerLambda = (LambdaFunction) mapCall.parameters().get(1);
        AppliedFunction newCall = (AppliedFunction) sole(outerLambda.body());
        NewInstance ni = (NewInstance) newCall.parameters().get(1);

        // fullName is a primitive: should be the verbatim parsed $src.firstName
        var fullNameValue = ni.properties().get("fullName").value();
        AppliedProperty fullNameProp = assertInstanceOf(AppliedProperty.class, fullNameValue);
        assertEquals("firstName", fullNameProp.property());
        assertEquals(new Variable("src"), fullNameProp.receiver());

        // department is class-typed: should be NewInstanceCast over $src.department.
        var deptValue = ni.properties().get("department").value();
        com.legend.model.spec.NewInstanceCast deptCast =
                assertInstanceOf(com.legend.model.spec.NewInstanceCast.class, deptValue,
                        "Class-typed M2M binding lowers to NewInstanceCast (^Target($srcExpr))");
        assertEquals("model::DeptInfo", deptCast.className(),
                "NewInstanceCast carries the target class FQN");

        // The cast's source expression is the verbatim parsed
        // PropertyBinding expression ($src.department) — referentially preserved.
        var pcm = (com.legend.model.ClassMapping.Pure)
                firstMapping(parsed).classMappings().stream()
                        .filter(cm -> cm.className().equals("model::StaffWithDept"))
                        .findFirst().orElseThrow();
        var deptPb = pcm.propertyBindings().stream()
                .filter(pb -> pb.propertyName().equals("department"))
                .findFirst().orElseThrow();
        assertSame(deptPb.expression(), deptCast.src(),
                "NewInstanceCast.src is the parsed $src.department, untouched");
    }

    @Test
    void m2mClassTypedBinding_unmappedTargetThrows() {
        // department: $src.department — target type model::DeptInfo — but
        // NO ClassMapping for DeptInfo exists anywhere in the model. Post-R3,
        // M2M class-typed bindings lower to NewInstanceCast(target, ...),
        // which requires the target to have its own synth fn. Without one,
        // the lowerer would emit a dangling reference, so the normalizer
        // refuses at synth time.
        ParsedModel parsed = ElementParser.parse(
                "Class model::DeptInfo { name: String[1]; } "
                        + "Class model::Department { name: String[1]; } "
                        + "Class model::Employee { department: model::Department[*]; } "
                        + "Class model::StaffWithDept { department: model::DeptInfo[*]; } "
                        + "Mapping my::M ( "
                        + "  *model::StaffWithDept: Pure { "
                        + "    ~src model::Employee "
                        + "    department: $src.department "
                        + "  } "
                        + ")");
        String exMsg = poisonReasons(parsed);
        assertTrue(exMsg.contains("model::DeptInfo"),
                () -> "Expected error to name the missing target type; got: " + exMsg);
        assertTrue(exMsg.contains("unmapped class"),
                () -> "Expected error to mention 'unmapped class'; got: " + exMsg);
    }

    @Test
    void m2mClassTypedBinding_mutualReferenceLowersToCasts() {
        // Two M2M classes whose class-typed bindings reference each other:
        //   A.b: $src.b  (b: model::B[1])
        //   B.a: $src.a  (a: model::A[1])
        // Post-R3, neither side recursively materializes the other; both
        // emit `^B($src.b)` / `^A($src.a)` (NewInstanceCast). Dispatch is
        // by FQN at lower time, so structural mutual reference no longer
        // recurses through synth and no cycle exception is thrown.
        ParsedModel parsed = ElementParser.parse(
                "Class model::A { b: model::B[1]; } "
                        + "Class model::B { a: model::A[1]; } "
                        + "Class model::SrcA { b: model::SrcB[1]; } "
                        + "Class model::SrcB { a: model::SrcA[1]; } "
                        + "Mapping my::M ( "
                        + "  *model::A: Pure { ~src model::SrcA b: $src.b } "
                        + "  *model::B: Pure { ~src model::SrcB a: $src.a } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        List<FunctionDefinition> lifted = liftedFunctions(normalized);
        assertEquals(2, lifted.size(),
                "Both A and B get their own synth fn; mutual reference does not recurse");

        for (FunctionDefinition fn : lifted) {
            AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
            LambdaFunction lam = (LambdaFunction) mapCall.parameters().get(1);
            NewInstance ni = (NewInstance)
                    ((AppliedFunction) sole(lam.body())).parameters().get(1);
            // The single property value is a NewInstanceCast over a $src.<prop>
            // — no inlined ^Inner construction.
            com.legend.model.spec.NewInstanceCast cast =
                    assertInstanceOf(com.legend.model.spec.NewInstanceCast.class,
                            ni.properties().values().iterator().next().value(),
                            "Class-typed property lowers to NewInstanceCast");
            assertTrue(cast.className().equals("model::A") || cast.className().equals("model::B"),
                    "Cast targets the property's declared class");
        }
    }

    @Test
    void m2mFunctionCallBody_passesThroughVerbatim() {
        // A non-trivial expression body (function call) must be preserved
        // referentially in the KeyExpression value -- the normalizer must
        // not rebuild parsed M2M ValueSpecs for primitive bindings.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { fullName: String[1]; } "
                        + "Class model::RawPerson { firstName: String[1]; lastName: String[1]; } "
                        + "Mapping my::M ( "
                        + "  *model::Person: Pure { "
                        + "    ~src model::RawPerson "
                        + "    fullName: $src.firstName + ' ' + $src.lastName "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition fn = soleSynth(normalized);
        var pcm = (com.legend.model.ClassMapping.Pure)
                firstMapping(parsed).classMappings().get(0);
        var pb = pcm.propertyBindings().get(0);

        AppliedFunction map = (AppliedFunction) sole(fn.body());
        LambdaFunction lambda = (LambdaFunction) map.parameters().get(1);
        AppliedFunction newCall = (AppliedFunction) sole(lambda.body());
        NewInstance ni = (NewInstance) newCall.parameters().get(1);

        assertSame(pb.expression(), ni.properties().get("fullName").value(),
                "Verbatim AST passthrough for primitive bindings");
    }

    // ====================================================================
    // Increment 3 - Relational join associate chains (multi-hop)
    // ====================================================================

    @Test
    @DisplayName("singleHopJoinTerminalColumn_emitsOneJoin")
    void singleHopJoinTerminalColumn_emitsOneJoin() {
        // Post-R3: chain prelude emits per-hop `join(~alias: tableRef, {s,t|...})`,
        // not the old `associate(...)` form. A single-hop JoinTerminalColumn
        // PM yields one `join` between the main `tableReference` and the
        // terminal `map` lambda.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { firmName: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    firmName: [db::DB] @Person_Firm | T_FIRM.LEGAL_NAME "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition fn = soleSynth(normalized);

        // Body shape: map(join(tableRef, ColSpec, cond), row | ^Person(...))
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        assertEquals("map", mapCall.function());

        AppliedFunction join = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("join", join.function());
        assertEquals(3, join.parameters().size(),
                "join takes (rel, ColSpec, cond)");

        // Upstream is the bare tableReference (no filter, no prior join)
        AppliedFunction upstream = (AppliedFunction) join.parameters().get(0);
        assertEquals("tableReference", upstream.function());

        // ColSpec(~Person_Firm: tableRef(db::DB, T_FIRM))
        com.legend.model.spec.ColSpec binding =
                (com.legend.model.spec.ColSpec) join.parameters().get(1);
        assertEquals("Person_Firm", binding.name(),
                "single-hop alias is the join name");
        LambdaFunction targetLambda = binding.function1();
        assertEquals(List.of(), targetLambda.parameters(),
                "ColSpec target lambda is 0-param (engine convention)");
        AppliedFunction targetTableRef = (AppliedFunction) sole(targetLambda.body());
        assertEquals("tableReference", targetTableRef.function());
        assertEquals(new PackageableElementPtr("db::DB"),
                targetTableRef.parameters().get(0));
        assertEquals(new com.legend.model.spec.CString("T_FIRM"),
                targetTableRef.parameters().get(1));

        // Condition lambda: {s, t | $s.FIRM_ID == $t.ID}
        LambdaFunction condLambda = (LambdaFunction) join.parameters().get(2);
        assertEquals(List.of(new Variable("s"), new Variable("t")),
                condLambda.parameters());
        AppliedFunction cond = (AppliedFunction) sole(condLambda.body());
        assertEquals("equal", cond.function());
        AppliedProperty left = (AppliedProperty) cond.parameters().get(0);
        assertEquals("FIRM_ID", left.property());
        assertEquals(new Variable("s"), left.receiver());
        AppliedProperty right = (AppliedProperty) cond.parameters().get(1);
        assertEquals("ID", right.property());
        assertEquals(new Variable("t"), right.receiver());

        // Project lambda: ^Person(firmName = $row.Person_Firm.LEGAL_NAME)
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        AppliedFunction newCall = (AppliedFunction) sole(projectLambda.body());
        NewInstance ni = (NewInstance) newCall.parameters().get(1);
        AppliedProperty firmName = (AppliedProperty) toOneInner(ni.properties().get("firmName").value());
        assertEquals("LEGAL_NAME", firmName.property());
        AppliedProperty firmAlias = (AppliedProperty) firmName.receiver();
        assertEquals("Person_Firm", firmAlias.property());
        assertEquals(new Variable("row"), firmAlias.receiver(),
                "Terminal column is $row.<alias>.<COL>");
    }

    @Test
    @DisplayName("multiHopJoinTerminalColumn_emitsJoinPerHopWithChainedSourcePaths")
    void multiHopJoinTerminalColumn_emitsJoinPerHopWithChainedSourcePaths() {
        // 2-hop: orgName: @Person_Firm > @Firm_Org | T_ORG.NAME
        // After hop 1, $s sees T_PERSON cols at top and Person_Firm as a
        // sub-row for T_FIRM. So hop 2's condition T_FIRM.ORG_ID = T_ORG.ID
        // becomes $s.Person_Firm.ORG_ID == $t.ID.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { orgName: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, ORG_ID INTEGER) "
                        + "  Table T_ORG    (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Firm_Org   ( T_FIRM.ORG_ID   = T_ORG.ID  ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    orgName: [db::DB] @Person_Firm > @Firm_Org | T_ORG.NAME "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition fn = soleSynth(normalized);

        // Body: map(join(join(tableRef, ColSpec1, cond1), ColSpec2, cond2), row | ^Person(...))
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction outerJoin = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("join", outerJoin.function(),
                "outer join is hop 2");
        AppliedFunction innerJoin = (AppliedFunction) outerJoin.parameters().get(0);
        assertEquals("join", innerJoin.function(),
                "inner join is hop 1");
        AppliedFunction tableRef = (AppliedFunction) innerJoin.parameters().get(0);
        assertEquals("tableReference", tableRef.function());

        // Hop 1: alias Person_Firm, target T_FIRM, cond $s.FIRM_ID = $t.ID
        com.legend.model.spec.ColSpec h1Spec =
                (com.legend.model.spec.ColSpec) innerJoin.parameters().get(1);
        assertEquals("Person_Firm", h1Spec.name());
        AppliedFunction h1Target = (AppliedFunction) sole(h1Spec.function1().body());
        assertEquals(new com.legend.model.spec.CString("T_FIRM"),
                h1Target.parameters().get(1));

        // Hop 2: alias Person_Firm__Firm_Org, target T_ORG
        com.legend.model.spec.ColSpec h2Spec =
                (com.legend.model.spec.ColSpec) outerJoin.parameters().get(1);
        assertEquals("Person_Firm__Firm_Org", h2Spec.name());
        AppliedFunction h2Target = (AppliedFunction) sole(h2Spec.function1().body());
        assertEquals(new com.legend.model.spec.CString("T_ORG"),
                h2Target.parameters().get(1));

        // Hop 2 condition: $s.Person_Firm.ORG_ID == $t.ID
        // This is the key multi-hop invariant: the source at hop 2 sees
        // hop 1's target as a sub-row reached through the hop 1 alias.
        LambdaFunction h2Cond = (LambdaFunction) outerJoin.parameters().get(2);
        AppliedFunction h2Eq = (AppliedFunction) sole(h2Cond.body());
        assertEquals("equal", h2Eq.function());
        // Left: $s.Person_Firm.ORG_ID
        AppliedProperty left = (AppliedProperty) h2Eq.parameters().get(0);
        assertEquals("ORG_ID", left.property());
        AppliedProperty leftSub = (AppliedProperty) left.receiver();
        assertEquals("Person_Firm", leftSub.property(),
                "Hop 2 cond's left side reaches T_FIRM via $s.Person_Firm");
        assertEquals(new Variable("s"), leftSub.receiver());
        // Right: $t.ID
        AppliedProperty right = (AppliedProperty) h2Eq.parameters().get(1);
        assertEquals("ID", right.property());
        assertEquals(new Variable("t"), right.receiver());

        // Project terminal: $row.orgName_h2.NAME
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        AppliedFunction newCall = (AppliedFunction) sole(projectLambda.body());
        NewInstance ni = (NewInstance) newCall.parameters().get(1);
        AppliedProperty orgName = (AppliedProperty) toOneInner(ni.properties().get("orgName").value());
        assertEquals("NAME", orgName.property());
        AppliedProperty orgAlias = (AppliedProperty) orgName.receiver();
        assertEquals("Person_Firm__Firm_Org", orgAlias.property(),
                "Project pulls from the LAST hop's alias");
        assertEquals(new Variable("row"), orgAlias.receiver());
    }

    @Test
    @DisplayName("selfJoinViaTargetColumnRef_targetTableEqualsSourceTable")
    void selfJoinViaTargetColumnRef_targetTableEqualsSourceTable() {
        // Person joined to Person via MANAGER_ID using {target} marker.
        // The condition T_PERSON.MGR_ID = {target}.ID is a self-join.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { managerId: Integer[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, MGR_ID INTEGER) "
                        + "  Join Person_Manager( T_PERSON.MGR_ID = {target}.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    managerId: [db::DB] @Person_Manager | T_PERSON.ID "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition fn = soleSynth(normalized);

        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction joinCall = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("join", joinCall.function());

        // Target table of the self-join is T_PERSON (same as source).
        com.legend.model.spec.ColSpec spec =
                (com.legend.model.spec.ColSpec) joinCall.parameters().get(1);
        AppliedFunction targetTableRef = (AppliedFunction) sole(spec.function1().body());
        assertEquals(new com.legend.model.spec.CString("T_PERSON"),
                targetTableRef.parameters().get(1),
                "Self-join's target table is the same as the source");

        // Condition: $s.MGR_ID == $t.ID. The {target}.ID translates to $t.ID
        // (TargetColumnRef -> targetVar), and T_PERSON.MGR_ID translates to
        // $s.MGR_ID (the source mapping for T_PERSON wins).
        LambdaFunction cond = (LambdaFunction) joinCall.parameters().get(2);
        AppliedFunction eq = (AppliedFunction) sole(cond.body());
        AppliedProperty left = (AppliedProperty) eq.parameters().get(0);
        assertEquals("MGR_ID", left.property());
        assertEquals(new Variable("s"), left.receiver(),
                "T_PERSON.MGR_ID resolves to source side $s");
        AppliedProperty right = (AppliedProperty) eq.parameters().get(1);
        assertEquals("ID", right.property());
        assertEquals(new Variable("t"), right.receiver(),
                "{target}.ID resolves to target side $t (NOT $s, despite same table)");
    }

    // ====================================================================
    // Increment 4 - Class-typed Relational Join PM materialization
    // ====================================================================

    @Test
    @DisplayName("relationalJoinPM_classTyped_mapped_emitsLegacyNavigateStep")
    void relationalJoinPM_classTyped_mapped_emitsLegacyNavigateStep() {
        // Person.firm (class-typed model::Firm[1]) with Firm MAPPED:
        // emits a pipeline `legacyNavigate(~firm: getAll(Firm), {s,t | <cond>})`
        // step (symmetric to clean-sheet navigate, doc §5.4.3). The
        // physical-column join condition lives verbatim in the lambda.
        // The terminal map reads $row.firm — no +propFk lift, no
        // top-level legacyAssociate.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Firm   { legalName: String[1]; revenue: Integer[1]; } "
                        + "Class model::Person { name: String[1]; firm: model::Firm[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, NAME VARCHAR(50), FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50), REVENUE INTEGER) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name: T_PERSON.NAME, "
                        + "    firm: [db::DB] @Person_Firm "
                        + "  } "
                        + "  model::Firm: Relational { "
                        + "    ~mainTable [db::DB] T_FIRM "
                        + "    legalName: T_FIRM.LEGAL_NAME, "
                        + "    revenue:   T_FIRM.REVENUE "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);

        // Find the Person synth fn (not Firm's).
        FunctionDefinition personFn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().endsWith("::Person"))
                .findFirst().orElseThrow();

        // Top-level is map(legacyNavigate(...), row | ^Person(...)).
        AppliedFunction mapCall = (AppliedFunction) sole(personFn.body());
        assertEquals("map", mapCall.function());

        // map's source is the legacyNavigate pipeline step.
        AppliedFunction nav = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("legacyNavigate", nav.function(),
                "Class-typed Join PM with mapped target emits a legacyNavigate step");
        assertEquals(4, nav.parameters().size(),
                "legacyNavigate takes (rel, ColSpec, targetRows, cond)");

        // Upstream of legacyNavigate is the bare tableReference.
        AppliedFunction upstream = (AppliedFunction) nav.parameters().get(0);
        assertEquals("tableReference", upstream.function());

        // ColSpec(~firm: {| getAll(model::Firm)}) — slot named after the property.
        com.legend.model.spec.ColSpec slot =
                (com.legend.model.spec.ColSpec) nav.parameters().get(1);
        assertEquals("firm", slot.name(),
                "legacyNavigate slot is the PM property name");
        LambdaFunction slotLambda = slot.function1();
        assertEquals(List.of(), slotLambda.parameters(),
                "slot body is a 0-param lambda");
        AppliedFunction getAll = (AppliedFunction) sole(slotLambda.body());
        assertEquals("getAll", getAll.function(),
                "slot materializes the target class via getAll(Firm)");
        PackageableElementPtr firmPtr = (PackageableElementPtr) sole(getAll.parameters());
        assertEquals("model::Firm", firmPtr.fullPath());

        // Condition lambda: {s, t | $s.FIRM_ID == $t.ID}.
        LambdaFunction condLambda = (LambdaFunction) nav.parameters().get(3);
        assertEquals(2, condLambda.parameters().size());
        AppliedFunction eq = (AppliedFunction) sole(condLambda.body());
        assertEquals("equal", eq.function());
        AppliedProperty sFk = (AppliedProperty) eq.parameters().get(0);
        assertEquals("FIRM_ID", sFk.property(), "source side reads the physical FK column");
        AppliedProperty tId = (AppliedProperty) eq.parameters().get(1);
        assertEquals("ID", tId.property(), "target side reads the target PK column");

        // Terminal ctor: name + firm (firm = $row.firm). No +propFk lift.
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        AppliedFunction newCall = (AppliedFunction) sole(projectLambda.body());
        NewInstance personNi = (NewInstance) newCall.parameters().get(1);
        assertEquals("model::Person", personNi.className());
        assertEquals(List.of("name", "firm"),
                List.copyOf(personNi.properties().keySet()),
                "Ctor has primitive `name` and class-typed `firm` (read from the slot)");

        KeyExpression firmKe = personNi.properties().get("firm");
        assertFalse(firmKe.isLocal(), "firm is a public property, not +local");
        AppliedProperty firmRead = (AppliedProperty) toOneInner(firmKe.value());
        assertEquals("firm", firmRead.property(), "firm reads $row.firm (the navigate slot)");
        assertEquals(new Variable("row"), firmRead.receiver());
    }

    // [DELETED 2026-05-17] Five tests asserting throws on inner-mapping
    // mismatches (missing sibling, mainTable mismatch, M2M sibling, inner
    // Join PM, inner filter). The redesign no longer walks sibling
    // mappings for class-typed Join PMs — Firm's own synth function owns
    // Firm-shaped construction. None of these conditions throw any more.
    // See progress.txt "MappingNormalizer Phase-E Inc 4 redesign".

    // ====================================================================
    // Increment 4 - Embedded PMs (inline sub-properties read from main table)
    // ====================================================================

    @Test
    @DisplayName("embeddedPM_materializesInnerClassFromOuterTableScope")
    void embeddedPM_materializesInnerClassFromOuterTableScope() {
        // firm ( legalName: T_PERSON.FIRM_NAME, employeeCount: T_PERSON.FIRM_COUNT )
        // produces:
        // firm = ^model::Firm(
        //   legalName     = $row.FIRM_NAME,
        //   employeeCount = $row.FIRM_COUNT)
        // Sub-PMs read from OUTER mainTable (no associate emitted).
        ParsedModel parsed = ElementParser.parse(
                "Class model::Firm   { legalName: String[1]; employeeCount: Integer[1]; } "
                        + "Class model::Person { name: String[1]; firm: model::Firm[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (NAME VARCHAR(50), FIRM_NAME VARCHAR(50), FIRM_COUNT INTEGER) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name: T_PERSON.NAME, "
                        + "    firm ( "
                        + "      legalName:     T_PERSON.FIRM_NAME, "
                        + "      employeeCount: T_PERSON.FIRM_COUNT "
                        + "    ) "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // No associates -- Embedded reads from outer scope.
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction underneath = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("tableReference", underneath.function(),
                "Embedded PMs do NOT emit associates; main table is the direct upstream");

        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        NewInstance personNi = (NewInstance) ((AppliedFunction) sole(projectLambda.body()))
                .parameters().get(1);

        // firm value is ^Firm(...).
        AppliedFunction firmNew = (AppliedFunction) personNi.properties().get("firm").value();
        assertEquals("new", firmNew.function());
        assertEquals(new PackageableElementPtr("model::Firm"), firmNew.parameters().get(0));
        NewInstance firmNi = (NewInstance) firmNew.parameters().get(1);
        assertEquals("model::Firm", firmNi.className());
        assertEquals(List.of("legalName", "employeeCount"),
                List.copyOf(firmNi.properties().keySet()));

        // legalName reads $row.FIRM_NAME (outer table, NOT a join alias).
        AppliedProperty legalName = (AppliedProperty) toOneInner(firmNi.properties().get("legalName").value());
        assertEquals("FIRM_NAME", legalName.property());
        assertEquals(new Variable("row"), legalName.receiver(),
                "Embedded sub-PM reads from outer $row, not a join alias");

        AppliedProperty empCount = (AppliedProperty) toOneInner(firmNi.properties().get("employeeCount").value());
        assertEquals("FIRM_COUNT", empCount.property());
        assertEquals(new Variable("row"), empCount.receiver());
    }

    @Test
    void embeddedPM_innerJoinSubPM_throws() {
        // firm ( legalName: T_PERSON.FIRM_NAME, owner: @Person_Other )
        // The Embedded contains a Join PM sub-PM, which would need a
        // hoisted associate -- not yet supported in value-position embeds.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Other  { name: String[1]; } "
                        + "Class model::Firm   { legalName: String[1]; owner: model::Other[1]; } "
                        + "Class model::Person { firm: model::Firm[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (FIRM_NAME VARCHAR(50), OTHER_ID INTEGER) "
                        + "  Table T_OTHER  (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Other( T_PERSON.OTHER_ID = T_OTHER.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    firm ( "
                        + "      legalName: T_PERSON.FIRM_NAME, "
                        + "      owner: [db::DB] @Person_Other "
                        + "    ) "
                        + "  } "
                        + ")");
        String exMsg = poisonReasons(parsed);
        assertTrue(exMsg.contains("Embedded sub-PM"),
                () -> "Expected nested-Join-in-Embedded error; got: " + exMsg);
    }

    @Test
    @DisplayName("inlineEmbeddedPM_splicesSiblingMappingsPMs")
    void inlineEmbeddedPM_splicesSiblingMappingsPMs() {
        // broker() Inline[broker_set]   - splice Broker[broker_set]'s PMs
        // as if they were sub-PMs of an Embedded named "broker".
        ParsedModel parsed = ElementParser.parse(
                "Class model::Broker { name: String[1]; license: String[1]; } "
                        + "Class model::Person { broker: model::Broker[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (BROKER_NAME VARCHAR(50), BROKER_LICENSE VARCHAR(50)) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    broker() Inline[broker_set] "
                        + "  } "
                        + "  model::Broker[broker_set]: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name:    T_PERSON.BROKER_NAME, "
                        + "    license: T_PERSON.BROKER_LICENSE "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition personFn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().endsWith("::Person"))
                .findFirst().orElseThrow();
        AppliedFunction mapCall = (AppliedFunction) sole(personFn.body());
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        NewInstance personNi = (NewInstance) ((AppliedFunction) sole(projectLambda.body()))
                .parameters().get(1);

        AppliedFunction brokerNew = (AppliedFunction) personNi.properties().get("broker").value();
        assertEquals("new", brokerNew.function());
        assertEquals(new PackageableElementPtr("model::Broker"), brokerNew.parameters().get(0));
        NewInstance brokerNi = (NewInstance) brokerNew.parameters().get(1);
        assertEquals("model::Broker", brokerNi.className());
        assertEquals(List.of("name", "license"),
                List.copyOf(brokerNi.properties().keySet()),
                "InlineEmbedded splices the sibling Broker mapping's PMs in declaration order");

        // Sub-PM values read from the OUTER $row (sibling's mainTable matches outer).
        AppliedProperty nameRef = (AppliedProperty) toOneInner(brokerNi.properties().get("name").value());
        assertEquals("BROKER_NAME", nameRef.property());
        assertEquals(new Variable("row"), nameRef.receiver());
    }

    @Test
    void inlineEmbeddedPM_unknownSetId_throws() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Broker { name: String[1]; } "
                        + "Class model::Person { broker: model::Broker[1]; } "
                        + "Database db::DB ( Table T_PERSON (NAME VARCHAR(50)) ) "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    broker() Inline[no_such_set] "
                        + "  } "
                        + ")");
        String exMsg = poisonReasons(parsed);
        assertTrue(exMsg.contains("no_such_set"),
                () -> "Expected unknown-setId error; got: " + exMsg);
    }

    // ====================================================================
    // Increment 4 - JoinMediated filter
    // ====================================================================

    @Test
    @DisplayName("joinMediatedFilter_sharesJoinAliasWithPMs")
    void joinMediatedFilter_sharesJoinAliasWithPMs() {
        // Position 2: a JoinMediated filter's chain is named by the join
        // name (Person_Firm), the SAME alias a PM using @Person_Firm
        // would emit. The chain join is shared. No reserved __filter
        // prefix.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (NAME VARCHAR(50), FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, IS_ACTIVE BIT) "
                        + "  Filter ActiveFirms( T_FIRM.IS_ACTIVE = 1 ) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~filter [db::DB] @Person_Firm | ActiveFirms "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name: T_PERSON.NAME "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Pipeline: map(filter(join(tableRef, ColSpec, cond), <lambda>), ^Person)
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction filterCall = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("filter", filterCall.function());

        AppliedFunction joinCall = (AppliedFunction) filterCall.parameters().get(0);
        assertEquals("join", joinCall.function());
        com.legend.model.spec.ColSpec spec =
                (com.legend.model.spec.ColSpec) joinCall.parameters().get(1);
        assertEquals("Person_Firm", spec.name(),
                "JoinMediated filter alias = join name (shared with any PM "
                        + "going through @Person_Firm)");

        AppliedFunction tableRef = (AppliedFunction) joinCall.parameters().get(0);
        assertEquals("tableReference", tableRef.function());

        // Filter condition: $row.Person_Firm.IS_ACTIVE == 1
        LambdaFunction filterLambda = (LambdaFunction) filterCall.parameters().get(1);
        assertEquals(List.of(new Variable("row")), filterLambda.parameters());
        AppliedFunction eq = (AppliedFunction) sole(filterLambda.body());
        assertEquals("equal", eq.function());
        AppliedProperty leftCol = (AppliedProperty) eq.parameters().get(0);
        assertEquals("IS_ACTIVE", leftCol.property());
        AppliedProperty filterAlias = (AppliedProperty) leftCol.receiver();
        assertEquals("Person_Firm", filterAlias.property(),
                "Filter's condition resolves T_FIRM through the shared "
                        + "Person_Firm alias, no reserved __filter namespace");
        assertEquals(new Variable("row"), filterAlias.receiver());
    }

    // ====================================================================
    // Increment 4 - distinct, EnumeratedColumn, LocalProperty, OtherwiseEmbedded
    // ====================================================================

    @Test
    @DisplayName("distinct_appendsDistinctBeforeMap")
    void distinct_appendsDistinctBeforeMap() {
        // ~distinct deduplicates the joined rowset BEFORE materialization,
        // not the class collection AFTER. Pipeline: tableRef -> distinct -> map.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "Database db::DB ( Table T_PERSON (NAME VARCHAR(50)) ) "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~distinct "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name: T_PERSON.NAME "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction distinctCall = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("distinct", distinctCall.function(),
                "distinct() sits between source pipeline and the terminal map()");
        assertEquals(1, distinctCall.parameters().size(),
                "distinct() with no args (zero-arg overload) over the relation");
        // engine semantics: DISTINCT over the MAPPED columns — the source
        // narrows to a select of exactly the consumed columns first (the
        // table's unmapped PK would otherwise defeat the dedup)
        AppliedFunction select = (AppliedFunction) distinctCall.parameters().get(0);
        assertEquals("select", select.function(),
                "~distinct dedups the MAPPED columns: select narrows first");
        AppliedFunction tableRef = (AppliedFunction) select.parameters().get(0);
        assertEquals("tableReference", tableRef.function());
    }

    @Test
    @DisplayName("enumeratedColumn_inlinesEnumerationMappingAsIfChain")
    void enumeratedColumn_inlinesEnumerationMappingAsIfChain() {
        // Position 2 redesign: EnumeratedColumn is inline-expanded to a
        // nested if/equal chain reading $row.<COL> with each source value
        // tested in turn. No 'enumValue' native; the lookup is structural.
        // Shape for two-value enum with one source per value:
        //   if(equal($row.STATUS, 'A'), {| app::Status.Active},
        //   if(equal($row.STATUS, 'I'), {| app::Status.Inactive},
        //   {| []}))
        ParsedModel parsed = ElementParser.parse(
                "Enum app::Status { Active, Inactive } "
                        + "Class model::Person { status: app::Status[1]; } "
                        + "Database db::DB ( Table T_PERSON (STATUS VARCHAR(50)) ) "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    status: EnumerationMapping StatusMapping : T_PERSON.STATUS "
                        + "  } "
                        + "  app::Status: EnumerationMapping StatusMapping { "
                        + "    Active:   'A', "
                        + "    Inactive: 'I' "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        NewInstance ni = (NewInstance) ((AppliedFunction) sole(projectLambda.body()))
                .parameters().get(1);

        // Outer: if(equal($row.STATUS, 'A'), {| Active}, <else>)
        AppliedFunction outerIf = (AppliedFunction) toOneInner(ni.properties().get("status").value());
        assertEquals("if", outerIf.function(),
                "EnumeratedColumn inlines as nested if(...) chain");
        assertEquals(3, outerIf.parameters().size());

        AppliedFunction outerEq = (AppliedFunction) outerIf.parameters().get(0);
        assertEquals("equal", outerEq.function());
        AppliedProperty colRefA = (AppliedProperty) outerEq.parameters().get(0);
        assertEquals("STATUS", colRefA.property());
        assertEquals(new Variable("row"), colRefA.receiver());
        assertEquals(new com.legend.model.spec.CString("A"),
                outerEq.parameters().get(1),
                "First branch tests against source value 'A'");

        LambdaFunction thenA = (LambdaFunction) outerIf.parameters().get(1);
        assertEquals(List.of(), thenA.parameters());
        com.legend.model.spec.EnumValue activeRef =
                (com.legend.model.spec.EnumValue) sole(thenA.body());
        assertEquals("app::Status", activeRef.fullPath());
        assertEquals("Active", activeRef.value());

        // Inner: if(equal($row.STATUS, 'I'), {| Inactive}, {| []})
        LambdaFunction elseOuter = (LambdaFunction) outerIf.parameters().get(2);
        AppliedFunction innerIf = (AppliedFunction) sole(elseOuter.body());
        assertEquals("if", innerIf.function());
        AppliedFunction innerEq = (AppliedFunction) innerIf.parameters().get(0);
        assertEquals(new com.legend.model.spec.CString("I"),
                innerEq.parameters().get(1),
                "Second branch tests against 'I'");
        LambdaFunction thenI = (LambdaFunction) innerIf.parameters().get(1);
        com.legend.model.spec.EnumValue inactiveRef =
                (com.legend.model.spec.EnumValue) sole(thenI.body());
        assertEquals("Inactive", inactiveRef.value());

        // Final else: empty collection (no match).
        LambdaFunction defaultBranch = (LambdaFunction) innerIf.parameters().get(2);
        com.legend.model.spec.PureCollection emptyColl =
                (com.legend.model.spec.PureCollection) sole(defaultBranch.body());
        assertEquals(List.of(), emptyColl.values());
    }

    @Test
    @DisplayName("localProperty_wrappingColumn_behavesLikeColumn")
    void localProperty_wrappingColumn_behavesLikeColumn() {
        // +localTag: String[1]: T_PERSON.TAG produces the same value
        // expression as a Column PM (just with a different name + declared
        // type, which is parser-level metadata).
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "Database db::DB ( Table T_PERSON (NAME VARCHAR(50), EXTRA VARCHAR(50)) ) "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name: T_PERSON.NAME, "
                        + "    +localTag: String[1]: T_PERSON.EXTRA "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        NewInstance ni = (NewInstance) ((AppliedFunction) sole(projectLambda.body()))
                .parameters().get(1);
        // Both keys present in declaration order.
        assertEquals(List.of("name", "localTag"),
                List.copyOf(ni.properties().keySet()));
        // localTag value: $row.EXTRA (translates as a Column would)
        AppliedProperty localTag = (AppliedProperty) ni.properties().get("localTag").value();
        assertEquals("EXTRA", localTag.property());
        assertEquals(new Variable("row"), localTag.receiver());
    }

    @Test
    @DisplayName("localProperty_wrappingExpression_behavesLikeExpression")
    void localProperty_wrappingExpression_behavesLikeExpression() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "Database db::DB ( Table T_PERSON (NAME VARCHAR(50), A VARCHAR(50), B VARCHAR(50)) ) "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name: T_PERSON.NAME, "
                        + "    +computed: String[1]: concat(T_PERSON.A, ' ', T_PERSON.B) "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        NewInstance ni = (NewInstance) ((AppliedFunction) sole(projectLambda.body()))
                .parameters().get(1);
        List<ValueSpecification> computed = plusChain(
                ni.properties().get("computed").value(), 3);
        assertEquals(3, computed.size(),
                "LocalProperty wrapping Expression unwraps to the Expression's translation (concat -> plus-chain)");
    }

    // ====================================================================
    // R4.5 — OtherwiseEmbedded co-named-slot dispatch-merge
    //
    // Inline-cached sub-properties live on the parent's row (partial
    // ^Inner constructor field co-named with the slot). The fallback's
    // FK lifts to a +<prop>Fk +local and a top-level legacyAssociate
    // naming the SAME property slot. The lowerer treats the co-named
    // contributions as dispatch-merge (D3 / R4.5).
    // ====================================================================

    @Test
    @DisplayName("otherwiseEmbedded_inlineAndJoinedProperties (R4.5 main case)")
    void otherwiseEmbedded_inlineAndJoinedProperties() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Firm   { legalName: String[1]; } "
                        + "Class model::Person { name: String[1]; firm: model::Firm[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (NAME VARCHAR(50), FIRM_ID INTEGER, FIRM_NAME VARCHAR(50)) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Firm[firm_set1]: Relational { "
                        + "    ~mainTable [db::DB] T_FIRM "
                        + "    legalName: T_FIRM.LEGAL_NAME "
                        + "  } "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name: T_PERSON.NAME, "
                        + "    firm ( "
                        + "      legalName: T_PERSON.FIRM_NAME "
                        + "    ) Otherwise ([firm_set1]: [db::DB] @Person_Firm) "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition personFn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().endsWith("::Person"))
                .findFirst().orElseThrow();

        // New shape: the OE fallback emits a legacyNavigate(~firm) pipeline
        // step; the terminal map composes otherwise(^Firm(<inline>), $row.firm).
        AppliedFunction nav = navStep(personFn);
        assertEquals("firm",
                ((com.legend.model.spec.ColSpec) nav.parameters().get(1)).name(),
                "OE fallback emits a legacyNavigate slot named after the property");

        // Condition: {s, t | $s.FIRM_ID == $t.ID}.
        LambdaFunction cond = (LambdaFunction) nav.parameters().get(3);
        AppliedFunction condEq = (AppliedFunction) sole(cond.body());
        assertEquals("equal", condEq.function());
        assertEquals("FIRM_ID", propOf(condEq.parameters().get(0)));
        assertEquals("s", recvOf(condEq.parameters().get(0)));
        assertEquals("ID", propOf(condEq.parameters().get(1)));

        // Ctor: name + firm; firm = otherwise(^Firm(legalName=$row.FIRM_NAME), $row.firm).
        NewInstance ni = ctorOf(personFn);
        assertEquals(List.of("name", "firm"), List.copyOf(ni.properties().keySet()),
                "Ctor declares the OE slot ('firm'); no +propFk carrier");
        AppliedFunction otherwiseCall = (AppliedFunction) ni.properties().get("firm").value();
        assertEquals("otherwise", otherwiseCall.function(),
                "OE composes otherwise(partial ^Firm, fallback slot)");

        // First arg: partial ^Firm with ONLY the inline-cached legalName.
        AppliedFunction firmNewCall = (AppliedFunction) otherwiseCall.parameters().get(0);
        assertEquals("new", firmNewCall.function());
        NewInstance partialFirm = (NewInstance) firmNewCall.parameters().get(1);
        assertEquals(List.of("legalName"),
                List.copyOf(partialFirm.properties().keySet()),
                "Partial inline ^Firm carries ONLY the embedded (inline-cached) properties");
        AppliedProperty legalNameInline =
                (AppliedProperty) toOneInner(partialFirm.properties().get("legalName").value());
        assertEquals("FIRM_NAME", legalNameInline.property(),
                "Inline-cached property reads from the parent's row (denormalized column)");
        assertEquals(new Variable("row"), legalNameInline.receiver());

        // Second arg: the fallback reads $row.firm (the legacyNavigate slot).
        AppliedProperty fallback = (AppliedProperty) otherwiseCall.parameters().get(1);
        assertEquals("firm", fallback.property(),
                "Fallback reads the navigate slot $row.firm");
        assertEquals(new Variable("row"), fallback.receiver());
    }

    @Test
    @DisplayName("otherwiseEmbedded_unmappedTargetClassThrows (R4.5)")
    void otherwiseEmbedded_unmappedTargetClassThrows() {
        // model::Firm is referenced by the OE PM but is NOT itself mapped
        // in this LegacyMappingDefinition. Lifting the fallback to a class-level
        // association requires Firm to have a class mapping (so Firm.all()
        // can be resolved).
        ParsedModel parsed = ElementParser.parse(
                "Class model::Firm   { legalName: String[1]; } "
                        + "Class model::Person { name: String[1]; firm: model::Firm[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (NAME VARCHAR(50), FIRM_ID INTEGER, FIRM_NAME VARCHAR(50)) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name: T_PERSON.NAME, "
                        + "    firm ( "
                        + "      legalName: T_PERSON.FIRM_NAME "
                        + "    ) Otherwise ([firm_set1]: [db::DB] @Person_Firm) "
                        + "  } "
                        + ")");
        // per-class fault isolation: recorded as a POISON, raised at use
        ModelBuilder mb = ModelBuilder.from(new com.legend.model.ParsedModel(
                parsed.elements(), parsed.imports()));
        MappingNormalizer.normalize(parsed, mb);
        String reason = mb.mappingPoisons.get("my::M::model::Person");
        assertTrue(reason != null && reason.contains("model::Firm"),
                () -> "Expected a poisoned binding naming the unmapped target class; got: " + reason);
    }

    @Test
    @DisplayName("otherwiseEmbedded_multiHopFallbackSupported (R4.5)")
    void otherwiseEmbedded_multiHopFallbackSupported() {
        // Multi-hop OE fallback chains are supported by the unified
        // emitJoinChain path: the intermediate hop is emitted as a clean
        // join() step and the final hop as a legacyNavigate(~firm). The
        // terminal map composes otherwise(^Firm(<inline>), $row.firm).
        ParsedModel parsed = ElementParser.parse(
                "Class model::Org    { name: String[1]; } "
                        + "Class model::Firm   { org: model::Org[1]; legalName: String[1]; } "
                        + "Class model::Person { name: String[1]; firm: model::Firm[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (NAME VARCHAR(50), FIRM_ID INTEGER, FIRM_NAME VARCHAR(50)) "
                        + "  Table T_FIRM   (ID INTEGER, ORG_ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Table T_ORG    (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Firm_Org  ( T_FIRM.ORG_ID = T_ORG.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Firm[firm_set1]: Relational { "
                        + "    ~mainTable [db::DB] T_FIRM "
                        + "    legalName: T_FIRM.LEGAL_NAME "
                        + "  } "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name: T_PERSON.NAME, "
                        + "    firm ( "
                        + "      legalName: T_PERSON.FIRM_NAME "
                        + "    ) Otherwise ([firm_set1]: [db::DB] @Person_Firm > @Firm_Org) "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition personFn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().endsWith("::Person"))
                .findFirst().orElseThrow();
        // Final hop emits legacyNavigate(~firm); the single intermediate
        // hop (Person_Firm) is a join beneath it.
        AppliedFunction nav = navStep(personFn);
        assertEquals("firm",
                ((com.legend.model.spec.ColSpec) nav.parameters().get(1)).name());
        AppliedFunction intermediate = (AppliedFunction) nav.parameters().get(0);
        assertEquals("join", intermediate.function(),
                "multi-hop OE fallback emits the intermediate hop as a join");
        // Terminal composes otherwise(^Firm(...), $row.firm).
        AppliedFunction otherwiseCall =
                (AppliedFunction) ctorOf(personFn).properties().get("firm").value();
        assertEquals("otherwise", otherwiseCall.function(),
                "multi-hop OE still composes otherwise(partial, fallback slot)");
    }

    @Test
    @DisplayName("joinTypeAnnotation_acceptedAndProducesSameASTAsNoAnnotation")
    void joinTypeAnnotation_acceptedAndProducesSameASTAsNoAnnotation() {
        // Property-mapping joins ALWAYS lower to LEFT OUTER JOIN -- a Person
        // with no matching firm should still appear with firmName = null,
        // not drop out. So the parser's (LEFT) annotation is informational
        // (captured for engine grammar fidelity) but the normalizer's AST
        // doesn't differentiate. Two mappings differing ONLY in a (LEFT)
        // annotation produce structurally-equal synth bodies.
        String shared =
                "Class model::Person { orgName: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, ORG_ID INTEGER) "
                        + "  Table T_ORG    (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Firm_Org   ( T_FIRM.ORG_ID   = T_ORG.ID  ) "
                        + ") ";
        String mappingWithoutAnnotation =
                "Mapping a::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    orgName: [db::DB] @Person_Firm > @Firm_Org | T_ORG.NAME "
                        + "  } "
                        + ")";
        String mappingWithLeftAnnotation =
                "Mapping a::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    orgName: [db::DB] @Person_Firm > (LEFT) @Firm_Org | T_ORG.NAME "
                        + "  } "
                        + ")";

        FunctionDefinition fnWithout = soleSynth(normalizeViaPipeline(
                ElementParser.parse(shared + mappingWithoutAnnotation)));
        FunctionDefinition fnWith = soleSynth(normalizeViaPipeline(
                ElementParser.parse(shared + mappingWithLeftAnnotation)));

        assertEquals(fnWithout.body(), fnWith.body(),
                "(LEFT) annotation must not affect the synth function's AST -- "
                        + "join type is a Phase H concern, defaults to LEFT OUTER");
    }

    @Test
    void unknownJoinName_throws() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { firmName: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    firmName: [db::DB] @Nonexistent | T_FIRM.LEGAL_NAME "
                        + "  } "
                        + ")");
        String exMsg = poisonReasons(parsed);
        assertTrue(exMsg.contains("Nonexistent"),
                () -> "Expected error to name the missing join; got: " + exMsg);
    }

    @Test
    void joinAndExpressionPM_ExpressionCanReferenceJoinedTable() {
        // After a join PM puts T_FIRM in scope as $row.firmName_h1, a
        // later Expression PM can reference T_FIRM columns -- the
        // tableScope tracks the alias across PMs in declaration order.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { firmName: String[1]; firmIdStr: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    firmName: [db::DB] @Person_Firm | T_FIRM.LEGAL_NAME, "
                        + "    firmIdStr: concat(T_FIRM.ID, '') "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition fn = soleSynth(normalized);

        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        AppliedFunction newCall = (AppliedFunction) sole(projectLambda.body());
        NewInstance ni = (NewInstance) newCall.parameters().get(1);

        // firmIdStr value: concat($row.firmName_h1.ID, '')
        AppliedFunction firmIdStr = (AppliedFunction) toOneInner(ni.properties().get("firmIdStr").value());
        assertEquals("plus", firmIdStr.function());   // concat -> plus-chain
        AppliedProperty idRef = (AppliedProperty) unwrapToString(firmIdStr.parameters().get(0));
        assertEquals("ID", idRef.property());
        AppliedProperty firmAlias = (AppliedProperty) idRef.receiver();
        assertEquals("Person_Firm", firmAlias.property(),
                "Expression PM reaches T_FIRM via the alias set up by the earlier Join PM");
    }

    // ====================================================================
    // Increment 3 - Hardening (multi-PM interactions, deeper chains,
    // declaration-order preservation, idempotence on Relational)
    // ====================================================================

    @Test
    @DisplayName("threeHopChain_pinsScopeWalkingAtDepth3")
    void threeHopChain_pinsScopeWalkingAtDepth3() {
        // Person -> Firm -> Org -> Country | T_COUNTRY.NAME
        // Hop 3's source side must reach hop 2's target via the hop-2 alias.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { countryName: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON  (ID INTEGER, FIRM_ID INTEGER) "
                        + "  Table T_FIRM    (ID INTEGER, ORG_ID INTEGER) "
                        + "  Table T_ORG     (ID INTEGER, COUNTRY_ID INTEGER) "
                        + "  Table T_COUNTRY (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm ( T_PERSON.FIRM_ID    = T_FIRM.ID ) "
                        + "  Join Firm_Org    ( T_FIRM.ORG_ID       = T_ORG.ID  ) "
                        + "  Join Org_Country ( T_ORG.COUNTRY_ID    = T_COUNTRY.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    countryName: [db::DB] @Person_Firm > @Firm_Org > @Org_Country | T_COUNTRY.NAME "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Outer-most call is map; underneath are three joins nested in
        // emission order (hop1 innermost, hop3 outermost).
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction h3Join = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("join", h3Join.function());
        AppliedFunction h2Join = (AppliedFunction) h3Join.parameters().get(0);
        assertEquals("join", h2Join.function());
        AppliedFunction h1Join = (AppliedFunction) h2Join.parameters().get(0);
        assertEquals("join", h1Join.function());

        // Aliases: hop1, hop1__hop2, hop1__hop2__hop3 (join-name concatenation)
        assertEquals("Person_Firm",
                ((com.legend.model.spec.ColSpec) h1Join.parameters().get(1)).name());
        assertEquals("Person_Firm__Firm_Org",
                ((com.legend.model.spec.ColSpec) h2Join.parameters().get(1)).name());
        assertEquals("Person_Firm__Firm_Org__Org_Country",
                ((com.legend.model.spec.ColSpec) h3Join.parameters().get(1)).name());

        // Hop 3 condition: $s.Person_Firm__Firm_Org.COUNTRY_ID == $t.ID
        // This is the load-bearing claim of the multi-hop algorithm: at
        // hop N, the source side reaches hop (N-1)'s target through that
        // hop's alias, NOT through the original mainTable scope.
        LambdaFunction h3Cond = (LambdaFunction) h3Join.parameters().get(2);
        AppliedFunction h3Eq = (AppliedFunction) sole(h3Cond.body());
        AppliedProperty left = (AppliedProperty) h3Eq.parameters().get(0);
        assertEquals("COUNTRY_ID", left.property());
        AppliedProperty leftSub = (AppliedProperty) left.receiver();
        assertEquals("Person_Firm__Firm_Org", leftSub.property(),
                "Hop 3 must reach T_ORG (hop 2 target) via the hop-2 alias");
        assertEquals(new Variable("s"), leftSub.receiver());
        AppliedProperty right = (AppliedProperty) h3Eq.parameters().get(1);
        assertEquals("ID", right.property());
        assertEquals(new Variable("t"), right.receiver());

        // Terminal: $row.countryName_h3.NAME
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        AppliedFunction newCall = (AppliedFunction) sole(projectLambda.body());
        NewInstance ni = (NewInstance) newCall.parameters().get(1);
        AppliedProperty countryName = (AppliedProperty) toOneInner(ni.properties().get("countryName").value());
        AppliedProperty terminalAlias = (AppliedProperty) countryName.receiver();
        assertEquals("Person_Firm__Firm_Org__Org_Country", terminalAlias.property());
    }

    @Test
    @DisplayName("multiPMMixedKinds_preservesDeclarationOrderInProjectKeys")
    void multiPMMixedKinds_preservesDeclarationOrderInProjectKeys() {
        // Deliberately mix PM kinds and put a Column PM BEFORE the Join PM
        // it depends on. The normalizer's two-pass design (joins first,
        // then project) means the Column PM still resolves -- AND the
        // project keys preserve source declaration order.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { "
                        + "  firmIdStr: String[1]; "
                        + "  fullName: String[1]; "
                        + "  firmName: String[1]; "
                        + "  age: Integer[1]; "
                        + "} "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRST VARCHAR(50), LAST VARCHAR(50), AGE INTEGER, FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    firmIdStr: concat(T_FIRM.ID, ''), "
                        + "    fullName:  concat(T_PERSON.FIRST, ' ', T_PERSON.LAST), "
                        + "    firmName:  [db::DB] @Person_Firm | T_FIRM.LEGAL_NAME, "
                        + "    age:       T_PERSON.AGE "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        AppliedFunction newCall = (AppliedFunction) sole(projectLambda.body());
        NewInstance ni = (NewInstance) newCall.parameters().get(1);

        // Source declaration order: firmIdStr, fullName, firmName, age.
        // Project keys must match -- otherwise downstream consumers
        // (graphFetch, ^Person construction) would see fields in the
        // wrong order, which leaks at every level.
        assertEquals(List.of("firmIdStr", "fullName", "firmName", "age"),
                List.copyOf(ni.properties().keySet()),
                "Project keys preserve source declaration order across "
                        + "mixed PM kinds");

        // firmIdStr (Column PM referencing T_FIRM, BEFORE the Join PM in
        // source) must resolve via the Join PM's alias -- the two-pass
        // design hoists ALL joins before project translation runs.
        AppliedFunction firmIdStr = (AppliedFunction) toOneInner(ni.properties().get("firmIdStr").value());
        assertEquals("plus", firmIdStr.function());   // concat -> plus-chain
        AppliedProperty idRef = (AppliedProperty) unwrapToString(firmIdStr.parameters().get(0));
        AppliedProperty firmAlias = (AppliedProperty) idRef.receiver();
        assertEquals("Person_Firm", firmAlias.property(),
                "Column PM declared BEFORE its required Join PM still resolves "
                        + "via the Join PM's alias (two-pass design)");

        // fullName (Expression PM, only T_PERSON refs) uses $row directly.
        List<ValueSpecification> fullName = plusChain(
                toOneInner(ni.properties().get("fullName").value()), 3);
        AppliedProperty firstRef = (AppliedProperty) fullName.get(0);
        assertEquals("FIRST", firstRef.property());
        assertEquals(new Variable("row"), firstRef.receiver(),
                "Main-table column ref goes directly through $row");

        // age (Column PM, T_PERSON) directly $row.AGE
        AppliedProperty age = (AppliedProperty) toOneInner(ni.properties().get("age").value());
        assertEquals("AGE", age.property());
        assertEquals(new Variable("row"), age.receiver());
    }

    @Test
    @DisplayName("filterPlusJoin_joinsBeforeFilter")
    void filterPlusJoin_joinsBeforeFilter() {
        // Direct filter + a Join PM. Pipeline:
        //   tableReference -> join -> filter -> map
        // Joins are emitted first so they can be shared between PMs
        // and a JoinMediated filter. A Direct filter (no joins) runs
        // AFTER the joins; since it only references the main table,
        // ordering vs. joins is semantically equivalent here.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { firmName: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER, IS_ACTIVE BIT) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Filter ActiveOnly( T_PERSON.IS_ACTIVE = 1 ) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~filter ActiveOnly "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    firmName: [db::DB] @Person_Firm | T_FIRM.LEGAL_NAME "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Outer is map; under it: filter -> join -> tableReference.
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction filter = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("filter", filter.function(),
                "filter is outermost upstream op (wraps the joins)");
        AppliedFunction joinCall = (AppliedFunction) filter.parameters().get(0);
        assertEquals("join", joinCall.function(),
                "join sits between tableReference and filter");
        AppliedFunction tableRef = (AppliedFunction) joinCall.parameters().get(0);
        assertEquals("tableReference", tableRef.function(),
                "tableReference is innermost");
    }

    @Test
    @DisplayName("twoJoinPMsThroughSameJoin_shareSingleJoin")
    void twoJoinPMsThroughSameJoin_shareSingleJoin() {
        // Aliases are derived from the join name, not the PM name. Two
        // PMs going through the same join (Person_Firm) share ONE join
        // (aliased "Person_Firm") and read different columns from the
        // shared sub-row.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { firmName: String[1]; firmId: Integer[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    firmName: [db::DB] @Person_Firm | T_FIRM.LEGAL_NAME, "
                        + "    firmId:   [db::DB] @Person_Firm | T_FIRM.ID "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        // Exactly ONE join: the shared Person_Firm chain.
        AppliedFunction joinCall = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("join", joinCall.function());
        com.legend.model.spec.ColSpec spec =
                (com.legend.model.spec.ColSpec) joinCall.parameters().get(1);
        assertEquals("Person_Firm", spec.name(),
                "Both PMs through @Person_Firm dedupe to ONE join");

        // Directly under the join is the bare tableReference -- no
        // second join for the second PM.
        AppliedFunction underJoin = (AppliedFunction) joinCall.parameters().get(0);
        assertEquals("tableReference", underJoin.function(),
                "No redundant second join; PMs share the prefix");

        // Both PMs read from the SAME sub-row.
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        AppliedFunction newCall = (AppliedFunction) sole(projectLambda.body());
        NewInstance ni = (NewInstance) newCall.parameters().get(1);

        AppliedProperty firmName = (AppliedProperty) toOneInner(ni.properties().get("firmName").value());
        assertEquals("LEGAL_NAME", firmName.property());
        assertEquals("Person_Firm",
                ((AppliedProperty) firmName.receiver()).property(),
                "firmName reads $row.Person_Firm.LEGAL_NAME");

        AppliedProperty firmId = (AppliedProperty) toOneInner(ni.properties().get("firmId").value());
        assertEquals("ID", firmId.property());
        assertEquals("Person_Firm",
                ((AppliedProperty) firmId.receiver()).property(),
                "firmId reads $row.Person_Firm.ID — same alias, different column");
    }

    @Test
    @DisplayName("expressionPMSpanningTwoJoinedTables_bothAliasesResolve")
    void expressionPMSpanningTwoJoinedTables_bothAliasesResolve() {
        // After joins to T_FIRM (firmName PM) and T_DEPT (deptName PM),
        // an Expression PM can reference BOTH tables in a single call.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { firmName: String[1]; deptName: String[1]; combo: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER, DEPT_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Table T_DEPT   (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Person_Dept( T_PERSON.DEPT_ID = T_DEPT.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    firmName: [db::DB] @Person_Firm | T_FIRM.NAME, "
                        + "    deptName: [db::DB] @Person_Dept | T_DEPT.NAME, "
                        + "    combo:    concat(T_FIRM.NAME, ' / ', T_DEPT.NAME) "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        AppliedFunction newCall = (AppliedFunction) sole(projectLambda.body());
        NewInstance ni = (NewInstance) newCall.parameters().get(1);

        // combo = concat($row.firmName_h1.NAME, ' / ', $row.deptName_h1.NAME)
        List<ValueSpecification> combo = plusChain(
                toOneInner(ni.properties().get("combo").value()), 3);

        AppliedProperty firmRef = (AppliedProperty) combo.get(0);
        assertEquals("NAME", firmRef.property());
        AppliedProperty firmAlias = (AppliedProperty) firmRef.receiver();
        assertEquals("Person_Firm", firmAlias.property(),
                "T_FIRM resolves through firmName PM's alias");

        // Middle arg is the ' / ' string literal.
        assertEquals(new com.legend.model.spec.CString(" / "),
                combo.get(1));

        AppliedProperty deptRef = (AppliedProperty) combo.get(2);
        assertEquals("NAME", deptRef.property());
        AppliedProperty deptAlias = (AppliedProperty) deptRef.receiver();
        assertEquals("Person_Dept", deptAlias.property(),
                "T_DEPT resolves through deptName PM's alias");
    }

    @Test
    @DisplayName("relationalNormalizeIsIdempotent")
    void relationalNormalizeIsIdempotent() {
        // The Relational path has more moving parts than M2M: chain
        // emission threads scope mutably across hops, the two-pass
        // structure walks PMs twice, and tableScope is a fresh
        // LinkedHashMap per mapping. Verify all of this is idempotent.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { firmName: String[1]; orgName: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER, IS_ACTIVE BIT) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50), ORG_ID INTEGER) "
                        + "  Table T_ORG    (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Filter ActiveOnly( T_PERSON.IS_ACTIVE = 1 ) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Firm_Org   ( T_FIRM.ORG_ID   = T_ORG.ID  ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~filter ActiveOnly "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    firmName: [db::DB] @Person_Firm | T_FIRM.LEGAL_NAME, "
                        + "    orgName:  [db::DB] @Person_Firm > @Firm_Org | T_ORG.NAME "
                        + "  } "
                        + ")");
        // Post-inversion normalize() cannot be re-applied (it returns a
        // NormalizedModel it does not accept — the re-normalization footgun
        // is gone at the type level), so the surviving property is
        // determinism: two independent passes over the same parsed input
        // produce structurally-equal lifted functions.
        NormalizedModel once = normalizeViaPipeline(parsed);
        NormalizedModel twice = normalizeViaPipeline(parsed);
        assertEquals(liftedFunctions(once), liftedFunctions(twice),
                "Two passes over a Relational mapping with filter + multi-hop "
                        + "joins must yield structurally-equal synth functions");
    }

    // ====================================================================
    // Idempotence
    // ====================================================================

    @Test
    void normalizeIsIdempotent() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "Class model::RawPerson { name: String[1]; } "
                        + "Mapping my::M ( "
                        + "  *model::Person: Pure { "
                        + "    ~src model::RawPerson "
                        + "    name: $src.name "
                        + "  } "
                        + ")");
        // normalize() is deterministic: two independent passes over the same
        // parsed input yield structurally-equal lifted functions. (It can no
        // longer be re-applied to its own output — NormalizedModel is not an
        // accepted input — so this replaces the old re-normalize idempotence.)
        NormalizedModel once = normalizeViaPipeline(parsed);
        NormalizedModel twice = normalizeViaPipeline(parsed);
        assertEquals(liftedFunctions(once), liftedFunctions(twice));
    }

    // ====================================================================
    // Catalog consistency: every native NAME the normalizer emits must
    // resolve in Pure.all() — except `new`, which is a parse-time `^Class`
    // desugar handled by a dedicated checker (NewChecker), not a catalog
    // native (its arg is a structured NewInstance binding map, so it is
    // never positionally overload-resolved; see docs/MAPPING_LEGACY_TO_FUNCTION.md
    // and the NewChecker discussion). This is the guard that originally
    // surfaced the distinctBy / otherwise registration gaps.
    // ====================================================================

    @Test
    @DisplayName("allEmittedNativesResolveInCatalog")
    void allEmittedNativesResolveInCatalog() {
        // A corpus spanning every MappingNormalizer emission path. Each
        // source is copied from a proven, self-contained test above so it
        // is guaranteed to parse + normalize.
        List<String> models = List.of(
                // M2M (Pure) with ~filter: getAll, filter, map, new, equal.
                "Class model::ActivePerson { name: String[1]; } "
                        + "Class model::RawPerson { name: String[1]; isActive: Boolean[1]; } "
                        + "Mapping my::M ( "
                        + "  *model::ActivePerson: Pure { "
                        + "    ~src model::RawPerson "
                        + "    ~filter $src.isActive == true "
                        + "    name: $src.name "
                        + "  } "
                        + ")",
                // Relational ~distinct: tableReference, distinct, map, new.
                "Class model::Person { name: String[1]; } "
                        + "Database db::DB ( Table T_PERSON (NAME VARCHAR(50)) ) "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~distinct "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name: T_PERSON.NAME "
                        + "  } "
                        + ")",
                // EnumerationMapping inline: if, equal, map, new.
                "Enum app::Status { Active, Inactive } "
                        + "Class model::Person { status: app::Status[1]; } "
                        + "Database db::DB ( Table T_PERSON (STATUS VARCHAR(50)) ) "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    status: EnumerationMapping StatusMapping : T_PERSON.STATUS "
                        + "  } "
                        + "  app::Status: EnumerationMapping StatusMapping { "
                        + "    Active:   'A', "
                        + "    Inactive: 'I' "
                        + "  } "
                        + ")",
                // ~groupBy with aggregate: groupBy, sum, map, new, tableReference.
                "Class model::P { k: String[1]; total: Integer[1]; } "
                        + "Database db::DB ( Table T (K VARCHAR(10), QTY INTEGER) ) "
                        + "Mapping my::M ( "
                        + "  *model::P: Relational { "
                        + "    ~groupBy([db::DB] T.K) "
                        + "    ~mainTable [db::DB] T "
                        + "    k:     T.K, "
                        + "    total: sum(T.QTY) "
                        + "  } "
                        + ")",
                // Relational AssociationMapping: legacyAssocPredicate.
                "Class model::Firm   { id: Integer[1]; } "
                        + "Class model::Person { firmId: Integer[1]; } "
                        + "Association model::Person_Firm { "
                        + "  firm: model::Firm[1]; person: model::Person[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_FIRM   (ID INTEGER) "
                        + "  Table T_PERSON (FIRM_ID INTEGER) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Firm: Relational { ~mainTable [db::DB] T_FIRM  id: T_FIRM.ID } "
                        + "  *model::Person: Relational { ~mainTable [db::DB] T_PERSON  firmId: T_PERSON.FIRM_ID } "
                        + "  model::Person_Firm: Relational { AssociationMapping ( "
                        + "    firm: [db::DB] @Person_Firm "
                        + "  ) } "
                        + ")",
                // Multi-hop OtherwiseEmbedded: join, legacyNavigate, otherwise, new.
                "Class model::Org    { name: String[1]; } "
                        + "Class model::Firm   { org: model::Org[1]; legalName: String[1]; } "
                        + "Class model::Person { name: String[1]; firm: model::Firm[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (NAME VARCHAR(50), FIRM_ID INTEGER, FIRM_NAME VARCHAR(50)) "
                        + "  Table T_FIRM   (ID INTEGER, ORG_ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Table T_ORG    (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Firm_Org  ( T_FIRM.ORG_ID = T_ORG.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Firm[firm_set1]: Relational { "
                        + "    ~mainTable [db::DB] T_FIRM "
                        + "    legalName: T_FIRM.LEGAL_NAME "
                        + "  } "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name: T_PERSON.NAME, "
                        + "    firm ( "
                        + "      legalName: T_PERSON.FIRM_NAME "
                        + "    ) Otherwise ([firm_set1]: [db::DB] @Person_Firm > @Firm_Org) "
                        + "  } "
                        + ")",
                // JSON-source cross-bake: sourceUrl, get, to, map, new.
                "Class model::Raw { name: String[1]; } "
                        + "Mapping my::M ( ) "
                        + "Runtime my::R { mappings: [my::M]; connections: [ "
                        + "ModelStore: [ json: #{ JsonModelConnection { "
                        + "class: model::Raw; url: 'data:application/json,[]'; } }# ] ]; }");

        Set<String> emitted = new TreeSet<>();
        for (String src : models) {
            NormalizedModel normalized = normalizeViaPipeline(ElementParser.parse(src));
            for (FunctionDefinition fn : liftedFunctions(normalized)) {
                for (ValueSpecification stmt : fn.body()) {
                    collectEmittedFunctionNames(stmt, emitted);
                }
            }
        }

        Set<String> catalog = Pure.all().stream()
                .map(d -> simpleName(d.qualifiedName()))
                .collect(Collectors.toSet());
        // `new` is the only intentionally-non-catalog emission (see header).
        Set<String> allowedNonCatalog = Set.of("new");

        SortedSet<String> offenders = new TreeSet<>(emitted);
        offenders.removeAll(catalog);
        offenders.removeAll(allowedNonCatalog);
        assertTrue(offenders.isEmpty(),
                () -> "MappingNormalizer emits function name(s) absent from Pure.all() "
                        + "and not in the allowed non-catalog set " + allowedNonCatalog + ":\n  "
                        + String.join("\n  ", offenders)
                        + "\nEither register the native in Pure.java or justify the special case.");

        // Guard against a vacuous walk: the corpus must actually exercise the
        // headline emission paths, so a regression that stops emitting (rather
        // than emits something new) also fails here.
        for (String marker : List.of(
                "map", "new", "tableReference", "getAll", "filter", "join",
                "legacyNavigate", "legacyAssocPredicate", "otherwise", "groupBy",
                "distinct", "if", "equal", "sum", "sourceUrl", "get", "to")) {
            assertTrue(emitted.contains(marker),
                    () -> "expected the model corpus to emit '" + marker
                            + "'; emitted set was " + emitted);
        }
    }

    /**
     * Recursively collects the simple name of every {@link AppliedFunction}
     * reachable from {@code vs}, descending through every composite
     * {@link ValueSpecification} variant (function args, property receivers,
     * lambda bodies, new-instance bindings, casts, collections, and the
     * {@code ~col} DSL nodes). Leaves (literals, variables, element ptrs,
     * enum values, type annotations) carry no nested function calls.
     */
    private static void collectEmittedFunctionNames(ValueSpecification vs, Set<String> out) {
        switch (vs) {
            case AppliedFunction af -> {
                out.add(simpleName(af.function()));
                for (ValueSpecification p : af.parameters()) {
                    collectEmittedFunctionNames(p, out);
                }
            }
            case AppliedProperty ap -> collectEmittedFunctionNames(ap.receiver(), out);
            case LambdaFunction lf -> {
                for (ValueSpecification b : lf.body()) {
                    collectEmittedFunctionNames(b, out);
                }
            }
            case NewInstance ni -> {
                for (KeyExpression ke : ni.properties().values()) {
                    collectEmittedFunctionNames(ke.value(), out);
                }
            }
            case NewInstanceCast nic -> collectEmittedFunctionNames(nic.src(), out);
            case PureCollection pc -> {
                for (ValueSpecification v : pc.values()) {
                    collectEmittedFunctionNames(v, out);
                }
            }
            case ColSpec cs -> {
                if (cs.function1() != null) collectEmittedFunctionNames(cs.function1(), out);
                if (cs.function2() != null) collectEmittedFunctionNames(cs.function2(), out);
            }
            case ColSpecArray ca -> {
                for (ColSpec cs : ca.colSpecs()) {
                    collectEmittedFunctionNames(cs, out);
                }
            }
            default -> {
                // Leaves: literals, Variable, PackageableElementPtr, EnumValue,
                // TypeAnnotation — no nested AppliedFunction.
            }
        }
    }

    private static String simpleName(String fqn) {
        int idx = fqn.lastIndexOf("::");
        return idx < 0 ? fqn : fqn.substring(idx + 2);
    }

    /**
     * Collects every {@link AppliedProperty} name reachable from {@code vs}
     * (descending through function args, lambda bodies, and collections).
     * Used to assert which physical columns a filter condition reads.
     */
    private static void collectPropertyNames(ValueSpecification vs, Set<String> out) {
        switch (vs) {
            case AppliedProperty ap -> {
                out.add(ap.property());
                collectPropertyNames(ap.receiver(), out);
            }
            case AppliedFunction af -> {
                for (ValueSpecification p : af.parameters()) {
                    collectPropertyNames(p, out);
                }
            }
            case LambdaFunction lf -> {
                for (ValueSpecification b : lf.body()) {
                    collectPropertyNames(b, out);
                }
            }
            case PureCollection pc -> {
                for (ValueSpecification v : pc.values()) {
                    collectPropertyNames(v, out);
                }
            }
            default -> { /* leaves carry no AppliedProperty */ }
        }
    }

    /**
     * Collects the table name (2nd CString arg) of every
     * {@code tableReference(db, table)} reachable from {@code vs}, including
     * those nested inside join/legacyNavigate ColSpec lambdas.
     */
    private static void collectTableReferenceTables(ValueSpecification vs, Set<String> out) {
        switch (vs) {
            case AppliedFunction af -> {
                if ("tableReference".equals(af.function()) && af.parameters().size() == 2
                        && af.parameters().get(1) instanceof com.legend.model.spec.CString cs) {
                    out.add(cs.value());
                }
                for (ValueSpecification pp : af.parameters()) {
                    collectTableReferenceTables(pp, out);
                }
            }
            case AppliedProperty ap -> collectTableReferenceTables(ap.receiver(), out);
            case LambdaFunction lf -> {
                for (ValueSpecification b : lf.body()) {
                    collectTableReferenceTables(b, out);
                }
            }
            case NewInstance ni -> {
                for (KeyExpression ke : ni.properties().values()) {
                    collectTableReferenceTables(ke.value(), out);
                }
            }
            case NewInstanceCast nic -> collectTableReferenceTables(nic.src(), out);
            case PureCollection pc -> {
                for (ValueSpecification v : pc.values()) {
                    collectTableReferenceTables(v, out);
                }
            }
            case ColSpec csp -> {
                if (csp.function1() != null) collectTableReferenceTables(csp.function1(), out);
                if (csp.function2() != null) collectTableReferenceTables(csp.function2(), out);
            }
            case ColSpecArray ca -> {
                for (ColSpec csp : ca.colSpecs()) {
                    collectTableReferenceTables(csp, out);
                }
            }
            default -> { /* leaves */ }
        }
    }

    // ====================================================================
    // Helpers
    // ====================================================================

    private static LegacyMappingDefinition firstMapping(ParsedModel pm) {
        for (PackageableElement el : pm.elements()) {
            if (el instanceof LegacyMappingDefinition md) {
                return md;
            }
        }
        fail("no LegacyMappingDefinition in model");
        throw new AssertionError("unreachable");
    }

    /** The canonical (binding-table) MappingDefinition for {@code fqn} in the normalized model. */
    private static com.legend.model.MappingDefinition canonicalMapping(
            NormalizedModel m, String fqn) {
        for (PackageableElement el : m.elements()) {
            if (el instanceof com.legend.model.MappingDefinition md
                    && md.qualifiedName().equals(fqn)) {
                return md;
            }
        }
        throw new AssertionError("no canonical MappingDefinition '" + fqn + "' in normalized model");
    }

    /**
     * Run Phase E.1 exactly as production does: invoke the single
     * {@link MappingNormalizer#normalize(ParsedModel, ModelBuilder)} method with
     * a real {@code ModelBuilder.from(new com.legend.model.ParsedModel(parsed.elements(), parsed.imports()))} resolution index &mdash; the same
     * two calls the {@code ModelNormalizer} / {@code Compiler} orchestrator makes.
     * This is call-site glue, not a second implementation: there is exactly one
     * {@code normalize} code path, and tests and prod both go through it.
     */

    /**
     * Per-class fault isolation (relational-corpus work): a class mapping
     * that fails to normalize POISONS its binding instead of sinking the
     * mapping; the reason is recorded and raised at use. These pins assert
     * the recorded reasons.
     */
    private static String poisonReasons(ParsedModel parsed) {
        ModelBuilder mb = ModelBuilder.from(new com.legend.model.ParsedModel(
                parsed.elements(), parsed.imports()));
        MappingNormalizer.normalize(parsed, mb);
        return String.join(" ;; ", mb.mappingPoisons.values());
    }

    private static NormalizedModel normalizeViaPipeline(ParsedModel parsed) {
        return MappingNormalizer.normalize(parsed, ModelBuilder.from(new com.legend.model.ParsedModel(parsed.elements(), parsed.imports())));
    }

    /**
     * The mapping realizing functions Phase E lifted into the element list
     * (hats {@code "class"} / {@code "assoc"}), in element order. Post-inversion
     * these are ordinary top-level {@link FunctionDefinition}s, not stored on
     * the {@link LegacyMappingDefinition} (docs/CLEAN_SHEET_INVERSION.md §1). Replaces
     * the old {@code firstMapping(...).mappingFunctions()} accessor.
     */
    private static List<FunctionDefinition> liftedFunctions(NormalizedModel m) {
        List<FunctionDefinition> out = new ArrayList<>();
        for (PackageableElement el : m.elements()) {
            if (el instanceof FunctionDefinition fd && fd.isSynthesized()
                    && (fd.synthesizedFrom().hat() == SynthHat.CLASS
                        || fd.synthesizedFrom().hat() == SynthHat.ASSOC)) {
                out.add(fd);
            }
        }
        return out;
    }

    private static FunctionDefinition soleSynth(NormalizedModel m) {
        List<FunctionDefinition> fns = liftedFunctions(m);
        assertEquals(1, fns.size(), "expected exactly one synth fn");
        return fns.get(0);
    }

    private static <T> T sole(List<T> list) {
        assertEquals(1, list.size(), "expected exactly one element");
        return list.get(0);
    }

    /**
     * Flattens the left-assoc {@code plus(plus(a,b),c)} chain the normalizer
     * emits for the variadic {@code concat} dynafunction (which has no
     * pure-function counterpart; real pure spells concatenation with plus)
     * back into its {@code n} argument list.
     */
    private static List<ValueSpecification> plusChain(ValueSpecification v, int n) {
        java.util.LinkedList<ValueSpecification> out = new java.util.LinkedList<>();
        ValueSpecification cur = v;
        while (out.size() < n - 1) {
            AppliedFunction af = assertInstanceOf(AppliedFunction.class, cur,
                    "expected a plus-chain node");
            assertEquals("plus", af.function());
            assertEquals(2, af.parameters().size());
            out.addFirst(unwrapToString(af.parameters().get(1)));
            cur = af.parameters().get(0);
        }
        out.addFirst(unwrapToString(cur));
        return List.copyOf(out);
    }

    /**
     * Peels the {@code toString(...)} coercion the normalizer wraps around
     * every concat argument (SQL concat coerces; plus(String, String) needs
     * the wrap said at emission).
     */
    private static ValueSpecification unwrapToString(ValueSpecification v) {
        // Peels the cast(..., @String) coercion around every concat argument
        // (SQL concat coerces via the DATABASE's VARCHAR cast — audit: pure
        // toString's ISO datetime form diverged from SQL-concat semantics).
        return v instanceof AppliedFunction af && af.function().equals("cast")
                && af.parameters().size() == 2 ? af.parameters().get(0) : v;
    }

    /**
     * Asserts a ctor-field value carries the {@code toOne(...)} wrapper the
     * normalizer emits around store reads bound to a {@code [1]}-declared
     * property (pure NewValidator subsumption, said at EMISSION) and
     * returns the wrapped read.
     */
    private static ValueSpecification toOneInner(ValueSpecification v) {
        AppliedFunction af = assertInstanceOf(AppliedFunction.class, v,
                "[1]-property ctor value must be toOne-wrapped");
        assertEquals("toOne", af.function());
        return sole(af.parameters());
    }

    /**
     * Extracts the {@code legacyNavigate} pipeline step from a synth fn
     * whose body is {@code map(legacyNavigate(...), row | ^Cls(...))}.
     * (New class-typed Join PM shape — doc §5.4.3.)
     */
    private static AppliedFunction navStep(FunctionDefinition fn) {
        AppliedFunction map = (AppliedFunction) sole(fn.body());
        assertEquals("map", map.function(), "synth body terminates in map(...)");
        AppliedFunction nav = (AppliedFunction) map.parameters().get(0);
        assertEquals("legacyNavigate", nav.function(),
                "class-typed Join PM emits a legacyNavigate pipeline step");
        return nav;
    }

    /** The {@code legacyNavigate} condition lambda's body (arg 3; arg 2 = target rows). */
    private static ValueSpecification navCondBody(FunctionDefinition fn) {
        AppliedFunction nav = navStep(fn);
        AppliedFunction tgtRows = (AppliedFunction) nav.parameters().get(2);
        assertEquals("tableReference", tgtRows.function(),
                "legacyNavigate carries the target's table row source");
        LambdaFunction cond = (LambdaFunction) nav.parameters().get(3);
        return sole(cond.body());
    }

    /** The terminal {@code ^Cls(...)} NewInstance of a synth fn's map lambda. */
    private static NewInstance ctorOf(FunctionDefinition fn) {
        AppliedFunction map = (AppliedFunction) sole(fn.body());
        LambdaFunction lam = (LambdaFunction) map.parameters().get(1);
        return (NewInstance) ((AppliedFunction) sole(lam.body())).parameters().get(1);
    }

    /** Property name of an AppliedProperty argument. */
    private static String propOf(ValueSpecification vs) {
        return ((AppliedProperty) vs).property();
    }

    /** Receiver variable name of an AppliedProperty argument. */
    private static String recvOf(ValueSpecification vs) {
        return ((Variable) ((AppliedProperty) vs).receiver()).name();
    }

    // ====================================================================
    // R4.3 — Multi-column AND-of-equality FK lift (composite keys)
    //
    // A class-typed Join PM whose last hop has a composite-key join
    // condition `T.A == T2.A && T.B == T2.B` lifts to N +<prop>Fk_<col>
    // +local fields on the outer ctor + one legacyAssociate whose
    // predicate is the AND of $p.<fk_i> == $t.<tgt_i> terms. OR / non-
    // equality / mixed-side conjuncts reject with a clear diagnostic.
    // ====================================================================

    @Test
    @DisplayName("joinPmCompositeKeyTwoColumns (R4.3)")
    void joinPmCompositeKeyTwoColumns() {
        // Composite-key FK: Person -> Address via (REGION, ADDR_ID).
        // Last hop: T_PERSON.REGION == T_ADDR.REGION && T_PERSON.ADDR_ID == T_ADDR.ID.
        // Expected lift:
        //   +addressFk_REGION  = $row.REGION
        //   +addressFk_ADDR_ID = $row.ADDR_ID
        //   legacyAssociate(~address: Address.all(),
        //       {p,t | and(equal($p.addressFk_REGION,  $t.REGION),
        //                  equal($p.addressFk_ADDR_ID, $t.ID))})
        ParsedModel parsed = ElementParser.parse(
                "Class model::Address { region: String[1]; } "
                        + "Class model::Person  { address: model::Address[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (REGION VARCHAR(10), ADDR_ID INTEGER) "
                        + "  Table T_ADDR   (REGION VARCHAR(10), ID INTEGER) "
                        + "  Join Person_Addr( T_PERSON.REGION = T_ADDR.REGION and T_PERSON.ADDR_ID = T_ADDR.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    address: [db::DB] @Person_Addr "
                        + "  } "
                        + "  model::Address: Relational { "
                        + "    ~mainTable [db::DB] T_ADDR "
                        + "    region: T_ADDR.REGION "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition personFn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().endsWith("::Person"))
                .findFirst().orElseThrow();

        // Condition appears verbatim in the legacyNavigate lambda:
        // and(equal($s.REGION, $t.REGION), equal($s.ADDR_ID, $t.ID)).
        AppliedFunction andCall = (AppliedFunction) navCondBody(personFn);
        assertEquals("and", andCall.function(),
                "Composite-key condition is the AND of per-pair equalities, verbatim");
        AppliedFunction eq1 = (AppliedFunction) andCall.parameters().get(0);
        AppliedFunction eq2 = (AppliedFunction) andCall.parameters().get(1);
        assertEquals("equal", eq1.function());
        assertEquals("equal", eq2.function());
        assertEquals("REGION", propOf(eq1.parameters().get(0)));
        assertEquals("s", recvOf(eq1.parameters().get(0)), "source side reads $s");
        assertEquals("REGION", propOf(eq1.parameters().get(1)));
        assertEquals("t", recvOf(eq1.parameters().get(1)), "target side reads $t");
        assertEquals("ADDR_ID", propOf(eq2.parameters().get(0)));
        assertEquals("ID", propOf(eq2.parameters().get(1)));

        // Ctor reads $row.address (the navigate slot) — no +propFk lift.
        NewInstance ni = ctorOf(personFn);
        assertEquals(List.of("address"), List.copyOf(ni.properties().keySet()),
                "No +propFk carriers; the class-typed property reads the slot");
        assertEquals("address", propOf(toOneInner(ni.properties().get("address").value())));
        assertEquals("row", recvOf(toOneInner(ni.properties().get("address").value())));
    }

    @Test
    @DisplayName("joinPmCompositeKeyThreeColumns (R4.3)")
    void joinPmCompositeKeyThreeColumns() {
        // Three-conjunct composite key. Pins that the AND-walker is
        // properly recursive (the parser left-associates 'and', so this
        // produces and(and(a, b), c) — the walker must descend both
        // sides).
        ParsedModel parsed = ElementParser.parse(
                "Class model::Target { ka: Integer[1]; } "
                        + "Class model::Source { tgt: model::Target[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_SRC (A INTEGER, B INTEGER, C INTEGER) "
                        + "  Table T_TGT (A INTEGER, B INTEGER, C INTEGER) "
                        + "  Join S_T( T_SRC.A = T_TGT.A and T_SRC.B = T_TGT.B and T_SRC.C = T_TGT.C ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Source: Relational { "
                        + "    ~mainTable [db::DB] T_SRC "
                        + "    tgt: [db::DB] @S_T "
                        + "  } "
                        + "  model::Target: Relational { "
                        + "    ~mainTable [db::DB] T_TGT "
                        + "    ka: T_TGT.A "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition srcFn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().endsWith("::Source"))
                .findFirst().orElseThrow();

        // Three-conjunct AND reconstructed verbatim in the legacyNavigate
        // lambda. The AND-walker descends both sides recursively; collect
        // all leaf equalities and assert the three source columns appear.
        AppliedFunction andOuter = (AppliedFunction) navCondBody(srcFn);
        assertEquals("and", andOuter.function());
        List<String> srcCols = new java.util.ArrayList<>();
        collectEqualitySourceCols(andOuter, srcCols);
        assertEquals(List.of("A", "B", "C"), srcCols,
                "All three conjunct source columns appear (recursive AND walk)");
        // Ctor reads $row.tgt (slot), no +propFk.
        assertEquals(List.of("tgt"), List.copyOf(ctorOf(srcFn).properties().keySet()));
    }

    /** Recursively collect source-side ($s) column names from a nested AND of equalities. */
    private static void collectEqualitySourceCols(AppliedFunction node, List<String> out) {
        if (node.function().equals("and")) {
            collectEqualitySourceCols((AppliedFunction) node.parameters().get(0), out);
            collectEqualitySourceCols((AppliedFunction) node.parameters().get(1), out);
        } else {
            // equal(...) leaf: source side is the $s.<col> argument.
            for (ValueSpecification arg : node.parameters()) {
                if (arg instanceof AppliedProperty ap
                        && ap.receiver() instanceof Variable v && v.name().equals("s")) {
                    out.add(ap.property());
                }
            }
        }
    }

    @Test
    @DisplayName("joinPmOrConjunctLifts (predicate rewriter)")
    void joinPmOrConjunctLifts() {
        // OR-shaped join condition. The predicate rewriter walks
        // structurally — every source ColumnRef becomes a +local and
        // gets replaced with $p.<localName>, every target ColumnRef
        // becomes $t.<col>, and the OR is reconstructed verbatim. The
        // multiplicity contract (declared [1]) is enforced at runtime
        // by the lowerer's cardinality fence, NOT statically here.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Target { id: Integer[1]; } "
                        + "Class model::Source { tgt: model::Target[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_SRC (A INTEGER, B INTEGER) "
                        + "  Table T_TGT (A INTEGER, B INTEGER, ID INTEGER) "
                        + "  Join S_T( T_SRC.A = T_TGT.A or T_SRC.B = T_TGT.B ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Source: Relational { "
                        + "    ~mainTable [db::DB] T_SRC "
                        + "    tgt: [db::DB] @S_T "
                        + "  } "
                        + "  model::Target: Relational { "
                        + "    ~mainTable [db::DB] T_TGT "
                        + "    id: T_TGT.ID "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition srcFn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().endsWith("::Source"))
                .findFirst().orElseThrow();

        // Condition: or(equal($s.A, $t.A), equal($s.B, $t.B)) — OR
        // reconstructed verbatim in the legacyNavigate lambda.
        AppliedFunction orCall = (AppliedFunction) navCondBody(srcFn);
        assertEquals("or", orCall.function(),
                "Condition reconstructed with OR verbatim from the source AST");
        AppliedFunction eq1 = (AppliedFunction) orCall.parameters().get(0);
        AppliedFunction eq2 = (AppliedFunction) orCall.parameters().get(1);
        assertEquals("equal", eq1.function());
        assertEquals("equal", eq2.function());
        assertEquals("A", propOf(eq1.parameters().get(0)));
        assertEquals("s", recvOf(eq1.parameters().get(0)));
        assertEquals("B", propOf(eq2.parameters().get(0)));
        assertEquals("s", recvOf(eq2.parameters().get(0)));
        // Ctor reads $row.tgt (slot), no +propFk.
        assertEquals(List.of("tgt"), List.copyOf(ctorOf(srcFn).properties().keySet()));
    }

    @Test
    @DisplayName("joinPmNonEqualityConjunctLifts (predicate rewriter)")
    void joinPmNonEqualityConjunctLifts() {
        // Non-equality conjunct in an AND. The predicate rewriter
        // preserves the operator; the only requirement is side-
        // separability of column references, which is satisfied here
        // (T_SRC.* on left side of each conjunct, T_TGT.* on right).
        ParsedModel parsed = ElementParser.parse(
                "Class model::Target { id: Integer[1]; } "
                        + "Class model::Source { tgt: model::Target[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_SRC (A INTEGER, B INTEGER) "
                        + "  Table T_TGT (A INTEGER, B INTEGER, ID INTEGER) "
                        + "  Join S_T( T_SRC.A = T_TGT.A and T_SRC.B < T_TGT.B ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Source: Relational { "
                        + "    ~mainTable [db::DB] T_SRC "
                        + "    tgt: [db::DB] @S_T "
                        + "  } "
                        + "  model::Target: Relational { "
                        + "    ~mainTable [db::DB] T_TGT "
                        + "    id: T_TGT.ID "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition srcFn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().endsWith("::Source"))
                .findFirst().orElseThrow();

        AppliedFunction andCall = (AppliedFunction) navCondBody(srcFn);
        assertEquals("and", andCall.function());
        AppliedFunction eq = (AppliedFunction) andCall.parameters().get(0);
        AppliedFunction lt = (AppliedFunction) andCall.parameters().get(1);
        assertEquals("equal", eq.function(),
                "First conjunct keeps `==` operator from source");
        assertEquals("lessThan", lt.function(),
                "Second conjunct keeps `<` operator from source (non-equality preserved)");
        assertEquals("s", recvOf(eq.parameters().get(0)), "source side reads $s");
        assertEquals("t", recvOf(lt.parameters().get(1)), "target side reads $t");
    }

    @Test
    @DisplayName("joinPmIsNullConjunctLifts (predicate rewriter)")
    void joinPmIsNullConjunctLifts() {
        // isNull on a TARGET column: pure-target conjunct, becomes
        // isNull($t.DELETED_AT) in the predicate with no +local needed.
        // Combined with an equality FK that DOES generate a +local.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Target { id: Integer[1]; } "
                        + "Class model::Source { active: model::Target[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_SRC (TGT_ID INTEGER) "
                        + "  Table T_TGT (ID INTEGER, DELETED_AT INTEGER) "
                        + "  Join S_T( T_SRC.TGT_ID = T_TGT.ID and T_TGT.DELETED_AT is null ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Source: Relational { "
                        + "    ~mainTable [db::DB] T_SRC "
                        + "    active: [db::DB] @S_T "
                        + "  } "
                        + "  model::Target: Relational { "
                        + "    ~mainTable [db::DB] T_TGT "
                        + "    id: T_TGT.ID "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition srcFn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().endsWith("::Source"))
                .findFirst().orElseThrow();

        // Ctor reads $row.active (slot) — no +propFk lift.
        assertEquals(List.of("active"), List.copyOf(ctorOf(srcFn).properties().keySet()));

        // Condition AND has equal($s.TGT_ID, $t.ID) then isNull($t.DELETED_AT).
        AppliedFunction andCall = (AppliedFunction) navCondBody(srcFn);
        assertEquals("and", andCall.function());
        AppliedFunction eq = (AppliedFunction) andCall.parameters().get(0);
        assertEquals("TGT_ID", propOf(eq.parameters().get(0)));
        assertEquals("s", recvOf(eq.parameters().get(0)), "source side reads $s");
        AppliedFunction isNullCall = (AppliedFunction) andCall.parameters().get(1);
        assertEquals("isEmpty", isNullCall.function());
        AppliedProperty deletedAt = (AppliedProperty) isNullCall.parameters().get(0);
        assertEquals("DELETED_AT", deletedAt.property());
        assertEquals("t", ((Variable) deletedAt.receiver()).name(),
                "Target-side ColumnRef reads $t.<col>");
    }

    @Test
    @DisplayName("joinPmFunctionCallOnSourceSide (predicate rewriter)")
    void joinPmFunctionCallOnSourceSide() {
        // Function call wrapping source columns: upper(T_SRC.NAME) == T_TGT.NAME.
        // The rewriter walks INTO the function call: T_SRC.NAME becomes
        // $p.tgtFk_NAME and the upper(...) wrapper is preserved.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Target { id: Integer[1]; } "
                        + "Class model::Source { tgt: model::Target[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_SRC (NAME VARCHAR(50)) "
                        + "  Table T_TGT (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join S_T( upper(T_SRC.NAME) = T_TGT.NAME ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Source: Relational { "
                        + "    ~mainTable [db::DB] T_SRC "
                        + "    tgt: [db::DB] @S_T "
                        + "  } "
                        + "  model::Target: Relational { "
                        + "    ~mainTable [db::DB] T_TGT "
                        + "    id: T_TGT.ID "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition srcFn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().endsWith("::Source"))
                .findFirst().orElseThrow();

        // Ctor reads $row.tgt (slot) — no +propFk lift.
        assertEquals(List.of("tgt"), List.copyOf(ctorOf(srcFn).properties().keySet()));

        // Condition: equal(upper($s.NAME), $t.NAME) — the upper(...) wrapper
        // around the source column is preserved verbatim in the lambda.
        AppliedFunction eq = (AppliedFunction) navCondBody(srcFn);
        assertEquals("equal", eq.function());
        AppliedFunction upperCall = (AppliedFunction) eq.parameters().get(0);
        assertEquals("upper", upperCall.function(),
                "Function call wrapper around source column is preserved in the condition");
        AppliedProperty inner = (AppliedProperty) upperCall.parameters().get(0);
        assertEquals("NAME", inner.property());
        assertEquals("s", ((Variable) inner.receiver()).name(),
                "Inner source ColumnRef reads $s.<col> INSIDE the function call");
    }

    @Test
    @DisplayName("joinPmMixedSideFunctionRewrites (predicate rewriter)")
    void joinPmMixedSideFunctionRewrites() {
        // A function call whose args mix source AND target columns:
        // equal(plus(T_SRC.A, T_TGT.B), 5). The rewriter handles this
        // by replacing EACH ColumnRef individually — T_SRC.A becomes
        // $p.tgtFk_A, T_TGT.B becomes $t.B, and the plus/equal wrappers
        // are preserved. (Engine handles this natively; we are at parity.)
        ParsedModel parsed = ElementParser.parse(
                "Class model::Target { id: Integer[1]; } "
                        + "Class model::Source { tgt: model::Target[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_SRC (A INTEGER) "
                        + "  Table T_TGT (B INTEGER, ID INTEGER) "
                        + "  Join S_T( plus(T_SRC.A, T_TGT.B) = 5 ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Source: Relational { "
                        + "    ~mainTable [db::DB] T_SRC "
                        + "    tgt: [db::DB] @S_T "
                        + "  } "
                        + "  model::Target: Relational { "
                        + "    ~mainTable [db::DB] T_TGT "
                        + "    id: T_TGT.ID "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition srcFn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().endsWith("::Source"))
                .findFirst().orElseThrow();

        // Ctor reads $row.tgt (slot) — no +propFk lift.
        assertEquals(List.of("tgt"), List.copyOf(ctorOf(srcFn).properties().keySet()));

        // Condition: equal(plus($s.A, $t.B), 5). Both refs translated
        // individually inside the plus(...) call — source to $s, target to $t.
        AppliedFunction eq = (AppliedFunction) navCondBody(srcFn);
        AppliedFunction plusCall = (AppliedFunction) eq.parameters().get(0);
        assertEquals("plus", plusCall.function());
        AppliedProperty arg0 = (AppliedProperty) plusCall.parameters().get(0);
        AppliedProperty arg1 = (AppliedProperty) plusCall.parameters().get(1);
        assertEquals("A", arg0.property(),
                "Source-side arg of mixed function reads $s.<col>");
        assertEquals("s", ((Variable) arg0.receiver()).name());
        assertEquals("B", arg1.property(),
                "Target-side arg of mixed function reads $t.<col>");
        assertEquals("t", ((Variable) arg1.receiver()).name());
    }

    @Test
    @DisplayName("joinPmThirdTableRejected (chain build catches multi-table joins)")
    void joinPmThirdTableRejected() {
        // ColumnRef referencing a table that is NEITHER source nor
        // target. Caught at chain-build time by determineTargetTable,
        // BEFORE the predicate rewriter runs. The rewriter has its own
        // defensive check ("neither the source nor the target") for any
        // such ColumnRef that reaches it, but in practice
        // determineTargetTable rejects multi-table joins first.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Target { id: Integer[1]; } "
                        + "Class model::Source { tgt: model::Target[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_SRC (A INTEGER) "
                        + "  Table T_TGT (A INTEGER, ID INTEGER) "
                        + "  Table T_OTHER (X INTEGER) "
                        + "  Join S_T( T_SRC.A = T_TGT.A and T_OTHER.X = 1 ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Source: Relational { "
                        + "    ~mainTable [db::DB] T_SRC "
                        + "    tgt: [db::DB] @S_T "
                        + "  } "
                        + "  model::Target: Relational { "
                        + "    ~mainTable [db::DB] T_TGT "
                        + "    id: T_TGT.ID "
                        + "  } "
                        + ")");
        String exMsg = poisonReasons(parsed);
        assertTrue(exMsg.contains("T_OTHER"),
                () -> "Diagnostic must name the offending table; got: " + exMsg);
        assertTrue(exMsg.contains("multi-table"),
                () -> "Diagnostic explains the multi-table join rejection; got: " + exMsg);
    }

    // ====================================================================
    // R4.2 — Multi-hop class-typed Join PM (L3 lifting)
    //
    // A class-typed Join PM with chain length > 1 lifts to exactly one
    // legacyAssociate, just like single-hop. The only difference: the
    // +<prop>Fk source column is read from the SECOND-TO-LAST hop's
    // sub-row (`$row.<allButLast>.<srcCol>`), not from $row directly.
    // The FK is extracted from the LAST hop's join condition.
    // ====================================================================

    @Test
    @DisplayName("joinPmTwoHopMappedClass (R4.2)")
    void joinPmTwoHopMappedClass() {
        // org: [db] @Person_Firm > @Firm_Org   (Org is mapped)
        // Last hop's condition is T_FIRM.ORG_ID == T_ORG.ID, so the
        // source-side column ORG_ID lives on the FIRM sub-row at
        // $row.Person_Firm; +orgFk = $row.Person_Firm.ORG_ID.
        // Predicate: $p.orgFk == $t.ID.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Org    { name: String[1]; } "
                        + "Class model::Person { name: String[1]; org: model::Org[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, NAME VARCHAR(50), FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, ORG_ID INTEGER) "
                        + "  Table T_ORG    (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Firm_Org   ( T_FIRM.ORG_ID    = T_ORG.ID  ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name: T_PERSON.NAME, "
                        + "    org:  [db::DB] @Person_Firm > @Firm_Org "
                        + "  } "
                        + "  model::Org: Relational { "
                        + "    ~mainTable [db::DB] T_ORG "
                        + "    name: T_ORG.NAME "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition personFn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().endsWith("::Person"))
                .findFirst().orElseThrow();

        // New shape: intermediate hop (Person_Firm) is a clean join();
        // the final hop (Firm_Org) is a legacyNavigate(~org) whose
        // condition reads through the prior hop's sub-row.
        AppliedFunction nav = navStep(personFn);
        assertEquals("org",
                ((com.legend.model.spec.ColSpec) nav.parameters().get(1)).name(),
                "final-hop slot is the PM property name");

        // Upstream of legacyNavigate is the intermediate Person_Firm join.
        AppliedFunction h1Join = (AppliedFunction) nav.parameters().get(0);
        assertEquals("join", h1Join.function(),
                "intermediate hop is a clean join step");

        // Condition: {s, t | $s.Person_Firm.ORG_ID == $t.ID}.
        LambdaFunction cond = (LambdaFunction) nav.parameters().get(3);
        AppliedFunction eq = (AppliedFunction) sole(cond.body());
        assertEquals("equal", eq.function());
        AppliedProperty srcCol = (AppliedProperty) eq.parameters().get(0);
        assertEquals("ORG_ID", srcCol.property());
        AppliedProperty srcSub = (AppliedProperty) srcCol.receiver();
        assertEquals("Person_Firm", srcSub.property(),
                "multi-hop final condition reads through the prior hop's sub-row");
        assertEquals("s", ((Variable) srcSub.receiver()).name());
        assertEquals("ID", propOf(eq.parameters().get(1)),
                "target side reaches T_ORG's PK column (last hop)");

        // Ctor reads $row.org (slot); no +propFk lift.
        NewInstance ni = ctorOf(personFn);
        assertEquals(List.of("name", "org"), List.copyOf(ni.properties().keySet()));
    }

    @Test
    @DisplayName("joinPmThreeHopMappedClass (R4.2)")
    void joinPmThreeHopMappedClass() {
        // Three-hop chain: division reachable via Person -> Firm -> Org -> Div.
        // Last hop's condition: T_ORG.DIV_ID == T_DIV.ID.
        // Source-side sub-row at second-to-last alias `Person_Firm__Firm_Org`.
        // +divisionFk = $row.Person_Firm__Firm_Org.DIV_ID.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Division { name: String[1]; } "
                        + "Class model::Person   { division: model::Division[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, ORG_ID INTEGER) "
                        + "  Table T_ORG    (ID INTEGER, DIV_ID INTEGER) "
                        + "  Table T_DIV    (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Firm_Org   ( T_FIRM.ORG_ID    = T_ORG.ID  ) "
                        + "  Join Org_Div    ( T_ORG.DIV_ID     = T_DIV.ID  ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    division: [db::DB] @Person_Firm > @Firm_Org > @Org_Div "
                        + "  } "
                        + "  model::Division: Relational { "
                        + "    ~mainTable [db::DB] T_DIV "
                        + "    name: T_DIV.NAME "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition personFn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().endsWith("::Person"))
                .findFirst().orElseThrow();

        // Two intermediate joins (Person_Firm, Person_Firm__Firm_Org) then
        // the final legacyNavigate(~division). The final condition reads
        // the source column through the depth-2 sub-row (concatenated alias).
        AppliedFunction nav = navStep(personFn);
        assertEquals("division",
                ((com.legend.model.spec.ColSpec) nav.parameters().get(1)).name());
        LambdaFunction cond = (LambdaFunction) nav.parameters().get(3);
        AppliedFunction eq = (AppliedFunction) sole(cond.body());
        AppliedProperty srcCol = (AppliedProperty) eq.parameters().get(0);
        assertEquals("DIV_ID", srcCol.property());
        AppliedProperty srcSub = (AppliedProperty) srcCol.receiver();
        assertEquals("Person_Firm__Firm_Org", srcSub.property(),
                "Three-hop final condition reaches the depth-2 sub-row via the "
              + "concatenated alias of the first two hops");
        assertEquals("s", ((Variable) srcSub.receiver()).name());
        // Ctor reads $row.division (slot); no +propFk lift.
        assertEquals(List.of("division"), List.copyOf(ctorOf(personFn).properties().keySet()));
    }

    @Test
    @DisplayName("joinPmMultiHopMissingPreludeEntryDiagnostic (R4.2)")
    void joinPmMultiHopMissingPreludeEntryDiagnostic() {
        // Synthetic regression guard: a multi-hop chain whose prefix
        // alias is somehow missing from the prelude must produce a clear
        // error (the chain dedup is the only mechanism preventing this,
        // so if a refactor breaks emitChain's idempotence assumption we
        // get a loud failure here). We trigger this by constructing a
        // valid multi-hop chain (so the test exercises the multi-hop
        // path) and verify that the prelude entry IS present — proving
        // the success path establishes the invariant the diagnostic
        // guards against.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Org    { name: String[1]; } "
                        + "Class model::Person { org: model::Org[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, ORG_ID INTEGER) "
                        + "  Table T_ORG    (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Firm_Org   ( T_FIRM.ORG_ID    = T_ORG.ID  ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    org: [db::DB] @Person_Firm > @Firm_Org "
                        + "  } "
                        + "  model::Org: Relational { "
                        + "    ~mainTable [db::DB] T_ORG "
                        + "    name: T_ORG.NAME "
                        + "  } "
                        + ")");
        // No exception thrown — the success path proves both prefix and
        // full-chain alias are present in the prelude.
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition personFn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().endsWith("::Person"))
                .findFirst().orElseThrow();
        // New shape: the final hop is a legacyNavigate; the single
        // intermediate hop (Person_Firm) is a join beneath it, whose
        // own upstream is the bare tableReference. This proves the
        // prelude chains the intermediate hop before the navigate.
        AppliedFunction nav = navStep(personFn);
        AppliedFunction h1Join = (AppliedFunction) nav.parameters().get(0);
        assertEquals("join", h1Join.function(),
                "intermediate hop is present as a join beneath the legacyNavigate");
        AppliedFunction base = (AppliedFunction) h1Join.parameters().get(0);
        assertEquals("tableReference", base.function(),
                "the intermediate join's upstream is the main tableReference");
    }

    @Test
    @DisplayName("joinPmMultiHopWithIntermediateSelfJoin (R4.2)")
    void joinPmMultiHopWithIntermediateSelfJoin() {
        // Intermediate hop is a self-join (T_NODE -> T_NODE via PARENT_ID).
        // Path: Person -> Firm via Person_Firm; Firm -> Firm via Firm_Parent (self).
        // The FK extraction must still use the LAST hop's condition,
        // which is a self-join: T_FIRM.PARENT_ID == {target}.ID.
        // Source side: T_FIRM (via $row.Person_Firm); column = PARENT_ID.
        // Target side: {target}.ID → column = ID.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Firm   { name: String[1]; } "
                        + "Class model::Person { parentFirm: model::Firm[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, PARENT_ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Firm_Parent( T_FIRM.PARENT_ID = {target}.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    parentFirm: [db::DB] @Person_Firm > @Firm_Parent "
                        + "  } "
                        + "  model::Firm: Relational { "
                        + "    ~mainTable [db::DB] T_FIRM "
                        + "    name: T_FIRM.NAME "
                        + "  } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition personFn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().endsWith("::Person"))
                .findFirst().orElseThrow();

        // Final hop is the self-join Firm_Parent; emitted as legacyNavigate.
        // Condition: {s, t | $s.Person_Firm.PARENT_ID == $t.ID}.
        AppliedFunction nav = navStep(personFn);
        assertEquals("parentFirm",
                ((com.legend.model.spec.ColSpec) nav.parameters().get(1)).name());
        LambdaFunction cond = (LambdaFunction) nav.parameters().get(3);
        AppliedFunction eq = (AppliedFunction) sole(cond.body());
        AppliedProperty srcCol = (AppliedProperty) eq.parameters().get(0);
        assertEquals("PARENT_ID", srcCol.property());
        assertEquals("Person_Firm",
                ((AppliedProperty) srcCol.receiver()).property(),
                "Self-join intermediate hop doesn't break the prior-hop sub-row read");
        assertEquals("ID", propOf(eq.parameters().get(1)),
                "{target}.ID resolves to $t.ID even with self-join intermediate");
    }

    // ====================================================================
    // R4.4 — Parallel JoinNavigation chains in PM bodies
    //
    // RelationalOperation.JoinNavigation can appear anywhere inside an
    // Expression PM body, LocalProperty wrapper, filter condition, or
    // groupBy key. emitAllJoins' Pass 2 walks every such RelationalOp
    // and hoists each unique chain into the prelude before `map(...)`.
    // The body then translates JoinNavigation by computing the alias
    // and recursively translating the terminal in the prelude-augmented
    // table scope, so the chain target table resolves through
    // $row.<alias>.<col>. emitChain dedupes shared chains.
    // ====================================================================

    @Test
    @DisplayName("expressionPmTwoIndependentChains (R4.4)")
    void expressionPmTwoIndependentChains() {
        // `concat([DB] @Person_Firm | T_FIRM.NAME, ' - ',
        //         [DB] @Person_Addr | T_ADDR.STREET)`
        // — two independent JoinNavigation chains in one Expression PM
        // body. Each chain must hoist a separate join() in the prelude;
        // the project lambda references both via $row.<alias>.<col>.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { displayName: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER, ADDR_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Table T_ADDR   (ID INTEGER, STREET VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Person_Addr( T_PERSON.ADDR_ID = T_ADDR.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    displayName: concat([db::DB] @Person_Firm | T_FIRM.NAME, "
                        + "                        ' - ', "
                        + "                        [db::DB] @Person_Addr | T_ADDR.STREET) "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Pipeline: map(join(join(tableRef, ~Person_Firm), ~Person_Addr), ...)
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction outerJoin = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("join", outerJoin.function(),
                "Outer join wraps the second chain (Person_Addr)");
        AppliedFunction innerJoin = (AppliedFunction) outerJoin.parameters().get(0);
        assertEquals("join", innerJoin.function(),
                "Inner join wraps the first chain (Person_Firm)");
        AppliedFunction tableRef = (AppliedFunction) innerJoin.parameters().get(0);
        assertEquals("tableReference", tableRef.function());

        // Aliases: PM declaration order drives prelude order.
        assertEquals("Person_Firm",
                ((com.legend.model.spec.ColSpec) innerJoin.parameters().get(1)).name());
        assertEquals("Person_Addr",
                ((com.legend.model.spec.ColSpec) outerJoin.parameters().get(1)).name());

        // Project lambda: concat($row.Person_Firm.NAME, ' - ', $row.Person_Addr.STREET).
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        NewInstance ni = (NewInstance) ((AppliedFunction) sole(projectLambda.body()))
                .parameters().get(1);
        List<ValueSpecification> concat = plusChain(
                toOneInner(ni.properties().get("displayName").value()), 3);

        AppliedProperty firmName = (AppliedProperty) concat.get(0);
        assertEquals("NAME", firmName.property());
        assertEquals("Person_Firm",
                ((AppliedProperty) firmName.receiver()).property(),
                "First chain's terminal reads $row.Person_Firm.NAME");

        AppliedProperty addrStreet = (AppliedProperty) concat.get(2);
        assertEquals("STREET", addrStreet.property());
        assertEquals("Person_Addr",
                ((AppliedProperty) addrStreet.receiver()).property(),
                "Second chain's terminal reads $row.Person_Addr.STREET");

        // Load-bearing: the two chains are INDEPENDENT (both start from
        // the main row), NOT a two-hop chain. The second join's condition
        // lambda must read its source-side column from $s directly, NOT
        // through the previous alias. If Pass 2 mistakenly threaded the
        // second JoinNavigation as a continuation of the first, this
        // would resolve to $s.Person_Firm.ADDR_ID.
        LambdaFunction outerCond = (LambdaFunction) outerJoin.parameters().get(2);
        AppliedFunction outerEq = (AppliedFunction) sole(outerCond.body());
        AppliedProperty outerSrcCol = (AppliedProperty) outerEq.parameters().get(0);
        assertEquals("ADDR_ID", outerSrcCol.property());
        assertEquals(new Variable("s"), outerSrcCol.receiver(),
                "Second chain is independent — its source-side column reads "
              + "$s directly, not through the first chain's alias");
    }

    @Test
    @DisplayName("expressionPmChainPlusInlineColumn (R4.4)")
    void expressionPmChainPlusInlineColumn() {
        // One chain + a main-table column ref in the SAME expression.
        // The chain hoists one join; the main column resolves through
        // $row directly. Pins coexistence of joined-row and main-row
        // reads in a single body.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { line: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER, NAME VARCHAR(50)) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    line: concat(T_PERSON.NAME, '@', "
                        + "                 [db::DB] @Person_Firm | T_FIRM.LEGAL_NAME) "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Exactly one join in the prelude.
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction joinCall = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("join", joinCall.function());
        assertEquals("tableReference",
                ((AppliedFunction) joinCall.parameters().get(0)).function(),
                "Single-chain mixed expression hoists exactly one join");

        // Project: concat($row.NAME, '@', $row.Person_Firm.LEGAL_NAME).
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        NewInstance ni = (NewInstance) ((AppliedFunction) sole(projectLambda.body()))
                .parameters().get(1);
        List<ValueSpecification> concat = plusChain(
                toOneInner(ni.properties().get("line").value()), 3);
        AppliedProperty mainCol = (AppliedProperty) concat.get(0);
        assertEquals("NAME", mainCol.property());
        assertEquals(new Variable("row"), mainCol.receiver(),
                "Main-table column reads $row directly");
        AppliedProperty firmCol = (AppliedProperty) concat.get(2);
        assertEquals("LEGAL_NAME", firmCol.property());
        assertEquals("Person_Firm",
                ((AppliedProperty) firmCol.receiver()).property(),
                "Chain terminal reads $row.Person_Firm.LEGAL_NAME");
    }

    @Test
    @DisplayName("filterConditionWithJoinNavigation (R4.4)")
    void filterConditionWithJoinNavigation() {
        // Direct filter condition contains a JoinNavigation:
        //   Filter ActiveFirms( [db]@Person_Firm | T_FIRM.IS_ACTIVE = 1 )
        // The chain must hoist BEFORE the filter; the filter's
        // condition lambda references $row.Person_Firm.IS_ACTIVE.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER, NAME VARCHAR(50)) "
                        + "  Table T_FIRM   (ID INTEGER, IS_ACTIVE INTEGER) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Filter ActiveFirms( [db::DB] @Person_Firm | T_FIRM.IS_ACTIVE = 1 ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~filter [db::DB] ActiveFirms "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name: T_PERSON.NAME "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Pipeline shape: map(filter(join(tableRef, ~Person_Firm), <cond>), ...)
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction filterCall = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("filter", filterCall.function());
        AppliedFunction joinCall = (AppliedFunction) filterCall.parameters().get(0);
        assertEquals("join", joinCall.function(),
                "Chain from the filter condition is hoisted BEFORE the filter");
        assertEquals("Person_Firm",
                ((com.legend.model.spec.ColSpec) joinCall.parameters().get(1)).name());

        // Filter condition: $row.Person_Firm.IS_ACTIVE == 1
        LambdaFunction filterLambda = (LambdaFunction) filterCall.parameters().get(1);
        AppliedFunction eq = (AppliedFunction) sole(filterLambda.body());
        assertEquals("equal", eq.function());
        AppliedProperty isActive = (AppliedProperty) eq.parameters().get(0);
        assertEquals("IS_ACTIVE", isActive.property());
        assertEquals("Person_Firm",
                ((AppliedProperty) isActive.receiver()).property(),
                "Filter condition reads the hoisted chain's sub-row");
    }

    @Test
    @DisplayName("expressionPmChainSharedWithStructuralJtcDeduped (R4.4)")
    void expressionPmChainSharedWithStructuralJtcDeduped() {
        // One PM is a JTC `firmName: @Person_Firm | T_FIRM.NAME`, another
        // is an Expression `tagLine: concat(@Person_Firm | T_FIRM.NAME, '!')`.
        // Both reference the SAME Person_Firm chain. emitChain must
        // dedupe via aliasToTargetTable — exactly one join in the prelude.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { firmName: String[1]; tagLine: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    firmName: [db::DB] @Person_Firm | T_FIRM.NAME, "
                        + "    tagLine:  concat([db::DB] @Person_Firm | T_FIRM.NAME, '!') "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Exactly ONE join: shape map(join(tableRef, ~Person_Firm), ...).
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction joinCall = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("join", joinCall.function());
        AppliedFunction underJoin = (AppliedFunction) joinCall.parameters().get(0);
        assertEquals("tableReference", underJoin.function(),
                "Structural JTC and nested JoinNavigation share one join (dedup)");
    }

    @Test
    @DisplayName("localPropertyBodyWithJoinNavigation (R4.4)")
    void localPropertyBodyWithJoinNavigation() {
        // A +local property whose body is an Expression containing a
        // JoinNavigation must hoist the chain. The LocalProperty arm of
        // collectJoinNavigationsInPMs is the only thing standing between
        // "works" and "throws"; without it the visitor silently skips
        // the wrapped body and translateRelOp later throws.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (ID INTEGER, FIRM_ID INTEGER, NAME VARCHAR(50)) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name: T_PERSON.NAME, "
                        + "    +displayFirm: String[1]: concat([db::DB] @Person_Firm | T_FIRM.LEGAL_NAME, '*') "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Pipeline shape: map(join(tableRef, ~Person_Firm), ...) — the
        // chain from the LocalProperty body has been hoisted.
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction joinCall = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("join", joinCall.function(),
                "Chain inside a LocalProperty body is hoisted into the prelude");
        assertEquals("Person_Firm",
                ((com.legend.model.spec.ColSpec) joinCall.parameters().get(1)).name());

        // The +local field's value is the translated expression reading
        // $row.Person_Firm.LEGAL_NAME.
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        NewInstance ni = (NewInstance) ((AppliedFunction) sole(projectLambda.body()))
                .parameters().get(1);
        KeyExpression displayKe = ni.properties().get("displayFirm");
        assertTrue(displayKe.isLocal(),
                "displayFirm is a +local field (isLocal=true)");
        AppliedFunction concat = (AppliedFunction) displayKe.value();
        assertEquals("plus", concat.function());   // concat -> plus-chain
        AppliedProperty subRowCol = (AppliedProperty) unwrapToString(concat.parameters().get(0));
        assertEquals("LEGAL_NAME", subRowCol.property());
        assertEquals("Person_Firm",
                ((AppliedProperty) subRowCol.receiver()).property(),
                "LocalProperty body resolves the chain terminal through "
              + "$row.Person_Firm.LEGAL_NAME");
    }

    @Test
    @DisplayName("multiHopJoinNavigationInExpression (R4.4)")
    void multiHopJoinNavigationInExpression() {
        // Nested expressions can carry MULTI-HOP JoinNavigation chains:
        //   concat([db] @Person_Firm > @Firm_Org | T_ORG.NAME, '!')
        // The hoisted chain must be a chained prelude (two joins, with
        // hop 2's source side reaching hop 1's target through the hop-1
        // alias) — same invariant pinned by threeHopChain for structural
        // JTCs, but here the chain is nested inside a function call body.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { tagline: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (FIRM_ID INTEGER, ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, ORG_ID INTEGER) "
                        + "  Table T_ORG    (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  Join Firm_Org   ( T_FIRM.ORG_ID    = T_ORG.ID  ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    tagline: concat([db::DB] @Person_Firm > @Firm_Org | T_ORG.NAME, '!') "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Pipeline shape: map(join(join(tableRef, ~Person_Firm), ~Person_Firm__Firm_Org), ...)
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction h2Join = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("join", h2Join.function());
        assertEquals("Person_Firm__Firm_Org",
                ((com.legend.model.spec.ColSpec) h2Join.parameters().get(1)).name(),
                "Hop 2 alias = join-name concatenation");
        AppliedFunction h1Join = (AppliedFunction) h2Join.parameters().get(0);
        assertEquals("join", h1Join.function());
        assertEquals("Person_Firm",
                ((com.legend.model.spec.ColSpec) h1Join.parameters().get(1)).name());
        AppliedFunction tableRef = (AppliedFunction) h1Join.parameters().get(0);
        assertEquals("tableReference", tableRef.function());

        // Hop 2's condition must reach T_FIRM (hop-1 target) through
        // $s.Person_Firm — the chained-source-path invariant.
        LambdaFunction h2Cond = (LambdaFunction) h2Join.parameters().get(2);
        AppliedFunction eq = (AppliedFunction) sole(h2Cond.body());
        AppliedProperty leftCol = (AppliedProperty) eq.parameters().get(0);
        assertEquals("ORG_ID", leftCol.property());
        AppliedProperty leftSub = (AppliedProperty) leftCol.receiver();
        assertEquals("Person_Firm", leftSub.property(),
                "Hop 2's source side reads through the hop-1 alias, not $s directly");
        assertEquals(new Variable("s"), leftSub.receiver());

        // Project body reads the chain terminal:
        // concat($row.Person_Firm__Firm_Org.NAME, '!').
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        NewInstance ni = (NewInstance) ((AppliedFunction) sole(projectLambda.body()))
                .parameters().get(1);
        AppliedFunction concat = (AppliedFunction) toOneInner(ni.properties().get("tagline").value());
        AppliedProperty terminal = (AppliedProperty) unwrapToString(concat.parameters().get(0));
        assertEquals("NAME", terminal.property());
        assertEquals("Person_Firm__Firm_Org",
                ((AppliedProperty) terminal.receiver()).property(),
                "Terminal reads through the FINAL hop's alias");
    }

    @Test
    @DisplayName("groupByKeyWithJoinNavigation (R4.4)")
    void groupByKeyWithJoinNavigation() {
        // ~groupBy([DB] @Person_Firm | T_FIRM.NAME) — a groupBy key
        // is itself a JoinNavigation. The chain must hoist before
        // groupBy; the key ColSpec's value lambda reads
        // $row.Person_Firm.NAME.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Bucket { firmName: String[1]; count: Integer[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (FIRM_ID INTEGER, ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Bucket: Relational { "
                        + "    ~groupBy([db::DB] @Person_Firm | T_FIRM.NAME) "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    firmName: [db::DB] @Person_Firm | T_FIRM.NAME, "
                        + "    count:    count(T_PERSON.ID) "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Pipeline: map(groupBy(join(tableRef, ~Person_Firm), ~[keys], ~[aggs]), ...)
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction groupBy = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("groupBy", groupBy.function());
        AppliedFunction joinCall = (AppliedFunction) groupBy.parameters().get(0);
        assertEquals("join", joinCall.function(),
                "Chain from the groupBy key is hoisted BEFORE the groupBy");

        // Key ColSpec lambda body: $row.Person_Firm.NAME
        com.legend.model.spec.ColSpecArray keyArr =
                (com.legend.model.spec.ColSpecArray) groupBy.parameters().get(1);
        com.legend.model.spec.ColSpec firmKey = keyArr.colSpecs().get(0);
        AppliedProperty terminal = (AppliedProperty) sole(firmKey.function1().body());
        assertEquals("NAME", terminal.property());
        assertEquals("Person_Firm",
                ((AppliedProperty) terminal.receiver()).property(),
                "groupBy key lambda reads through the hoisted chain's sub-row");
    }

    // ====================================================================
    // R4.1 — `~groupBy` consumes declared keys verbatim
    //
    // The mapping's `~groupBy(...)` is the authoritative key list.
    // Keys are emitted into the `groupBy` call's first ColSpecArray
    // verbatim; non-agg PMs whose body matches a key adopt the PM's
    // name on that key; aggregate-shaped PMs emit one agg-spec each;
    // per-row formulas outside the keys are rejected.
    // ====================================================================

    @Test
    @DisplayName("groupByExplicitKeysVerbatimFromMapping (R4.1)")
    void groupByExplicitKeysVerbatimFromMapping() {
        // Two declared keys + one aggregate. The emitted groupBy must
        // carry exactly two keys in declaration order, even though the
        // class has three properties total.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Position { acctNum: String[1]; gsn: String[1]; quantity: Integer[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_TRADE (ACC_NUM VARCHAR(50), GSN VARCHAR(50), QTY INTEGER) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Position: Relational { "
                        + "    ~groupBy([db::DB] T_TRADE.ACC_NUM, [db::DB] T_TRADE.GSN) "
                        + "    ~mainTable [db::DB] T_TRADE "
                        + "    acctNum:  T_TRADE.ACC_NUM, "
                        + "    gsn:      T_TRADE.GSN, "
                        + "    quantity: sum(T_TRADE.QTY) "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Pipeline shape: map(groupBy(tableRef, ~[keys], ~[aggs]), row | ^Position(...))
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        assertEquals("map", mapCall.function());
        AppliedFunction groupBy = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("groupBy", groupBy.function(),
                "Pipeline must include a groupBy step when ~groupBy is declared");
        AppliedFunction tableRef = (AppliedFunction) groupBy.parameters().get(0);
        assertEquals("tableReference", tableRef.function());

        // Key ColSpecArray: exactly 2 keys, matching declaration order.
        com.legend.model.spec.ColSpecArray keyArr =
                (com.legend.model.spec.ColSpecArray) groupBy.parameters().get(1);
        assertEquals(2, keyArr.colSpecs().size(),
                "Key count matches rcm.groupBy() verbatim — no inferred keys");

        // Both keys match a non-agg PM, so they adopt the PM names
        // (acctNum, gsn) — the constructor reads $row.acctNum / $row.gsn.
        assertEquals("acctNum", keyArr.colSpecs().get(0).name());
        assertEquals("gsn",     keyArr.colSpecs().get(1).name());

        // Agg ColSpecArray: exactly 1 agg-spec for `quantity`.
        com.legend.model.spec.ColSpecArray aggArr =
                (com.legend.model.spec.ColSpecArray) groupBy.parameters().get(2);
        assertEquals(1, aggArr.colSpecs().size());
        assertEquals("quantity", aggArr.colSpecs().get(0).name());
    }

    @Test
    @DisplayName("groupByKeyNotExposedAsPm (R4.1 identity-only key)")
    void groupByKeyNotExposedAsPm() {
        // ~groupBy(ACC_NUM, PRODUCT_ID, GSN) — but only acctNum and gsn
        // are exposed as PMs; PRODUCT_ID is identity-only. Engine groups
        // by it for entity identity but never projects it. Core's
        // emission must carry it in the groupBy keys with a synth name
        // (`k<i>__PRODUCT_ID`) and NOT in the terminal constructor.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Position { acctNum: String[1]; gsn: String[1]; quantity: Integer[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_TRADE (ACC_NUM VARCHAR(50), PRODUCT_ID INTEGER, GSN VARCHAR(50), QTY INTEGER) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Position: Relational { "
                        + "    ~groupBy([db::DB] T_TRADE.ACC_NUM, [db::DB] T_TRADE.PRODUCT_ID, [db::DB] T_TRADE.GSN) "
                        + "    ~mainTable [db::DB] T_TRADE "
                        + "    acctNum:  T_TRADE.ACC_NUM, "
                        + "    gsn:      T_TRADE.GSN, "
                        + "    quantity: sum(T_TRADE.QTY) "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        AppliedFunction groupBy = (AppliedFunction) mapCall.parameters().get(0);
        com.legend.model.spec.ColSpecArray keyArr =
                (com.legend.model.spec.ColSpecArray) groupBy.parameters().get(1);
        assertEquals(3, keyArr.colSpecs().size(),
                "All three keys appear in groupBy, including the identity-only PRODUCT_ID");

        // Key 0 (ACC_NUM) → adopted PM name `acctNum`.
        // Key 1 (PRODUCT_ID) → synth name `k1__PRODUCT_ID` (no PM matches).
        // Key 2 (GSN) → adopted PM name `gsn`.
        assertEquals("acctNum",         keyArr.colSpecs().get(0).name());
        assertEquals("k1__PRODUCT_ID",  keyArr.colSpecs().get(1).name(),
                "Identity-only key gets a synth `k<i>__<col>` name");
        assertEquals("gsn",             keyArr.colSpecs().get(2).name());

        // Constructor only knows acctNum, gsn, quantity — PRODUCT_ID is
        // never projected.
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        NewInstance ni = (NewInstance) ((AppliedFunction) sole(projectLambda.body()))
                .parameters().get(1);
        assertEquals(List.of("acctNum", "gsn", "quantity"),
                List.copyOf(ni.properties().keySet()),
                "Identity-only keys are absent from the terminal ^Class(...)");
    }

    @Test
    @DisplayName("groupByAggregatePmSplitsIntoSelectorAndAggregator (R4.1)")
    void groupByAggregatePmSplitsIntoSelectorAndAggregator() {
        // sum(QTY) splits into a selector lambda (row | $row.QTY) and an
        // aggregator lambda (vals | $vals -> sum()) on the ColSpec.
        ParsedModel parsed = ElementParser.parse(
                "Class model::P { k: String[1]; total: Integer[1]; } "
                        + "Database db::DB ( "
                        + "  Table T (K VARCHAR(10), QTY INTEGER) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::P: Relational { "
                        + "    ~groupBy([db::DB] T.K) "
                        + "    ~mainTable [db::DB] T "
                        + "    k:     T.K, "
                        + "    total: sum(T.QTY) "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));
        AppliedFunction groupBy = (AppliedFunction) ((AppliedFunction) sole(fn.body()))
                .parameters().get(0);
        com.legend.model.spec.ColSpecArray aggArr =
                (com.legend.model.spec.ColSpecArray) groupBy.parameters().get(2);
        com.legend.model.spec.ColSpec totalSpec = aggArr.colSpecs().get(0);
        assertEquals("total", totalSpec.name());

        // Selector: {row | $row.QTY}
        LambdaFunction selector = totalSpec.function1();
        assertEquals(List.of(new Variable("row")), selector.parameters());
        AppliedProperty qtyRef = (AppliedProperty) sole(selector.body());
        assertEquals("QTY", qtyRef.property());

        // Aggregator: {vals | sum($vals)}
        LambdaFunction aggregator = totalSpec.function2();
        assertEquals(1, aggregator.parameters().size(),
                "Aggregator lambda binds a single 'vals' variable");
        AppliedFunction aggCall = (AppliedFunction) sole(aggregator.body());
        assertEquals("sum", aggCall.function());
    }

    @Test
    @DisplayName("groupByPerRowFormulaOutsideKeyRejected (R4.1)")
    void groupByPerRowFormulaOutsideKeyRejected() {
        // PM `extra: T.K + '_x'` is a per-row formula. It's neither an
        // aggregate nor a declared groupBy key, so the normalizer must
        // refuse — otherwise SQL would reject the GROUP BY clause.
        ParsedModel parsed = ElementParser.parse(
                "Class model::P { k: String[1]; extra: String[1]; total: Integer[1]; } "
                        + "Database db::DB ( "
                        + "  Table T (K VARCHAR(10), QTY INTEGER) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::P: Relational { "
                        + "    ~groupBy([db::DB] T.K) "
                        + "    ~mainTable [db::DB] T "
                        + "    k:     T.K, "
                        + "    extra: concat(T.K, '_x'), "
                        + "    total: sum(T.QTY) "
                        + "  } "
                        + ")");
        String exMsg = poisonReasons(parsed);
        assertTrue(exMsg.contains("extra"),
                () -> "Expected error to name the orphan PM; got: " + exMsg);
        assertTrue(exMsg.contains("~groupBy"),
                () -> "Expected error to reference ~groupBy; got: " + exMsg);
    }

    @Test
    @DisplayName("groupByConstructorReadsRowByPmName (R4.1)")
    void groupByConstructorReadsRowByPmName() {
        // The terminal ^Class(...) reads each property as $row.<pmName>,
        // NOT as $row.<originalColumn>. The key ColSpec was aliased to
        // the PM name in applyGroupBy precisely so this dispatch works.
        ParsedModel parsed = ElementParser.parse(
                "Class model::P { k: String[1]; total: Integer[1]; } "
                        + "Database db::DB ( "
                        + "  Table T (K VARCHAR(10), QTY INTEGER) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::P: Relational { "
                        + "    ~groupBy([db::DB] T.K) "
                        + "    ~mainTable [db::DB] T "
                        + "    k:     T.K, "
                        + "    total: sum(T.QTY) "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        NewInstance ni = (NewInstance) ((AppliedFunction) sole(projectLambda.body()))
                .parameters().get(1);

        // k = $row.k  (not $row.K — the key was aliased to the PM name)
        AppliedProperty kVal = (AppliedProperty) toOneInner(ni.properties().get("k").value());
        assertEquals("k", kVal.property(),
                "Constructor reads the key by its aliased PM name, not the raw column");
        assertEquals(new Variable("row"), kVal.receiver());

        // total = $row.total  (the agg-spec's emitted column)
        AppliedProperty totalVal = (AppliedProperty) toOneInner(ni.properties().get("total").value());
        assertEquals("total", totalVal.property());
        assertEquals(new Variable("row"), totalVal.receiver());
    }

    // ====================================================================
    // R2 — mapped-class index validation
    //
    // The normalizer builds a Set<String> of every class FQN that has a
    // ClassMapping somewhere in the model. Building this set ALSO enforces
    // the design's multi-set-id deferral: within ONE LegacyMappingDefinition, the
    // same class FQN must not appear in two ClassMappings.
    // ====================================================================

    @Test
    @DisplayName("R2: duplicate ClassMapping within one LegacyMappingDefinition is rejected")
    void duplicateClassMappingWithinOneMappingIsRejected() {
        // Two M2M ClassMappings, both targeting model::Person, in a single
        // LegacyMappingDefinition. Differ only by setId (one default, one named).
        // The deferred multi-set-id path should trip the index-build check.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "Class model::SrcA { name: String[1]; } "
                        + "Class model::SrcB { name: String[1]; } "
                        + "Mapping my::M ( "
                        + "  *model::Person: Pure { ~src model::SrcA name: $src.name } "
                        + "  *model::Person[alt]: Pure { ~src model::SrcB name: $src.name } "
                        + ")");
        com.legend.error.ModelException ex = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.ModelException.class,
                () -> normalizeViaPipeline(parsed));
        assertTrue(ex.getMessage().contains("model::Person"),
                () -> "Expected duplicate-mapping error to name the class; got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("multiple ClassMappings"),
                () -> "Expected error to mention 'multiple ClassMappings'; got: " + ex.getMessage());
    }

    @Test
    @DisplayName("R2: same class mapped in two distinct LegacyMappingDefinitions is allowed")
    void sameClassInTwoMappingsIsAllowed() {
        // Each LegacyMappingDefinition is its own setId namespace; mapping the
        // same class from two LegacyMappingDefinitions is a legitimate setup
        // (e.g. test vs prod). The R2 index is a Set, idempotent on union.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "Class model::SrcA { name: String[1]; } "
                        + "Class model::SrcB { name: String[1]; } "
                        + "Mapping my::MA ( "
                        + "  *model::Person: Pure { ~src model::SrcA name: $src.name } "
                        + ") "
                        + "Mapping my::MB ( "
                        + "  *model::Person: Pure { ~src model::SrcB name: $src.name } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        // Both mappings get a synth; nothing thrown. Each lifts a distinct
        // FQN, both ordinary top-level elements.
        List<FunctionDefinition> lifted = liftedFunctions(normalized);
        assertEquals(2, lifted.size(),
                "Both LegacyMappingDefinitions should produce one synth each");
        assertEquals(java.util.Set.of("my::MA$class$model::Person", "my::MB$class$model::Person"),
                lifted.stream().map(FunctionDefinition::qualifiedName)
                        .collect(Collectors.toSet()));
    }

    @Test
    @DisplayName("same simple name, different packages, one mapping: lifted FQNs do NOT collide")
    void sameSimpleNameDifferentPackagesDoNotCollide() {
        // The lifted FQN embeds the FULL class FQN, so a::Person and b::Person
        // mapped in the SAME mapping lift to distinct functions. A simple-name
        // scheme (<mapping>$class$Person) would collapse both to one FQN and
        // silently drop a mapping from findFunction — the injectivity bug the
        // full-FQN encoding rules out by construction (CLEAN_SHEET_INVERSION §3).
        ParsedModel parsed = ElementParser.parse(
                "Class a::Person { name: String[1]; } "
                        + "Class b::Person { name: String[1]; } "
                        + "Class src::RawA { name: String[1]; } "
                        + "Class src::RawB { name: String[1]; } "
                        + "Mapping my::M ( "
                        + "  *a::Person: Pure { ~src src::RawA name: $src.name } "
                        + "  *b::Person: Pure { ~src src::RawB name: $src.name } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        List<FunctionDefinition> lifted = liftedFunctions(normalized);
        assertEquals(java.util.Set.of("my::M$class$a::Person", "my::M$class$b::Person"),
                lifted.stream().map(FunctionDefinition::qualifiedName)
                        .collect(Collectors.toSet()),
                "two same-simple-name classes must lift to distinct, non-colliding FQNs");
        // Both resolve independently through the one findFunction index.
        ModelBuilder mb = ModelBuilder.from(new com.legend.model.ParsedModel(normalized.elements(), normalized.imports()));
        assertEquals(1, mb.findFunction("my::M$class$a::Person").size());
        assertEquals(1, mb.findFunction("my::M$class$b::Person").size());
    }

    // ====================================================================
    // AssociationMapping → legacyAssocPredicate  —  doc §5.6.1
    //
    // A Relational AssociationMapping synthesizes one
    // (A[1], B[1]) -> Boolean[1] predicate function whose body wraps the
    // join condition in legacyAssocPredicate(a, b, {srcRow, tgtRow|cond}).
    // ====================================================================

    @Test
    @DisplayName("associationMapping_singleHop_emitsLegacyAssocPredicate")
    void associationMapping_singleHop_emitsLegacyAssocPredicate() {
        // Association ends: firm -> model::Firm (end1 = classA),
        //                   person -> model::Person (end2 = classB).
        // classA's main table (T_FIRM) is the predicate's source row; the
        // join's other table (T_PERSON) is the target row.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Firm   { id: Integer[1]; } "
                        + "Class model::Person { firmId: Integer[1]; } "
                        + "Association model::Person_Firm { "
                        + "  firm: model::Firm[1]; person: model::Person[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_FIRM   (ID INTEGER) "
                        + "  Table T_PERSON (FIRM_ID INTEGER) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Firm: Relational { ~mainTable [db::DB] T_FIRM  id: T_FIRM.ID } "
                        + "  *model::Person: Relational { ~mainTable [db::DB] T_PERSON  firmId: T_PERSON.FIRM_ID } "
                        + "  model::Person_Firm: Relational { AssociationMapping ( "
                        + "    firm: [db::DB] @Person_Firm "
                        + "  ) } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition assocFn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().endsWith("::Person_Firm"))
                .findFirst().orElseThrow();

        // Signature: (a: model::Firm[1], b: model::Person[1]) -> Boolean[1].
        assertEquals(2, assocFn.parameters().size());
        assertEquals("a", assocFn.parameters().get(0).name());
        assertEquals("model::Firm",
                ((com.legend.model.TypeExpression.NameRef) assocFn.parameters().get(0).type()).name());
        assertEquals("b", assocFn.parameters().get(1).name());
        assertEquals("model::Person",
                ((com.legend.model.TypeExpression.NameRef) assocFn.parameters().get(1).type()).name());
        assertEquals("meta::pure::metamodel::type::Boolean",
                ((com.legend.model.TypeExpression.NameRef) assocFn.returnType()).name());

        // Body: legacyAssocPredicate($a, $b, #>{srcTable}#, #>{tgtTable}#,
        // {srcRow, tgtRow | cond}) — the two ends' ~mainTable rows are
        // spelled in the call so the adapter lambda's rows TYPE (no Any).
        AppliedFunction lap = (AppliedFunction) sole(assocFn.body());
        assertEquals("legacyAssocPredicate", lap.function());
        assertEquals(new Variable("a"), lap.parameters().get(0));
        assertEquals(new Variable("b"), lap.parameters().get(1));
        AppliedFunction srcRef = (AppliedFunction) lap.parameters().get(2);
        assertEquals("tableReference", srcRef.function());
        assertEquals(new com.legend.model.spec.CString("T_FIRM"),
                srcRef.parameters().get(1), "src row source is classA's (Firm) ~mainTable");
        AppliedFunction tgtRef = (AppliedFunction) lap.parameters().get(3);
        assertEquals("tableReference", tgtRef.function());
        assertEquals(new com.legend.model.spec.CString("T_PERSON"),
                tgtRef.parameters().get(1), "tgt row source is classB's (Person) ~mainTable");
        LambdaFunction adapter = (LambdaFunction) lap.parameters().get(4);
        assertEquals(List.of(new Variable("srcRow"), new Variable("tgtRow")),
                adapter.parameters(),
                "adapter lambda binds the two physical-row parameters");

        // cond: equal($tgtRow.FIRM_ID, $srcRow.ID). srcRow is classA's
        // (Firm) main-table row T_FIRM; tgtRow is the join's other table.
        AppliedFunction eq = (AppliedFunction) sole(adapter.body());
        assertEquals("equal", eq.function());
        java.util.Map<String, String> recvByCol = new java.util.HashMap<>();
        for (ValueSpecification arg : eq.parameters()) {
            AppliedProperty ap = (AppliedProperty) arg;
            recvByCol.put(ap.property(), ((Variable) ap.receiver()).name());
        }
        assertEquals("tgtRow", recvByCol.get("FIRM_ID"),
                "T_PERSON.FIRM_ID is the join's target-table side -> $tgtRow");
        assertEquals("srcRow", recvByCol.get("ID"),
                "T_FIRM.ID is classA's source-table side -> $srcRow");
    }

    // ====================================================================
    // ~primaryKey is parsed but NOT lowered  —  doc §5.3.6
    // ====================================================================

    @Test
    @DisplayName("primaryKey_isParsedButNotLowered")
    void primaryKey_isParsedButNotLowered() {
        // ~primaryKey is object-identity metadata in the engine (used at
        // graph-fetch time, never as a row-level DISTINCT in the query;
        // HelperRelationalBuilder.processRelationalPrimaryKey). lite has no
        // graph-fetch consumer yet, so it is parsed but produces no pipeline
        // step: the body is just tableReference -> map, with no distinct /
        // distinctBy between source and map.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Trade { acct: String[1]; } "
                        + "Database db::DB ( Table T_TRADE (ACC_NUM VARCHAR(50)) ) "
                        + "Mapping my::M ( "
                        + "  *model::Trade: Relational { "
                        + "    ~primaryKey([db::DB] T_TRADE.ACC_NUM) "
                        + "    ~mainTable [db::DB] T_TRADE "
                        + "    acct: T_TRADE.ACC_NUM "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // ~primaryKey is still parsed onto the ClassMapping.
        var rcm = (com.legend.model.ClassMapping.Relational)
                firstMapping(parsed).classMappings().get(0);
        assertEquals(1, rcm.primaryKey().size(),
                "~primaryKey is parsed onto the ClassMapping");

        // But it adds no pipeline step: map's source is the bare tableReference.
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        assertEquals("map", mapCall.function());
        AppliedFunction source = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("tableReference", source.function(),
                "~primaryKey emits no distinct/distinctBy step: map reads the bare table");
    }

    // ====================================================================
    // M2M ~src chain cycle detection  —  doc §5.5
    // ====================================================================

    @Test
    @DisplayName("m2mSrcChainCycle_rejected")
    void m2mSrcChainCycle_rejected() {
        // A.~src = B and B.~src = A forms a circular source chain; the
        // normalizer's DFS must reject it with a clear diagnostic.
        ParsedModel parsed = ElementParser.parse(
                "Class model::A { x: String[1]; } "
                        + "Class model::B { x: String[1]; } "
                        + "Mapping my::M ( "
                        + "  *model::A: Pure { ~src model::B  x: $src.x } "
                        + "  *model::B: Pure { ~src model::A  x: $src.x } "
                        + ")");
        com.legend.error.ModelException ex = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.ModelException.class,
                () -> normalizeViaPipeline(parsed));
        assertTrue(ex.getMessage().contains("Circular M2M"),
                () -> "Expected circular-M2M diagnostic; got: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("model::A") && ex.getMessage().contains("model::B"),
                () -> "Expected both classes named in the cycle; got: " + ex.getMessage());
    }

    // ====================================================================
    // PM-name validation  —  a PM must target a declared property
    // ====================================================================

    @Test
    @DisplayName("pmName_notDeclaredOnClass_rejected")
    void pmName_notDeclaredOnClass_rejected() {
        // PM 'bogus' has no corresponding property on model::Person. The
        // normalizer rejects it (a +local would be exempt; this is not one).
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "Database db::DB ( Table T_PERSON (NAME VARCHAR(50), AGE INTEGER) ) "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    name:  T_PERSON.NAME, "
                        + "    bogus: T_PERSON.AGE "
                        + "  } "
                        + ")");
        String exMsg = poisonReasons(parsed);
        assertTrue(exMsg.contains("bogus"),
                () -> "Expected error to name the offending PM; got: " + exMsg);
        assertTrue(exMsg.contains("not declared"),
                () -> "Expected 'not declared' diagnostic; got: " + exMsg);
    }

    // ====================================================================
    // View-backed mapping  —  doc §5.3.7
    //
    // A ~mainTable that resolves to a View expands as a macro: PMs that
    // reference view columns are rewritten to the underlying physical
    // expression, and view-level ~filter/~distinct/~groupBy layer onto
    // the pipeline.
    // ====================================================================

    @Test
    @DisplayName("viewBackedMapping_rewritesColumnsAndMergesViewDistinct")
    void viewBackedMapping_rewritesColumnsAndMergesViewDistinct() {
        // ~mainTable resolves to View V_PERSON. The PM `name: V_PERSON.pname`
        // rewrites through the view's column mapping (pname -> T_PERSON.NAME),
        // and the view-level ~distinct layers a distinct() into the pipeline.
        // Engine parity: the source is the view's inferred underlying physical
        // table (T_PERSON), not the view name.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (NAME VARCHAR(50)) "
                        + "  View V_PERSON ( ~distinct  pname: T_PERSON.NAME ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] V_PERSON "
                        + "    name: V_PERSON.pname "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Pipeline: tableReference(db::DB, T_PERSON) -> distinct() -> map.
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        assertEquals("map", mapCall.function());
        AppliedFunction distinctCall = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("distinct", distinctCall.function(),
                "view-level ~distinct merges into the pipeline as distinct()");
        AppliedFunction select = (AppliedFunction) distinctCall.parameters().get(0);
        assertEquals("select", select.function(),
                "~distinct dedups the MAPPED columns: select narrows first");
        AppliedFunction tableRef = (AppliedFunction) select.parameters().get(0);
        assertEquals("tableReference", tableRef.function());
        assertEquals("T_PERSON",
                ((com.legend.model.spec.CString) tableRef.parameters().get(1)).value(),
                "source is the view's inferred underlying physical table, not the view name");

        // The PM rewrote through the view column: name reads the underlying
        // physical column NAME off the (view) row.
        LambdaFunction projectLambda = (LambdaFunction) mapCall.parameters().get(1);
        NewInstance ni = (NewInstance) ((AppliedFunction) sole(projectLambda.body()))
                .parameters().get(1);
        AppliedProperty nameVal = (AppliedProperty) toOneInner(ni.properties().get("name").value());
        assertEquals("NAME", nameVal.property(),
                "view column pname rewrote to its underlying physical column NAME");
        assertEquals(new Variable("row"), nameVal.receiver());
    }

    @Test
    @DisplayName("viewWithMultipleRootTables_throws")
    void viewWithMultipleRootTables_throws() {
        // A view whose non-join columns reference two different physical
        // tables cannot resolve to a single root; inferViewMainTable must
        // reject it loudly (engine parity: a view has one main table).
        ParsedModel parsed = ElementParser.parse(
                "Class model::Mix { a: String[1]; b: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_A (X VARCHAR(50)) "
                        + "  Table T_B (Y VARCHAR(50)) "
                        + "  View V_MIX ( a: T_A.X, b: T_B.Y ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Mix: Relational { "
                        + "    ~mainTable [db::DB] V_MIX "
                        + "    a: V_MIX.a, "
                        + "    b: V_MIX.b "
                        + "  } "
                        + ")");
        String exMsg = poisonReasons(parsed);
        assertTrue(exMsg.contains("V_MIX")
                        && exMsg.contains("single root table"),
                () -> "Expected a multi-root-table diagnostic naming V_MIX; got: "
                        + exMsg);
    }

    @Test
    @DisplayName("viewWithOnlyJoinBackedColumns_throws")
    void viewWithOnlyJoinBackedColumns_throws() {
        // Every view column navigates a join, so no column anchors the
        // view's own root table; inferViewMainTable must reject it (it has
        // nothing to use as the tableReference source).
        ParsedModel parsed = ElementParser.parse(
                "Class model::J { fname: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  View V_ALLJOIN ( fname: @Person_Firm | T_FIRM.LEGAL_NAME ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::J: Relational { "
                        + "    ~mainTable [db::DB] V_ALLJOIN "
                        + "    fname: V_ALLJOIN.fname "
                        + "  } "
                        + ")");
        String exMsg = poisonReasons(parsed);
        assertTrue(exMsg.contains("V_ALLJOIN")
                        && exMsg.contains("cannot infer underlying main table"),
                () -> "Expected a no-root-table diagnostic naming V_ALLJOIN; got: "
                        + exMsg);
    }

    @Test
    @DisplayName("viewColumnBackedByJoin_rewritesToJoinTerminalColumn")
    void viewColumnBackedByJoin_rewritesToJoinTerminalColumn() {
        // A view column whose expression is a JoinNavigation with a column
        // terminal (@Person_Firm | T_FIRM.LEGAL_NAME) rewrites a referencing
        // PM into a JoinTerminalColumn: the pipeline emits a join() step over
        // the inferred root table (T_PERSON), and the property reads the
        // terminal column off the joined sub-row.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; firmName: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (NAME VARCHAR(50), FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, LEGAL_NAME VARCHAR(50)) "
                        + "  Join Person_Firm( T_PERSON.FIRM_ID = T_FIRM.ID ) "
                        + "  View V_PERSON ( "
                        + "    pname:     T_PERSON.NAME, "
                        + "    pfirmName: @Person_Firm | T_FIRM.LEGAL_NAME "
                        + "  ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] V_PERSON "
                        + "    name:     V_PERSON.pname, "
                        + "    firmName: V_PERSON.pfirmName "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Pipeline: join(tableReference(T_PERSON), ~Person_Firm: ..., cond) -> map.
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        assertEquals("map", mapCall.function());
        AppliedFunction joinCall = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("join", joinCall.function(),
                "join-backed view column emits a join() step over the root table");
        AppliedFunction tableRef = (AppliedFunction) joinCall.parameters().get(0);
        assertEquals("tableReference", tableRef.function());
        assertEquals("T_PERSON",
                ((com.legend.model.spec.CString) tableRef.parameters().get(1)).value(),
                "source is the inferred physical root table");

        // firmName reads the terminal column off the joined sub-row:
        // $row.Person_Firm.LEGAL_NAME.
        NewInstance ni = ctorOf(fn);
        AppliedProperty firmVal = (AppliedProperty) toOneInner(ni.properties().get("firmName").value());
        assertEquals("LEGAL_NAME", firmVal.property(),
                "firmName rewrote to the join terminal column");
        AppliedProperty subRow = (AppliedProperty) firmVal.receiver();
        assertEquals("Person_Firm", subRow.property(),
                "terminal column is read off the join sub-row slot");
        assertEquals(new Variable("row"), subRow.receiver());
    }

    @Test
    @DisplayName("viewFilterAndMappingFilterAndDistinct_allThreeSurvive")
    void viewFilterAndMappingFilterAndDistinct_allThreeSurvive() {
        // AUDIT_2026_07 §16 probe — REFUTED and pinned: ~distinct wraps the
        // SOURCE pipeline (pre-map row dedup), not the map terminal, so the
        // both-filters layering composes with it. Sequencing: view filter ->
        // distinct (the view's contract) -> mapping filter -> map.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; age: Integer[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (NAME VARCHAR(50), AGE INTEGER, ACTIVE INTEGER) "
                        + "  Filter ActiveFilter ( T_PERSON.ACTIVE = 1 ) "
                        + "  Filter AdultFilter  ( T_PERSON.AGE > 18 ) "
                        + "  View V_PERSON ( "
                        + "    ~filter ActiveFilter "
                        + "    ~distinct "
                        + "    pname: T_PERSON.NAME, "
                        + "    page:  T_PERSON.AGE "
                        + "  ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~filter AdultFilter "
                        + "    ~mainTable [db::DB] V_PERSON "
                        + "    name: V_PERSON.pname, "
                        + "    age:  V_PERSON.page "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Pipeline: map(filter(distinct(filter(tableReference, view)), mapping)).
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        assertEquals("map", mapCall.function());
        AppliedFunction mappingFilter = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("filter", mappingFilter.function(),
                "the mapping ~filter applies over the deduped view rows");
        AppliedFunction distinct = (AppliedFunction) mappingFilter.parameters().get(0);
        assertEquals("distinct", distinct.function(),
                "~distinct must survive the filter layering (the view's contract)");
        AppliedFunction select = (AppliedFunction) distinct.parameters().get(0);
        assertEquals("select", select.function(),
                "~distinct dedups the MAPPED columns: select narrows first");
        AppliedFunction viewFilter = (AppliedFunction) select.parameters().get(0);
        assertEquals("filter", viewFilter.function(),
                "the view ~filter applies before the dedup");
        assertEquals("tableReference",
                ((AppliedFunction) viewFilter.parameters().get(0)).function());
    }

    @Test
    @DisplayName("mapping ~filter over a grouped class pipeline lands BELOW the groupBy (WHERE, not HAVING)")
    void viewFilterAndMappingFilterAndGroupBy_filterBelowAggregation() {
        // Audit 18 finding 6: the engine evaluates the view filter AND the
        // mapping filter both in WHERE position, BEFORE aggregation.
        // Layering the mapping filter over the grouped relation silently
        // turned WHERE amount > 10 into HAVING sum(amount) > 10 whenever a
        // grouped column kept the filtered physical name.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Sale { region: String[1]; amount: Integer[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_SALE (REGION VARCHAR(50), AMOUNT INTEGER, ACTIVE INTEGER) "
                        + "  Filter ActiveFilter ( T_SALE.ACTIVE = 1 ) "
                        + "  Filter BigFilter    ( T_SALE.AMOUNT > 10 ) "
                        + "  View V_SALE ( "
                        + "    ~filter ActiveFilter "
                        + "    vregion: T_SALE.REGION, "
                        + "    vamount: T_SALE.AMOUNT "
                        + "  ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Sale: Relational { "
                        + "    ~filter BigFilter "
                        + "    ~groupBy([db::DB] T_SALE.REGION) "
                        + "    ~mainTable [db::DB] V_SALE "
                        + "    region: V_SALE.vregion, "
                        + "    amount: sum(T_SALE.AMOUNT) "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Pipeline: map(groupBy(filter(filter(tableReference, view), mapping))).
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        assertEquals("map", mapCall.function());
        AppliedFunction groupBy = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("groupBy", groupBy.function(),
                "aggregation stays the outermost source op");
        AppliedFunction mappingFilter = (AppliedFunction) groupBy.parameters().get(0);
        assertEquals("filter", mappingFilter.function(),
                "the mapping ~filter applies BEFORE aggregation (WHERE, not HAVING)");
        AppliedFunction viewFilter = (AppliedFunction) mappingFilter.parameters().get(0);
        assertEquals("filter", viewFilter.function(),
                "the view ~filter applies first");
        assertEquals("tableReference",
                ((AppliedFunction) viewFilter.parameters().get(0)).function());
    }

    @Test
    @DisplayName("viewFilterAndMappingFilter_layerBothFilters")
    void viewFilterAndMappingFilter_layerBothFilters() {
        // A view carries its own ~filter AND the class mapping adds another
        // ~filter. Both must survive: the engine sequences the view filter
        // first, then the mapping filter, over the physical table rows. The
        // pipeline therefore nests two filter() steps over tableReference.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; age: Integer[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (NAME VARCHAR(50), AGE INTEGER, ACTIVE INTEGER) "
                        + "  Filter ActiveFilter ( T_PERSON.ACTIVE = 1 ) "
                        + "  Filter AdultFilter  ( T_PERSON.AGE > 18 ) "
                        + "  View V_PERSON ( "
                        + "    ~filter ActiveFilter "
                        + "    pname: T_PERSON.NAME, "
                        + "    page:  T_PERSON.AGE "
                        + "  ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~filter AdultFilter "
                        + "    ~mainTable [db::DB] V_PERSON "
                        + "    name: V_PERSON.pname, "
                        + "    age:  V_PERSON.page "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));

        // Pipeline: filter(filter(tableReference(T_PERSON), <view>), <mapping>) -> map.
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        assertEquals("map", mapCall.function());
        AppliedFunction outerFilter = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("filter", outerFilter.function(),
                "the mapping ~filter layers as the outer filter step");
        AppliedFunction innerFilter = (AppliedFunction) outerFilter.parameters().get(0);
        assertEquals("filter", innerFilter.function(),
                "the view ~filter remains as the inner filter step");
        AppliedFunction tableRef = (AppliedFunction) innerFilter.parameters().get(0);
        assertEquals("tableReference", tableRef.function());
        assertEquals("T_PERSON",
                ((com.legend.model.spec.CString) tableRef.parameters().get(1)).value());

        // Sanity: the two filter conditions read different columns (AGE vs
        // ACTIVE), proving neither filter was dropped or duplicated.
        Set<String> outerCols = new TreeSet<>();
        Set<String> innerCols = new TreeSet<>();
        collectPropertyNames(outerFilter.parameters().get(1), outerCols);
        collectPropertyNames(innerFilter.parameters().get(1), innerCols);
        assertTrue(outerCols.contains("AGE"),
                () -> "outer (mapping) filter should read AGE; read " + outerCols);
        assertTrue(innerCols.contains("ACTIVE"),
                () -> "inner (view) filter should read ACTIVE; read " + innerCols);
    }

    @Test
    @DisplayName("viewGroupBy_layersGroupByStep")
    void viewGroupBy_layersGroupByStep() {
        // A view-level ~groupBy is merged into the effective mapping and
        // emitted as a groupBy() pipeline step over the inferred root table.
        ParsedModel parsed = ElementParser.parse(
                "Class model::G { k: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T (K VARCHAR(10), V VARCHAR(10)) "
                        + "  View V_G ( ~groupBy([db::DB] T.K)  vk: T.K ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::G: Relational { "
                        + "    ~mainTable [db::DB] V_G "
                        + "    k: V_G.vk "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        assertEquals("map", mapCall.function());
        AppliedFunction groupBy = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("groupBy", groupBy.function(),
                "view-level ~groupBy merges into the pipeline as a groupBy() step");
        AppliedFunction tableRef = (AppliedFunction) groupBy.parameters().get(0);
        assertEquals("tableReference", tableRef.function());
        assertEquals("T",
                ((com.legend.model.spec.CString) tableRef.parameters().get(1)).value(),
                "groupBy runs over the inferred physical root table");
    }

    @Test
    @DisplayName("chainAliasCollision_singleJoinNamedLikeChainPrefix")
    void chainAliasCollision_singleJoinNamedLikeChainPrefix() {
        // #2 chain-alias ambiguity — now FIXED. The hop dedup key is the
        // STRUCTURED join path (ordered join names), not the lossy '__'-joined
        // string. A two-hop chain [A, B] and a single join literally NAMED
        // "A__B" therefore stay distinct sub-rows, so both T_END (chain
        // terminal) and T_OTHER (single-join target) are reachable. Slot
        // names stay human-readable ("A__B") and are disambiguated only on a
        // genuine collision; readers recover the slot via the structured
        // registry (Pipeline.pathToSlot), never by re-flattening.
        //
        // Before the fix this threw "ColumnRef references table 'T_OTHER'
        // not in scope" because the single join was deduped away.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { endName: String[1]; otherName: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_ROOT  (MID_ID INTEGER, OTHER_ID INTEGER) "
                        + "  Table T_MID   (ID INTEGER, END_ID INTEGER) "
                        + "  Table T_END   (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Table T_OTHER (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join A    ( T_ROOT.MID_ID = T_MID.ID ) "
                        + "  Join B    ( T_MID.END_ID = T_END.ID ) "
                        + "  Join A__B ( T_ROOT.OTHER_ID = T_OTHER.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_ROOT "
                        + "    endName:   [db::DB] @A > @B | T_END.NAME, "
                        + "    otherName: [db::DB] @A__B | T_OTHER.NAME "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));
        Set<String> tables = new TreeSet<>();
        collectTableReferenceTables(sole(fn.body()), tables);
        assertTrue(tables.contains("T_END"),
                () -> "chain [A, B] terminal T_END must be reachable; saw " + tables);
        assertTrue(tables.contains("T_OTHER"),
                () -> "single join @A__B target T_OTHER must be reachable (no collision "
                        + "with the [A, B] chain alias); saw " + tables);

        // The two properties read from DISTINCT sub-row slots.
        AppliedFunction mapCall = (AppliedFunction) sole(fn.body());
        LambdaFunction lambda = (LambdaFunction) mapCall.parameters().get(1);
        NewInstance ni = (NewInstance) ((AppliedFunction) sole(lambda.body()))
                .parameters().get(1);
        String endSlot   = ((AppliedProperty) ((AppliedProperty)
                toOneInner(ni.properties().get("endName").value())).receiver()).property();
        String otherSlot = ((AppliedProperty) ((AppliedProperty)
                toOneInner(ni.properties().get("otherName").value())).receiver()).property();
        org.junit.jupiter.api.Assertions.assertNotEquals(endSlot, otherSlot,
                "chain [A, B] and single join A__B must resolve to different slots");
    }

    // ====================================================================
    // JSON-source mapping  —  RelationalMapping.variantIdentity parity
    //
    // A Runtime's JsonModelConnection cross-bakes a sourceUrl-backed
    // ClassMapping; each property reads the single VARIANT `data` column
    // via to(get($row.data, 'prop'), @Type) — 2-arg get + typed `to`.
    // ====================================================================

    @Test
    @DisplayName("jsonSourceMapping_emitsToWrappedGet")
    void jsonSourceMapping_emitsToWrappedGet() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Raw { name: String[1]; } "
                        + "Mapping my::M ( ) "
                        + "Runtime my::R { mappings: [my::M]; connections: [ "
                        + "ModelStore: [ json: #{ JsonModelConnection { "
                        + "class: model::Raw; url: 'data:application/json,[]'; } }# ] ]; }");
        NormalizedModel normalized = normalizeViaPipeline(parsed);
        FunctionDefinition rawFn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().endsWith("::Raw"))
                .findFirst().orElseThrow(() ->
                        new AssertionError("expected a cross-baked synth fn for model::Raw"));

        // Source is sourceUrl('data:...').
        AppliedFunction mapCall = (AppliedFunction) sole(rawFn.body());
        AppliedFunction srcCall = (AppliedFunction) mapCall.parameters().get(0);
        assertEquals("sourceUrl", srcCall.function());

        // name = to(get($row.data, 'name'), @String).
        LambdaFunction lambda = (LambdaFunction) mapCall.parameters().get(1);
        NewInstance ni = (NewInstance) ((AppliedFunction) sole(lambda.body()))
                .parameters().get(1);
        AppliedFunction toCall = (AppliedFunction) toOneInner(ni.properties().get("name").value());
        assertEquals("to", toCall.function());
        assertEquals(2, toCall.parameters().size());

        AppliedFunction getCall = (AppliedFunction) toCall.parameters().get(0);
        assertEquals("get", getCall.function());
        assertEquals(2, getCall.parameters().size(),
                "get is the 2-arg variant accessor get(Variant, key)");
        AppliedProperty dataRef = (AppliedProperty) getCall.parameters().get(0);
        assertEquals("data", dataRef.property());
        assertEquals(new Variable("row"), dataRef.receiver());
        assertEquals("name",
                ((com.legend.model.spec.CString) getCall.parameters().get(1)).value());

        // Typed cast target @String.
        com.legend.model.spec.TypeAnnotation.Named typeAnn =
                (com.legend.model.spec.TypeAnnotation.Named) toCall.parameters().get(1);
        assertEquals("String",
                ((com.legend.model.TypeExpression.NameRef) typeAnn.type()).name());
    }

    // ====================================================================
    // Multi-hop AssociationMapping → per-end navigation (Option A).
    //
    // A (A,B)->Boolean predicate cannot bind the intermediate physical row
    // of a join chain. Instead the multi-hop end is injected as a class-typed
    // Join PM on its owning class and flows through the same emitJoinChain
    // machinery as a class-typed property mapping: intermediate hops become
    // join() steps, the final hop a legacyNavigate(~prop). No standalone
    // predicate function is emitted. See docs/MAPPING_LEGACY_TO_FUNCTION.md §5.6.1b.
    // ====================================================================

    @Test
    @DisplayName("multiHopAssociation_injectedAsPerEndNavigation")
    void multiHopAssociation_injectedAsPerEndNavigation() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::City   { name: String[1]; } "
                        + "Class model::Person { x: Integer[1]; } "
                        + "Association model::Person_City { "
                        + "  person: model::Person[1]; city: model::City[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (ADDR_ID INTEGER) "
                        + "  Table T_ADDR   (ID INTEGER, CITY_ID INTEGER) "
                        + "  Table T_CITY   (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_Address( T_PERSON.ADDR_ID = T_ADDR.ID ) "
                        + "  Join Address_City( T_ADDR.CITY_ID = T_CITY.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::City:   Relational { ~mainTable [db::DB] T_CITY    name: T_CITY.NAME } "
                        + "  *model::Person: Relational { ~mainTable [db::DB] T_PERSON  x: T_PERSON.ADDR_ID } "
                        + "  model::Person_City: Relational { AssociationMapping ( "
                        + "    city: [db::DB] @Person_Address > @Address_City "
                        + "  ) } "
                        + ")");
        NormalizedModel normalized = normalizeViaPipeline(parsed);

        // The `city` end is owned by Person (the opposite end's target), so it
        // is injected into Person's realizing function as a class-typed Join.
        FunctionDefinition personFn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().endsWith("::Person"))
                .findFirst().orElseThrow();

        // Final hop emits legacyNavigate(~city); the intermediate hop
        // (Person_Address) is a join() beneath it — the shared multi-hop path.
        AppliedFunction nav = navStep(personFn);
        assertEquals("city",
                ((com.legend.model.spec.ColSpec) nav.parameters().get(1)).name(),
                "multi-hop association end emits legacyNavigate(~city)");
        AppliedFunction intermediate = (AppliedFunction) nav.parameters().get(0);
        assertEquals("join", intermediate.function(),
                "the intermediate Person_Address hop is a join() step");
        assertEquals("Person_Address",
                ((com.legend.model.spec.ColSpec) intermediate.parameters().get(1)).name(),
                "intermediate join slot is the first join's alias");

        // LOAD-BEARING: the final-hop condition must read the intermediate
        // ADDR row through the prior hop's sub-row alias, i.e.
        //   {s, t | $s.Person_Address.CITY_ID == $t.ID}
        // A naive predicate-style desugar could not bind $s.Person_Address;
        // this proves the injected chain carries the intermediate correctly.
        LambdaFunction cond = (LambdaFunction) nav.parameters().get(3);
        AppliedFunction eq = (AppliedFunction) sole(cond.body());
        assertEquals("equal", eq.function());
        AppliedProperty srcCol = (AppliedProperty) eq.parameters().get(0);
        assertEquals("CITY_ID", srcCol.property(), "source side reads ADDR.CITY_ID");
        AppliedProperty srcSub = (AppliedProperty) srcCol.receiver();
        assertEquals("Person_Address", srcSub.property(),
                "final condition reaches the intermediate row through the hop-1 alias");
        assertEquals("s", ((Variable) srcSub.receiver()).name());
        assertEquals("ID", propOf(eq.parameters().get(1)),
                "target side reaches City's PK column ($t.ID)");

        // Terminal map projects city = $row.city (the navigation slot).
        ValueSpecification cityVal = ctorOf(personFn).properties().get("city").value();
        assertEquals("city", ((AppliedProperty) cityVal).property(),
                "Person ctor reads the navigated city slot");

        // OWNERSHIP: `city` is owned by Person (the opposite end's target), so
        // it lands on Person — NOT on City. City's realizing function carries
        // no navigation (its source is a bare tableReference).
        FunctionDefinition cityFn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().endsWith("::City"))
                .findFirst().orElseThrow();
        AppliedFunction cityMap = (AppliedFunction) sole(cityFn.body());
        assertEquals("tableReference",
                ((AppliedFunction) cityMap.parameters().get(0)).function(),
                "the city end was routed to Person, so City's source is unnavigated");

        // No standalone (A,B)->Boolean predicate function is emitted.
        boolean hasPredicateFn = liftedFunctions(normalized).stream()
                .anyMatch(f -> f.qualifiedName().endsWith("::Person_City"));
        org.junit.jupiter.api.Assertions.assertFalse(hasPredicateFn,
                "multi-hop association emits no standalone predicate function");
    }

    // ====================================================================
    // Inherited-property resolution (L1)
    //
    // A PM may target a property declared on a SUPERCLASS; validation and
    // type resolution must walk the generalization chain.
    // ====================================================================

    @Test
    @DisplayName("inheritedProperty_isAcceptedByValidation")
    void inheritedProperty_isAcceptedByValidation() {
        // model::Person extends model::Base; the `id` PM targets the
        // inherited property. Pre-fix this threw "not declared".
        ParsedModel parsed = ElementParser.parse(
                "Class model::Base { id: Integer[1]; } "
                        + "Class model::Person extends model::Base { name: String[1]; } "
                        + "Database db::DB ( Table T_PERSON (ID INTEGER, NAME VARCHAR(50)) ) "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    id:   T_PERSON.ID, "
                        + "    name: T_PERSON.NAME "
                        + "  } "
                        + ")");
        FunctionDefinition fn = soleSynth(normalizeViaPipeline(parsed));
        NewInstance ni = (NewInstance) ((AppliedFunction)
                ((LambdaFunction) ((AppliedFunction) sole(fn.body())).parameters().get(1))
                        .body().get(0)).parameters().get(1);
        assertTrue(ni.properties().containsKey("id"),
                "inherited property 'id' is mapped without a 'not declared' rejection");
        assertEquals(List.of("id", "name"), List.copyOf(ni.properties().keySet()));
    }

    // ====================================================================
    // extends [parentSetId]  —  doc §5.2.3
    //
    // A child Relational binding absorbs the parent set's property mappings
    // (resolved by setId), child overriding on property-name conflict;
    // multi-level extends resolves recursively. The parent's ~mainTable is
    // NOT auto-copied (the child declares its own).
    // ====================================================================

    /** Ctor NewInstance of a synth fn whose FQN ends with {@code suffix}. */
    private static NewInstance synthCtor(NormalizedModel normalized, String suffix) {
        FunctionDefinition fn = liftedFunctions(normalized).stream()
                .filter(f -> f.qualifiedName().endsWith(suffix))
                .findFirst().orElseThrow(() ->
                        new AssertionError("no synth fn ending with " + suffix));
        return (NewInstance) ((AppliedFunction) sole(
                ((LambdaFunction) ((AppliedFunction) sole(fn.body()))
                        .parameters().get(1)).body())).parameters().get(1);
    }

    @Test
    @DisplayName("extends_mergesInheritedPropertyMappings")
    void extends_mergesInheritedPropertyMappings() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "Class model::Employee extends model::Person { salary: Integer[1]; } "
                        + "Database db::DB ( Table T (NAME VARCHAR(50), SAL INTEGER) ) "
                        + "Mapping my::M ( "
                        + "  model::Person[person]: Relational { "
                        + "    ~mainTable [db::DB] T  name: T.NAME "
                        + "  } "
                        + "  *model::Employee[emp] extends [person]: Relational { "
                        + "    ~mainTable [db::DB] T  salary: T.SAL "
                        + "  } "
                        + ")");
        NewInstance ni = synthCtor(normalizeViaPipeline(parsed), "::Employee");
        // Parent PM 'name' is inherited, then child's 'salary' (declaration order).
        assertEquals(List.of("name", "salary"), List.copyOf(ni.properties().keySet()),
                "child absorbs parent PMs (parent first), then its own");
        AppliedProperty nameRead = (AppliedProperty) toOneInner(ni.properties().get("name").value());
        assertEquals("NAME", nameRead.property(),
                "inherited 'name' still reads the parent's T.NAME column");
    }

    @Test
    @DisplayName("extends_childOverridesParentOnPropertyNameConflict")
    void extends_childOverridesParentOnPropertyNameConflict() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { name: String[1]; } "
                        + "Class model::Employee extends model::Person { } "
                        + "Database db::DB ( Table T (NAME VARCHAR(50), FULL_NAME VARCHAR(50)) ) "
                        + "Mapping my::M ( "
                        + "  model::Person[person]: Relational { "
                        + "    ~mainTable [db::DB] T  name: T.NAME "
                        + "  } "
                        + "  *model::Employee[emp] extends [person]: Relational { "
                        + "    ~mainTable [db::DB] T  name: T.FULL_NAME "
                        + "  } "
                        + ")");
        NewInstance ni = synthCtor(normalizeViaPipeline(parsed), "::Employee");
        assertEquals(List.of("name"), List.copyOf(ni.properties().keySet()),
                "conflicting property appears once");
        AppliedProperty nameRead = (AppliedProperty) toOneInner(ni.properties().get("name").value());
        assertEquals("FULL_NAME", nameRead.property(),
                "child PM wins on property-name conflict");
    }

    @Test
    @DisplayName("extends_multiLevelResolvesRecursively")
    void extends_multiLevelResolvesRecursively() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::A { pa: String[1]; } "
                        + "Class model::B extends model::A { pb: String[1]; } "
                        + "Class model::C extends model::B { pc: String[1]; } "
                        + "Database db::DB ( Table T (PA VARCHAR(9), PB VARCHAR(9), PC VARCHAR(9)) ) "
                        + "Mapping my::M ( "
                        + "  model::A[a]: Relational { ~mainTable [db::DB] T  pa: T.PA } "
                        + "  model::B[b] extends [a]: Relational { ~mainTable [db::DB] T  pb: T.PB } "
                        + "  *model::C[c] extends [b]: Relational { ~mainTable [db::DB] T  pc: T.PC } "
                        + ")");
        NewInstance ni = synthCtor(normalizeViaPipeline(parsed), "::C");
        assertEquals(List.of("pa", "pb", "pc"), List.copyOf(ni.properties().keySet()),
                "grandparent + parent + child PMs flatten in inheritance order");
    }

    @Test
    @DisplayName("extends_unknownParentSetIdThrows")
    void extends_unknownParentSetIdThrows() {
        ParsedModel parsed = ElementParser.parse(
                "Class model::Employee { x: String[1]; } "
                        + "Database db::DB ( Table T (X VARCHAR(9)) ) "
                        + "Mapping my::M ( "
                        + "  *model::Employee[emp] extends [nope]: Relational { "
                        + "    ~mainTable [db::DB] T  x: T.X "
                        + "  } "
                        + ")");
        com.legend.error.ModelException ex = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.ModelException.class,
                () -> normalizeViaPipeline(parsed));
        assertTrue(ex.getMessage().contains("nope"),
                () -> "Expected the unknown parent set id to be named; got: " + ex.getMessage());
    }

    // ====================================================================
    // Physical-table-name row-scope clash
    //
    // The row scope is keyed by physical table name. When a join chain
    // reaches the SAME non-main table through more than one path, two
    // distinct sub-rows share that table name. A join-terminal column pins
    // its own sub-row (and still resolves), but a BARE column reference to
    // that table is irreducibly ambiguous and must fail loudly rather than
    // silently resolve to an arbitrary sub-row (AGENTS.md invariant 4).
    // ====================================================================

    @Test
    @DisplayName("sameTableTwoJoins_terminalColumnsPinTheirOwnSubRow")
    void sameTableTwoJoins_terminalColumnsPinTheirOwnSubRow() {
        // Two FKs to T_FIRM => two sub-rows of the same table. Each
        // join-terminal column reads from its own slot; no ambiguity.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { primaryFirmName: String[1]; secondaryFirmName: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (PRIMARY_FIRM_ID INTEGER, SECONDARY_FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_PrimaryFirm  ( T_PERSON.PRIMARY_FIRM_ID   = T_FIRM.ID ) "
                        + "  Join Person_SecondaryFirm( T_PERSON.SECONDARY_FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    primaryFirmName:   [db::DB] @Person_PrimaryFirm   | T_FIRM.NAME, "
                        + "    secondaryFirmName: [db::DB] @Person_SecondaryFirm | T_FIRM.NAME "
                        + "  } "
                        + ")");
        NewInstance ni = synthCtor(normalizeViaPipeline(parsed), "::Person");
        AppliedProperty primary = (AppliedProperty) toOneInner(ni.properties().get("primaryFirmName").value());
        assertEquals("NAME", primary.property());
        assertEquals("Person_PrimaryFirm", ((AppliedProperty) primary.receiver()).property(),
                "primary terminal column reads from its own sub-row slot");
        AppliedProperty secondary = (AppliedProperty) toOneInner(ni.properties().get("secondaryFirmName").value());
        assertEquals("NAME", secondary.property());
        assertEquals("Person_SecondaryFirm", ((AppliedProperty) secondary.receiver()).property(),
                "secondary terminal column reads from its OWN, distinct sub-row slot");
    }

    @Test
    @DisplayName("sameTableTwoJoins_bareColumnRefIsAmbiguousAndThrows")
    void sameTableTwoJoins_bareColumnRefIsAmbiguousAndThrows() {
        // Same two-FK shape, but a third PM reads T_FIRM.NAME by bare table
        // name. The chain reaches T_FIRM via two paths, so the bare ref
        // cannot identify which firm — must fail loudly with guidance.
        ParsedModel parsed = ElementParser.parse(
                "Class model::Person { "
                        + "  primaryFirmName: String[1]; secondaryFirmName: String[1]; label: String[1]; } "
                        + "Database db::DB ( "
                        + "  Table T_PERSON (PRIMARY_FIRM_ID INTEGER, SECONDARY_FIRM_ID INTEGER) "
                        + "  Table T_FIRM   (ID INTEGER, NAME VARCHAR(50)) "
                        + "  Join Person_PrimaryFirm  ( T_PERSON.PRIMARY_FIRM_ID   = T_FIRM.ID ) "
                        + "  Join Person_SecondaryFirm( T_PERSON.SECONDARY_FIRM_ID = T_FIRM.ID ) "
                        + ") "
                        + "Mapping my::M ( "
                        + "  *model::Person: Relational { "
                        + "    ~mainTable [db::DB] T_PERSON "
                        + "    primaryFirmName:   [db::DB] @Person_PrimaryFirm   | T_FIRM.NAME, "
                        + "    secondaryFirmName: [db::DB] @Person_SecondaryFirm | T_FIRM.NAME, "
                        + "    label: T_FIRM.NAME "
                        + "  } "
                        + ")");
        String exMsg = poisonReasons(parsed);
        assertTrue(exMsg.contains("Ambiguous") && exMsg.contains("T_FIRM"),
                () -> "Expected an ambiguous-table diagnostic naming T_FIRM; got: " + exMsg);
    }
}
